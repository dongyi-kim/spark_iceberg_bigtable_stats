"""stats_refresh — Iceberg 테이블 Puffin NDV 통계 생성 DAG (표준 경로).

적용 대상
  · 증분적재형 중소형 테이블: compaction 후처리 트리거 + 갱신 가드
  · 전체적재형 테이블 전부:   적재 후처리 트리거, 가드 없이 매번 재계산

태스크 그래프
  resolve_targets(request.json) ─▶ guard(file) ─▶ compute_stats(result.json) ─▶ verify(file)

설계 원칙과 기술 근거 (코드를 고칠 때 깨지면 안 되는 성질들)

  [단일 DAG] 온보딩 전용 DAG를 두지 않는다. 온보딩은 로직이 아니라 "통계가
    아직 없는 상태"일 뿐이고, guard가 "최초면 무조건 실행"으로 흡수한다.
    DAG를 나누면 국면 전환(온보딩 완료 시점)을 사람이 관리해야 하고,
    같은 테이블에 대한 동시 실행 방지(max_active_runs=1)가 DAG 경계를
    넘는 순간 무력화된다.

  [멱등성] compute_table_stats는 스냅샷 기준 결정적이다. 단 실행 도중
    새 커밋이 끼어들 수 있으므로 resolve 단계에서 snapshot_id를 읽어
    고정 인자로 넘긴다 → 재시도가 항상 같은 결과를 만든다.

  [실패 격리] 통계 실패가 적재/컴팩션 파이프라인을 실패시키면 안 된다.
    그래서 이 DAG는 독립이고, 상류는 TriggerDagRunOperator(
    wait_for_completion=False)로만 이 DAG를 깨운다. 실패는 알림 콜백만.

  [가드의 근거] CBO는 NDV가 자릿수 단위로 어긋날 때만 결정을 바꾼다.
    따라서 "변화량 50% 초과 / 최소 24h 간격 / 30일 무갱신 강제" 가드로
    실행량을 통제한다. 판정은 스냅샷 summary(added-records)만 읽으므로
    비용이 사실상 0이다(테이블 스캔 없음).

  [FULL 분기의 근거] 전체적재형은 매 적재가 데이터 전면 교체라 이전
    통계가 무의미하다 → 가드 없이 재적재 직후 항상 재계산한다.
    재계산 완료 전까지는 옛 통계가 조상 스냅샷 경유로 읽히는 짧은 구간이
    있으므로, 적재 체인에서 즉시 트리거해 그 구간을 최소화한다.

환경 변수 오버라이드 (로컬 테스트용 — 실환경은 기본값)
  STATS_CATALOG          Spark 카탈로그 별칭 (기본 lake, 로컬 벤치 스택 ice)
  STATS_SPARK_JOB        spark job 경로
  STATS_SUBMIT_MODE      spark(기본, SparkSubmitOperator) | docker(compose exec — 로컬)
  STATS_EXCHANGE_DIR     Airflow·Spark driver 공유 디렉터리
  STATS_DOCKER_EXCHANGE_DIR  docker spark 컨테이너의 마운트 경로
  STATS_BENCH_DIR        docker 모드에서 compose 디렉터리
"""
from __future__ import annotations

import json
import os
import shlex
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

SUBMIT_MODE = os.environ.get("STATS_SUBMIT_MODE", "spark")
if SUBMIT_MODE == "spark":
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
if SUBMIT_MODE not in {"spark", "docker"}:
    raise ValueError("STATS_SUBMIT_MODE must be spark or docker")

CATALOG_NAME = os.environ.get("STATS_CATALOG", "lake")
# pyiceberg REST 접속 — 이식: Airflow Connection으로. env는 로컬 테스트 오버라이드.
REST_URI = os.environ.get("STATS_REST_URI", "http://polaris:8181/api/catalog")
REST_PROPS = json.loads(os.environ.get("STATS_REST_PROPS", "{}"))  # credential·warehouse 등
SPARK_JOB = os.environ.get("STATS_SPARK_JOB",
                           "/opt/jobs/stats_refresh/spark_stats_job.py")
BENCH_DIR = os.environ.get("STATS_BENCH_DIR", "")

# XCom 대신 쓰는 run 별 파일 교환소. 운영에서는 Airflow worker와
# Spark driver가 같은 경로를 보도록 NAS/공유 volume을 마운트해야 한다.
if SUBMIT_MODE == "docker":
    _default_exchange = os.path.join(BENCH_DIR, "results", "stats-exchange")
else:
    _default_exchange = "/tmp/iceberg-stats-exchange"
EXCHANGE_DIR = os.environ.get("STATS_EXCHANGE_DIR", _default_exchange)
DOCKER_EXCHANGE_DIR = os.environ.get("STATS_DOCKER_EXCHANGE_DIR", "/stats-exchange")
RUN_KEY = "{{ run_id | replace(':', '_') | replace('/', '_') | replace('+', '_') }}"
PROTOCOL_VERSION = 1

# ── Job 선언 ──────────────────────────────────────────────────────────────────
# 실서비스에서는 이 리스트를 Haflow job config가 렌더링한다. 컬럼 목록은
# 코드에 두지 않는 것이 원칙(선정 기준이 바뀌어도 재배포 없이 반영) —
# columns는 중앙 config 테이블/추천 파이프라인의 산출물을 참조하는 자리다.
JOBS = [
    {
        "table": "bench.fact_sensor",
        "load_type": "INCREMENTAL",
        "columns": ["device_id", "lot_id", "wafer_id", "site_id", "event_type"],
        "guards": {"changed_ratio": 0.5, "min_interval_h": 24, "max_age_d": 30},
        "resources": {"queue": "stats", "executor_instances": "2"},
        # 상류 compaction DAG가 트리거하는 것이 1차 경로. 스케줄은 안전망 —
        # 가드가 있으므로 중복 실행돼도 skip으로 끝난다(멱등).
        "schedule": "@daily",
    },
    {
        "table": "bench.dim_device",
        "load_type": "FULL",
        "columns": ["device_id", "site_id"],
        "guards": {},                    # FULL은 가드 없음 — 아래 guard_check 참조
        "resources": {"queue": "stats", "executor_instances": "1"},
        "schedule": None,                # 적재 DAG 트리거 전용 (재적재마다 실행)
    },
]


# ── 태스크 구현 ───────────────────────────────────────────────────────────────
def _iceberg_table(table: str):
    """pyiceberg REST로 테이블 로드.

    Airflow 워커에서 Spark 세션 없이 metadata.json·스냅샷만 읽기 위한 경량 경로.
    (pyiceberg 0.7+ 기준 — 워커 이미지에 없으면 이 두 판정 태스크를 Spark thin job
    으로 옮겨도 로직은 동일하다.)
    """
    from pyiceberg.catalog import load_catalog
    cat = load_catalog("stats", **{"type": "rest", "uri": REST_URI, **REST_PROPS})
    return cat.load_table(table)


def _read_json(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        value = json.load(f)
    if not isinstance(value, dict):
        raise ValueError(f"JSON object expected: {path}")
    return value


def _write_json_atomic(path: str, value: dict):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, target)


def resolve_targets(job: dict, request_file: str, result_file: str, request_id: str):
    """스냅샷 고정 + 가드 판정 사실 수집. 비용: 메타데이터 읽기뿐(스캔 0).

    수집하는 사실:
      snapshot_id            이번 실행이 기준 삼을 스냅샷 (멱등성의 축)
      total_records          현재 스냅샷 summary의 총 행 수 (ndv sanity 상한)
      last_stats_*           마지막 통계의 스냅샷·시각 — metadata.statistics 배열에서
                             가장 최근 등록을 찾는다 (없으면 "최초 온보딩")
      added_records_since    마지막 통계 이후 스냅샷들의 added-records 합 —
                             변화량 가드의 입력. summary만 합산하므로 비용 0.
    """
    tbl = _iceberg_table(job["table"])
    snap = tbl.current_snapshot()
    if snap is None:
        raise ValueError(f"{job['table']}: no snapshot (빈 테이블)")
    total = int(snap.summary.get("total-records", 0)) if snap.summary else 0

    last_stats_snap_id, last_stats_ts_ms = None, None
    for sf in (tbl.metadata.statistics or []):
        sid = int(sf.snapshot_id)
        s = tbl.snapshot_by_id(sid)
        ts = s.timestamp_ms if s else 0
        if last_stats_ts_ms is None or ts > last_stats_ts_ms:
            last_stats_snap_id, last_stats_ts_ms = sid, ts

    added_since = 0
    if last_stats_snap_id is not None:
        seen_last = False
        for s in sorted(tbl.snapshots(), key=lambda x: x.timestamp_ms):
            if seen_last and s.summary:
                added_since += int(s.summary.get("added-records", 0) or 0)
            if s.snapshot_id == last_stats_snap_id:
                seen_last = True

    request = {
        "protocol_version": PROTOCOL_VERSION,
        "kind": "stats_refresh",
        "request_id": request_id,
        "catalog": CATALOG_NAME,
        "table": job["table"],
        "load_type": job["load_type"],
        "snapshot_id": int(snap.snapshot_id),
        "total_records": total,
        "columns": list(job["columns"]),
        "guards": dict(job["guards"]),
        "last_stats_snapshot_id": last_stats_snap_id,
        "last_stats_ts_ms": last_stats_ts_ms,
        "added_records_since_stats": added_since,
    }
    # 같은 run을 재시도할 때 이전 결과를 성공으로 오판하지 않도록
    # resolve 시점에 결과를 초기화한다. run 디렉터리 밖은 건드리지 않는다.
    if os.path.exists(result_file):
        os.unlink(result_file)
    _write_json_atomic(request_file, request)
    print(f"[exchange] request={request_file} request_id={request_id}")


def guard_check(request_file: str) -> bool:
    """실행/skip 판정. False면 이후 태스크 전부 skip (ShortCircuit).

    FULL:        항상 실행. 재적재 = 전면 교체라 이전 통계가 무의미하므로
                 가드를 둘 이유가 없다 (변화량 가드는 어차피 항상 발동).
    INCREMENTAL: ① 통계가 아예 없으면(최초 온보딩) 무조건 실행 — 이것이
                    "온보딩 DAG가 따로 없는" 이유의 구현부다.
                 ② 24h 이내 재실행 금지 (고빈도 compaction의 과잉 실행 방지)
                 ③ 30일 무갱신이면 강제 실행 (조용히 도메인이 자라는 키 보호)
                 ④ 마지막 통계 이후 추가 행이 당시 규모의 50%를 넘으면 실행
                    — "자릿수 원칙"의 보수적 적용. 그 미만이면 NDV가 플랜을
                    바꿀 만큼 변했을 가능성이 낮아 skip.
    """
    f = _read_json(request_file)
    if f["load_type"] == "FULL":
        return True
    g = f["guards"]
    if f["last_stats_snapshot_id"] is None:
        print("guard: 최초 온보딩 — 실행")
        return True
    now_ms = datetime.now(timezone.utc).timestamp() * 1000
    age_h = (now_ms - f["last_stats_ts_ms"]) / 3600_000
    if age_h < g.get("min_interval_h", 24):
        print(f"guard: 마지막 통계 {age_h:.1f}h 전 (< {g['min_interval_h']}h) — skip")
        return False
    if age_h > g.get("max_age_d", 30) * 24:
        print(f"guard: {age_h/24:.0f}일 무갱신 — 하한 가드 강제 실행")
        return True
    base = max(f["total_records"] - f["added_records_since_stats"], 1)
    ratio = f["added_records_since_stats"] / base
    if ratio > g.get("changed_ratio", 0.5):
        print(f"guard: 변화량 {ratio:.0%} > {g['changed_ratio']:.0%} — 실행")
        return True
    print(f"guard: 변화량 {ratio:.0%} — skip (NDV는 자릿수 정확도면 충분)")
    return False


def verify_and_metrics(request_file: str, result_file: str):
    """Spark job이 atomic write한 result.json을 읽어 성공 판정 + 메트릭.

    검증 1~3(파일 실재 / metadata 등록·컬럼 일치 / ndv sanity)은 spark job
    내부에서 수행이 끝난 상태다 — 여기서는 결과를 판정하고 소요시간·NDV를
    메트릭으로 남긴다(초안: 로그. 실서비스: Haflow 통계로 교체).
    """
    request = _read_json(request_file)
    if not os.path.exists(result_file):
        raise ValueError(f"spark job result file 없음: {result_file}")
    r = _read_json(result_file)
    if r.get("protocol_version") != PROTOCOL_VERSION:
        raise ValueError(f"result protocol mismatch: {r.get('protocol_version')}")
    if r.get("request_id") != request.get("request_id"):
        raise ValueError("stale/mismatched result file: request_id mismatch")
    if not r.get("ok"):
        raise ValueError(f"stats job failed: {r}")
    print(f"[metrics] table={r['table']} elapsed={r['elapsed_s']}s "
          f"file={r['file_bytes']}B ndv={r['ndv']}")
    return r


def alert_on_failure(context):
    """실패 알림만 — 본 파이프라인은 이 DAG와 분리되어 있어 영향 없음."""
    print(f"[ALERT] stats_refresh 실패: {context['task_instance'].task_id} "
          f"({context['dag'].dag_id})")


# ── DAG 팩토리 ────────────────────────────────────────────────────────────────
def build_dag(job: dict) -> DAG:
    safe = job["table"].replace(".", "__")
    dag_id = f"stats_refresh__{safe}"
    host_run_dir = f"{EXCHANGE_DIR}/{dag_id}/{RUN_KEY}"
    driver_run_dir = (
        f"{DOCKER_EXCHANGE_DIR}/{dag_id}/{RUN_KEY}"
        if SUBMIT_MODE == "docker" else host_run_dir
    )
    request_file = f"{host_run_dir}/request.json"
    result_file = f"{host_run_dir}/result.json"
    driver_request_file = f"{driver_run_dir}/request.json"
    driver_result_file = f"{driver_run_dir}/result.json"
    request_id = f"{dag_id}:{{{{ run_id }}}}"
    dag = DAG(
        dag_id=dag_id,
        description=f"Puffin NDV 생성: {job['table']} ({job['load_type']})",
        schedule=job.get("schedule"),
        start_date=datetime(2026, 7, 1),
        catchup=False,
        # 같은 테이블에 통계 커밋이 겹치면 Iceberg 낙관적 커밋 충돌로 한쪽이
        # 재시도된다 — DAG 단위 직렬화가 가장 싼 예방책.
        max_active_runs=1,
        default_args={
            "owner": "etl-stats",
            "retries": 1,                # snapshot_id 고정이라 재시도 멱등
            "retry_delay": timedelta(
                minutes=int(os.environ.get("STATS_RETRY_DELAY_MIN", "10"))),
            "on_failure_callback": alert_on_failure,
        },
        tags=["stats", "iceberg", job["load_type"].lower()],
    )
    with dag:
        resolve = PythonOperator(
            task_id="resolve_targets",
            python_callable=resolve_targets,
            op_kwargs={
                "job": job,
                "request_file": request_file,
                "result_file": result_file,
                "request_id": request_id,
            },
            do_xcom_push=False,
        )
        guard = ShortCircuitOperator(
            task_id="guard",
            python_callable=guard_check,
            op_kwargs={"request_file": request_file},
            do_xcom_push=False,
        )
        _args = (
            f"--request-file {shlex.quote(driver_request_file)} "
            f"--result-file {shlex.quote(driver_result_file)}"
        )
        if SUBMIT_MODE == "docker":
            # 로컬 테스트: 호스트 results/stats-exchange가 컨테이너의
            # /stats-exchange에 마운트된다. stdout은 순수 로그일 뿐 결과 전달이 아니다.
            compute = BashOperator(
                task_id="compute_stats",
                bash_command=(
                    "set -euo pipefail\n"
                    f"cd {shlex.quote(BENCH_DIR)}\n"
                    "docker-compose exec -T spark "
                    f"/opt/spark/bin/spark-submit --conf spark.driver.memory=4g "
                    f"/opt/spark_stats_job.py {_args}"
                ),
                do_xcom_push=False,
            )
        else:
            compute = SparkSubmitOperator(
                task_id="compute_stats",
                application=SPARK_JOB,
                conn_id="spark_default",       # 이식: ETL Spark, 통계 전용 큐
                queue=job["resources"].get("queue", "stats"),
                conf={"spark.executor.instances":
                      job["resources"].get("executor_instances", "2")},
                application_args=[
                    "--request-file", driver_request_file,
                    "--result-file", driver_result_file,
                ],
                do_xcom_push=False,
            )
        verify = PythonOperator(
            task_id="verify_and_metrics",
            python_callable=verify_and_metrics,
            op_kwargs={"request_file": request_file, "result_file": result_file},
            do_xcom_push=False,
        )
        resolve >> guard >> compute >> verify
    return dag


for _job in JOBS:
    globals()[f"dag_stats_refresh__{_job['table'].replace('.', '__')}"] = build_dag(_job)
