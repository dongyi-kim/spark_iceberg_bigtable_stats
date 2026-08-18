"""stats_bigtable_sketch — 초대형 파티션 테이블 통계의 파티션-순차 생성 DAG (PoC 초안).

⚠ 비표준 커스텀 경로 (책자 4장 §1.7.5-b). 표준 stats_refresh(전량 스캔)로 감당되는
테이블은 이 DAG를 쓰지 않는다 — 표준 수단 소진 후, 초대형(수백 dt·2년 보관)에만.

한 런 = "파티션 N개 처리" (온보딩 페이싱 — 파티션 수 고정):
  * 온보딩 국면: 매일 N개씩 과거 파티션을 sketch로 적립 (~730 dt ÷ 30/일 ≈ 24일)
  * 정상 국면: 신규 dt 1~2개 + 재적재 감지분만 처리 → union → Puffin 갱신 커밋
  * 실패해도 마지막 성공 파티션까지 S3 상태에 체크포인트 — 다음 런이 이어받음
    (표준 procedure에는 없는 이 "재개 가능성"이 이 경로의 존재 이유)

Airflow 역할은 스케줄·요청 파일 생성·알림뿐 — 판단은 전부 spark job이
상태 기반으로 수행한다. XCom은 사용하지 않고, 수동 실행 제어값은
nullable Airflow Param으로 받아 run별 request.json에 고정한다.
(멱등: 같은 상태에서 재실행하면 같은 파티션을 다시 계산할 뿐 부작용 없음).
"""
from __future__ import annotations

import json
import os
import shlex
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# 서밋 모드:
#   spark  (기본)  — SparkSubmitOperator (실서비스: ETL Spark 클러스터)
#   docker         — 로컬 PoC 스택의 spark 컨테이너에 compose exec로 제출
#                    (호스트에 spark-submit·provider 불필요 — `airflow dags test`용)
SUBMIT_MODE = os.environ.get("STATS_SUBMIT_MODE", "spark")
if SUBMIT_MODE == "spark":
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
if SUBMIT_MODE not in {"spark", "docker"}:
    raise ValueError("STATS_SUBMIT_MODE must be spark or docker")
BENCH_DIR = os.environ.get("STATS_BENCH_DIR", "")   # docker 모드: bench/ 경로

# ── 대상 선언 (실서비스: Haflow job config가 렌더링; 초대형은 테이블당 1 DAG) ──
JOB = {
    "table": "bench.fact_sensor",             # 예시 — 실환경: 대상 초대형 테이블
    "partition_col": "dt",                    # dt(일) 또는 mt(월)
    "columns": ["device_id", "lot_id"],       # 조인 키 소수 (§1.6 추천+승인 산출물)
    "state_prefix": "s3://warehouse/stats-state",
    "max_partitions_per_run": 30,             # 온보딩 페이싱: 런당 파티션 수 고정
    "resources": {"queue": "stats", "executor_instances": "4"},
}
CATALOG_NAME = os.environ.get("STATS_CATALOG", "lake")   # 로컬 PoC 스택은 "ice"
SPARK_JOB = os.environ.get("STATS_SPARK_JOB",
                           "/opt/jobs/stats_bigtable_sketch/sketch_stats_job.py")

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


def prepare_request(job: dict, request_file: str, result_file: str,
                    request_id: str, max_partitions=None,
                    force_partitions=None, no_publish=None):
    """nullable Airflow Param을 DAG 기본값과 병합해 완전한 run 요청으로 고정."""
    effective = {
        "protocol_version": PROTOCOL_VERSION,
        "kind": "stats_bigtable_sketch",
        "request_id": request_id,
        "catalog": CATALOG_NAME,
        "table": job["table"],
        "partition_col": job["partition_col"],
        "columns": list(job["columns"]),
        "state_prefix": job["state_prefix"],
        "max_partitions": job["max_partitions_per_run"],
        "force_partitions": [],
        "publish": True,
    }
    if max_partitions is not None:
        effective["max_partitions"] = max_partitions
    if force_partitions is not None:
        if not isinstance(force_partitions, str):
            raise ValueError("force_partitions must be string or null")
        effective["force_partitions"] = list(dict.fromkeys(
            p.strip() for p in force_partitions.split(",") if p.strip()
        ))
    if no_publish is not None:
        if not isinstance(no_publish, bool):
            raise ValueError("no_publish must be boolean or null")
        effective["publish"] = not no_publish

    resolved_max_partitions = effective["max_partitions"]
    if isinstance(resolved_max_partitions, bool) or not isinstance(resolved_max_partitions, int):
        raise ValueError("max_partitions must be integer")
    if resolved_max_partitions < 1:
        raise ValueError("max_partitions must be >= 1")
    if not effective["columns"]:
        raise ValueError("columns must not be empty")
    effective["optional_params"] = {
        "max_partitions": max_partitions,
        "force_partitions": force_partitions,
        "no_publish": no_publish,
    }

    if os.path.exists(result_file):
        os.unlink(result_file)
    _write_json_atomic(request_file, effective)
    print(f"[exchange] request={request_file} request_id={request_id} "
          f"optional_params={effective['optional_params']}")


def verify_and_metrics(request_file: str, result_file: str):
    request = _read_json(request_file)
    if not os.path.exists(result_file):
        raise ValueError(f"spark job result file 없음: {result_file}")
    r = _read_json(result_file)
    if r.get("protocol_version") != PROTOCOL_VERSION:
        raise ValueError(f"result protocol mismatch: {r.get('protocol_version')}")
    if r.get("request_id") != request.get("request_id"):
        raise ValueError("stale/mismatched result file: request_id mismatch")
    if not r.get("ok"):
        raise ValueError(f"sketch job failed: {r}")
    print(f"[metrics] processed={len(r['processed'])} "
          f"(재적재 재계산 {len(r['stale_recomputed'])}) coverage={r['coverage']:.1%} "
          f"elapsed={r['elapsed_s']}s published={bool(r['published'])} ndv={r['ndv']}")
    # 온보딩 진척 알림 포인트: coverage < 1.0 이면 남은 일수 추정치를 함께 보고
    return r


def alert_on_failure(context):
    print(f"[ALERT] stats_bigtable_sketch 실패: {context['task_instance'].task_id}")
    # 실서비스: Haflow 알림 체계 호출. 체크포인트 덕에 다음 런이 이어받으므로
    # 알림만 하고 수동 개입은 반복 실패 시에만.


safe = JOB["table"].replace(".", "__")
dag_id = f"stats_bigtable_sketch__{safe}"
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
with DAG(
    dag_id=dag_id,
    description=f"파티션-순차 sketch 통계 (PoC): {JOB['table']} ({JOB['partition_col']})",
    schedule="@daily",                        # 온보딩·정상 국면 공통 — job이 상태로 판단
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,                        # 상태 파일 동시 갱신 금지
    # 명시적 type은 기본 required다. null을 함께 허용해야 UI에서
    # 비워 둘 수 있다. schedule DAG이므로 default=None도 schema에 유효해야 한다.
    params={
        "force_partitions": Param(None, type=["null", "string"]),
        "max_partitions": Param(None, type=["null", "integer"], minimum=1),
        "no_publish": Param(None, type=["null", "boolean"]),
    },
    # Jinja 결과를 문자열 "None"/"False"로 바꾸지 않고 Python 타입으로 유지.
    render_template_as_native_obj=True,
    default_args={
        "owner": "etl-stats",
        "retries": 1,                          # 파티션 단위 체크포인트라 재시도 저비용
        "retry_delay": timedelta(minutes=int(os.environ.get("STATS_RETRY_DELAY_MIN", "15"))),
        "on_failure_callback": alert_on_failure,
    },
    tags=["stats", "iceberg", "bigtable", "poc-nonstandard"],
) as dag:
    prepare = PythonOperator(
        task_id="prepare_request",
        python_callable=prepare_request,
        op_kwargs={
            "job": JOB,
            "request_file": request_file,
            "result_file": result_file,
            "request_id": request_id,
            "max_partitions": "{{ params.max_partitions }}",
            "force_partitions": "{{ params.force_partitions }}",
            "no_publish": "{{ params.no_publish }}",
        },
        do_xcom_push=False,
    )
    _args = (
        f"--request-file {shlex.quote(driver_request_file)} "
        f"--result-file {shlex.quote(driver_result_file)}"
    )
    if SUBMIT_MODE == "docker":
        # 로컬 PoC: 호스트 results/stats-exchange가 컨테이너의
        # /stats-exchange에 마운트된다. stdout은 순수 로그일 뿐 결과 전달이 아니다.
        sketch = BashOperator(
            task_id="sketch_batch",
            bash_command=(
                "set -euo pipefail\n"
                f"cd {shlex.quote(BENCH_DIR)}\n"
                "docker-compose exec -T spark "
                f"/opt/spark/bin/spark-submit --conf spark.driver.memory=4g "
                f"/opt/sketch_stats_job.py {_args}"
            ),
            do_xcom_push=False,
        )
    else:
        sketch = SparkSubmitOperator(
            task_id="sketch_batch",
            application=SPARK_JOB,
            conn_id="spark_default",           # 이식: ETL 전용 Spark, 통계 전용 큐
            queue=JOB["resources"]["queue"],
            conf={"spark.executor.instances": JOB["resources"]["executor_instances"]},
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
    prepare >> sketch >> verify
