"""spark_stats_job.py — Iceberg 표준 procedure로 Puffin NDV 생성 + 검증 (표준 경로).

spark-submit 진입점. DAG(stats_refresh_dag.py)의 compute_stats 태스크가 호출한다.

기술 배경 — compute_table_stats의 세 가지 성질이 이 파일의 구조를 결정한다:
  1) 전량 스캔: 파티션 인자가 없다(인자: table/snapshot_id/columns). 비용은
     "행 수 × 대상 컬럼 수"에 비례하므로 columns 한정이 유일한 비용 레버다.
  2) 스냅샷 기준 결정적: 같은 snapshot_id로 재실행하면 같은 결과 — DAG가
     resolve 단계에서 고정한 snapshot_id를 받아 그대로 쓴다(재시도 멱등).
  3) 커밋 산출물: Puffin 파일(컬럼별 theta sketch blob + footer의 ndv 값)이
     metadata.json의 statistics 배열에 등록된다. StarRocks/Spark는 footer의
     ndv만 읽고, sketch 본문은 다른 소비자(union·타 엔진)용이다.

검증을 job 안에 두는 이유: "procedure가 정상 종료했다" ≠ "엔진이 읽을 수 있다".
성공의 정의를 "소비 가능 상태"로 두고 3단계를 이 안에서 확인한다:
  검증 1  반환된 statistics_file이 스토리지에 실재하는가
  검증 2  metadata.json에 대상 snapshot-id로 등록됐고 blob fields가
          요청 컬럼 집합과 일치하는가
  검증 3  각 ndv가 0 < ndv <= total-records 인가 (스냅샷 summary 대비)
(검증 4 메트릭은 DAG의 verify 태스크, 검증 5 소비자 EXPLAIN 확인은 일일 배치 몫)

출력 프로토콜: --result-file에 JSON을 atomic write한다. stdout의
RESULT|<json>은 사람이 로그에서 볼 수 있게 남기지만 DAG는 읽지 않는다.
실패 시에도 {"ok": false, ...}를 결과 파일에 남기고 exit 1한다.

사용:
  spark-submit spark_stats_job.py \
    --request-file /shared/stats/run/request.json \
    --result-file /shared/stats/run/result.json
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

PROTOCOL_VERSION = 1
_RESULT_FILE = None
_REQUEST_ID = None


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


def emit_result(value: dict):
    payload = {
        "protocol_version": PROTOCOL_VERSION,
        "request_id": _REQUEST_ID,
        **value,
    }
    if _RESULT_FILE:
        _write_json_atomic(_RESULT_FILE, payload)
    print(f"RESULT|{json.dumps(payload, ensure_ascii=False)}", flush=True)


def fail(msg: str, **extra):
    emit_result({"ok": False, "error": msg, **extra})
    sys.exit(1)


def load_request(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        request = json.load(f)
    if not isinstance(request, dict):
        raise ValueError("request must be a JSON object")
    if request.get("protocol_version") != PROTOCOL_VERSION:
        raise ValueError(f"unsupported protocol_version: {request.get('protocol_version')}")
    if request.get("kind") != "stats_refresh":
        raise ValueError(f"unexpected request kind: {request.get('kind')}")
    required = {"request_id", "catalog", "table", "snapshot_id", "columns"}
    missing = sorted(required - request.keys())
    if missing:
        raise ValueError(f"missing request fields: {missing}")
    if not isinstance(request["columns"], list) or not request["columns"]:
        raise ValueError("columns must be a non-empty JSON array")
    return request


def read_json_via_io(spark, jvm, io, path: str) -> dict:
    """Iceberg FileIO로 JSON 파일 읽기.

    Hadoop FS를 쓰지 않는 이유: Iceberg 전용 스택에는 s3:// 스킴의 Hadoop
    FileSystem이 설정돼 있지 않은 경우가 많다. 테이블을 실제로 읽고 쓰는
    바로 그 IO(S3FileIO — 카탈로그의 엔드포인트·자격증명 포함)를 재사용한다.
    바이트 수거는 commons-io로 한 번에 (py4j 왕복 1회 — 바이트 루프 금지).
    """
    stream = io.newInputFile(path).newStream()
    try:
        data = bytes(jvm.org.apache.commons.io.IOUtils.toByteArray(stream))
    finally:
        stream.close()
    return json.loads(data.decode())


def main():
    global _RESULT_FILE, _REQUEST_ID
    ap = argparse.ArgumentParser()
    ap.add_argument("--request-file", required=True)
    ap.add_argument("--result-file", required=True)
    a = ap.parse_args()
    _RESULT_FILE = a.result_file
    try:
        request = load_request(a.request_file)
    except Exception as e:
        fail(f"invalid request file: {type(e).__name__}: {str(e)[:300]}")
    _REQUEST_ID = str(request["request_id"])
    catalog = str(request["catalog"])
    table = str(request["table"])
    snapshot_id = int(request["snapshot_id"])
    cols = [str(c).strip() for c in request["columns"] if str(c).strip()]
    if not cols:
        fail("no target columns")
    fq = f"{catalog}.{table}"

    spark = SparkSession.builder.appName(f"stats-refresh:{table}").getOrCreate()
    jvm = spark._jvm                                                # noqa: SLF001

    # ── compute: 표준 procedure 호출 (전량 스캔 — 대상 컬럼만) ──
    col_arr = ", ".join(f"'{c}'" for c in cols)
    t0 = time.time()
    print(f"[stats] compute_table_stats {table} snapshot={snapshot_id} "
          f"cols={cols}", flush=True)
    try:
        res = spark.sql(
            f"CALL {catalog}.system.compute_table_stats("
            f"table => '{table}', snapshot_id => {snapshot_id}, "
            f"columns => array({col_arr}))").collect()
    except Exception as e:
        fail(f"compute_table_stats: {type(e).__name__}: {str(e)[:300]}")
    elapsed = round(time.time() - t0, 1)
    stats_file = str(res[0][0]) if res and res[0] else ""
    if not stats_file:
        fail("procedure returned no statistics_file")

    # 이후 검증은 전부 테이블의 FileIO·메타테이블 경유 (Hadoop FS 불필요)
    tbl = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, fq)                                   # noqa: SLF001
    tbl.refresh()   # Spark 카탈로그 테이블 캐시 무효화 — 최신 metadata 기준으로 검증

    # ── 검증 1: statistics_file 실재 ──
    try:
        f = tbl.io().newInputFile(stats_file)
        if not f.exists():
            fail("statistics_file not found on storage", statistics_file=stats_file)
        size = int(f.getLength())
    except SystemExit:
        raise
    except Exception as e:
        fail(f"verify-1 storage check: {str(e)[:200]}", statistics_file=stats_file)

    # ── 검증 2: metadata.json 등록 + 요청 컬럼 일치 ──
    # blob의 fields는 컬럼 이름이 아니라 스키마 field-id — 이름으로 역해석해
    # 요청 집합과 비교한다 (컬럼 rename에도 안전한 것이 field-id 방식의 이유).
    try:
        meta_path = (spark.sql(
            f"SELECT file FROM {fq}.metadata_log_entries "
            f"ORDER BY timestamp DESC LIMIT 1").collect()[0][0])
        meta = read_json_via_io(spark, jvm, tbl.io(), meta_path)
        entry = next((s for s in meta.get("statistics", [])
                      if int(s.get("snapshot-id", -1)) == snapshot_id), None)
        if entry is None:
            fail("not registered in metadata.statistics for target snapshot")
        id2name = {}
        for f_ in meta.get("schemas", [{}])[-1].get("fields", []):
            id2name[f_.get("id")] = f_.get("name")
        covered = {id2name.get(fid) for blob in entry.get("blob-metadata", [])
                   for fid in blob.get("fields", [])}
        missing = [c for c in cols if c not in covered]
        if missing:
            fail("blob fields mismatch", missing=missing)
    except SystemExit:
        raise
    except Exception as e:
        fail(f"verify-2 metadata check: {str(e)[:200]}")

    # ── 검증 3: ndv sanity — 0 < ndv <= total-records × 1.05 ──
    # "행 수보다 큰 고유값"은 논리적으로 불가능하지만, theta sketch는 ±1.6%대
    # 근사라 전-유니크 컬럼(NDV ≈ 행 수)에서 행 수를 소폭 넘을 수 있다 —
    # E2E에서 실측된 케이스(2,009,381 vs 2,000,000, +0.47%). 5% 여유를 두고,
    # 그 이상의 초과만 생성 이상 신호(잘못된 스냅샷 기준 등)로 판정한다.
    ndv = {}
    try:
        snap = (spark.sql(
            f"SELECT summary FROM {fq}.snapshots "
            f"WHERE snapshot_id = {snapshot_id}").collect())
        total = int(dict(snap[0][0]).get("total-records", 0)) if snap else 0
        for blob in entry.get("blob-metadata", []):
            v = blob.get("properties", {}).get("ndv")
            name = id2name.get((blob.get("fields") or [None])[0])
            if v is not None and name:
                ndv[name] = int(v)
        bad = {k: v for k, v in ndv.items() if not (0 < v <= max(total, 1) * 1.05)}
        if bad:
            fail("ndv sanity violated", bad=bad, total_records=total)
    except SystemExit:
        raise
    except Exception as e:
        fail(f"verify-3 ndv sanity: {str(e)[:200]}")

    emit_result({
        "ok": True, "table": table, "snapshot_id": snapshot_id,
        "columns": cols, "elapsed_s": elapsed,
        "statistics_file": stats_file, "file_bytes": size, "ndv": ndv,
    })


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        fail(f"unhandled: {type(exc).__name__}: {str(exc)[:300]}")
