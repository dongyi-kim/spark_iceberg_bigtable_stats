"""sketch_stats_job.py — 초대형 파티션 테이블의 파티션-순차 theta sketch 통계 생성 (PoC 초안).

⚠ 비표준 커스텀 잡 (책자 4장 §1.7.5-b) — 표준 `compute_table_stats`(전량 스캔)로
감당 안 되는 초대형 테이블(dt/mt 파티션, 보관 ~2년) 전용. 표준 수단 소진 후 PoC로만.

동작 (매 런):
  1. discover  — Iceberg `.partitions` 메타테이블에서 파티션 목록과
                 last_updated_snapshot_id 조회 (스캔 0)
  2. plan      — 작업 목록 = 재적재 감지 파티션(자동 재계산) + 미처리 파티션(온보딩
                 커서 순서), 런당 request.max_partitions 개로 절단
                 (파티션 수 고정 페이싱)
  3. compute   — 파티션별로 대상 컬럼 K개의 theta sketch를 1-pass로 계산
                 (Apache DataSketches python — 자바와 직렬화 호환) → S3에 저장
  4. publish   — 전 파티션 커버 시(또는 이미 커버 상태에서 갱신 시) 컬럼별로
                 전 파티션 sketch를 union → Iceberg 표준 Puffin
                 (blob type `apache-datasketches-theta-v1`, footer에 ndv)로 작성,
                 UpdateStatistics 커밋 → SR·Spark·(Trino) 모두 소비 가능
  5. result.json atomic write (Airflow verify 태스크 프로토콜)

상태 (S3 직접 저장, request.state_prefix 하위):
  {prefix}/{table}/_state.json                     — 파티션별 (계산 시점 last_updated_snapshot_id, 행 수)
  {prefix}/{table}/{column}/{partition}.theta      — 파티션·컬럼별 compact theta sketch

의존: executor·driver에 `pip install datasketches` (Apache DataSketches python 바인딩).
      Iceberg REST 카탈로그가 Spark 세션에 설정되어 있을 것.

사용:
  spark-submit sketch_stats_job.py \
    --request-file /shared/stats/run/request.json \
    --result-file /shared/stats/run/result.json
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

from pyspark.sql import SparkSession

SKETCH_LG_K = 12          # theta sketch 정밀도 (±1.6%대) — compute_table_stats와 동급 목표
BLOB_TYPE = "apache-datasketches-theta-v1"   # Iceberg StandardBlobTypes
PROTOCOL_VERSION = 1
_RESULT_FILE = None
_REQUEST_ID = None


def log(msg):
    print(f"[sketch] {msg}", flush=True)


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
    print("RESULT|" + json.dumps(payload, ensure_ascii=False), flush=True)


def fail(msg, **extra):
    emit_result({"ok": False, "error": msg, **extra})
    sys.exit(1)


def load_request(path: str) -> SimpleNamespace:
    with open(path, encoding="utf-8") as f:
        request = json.load(f)
    if not isinstance(request, dict):
        raise ValueError("request must be a JSON object")
    if request.get("protocol_version") != PROTOCOL_VERSION:
        raise ValueError(f"unsupported protocol_version: {request.get('protocol_version')}")
    if request.get("kind") != "stats_bigtable_sketch":
        raise ValueError(f"unexpected request kind: {request.get('kind')}")
    required = {
        "request_id", "catalog", "table", "partition_col", "columns",
        "state_prefix", "max_partitions", "force_partitions", "publish",
    }
    missing = sorted(required - request.keys())
    if missing:
        raise ValueError(f"missing request fields: {missing}")
    columns = request["columns"]
    force = request["force_partitions"]
    max_partitions = request["max_partitions"]
    publish = request["publish"]
    if not isinstance(columns, list) or not columns:
        raise ValueError("columns must be a non-empty JSON array")
    if not isinstance(force, list):
        raise ValueError("force_partitions must be a JSON array")
    if isinstance(max_partitions, bool) or not isinstance(max_partitions, int):
        raise ValueError("max_partitions must be integer")
    if max_partitions < 1:
        raise ValueError("max_partitions must be >= 1")
    if not isinstance(publish, bool):
        raise ValueError("publish must be boolean")
    return SimpleNamespace(
        request_id=str(request["request_id"]),
        catalog=str(request["catalog"]),
        table=str(request["table"]),
        partition_col=str(request["partition_col"]),
        columns=",".join(str(c) for c in columns),
        state_prefix=str(request["state_prefix"]),
        max_partitions=max_partitions,
        force_partitions=",".join(str(p) for p in force),
        publish="true" if publish else "false",
    )


# ── S3 상태 입출력 — Iceberg FileIO 재사용 ───────────────────────────────────
# 테이블을 쓰는 바로 그 IO(S3FileIO — 카탈로그의 엔드포인트·자격증명 설정 포함)를
# 그대로 쓴다. Hadoop FS를 쓰지 않는 이유: Iceberg 전용 스택에는 s3:// 스킴의
# Hadoop FileSystem이 설정돼 있지 않은 경우가 많다(이 PoC 스택이 그 예).
class Store:
    def __init__(self, jvm, file_io, prefix: str):
        self.jvm, self.io = jvm, file_io
        self.prefix = prefix.rstrip("/")

    def read_bytes(self, rel):
        f = self.io.newInputFile(f"{self.prefix}/{rel}")
        if not f.exists():
            return None
        stream = f.newStream()
        try:
            # commons-io로 한 번에 byte[] 수거 (py4j 왕복 1회 — 바이트 루프 금지)
            return bytes(self.jvm.org.apache.commons.io.IOUtils.toByteArray(stream))
        finally:
            stream.close()

    def write_bytes(self, rel, data: bytes):
        out = self.io.newOutputFile(f"{self.prefix}/{rel}").createOrOverwrite()
        out.write(bytearray(data))
        out.close()


# ── sketch 계산 (파티션 1개, 컬럼 K개, 1-pass) ───────────────────────────────
def compute_partition_sketches(spark, fq: str, pcol: str, pval: str, cols: list[str]):
    """파티션 하나를 한 번 스캔해 대상 컬럼 K개의 theta sketch를 동시에 만든다.

    동작 구조 (theta sketch의 병합 가능성을 2단으로 활용):
      1) executor: 각 Spark 태스크가 자기 데이터 조각으로 "부분 sketch"를 만들어 반환
      2) driver:   부분 sketch들을 union → 이 파티션의 최종 sketch
    부분으로 나눠 만들어도 union 결과는 전체를 한 번에 스캔한 것과 동일 — 이 성질이
    이 잡 전체(파티션별 적립 → 나중에 전체 union)의 근거이기도 하다.

    성능 노트: sketch.update()는 C++ 바인딩이라 python 루프여도 코어당 수백만 행/s.
    컬럼 K개를 같은 스캔에서 갱신하므로 파티션당 스캔은 정확히 1회다.
    """
    import pandas as pd  # noqa: F401  (mapInPandas 요건)

    df = spark.table(fq).where(f"{pcol} = '{pval}'").select(*cols)

    def build(batches):
        from datasketches import update_theta_sketch
        sks = {c: update_theta_sketch(lg_k=SKETCH_LG_K) for c in cols}
        n = 0
        for pdf in batches:
            n += len(pdf)
            for c in cols:
                col = pdf[c].dropna()
                upd = sks[c].update
                for v in col.to_numpy():
                    upd(v if isinstance(v, (str, bytes, int, float)) else str(v))
        import pandas as pd
        yield pd.DataFrame({
            "col": list(cols),
            "sk": [sks[c].compact().serialize() for c in cols],
            "rows": [n] * len(cols),
        })

    parts = df.mapInPandas(build, schema="col string, sk binary, rows long").collect()
    from datasketches import theta_union, compact_theta_sketch
    out, rows = {}, 0
    for c in cols:
        u = theta_union()
        for r in parts:
            if r["col"] == c:
                u.update(compact_theta_sketch.deserialize(bytes(r["sk"])))
        out[c] = u.get_result().serialize()
    rows = sum(r["rows"] for r in parts if r["col"] == cols[0])
    return out, rows


# ── Puffin 작성 + 커밋 (Iceberg Java API via py4j) ───────────────────────────
def publish_puffin(spark, catalog: str, table: str, cols: list[str],
                   col_sketch_bytes: dict, col_ndv: dict):
    """union된 컬럼별 sketch를 Iceberg 표준 통계 파일(Puffin)로 만들어 테이블에 커밋.

    표준 procedure(compute_table_stats)가 내부에서 하는 일을 그대로 재현한다:
      Puffin 파일 작성(blob = 컬럼별 theta sketch + properties.ndv)
      → GenericStatisticsFile로 감싸 updateStatistics() 커밋(metadata.json 등록).
    소비자별로 읽는 부분: StarRocks·Spark는 blob metadata의 `ndv` 요약값만,
    Trino류는 sketch 본문까지 — 그래서 본문도 진짜 theta로 채운다(호환성).

    구현 노트: PySpark에는 이 API의 python 래퍼가 없어 py4j로 JVM 객체를 직접
    다룬다. 호출이 어색해 보여도 전부 "자바 코드 한 줄"의 직역이다.
    """
    jvm = spark._jvm                                                # noqa: SLF001
    tbl = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, f"{catalog}.{table}")                 # noqa: SLF001
    # Spark 카탈로그의 테이블 캐시 때문에 세션 시작 시점 스냅샷이 올 수 있다 —
    # refresh로 최신 스냅샷에 통계를 붙인다 (동시 변경 테스트에서 실측된 이슈).
    tbl.refresh()
    snap = tbl.currentSnapshot()
    snap_id, seq = snap.snapshotId(), snap.sequenceNumber()

    path = f"{tbl.location()}/metadata/{snap_id}-{uuid.uuid4()}.stats"
    out = tbl.io().newOutputFile(path)
    writer = jvm.org.apache.iceberg.puffin.Puffin.write(out) \
        .createdBy("stats_bigtable_sketch (partition-union PoC)").build()
    for c in cols:
        fid = tbl.schema().findField(c).fieldId()
        fields = jvm.java.util.ArrayList()
        fields.add(jvm.java.lang.Integer(fid))
        props = jvm.java.util.HashMap()
        props.put("ndv", str(int(col_ndv[c])))
        blob = jvm.org.apache.iceberg.puffin.Blob(
            BLOB_TYPE, fields, snap_id, seq,
            jvm.java.nio.ByteBuffer.wrap(bytearray(col_sketch_bytes[c])),
            None, props)
        writer.add(blob)
    writer.close()

    # Puffin writer의 blob metadata(puffin 패키지 타입)를 테이블 커밋용
    # GenericBlobMetadata로 변환해야 REST 직렬화가 된다 — 표준 procedure도 동일하게 함.
    from_generic = getattr(jvm.org.apache.iceberg.GenericBlobMetadata, "from")  # 'from'은 py 예약어
    blobs = jvm.java.util.ArrayList()
    it = writer.writtenBlobsMetadata().iterator()
    while it.hasNext():
        blobs.add(from_generic(it.next()))
    stats_file = jvm.org.apache.iceberg.GenericStatisticsFile(
        snap_id, path, writer.fileSize(), writer.footerSize(), blobs)
    # setStatistics 시그니처가 Iceberg 버전에 따라 1-인자/2-인자(구형) — 둘 다 시도.
    # 둘 다 실패하면 사용 중인 Iceberg 버전을 기록해 주세요 (PoC-GUIDE §7-1).
    upd = tbl.updateStatistics()
    try:
        upd.setStatistics(stats_file).commit()          # Iceberg 신형 API
    except Exception:
        upd.setStatistics(snap_id, stats_file).commit()  # 구형 시그니처 폴백
    return {"snapshot_id": snap_id, "statistics_file": path,
            "file_bytes": int(writer.fileSize())}


def main():
    global _RESULT_FILE, _REQUEST_ID
    ap = argparse.ArgumentParser()
    ap.add_argument("--request-file", required=True)
    ap.add_argument("--result-file", required=True)
    cli = ap.parse_args()
    _RESULT_FILE = cli.result_file
    try:
        a = load_request(cli.request_file)
    except Exception as e:
        fail(f"invalid request file: {type(e).__name__}: {str(e)[:300]}")
    _REQUEST_ID = a.request_id
    cols = [c.strip() for c in a.columns.split(",") if c.strip()]
    fq = f"{a.catalog}.{a.table}"
    pcol = a.partition_col

    spark = SparkSession.builder.appName(f"sketch-stats:{a.table}").getOrCreate()
    jvm = spark._jvm                                                # noqa: SLF001
    jtbl = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, f"{a.catalog}.{a.table}")             # noqa: SLF001
    store = Store(jvm, jtbl.io(), f"{a.state_prefix}/{a.table.replace('.', '__')}")

    # ── 1. discover: 파티션 목록 + 최종 갱신 스냅샷 + 행 수 (메타테이블 — 스캔 0) ──
    rows = spark.sql(
        f"SELECT partition.{pcol} AS p, last_updated_snapshot_id AS s, "
        f"record_count AS rc FROM {fq}.partitions").collect()
    live = {str(r["p"]): (int(r["s"]), int(r["rc"])) for r in rows}
    if not live:
        fail("no partitions found")
    # 스냅샷별 operation — 컴팩션(replace) 판별용 (역시 메타테이블만)
    ops = {int(r["snapshot_id"]): str(r["operation"]) for r in
           spark.sql(f"SELECT snapshot_id, operation FROM {fq}.snapshots").collect()}

    raw = store.read_bytes("_state.json")
    state = json.loads(raw) if raw else {"partitions": {}}
    done = state["partitions"]                    # {pval: {"snap": id, "rows": n}}

    # ── 2. plan: 무엇을 계산할지 결정 ──
    #  변경 감지 = 파티션의 last_updated_snapshot_id가 계산 시점과 다름. 단:
    #   · 그 변경이 컴팩션(operation='replace')이고 행 수도 그대로면 **데이터 불변** —
    #     sketch 재계산 없이 상태의 스냅샷만 동기화한다(컴팩션 후 불필요 재계산 방지).
    #   · 그 외(append로 증가·overwrite 재적재)는 stale → 해당 파티션만 재계산.
    #  new = 아직 sketch가 없는 파티션(온보딩 잔여 + 신규 적재분), 오래된 것부터.
    #  런당 max_partitions개로 절단 = "파티션 수 고정" 페이싱 — 런 시간 예측 가능.
    forced = [p for p in a.force_partitions.split(",") if p]
    stale, synced = [], []
    for p in sorted(live):
        if p not in done or done[p]["snap"] == live[p][0]:
            continue
        cur_snap, cur_rc = live[p]
        if ops.get(cur_snap) == "replace" and done[p].get("rows") == cur_rc:
            done[p]["snap"] = cur_snap            # 컴팩션 — 계산 생략, 상태만 맞춤
            synced.append(p)
        else:
            stale.append(p)
    if synced:
        store.write_bytes("_state.json", json.dumps(state, ensure_ascii=False).encode())
    new = [p for p in sorted(live) if p not in done]
    work = list(dict.fromkeys(forced + stale + new))[: a.max_partitions]
    log(f"partitions live={len(live)} done={len(done)} stale={len(stale)} "
        f"compaction-synced={len(synced)} new={len(new)} → 이번 런 {len(work)}개")

    # ── 3. compute: 파티션별 sketch → S3 ──
    t0 = time.time()
    for pval in work:
        ts = time.time()
        sk, nrows = compute_partition_sketches(spark, fq, pcol, pval, cols)
        for c in cols:
            store.write_bytes(f"{c}/{pval}.theta", sk[c])
        done[pval] = {"snap": live.get(pval, (-1, 0))[0], "rows": nrows}
        # 파티션 1개 끝날 때마다 상태 저장 — 여기서 잡이 죽어도 다음 런이 이 지점부터
        # 이어받는다(전량 재실행 없음). 표준 procedure 대비 이 잡의 존재 이유.
        store.write_bytes("_state.json",
                          json.dumps(state, ensure_ascii=False).encode())
        log(f"  {pval}: rows={nrows:,} {time.time()-ts:.1f}s")

    coverage = len([p for p in live if p in done]) / len(live)

    # ── 4. publish: 전 파티션 커버 시에만 union → Puffin 커밋 ──
    # 온보딩이 끝나기 전(coverage<100%)의 부분 union은 실제보다 작은 NDV라서
    # 게시하면 오히려 플랜을 왜곡한다 — 그래서 커버리지 게이트를 둔다.
    # "published" 마커: 계산은 다 됐는데 게시 단계에서 죽은 경우, 다음 런(work=0)이
    # 게시만 재시도할 수 있게 한다.
    published = None
    need_publish = coverage >= 1.0 and (work or "published" not in state)
    if need_publish and a.publish == "true":
        from datasketches import theta_union, compact_theta_sketch
        col_bytes, col_ndv = {}, {}
        for c in cols:
            u = theta_union()
            for pval in live:
                b = store.read_bytes(f"{c}/{pval}.theta")
                if b is None:
                    fail(f"sketch missing: {c}/{pval}")
                u.update(compact_theta_sketch.deserialize(b))
            res = u.get_result()
            col_bytes[c], col_ndv[c] = res.serialize(), res.get_estimate()
        try:
            published = publish_puffin(spark, a.catalog, a.table, cols, col_bytes, col_ndv)
        except Exception as e:
            fail(f"publish: {type(e).__name__}: {str(e)[:300]}",
                 coverage=round(coverage, 4), processed=work)
        state["published"] = published
        store.write_bytes("_state.json", json.dumps(state, ensure_ascii=False).encode())
        log(f"published: {published}")
    elif coverage < 1.0:
        log(f"onboarding 진행 중 — coverage {coverage:.1%}, publish 보류")

    emit_result({
        "ok": True, "table": a.table, "processed": work,
        "stale_recomputed": [p for p in work if p in stale or p in forced],
        "compaction_synced": synced,
        "coverage": round(coverage, 4),
        "elapsed_s": round(time.time() - t0, 1),
        "published": published,
        "ndv": {c: int(col_ndv[c]) for c in cols} if published else None,
    })


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        fail(f"unhandled: {type(exc).__name__}: {str(exc)[:300]}")
