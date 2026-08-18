# PoC 가이드 — 초대형 테이블 파티션-순차 통계 생성 (stats_bigtable_sketch)

> **이 문서 하나로 배경 이해 → 환경 준비 → 실행 → 검증 → 보고까지** 가능하도록 작성했습니다.
> 소요 예상: 배경 읽기 15분 + 환경 준비 20분 + 실행·검증 반나절.
> 막히면 §8 트러블슈팅부터 보세요. 배경 이론은 private 원본 저장소의
> [00-overview.md](https://github.com/dongyi-kim/research-and-poc/blob/main/starrocks-iceberg-statistics/00-overview.md)를 참고하세요.

> ✅ **1차 E2E 검증 완료 (2026-07-31, 로컬 벤치 스택 + Airflow 2.9.3 `dags test`)** — 게이트 결과는 §6-b, 운영 변화 시나리오(append·재적재·컴팩션) 동작은 §5.5. 작업자 PoC는 이 결과의 **재현 + 실환경 이식 검증**이 목표입니다.

---

## 0. 한 줄 요약 — 무엇을 검증하나

**"수백 개 파티션짜리 초대형 Iceberg 테이블의 NDV 통계를, 전체 스캔 없이 파티션 단위로 나눠 만들고(sketch), 합쳐서(union), 표준 통계 파일(Puffin)로 게시하면 — StarRocks/Spark가 정말 그걸 읽고, 값도 정확한가?"**

성공하면: 2년치(dt ~730개) 테이블의 통계 온보딩을 "하룻밤 전체 스캔"이 아니라 "매일 30개 파티션 × 24일"로 안전하게 완료할 수 있고, 이후 갱신은 신규 파티션만 계산하면 됩니다.

---

## 1. 배경 (5분)

### 1.1 왜 통계가 필요한가

- SQL 엔진의 옵티마이저(CBO)는 **통계**(행 수, 컬럼별 고유값 수=NDV)로 실행 계획의 비용을 계산합니다.
- 통계가 없으면 조인 전략(broadcast/shuffle)·조인 순서를 잘못 골라 **메모리 과대추정 → 스케줄링 낭비·OOM**이 발생합니다.
- 로컬 벤치 실측: 무통계는 메모리 추정이 중앙값 2.4배 과대(최대 12.7배), 동시 부하에서 완주율 15% vs 통계 49%. 상세: private 원본 저장소의 [벤치 브리핑](https://github.com/dongyi-kim/research-and-poc/blob/main/starrocks-iceberg-statistics/bench/report/full/BRIEFING.md).

### 1.2 왜 "표준 방법"으로는 안 되나 (실측으로 확인된 한계)

Iceberg 표준 통계 생성은 Spark procedure `compute_table_stats` 하나입니다. 문제는:

| 한계 | 실측/확인 내용 |
|---|---|
| **파티션 지정 불가** | 인자가 table/snapshot_id/columns뿐 — 매 실행이 **대상 컬럼 전량 스캔** (Iceberg 1.10 확인) |
| StarRocks `ANALYZE`도 마찬가지 | 외부 Iceberg 테이블에 `ANALYZE ... PARTITION` 구문은 거부됨(구문 오류, 실측) — SAMPLE만 동작 |
| 전량 스캔의 비용 | 9,000만 행 테이블 top-5 컬럼 = 20초(실측). **비용은 행 수에 비례**하므로 수백억 행이면 시간 단위, 실패 시 처음부터 재실행 |

즉 "2년치 대형 테이블"은 온보딩(최초 1회 전체)도, 갱신(재계산)도 표준 수단으로는 비쌉니다.

### 1.3 아이디어 — theta sketch는 "합칠 수 있다"

NDV를 정확히 세려면 전체를 봐야 하지만, **theta sketch**라는 근사 자료구조(오차 ±1.6%대, 크기 수십 KB)는 특별한 성질이 있습니다:

> **부분별로 만든 sketch를 union하면, 전체를 한 번에 스캔한 것과 같은 NDV가 나온다.**

그래서 이런 전략이 성립합니다:

```
[온보딩]  과거 파티션을 매일 30개씩 sketch로 계산해 S3에 적립 (며칠에 나눠, 중단돼도 이어서)
[정상]    매일 신규 파티션 1~2개만 계산해 적립
[게시]    전 파티션 sketch를 union → 컬럼별 NDV → Iceberg 표준 통계 파일(Puffin)로 커밋
[소비]    StarRocks·Spark가 그 통계를 읽어 플랜 개선 (기존 벤치에서 효과 실증 완료)
```

⚠ 단, 이것은 **Iceberg 표준 procedure가 아니라 커스텀 잡**입니다(라이브러리는 전부 표준: DataSketches + Iceberg Puffin API). 그래서 채택 전에 이 PoC로 5가지를 검증합니다(§5).

### 1.4 용어 최소 세트

| 용어 | 뜻 |
|---|---|
| **NDV** | 컬럼의 고유값 개수 — 조인/집계 크기 추정의 핵심 입력 |
| **theta sketch** | NDV 근사 자료구조 (Apache DataSketches). **병합(union) 가능**이 핵심 성질 |
| **Puffin** | Iceberg가 통계를 담는 표준 파일 포맷. 파일 안에 blob(=컬럼별 sketch)들과, blob마다 `ndv` 요약값이 있음 |
| **StatisticsFile 커밋** | Puffin 파일을 테이블 metadata.json에 등록하는 것 — 등록돼야 엔진이 읽음 |
| **`.partitions` 메타테이블** | `SELECT ... FROM cat.db.tbl.partitions` — 파티션 목록·파티션별 마지막 갱신 스냅샷을 **스캔 없이** 알려줌 (이 잡의 계획 수립 근거) |

---

## 2. 코드 구조 (10분이면 다 읽힘)

| 파일 | 역할 | 핵심 함수 |
|---|---|---|
| [`sketch_stats_job.py`](sketch_stats_job.py) | spark-submit 본체 (~250줄) | `main()` 흐름: discover → plan → compute → publish. 파티션 계산은 `compute_partition_sketches`, Puffin 커밋은 `publish_puffin` |
| [`bigtable_sketch_dag.py`](bigtable_sketch_dag.py) | Airflow DAG (일 1런 스케줄·파라미터·알림) | PoC 단계에서는 **안 써도 됨** — spark-submit 직접 실행으로 충분 |

상태 레이아웃 (S3):
```
{state-prefix}/{table}/_state.json              ← 파티션별 (계산 시점 스냅샷, 행 수) — 재개·재적재 감지 근거
{state-prefix}/{table}/{column}/{partition}.theta ← 파티션·컬럼별 sketch 바이너리 (컬럼당 ~수 KB)
```

설계 성질 3개만 기억하면 코드가 읽힙니다:
1. **멱등** — 같은 상태에서 재실행하면 같은 파티션을 다시 계산할 뿐, 부작용 없음.
2. **체크포인트** — 파티션 1개 끝날 때마다 `_state.json` 갱신 → 어디서 죽어도 다음 런이 이어받음.
3. **게시 게이트** — 전 파티션이 계산되기 전(coverage<100%)에는 Puffin을 커밋하지 않음 (부분 NDV의 과소 게시 방지).

---

## 3. 환경 준비 (로컬 벤치 스택 재사용 — 20분)

PoC는 private `research-and-poc` 저장소에 이미 구축된 벤치 스택
(`starrocks-iceberg-statistics/bench/`)을 그대로 씁니다.
대상 테이블도 이미 있습니다: **`bench.fact_sensor` (9,000만 행, dt 일파티션 30개, 2026-06-01~30)** — "초대형 2년 테이블"의 축소 모형.

```bash
cd <research-and-poc>/starrocks-iceberg-statistics/bench

# 1) 스택 기동 (MinIO → Postgres → Polaris → SR FE → CN×3 → Spark 순)
docker start sr-stats-bench_minio_1 sr-stats-bench_postgres_1 && sleep 5
docker start sr-stats-bench_polaris_1 && sleep 10
docker start sr-stats-bench_starrocks-fe_1 && sleep 15
docker start sr-stats-bench_starrocks-cn{1,2,3}_1
docker start sr-stats-bench_spark_1

# 2) Spark 컨테이너에 DataSketches python 설치 (1회)
docker-compose exec -T spark pip install datasketches

# 3) docker-compose.yml의 request/result 공유 volume 반영
docker-compose up -d spark

# 4) 잡 파일을 컨테이너로 복사
docker cp ../jobs/stats_bigtable_sketch/sketch_stats_job.py \
  sr-stats-bench_spark_1:/opt/sketch_stats_job.py
```

> 메모리 주의: 이 스택은 Mac Docker(23GiB)를 꽉 채워 씁니다. sketch 계산 중에는 SR 쿼리를 병행하지 말고, SR 검증(§5-G2)할 때는 Spark 잡을 쉬게 하세요.

---

## 4. 실행 가이드

Spark 카탈로그 별칭은 `ice` (스택에 이미 설정됨). 기본 실행:

```bash
DC="docker-compose exec -T spark /opt/spark/bin/spark-submit --conf spark.driver.memory=4g"

# Airflow 없이 직접 실행할 때도 동일한 파일 프로토콜을 쓴다.
mkdir -p results/stats-exchange/manual
cat > results/stats-exchange/manual/request.json <<'JSON'
{
  "protocol_version": 1,
  "kind": "stats_bigtable_sketch",
  "request_id": "manual-onboarding",
  "catalog": "ice",
  "table": "bench.fact_sensor",
  "partition_col": "dt",
  "columns": ["device_id", "lot_id"],
  "state_prefix": "s3://warehouse/stats-state",
  "max_partitions": 10,
  "force_partitions": [],
  "publish": true
}
JSON

# ── (A) 온보딩 1런: 파티션 10개씩 나눠 처리 (30개를 3런으로 — 페이싱 검증 겸용)
$DC /opt/sketch_stats_job.py \
  --request-file /stats-exchange/manual/request.json \
  --result-file /stats-exchange/manual/result.json

# ── (B) 2~3런 반복 → 3번째 런에서 coverage 100% 도달 시 자동으로 union→Puffin 커밋
#        (커밋 없이 계산만 보고 싶으면 request.json의 publish=false)

# ── (C) 이후 재실행: "처리할 것 없음(0개)"이 정상 — 신규/재적재 파티션이 생겼을 때만 일함
```

로그 읽는 법:
```
[sketch] partitions live=30 done=10 stale=0 new=20 → 이번 런 10개   ← plan 결과
[sketch]   2026-06-11: rows=3,000,000 12.3s                         ← 파티션별 진행(체크포인트)
[sketch] published: {'snapshot_id': ..., 'statistics_file': 's3://...stats'}
RESULT|{"ok": true, "processed": [...], "coverage": 1.0, "ndv": {...}}  ← 기계 판독용 최종 요약
```

---

## 5. 검증 게이트 5항 — 이걸 통과해야 "채택 후보"

### G1. 정확도 — sketch-union NDV vs 표준 procedure NDV

```bash
# 표준 방식으로 같은 컬럼 통계 생성 (대조군 — 전량 스캔이라 느린 게 정상)
docker-compose exec -T spark /opt/spark/bin/spark-submit --conf spark.driver.memory=4g \
  /dev/stdin <<'EOF'
from pyspark.sql import SparkSession
s = SparkSession.builder.getOrCreate()
r = s.sql("CALL ice.system.compute_table_stats(table => 'bench.fact_sensor', "
          "columns => array('device_id','lot_id'))").collect()
print(r)
EOF
```
그다음 두 통계 파일의 `ndv`를 비교 — metadata.json의 `statistics` 배열에서 각 blob의 `properties.ndv`를 읽습니다 (§8-확인법 참조).
**판정: 오차 ≤ 5% (기대: ~1.6%대)**

### G2. 소비 — StarRocks·Spark가 실제로 읽는가

```sql
-- StarRocks (mysql -h127.0.0.1 -P9030 -uroot)
SET enable_iceberg_column_statistics = true;
SET enable_query_trigger_analyze = false;
EXPLAIN COSTS SELECT count(*) FROM ice.bench.fact_sensor f
  JOIN ice.bench.dim_device d ON f.device_id = d.device_id WHERE f.dt='2026-06-15';
```
**판정: 스캔 노드 컬럼 통계에 우리가 만든 NDV가 반영되고, 무통계 대비 플랜(조인 전략/카디널리티)이 달라짐.**
비교 기준이 필요하면 `SET enable_iceberg_column_statistics=false`로 같은 EXPLAIN을 떠서 diff.
Spark: `spark.sql.cbo.enabled=true`로 `EXPLAIN COST` 실행 → `distinct_count` 반영 확인 (Spark는 반영 폭이 좁아도 정상 — 주 소비자는 SR).

### G3. 재적재(backfill) 자동 감지

```bash
# 파티션 하나를 재적재 (Spark에서)
#   INSERT OVERWRITE ... WHERE dt='2026-06-05' 형태로 같은 데이터 재기록
# 그 후 잡 재실행 → 로그에 stale=1 이 잡히고 해당 파티션만 재계산되는지 확인
```
**판정: 해당 파티션만 재계산(전체 재스캔 아님) + 이후 publish NDV 정상.**

### G4. 중단·재개

온보딩 런 도중 Ctrl-C(또는 컨테이너 kill) → 재실행.
**판정: 이미 끝난 파티션은 건너뛰고 이어서 진행 (`done=` 숫자로 확인).**

### G5. 비용 선형성

각 런의 파티션별 소요(로그의 `Ns`)를 기록.
**판정: 파티션당 소요가 대체로 일정(행 수 비례) — 런 시간이 request의 `max_partitions`로 예측 가능.**

### 5.5 운영 중 변화 시나리오 — 데이터가 움직일 때 잡이 어떻게 반응하나 (검증 완료)

운영에서 반드시 만나는 4가지 변화를 실제로 일으킨 뒤 **한 런**으로 확인한 결과:

| 시나리오 | 만든 변화 | 잡의 반응 (실측) | 비용 |
|---|---|---|---|
| **신규 파티션** | `dt=2026-07-01` 신규 적재 (996k행) | `new`로 분류 → 그 파티션만 계산 → 전체 union 재게시 | 신규분 스캔만 |
| **기존 파티션 증가(append)** | `dt=2026-06-10`에 10% 중복 append (3.0M→3.3M) | `last_updated_snapshot_id` 변화 감지 → **stale** → 그 파티션만 재계산 | 해당 파티션 1회 스캔 |
| **재적재(overwrite/backfill)** | `dt=2026-06-05` INSERT OVERWRITE | 〃 stale → 재계산 | 〃 |
| **컴팩션** | `rewrite_data_files(dt=2026-06-03)` | **compaction-synced** — 스냅샷 operation=`replace`+행 수 동일 → 데이터 불변 판정, **재계산 없이 상태만 동기화** | **0** (메타 조회뿐) |

실측 RESULT (한 런, 10.7초):
```
processed=[06-05, 06-10, 07-01] · stale_recomputed=[06-05, 06-10] · compaction_synced=[06-03]
coverage=100% → union → Puffin 재게시 (새 스냅샷에 커밋)
```

동작 원리와 주의점:
- 변화 감지는 전부 `.partitions` 메타테이블(스캔 0)로 — 파티션별 `last_updated_snapshot_id`가 상태 기록과 다르면 변화.
- **컴팩션 최적화**: 마지막 변경 스냅샷의 `operation='replace'` **이고** 행 수(record_count)도 기록과 같으면 데이터 불변으로 보고 재계산을 생략한다. 행 수 비교를 함께 하는 이유: replace 직전에 append가 끼어든 경우(행 수가 달라짐)를 재계산으로 흘려보내기 위한 방어.
- append가 **중복 위주**면 재계산해도 NDV가 거의 안 변하는 게 정상(위 실측에서도 NDV 동일) — 낭비가 아니라 "확인 비용"이며 파티션 1개 스캔이라 싸다.
- 재게시는 항상 **현재 스냅샷**에 커밋되므로, 갱신 후 쿼리부터 새 통계가 적용된다.

### 5.6 런 **도중** 동시 변경 내성 — 실전 조건 테스트 (검증 완료)

운영에서는 잡이 도는 동안에도 적재·컴팩션이 멈추지 않는다. 31개 파티션 전체 재계산 런(67초)이 도는 **도중에** 세 가지를 동시에 실행했다:

| 런 도중 일어난 일 | 그 런의 동작 | **다음 런의 재조정 (실측 8.5초)** |
|---|---|---|
| 이미 계산 지나간 파티션(06-01)에 append | 그 런의 sketch는 append 이전 데이터 기준 | `stale` 감지 → 재계산 (3,149,729행으로 교정) ✅ |
| 아직 안 계산된 파티션(07-01)에 append | 계산 시점엔 새 데이터가 이미 포함됨 | 기록이 보수적이라 `stale`로 한 번 더 재계산 — **잉여 1회, 놓침은 아님** ✅ |
| 다른 파티션(06-07) 컴팩션 | — | `compaction-synced` — 재계산 0 ✅ |
| (공통) 동시 커밋 3건과의 충돌 | **런 완주 + Puffin 게시 성공** — 커밋 실패 없음 | — |

**핵심 성질 — 보수적 기록이라 "놓침"이 구조적으로 불가능하다**: 상태에는 discover(런 시작) 시점의 파티션 스냅샷을 기록하고, 계산은 그보다 같거나 새로운 데이터를 읽는다. 따라서 런 도중 변경은 반드시 "기록 ≠ 현재"로 남아 다음 런이 잡아낸다. 최악의 비용은 파티션 1개 잉여 재계산(수 초)이고, 오래된 통계를 최신으로 오인하는 방향의 오류는 없다.

**발견·수정 1건**: Spark 카탈로그의 테이블 캐시 때문에 게시가 세션 시작 시점 스냅샷에 붙을 수 있음 → `publish_puffin`에 `tbl.refresh()` 추가(코드 반영). SR은 조상 스냅샷의 통계도 찾아 읽으므로 구 스냅샷에 붙어도 동작은 했지만, 최신 스냅샷에 붙는 것이 정석.

---

## 6. 보고 양식 (이 표만 채워서 회신)

| 게이트 | 결과 (통과/실패) | 측정값 | 특이사항 |
|---|---|---|---|
| G1 정확도 | | sketch NDV / 표준 NDV / 오차 % | |
| G2 SR 소비 | | EXPLAIN diff 요약 | |
| G2 Spark 소비 | | distinct_count 반영 여부 | |
| G3 재적재 감지 | | stale 감지·재계산 파티션 | |
| G4 재개 | | 중단 지점 / 재개 확인 | |
| G5 선형성 | | 파티션당 평균 소요 s | |
| 총평 | | | 채택/보류/수정 필요 |

### 6-b. 1차 E2E 검증 결과 (2026-07-31 — 재현 목표치로 사용)

로컬 벤치 스택(`bench.fact_sensor` 90M×30dt) + Airflow 2.9.3 `dags test` (docker 서밋 모드):

| 게이트 | 결과 | 측정값 |
|---|---|---|
| G1 정확도 | **통과** | 실제 875,000/400,000 → sketch **+0.56% / −1.27%** (표준 procedure는 +1.33%/+0.25% — 동급) |
| G2 SR 소비 | **통과** | `device_id NDV=879912`가 EXPLAIN COSTS 컬럼 통계에 그대로 반영 (OFF 시 UNKNOWN/1.0) |
| G3 재적재 감지 | **통과** | overwrite·append 파티션만 stale 재계산, 신규 파티션 new 처리 — §5.5 |
| G4 재개 | **통과** | 2개 처리 후 종료 → 다음 런이 done=2에서 이어받아 28개 처리. 게시 단계 실패 후 재런의 게시 재시도(published 마커)도 확인 |
| G5 선형성 | **통과** | 3M행 파티션당 1.8~4.0초로 일정, 온보딩 30개 총 ~90초(로컬) |
| 컴팩션(추가) | **통과** | rewrite_data_files 후 재계산 0 (compaction-synced) — §5.5 |

검증 중 수정된 구현 이슈 3건 (작업자 환경에서도 유의):
1. 상태 저장은 Hadoop FS가 아니라 **Iceberg FileIO** 사용 (s3:// 스킴이 Hadoop FS에 없음)
2. Puffin blob metadata는 `GenericBlobMetadata.from(...)` 변환 후 커밋 (미변환 시 REST 직렬화 실패)
3. 게시 단계 실패 대비 상태에 `published` 마커 — 다음 런이 계산 없이 게시만 재시도

### 6-c. Airflow로 직접 돌리기 (로컬 — 검증에 사용한 방법)

```bash
python3 -m venv af-venv && af-venv/bin/pip install "apache-airflow==2.9.1" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt"

export AIRFLOW_HOME=$PWD/airflow-home
export AIRFLOW__CORE__DAGS_FOLDER=<repo>/stats_bigtable_sketch
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export STATS_SUBMIT_MODE=docker           # 로컬: compose exec로 제출 (호스트 spark 불필요)
export STATS_BENCH_DIR=<research-and-poc>/starrocks-iceberg-statistics/bench
export STATS_CATALOG=ice                  # 로컬 카탈로그 별칭 (실환경 기본 lake)
export STATS_RETRY_DELAY_MIN=1

af-venv/bin/airflow db migrate
af-venv/bin/airflow dags test stats_bigtable_sketch__bench__fact_sensor
```

`max_partitions`·`force_partitions`·`no_publish`은 Airflow Trigger UI의 optional
Param으로 입력한다. 비워 두면 기본값을 사용한다. nullable 스키마와
병합 규칙은 [README §2.2](README.md)에 있다.

실환경(etl-airflow)에서는 `STATS_SUBMIT_MODE` 미설정(기본 spark) → SparkSubmitOperator 경로.

---

## 7. 알려진 미검증 지점 (PoC에서 부딪히면 정상 — 기록해 주세요)

1. **`updateStatistics().setStatistics(...)` 시그니처** — Iceberg 버전에 따라 1-인자/2-인자. 코드에 폴백이 있으나 둘 다 실패하면 버전 기록.
2. **Puffin `Blob` 생성자 시그니처** — 1.10 기준으로 작성. py4j 오류 나면 오류 메시지 전체 캡쳐.
3. **DataSketches python↔java 직렬화 호환** — 설계상 호환(공식 보장)이나 G1이 최종 확인.
4. **Polaris 경유 통계 커밋 충돌** — compaction과 동시 커밋 시 재시도 동작.
5. 파티션 컬럼이 **변환형**(예: `month(ts)`)인 테이블 — 이 초안은 identity(dt/mt 컬럼) 가정. `.partitions` 조회식 수정 필요.

## 8. 트러블슈팅

- **`ModuleNotFoundError: datasketches`** → §3-2 설치를 driver뿐 아니라 executor에도 (로컬 스택은 단일 컨테이너라 한 번이면 됨).
- **metadata.json에서 ndv 확인하는 법** →
  ```bash
  docker-compose exec -T spark /opt/spark/bin/spark-submit /dev/stdin <<'EOF'
  from pyspark.sql import SparkSession
  s = SparkSession.builder.getOrCreate()
  p = s.sql("SELECT file FROM ice.bench.fact_sensor.metadata_log_entries ORDER BY timestamp DESC LIMIT 1").collect()[0][0]
  print(p)  # 이 경로를 mc cat 등으로 열어 "statistics" 배열의 blob properties.ndv 확인
  EOF
  ```
- **`.partitions` 조회가 비어 있음** → 카탈로그 별칭·테이블명 확인 (`ice.bench.fact_sensor.partitions`).
- **커밋은 됐는데 SR이 안 읽음** → SR 세션에서 `enable_iceberg_column_statistics=true`인지, 그리고 그 컬럼에 SR **수집 통계**가 남아 있지 않은지(`_statistics_.external_column_statistics` — 남아 있으면 그 컬럼은 수집 통계가 우선). 정리는 `TRUNCATE`만 유효.
- **메모리 부족** → request/Param의 `max_partitions`를 줄이거나 SR CN을 잠시 stop.

## 9. 참고

- 설계 배경·결정 근거: [README.md](README.md) · private 원본 저장소의 [4장 §1.7.5](https://github.com/dongyi-kim/research-and-poc/blob/main/starrocks-iceberg-statistics/04-options.md)
- 통계 효과의 실측 근거(왜 이걸 하는가): private 원본 저장소의 [벤치 브리핑](https://github.com/dongyi-kim/research-and-poc/blob/main/starrocks-iceberg-statistics/bench/report/full/BRIEFING.md)
- 표준 경로(일반 테이블용) 잡: [../stats_refresh/](../stats_refresh/)
