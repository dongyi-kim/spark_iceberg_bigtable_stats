# stats_bigtable_sketch — 초대형 경로 (파티션-순차 theta sketch)

증분적재형 **초대형** 테이블(dt/mt 파티션 수백 개, 전량 스캔이 비현실적) 전용.
중소형·전체적재형은 [../stats_refresh/](../stats_refresh/)를 쓴다. 경로 선택 기준·공통 전제는 [../README.md](../README.md). PoC 수행자용 실행 가이드는 [PoC-GUIDE.md](PoC-GUIDE.md).

상태: **E2E 검증 완료** (로컬 벤치 스택 90M×31파티션 — 온보딩 완주·NDV 오차 ≤1.3%·SR 소비 확인·변화 시나리오 4종·런 도중 동시 변경 내성·Airflow `dags test`).

⚠ 비표준 커스텀 잡 — 라이브러리는 전부 표준(Apache DataSketches, Iceberg Puffin API)이지만 procedure가 아니다. 표준 경로로 감당되는 테이블에는 쓰지 않는다.

## 1. 기술 배경 — 왜 이 구조인가

### 1.1 표준 수단의 한계 (실측 확인)

`compute_table_stats`에는 파티션 인자가 없어 매 실행이 대상 컬럼 **전량 스캔**이다. StarRocks `ANALYZE ... PARTITION`도 외부 Iceberg 테이블에서 구문 오류(SAMPLE만 동작). 즉 "특정 파티션만 통계 갱신"은 표준에 없다 — 보관 2년(dt ~730개) 테이블은 온보딩도 갱신도 전량 스캔이 된다.

### 1.2 theta sketch의 병합 가능성이 해법

theta sketch는 각 값의 64bit 해시 중 가장 작은 K개(lg_k=12 → 최대 4,096개)만 유지하는 NDV 근사 자료구조(오차 ±1.6%대)로, **부분별 sketch를 union하면 전체를 한 번에 스캔한 것과 같은 추정**이 나온다. 따라서:

```
파티션별 sketch를 S3에 적립 (파티션당 1회 스캔, 며칠에 나눠 가능)
→ 전 파티션 union → 컬럼별 NDV → 표준 Puffin으로 게시 (테이블 단위)
→ 이후엔 신규·변경 파티션만 계산해 다시 union
```

비용이 "변경량"에 비례하고, 실패 시 마지막 성공 파티션부터 재개된다(표준 procedure에 없는 성질 — 이 경로의 존재 이유).

### 1.3 변경 감지 — 파티션별 버전 번호는 Iceberg가 공짜로 준다

`.partitions` 메타테이블의 `last_updated_snapshot_id`(+`record_count`)를 감지 키로 쓴다 — 스캔 0, 파티션 수 비례 메타 조회. 상태에는 **계산 시점의 그 값**을 기록하고 등호 비교로 감지한다:

| 감지 | 조건 | 처리 |
|---|---|---|
| 신규 파티션 | 상태에 없음 | `new` — 계산 후 편입 |
| append·overwrite | 기록 스냅샷 ≠ 현재 | `stale` — 그 파티션만 재계산 |
| **컴팩션** | 현재 스냅샷 operation=`replace` **이고** record_count 동일 | `synced` — 데이터 불변, **재계산 없이 상태만 동기화**. record_count 병행 비교는 replace 직전에 append가 낀 경우를 재계산으로 흘리는 방어 |

**보수적 기록의 성질**: 상태에는 런 시작(discover) 시점 스냅샷을 기록하고 계산은 그 이후 데이터를 읽으므로, 런 도중 변경은 반드시 "기록≠현재"로 남아 다음 런이 잡는다 — **놓침은 구조적으로 불가능**, 최악 비용은 파티션 1개 잉여 재계산 (동시 변경 테스트로 실증).

### 1.4 게시(publish)의 규칙

- **커버리지 게이트**: 전 파티션 sketch가 모이기 전의 부분 union은 실제보다 작은 NDV → 게시하면 플랜을 왜곡하므로 커버리지 100% 전에는 게시하지 않는다.
- **`published` 마커**: 계산은 끝났는데 게시에서 죽은 경우, 다음 런(작업 0개)이 게시만 재시도한다.
- 산출물은 **표준 Puffin** — blob type `apache-datasketches-theta-v1`에 진짜 theta 본문 + footer `ndv`. StarRocks·Spark는 footer만 읽고(소비 확인 실측), 본문이 표준이라 타 엔진 호환.
- 커밋 전 `tbl.refresh()` 필수 — Spark 카탈로그 테이블 캐시 때문에 세션 시작 시점(구) 스냅샷에 통계가 붙는 문제 실측·수정.

### 1.5 구현 세부 (부딪히기 쉬운 세 곳)

1. **상태·sketch 저장은 Iceberg FileIO** (`tbl.io()`) — Hadoop FS에 s3:// 스킴이 없는 스택이 흔하다. 바이트 수거는 commons-io로 한 번에(py4j 바이트 루프 금지).
2. **Puffin blob metadata는 `GenericBlobMetadata.from(...)` 변환 후 커밋** — puffin 패키지 타입 그대로 넣으면 REST 직렬화 실패.
3. `updateStatistics().setStatistics(...)` 시그니처는 Iceberg 버전별 1/2-인자 — 코드에 폴백 있음.

## 2. 적용 방법

### 2.1 상태 레이아웃 (S3, 테이블 FileIO 재사용)

```
{state-prefix}/{table}/_state.json                  파티션별 {계산 시점 snapshot_id, record_count} + published 마커
{state-prefix}/{table}/{column}/{partition}.theta   파티션·컬럼별 compact sketch (~수 KB)
```

파티션 1개 처리마다 `_state.json`을 갱신(체크포인트) — sketch를 먼저 쓰고 상태를 나중에 쓰므로, 어디서 죽어도 최악이 "그 파티션 재계산"이다. 상태와 sketch를 같은 저장소에 두는 이유가 이 순서 보장이다.

### 2.2 DAG 운용 — 온보딩도 같은 DAG

일 1런 스케줄 하나면 된다. 온보딩은 별도 DAG가 아니라 **상태가 빈 시기**일 뿐:

| 국면 | 런의 동작 | 운용 |
|---|---|---|
| 온보딩 | 미처리 파티션을 오래된 순으로 `max_partitions`개씩 (dt 730개 ÷ 30/런 ≈ 24일) | 개시만 수동 트리거(승인) + 야간엔 Param으로 상향 가능 |
| 정상 | 신규 dt 1~2개 + 변경 감지분만 (실측: 재조정 런 8.5초) | 방치 — 할 일 없으면 0개 처리로 종료 |

Airflow UI의 세 제어값은 모두 optional이다. 명시적 `type`만 주면
Airflow 2.9.1이 required로 해석하므로 `null`을 함께 허용한다.

```python
params={
    "force_partitions": Param(None, type=["null", "string"]),
    "max_partitions": Param(None, type=["null", "integer"], minimum=1),
    "no_publish": Param(None, type=["null", "boolean"]),
}
```

- `max_partitions=None` → job 기본값 30
- `force_partitions=None` → 강제 재계산 없음
- `no_publish=None` → 게시함(`publish=true`)

`render_template_as_native_obj=True`로 `None`/integer/boolean을 문자열로 변환하지
않는다. `prepare_request`가 입력값과 기본값을 병합해 run별 `request.json`에
고정한다. Spark job은 optional 해석을 하지 않고 완성된 요청만 받는다.
`max_active_runs=1`로 상태 동시 접근을 차단한다.

### 2.3 실행 (로컬 검증 환경 기준)

```bash
mkdir -p results/stats-exchange/manual
cat > results/stats-exchange/manual/request.json <<'JSON'
{
  "protocol_version": 1,
  "kind": "stats_bigtable_sketch",
  "request_id": "manual-001",
  "catalog": "ice",
  "table": "bench.fact_sensor",
  "partition_col": "dt",
  "columns": ["device_id", "lot_id"],
  "state_prefix": "s3://warehouse/stats-state",
  "max_partitions": 30,
  "force_partitions": [],
  "publish": true
}
JSON

docker-compose exec -T spark /opt/spark/bin/spark-submit --conf spark.driver.memory=4g \
  /opt/sketch_stats_job.py \
  --request-file /stats-exchange/manual/request.json \
  --result-file /stats-exchange/manual/result.json
```

Airflow 경유·env 오버라이드(`STATS_SUBMIT_MODE=docker` 등)는 [PoC-GUIDE §6-c](PoC-GUIDE.md).

## 3. 검증된 사실 (재현 목표치)

| 항목 | 실측 |
|---|---|
| 정확도 | 실제 875,000/400,000 대비 sketch +0.56%/−1.27% (표준 procedure +1.33%/+0.25% — 동급) |
| SR 소비 | EXPLAIN COSTS 컬럼 통계에 게시 NDV 그대로 반영 (OFF 시 UNKNOWN) |
| 비용 | 3M행 파티션당 1.8~4.0초 (스캔 1회, 컬럼 K개 동시), Puffin 66KB |
| 변화 시나리오 | 신규=new / append·overwrite=stale 재계산 / 컴팩션=synced 재계산 0 — 한 런에서 동시 처리 |
| 동시 변경 내성 | 31파티션 런(67s) 도중 append×2+컴팩션×1 → 완주·게시 성공, 다음 런 8.5s 재조정 |
| 재개 | 중단 후 다음 런이 체크포인트에서 계속, 게시 실패 후 게시만 재시도 |

## 4. 이식 체크리스트

1. driver·executor 이미지에 `pip install datasketches` (python-java 직렬화 호환은 공식 보장, G1로 재확인)
2. 사내 Iceberg 버전의 `setStatistics` 시그니처·`Blob` 생성자 (1.10 기준 작성)
3. 파티션 컬럼 identity(dt/mt) 가정 — 변환 파티션(`month(ts)` 등)이면 `.partitions` 조회식 수정
4. 통계 커밋 vs compaction의 낙관적 충돌: `max_active_runs=1` + 재시도(멱등)로 흡수
5. 소비 전제·수집 통계 우선순위는 [../README.md](../README.md) 공통 전제 참조
6. 대상 컬럼 추가 시 그 컬럼만 전 파티션 재온보딩 필요 (파티션 수 고정 페이싱으로 분할 실행)
