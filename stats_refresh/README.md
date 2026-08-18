# stats_refresh — 표준 경로 (Iceberg `compute_table_stats`)

증분적재형 **중소형** 테이블과 **전체적재형 전부**에 적용하는 통계 생성 job.
초대형 증분형(파티션 수백 개)은 [../stats_bigtable_sketch/](../stats_bigtable_sketch/)를 쓴다. 경로 선택 기준·공통 전제는 [../README.md](../README.md).

상태: **E2E 검증 완료** (로컬 벤치 스택 + Airflow 2.9.3 `dags test`, 2026-08-01):
- FULL 경로(dim_device 2M): resolve(pyiceberg+Polaris REST 인증) → guard → compute 5.3s → 검증 1~3 → verify 메트릭, 런 success
- INCREMENTAL 경로(fact_sensor): guard가 "마지막 통계 14.5h < 24h → skip"을 정확히 판정, downstream skip 후 런 success
- E2E에서 발견·수정된 버그 1건: 검증 3의 ndv 상한이 근사 오차를 불허해 정상 결과(전-유니크 컬럼 NDV가 행 수를 +0.47% 초과)를 실패 처리 → 상한을 `total × 1.05`로 완화 (스케치 ±1.6% 오차 근거)
- RESULT 실패 프로토콜의 동작(ok:false → verify 실패)도 위 버그 케이스로 자연 검증됨

## 1. 적용 방법

### 1.1 트리거 배선

```python
# 증분적재형: compaction DAG 말미 (+ @daily 스케줄은 안전망 — 가드가 중복을 skip)
# 전체적재형: 적재 DAG 말미 (스케줄 없음 — 재적재마다 실행)
TriggerDagRunOperator(
    task_id="kick_stats_refresh",
    trigger_dag_id="stats_refresh__db__my_table",
    wait_for_completion=False,   # non-blocking: 통계 실패가 적재를 실패시키지 않음
)
```

전체적재형에서 트리거를 적재 **직후**에 두는 이유: overwrite 커밋부터 재계산 완료까지는 옛 통계가 조상 스냅샷 경유로 읽히는 구간이므로, 체인 직결로 그 구간을 최소화한다.

### 1.2 Job 선언 (config)

| 키 | 값 | 기술적 의미 |
|---|---|---|
| `load_type` | `INCREMENTAL` / `FULL` | 가드 분기. FULL = 매 재적재가 전면 교체 → 이전 통계 무의미 → **가드 없이 항상 실행** |
| `columns` | 조인/집계 키 소수 | `compute_table_stats`의 유일한 비용 레버 (비용 ∝ 행 수 × 컬럼 수). 조인 키는 **양쪽 테이블 쌍**으로 등록 |
| `guards` | `{changed_ratio, min_interval_h, max_age_d}` | INCREMENTAL 전용, §2.2 |
| `resources.queue` | 통계 전용 큐 | 적재 리소스와 격리 |

### 1.3 온보딩

별도 절차 없음 — **첫 실행이 온보딩**이다. 가드의 첫 분기가 "통계가 아예 없으면 무조건 실행"이므로, DAG를 켜면(또는 첫 트리거가 오면) 최초 생성이 일어난다. 대형 테이블의 최초 실행만 비용을 산출해 off-peak에 수동 트리거하면 된다.

## 2. 동작과 기술 배경

```
resolve_targets(request.json) ─▶ guard(file) ─▶ compute_stats(result.json) ─▶ verify(file)
```

### 2.1 resolve_targets — 스냅샷 고정 (멱등성의 축)

`compute_table_stats`는 스냅샷 기준 결정적이지만, 실행 중 새 커밋이 끼어들 수 있다. 그래서 시작 시점에 `snapshot_id`를 읽어 **고정 인자**로 넘긴다 → 재시도가 항상 같은 스냅샷·같은 결과. 부수로 가드 판정에 필요한 사실(마지막 통계 시각, 이후 누적 added-records)을 metadata·스냅샷 summary에서 수집한다 — **테이블 스캔 0**.

### 2.2 guard — 실행량 통제 (INCREMENTAL만)

근거: CBO 결정(broadcast/조인 순서)은 NDV가 **자릿수 단위**로 어긋날 때만 바뀐다. 따라서:

| 규칙 | 값(초깃값) | 이유 |
|---|---|---|
| 최초 온보딩 | 무조건 실행 | 온보딩 DAG를 따로 두지 않는 구현부 |
| 최소 간격 | 24h | 고빈도 compaction 테이블의 과잉 실행 방지 |
| 변화량 | 추가 행 > 당시 규모의 50% | "몇 배 변화만 플랜을 바꾼다"의 보수적 적용. 판정 입력은 스냅샷 summary(`added-records`) 합산뿐 — 비용 ≈ 0 |
| 강제 갱신 | 30일 무갱신 | 조용히 도메인이 자라는 키(신규 장비 ID류) 보호 |

skip이 잦은 것이 정상이다 — 도메인 고정 키(장비 목록 등)는 append가 이어져도 NDV가 안 변하므로 사실상 온보딩 1회로 끝난다.

### 2.3 compute_stats — 표준 procedure

`CALL <catalog>.system.compute_table_stats(table, snapshot_id, columns)` — 산출물은 Puffin 파일(컬럼별 theta sketch blob + footer `ndv`)이고 metadata.json `statistics` 배열에 등록된다. 유의할 성질:
- **파티션 인자가 없다** → 매 실행 전량 스캔. 이것이 초대형 테이블에서 이 경로를 못 쓰는 이유이자, `columns` 한정이 유일한 비용 레버인 이유.
- 실측 단가(로컬): 9,000만 행 × 5컬럼 = 20.4초. 파일 크기 컬럼당 ~20KiB(행 수 무관).

### 2.4 verify — "소비 가능 상태"가 성공의 정의

procedure 정상 종료만으로는 부족하다. spark job이 3단계를 내장 검증한다:
1. statistics_file **실재** (테이블 FileIO로 확인 — Hadoop FS 불필요)
2. metadata.json **등록** + blob fields(=스키마 field-id)를 이름으로 역해석해 요청 컬럼과 일치 확인
3. **ndv sanity**: `0 < ndv ≤ total-records × 1.05` — sketch ±1.6% 근사라 전-유니크 컬럼에서 행 수를 소폭 넘는 것은 정상(E2E 실측 +0.47%). 그 이상 초과만 생성 이상 신호

결과는 run별 `result.json`에 atomic write되고 DAG verify 태스크가 읽는다.
`request_id`를 대조하므로 이전 재시도/런의 결과를 잘못 읽지 않는다.
stdout `RESULT|<json>`은 로그 가독성을 위해만 유지한다.

### 2.5 실패 격리·동시성

- 독립 DAG + 상류는 non-blocking 트리거만 → 통계 실패는 알림으로 끝나고 파이프라인은 무사.
- `max_active_runs=1` — 같은 테이블에 통계 커밋이 겹치면 Iceberg 낙관적 커밋 충돌로 재시도가 나므로, DAG 단위 직렬화가 가장 싼 예방책.

## 3. 로컬 테스트 (환경 변수 오버라이드)

```bash
export STATS_SUBMIT_MODE=docker      # compose exec로 제출 (호스트 spark 불필요)
export STATS_BENCH_DIR=<research-and-poc>/starrocks-iceberg-statistics/bench
export STATS_CATALOG=ice             # 로컬 벤치 스택 카탈로그 별칭
airflow dags test stats_refresh__bench__fact_sensor
```

Docker 모드는 호스트 `<bench>/results/stats-exchange`를 Spark 컨테이너
`/stats-exchange`에 마운트한다. 운영은 `STATS_EXCHANGE_DIR`을 Airflow worker와
Spark driver에 공통 마운트된 NAS 경로로 지정한다.

## 4. 이식 체크리스트

1. `REST_URI`·인증 → Airflow Connection (pyiceberg가 워커에 없으면 resolve/guard를 Spark thin job으로 이관 — 로직 동일)
2. SparkSubmitOperator `conn_id`·큐 → ETL Spark + 통계 전용 큐
3. `JOBS` 리스트 → 중앙 config 렌더링 (컬럼 목록은 코드에 두지 않는다)
4. 알림 콜백 → 표준 알림 체계
5. 소비 전제 확인 — 세션 `enable_iceberg_column_statistics=true`, 대상 컬럼에 SR 수집 통계 잔존 여부(있으면 그 컬럼은 수집 통계 우선)
6. `STATS_EXCHANGE_DIR` 공유 volume의 worker/driver 동일 경로·쓰기 권한·TTL 확인
