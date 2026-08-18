# spark_iceberg_bigtable_stats

Spark와 Airflow로 Iceberg 테이블의 Puffin NDV 통계를 생성하는 두 가지 job이다.
테이블 유형에 따라 아래 경로 중 하나를 적용한다. 온보딩(최초 생성) 전용 DAG는
어느 경로에도 없다. **상태가 국면을 결정**하므로 같은 DAG가 온보딩과 증분
갱신을 모두 처리한다.

| | [`stats_refresh/`](stats_refresh/) — 표준 경로 | [`stats_bigtable_sketch/`](stats_bigtable_sketch/) — 초대형 경로 |
|---|---|---|
| 적용 대상 | 증분적재형 중소형 · **전체적재형 전부** | 증분적재형 **초대형** (파티션 수백 개, 전량 스캔이 비현실적) |
| 생성 수단 | Iceberg 표준 `compute_table_stats` (전량 스캔) | 파티션별 theta sketch 적립 → union (커스텀, 증분) |
| 온보딩 | 첫 실행 = 온보딩 (가드가 "최초면 무조건 실행") | 같은 DAG의 페이싱 국면 (`max_partitions`로 나눠 며칠간) |
| 갱신 | INCREMENTAL: compaction 후 + 가드 / FULL: 재적재마다 가드 없이 | 신규·변경 파티션만 증분 편입 |
| 검증 상태 | **E2E 검증 완료** (FULL·guard 분기·검증 1~3·Airflow) | **E2E 검증 완료** (온보딩·변화 시나리오·동시 변경·Airflow) |

## 공통 기술 전제

1. **관리 대상은 NDV(Puffin)뿐이다.** row count·min/max·null 수는 writer가 파일 단위로 자동 기록(manifest)하므로 job이 만들 것이 없다. `compute_partition_stats`(partition statistics)는 StarRocks가 읽지 않으므로(소스 참조 0건) 어느 경로도 만들지 않는다.
2. **NDV는 테이블 단위 값이다.** 표준 산출물에 파티션별 NDV는 없다 — 파티션 축이 필요한 초대형 경로는 그래서 sketch를 자체 상태(S3)로 유지하고, 게시물은 항상 테이블 단위 NDV다.
3. **소비 조건 (StarRocks 4.1)**: 세션 `enable_iceberg_column_statistics=true`(opt-in) + `enable_read_iceberg_puffin_ndv=true`(기본). 같은 컬럼에 SR **수집 통계**가 남아 있으면 그 컬럼은 수집 통계가 우선한다(컬럼 단위) — 정리는 `TRUNCATE TABLE _statistics_.external_column_statistics`만 유효(shared-data 실측; `DROP STATS`는 무효).
4. **통계는 플랜 선택에만 관여한다.** 낡아도 쿼리 결과는 불변 — 갱신 정책(가드)은 정합성 장치가 아니라 플랜 품질 장치다. CBO 결정은 NDV가 자릿수 단위로 어긋날 때만 바뀌므로, "매 적재마다 갱신"이 아니라 "의미 있게 변했을 때만 갱신"이 원칙이다.

## Airflow 2.9.1 파일 교환 프로토콜

두 DAG 모두 XCom을 사용하지 않는다. `resolve_targets`/`prepare_request`가
run별 `request.json`을 쓰고 Spark job이 `result.json`을 atomic write하며,
verify는 두 파일의 `request_id`가 같은지까지 검증한다.

```
${STATS_EXCHANGE_DIR}/<dag-id>/<sanitized-run-id>/
├── request.json
└── result.json
```

- `STATS_EXCHANGE_DIR`은 Airflow worker와 Spark driver에 **동일 경로**로 마운트된
  공유 파일시스템이어야 한다. 운영에서 로컬 `/tmp` 기본값을 그대로 쓰지 말고
  NAS/공유 volume 경로를 명시한다.
- 모든 Operator는 `do_xcom_push=False`다. stdout의 `RESULT|...`은 사람용 로그일
  뿐 태스크 간 전달 수단이 아니다.
- run 디렉터리는 감사·장애 분석을 위해 남긴다. 보존 기간 이후 정리는
  별도 TTL 정책으로 운영한다.

현재 검증 범위(2026-08-18): JSON 계약·nullable Param 병합·stale result 차단·DAG
import 경로 단위 테스트 5건 통과. **Airflow 2.9.1 + 실제 공유 volume + Spark
E2E는 운영 이식 전 smoke로 남아 있다.** 기존 Airflow 2.9.3 E2E 실측과는
입출력 계약이 달라졌으므로 이 항목을 대체하지 않는다.

## KPO(KubernetesPodOperator) 기반 etl-airflow 이식

두 경로 모두 KPO로 매핑 가능하며, 특히 초대형 경로는 이미 "팟 하나 = 런 하나" 구조다
(판단 로직이 잡 내부 + 상태가 S3 → Airflow에는 스케줄·재시도·알림만 남음).

1. **오퍼레이터**: SparkSubmitOperator → KPO(spark 클라이언트 이미지). driver는
   client 모드로 KPO 팟 안에, executor만 k8s 분산.
   `datasketches`·잡 파일은 이미지에 포함(체크리스트의 설치 항목이 이미지 빌드로 해결).
2. **결과 프로토콜**: KPO에도 같은 공유 volume을 마운트하고
   `--request-file`/`--result-file`을 전달한다. XCom sidecar는 필요 없다.
3. **stats_refresh의 resolve/guard**: 워커 PythonOperator(pyiceberg) 대신 판정 로직을
   spark 잡 시작부로 접는 변형이 KPO에 자연스럽다(sketch 잡이 이미 그 구조).
   skip이면 RESULT에 `skipped: true`로 즉시 종료 — 로직 동일, 태스크 수만 감소.
4. **팟 리소스 스펙 = 통계 전용 격리**: requests/limits로 큐 분리를 대체.

## 병렬화·성능 특성 (sketch 경로)

- 병렬 축: **파티션 내부**(mapInPandas — executor 수에 비례) × **컬럼 간**(1-pass 동시).
  파티션 **간**은 순차 — 개별 파티션이 클러스터를 채울 만큼 크면(초대형의 정상 케이스)
  총 시간은 `총 스캔량 ÷ 클러스터 처리량`이라 파티션-간 병렬을 더해도 안 줄어든다.
- sketch 갱신은 C++ 바인딩(코어당 수백만 updates/s) — 병목은 통상 S3 스캔 I/O.
- 개선 여지는 "작은 파티션 다수"일 때뿐: 파티션당 고정 오버헤드가 지배하면
  `WHERE dt IN (...)` 1-스캔 + 파티션 그룹핑 **배치 모드**를 추가한다
  (체크포인트 입도가 배치 단위가 되는 트레이드오프). 판단 기준:
  런 로그에서 "파티션당 소요 ≪ 잡 오버헤드"가 관측될 때.
