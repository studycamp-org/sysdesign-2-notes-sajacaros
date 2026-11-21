# mini-adclick — 6장의 문제를 직접 일으켜 보는 광고 클릭 집계 실험실

데이터가 많다는 흉내는 내지 않는다. **사람이 휴대폰(브라우저)으로 광고를 누르고, 네트워크와 서버에 문제를 일으키면**
6장 3단계의 장치들이 어떻게 대응하는지 클릭 하나 단위로 본다. 집계는 교재와 같이 **Apache Flink** 가 한다.

구성도: [`../architecture.html`](../architecture.html)

```
 휴대폰(탭) ──HTTP──▶ 광고 서버(api) ──clicks.log──▶ Fluent Bit ──▶ Kafka raw-clicks ──▶ Apache Flink
  ✈ 비행기 모드         · 정답 장부                                                      │ SQL: 10초 TUMBLE · 워터마크
  📶 불안정 네트워크     · 1초 tick                                                       │ COUNT(DISTINCT click_id)
                       · Flink 잡 관리 (SQL Gateway·REST, 💥 = TaskManager 강제 종료)     ▼ 체크포인트 · Kafka 싱크(NONE/ALO/EOS)
                     ClickHouse ◀── Kafka 엔진 + MV ◀── Kafka agg-results ◀──────────────┘
                         ▲  (윈도 결과를 더한다)
                         │ 보정
 RustFS (Parquet) ◀── archiver ◀── raw-clicks          recon (DuckDB 재집계 검증) ──┘
```

## 실행

```bash
./run.sh                  # 빌드하고 띄운다 (Flink 잡 배포까지 30초쯤)
docker compose down -v    # 끝. 모든 데이터가 사라진다
```

| 화면 | 주소 |
|---|---|
| 실험실 (휴대폰·운영자) | http://localhost:8006 — 탭을 여러 개 열면 휴대폰이 여러 대 |
| Flink UI | http://localhost:8081 — 잡 그래프, 체크포인트, 워터마크, 재시작 기록 |
| ClickHouse | http://localhost:8123/play (`default` / `demo`) |
| RustFS 콘솔 | http://localhost:9001 (`rustfsadmin` / `rustfsadmin`) |

## 축소한 것

| 교재 | 여기 | 이유 |
|---|---|---|
| 1분 텀블링 윈도 | **10초** | 결과를 1분씩 기다리면 실험이 안 된다 |
| 워터마크 15초 | **3초** (0~10초 조절) | 같은 이유 |
| 하루 10억 건 | 사람이 누른 만큼 | 문제를 보이는 게 목적 |
| 일일 재집계 검증 | 버튼 | |

## 교재 ↔ Flink

| 교재 | Flink 에서 |
|---|---|
| Aggregation Service (Map·Aggregate·Reduce) | Flink SQL 잡 하나 (`app/flinkctl.py` 의 `agg_query`) |
| 이벤트 시각 · 텀블링 윈도 | `TUMBLE(TABLE clicks_src, DESCRIPTOR(ts), INTERVAL '10' SECOND)` |
| 워터마크 | `WATERMARK FOR ts AS ts - INTERVAL 'N' SECOND`. 광고 서버의 1초 tick 이 클릭이 드물어도 워터마크를 민다 |
| 중복 제거 | `COUNT(DISTINCT click_id)` |
| 처리 시각 (비교용) | `TUMBLE(…, DESCRIPTOR(pt), …)`, `pt AS PROCTIME()` |
| 스냅숏 (그림 26·27) | **체크포인트**: 윈도 상태 + Kafka 오프셋 |
| 해법 1 · 2 · 3 (그림 18~20) | Kafka 싱크 `sink.delivery-guarantee` = `none` · `at-least-once` · `exactly-once`(체크포인트에 묶인 2단계 커밋) |
| 집계 노드 장애 | TaskManager 컨테이너 강제 종료 → JobManager 가 알아채고(하트비트 5초) 마지막 체크포인트에서 재시작 |
| 코드 배포 | 세이브포인트를 찍고 새 SQL 로 다시 제출 |
| 데이터 재계산 (그림 11, 카파) | 같은 SQL 의 **유한(bounded) 잡**: `raw-clicks` 처음 → 지금 끝까지 읽고 끝난다 |

전달 보장 세 가지를 장애로 재현하려고 체크포인트 주기와 프로듀서 버퍼를 일부러 다르게 뒀다.

| 전략 | 싱크 보장 | 체크포인트 | 프로듀서 `linger.ms` | 💥 시점 | 결과 |
|---|---|---|---|---|---|
| ① 최대 한 번 | NONE | 1초 | 8초 | 결과가 버퍼에 있고 체크포인트는 이미 지난 뒤 | **유실** |
| ② 최소 한 번 | AT_LEAST_ONCE | 30초 | 0 | 결과는 나갔고 다음 체크포인트는 아직 | **중복** |
| ③ 정확히 한 번 | EXACTLY_ONCE | 2초 | 0 | 언제든 | **정확** (결과가 체크포인트 커밋 때 보인다 → 최대 2초 늦음) |

## 실험 (화면 위 '실험 순서'와 같다)

| # | 교재 | 하는 것 | 보이는 것 |
|---|---|---|---|
| 1 | — | 광고 몇 번 누르기 | 점이 윈도 안에 찍히고, 결과가 확정되면 장부 = 집계 ✓ |
| 2 | 이벤트 시각 vs 처리 시각 (그림 12) | ✈ 켜고 누른 뒤 8초쯤 있다가 끄기 | 점은 누른 시각에, 점선이 도착 시각으로. 처리 시각 열은 다른 윈도에 센다 (≠) |
| 3 | 워터마크 (그림 13·14) | 워터마크 0초 + 2~3초 ✈ → 5초로 올리고 다시 | 0초면 ✗ 버림(−1, Flink 메트릭 '늦어서 버림' +1), 5초면 잡힘. 대신 결과가 늦게 나온다 |
| 4 | 아주 늦은 이벤트 + 재집계 검증 (그림 28) | ✈ 30초 넘게 → 🔍 재집계 검증 | ✗ 버림 → 장부보다 모자람 → DuckDB 가 원본에서 찾아 '보정 +1' |
| 5 | 클라이언트 중복 | 📶 불안정한 네트워크 켜고 누르기 | 응답 유실로 재전송(↻2, ↻3) → 같은 click_id 는 한 번만 |
| 6 | 노드 장애: 유실·중복 (그림 17~20) | 전략 ①②③ → 💥 장전 → 광고 누르기 | ① **−N 유실** / ② **+N 중복** / ③ **✓** |
| 7 | 스냅숏 = 체크포인트 (그림 26·27) | ②에서 체크포인트 켬/끔 → 💥 | 켜면 '체크포인트 #n 에서 복구, 그 뒤 로그 몇 줄만 다시', 끄면 '배포 때 세이브포인트로 되돌아가 전부 다시 읽고 이미 낸 결과를 또 냄' |
| 8 | 버그 배포 → 재계산 (그림 11) | 🐞 배포 → 누르기 → ✅ 수정 → 🔁 재계산 | 버그 동안 +N(2배) → 유한 잡이 닫힌 윈도를 원본부터 다시 집계해 갈아 끼움 |

### 확인한 결과 (2026-11-21, 스크립트로 휴대폰 흉내)

| 시나리오 | 장부 | 이벤트 시각 집계 | 처리 시각 집계 |
|---|---|---|---|
| 정상 3건 + 같은 click_id 재전송 1건 | 4 | 4 ✓ | 4 |
| 윈도 끝 2초 뒤 도착, 워터마크 3초 | 2 | 2 ✓ | 1 (다른 윈도에 셈) |
| 같은 상황, 워터마크 0초 (세이브포인트로 재배포) | 2 | 1 (−1, Flink 늦어서 버림 1) | 1 |
| 40초 늦게 도착 | 5 | 4 (−1) | 4 |
| ① NONE + 💥 | 3 | 0 (−3 유실) | 0 |
| ② AT_LEAST_ONCE + 💥 | 3 | 6 (+3 중복) | 3 |
| ③ EXACTLY_ONCE + 💥 | 3 | 3 ✓ | 3 |
| ② + 체크포인트 끔 + 💥 | 앞 윈도 1 | 2 (이미 낸 결과를 또 냄) | |
| 🐞 버그 버전 | 3 | 6 (+3) → 🔁 재계산 후 3 ✓ | 3 |

재계산 뒤 남은 불일치 0, 재집계 검증 불일치 0.

### 알아 둘 점: 재계산 결과는 실시간과 다를 수 있다

재계산은 ①②로 망가진 윈도와 버그 윈도를 원본에서 바로잡는다. 그런데 **실시간에서 늦어서 버려진 클릭까지 센다**
(위 40초 지각·워터마크 0초 윈도가 재계산 후 장부와 같아졌다). 과거 로그를 몇 초 만에 몰아 읽으면 Flink 의 워터마크가
주기적으로(200ms)만 올라가서, 늦은 클릭이 도착할 때 그 윈도가 아직 열려 있기 때문이다.
워터마크는 이벤트 시각만이 아니라 **처리 속도**에도 달려 있다는 것, 그래서 재처리 결과가 실시간 결과와 꼭 같지는 않다는 것을
보여 주는 예다. 마지막 기준은 원본을 다시 세는 재집계 검증이다.

## 구성 요소

| 구성 요소 | 파일 | 하는 일 |
|---|---|---|
| api (광고 서버) | `app/api.py`, `app/static/index.html` | 휴대폰 클릭 수신 → `clicks.log`, 정답 장부, 1초 tick, Flink 잡 관리(배포·세이브포인트 재배포·💥·재계산), 화면 |
| Flink 잡 정의 | `app/flinkctl.py` | 집계 SQL, SQL Gateway 제출, REST(세이브포인트·체크포인트·워터마크·늦은 레코드 메트릭) |
| jobmanager / taskmanager / sql-gateway | `flink/Dockerfile` | Flink 2.2.1 + Kafka SQL 커넥터 5.0.0-2.2. 체크포인트·세이브포인트는 공유 볼륨 `flinkdata` |
| fluent-bit | `fluent-bit/fluent-bit.yaml` | Log Watcher: tail → Kafka `raw-clicks` |
| kafka / kafka-init | (compose) | Apache Kafka 4.1 KRaft. `raw-clicks`(1 파티션), `agg-results` |
| clickhouse | `clickhouse/init.sql` | Kafka 엔진(read_committed) → MV → MergeTree. 질의는 `sum(clicks)` |
| archiver | `app/archiver.py` | `raw-clicks` → Parquet → RustFS (3초마다, 최소 한 번) |
| recon | `app/recon.py` | DuckDB 로 Parquet 원본을 click_id 기준으로 다시 세어 ClickHouse 와 비교, 차이만큼 보정 |

### 설계 메모

- **집계 DB 는 '더하기'다.** 덮어쓰기(Replacing) 방식이면 같은 윈도 결과가 두 번 와도 티가 안 나서 그림 19의 중복을 볼 수 없다.
- **tick.** 클릭이 드물면 이벤트 시각이 안 흘러 윈도가 안 닫힌다. 광고 서버가 1초마다 '지금 시각'을 로그에 넣어 워터마크를 민다.
- **클릭 하나하나의 판정은 Flink 가 내보내지 않는다.** 화면의 ✗(늦음)는 광고 서버가 Flink 의 실제 워터마크로 같은 규칙을 적용해 표시하고,
  중복·유실은 (윈도, 광고) 칸 단위로 장부와 비교해 표시한다.
- **집계는 한 단계(ONE_PHASE)로 한다.** 로컬·글로벌 2단계로 나누면 로컬 단계가 늦은 레코드를 메트릭 없이 버려 '늦어서 버린 수'가 안 보인다.
- **💥 는 도커 소켓으로 TaskManager 를 죽였다 켠다.** 실습용이다.

## 교재와 다른 점

- Cassandra → **ClickHouse**, Database Writer → **Kafka 엔진 + MV**.
- 병렬도 1. Map/Aggregate/Reduce 노드 분산과 핫스팟은 다루지 않았다.
- top N, 필터(국가·IP)는 뺐다. 이 데모는 3단계의 정확성 문제에 집중한다.
