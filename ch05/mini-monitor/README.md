# mini-monitor — 5장 지표 모니터링·경보 시스템을 직접 띄워 보기

`ch05/정리.md` 의 최종 설계(그림 22)를 작게 구현했다. 가짜 서버(시뮬레이터) N대가 지표를 내보내면,
수집 → Kafka → TimescaleDB → 질의 서비스 → 경보·시각화까지 한 번에 돈다. 성능이 아니라 **흐름을 눈으로 보는 것**이 목적이다.

```
 sim-push ×N ──push(라인 프로토콜)──▶ collector-push ×2 ─┐            ┌▶ writer ×2 ──▶ TimescaleDB
   (에이전트·로컬 버퍼)        (DNS 라운드로빈)         ├▶ Redpanda ─┘   (소비자 그룹)   하이퍼테이블·연속 집계
 sim-pull ×N ◀──pull(/metrics)── collector-pull ×2 ─┘   metrics 토픽                  압축·보관
      │                       (안정 해시 링으로 몫 나눔)    (6 파티션)                     │
      │                                                                                 ▼
      └── 등록·박동 ──▶ discovery (서비스 탐색, etcd 역할)          query (질의 서비스, Prometheus API 호환)
                                                                     │            │             │
                                                                     ▼            ▼             ▼
                                                                  Grafana   alerter(expr)   SLO 페이지
                                                                                  │
                                                                     Redpanda alerts ──▶ notifier (채널 전송)
```

| 교재 | 여기서는 |
|---|---|
| 지표 출처 | `simulator.py` — cpu·mem·disk·rps·p99 지연·오류율을 5초마다. 2분 주기 사인파 + 잡음 |
| pull 모델 + 서비스 탐색 (그림 8~10) | `sim-pull` 이 `/metrics`(Prometheus 형식)를 열고 `discovery` 에 등록. `collector-pull` 이 롱 폴링으로 목록 변경을 통지받는다 |
| 안정 해시 링 (그림 11) | 수집기마다 가상 노드 16개. 수집기를 늘리거나 죽이면 몫이 다시 나뉜다 |
| push 모델 + 로드밸런서 (그림 12~13) | `sim-push` 에이전트가 라인 프로토콜로 보낸다. 실패하면 로컬 버퍼에 쌓았다 재전송. LB 대신 도커 DNS 라운드로빈 |
| Kafka (그림 15~16) | Redpanda (Kafka 호환, JVM 없음). `metrics` 토픽 6 파티션, **지표 이름이 키** |
| 소비자 → TSDB | `writer` 소비자 그룹. 모아서 한 번에 INSERT 하고 **DB 에 쓴 뒤 커밋** |
| 시계열 DB | **TimescaleDB** (PostgreSQL 확장). 스키마는 `db/init.sql` |
| 인코딩·압축 | 15분 지난 청크를 컬럼형 압축 (`segmentby name, host`) |
| 다운샘플링 | 연속 집계 `metrics_1m` → `metrics_1h` (계층형). avg 만이 아니라 min/max/count 도 남긴다 |
| 보관 기간 | 보존 정책: 원본 6시간 · 1분 2일 · 1시간 30일 (교재 7일·30일·1년을 줄임) |
| 질의 서비스 | `query` — TSDB 앞의 얇은 계층. **Prometheus HTTP API 호환**이라 Grafana 는 기본 Prometheus 데이터소스로 붙는다. 질의어는 PromQL 의 작은 부분집합(MiniQL). step 에 따라 원본/1분/1시간 롤업을 골라 읽는다. 캐시는 뺐다 |
| 경보 시스템 (그림 19~20) | `rules.yaml`(PromQL 식 `expr`) → `alerter`(5초마다 질의 서비스에 물어 평가, 상태 inactive→pending→firing→resolved 를 `alerts` 테이블에) → host 별 병합 → Kafka `alerts` → `notifier` |
| 시각화 (그림 21) | Grafana (기성품, 질의 서비스 경유) + 흐름을 보는 파이프라인 뷰(직접 만듦) |
| (덤) SLO | `slo.yaml` → 파이프라인 뷰 `/slo`. SLI·오류 예산·소진율을 질의 서비스로 계산 (1시간 창) |

## 실행

필요한 것: Docker (Docker Desktop 이면 WSL integration 켜기)

```bash
./run.sh            # push 4대 + pull 4대
./run.sh 10 6       # push 10대 + pull 6대. 실행 중에 다시 부르면 개수만 바뀐다
docker compose logs -f alerter notifier
docker compose down      # 끝. DB·Kafka 는 메모리(tmpfs)에만 있어서 멈추면 데이터도 사라진다
```

> 시뮬레이터 수는 **파이프라인 뷰 ① 지표 출처 칸의 −/+ (또는 숫자 입력 후 Enter)** 로도 바꿀 수 있다.
> `pipeline` 컨테이너에 docker.sock 을 넘겨 Docker API 로 같은 서비스 컨테이너를 복제·삭제한다 (compose `--scale` 과 같은 결과, 최대 30대).
> 터미널에서는 꼭 `./run.sh` 로 바꾼다. `docker compose up -d` 를 `--scale` 없이 부르면 시뮬레이터가 1대씩으로 줄어든다.
> 줄어든 호스트는 "죽은 서버"와 구별되지 않으므로 `no_data`/`instance_down` 경보가 뜬다 (실제 시스템도 똑같다).

| 화면 | 주소 | 보는 것 |
|---|---|---|
| 파이프라인 뷰 | http://localhost:8000 | 출처 → 수집기 → Kafka 파티션/lag → 소비자 → DB → 질의 서비스 → 경보 흐름, 호스트 맵, 해시 링, 장애 버튼, 시뮬레이터 수 조절 |
| 경보 | http://localhost:8000/alerts | 규칙(expr), 경보 저장소 상태, 보낸 알림과 분당 그래프 |
| SLO | http://localhost:8000/slo | 요청 성공률·응답 지연·수집 가용성의 SLI, 오류 예산, 소진율 |
| Grafana | http://localhost:3000/d/mini-monitor | Datadog 식 대시보드: 개요 숫자, 호스트 맵, 시스템 지표, 수집 방식, 다운샘플링 비교, 청크, 경보·알림 |
| TimescaleDB | `psql postgresql://postgres:postgres@localhost:5432/metrics` | 직접 SQL |

## 해 볼 것

1. **장애 넣기** — 파이프라인 뷰 호스트 맵에서 칸을 누르고 버튼을 누른다.
   - 🔥 CPU 스파이크 → 15초 뒤 `high_cpu`, `high_latency` firing → slack 알림. 끝나면 resolved 알림
   - 💧 메모리 누수 → `memory_high`
   - 💀 다운: **pull 호스트**는 수집기가 응답을 못 받아 `up=0` 을 직접 남기므로 10초 만에 `instance_down`.
     **push 호스트**는 그냥 조용해질 뿐이라 "20초째 안 옴"으로 `no_data`. 교재 pull vs push 표의 "상태 확인" 줄을 그대로 본다
2. **DB 장애 흉내** — 소비자 카드의 `DB 장애` 버튼. 쓰기를 멈추면 Kafka 파티션에 빨간 lag 가 쌓이고,
   `재개` 하면 따라잡아 0 으로 돌아간다. 잃은 데이터가 없다 (그림 15 의 이유)
3. **수집기 늘리기** — `docker compose up -d --scale collector-pull=3 --scale sim-pull=6 --scale sim-push=4 --no-recreate`.
   해시 링 색이 다시 나뉘고 수집기 로그에 `내 몫 변경` 이 찍힌다. 하나를 죽이면(`docker stop mini-monitor-collector-pull-1`) 6초 뒤 남은 수집기가 몫을 넘겨받는다
4. **다운샘플링** — Grafana 의 "다운샘플링 비교" 패널. 원본 5초 vs 1분 avg/max/min vs 1시간 avg.
   스파이크를 넣으면 1분 **avg** 는 뭉개지고 **max** 는 남는다 (정리.md 토론 7번)
5. **압축** — 15~20분 지나면 Grafana 압축률과 "청크" 표에 `압축됨` 이 나온다
   ```sql
   SELECT chunk_name, before_compression_total_bytes, after_compression_total_bytes
   FROM chunk_compression_stats('metrics');
   ```
6. **카디널리티** — `활성 시계열` = (지표 이름, host) 조합 수. 시뮬레이터를 늘리면 그만큼 곱으로 는다
7. **SQL 로 시계열 질의** — 교재는 "SQL 은 복잡하다"고 했지만 TimescaleDB 함수로는 짧다
   ```sql
   SELECT time_bucket('1 minute', time) AS t, host, avg(value), max(value)
   FROM metrics WHERE name = 'cpu_usage' AND time > now() - interval '15 minutes'
   GROUP BY t, host ORDER BY t;
   ```

8. **질의 서비스** — Grafana 도 경보도 여기에만 묻는다. 직접 물어보기:
   ```bash
   docker compose exec pipeline python -c "import urllib.request as u,urllib.parse as p;print(u.urlopen('http://query:8080/api/v1/query',p.urlencode({'query':'topk(3, avg_over_time(cpu_usage[30s]))'}).encode()).read().decode())"
   ```
   파이프라인 뷰 ⑥ 칸에서 클라이언트별 질의 수, 최근 질의, 어느 저장 계층(raw/1m/1h)을 읽었는지 보인다.
   Grafana 시간 범위를 2일로 넓히면 1m 롤업 쪽 막대가 는다
9. **SLO** — `/slo` 를 띄워 두고 호스트 하나에 🔥 스파이크를 넣으면 요청 성공률의 5분 소진율이 뛰고 오류 예산이 줄어든다.
   pull 호스트를 💀 다운시키면 수집 가용성 예산이 깎인다

## 구성 요소

| 서비스 | 파일 | 하는 일 |
|---|---|---|
| discovery | `app/discovery.py` | 등록·박동(TTL 6초)·롱 폴링으로 변경 알림 |
| sim-push / sim-pull | `app/simulator.py` | 가짜 서버. `/control` 로 spike·leak·down |
| collector-push / -pull | `app/collector.py` | 받거나 당겨 와서 Kafka 로 |
| redpanda | (이미지) | Kafka 호환 브로커 |
| writer | `app/writer.py` | Kafka → TimescaleDB, `/control` 로 pause·resume |
| timescaledb | `db/init.sql` | 하이퍼테이블, 압축·연속 집계·보존 정책, 경보 저장소 |
| query | `app/query.py` | 질의 서비스. MiniQL 파싱 → SQL, Prometheus API(`/api/v1/query`, `query_range`, `labels`, `series`) |
| alerter | `app/alerter.py`, `app/rules.yaml` | 질의 서비스로 규칙 평가, 상태 저장, 병합 후 Kafka 로 (최소 한 번) |
| notifier | `app/notifier.py` | Kafka → 채널 (실제로는 `notifications` 테이블과 로그). `WEBHOOK_URL` 을 주면 POST |
| pipeline | `app/pipeline.py`, `app/static/`, `app/slo.yaml` | 파이프라인 뷰·경보·SLO 페이지. docker.sock 으로 시뮬레이터 수 조절 |
| grafana | `grafana/` | 데이터소스·대시보드 자동 등록. 대시보드는 `python3 grafana/make_dashboard.py > grafana/dashboards/mini-monitor.json` 으로 다시 만든다 |

## 교재와 다르게 한 것

- **질의 서비스는 두고 캐시는 뺐다**. 질의어는 PromQL 의 작은 부분집합(MiniQL)이고 `age_over_time` 같은 자체 함수가 하나 있다.
  범위 질의는 step 구간마다 묶어 계산하는 근사다(진짜 PromQL 은 각 시점에서 창을 미끄러뜨린다).
  저장소 내부(청크·압축·적재량)와 경보 저장소 패널만 Grafana 가 TimescaleDB 를 직접 본다
- TimescaleDB 2.17 의 벡터화 집계가 압축 청크를 jsonb 로 GROUP BY 할 때 labels 를 NULL 로 주는 버그가 있어, 질의 서비스 연결에서 끈다
- **경보 저장소가 Cassandra 가 아니라 같은 PostgreSQL** 이다. 실습 규모라 DB 를 하나 더 두지 않았다
- **로드밸런서 대신 도커 DNS 라운드로빈**. 에이전트가 매 요청 새로 이름을 풀어 수집기 2대에 나눠 보낸다
- **병합은 같은 평가 주기 안에서만**. 같은 host 의 경보가 5초 차이로 뜨면 알림 두 건이 된다
- 수치는 실습용으로 줄였다: 수집 주기 5초, 청크 10분, 압축 15분, 보관 6시간
