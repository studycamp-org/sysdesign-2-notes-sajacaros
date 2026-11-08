# mini-mq — 4장 분산 메시지 큐를 직접 만들어 보기

`ch04/정리.md` 의 설계를 Spring Boot로 작게 구현한다. 성능이 아니라 **동작을 눈으로 보는 것**이 목적이다.
주키퍼 없이, 브로커끼리 컨트롤러를 직접 뽑는다(KRaft combined 모드와 같은 모양).

## 단계

| 단계 | 내용 | 정리.md / 교재 그림 | 상태 |
|---|---|---|---|
| 1 | 세그먼트 로그 저장소, 생산자 버퍼·일괄 전송, 풀 + 롱 폴링 소비자, 오프셋 커밋 | 그림 4.9~4.14, 4.21, 4.32~4.33 | ✅ |
| 2 | 컨트롤러 선출 (Raft-lite: term·과반 투표·메타데이터 스냅샷 복제·epoch 펜싱) | 그림 4.8, 4.22 자리 | ⏳ |
| 3 | 복제: 리더/팔로워, ISR, 합의 오프셋, ACK=0/1/all, 최소 ISR, 브로커 장애 복구 | 그림 4.23~4.28 | ⏳ |
| 4 | 소비자 그룹 코디네이터, 재조정, `__consumer_offsets` 내부 토픽 | 그림 4.15~4.20 | ⏳ |

## 실행

필요한 것: Docker, JDK 21 (없으면 Gradle 툴체인이 받아 온다)

```bash
./run.sh                      # jar 빌드 + docker compose up -d
open http://localhost:8081    # 대시보드
docker compose logs -f producer consumer-search consumer-anomaly
docker compose down -v        # 끝. -v 는 브로커 데이터까지 지운다
```

## 1단계 구성

```
producer ──(서버 ID 키, 버퍼에 모아 일괄 전송)──▶ broker-1 ◀──(풀 + 롱 폴링)── consumer-search  (처리 후 커밋)
                                                 │                       └── consumer-anomaly (처리 전 커밋, 느림)
                                                 └─ /data/logs/Partition-{0..5}/segment-N.log
```

- **producer**: S1 로그창고 흉내. 서버 20대가 초당 200건. 키 = `server-NN`, 값에 `run=<생산자 실행 ID> seq=<서버별 일련번호>`
- **consumer-search**: 처리 후 커밋(최소 한 번). 20초마다 "묶음 처리 도중 죽음"을 흉내 낸다
- **consumer-anomaly**: 처리 전 커밋(최대 한 번), 한 건에 8ms라 생산보다 느리다 → lag 가 쌓인다(S1 돌발 상황)

소비자는 서버별 seq 로 `gap`(유실)과 `dup`(중복)을 센다. 같은 키는 같은 파티션이라 죽지 않는 한 둘 다 0 이다.

## 여기서 볼 수 있는 것

| 관찰 | 어디서 | 정리.md 의 어느 문장 |
|---|---|---|
| 세그먼트가 256KB 마다 새로 생기고, 5분 지나면 앞에서부터 지워진다 | 대시보드 막대 | 데이터 저장소: active 세그먼트에만 쓰고 오래된 것은 잘라낸다 |
| 묶음이 평균 1.8건밖에 안 찬다 | producer 로그 | 배치 크기 = 처리량 vs 지연. 200건/s ÷ 6파티션이면 20ms 동안 파티션당 0.7건 → `DEMO_LINGER_MS` 를 늘리면 묶음이 커지고 지연이 는다 |
| search 가 죽을 때마다 처리한 만큼 `dup` 이 는다, `gap` 은 0 | consumer-search 로그 | 최소 한 번: 처리 후 커밋 → 커밋 전에 죽으면 다시 읽어 중복 |
| anomaly 가 죽으면 처리 못 한 만큼 `gap` 이 는다, `dup` 은 0 | consumer-anomaly 로그 | 최대 한 번: 처리 전 커밋 → 처리 중 죽으면 유실 |
| anomaly 의 lag 가 계속 쌓인다 | 대시보드 그룹 표 | 소비가 생산보다 느리면 소비자를 늘리거나 기다린다(풀 모델) |
| 브로커를 재시작해도 오프셋이 이어진다 | `docker compose restart broker-1` | 세그먼트 파일을 다시 훑어 오프셋 위치를 복구, 쓰다 만 꼬리는 잘라낸다 |
| 그런데 브로커가 내려간 동안 생산자 재시도(합계 약 6초)가 바닥나면 그만큼 **유실**되고, 소비자 `gap` 으로 똑같이 잡힌다 | producer `failed=12` ↔ search `gap=12` | 사본이 하나뿐이면 브로커 하나가 단일 장애점 → 3단계 복제가 필요한 이유 |

실측 예 (crash 20초 주기):

```
[consumer search]  ... gap=0 dup=10 crash=1     ← 묶음 21건 중 10건 처리하고 죽음
[consumer search]  ... gap=0 dup=23 crash=2     ← 27건 중 13건
[consumer anomaly] ... gap=1354 dup=0 crash=1   ← 2708건 중 1354건 처리하고 죽음

# 브로커 재시작 실험
[producer]         ... retries=36 failed=12      ← 재시도 소진
[consumer search]  ... gap=12 dup=19 crash=1     ← 유실 12 = failed 12, 중복 = 재연결 7 + 크래시 12
```

## 설정 (환경 변수)

| 변수 | 기본 | 뜻 |
|---|---|---|
| `MINIMQ_SEGMENT_BYTES` | 1MB | 세그먼트 크기 |
| `MINIMQ_RETENTION_MS` | 2주 | 보관 기간 |
| `MINIMQ_FLUSH_EVERY_WRITE` | false | true 면 쓸 때마다 fsync. false 면 OS 페이지 캐시에 맡긴다 |
| `DEMO_RATE_PER_SEC` / `DEMO_SERVERS` | 200 / 20 | 생산 속도 / 키 개수 |
| `DEMO_BATCH_SIZE` / `DEMO_LINGER_MS` | 50 / 20 | 생산자 묶음 크기 / 최대 대기 시간 |
| `DEMO_COMMIT_MODE` | after | before = 최대 한 번, after = 최소 한 번 |
| `DEMO_PROCESS_MS` | 0 | 소비자 한 건 처리 시간 |
| `DEMO_CRASH_EVERY_SEC` | 0 | 처리 도중 죽음을 흉내 내는 주기 |

## 모듈

| 모듈 | 내용 |
|---|---|
| `common` | 메시지 자료 구조(표 4.1), 요청·응답, CRC, 파티셔너 |
| `client` | 생산자·소비자 라이브러리. 스프링 없이 `java.net.http` 만 쓴다 |
| `broker` | Spring Boot. `LogSegment`(WAL 파일 하나) → `PartitionLog`(세그먼트 나열) → `LogManager`(토픽) + `OffsetStore` + 대시보드 |
| `demo` | 생산자·소비자 데모 앱 (`DEMO_ROLE` 로 고른다) |

## 일부러 단순하게 한 것

- **HTTP/JSON** 으로 주고받는다. 교재의 "메시지를 변형 없이 전달해 복사 최소화" 이점은 재현하지 않는다
- 오프셋 → 파일 위치 색인을 메모리에 다 들고 있고, 재시작 때 파일을 처음부터 훑는다 (카프카는 희소 색인 파일)
- 1단계는 브로커 1대라 ACK 설정이 의미가 없다 (사실상 ACK=1). 3단계에서 다룬다
- 1단계 소비자는 그룹 안 소비자가 1명이라고 가정하고 모든 파티션을 맡는다. 4단계에서 코디네이터가 나눠 준다
- 토픽 메타데이터와 오프셋은 브로커 로컬 JSON 파일에 둔다. 2·4단계에서 옮긴다
- 파티션별 전송 스레드는 하나뿐이다 (카프카 `max.in.flight=1` 과 같음). 그래서 재시도해도 파티션 안 순서가 뒤집히지 않는다

## 알려진 이슈

- WSL2 에서 한 번, 모든 컨테이너가 약 48초 동시에 멈췄다가 같은 순간 다시 움직였다. 서로 기다리지 않는 JVM 셋이 함께 멈췄으므로 호스트 쪽 일시 정지로 보지만 확인하지는 못했다
