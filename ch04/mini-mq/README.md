# mini-mq — 4장 분산 메시지 큐를 직접 만들어 보기

`ch04/정리.md` 의 설계를 Spring Boot로 작게 구현한다. 성능이 아니라 **동작을 눈으로 보는 것**이 목적이다.
주키퍼 없이, 브로커끼리 컨트롤러를 직접 뽑는다(KRaft combined 모드와 같은 모양).

## 단계

| 단계 | 내용 | 정리.md / 교재 그림 | 상태 |
|---|---|---|---|
| 1 | 세그먼트 로그 저장소, 생산자 버퍼·일괄 전송, 풀 + 롱 폴링 소비자, 오프셋 커밋 | 그림 4.9~4.14, 4.21, 4.32~4.33 | ✅ |
| 2 | 컨트롤러 선출 (Raft-lite: term·과반 투표·메타데이터 스냅샷 복제·epoch 펜싱), 파티션 배정, 클라이언트 라우팅 | 그림 4.8, 4.11~4.12, 4.22 자리 | ✅ |
| 3 | 복제: 리더/팔로워, ISR, 합의 오프셋(HW), ACK=1/all, 최소 ISR, 장애 복구, 꼬리 잘라내기, 오프셋도 복제(`__consumer_offsets`) | 그림 4.23~4.28 | ✅ |
| 4 | 소비자 그룹 코디네이터, 재조정(합류·나가기·장애), 리더 소비자의 배치 계획, 세대 펜싱 | 그림 4.15~4.20 | ✅ |

## 실행

필요한 것: Docker, JDK 21 (없으면 Gradle 툴체인이 받아 온다)

```bash
./run.sh                      # jar 빌드 + docker compose up -d
open http://localhost:8081    # 대시보드 (8082, 8083 도 같은 화면)
docker compose logs -f producer-web producer-batch consumer-logs consumer-orders consumer-metrics
docker compose down -v        # 끝. -v 는 브로커 데이터까지 지운다
RAFT_RPC_LOG=DEBUG ./run.sh   # Raft 박동 송수신까지 로그로 보기
```

## 대시보드

http://localhost:8081 (8082, 8083 도 같은 화면). 위에서 아래로:

- **컨트롤 플레인**: 브로커 3대. 👑 = 컨트롤러(Raft 리더), 금색 점선 = 박동 + 메타데이터 복제
- **데이터 플레인**: 생산자 → 토픽(● 허브) → 파티션 → 소비자 그룹. 선 굵기 = 초당 건수. 생산자 칸에 토픽별 acks
  - 파티션 왼쪽 띠 색 = 리더 브로커, 사본 칸 = ★ 리더 / 꽉 참 ISR / 점선 ISR 밖, 칸 위 화살표 = 복제(주황 뒤처짐, 빨강 ISR 밖)
- **보라 점선** = 클라이언트가 지도(메타데이터)를 받아 감 = 라우팅. 평소엔 없고 장애 직후에 나타난다
- **소비자 그룹 상자**: 세대·상태(안정 / ⏳ 재조정 중 / ⏳ 배정 대기), 멤버마다 맡은 파티션·속도·lag. 굵은 테두리 = 리더 소비자, 흐린 칸 = 노는 멤버
- **초록 점선** = 그룹 → 코디네이터 브로커 (박동·합류·오프셋 커밋)
- 맨 위 버튼: **고립** / **느린 복제**(끔 → 2초 → 6초) / **강제 종료**(15초 뒤 다시 뜸)
- 멤버 칸의 **⏸** = 15초 멈춤(박동도 끊김, 장애 흉내) / **👋** = 그룹에서 나갔다가 10초 뒤 다시 합류

## 지금 데모 구성 (docker-compose.yml)

```
producer-web   ─┬─▶ logs    6 파티션, ACK=1,   최소 ISR 1  ─▶ 그룹 search  × 3
                ├─▶ orders  3 파티션, ACK=all, 최소 ISR 2  ─▶ 그룹 payment × 4 (1명 놀기)
                └─▶ metrics 3 파티션, ACK=0                 ─▶ 그룹 monitor × 2
producer-batch ─┬─▶ logs
                └─▶ orders
```

- 생산자는 `DEMO_TOPICS="토픽:파티션수:acks:최소ISR:초당건수,..."` 로 여러 토픽에 보낸다. 토픽마다 생산자 객체를 따로 둔다(acks 가 다르니까)
- 키 = `생산자이름-서버번호`(web-03, batch-07). 여러 생산자가 같은 토픽에 써도 순서 검사가 섞이지 않게
- 소비자는 키별 seq 로 `gap`(ACK 받고 사라짐)·`dup`(다시 읽음)을 센다. 생산자는 ACK=0 빼고 끝없이 재시도하므로 gap 은 진짜 유실만 뜻한다
- ACK=0 도 응답이 올 때까지는 다음 묶음을 보내지 않는다(결과는 안 본다). 그래야 파티션 안 순서가 뒤바뀌지 않는다

## 4단계: 소비자 그룹 재조정 (그림 4.15~4.20)

소비자 늘리기: `docker compose up -d --no-recreate --scale consumer-logs=4 consumer-logs`

| 교재 | 여기서 | 코드 |
|---|---|---|
| 코디네이터 찾기 (그룹 이름 해시) | 그룹 이름 해시 → `__consumer_offsets` 파티션 → 그 리더 브로커가 코디네이터 | `Topics.offsetsPartition` |
| 합류 → 리더 소비자 선출 → 배치 계획 → 동기화 | join(모두 올 때까지 응답 보류) → 세대 +1, 리더 소비자 지정 → 리더가 계획(range / roundrobin) → sync 로 각자 몫 | `GroupCoordinator`, `Consumer.rejoin`, `Consumer.assign` |
| 재합류 요청은 박동 응답으로 (수동적) | 재조정이 시작되면 다음 박동 응답에 `rebalance=true` | `GroupCoordinator.heartbeat` |
| 나가기 (그림 4.19) | 나간다고 알리면 바로 재조정 | `leave` |
| 장애 (그림 4.20) | 박동이 6초 끊기면 죽음 처리 → 재조정 | `GroupCoordinator.tick` |
| 재조정 동안 소비 멈춤 | 다시 합류해 배정을 받기 전엔 poll 하지 않는다 | `Consumer.poll` |
| (교재 밖) 세대 펜싱 | 오프셋 커밋에 세대를 붙이고, 재조정 전 세대의 커밋은 거절(ILLEGAL_GENERATION) | `checkGeneration` |

### 실제로 본 것

| 실험 | 결과 |
|---|---|
| 기동 | 두 그룹 모두 한 번에 세대 1. search [0,1] [2,3] [4,5], payment 3명 배정 + 1명 놀기 |
| ⏸ 멤버 하나 멈춤 (그림 4.20) | 6초 뒤 "박동 끊김(장애)" → 세대 2: 남은 둘이 [0,1,2] [3,4,5]. 15초 뒤 새 멤버로 합류 → 세대 3: 다시 2개씩 |
| 👋 리더 소비자가 나감 (그림 4.19) | 1초 안에 세대 2, **새 리더 소비자**가 계획을 세우고 놀던 멤버가 파티션을 넘겨받음 |
| 소비자 하나 추가 (그림 4.18) | 세대 4: 6 파티션 ÷ 4명 = [2,2,1,1] (range) |
| 코디네이터 브로커 강제 종료 | `__consumer_offsets` 리더가 옮겨 가 새 코디네이터. 그룹 상태는 메모리에만 있어 **세대 1부터 다시**, 모두 새 멤버로 합류 (약 2초) |
| 위 실험들 동안 | 소비자 7명 모두 gap=0 |

### 만들면서 잡은 버그 (교훈)

- **빈 클러스터 기동 때 컨트롤러가 2초마다 바뀌었다 (term 64).** Raft 시계용 `ScheduledExecutorService` 를 스프링 빈으로 등록했더니
  스프링이 그걸 `@Scheduled` 공용 스케줄러로 썼다. 새 컨트롤러가 `__consumer_offsets` 를 만들며 커밋을 3초 기다리는 동안 같은 스레드의
  박동이 멈춰 팔로워가 다시 선거 → 새 컨트롤러가 또 기다림. 스레드 덤프로 찾았다. Raft 시계는 전용 스레드로, 스케줄 작업은 기다리지 않게 고쳤다.
- **일시적인 연결 오류에 소비자가 그룹에서 나갔다**(나갔다 들어오느라 재조정 2번). 오류에는 멤버십을 유지한 채 다시 시도하게 고쳤다.

### 4단계의 한계

- 재조정은 **전부 멈춤(eager)** 방식이다. 한 명만 바뀌어도 모두 파티션을 내려놓고 다시 받는다 (카프카의 협력적 재조정은 바뀌는 파티션만 옮긴다)
- 그룹 상태(멤버·세대)를 저장하지 않아 코디네이터가 바뀌면 처음부터 다시 합류한다 (카프카는 `__consumer_offsets` 에 그룹 메타데이터도 쓴다)
- 처리가 오래 걸려도 박동은 따로 가므로 "멈춘 처리"를 잡지 못한다 (카프카 `max.poll.interval.ms`)

## 3단계: 복제 (그림 4.23~4.28)

| 개념 | 여기서 | 코드 |
|---|---|---|
| 리더 사본 / 단순 사본 | 생산자는 리더에만 쓰고, 팔로워가 리더에서 가져간다(pull) | `ReplicaManager.followLoop` |
| ISR | 리더가 0.5초마다 판단: 4초 동안 안 왔거나 1000건 넘게 뒤처지면 빼 달라고, 따라잡으면 넣어 달라고 컨트롤러에 요청 | `evaluateIsr`, `Controller.alterIsr` |
| 합의 오프셋 (HW) | ISR 전원의 LEO 최솟값. **소비자는 HW 앞까지만** 읽는다 | `maybeAdvanceHighWatermark` |
| ACK=1 / all | 1 = 리더 저장 후 응답, all = HW 가 넘어가면 응답 | `ReplicaManager.produce` |
| 최소 ISR | ACK=all 인데 ISR 이 이보다 적으면 **저장하지 않고 503** | 〃 |
| 브로커 장애 복구 | 컨트롤러가 죽은 브로커를 ISR 에서 빼고, 리더였으면 ISR 에서 새 리더 + leader epoch +1. ISR 이 다 죽으면 리더 없음 | `Controller.onBrokerDead` |
| 꼬리 잘라내기 (교재 밖, KIP-101) | 돌아온 옛 리더는 "내 마지막 epoch 가 새 리더 로그에서 어디까지?"를 묻고 그 뒤를 자른다 | `truncateToLeader`, `PartitionLog.endOffsetForEpoch` |
| 상태 저장소 | 오프셋도 내부 토픽 `__consumer_offsets`(사본 3, ACK=all)에. 그룹 이름 해시 → 파티션 → 그 리더가 코디네이터 | `OffsetStore` |

생산자는 재시도를 끝없이 한다. 그래서 소비자의 `gap` 은 전부 **ACK 를 받았는데 사라진** 메시지다.

### 실제로 본 것

| 실험 | 결과 |
|---|---|
| 평소 | ACK=1 과 ACK=all 의 응답 지연이 비슷하다 (둘 다 약 30ms, 대부분 linger 20ms). 팔로워가 롱 폴링으로 바로 가져가기 때문 |
| 팔로워 둘을 **느린 복제 2초** | ACK=all 응답 지연 26ms → **평균 3초, 최대 4.5초**. ACK=1 은 그대로. "가장 느린 ISR 을 기다린다" |
| 그 상태에서 리더 **강제 종료** | 컨트롤러가 3초 뒤 죽음 표시 → 리더 이동(epoch 0→1), ISR [2,3]. 생산자가 지도를 다시 받음(보라 점선) |
| 옛 리더가 돌아옴 | `✂ logs-0 꼬리 4건, logs-3 꼬리 3건 잘라냄` → **consumer-search gap=7** (ACK=1 이 응답하고 잃은 것). **consumer-payment gap=0** (ACK=all) |
| 팔로워 둘을 **느린 복제 6초** | 4초 기준을 넘어 ISR=[리더] 로 줄어듦. 그 파티션에 ACK=all → **503 NOT_ENOUGH_REPLICAS**, ACK=1 → 200. orders 생산자는 재시도하며 기다림(유실 0) |
| 복구 | 지연을 풀면 따라잡은 사본이 ISR 로 복귀, ACK=all 지연 30ms 대로 |

### 3단계의 한계

- **선호 리더 복귀 없음**: 죽었다 돌아온 브로커는 팔로워로만 남는다. 리더가 한 브로커에 몰린다(실험 뒤 broker-2 가 리더 6개, broker-1 이 0개)
- 소비자는 커밋된 오프셋을 받아야 시작한다. 그룹 코디네이터 브로커가 죽으면 `__consumer_offsets` 리더가 옮겨질 때까지(몇 초) 소비가 멈춘다
- HW 를 디스크에 남기지 않는다. 재시작한 리더는 팔로워가 한 번씩 올 때까지 HW 를 올리지 않는다
- ISR 이 모두 죽으면 깨끗한 리더 선출만 한다(ISR 밖 사본을 리더로 올리는 unclean 선출 없음 = 가용성보다 영속성)

## 2단계: 주키퍼 없이 컨트롤러 뽑기 (Raft-lite)

```
             ┌──────── Raft-lite: 투표 · 박동 · 메타데이터 스냅샷 복제 ────────┐
             ▼                                ▼                               ▼
         broker-1                         broker-2 👑 컨트롤러               broker-3
     logs-0, logs-3                     logs-1, logs-4                   logs-2, logs-5
             ▲                                ▲                               ▲
             └──── 생산자·소비자: 아무 브로커에서 /metadata(지도)를 받아 파티션 리더로 직접 ────┘
```

교재는 조정 서비스(주키퍼)가 브로커 중 하나를 컨트롤러로 뽑는다. 여기서는 브로커 3대가 **서로 투표해서** 뽑는다.
카프카가 주키퍼를 걷어낸 KRaft(combined 모드)와 같은 모양이다.

| 정리.md 의 역할 | 누가 | 코드 |
|---|---|---|
| 리더 선출 (조정 서비스) | 브로커 3대가 Raft-lite 로 | `raft/RaftNode` |
| 메타데이터 저장소 | 과반이 받은 스냅샷 (`raft.json`) | `ClusterMetadata`, `FileRaftStorage` |
| 서비스 탐색 | 컨트롤러가 박동 응답으로 판단, 3초 끊기면 죽음 표시 | `Controller.checkLiveness` |
| 파티션 배정 | 컨트롤러가 살아 있는 브로커에 라운드 로빈 | `Controller.createTopic` |
| 라우팅 (지도를 보고 찾아가기) | 생산자·소비자 라이브러리 | `client/ClusterView` |

Raft-lite 가 Raft 에서 가져온 것: term, 무작위 선거 타임아웃, 과반 투표, 한 term 에 한 표(디스크에 남긴 뒤 응답),
**선출 제한**(메타데이터가 뒤처진 후보에게는 표를 안 준다), **자기 term 의 항목만 세어 커밋**.
단순화한 것: 로그 대신 메타데이터 스냅샷 전체를 보낸다, 멤버십 고정, PreVote 없음.

### 대시보드에서 해 볼 실험

맨 위 **고립** 버튼은 그 브로커를 네트워크에서 떼어 낸 것처럼 만든다 (Raft·생산·소비 요청을 503 으로 막고, 밖으로도 못 보냄).
진짜로 죽이려면 `docker compose stop broker-2` / `start broker-2`.

| 실험 | 실제로 본 것 |
|---|---|
| 첫 기동 | broker-2·3 이 term 1 에 동시 출마(표 갈림). broker-1 의 표를 먼저 받은 broker-2 가 2/3 로 당선. broker-3 은 "이미 나에게 투표함"으로 거절 |
| **리더 고립** | 1.5~3초 뒤 broker-3 이 term 2 로 당선. 고립된 broker-2 는 여전히 term 1 리더라고 믿고 다른 둘을 "죽음"으로 바꾸려 했지만 **커밋 못 함**(받기만 한 v4, 커밋 v2). 새 컨트롤러는 broker-2 를 죽음으로 커밋(v4). 같은 v4 인데 내용이 다르다 |
| 리더 고립 중 클라이언트 | broker-2 가 리더인 파티션 1·4 만 멈춤(`리더없음=[1, 4]`), 나머지는 계속. 생산자 재시도가 바닥난 15건 = 소비자 `gap=15` |
| **고립 해제 (펜싱)** | 돌아온 broker-2 의 term 1 박동을 둘이 "낡은 term" 으로 거부 → broker-2 는 응답의 term 2 를 보고 내려옴, 자기의 커밋 안 된 v4 를 버리고 새 리더의 v4 를 받음 |
| **팔로워 고립 10초** | 혼자 선거를 반복해 term 6 까지 올림 → 돌아오자 멀쩡한 리더가 그 term 을 보고 **내려옴**. 그러나 broker-1 은 메타데이터가 뒤처져(v5 < v6) 표를 못 받고, term 7 에 broker-2 가 당선. 쓸데없는 리더 교체 = PreVote 가 막는 문제 |
| 리더 컨테이너 정지 | 남은 둘 중 하나가 term 10 으로 당선, 3초 뒤 죽은 브로커 표시. 다시 켜면 팔로워로 합류. 재배포해도 term 이 이어진다(`raft.json`) |

### 2단계의 한계

- (3단계에서 풂) 사본이 하나뿐이라 리더 브로커가 죽으면 그 파티션이 멈췄다. 오프셋도 리더 로컬 파일이었다.
- PreVote 가 없어 고립됐다 돌아온 노드가 리더를 끌어내린다. 일부러 남겨 뒀다(눈에 보이게). 넣는다면: 출마 전에 "지금 리더 박동이 끊겼는지"를 과반에게 먼저 물어본다.
- 커밋 전에 리더 자리를 잃으면 요청한 쪽은 실패를 받지만, 실제로는 나중에 커밋될 수도 있다(결과 미상). Raft 의 원래 성질이다.

## 1단계: 로그 저장소와 전달 보장

> 지금 `docker-compose.yml` 은 3단계 구성이다. 아래의 anomaly 소비자·크래시 실험을 다시 보려면 compose 끝 주석대로 소비자에 `DEMO_CRASH_EVERY_SEC`·`DEMO_COMMIT_MODE: before`·`DEMO_PROCESS_MS` 를 넣는다.

```
producer ──(서버 ID 키, 버퍼에 모아 일괄 전송)──▶ 파티션 리더 ◀──(풀 + 롱 폴링)── consumer-search  (처리 후 커밋)
                                                 │                       └── consumer-anomaly (처리 전 커밋, 느림)
                                                 └─ /data/logs/Partition-{0..5}/segment-N.log
```

- **producer**: S1 로그창고 흉내. 서버 20대가 초당 200건. 키 = `server-NN`, 값에 `run=<생산자 실행 ID> seq=<서버별 일련번호>`
- **consumer-search**: 처리 후 커밋(최소 한 번). 20초마다 "묶음 처리 도중 죽음"을 흉내 낸다
- **consumer-anomaly**: 처리 전 커밋(최대 한 번), 한 건에 8ms라 생산보다 느리다 → lag 가 쌓인다(S1 돌발 상황)

소비자는 서버별 seq 로 `gap`(유실)과 `dup`(중복)을 센다. 같은 키는 같은 파티션이라 죽지 않는 한 둘 다 0 이다.

### 1단계에서 볼 수 있는 것

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
| `MINIMQ_PEERS` | — | 클러스터 구성원 `1=http://broker-1:8080,...` (나 포함) |
| `MINIMQ_ELECTION_MIN_MS` / `MAX_MS` | 1500 / 3000 | 선거 타임아웃 범위. 노드마다 무작위 |
| `MINIMQ_HEARTBEAT_MS` | 500 | 컨트롤러 박동 주기 |
| `MINIMQ_BROKER_DEAD_MS` | 3000 | 이 시간 응답이 없으면 컨트롤러가 죽음으로 표시 |
| `MINIMQ_REPLICA_LAG_TIME_MS` / `LAG_MAX_MESSAGES` | 4000 / 1000 | 이 시간 안 오거나 이만큼 뒤처지면 ISR 에서 뺀다 |
| `MINIMQ_ACK_TIMEOUT_MS` | 5000 | ACK=all 이 ISR 전원을 기다리는 최대 시간 |
| `DEMO_NAME` / `DEMO_TOPICS` | server / (비움) | 생산자 이름(키 앞에 붙음) / 여러 토픽: `토픽:파티션수:acks:최소ISR:초당건수,...` |
| `DEMO_ACKS` / `DEMO_MIN_INSYNC_REPLICAS` | 1 / 1 | `DEMO_TOPICS` 를 안 쓸 때: 생산자 acks(0, 1, all) / 최소 ISR |
| `MINIMQ_SESSION_TIMEOUT_MS` / `REBALANCE_TIMEOUT_MS` | 6000 / 4000 | 소비자 박동이 끊겼다고 볼 시간 / 재조정 때 다시 합류를 기다리는 시간 |
| `DEMO_ASSIGNOR` | range | 리더 소비자의 배치 방식: range \| roundrobin |
| `DEMO_RATE_PER_SEC` / `DEMO_SERVERS` | 200 / 20 | 생산 속도 / 키 개수 |
| `DEMO_BATCH_SIZE` / `DEMO_LINGER_MS` | 50 / 20 | 생산자 묶음 크기 / 최대 대기 시간 |
| `DEMO_COMMIT_MODE` | after | before = 최대 한 번, after = 최소 한 번 |
| `DEMO_PROCESS_MS` | 0 | 소비자 한 건 처리 시간 |
| `DEMO_CRASH_EVERY_SEC` | 0 | 처리 도중 죽음을 흉내 내는 주기 |

## 모듈

| 모듈 | 내용 |
|---|---|
| `common` | 메시지 자료 구조(표 4.1), 요청·응답, CRC, 파티셔너 |
| `client` | 생산자·소비자(그룹 합류·배치 계획·박동) 라이브러리 + `ClusterView`(지도). 스프링 없이 `java.net.http` 만 쓴다 |
| `broker` | Spring Boot. `LogSegment`(WAL 파일 하나) → `PartitionLog`(세그먼트 나열, HW, epoch 캐시) → `ReplicaManager`(리더·팔로워, ISR) + `OffsetStore` + `GroupCoordinator` + `raft/`(선출·메타데이터 복제) + `Controller` + `Traffic`·대시보드 |
| `demo` | 생산자·소비자 데모 앱 (`DEMO_ROLE` 로 고른다) |

## 일부러 단순하게 한 것

- **HTTP/JSON** 으로 주고받는다. 교재의 "메시지를 변형 없이 전달해 복사 최소화" 이점은 재현하지 않는다
- 오프셋 → 파일 위치 색인을 메모리에 다 들고 있고, 재시작 때 파일을 처음부터 훑는다 (카프카는 희소 색인 파일)
- 파티션별 전송 스레드는 하나뿐이다 (카프카 `max.in.flight=1` 과 같음). 그래서 재시도해도 파티션 안 순서가 뒤집히지 않는다

## 알려진 이슈

- WSL2 에서 한 번, 모든 컨테이너가 약 48초 동시에 멈췄다가 같은 순간 다시 움직였다. 서로 기다리지 않는 JVM 셋이 함께 멈췄으므로 호스트 쪽 일시 정지로 보지만 확인하지는 못했다
