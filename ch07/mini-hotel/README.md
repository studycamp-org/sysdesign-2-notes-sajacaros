# mini-hotel — 7장 호텔 예약 시스템을 실제 사이트처럼

겉은 평범한 호텔 예약 사이트 **쉼표 스테이**다. 호텔을 고르고, 예약창에 들어가고, 결제하고, 취소한다.
오른쪽 아래 **`</> 개발자`** 버튼을 누르면 그 클릭 하나가 서비스들을 어떻게 지나가는지
(API 호출 · SQL · 트랜잭션 · 캐시 · 예약 상태 변화)가 그대로 보인다.

```
 손님 브라우저 ──▶ Public API Gateway ──┬──▶ Hotel Service ──────▶ [hotel 스키마]
  (탭마다 다른 사람)  정적 파일(CDN 역할)  ├──▶ Rate Service ───────▶ [rate 스키마]
                   인증 · 처리율 제한     ├──▶ Reservation Service ─▶ [reservation 스키마: 예약 + 재고]
                   [guest 스키마]        │        │  ▲    └────────▶ Redis (재고 캐시)
                                        │        ▼  │ 결제 결과
                                        └──▶ Payment Service ────▶ [payment 스키마]

 호텔 직원 ──▶ Internal API ──▶ Hotel Management Service ──▶ Hotel · Reservation Service
```

교재 그림 4·5와 같은 구성이다. 서비스마다 컨테이너를 따로 띄웠다(같은 이미지, `MODULE` 만 다름).

## 실행

```bash
./run.sh                 # 빌드하고 띄운다 (10초쯤)
docker compose down      # 끝. DB·캐시는 메모리에만 있어 모두 사라진다
```

| 화면 | 주소 |
|---|---|
| 호텔 예약 사이트 + 개발자 패널 | http://localhost:8007 — 탭을 여러 개 열고 탭마다 다른 사람으로 로그인할 수 있다 |
| 호텔 직원 관리자 | http://localhost:8008 — 예약 목록, 손님 대신 취소, 날짜별 판매 객실 수 조정 |
| PostgreSQL | `psql postgresql://postgres:postgres@localhost:15437/hotel` |

## DB: 인스턴스 하나, 서비스마다 스키마와 계정

| 스키마 | 주인(계정) | 테이블 |
|---|---|---|
| `guest` | `gateway_svc` | guest |
| `hotel` | `hotel_svc` | hotel, room_type, room |
| `rate` | `rate_svc` | room_type_rate |
| `reservation` | `reservation_svc` | **reservation, room_type_inventory**, status_history |
| `payment` | `payment_svc` | payment |

- 스키마만 나누면 PostgreSQL 은 같은 DB 안이라 스키마를 넘는 조인·트랜잭션을 그냥 허용한다.
  그래서 **서비스마다 계정을 따로 두고 자기 스키마에만 권한을 줬다.** Payment Service 가
  `reservation.reservation` 을 직접 고치면 `permission denied for schema reservation` 이 난다(실험 ⑤).
- 예약과 재고는 같은 `reservation` 스키마에 있어 **한 트랜잭션**으로 처리한다. 교재가 고른 절충이다.
- 결제와 예약 상태는 다른 서비스라 한 트랜잭션으로 묶이지 않는다. 그래서 API 로 알리고,
  알림이 실패하면 다시 보내고, 늦었으면 보상 트랜잭션으로 환불한다(실험 ④, 사가의 축소판).
- 샤딩은 하지 않았다. 초당 예약 3건 규모에서는 차이가 보이지 않고, 라우팅 코드가 정작 보고 싶은 부분보다 커진다.
  대신 **Redis 재고 캐시(그림 16)** 를 넣어 캐시와 DB가 어긋나는 상황을 겪어 볼 수 있게 했다.
- `room_type_inventory` 는 앞으로 **2년치 행을 미리** 만들고, 매시간 도는 작업이 새 날짜 행을 채운다.
  처음 띄울 때 사이트가 비어 보이지 않도록 다른 채널 예약이 있는 것처럼 `total_reserved` 를 35~80% 채워 둔다.

## 예약 흐름

| 손님 화면 | API | 하는 일 |
|---|---|---|
| 호텔 상세 | `GET /v1/hotels/{id}` · `GET /v1/rates/hotels` · `GET /v1/inventory` | 객실 유형, 날짜별 요금 합계, 남은 객실(캐시에서 읽음) |
| "예약하기" → 예약창 | `POST /v1/reservation-orders` | 캐시로 빈방 확인 → Rate Service 에 요금 → **`reservationID` 발급**(스노플레이크) → 예약창 정보를 Redis 에 15분 |
| "예약 완료" | `POST /v1/reservations` | 락 방식대로 재고를 잡고 `reservation` 행 INSERT → **pending**. 같은 `reservationID` 가 또 오면 기본 키 유니크 제약 → 처음 결과를 돌려준다 |
| "결제하기" | `POST /v1/payments` | 가짜 PG 승인 → Reservation Service 에 결과 통보 → **paid** / **rejected**(재고 반환) |
| 내 예약 → 취소 | `DELETE /v1/reservations/{id}` | pending → **cancelled**, paid → Payment Service 에 환불 요청 → **refunded**. 둘 다 재고 반환 |
| (시간 초과) | 주기 작업 | 결제 대기 한도를 넘긴 pending → **cancelled**, 재고 반환 |

`reservationID` 를 발급하는 API(`POST /v1/reservation-orders`)는 교재에 없어서 그림 8의 ①·②를 보고 만들었다.

## 락 방식 (실험 패널에서 바꾼다)

| 방식 | 재고를 잡는 SQL | 동시에 6명이 마지막 1실 (경쟁 구간 300ms) |
|---|---|---|
| 잠금 없음 | `SELECT` → 검사 → `UPDATE … + n` | **6명 모두 성공 → 5실 초과 판매** |
| 비관적 락 | `SELECT … FOR UPDATE` → 검사 → `UPDATE` | 1명 성공. 나머지는 잠금을 기다렸다가 검사에서 거절 |
| 낙관적 락 | `SELECT version` → 검사 → `UPDATE … WHERE version = 읽은 값` | 1명 성공. 나머지는 충돌 → 재시도 → 검사에서 거절 |
| DB 제약 조건 | 잠금 없음과 같고, 테이블에 `CHECK (total_reserved <= 한도)` | 1명 성공. 나머지는 `CheckViolation` 으로 롤백 |
| 조건부 UPDATE | `UPDATE … WHERE total_reserved + n <= 한도` 한 문장, 바뀐 행 수 = 숙박일 수인지 확인 | 1명 성공 (토론거리 2번) |

2026-11-26 에 스크립트로 확인한 결과다. 경쟁 구간을 0ms 로 둬도 잠금 없음에서는 가끔 초과 판매가 난다.

DB 제약 조건 방식은 고를 때 서비스가 `ALTER TABLE … ADD CONSTRAINT` 로 붙이고, 다른 방식으로 바꾸면 뗀다.
제약이 스키마에 들어 있어 코드처럼 버전 관리하기 어렵다는 교재의 지적을 그대로 겪는다.

## 실험 (개발자 패널 → 실험 탭)

| # | 교재 | 하는 것 | 보이는 것 |
|---|---|---|---|
| ① | 경쟁 조건 (그림 11~14) | 락 방식을 고르고 "동시 예약 실행" | 봇 N명 중 몇 명이 성공했는지, 한도를 넘겨 팔렸는지. 재고 탭에 초과한 날이 빨갛게 |
| ② | 멱등 키 (그림 7·10) | 버튼 중복 클릭 방지를 끄고 "예약 완료"를 빠르게 두 번 | 흐름 탭: 두 번째 요청은 재고를 잡았다가 `reservation_id` 유니크 제약에 걸려 롤백, 처음 결과를 받음 |
| ③ | 캐시 (그림 16) | 캐시 반영 지연 10초 + 남은 객실 1개 + 탭 두 개 | 탭 B는 10초 동안 "1개 남음"을 보고 예약을 누르지만 DB 최종 검사에서 거절. 재고 탭의 노란 칸 = 캐시 ≠ DB |
| ④ | 서비스 간 일관성 (그림 19) | 결제를 "결제 후 알림 유실", 결제 대기 한도 30초 → 예약·결제 → 30초 뒤 결제를 "성공"으로 | 돈은 빠졌는데 예약은 pending → 만료로 cancelled → 알림 재전송이 409 → **보상 트랜잭션으로 자동 환불** |
| ⑤ | 서비스 경계 (그림 17) | "직접 UPDATE 시도" | `permission denied for schema reservation` |

탭마다 다른 사람으로 로그인하면 직접 손으로 경쟁을 만들 수도 있다(경쟁 구간을 1500ms 로 두면 쉽다).

## 교재와 다르게 한 것

| 교재 | 여기 | 이유 |
|---|---|---|
| `room_type_rate` 기본 키 `(hotel_id, date)` | `(hotel_id, room_type_id, date)` | 객실 유형마다 요금이 다르다 |
| 예약 SQL `date BETWEEN start AND end` | `date >= start AND date < end` | 체크아웃 날 밤은 묵지 않는다. BETWEEN 이면 하루를 더 잡는다 |
| `UPDATE` 에 `hotel_id` 조건 없음 | 넣음 | 다른 호텔의 같은 `room_type_id` 행까지 바뀔 수 있다 |
| `CHECK (total_inventory - total_reserved >= 0)` | `CHECK (total_reserved <= floor(total_inventory × 110%))` | 초과 예약 10% 와 맞지 않는다(토론거리 1번) |
| MySQL (기본 격리 수준 REPEATABLE READ) | PostgreSQL (기본 READ COMMITTED) | 그림 11의 경쟁 조건은 똑같이 생긴다 |
| 재고 갱신 후 캐시 반영: 앱 코드 또는 CDC(Debezium) | 앱 코드, 지연 시간 조절 | Kafka·Debezium 까지 띄우면 무거워진다 |
| 샤딩 (`hash(hotel_id) % N`) | 안 함 (N = 1) | 위 DB 절 참고 |
| 검색 | 지역 고르기만 | 교재도 범위 밖 |

## 파일

| 파일 | 내용 |
|---|---|
| `db/init.sql` | 스키마·계정·테이블 |
| `app/gateway.py` | Public API Gateway / Internal API (`GATEWAY_MODE`), 개발자 패널용 SSE |
| `app/hotel_svc.py` · `rate_svc.py` · `reservation_svc.py` · `payment_svc.py` · `mgmt_svc.py` | 서비스 |
| `app/common.py` | 추적(Redis 스트림 `trace`), SQL 기록, 서비스 사이 호출, 실험 설정(`cfg`) |
| `app/catalog.py` | 처음 넣는 호텔 5곳 |
| `app/static/index.html` | 손님 사이트 + 개발자 패널 |
| `app/static/admin.html` | 호텔 직원 관리자 |
