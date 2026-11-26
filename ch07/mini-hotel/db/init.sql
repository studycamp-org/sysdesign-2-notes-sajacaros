-- mini-hotel 데이터베이스 초기화
-- PostgreSQL 인스턴스 1개, DB 1개 안에 서비스마다 스키마를 하나씩 둔다 (그림 4·5).
-- 서비스마다 DB 계정을 따로 두고, 그 계정은 자기 스키마만 쓸 수 있다.
-- → Payment Service 가 reservation 스키마를 직접 건드리면 permission denied.
--   서비스 경계를 넘는 일은 반드시 API 호출로 한다 (그림 19의 상황).

CREATE ROLE gateway_svc     LOGIN PASSWORD 'gateway';
CREATE ROLE hotel_svc       LOGIN PASSWORD 'hotel';
CREATE ROLE rate_svc        LOGIN PASSWORD 'rate';
CREATE ROLE reservation_svc LOGIN PASSWORD 'reservation';
CREATE ROLE payment_svc     LOGIN PASSWORD 'payment';

REVOKE ALL ON SCHEMA public FROM PUBLIC;

CREATE SCHEMA guest       AUTHORIZATION gateway_svc;
CREATE SCHEMA hotel       AUTHORIZATION hotel_svc;
CREATE SCHEMA rate        AUTHORIZATION rate_svc;
CREATE SCHEMA reservation AUTHORIZATION reservation_svc;
CREATE SCHEMA payment     AUTHORIZATION payment_svc;

ALTER ROLE gateway_svc     SET search_path = guest;
ALTER ROLE hotel_svc       SET search_path = hotel;
ALTER ROLE rate_svc        SET search_path = rate;
ALTER ROLE reservation_svc SET search_path = reservation;
ALTER ROLE payment_svc     SET search_path = payment;

-- ── guest (Public API Gateway 의 인증이 쓴다) ─────────────────────────
SET ROLE gateway_svc;
CREATE TABLE guest.guest (
  guest_id   serial PRIMARY KEY,
  name       text NOT NULL UNIQUE,
  email      text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
RESET ROLE;

-- ── hotel (Hotel Service) ───────────────────────────────────────────
SET ROLE hotel_svc;
CREATE TABLE hotel.hotel (
  hotel_id    int PRIMARY KEY,
  name        text NOT NULL,
  city        text NOT NULL,
  address     text NOT NULL,
  stars       int  NOT NULL,
  scene       text NOT NULL,          -- 화면 그림 종류
  description text NOT NULL
);
CREATE TABLE hotel.room_type (
  room_type_id int PRIMARY KEY,
  hotel_id     int NOT NULL REFERENCES hotel.hotel,
  name         text NOT NULL,
  beds         text NOT NULL,
  size_m2      int  NOT NULL,
  capacity     int  NOT NULL,
  amenities    text[] NOT NULL
);
CREATE TABLE hotel.room (
  room_id      serial PRIMARY KEY,
  hotel_id     int NOT NULL REFERENCES hotel.hotel,
  room_type_id int NOT NULL REFERENCES hotel.room_type,
  room_number  text NOT NULL,
  floor        int  NOT NULL,
  is_available boolean NOT NULL DEFAULT true,
  UNIQUE (hotel_id, room_number)
);
RESET ROLE;

-- ── rate (Rate Service) ─────────────────────────────────────────────
-- 교재 그림 2는 기본 키가 (hotel_id, date) 뿐이다. 객실 유형별 요금이므로 room_type_id 를 넣었다.
SET ROLE rate_svc;
CREATE TABLE rate.room_type_rate (
  hotel_id     int  NOT NULL,
  room_type_id int  NOT NULL,
  date         date NOT NULL,
  rate         int  NOT NULL,
  PRIMARY KEY (hotel_id, room_type_id, date)
);
RESET ROLE;

-- ── reservation (Reservation Service) ───────────────────────────────
-- 예약과 재고가 한 스키마 = 한 트랜잭션 (교재가 고른 절충)
SET ROLE reservation_svc;
CREATE TABLE reservation.room_type_inventory (
  hotel_id        int  NOT NULL,
  room_type_id    int  NOT NULL,
  date            date NOT NULL,
  total_inventory int  NOT NULL,
  total_reserved  int  NOT NULL DEFAULT 0,
  version         int  NOT NULL DEFAULT 0,       -- 낙관적 락용
  PRIMARY KEY (hotel_id, room_type_id, date)
);
-- DB 제약 조건(방법 3)은 실험 패널에서 고를 때 서비스가 ALTER TABLE 로 붙였다 뗀다.

CREATE TABLE reservation.reservation (
  reservation_id bigint PRIMARY KEY,             -- 멱등 키. 예약창에 들어갈 때 발급
  hotel_id       int  NOT NULL,
  room_type_id   int  NOT NULL,
  start_date     date NOT NULL,                  -- 체크인
  end_date       date NOT NULL,                  -- 체크아웃 (그날 밤은 묵지 않는다)
  room_count     int  NOT NULL,
  guest_id       int  NOT NULL,
  status         text NOT NULL CHECK (status IN ('pending','paid','rejected','cancelled','refunded')),
  amount         int  NOT NULL,
  status_reason  text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON reservation.reservation (guest_id, created_at DESC);
CREATE INDEX ON reservation.reservation (hotel_id, start_date);

CREATE TABLE reservation.status_history (
  id             bigserial PRIMARY KEY,
  reservation_id bigint NOT NULL,
  from_status    text,
  to_status      text NOT NULL,
  reason         text,
  at             timestamptz NOT NULL DEFAULT now()
);
RESET ROLE;

-- ── payment (Payment Service) ───────────────────────────────────────
SET ROLE payment_svc;
CREATE TABLE payment.payment (
  payment_id     serial PRIMARY KEY,
  reservation_id bigint NOT NULL UNIQUE,         -- 결제도 예약 하나에 한 번 (결제 멱등)
  guest_id       int  NOT NULL,
  amount         int  NOT NULL,
  card_last4     text NOT NULL,
  status         text NOT NULL CHECK (status IN ('charged','failed','refunded')),
  notified       boolean NOT NULL DEFAULT false, -- Reservation Service 에 결과를 알렸나
  note           text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now()
);
RESET ROLE;
