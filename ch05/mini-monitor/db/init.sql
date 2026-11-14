-- TimescaleDB: PostgreSQL 확장으로 동작하는 시계열 DB
-- 교재의 보관 정책(7일 원본 → 30일 1분 → 1년 1시간)을 실습용으로 줄였다:
--   원본 6시간 · 1분 롤업 2일 · 1시간 롤업 30일. 청크 10분, 15분 지난 청크는 압축.
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 시계열 = 이름 + 레이블 집합 + (값, 타임스탬프). host 는 가장 자주 거르는 레이블이라 열로 뺐다.
CREATE TABLE metrics (
    time   timestamptz      NOT NULL,
    name   text             NOT NULL,
    host   text             NOT NULL,
    labels jsonb            NOT NULL DEFAULT '{}',
    value  double precision NOT NULL
);
-- 하이퍼테이블: 겉은 테이블 하나, 안은 10분짜리 청크로 자동 분할
SELECT create_hypertable('metrics', by_range('time', INTERVAL '10 minutes'));
CREATE INDEX ON metrics (name, host, time DESC);
CREATE INDEX ON metrics USING gin (labels);

-- ① 인코딩과 압축: 같은 (name, host) 끼리 묶어 시간 순으로 컬럼형 압축 (델타·delta-of-delta·XOR 인코딩)
ALTER TABLE metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'name, host',
    timescaledb.compress_orderby   = 'time DESC'
);
SELECT add_compression_policy('metrics', INTERVAL '15 minutes', schedule_interval => INTERVAL '5 minutes');

-- ② 다운샘플링: 연속 집계. 평균만 남기면 스파이크가 사라지므로 min/max/count 도 남긴다
CREATE MATERIALIZED VIEW metrics_1m WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket('1 minute', time) AS bucket, name, host,
       avg(value) AS avg, min(value) AS min, max(value) AS max, count(*) AS n
FROM metrics GROUP BY bucket, name, host WITH NO DATA;
SELECT add_continuous_aggregate_policy('metrics_1m',
    start_offset => INTERVAL '1 hour', end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '1 minute');

-- 1시간 롤업은 1분 롤업 위에 다시 쌓는다(계층형 연속 집계)
CREATE MATERIALIZED VIEW metrics_1h WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket('1 hour', bucket) AS bucket, name, host,
       sum(avg * n) / sum(n) AS avg, min(min) AS min, max(max) AS max, sum(n) AS n
FROM metrics_1m GROUP BY 1, name, host WITH NO DATA;
SELECT add_continuous_aggregate_policy('metrics_1h',
    start_offset => INTERVAL '1 day', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '5 minutes');

-- ③ 보관 기간: 오래된 청크를 통째로 지운다 (DELETE 가 아니라 청크 drop 이라 싸다)
SELECT add_retention_policy('metrics',    INTERVAL '6 hours');
SELECT add_retention_policy('metrics_1m', INTERVAL '2 days');
SELECT add_retention_policy('metrics_1h', INTERVAL '30 days');

-- 경보 저장소 (교재는 Cassandra 같은 KV. 여기선 같은 PostgreSQL 에 둔다)
-- 상태: inactive → pending → firing → resolved
CREATE TABLE alerts (
    rule           text        NOT NULL,
    host           text        NOT NULL,
    severity       text        NOT NULL,
    state          text        NOT NULL,
    value          double precision,
    summary        text,
    pending_since  timestamptz,
    fired_at       timestamptz,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    notified_state text,                       -- 마지막으로 Kafka 에 넘긴 상태 (최소 한 번 재시도용)
    PRIMARY KEY (rule, host)
);

-- 경보 소비자가 채널로 "보낸" 기록
CREATE TABLE notifications (
    id        bigserial PRIMARY KEY,
    sent_at   timestamptz NOT NULL DEFAULT now(),
    host      text NOT NULL,
    state     text NOT NULL,
    channel   text NOT NULL,
    message   text NOT NULL,
    alert_key text NOT NULL,
    UNIQUE (alert_key, channel)               -- 같은 알림이 두 번 와도 한 번만 남는다(중복 제거)
);
