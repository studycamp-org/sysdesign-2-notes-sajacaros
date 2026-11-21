-- ch06 광고 클릭 집계 결과 저장소
--   agg_queue (Kafka 엔진, agg-results 구독) ──MV──▶ ad_clicks
--
-- 집계 서비스는 배치마다 (윈도, 광고)별 '증가분'을 보낸다. 이 테이블은 더하기만 한다.
--   그래서 같은 결과가 두 번 들어오면 값이 실제로 커진다 (교재 그림 19 의 중복이 그대로 보인다).
--   질의는 늘 sum(clicks). 재집계 검증의 보정도 차이만큼의 증가분 행이다.
--
-- kind  : 'event' 이벤트 시각 윈도 | 'proc' 처리 시각 윈도 (그림 12 비교용)
-- source: 'stream' 실시간 | 'recon' 재집계 검증 보정 | 'recalc' 재계산

CREATE DATABASE IF NOT EXISTS adclick;

-- librdkafka 기본값이 read_committed 라 커밋된 트랜잭션만 읽는다 (중단된 트랜잭션의 결과는 안 보인다)
CREATE TABLE adclick.agg_queue
(
    kind         LowCardinality(String),
    window_start DateTime('UTC'),
    ad_id        String,
    clicks       Int64,
    source       LowCardinality(String),
    emitted_at   DateTime64(3, 'UTC')
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'agg-results',
         kafka_group_name = 'clickhouse',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_flush_interval_ms = 500;

CREATE TABLE adclick.ad_clicks
(
    kind         LowCardinality(String),
    window_start DateTime('UTC'),
    ad_id        String,
    clicks       Int64,
    source       LowCardinality(String),
    emitted_at   DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (kind, window_start, ad_id);

CREATE MATERIALIZED VIEW adclick.mv_ad_clicks TO adclick.ad_clicks AS
SELECT kind, window_start, ad_id, clicks, source, emitted_at FROM adclick.agg_queue;
