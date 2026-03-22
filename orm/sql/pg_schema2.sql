-- TimescaleDB kline hypertables for banbot v0.3+
-- Unified columns: no `info` field; uses quote, buy_volume, trade_num instead.

CREATE TABLE IF NOT EXISTS "public"."kline_1m"
(
    "sid"        int4   NOT NULL,
    "time"       int8   NOT NULL,
    "open"       float8 NOT NULL,
    "high"       float8 NOT NULL,
    "low"        float8 NOT NULL,
    "close"      float8 NOT NULL,
    "volume"     float8 NOT NULL,
    "quote"      float8 NOT NULL DEFAULT 0,
    "buy_volume" float8 NOT NULL DEFAULT 0,
    "trade_num"  int8   NOT NULL DEFAULT 0,
    CONSTRAINT "kline_1m_sid_time_pkey" UNIQUE ("sid", "time")
);
CREATE INDEX IF NOT EXISTS "kline_1m_sid_idx" ON "public"."kline_1m" USING btree ("sid");
SELECT create_hypertable('kline_1m', by_range('time', 5184000000), if_not_exists => TRUE);
ALTER TABLE "public"."kline_1m" SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time DESC',
    timescaledb.compress_segmentby = 'sid'
);
SELECT add_compression_policy('kline_1m', 5184000000, if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS "public"."kline_5m"
(
    "sid"        int4   NOT NULL,
    "time"       int8   NOT NULL,
    "open"       float8 NOT NULL,
    "high"       float8 NOT NULL,
    "low"        float8 NOT NULL,
    "close"      float8 NOT NULL,
    "volume"     float8 NOT NULL,
    "quote"      float8 NOT NULL DEFAULT 0,
    "buy_volume" float8 NOT NULL DEFAULT 0,
    "trade_num"  int8   NOT NULL DEFAULT 0,
    CONSTRAINT "kline_5m_sid_time_pkey" UNIQUE ("sid", "time")
);
CREATE INDEX IF NOT EXISTS "kline_5m_sid_idx" ON "public"."kline_5m" USING btree ("sid");
SELECT create_hypertable('kline_5m', by_range('time', 7776000000), if_not_exists => TRUE);
ALTER TABLE "public"."kline_5m" SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time DESC',
    timescaledb.compress_segmentby = 'sid'
);
SELECT add_compression_policy('kline_5m', 5184000000, if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS "public"."kline_15m"
(
    "sid"        int4   NOT NULL,
    "time"       int8   NOT NULL,
    "open"       float8 NOT NULL,
    "high"       float8 NOT NULL,
    "low"        float8 NOT NULL,
    "close"      float8 NOT NULL,
    "volume"     float8 NOT NULL,
    "quote"      float8 NOT NULL DEFAULT 0,
    "buy_volume" float8 NOT NULL DEFAULT 0,
    "trade_num"  int8   NOT NULL DEFAULT 0,
    CONSTRAINT "kline_15m_sid_time_pkey" UNIQUE ("sid", "time")
);
CREATE INDEX IF NOT EXISTS "kline_15m_sid_idx" ON "public"."kline_15m" USING btree ("sid");
SELECT create_hypertable('kline_15m', by_range('time', 31536000000), if_not_exists => TRUE);
ALTER TABLE "public"."kline_15m" SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time DESC',
    timescaledb.compress_segmentby = 'sid'
);
SELECT add_compression_policy('kline_15m', 7776000000, if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS "public"."kline_1h"
(
    "sid"        int4   NOT NULL,
    "time"       int8   NOT NULL,
    "open"       float8 NOT NULL,
    "high"       float8 NOT NULL,
    "low"        float8 NOT NULL,
    "close"      float8 NOT NULL,
    "volume"     float8 NOT NULL,
    "quote"      float8 NOT NULL DEFAULT 0,
    "buy_volume" float8 NOT NULL DEFAULT 0,
    "trade_num"  int8   NOT NULL DEFAULT 0,
    CONSTRAINT "kline_1h_sid_time_pkey" UNIQUE ("sid", "time")
);
CREATE INDEX IF NOT EXISTS "kline_1h_sid_idx" ON "public"."kline_1h" USING btree ("sid");
SELECT create_hypertable('kline_1h', by_range('time', 63072000000), if_not_exists => TRUE);
ALTER TABLE "public"."kline_1h" SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time DESC',
    timescaledb.compress_segmentby = 'sid'
);
SELECT add_compression_policy('kline_1h', 15552000000, if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS "public"."kline_1d"
(
    "sid"        int4   NOT NULL,
    "time"       int8   NOT NULL,
    "open"       float8 NOT NULL,
    "high"       float8 NOT NULL,
    "low"        float8 NOT NULL,
    "close"      float8 NOT NULL,
    "volume"     float8 NOT NULL,
    "quote"      float8 NOT NULL DEFAULT 0,
    "buy_volume" float8 NOT NULL DEFAULT 0,
    "trade_num"  int8   NOT NULL DEFAULT 0,
    CONSTRAINT "kline_1d_sid_time_pkey" UNIQUE ("sid", "time")
);
CREATE INDEX IF NOT EXISTS "kline_1d_sid_idx" ON "public"."kline_1d" USING btree ("sid");
SELECT create_hypertable('kline_1d', by_range('time', 94608000000), if_not_exists => TRUE);
ALTER TABLE "public"."kline_1d" SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time DESC',
    timescaledb.compress_segmentby = 'sid'
);
SELECT add_compression_policy('kline_1d', 94608000000, if_not_exists => TRUE);
