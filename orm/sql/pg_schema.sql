-- TimescaleDB base schema for banbot v0.3+
-- Uses sranges instead of kinfo+khole; no `info` field in kline/kline_un.
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ----------------------------
-- sranges: non-contiguous covered range tracker (replaces kinfo+khole)
-- ----------------------------
CREATE TABLE IF NOT EXISTS "public"."sranges"
(
    "sid"       int4       NOT NULL,
    "tbl"       text       NOT NULL,
    "timeframe" varchar(5) NOT NULL,
    "start_ms"  int8       NOT NULL,
    "stop_ms"   int8       NOT NULL,
    "has_data"  boolean    NOT NULL DEFAULT true,
    CONSTRAINT "sranges_pkey" UNIQUE ("sid", "tbl", "timeframe", "start_ms")
);
CREATE INDEX IF NOT EXISTS "idx_sranges_sid_tf" ON "public"."sranges" USING btree ("sid", "tbl", "timeframe");

-- ----------------------------
-- kline_un: unfinished (in-progress) bar cache
-- Unified fields: quote, buy_volume, trade_num (no `info`)
-- ----------------------------
CREATE TABLE IF NOT EXISTS "public"."kline_un"
(
    "sid"        int4       NOT NULL,
    "timeframe"  varchar(5) NOT NULL,
    "start_ms"   int8       NOT NULL,
    "stop_ms"    int8       NOT NULL,
    "expire_ms"  int8       NOT NULL DEFAULT 0,
    "open"       float8     NOT NULL,
    "high"       float8     NOT NULL,
    "low"        float8     NOT NULL,
    "close"      float8     NOT NULL,
    "volume"     float8     NOT NULL,
    "quote"      float8     NOT NULL DEFAULT 0,
    "buy_volume" float8     NOT NULL DEFAULT 0,
    "trade_num"  int8       NOT NULL DEFAULT 0,
    CONSTRAINT "kline_un_sid_tf_pkey" UNIQUE ("sid", "timeframe")
);

-- ----------------------------
-- exsymbol: exchange symbol registry
-- ----------------------------
CREATE TABLE IF NOT EXISTS "public"."exsymbol"
(
    "id"        int4        NOT NULL PRIMARY KEY,
    "exchange"  varchar(50) NOT NULL,
    "exg_real"  varchar(50) NOT NULL DEFAULT '',
    "market"    varchar(20) NOT NULL,
    "symbol"    varchar(50) NOT NULL,
    "combined"  boolean     NOT NULL DEFAULT false,
    "list_ms"   int8        NOT NULL DEFAULT 0,
    "delist_ms" int8        NOT NULL DEFAULT 0,
    CONSTRAINT "ix_exsymbol_unique" UNIQUE ("exchange", "market", "symbol")
);

-- ----------------------------
-- calendars: trading calendar (market open/close ranges)
-- `market` field aligned with QuestDB version
-- ----------------------------
CREATE TABLE IF NOT EXISTS "public"."calendars"
(
    "id"       SERIAL      NOT NULL PRIMARY KEY,
    "market"   varchar(50) NOT NULL,
    "start_ms" int8        NOT NULL,
    "stop_ms"  int8        NOT NULL
);
CREATE INDEX IF NOT EXISTS "idx_calendars_market" ON "public"."calendars" USING btree ("market");
CREATE UNIQUE INDEX IF NOT EXISTS "idx_calendars_market_start" ON "public"."calendars" ("market", "start_ms");

-- ----------------------------
-- adj_factors: price adjustment factors (no auto-increment id)
-- ----------------------------
CREATE TABLE IF NOT EXISTS "public"."adj_factors"
(
    "sid"      int4   NOT NULL,
    "sub_id"   int4   NOT NULL,
    "start_ms" int8   NOT NULL,
    "factor"   float8 NOT NULL,
    CONSTRAINT "adj_factors_pkey" UNIQUE ("sid", "sub_id", "start_ms")
);
CREATE INDEX IF NOT EXISTS "idx_adj_factors_sid" ON "public"."adj_factors" USING btree ("sid");

-- ----------------------------
-- ins_kline: in-flight insert job lock table
-- ----------------------------
CREATE TABLE IF NOT EXISTS "public"."ins_kline"
(
    "sid"       int4       NOT NULL,
    "timeframe" varchar(5) NOT NULL,
    "start_ms"  int8       NOT NULL,
    "stop_ms"   int8       NOT NULL,
    CONSTRAINT "ins_kline_sid_tf_pkey" UNIQUE ("sid", "timeframe")
);
