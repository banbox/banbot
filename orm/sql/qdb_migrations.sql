-- QuestDB schema migrations.
-- Each migration section starts with: -- version <n>

-- version 1
-- Base schema (idempotent upsert-by-(sid,ts) tables in QuestDB)

create table if not exists kline_1m (
  sid int,
  ts timestamp,
  open double,
  high double,
  low double,
  close double,
  volume double,
  quote double,
  buy_volume double,
  trade_num long
) timestamp(ts) partition by week dedup upsert keys(sid, ts);

create table if not exists kline_5m (
  sid int,
  ts timestamp,
  open double,
  high double,
  low double,
  close double,
  volume double,
  quote double,
  buy_volume double,
  trade_num long
) timestamp(ts) partition by month dedup upsert keys(sid, ts);

create table if not exists kline_15m (
  sid int,
  ts timestamp,
  open double,
  high double,
  low double,
  close double,
  volume double,
  quote double,
  buy_volume double,
  trade_num long
) timestamp(ts) partition by month dedup upsert keys(sid, ts);

create table if not exists kline_1h (
  sid int,
  ts timestamp,
  open double,
  high double,
  low double,
  close double,
  volume double,
  quote double,
  buy_volume double,
  trade_num long
) timestamp(ts) partition by year dedup upsert keys(sid, ts);

create table if not exists kline_1d (
  sid int,
  ts timestamp,
  open double,
  high double,
  low double,
  close double,
  volume double,
  quote double,
  buy_volume double,
  trade_num long
) timestamp(ts) partition by year dedup upsert keys(sid, ts);

-- version 3
-- Migrate relational metadata from SQLite (banpub.db) to QuestDB.
-- All tables use WAL + DEDUP UPSERT for concurrent-safe append-only writes.
-- Logical deletion (is_deleted=true) replaces physical DELETE.

CREATE TABLE IF NOT EXISTS exsymbol_q (
  sid        INT,
  ts         TIMESTAMP,
  exchange   SYMBOL,
  exg_real   SYMBOL,
  market     SYMBOL,
  symbol     SYMBOL,
  combined   BOOLEAN,
  list_ms    LONG,
  delist_ms  LONG,
  is_deleted BOOLEAN,
  deleted_at TIMESTAMP
) TIMESTAMP(ts) PARTITION BY YEAR WAL
DEDUP UPSERT KEYS(sid, ts);

CREATE TABLE IF NOT EXISTS calendars_q (
  ts         TIMESTAMP,
  market     SYMBOL,
  start_ms   LONG,
  stop_ms    LONG,
  is_deleted BOOLEAN,
  deleted_at TIMESTAMP
) TIMESTAMP(ts) PARTITION BY YEAR WAL
DEDUP UPSERT KEYS(market, start_ms, ts);

CREATE TABLE IF NOT EXISTS adj_factors_q (
  ts         TIMESTAMP,
  sid        INT,
  sub_id     INT,
  start_ms   LONG,
  factor     DOUBLE,
  is_deleted BOOLEAN,
  deleted_at TIMESTAMP
) TIMESTAMP(ts) PARTITION BY MONTH WAL
DEDUP UPSERT KEYS(sid, sub_id, start_ms, ts);

CREATE TABLE IF NOT EXISTS sranges_q (
  sid        INT,
  ts         TIMESTAMP,
  tbl        SYMBOL,
  timeframe  SYMBOL,
  start_ms   LONG,
  stop_ms    LONG,
  has_data   BOOLEAN,
  is_deleted BOOLEAN,
  deleted_at TIMESTAMP
) TIMESTAMP(ts) PARTITION BY MONTH WAL
DEDUP UPSERT KEYS(sid, tbl, timeframe, start_ms, ts);

CREATE TABLE IF NOT EXISTS ins_kline_q (
  sid        INT,
  timeframe  SYMBOL,
  ts         TIMESTAMP,
  start_ms   LONG,
  stop_ms    LONG,
  is_deleted BOOLEAN,
  deleted_at TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY WAL
DEDUP UPSERT KEYS(sid, timeframe, ts);

CREATE TABLE IF NOT EXISTS kline_un_q (
  sid        INT,
  timeframe  SYMBOL,
  ts         TIMESTAMP,
  stop_ms    LONG,
  expire_ms  LONG,
  open       DOUBLE,
  high       DOUBLE,
  low        DOUBLE,
  close      DOUBLE,
  volume     DOUBLE,
  quote      DOUBLE,
  buy_volume DOUBLE,
  trade_num  LONG,
  is_deleted BOOLEAN,
  deleted_at TIMESTAMP
) TIMESTAMP(ts) PARTITION BY MONTH WAL
DEDUP UPSERT KEYS(sid, timeframe, ts)
