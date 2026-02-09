-- QuestDB schema migrations.
-- Each migration section starts with: -- version <n>

-- version 1
-- Base schema (idempotent upsert-by-(sid,ts) tables in QuestDB)

create table if not exists kline_1m (
  sid int,
  ts timestamp,
  time long,
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
  time long,
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
  time long,
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
  time long,
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
  time long,
  open double,
  high double,
  low double,
  close double,
  volume double,
  quote double,
  buy_volume double,
  trade_num long
) timestamp(ts) partition by year dedup upsert keys(sid, ts);

