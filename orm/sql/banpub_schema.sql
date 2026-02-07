-- BanPub SQLite schema (public relational/meta tables + UI task)
-- Note: keep idempotent (IF NOT EXISTS) to support upgrades without dropping data.

-- ----------------------------
-- Table: exsymbol
-- ----------------------------
CREATE TABLE IF NOT EXISTS exsymbol
(
    id        INTEGER PRIMARY KEY,
    exchange  TEXT    NOT NULL,
    exg_real  TEXT    NOT NULL DEFAULT '',
    market    TEXT    NOT NULL,
    symbol    TEXT    NOT NULL,
    combined  INTEGER NOT NULL DEFAULT 0,
    list_ms   INTEGER NOT NULL DEFAULT 0,
    delist_ms INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_exs_exchange ON exsymbol (exchange);
CREATE INDEX IF NOT EXISTS idx_exs_exg_market_symbol ON exsymbol (exchange, market, symbol);

-- ----------------------------
-- Table: calendars
-- ----------------------------
CREATE TABLE IF NOT EXISTS calendars
(
    id       INTEGER PRIMARY KEY,
    name     TEXT    NOT NULL,
    start_ms INTEGER NOT NULL,
    stop_ms  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cal_name_start ON calendars (name, start_ms);
CREATE INDEX IF NOT EXISTS idx_cal_name_stop  ON calendars (name, stop_ms);

-- ----------------------------
-- Table: adj_factors
-- ----------------------------
CREATE TABLE IF NOT EXISTS adj_factors
(
    id       INTEGER PRIMARY KEY,
    sid      INTEGER NOT NULL,
    sub_id   INTEGER NOT NULL DEFAULT 0,
    start_ms INTEGER NOT NULL,
    factor   REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_adj_sid_start ON adj_factors (sid, start_ms);
CREATE INDEX IF NOT EXISTS idx_adj_subid_start ON adj_factors (sub_id, start_ms);

-- ----------------------------
-- Table: sranges (downloaded / no-data ranges)
-- ----------------------------
CREATE TABLE IF NOT EXISTS sranges
(
    id          INTEGER PRIMARY KEY,
    sid         INTEGER NOT NULL,
    tbl         TEXT    NOT NULL,
    timeframe   TEXT    NOT NULL,
    start_ms    INTEGER NOT NULL,
    stop_ms     INTEGER NOT NULL,
    has_data    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_srange_sid_tbl_tf_start ON sranges (sid, tbl, timeframe, start_ms);
CREATE INDEX IF NOT EXISTS idx_srange_sid_tbl_tf_stop  ON sranges (sid, tbl, timeframe, stop_ms);
CREATE INDEX IF NOT EXISTS idx_srange_sid_hasdata ON sranges (sid, has_data);

-- ----------------------------
-- Table: ins_kline (best-effort lock + recovery)
-- ----------------------------
CREATE TABLE IF NOT EXISTS ins_kline
(
    id         INTEGER PRIMARY KEY,
    sid        INTEGER NOT NULL,
    timeframe  TEXT    NOT NULL,
    start_ms   INTEGER NOT NULL,
    stop_ms    INTEGER NOT NULL,
    created_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ins_sid_tf ON ins_kline (sid, timeframe);
CREATE INDEX IF NOT EXISTS idx_ins_created ON ins_kline (created_ms);

-- ----------------------------
-- Table: kline_un (unfinished candle per sid+timeframe)
-- ----------------------------
CREATE TABLE IF NOT EXISTS kline_un
(
    sid       INTEGER NOT NULL,
    timeframe TEXT    NOT NULL,
    start_ms  INTEGER NOT NULL,
    stop_ms   INTEGER NOT NULL,
    expire_ms INTEGER NOT NULL,
    open      REAL    NOT NULL,
    high      REAL    NOT NULL,
    low       REAL    NOT NULL,
    close     REAL    NOT NULL,
    volume    REAL    NOT NULL,
    quote     REAL    NOT NULL,
    buy_volume REAL   NOT NULL,
    trade_num INTEGER NOT NULL,
    PRIMARY KEY (sid, timeframe)
);

-- ----------------------------
-- Table: task (UI backtest task)
-- ----------------------------
CREATE TABLE IF NOT EXISTS task
(
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    mode         TEXT    NOT NULL, -- backtest, inner
    args         TEXT    NOT NULL,
    config       TEXT    NOT NULL,
    path         TEXT    NOT NULL,
    strats       TEXT    NOT NULL,
    periods      TEXT    NOT NULL,
    pairs        TEXT    NOT NULL,
    create_at    INTEGER NOT NULL,
    start_at     INTEGER NOT NULL,
    stop_at      INTEGER NOT NULL,
    status       INTEGER NOT NULL,
    progress     REAL    NOT NULL DEFAULT 0,
    order_num    INTEGER NOT NULL,
    profit_rate  REAL    NOT NULL,
    win_rate     REAL    NOT NULL,
    max_drawdown REAL    NOT NULL,
    sharpe       REAL    NOT NULL,
    info         TEXT    NOT NULL, -- 存放不需检索的信息
    note         TEXT    NOT NULL
);
