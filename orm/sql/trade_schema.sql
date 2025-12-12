-- ----------------------------
-- Table structure for bottask
-- ----------------------------
--DROP TABLE IF EXISTS bottask;
CREATE TABLE bottask
(
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    mode      TEXT    NOT NULL,
    name      TEXT    NOT NULL,
    create_at INTEGER NOT NULL,
    start_at  INTEGER NOT NULL,
    stop_at   INTEGER NOT NULL,
    info      TEXT    NOT NULL
);

-- ----------------------------
-- Table structure for exorder
-- ----------------------------
--DROP TABLE IF EXISTS exorder;
CREATE TABLE exorder
(
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id    INTEGER NOT NULL,
    inout_id   INTEGER NOT NULL,
    symbol     TEXT    NOT NULL,
    enter      BOOL    NOT NULL,
    order_type TEXT    NOT NULL,
    order_id   TEXT    NOT NULL,
    side       TEXT    NOT NULL,
    create_at  INTEGER NOT NULL,
    price      REAL    NOT NULL,
    average    REAL    NOT NULL,
    quantity   REAL    NOT NULL,
    filled     REAL    NOT NULL,
    status     INTEGER NOT NULL,
    fee        REAL    NOT NULL,
    fee_quote  REAL    NOT NULL,
    fee_type   TEXT    NOT NULL,
    update_at  INTEGER NOT NULL
);

CREATE INDEX idx_od_inout_id ON exorder (inout_id);
CREATE INDEX idx_od_status   ON exorder (status);
CREATE INDEX idx_od_task_id  ON exorder (task_id);

-- ----------------------------
-- Table structure for iorder
-- ----------------------------
--DROP TABLE IF EXISTS iorder;
CREATE TABLE iorder
(
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     INTEGER NOT NULL,
    symbol      TEXT    NOT NULL,
    sid         INTEGER NOT NULL,
    timeframe   TEXT    NOT NULL,
    short       BOOL    NOT NULL,
    status      INTEGER NOT NULL,
    enter_tag   TEXT    NOT NULL,
    stop        REAL    NOT NULL,
    init_price  REAL    NOT NULL,
    quote_cost  REAL    NOT NULL,
    exit_tag    TEXT    NOT NULL,
    leverage    REAL    NOT NULL,
    enter_at    INTEGER NOT NULL, -- Think carefully before updating this, as it would change the Key
    exit_at     INTEGER NOT NULL,
    strategy    TEXT    NOT NULL,
    stg_ver     INTEGER NOT NULL,
    max_pft_rate REAL    NOT NULL,
    max_draw_down REAL  NOT NULL,
    profit_rate REAL    NOT NULL,
    profit      REAL    NOT NULL,
    info        TEXT    NOT NULL
);

CREATE INDEX idx_io_status  ON iorder (status);
CREATE INDEX idx_io_task_id ON iorder (task_id);
