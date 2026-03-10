CREATE TABLE IF NOT EXISTS task
(
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    mode      TEXT    NOT NULL, -- backtest, inner
    args      TEXT    NOT NULL,
    config    TEXT    NOT NULL,
    path      TEXT    NOT NULL,
    strats    TEXT    NOT NULL,
    periods   TEXT    NOT NULL,
    pairs     TEXT    NOT NULL,
    create_at INTEGER NOT NULL,
    start_at  INTEGER NOT NULL,
    stop_at   INTEGER NOT NULL,
    status    INTEGER NOT NULL,
    progress  REAL    NOT NULL DEFAULT 0,
    order_num INTEGER NOT NULL,
    profit_rate REAL  NOT NULL,
    win_rate  REAL    NOT NULL,
    max_drawdown REAL NOT NULL,
    sharpe    REAL    NOT NULL,
    info      TEXT    NOT NULL, -- 存放不需检索的信息
    note      TEXT    NOT NULL
);
