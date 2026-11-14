# Frequently asked questions

## How do I upgrade Banbot?

Choose a target version in your strategy project, update dependencies and rebuild:

```shell
go get github.com/banbox/banbot@VERSION
go mod tidy
go build -o bot
```

Replace `VERSION` with the version you intend to use. Editing `go.mod` or running `go mod tidy` does not replace an already deployed executable. Preserve configuration, databases and execution ledgers, and check [Configuration](./configuration.md) and strategy API compatibility. The current source requires Go 1.26.0 and declares toolchain Go 1.26.8.

For Docker, update containers using your selected image version while retaining data volumes. Follow that deployment project's build and startup instructions.

## Why do I need a strategy project?

Go strategies compile together with Banbot into one executable. Use an example project or [initialize your own](./init_project.md). Rebuild after changing Go strategies. Factor expressions and portfolio settings may also be defined in YAML; see [Multi-factor strategies](./factor.md).

## Which strategies and markets are supported?

- Time-series strategies: main/auxiliary timeframe and custom-series events across symbols, timeframes and accounts.
- Factor strategies: cross-sectional factors, portfolio research, weights/events backtests, statistical pairs and mixed time-series/factor events replay.
- Live trading: ordinary time-series and factor/mixed paths have different setup requirements. Real factor/mixed trading requires current-session data, execution and reconciliation verification; see [Live trading](./live_trading.md).

Built-in exchange entry points are Binance, OKX and Bybit. Market-data and historical replay capabilities come from banexg adapters. Default Hub live execution currently accepts only linear perpetual accounts in `prod`/`dry_run`; see [Shared accounts and Hub](./execution.md). Statistical pairs do not imply ready-made triangular, cross-exchange or high-frequency strategies. China-market design documents do not constitute delivered stock/futures integration.

## Do backtests and downloads require API keys?

Public-market-data backtests and downloads do not require trading credentials. Real trading requires credentials for the chosen account/environment. Complete a [first backtest](./quick_local.md) before configuring live credentials.

## Why does WebUI require a password in Docker?

`bot web` defaults to `127.0.0.1:8000` locally and `0.0.0.0` in Docker. Binding to `0.0.0.0` or `::` requires `--password`; the Basic Auth username is always `banbot`. Development UI credentials are separate from live Dashboard users.

## Can I open shorts or limit order counts?

Use `s.OpenOrder(&strat.EnterReq{Tag: "short", Short: true})` on a contract market that supports shorts. The spot path rejects short entries.

Orders are constrained by available funds, `max_open_orders`, account limits and strategy `EachMaxLong`/`EachMaxShort` settings. Accepting a signal does not mean the order has filled; the order manager and exchange determine execution.

## Can I exit only part of a position?

Inside a strategy callback:

```go
s.CloseOrders(&strat.ExitReq{Tag: "close", ExitRate: 0.5, FilledOnly: true})
```

Use `OrderID` to restrict the request to one order. See [Strategy API](../api/strat.md) for fields and return values.

## Do configuration changes require a restart?

YAML is not an automatic hot-reload interface; edits normally require a restart. Dynamic symbol changes and remote controls use their own APIs. Order synchronization, shared-account recovery and takeover have different requirements. Retain configuration, order databases, execution ledgers and checkpoints; current exchange positions alone cannot identify every strategy's ownership. See [Live trading](./live_trading.md).

## How do backtests, real-time simulation and historical replay differ?

For ordinary time-series strategies, `env: dry_run` consumes real-time data and simulates fills locally. For factor/mixed configurations, `trade --dry-run` runs historical events replay. Factor `backtest --mode weights` is for weight research; mixed replay requires events. See [Backtesting](./backtest.md).

## Why do live and backtest orders differ?

Check logs, effective configuration, universe, timezone, timeframes, leverage and stake. Then compare market data, fees, slippage, funding and fill conditions. OHLCV matching cannot reproduce every order-book detail, network delay or real fill sequence. For external data, also check historical availability times and revisions.

## Why is downloaded or written data not immediately visible?

Check database connectivity, logs and `database.max_pool_size`. QuestDB WAL writes become visible asynchronously; successful writes do not imply immediate reads. Do not clear data or replace tables because of one empty read. To repair K-line coverage metadata, use `kline repair-ranges` from [Data tools](../advanced/kline_tools.md) after checking stored ranges.

## Why does SQLite report NOT NULL for a float64 field?

NaN/Inf may become NULL when written to SQLite. Validate fields that require finite numbers, or apply `utils.NanInfTo(v, 0)` before writing. Explicit NULL and missing values in custom time-series fields have distinct meanings; do not replace all of them with zero.

## Why do Go dependency downloads fail with unexpected EOF?

Check `GOPROXY`, connectivity and proxy nodes; retry with a reachable source or node. Distinguish dependency download failures from exchange connectivity at runtime.

## What information helps with other questions?

Check [Commands](./bot_usage.md) and [API](../api/index.md). For issues or community questions, include the version, command, redacted configuration, error logs and a minimal reproduction. Do not publish API keys, passwords or tokens.
