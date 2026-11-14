# Go API reference

Start with [strat](strat.md) to write strategies, [orm](orm.md) and [data](data.md) to add data, and [factor](factor.md) for portfolios and statistical pairs. Ordinary users configure YAML and CLI commands without assembling Runtimes themselves.

## Find an API by purpose

| Package | Purpose |
| --- | --- |
| [strat](strat.md) | Strategies, StratJob, data callbacks and order intents |
| [orm](orm.md) | DataSeries, arbitrary fields, storage and order models |
| [data](data.md) | Sources, subscriptions, historical replay and live data |
| [factor](factor.md) | Factor graphs, expressions, portfolios, research and runners |
| [biz](biz.md) | Order managers, wallets and trading events |
| [goods](goods.md) | Symbol filters and refresh |
| [exg](exg.md) | Unified banexg exchange interfaces and construction |
| [btime](btime.md) | Simulation/live clocks and time conversion |
| [config](config.md) | YAML, RunSpec, snapshots and path resolution |
| [runtime](runtime.md) | Process/Runtime lifecycle and dependency injection |
| [core](core.md) | Run state, constants and parameter types |
| [com](com.md) | Market prices, progress and schedulers |
| [opt](opt.md) | Backtesting, optimization, rolling replay and reports |
| [live](live.md) | Live lifecycle and remote-control boundaries |
| [rpc](rpc.md) | Task notification sessions, channels and templates |
| [web](web.md) | Development server, live monitoring and authentication |
| [entry](entry.md) | Command extensions, engine routing and resource setup |
| [utils](utils.md) | File, collection, timeframe and metric utilities |

## Embedding and compatibility APIs

Each backtest, live or optimization task has its own `runtime.Runtime` with a configuration snapshot, clock, market/symbol state, strategies, orders, wallets, subscriptions and notifications. When embedding several tasks, pass dependencies through constructors, receivers or `BizDeps()`/`DataDeps()` projections. Use `context.Context` for cancellation and deadlines.

Package-level configuration, clock, order and strategy helpers retained in these pages are compatibility entry points. New multi-task code should not use them to find a “current task.” Prefer the `StratJob` and strategy-bound dependencies inside callbacks. Release borrowed storage, exchanges and shared accounts according to their owner's lifecycle.

`execution` manages shared accounts, logical ownership, budgets, net execution and recovery. See [Live trading](../guide/live_trading.md) and [Factor guide](../guide/factor.md) for user operations. Do not bypass account execution by calling exchange write APIs directly.

Signatures follow the Banbot version referenced by your strategy project. Rebuild after upgrading; for mismatches, check the dependency version and the corresponding API page.
