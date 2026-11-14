# opt package

`opt` provides time-series backtesting, hyperparameter/rolling optimization, and report tools. Unified entry separately assembles factor/mixed `backtest`; these time-series optimization APIs do not search factor parameters. See [backtesting](../guide/backtest.md), [optimization](../guide/hyperopt.md), and [rolling backtests](../guide/roll_btopt.md) for CLI use.

## Backtest instances

```go
func NewBackTestWithRuntimeDeps(
    deps biz.RuntimeDeps, isOpt bool, outDir string,
) (*BackTest, *errs.Error)

func NewBackTestLiteWithRuntimeDeps(
    deps biz.RuntimeDeps, isOpt bool, onBar data.FnDataSeries,
    getEnd data.FnGetInt64, pBar *utils.StagedPrg,
) (*BackTestLite, *errs.Error)
```

Supply complete dependencies from the same Runtime; missing required state fails. Legacy `NewBackTest` was removed. `BackTest` exposes `Init()`, `Run()`, `RunContext(context.Context)`, and `FeedDataSeries(*orm.DataSeries)`; the lightweight `FeedDataSeries` returns bool. Private historical-provider and progress fields are not constructor inputs.

Each runner binds its own clock, orders, wallet, and data. `orm.DataSeries.Values map[string]any` carries extension fields and type/NULL semantics; do not narrow input to a fixed OHLCV structure.

## Optimization and factories

```go
type BacktestFactory func(
    snapshot *config.Snapshot, isOpt bool, outDir string,
) (*BackTest, func(), *errs.Error)

func RunOptimize(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error
func RunBTOverOpt(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error
func RunRollBTPicker(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error
func CollectOptLog(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error
```

All four require a Snapshot and isolated factory; old `CmdArgs`-only calls are unsupported. Each factory call creates a Runtime and returns cleanup; release resources after completion and on failure. Derived Snapshots copy intervals, pairs, and policies. Trials must not share mutable account/wallet state.

`RunOptimize` writes an `OutPath` log file and prints selected policy YAML. `RunBTOverOpt` carries balances and accumulates reports across windows. `RunRollBTPicker` writes per-window scores and ranks. `CollectOptLog` sorts and rewrites a single file or recursively collects `.log` files from a directory; see the [optimization guide](../guide/hyperopt.md) for output boundaries.

`OptInfo` holds `Params`, `Score`, direction, trial ID, and `BTResult`; `OptGroup` associates source policy, timeframe, and symbol. `DescGroups(items)` separates nonnegative and negative scores. `AvgGoodDesc(items, startRate, endRate)` averages parameters over a selected rank interval, falling back to negative scores if no nonnegative candidates exist. These are not raw-profit quantiles.

## Reports and result fields

`NewBTResult() *BTResult` creates a result. `NewReportDeps(deps biz.RuntimeDeps) *ReportDeps` binds task configuration, orders, wallet, clock, storage, and logger; installing globals is not a substitute. `DumpLineGraph` is no longer a public opt API.

| Go field / JSON field | Meaning |
| --- | --- |
| `TotProfit` / `totProfit`, `TotProfitPct` / `totProfitPct` | Total profit amount and percentage |
| `WinRatePct` / `winRatePct` | Trade win percentage, not a 0–1 fraction |
| `MaxDrawDownPct` / `maxDrawDownPct` | Maximum drawdown percentage over the full equity path |
| `ShowDrawDownPct` / `showDrawDownPct` | Chart-sampled drawdown, which may differ |
| `SharpeRatio`, `SortinoRatio` | Risk-adjusted return ratios |
| `UlcerIndex`, `UlcerPerformanceIndex` | Pointer metrics; may be null when undefined |
| `DailyEquity` / `dailyEquity` | `[UTC day-end milliseconds, equity]`, daily without decimation |
| `GapExits`, `DelistExits` | Data-gap/delisting forced-exit counts |
| `GapExitPnlShare`, `DelistExitPnlShare` | Absolute profit sum for those exits divided by all orders' absolute profit sum |
| `Liquidated` and related fields | Optional liquidation/bankruptcy evidence; see [backtesting](../guide/backtest.md) |

`PlotData` includes `Labels`, `OdNum`, `JobNum`, `Real`, `Available`, `Profit`, `UnrealizedPOL`, `WithDraw`, and `More`. `RowPart` holds grouped order counts, profit, cost, and risk metrics. `Orders` is `[]*ormo.InOutOrder` and is not serialized to JSON.

`BTResult.Score()` uses total profit percentage and displayed drawdown; optimization `loss` is its negative. Ordinary backtests return an error and nonzero command exit if `detail.json` cannot be written. Missing output is not success. Liquidation ends normally with a result, so exit code alone does not establish profitability.

## Tool boundaries

Use CLI `tool cmp-orders` or `CompareExgBTOrdersWithRuntimeDeps(options CompareOrdersOptions, deps CompareOrdersDeps) error`. Legacy `CompareExgBTOrders(args []string)` is not the current CLI assembly entry. `RegisterFactor(name string, factor FactorFunc)` / `GetFactor` / `SnapshotFactors` manage rolling-selection functions for `tool bt-factor`, separate from the native factor runner.

Factor `events.jsonl`, `resolved.json`, `run.json`, and account audit artifacts are distinct from time-series `orders.gob`; its presence or absence cannot establish factor-task success. See [factor API](./factor.md) and [Runtime API](./runtime.md).
