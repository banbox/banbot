# opt Package

The opt package provides strategy optimization functionality.

Backtest and optimization runners receive one Runtime's dependencies through `NewBackTestLiteWithRuntimeDeps` and `NewBackTestWithRuntimeDeps`. Each runner uses its own clock, strategies, orders, wallets, and data projection; optimization iterations also create and release their state with a Runtime lifecycle.

## Main Structures

### BackTest
Backtest instance structure.

Fields:
- `Trader biz.Trader` - Trader interface implementation
- `BTResult *BTResult` - Backtest result
- `lastDumpMs int64` - Last timestamp when backtest status was saved
- `dp *data.HistProvider` - Historical data provider
- `isOpt bool` - Whether in hyperparameter optimization mode
- `PBar *utils.StagedPrg` - Progress bar

### BTResult
Backtest result structure.

Fields:
- `MaxOpenOrders int` - Maximum number of concurrent open orders
- `MinReal float64` - Minimum assets
- `MaxReal float64` - Maximum assets
- `MaxDrawDownPct float64` - Maximum drawdown percentage
- `ShowDrawDownPct float64` - Displayed maximum drawdown percentage
- `MaxDrawDownVal float64` - Maximum drawdown value
- `ShowDrawDownVal float64` - Displayed maximum drawdown value
- `BarNum int` - Number of candlesticks
- `TimeNum int` - Number of time periods
- `OrderNum int` - Number of orders
- `Plots *PlotData` - Plot data
- `StartMS int64` - Start timestamp (milliseconds)
- `EndMS int64` - End timestamp (milliseconds)
- `PlotEvery int` - Plot interval
- `TotalInvest float64` - Total investment amount
- `OutDir string` - Output directory
- `TotProfit float64` - Total profit
- `TotCost float64` - Total cost
- `TotFee float64` - Total fees
- `TotProfitPct float64` - Total profit percentage
- `WinRatePct float64` - Win rate
- `SharpeRatio float64` - Sharpe ratio
- `SortinoRatio float64` - Sortino ratio

### PlotData
Plot data structure.

Fields:
- `Labels []string` - Time labels
- `OdNum []int` - Number of orders
- `JobNum []int` - Number of jobs
- `Real []float64` - Actual assets
- `Available []float64` - Available assets
- `Profit []float64` - Realized profit
- `UnrealizedPOL []float64` - Unrealized profit/loss
- `WithDraw []float64` - Withdrawal amount

### RowPart
Backtest statistics row data structure.

Fields:
- `WinCount int` - Number of profitable orders
- `OrderNum int` - Total number of orders
- `ProfitSum float64` - Total profit amount
- `ProfitPctSum float64` - Total profit percentage
- `CostSum float64` - Total cost
- `Durations []int` - List of holding durations
- `Orders []*InOutOrder` - List of orders
- `Sharpe float64` - Sharpe ratio
- `Sortino float64` - Sortino ratio

## Main Features

### NewBackTestWithRuntimeDeps

Signature: NewBackTestWithRuntimeDeps(deps biz.RuntimeDeps, isOpt bool, outDir string) (*BackTest, *errs.Error). NewBackTest was removed. Pass complete dependencies from one Runtime; missing state fails. Lightweight replay uses NewBackTestLiteWithRuntimeDeps.

### RunBTOverOpt

Signature: RunBTOverOpt(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error. The entry injects the snapshot and isolated backtest factory; CmdArgs alone is insufficient. This is the TS optimization/report path, not factor/mixed parameter search.

### RunRollBTPicker

Signature: RunRollBTPicker(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error. The entry injects the snapshot and isolated backtest factory; CmdArgs alone is insufficient. This is the TS optimization/report path, not factor/mixed parameter search.

### RunOptimize

Signature: RunOptimize(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error. The entry injects the snapshot and isolated backtest factory; CmdArgs alone is insufficient. This is the TS optimization/report path, not factor/mixed parameter search.

### CollectOptLog

Signature: CollectOptLog(args *config.CmdArgs, snapshot *config.Snapshot, factory BacktestFactory) *errs.Error. The entry injects the snapshot and isolated backtest factory; CmdArgs alone is insufficient. This is the TS optimization/report path, not factor/mixed parameter search.

### NewBTResult
Create a new backtest result instance.

Returns:
- `*BTResult` - Backtest result instance pointer

### AvgGoodDesc
Calculate average optimization results within specified return rate range.

Parameters:
- `items []*OptInfo` - Optimization information list
- `startRate float64` - Start return rate
- `endRate float64` - End return rate

Returns:
- `*OptInfo` - Average optimization information

### DescGroups
Group optimization results by return rate.

Parameters:
- `items []*OptInfo` - Optimization information list

Returns:
- `[]*OptInfo, []*OptInfo` - Good group and bad group optimization information lists

### CompareExgBTOrders
Compare exchange backtest orders.

Parameters:
- `args []string` - Command line argument list

## Factor-engine integration

Optimization factories retain isolated Runtime ownership; factor JSON lines/account audit Gob differ from legacy orders.gob.

[Factor API](factor.md) / [Guide](../guide/factor.md)


## Factories, reports and resources

BacktestFactory is func(snapshot *config.Snapshot, isOpt bool, outDir string) (*BackTest, func(), *errs.Error); cleanup releases only that run's owned state. Derived snapshots copy ranges/pairs/policies without sharing mutable execution accounts. NewReportDeps(biz.RuntimeDeps) binds reports to orders/clock/symbols/storage/logger, without installing globals. DumpLineGraph is no longer a public opt API. See [runtime](runtime.md).
