# opt Package

The opt package provides strategy optimization functionality.

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

### NewBackTest
Create a new backtest instance.

Parameters:
- `isOpt bool` - Whether in hyperparameter optimization mode
- `outDir string` - Output directory path

Returns:
- `*BackTest` - Backtest instance pointer

### RunBTOverOpt
Run backtest mode based on continuous parameter tuning, approximating live trading conditions and avoiding using future information for parameter tuning.

Parameters:
- `args *config.CmdArgs` - Command line argument configuration

Returns:
- `*errs.Error` - Error information

### RunRollBTPicker
Execute rolling backtest stock picker.

Parameters:
- `args *config.CmdArgs` - Command line argument configuration

Returns:
- `*errs.Error` - Error information

### RunOptimize
Execute strategy parameter optimization.

Parameters:
- `args *config.CmdArgs` - Command line argument configuration

Returns:
- `*errs.Error` - Error information

### CollectOptLog
Collect and analyze optimization logs.

Parameters:
- `args *config.CmdArgs` - Command line argument configuration

Returns:
- `*errs.Error` - Error information

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

### DumpLineGraph
Generate and save line graph.

Parameters:
- `path string` - Output file path
- `title string` - Chart title
- `label []string` - Label list
- `prec float64` - Precision
- `tplData []byte` - Template data
- `items []*ChartDs` - Chart datasets

Returns:
- `*errs.Error` - Error information

### CompareExgBTOrders
Compare exchange backtest orders.

Parameters:
- `args []string` - Command line argument list 