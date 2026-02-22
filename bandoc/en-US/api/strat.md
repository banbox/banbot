# strat Package

The strat package provides trading strategy-related functionality definitions and implementations.

## Main Structures

### TradeStrat
Core structure for trading strategy, defines basic properties and behaviors of a strategy.

Public fields:
- `Name string` - Strategy name
- `Version int` - Strategy version number
- `WarmupNum int` - Number of candlesticks needed for warmup
- `MinTfScore float64` - Minimum time frame quality, default 0.75
- `WsSubs map[string]string` - WsSubs        map[string]string    // websocket subscription: core.WsSubKLine, core.WsSubTrade, core.WsSubDepth
- `DrawDownExit bool` - Whether to enable drawdown exit
- `HedgeOff bool` - turn off future hedge mode
- `BatchInOut bool` - Whether to batch execute entry/exit
- `BatchInfo bool` - Whether to perform batch processing after OnInfoBar
- `StakeRate float64` - Relative basic amount billing rate
- `StopLoss float64` - Stop loss rate for all orders opened by this strategy (without leverage)
- `StopEnterBars int` - Timeout candlestick number for limit entry orders
- `EachMaxLong int` - Maximum number of long orders per trading pair, -1 to disable
- `EachMaxShort int` - Maximum number of short orders per trading pair, -1 to disable
- `AllowTFs []string` - Allowed running time periods, use global configuration when not provided
- `Outputs []string` - Content of the text file output by the strategy, where each string is one line
- `Policy *config.RunPolicyConfig` - Strategy running configuration

### StratJob
Strategy task instance, responsible for executing specific trading operations.

Public fields:
- `Strat *TradeStrat` - Parent strategy
- `Env *ta.BarEnv` - Indicator environment
- `Entrys []*EnterReq` - Entry request list
- `Exits []*ExitReq` - Exit request list
- `LongOrders []*ormo.InOutOrder` - Long order list
- `ShortOrders []*ormo.InOutOrder` - Short order list
- `Symbol *orm.ExSymbol` - Currently running currency
- `TimeFrame string` - Current running time frame
- `Account string` - Account to which the current task belongs
- `TPMaxs map[int64]float64` - Price at maximum profit of the order
- `OrderNum int` - Total number of unfinished orders
- `EnteredNum int` - Number of fully/partially entered orders
- `CheckMS int64` - Last timestamp of signal processing, 13-digit milliseconds
- `MaxOpenLong int` - Maximum number of long positions, 0 for unlimited, -1 to disable
- `MaxOpenShort int` - Maximum number of short positions, 0 for unlimited, -1 to disable
- `CloseLong bool` - Whether to allow closing long positions
- `CloseShort bool` - Whether to allow closing short positions
- `ExgStopLoss bool` - Whether to allow exchange stop loss
- `LongSLPrice float64` - Default long stop loss price when opening position
- `ShortSLPrice float64` - Default short stop loss price when opening position
- `ExgTakeProfit bool` - Whether to allow exchange take profit
- `LongTPPrice float64` - Default long take profit price when opening position
- `ShortTPPrice float64` - Default short take profit price when opening position
- `IsWarmUp bool` - Whether currently in warmup state
- `More interface{}` - Additional information for strategy customization

### JobEnv
Represents a job for additional product data in OnBatchInfos.

Public fields:
- `Job *StratJob` - Strategy job instance
- `Env *ta.BarEnv` - Indicator runtime environment
- `Symbol string` - Trading pair name

### BatchMap
Batch execution task pool for all targets in the current exchange market time cycle.

Public fields:
- `Map map[string]*JobEnv` - Task mapping
- `TFMSecs int64` - Time frame milliseconds
- `ExecMS int64` - Timestamp for executing batch tasks, delayed by a few seconds upon receiving a new target; execution starts when delay exceeds DelayBatchMS without receiving new targets

### PairSub
Trading pair subscription information.

Public fields:
- `Pair string` - Trading pair name
- `TimeFrame string` - Time frame
- `WarmupNum int` - Warmup number

### EnterReq
Open position request structure.

Public fields:
- `Tag string` - Entry signal
- `StgyName string` - Strategy name
- `Short bool` - Whether to short sell
- `OrderType int` - Order type
- `Limit float64` - Entry price for limit order, order will be submitted as limit order when specified
- `Stop float64` - Stop price, buy orders enter when the price rises to the trigger price (vice versa for sell orders).
- `CostRate float64` - Opening ratio, default 1x according to configuration, used for calculating LegalCost
- `LegalCost float64` - Amount in fiat currency, ignores CostRate when specified
- `Leverage float64` - Leverage ratio
- `Amount float64` - Entry target amount, calculated from LegalCost and price
- `StopLossVal float64` - Distance from entry price to stop loss price, used to calculate StopLoss
- `StopLoss float64` - Stop loss trigger price, submits a stop loss order on exchange when not empty
- `StopLossLimit float64` - Stop loss limit price, StopLoss not available when provided
- `StopLossRate float64` - Stop loss exit ratio, 0 means full exit, must be between (0,1]
- `StopLossTag string` - Reason for stop loss
- `TakeProfitVal float64` - Distance from entry price to take profit price, used to calculate TakeProfit
- `TakeProfit float64` - Take profit trigger price, submits a take profit order on exchange when not empty
- `TakeProfitLimit float64` - Take profit limit price, TakeProfit not available when provided
- `TakeProfitRate float64` - Take profit exit ratio, 0 means full exit, must be between (0,1]
- `TakeProfitTag string` - Reason for take profit
- `StopBars int` - Number of bars after which unfilled entry limit order will be cancelled
- `ClientID string` - Used to set the trailing part of the ClientOrderID submitted to the exchange.
- `Infos map[string]string` - Used to store additional information for the order.

### ExitReq
Close position request structure.

Public fields:
- `Tag string` - Exit signal
- `StgyName string` - Strategy name
- `EnterTag string` - Only exit orders with EnterTag as entry signal
- `Dirt int` - Direction (core.OdDirt*), long/short/both
- `OrderType int` - Order type
- `Limit float64` - Exit price for limit order, order will be submitted as limit order when specified
- `ExitRate float64` - Exit ratio, default 100% means all orders exit
- `Amount float64` - Target amount to exit, ExitRate invalid when specified
- `OrderID int64` - Only exit specified order
- `UnFillOnly bool` - When True, only exit unfilled portion
- `FilledOnly bool` - When True, only exit filled orders
- `Force bool` - Whether to force exit

## Strategy Management Methods

### New
Create a new trading strategy instance.

Parameters:
- `pol *config.RunPolicyConfig` - Strategy running configuration

Returns:
- `*TradeStrat` - Trading strategy instance

### Get
Get strategy instance based on trading pair and strategy ID.

Parameters:
- `pair string` - Trading pair name
- `stratID string` - Strategy ID

Returns:
- `*TradeStrat` - Trading strategy instance, returns nil if not exists

### GetStratPerf
Get strategy performance configuration.

Parameters:
- `pair string` - Trading pair name
- `strat string` - Strategy name

Returns:
- `*config.StratPerfConfig` - Strategy performance configuration

### AddStratGroup
Add strategy group.

Parameters:
- `group string` - Strategy group name
- `items map[string]FuncMakeStrat` - Strategy creation function mapping

## Order Management Methods

### GetJobs
Get all strategy jobs for the specified account.

Parameters:
- `account string` - Account name

Returns:
- `map[string]map[string]*StratJob` - Job mapping grouped by trading pair and strategy

### GetInfoJobs
Get information strategy jobs for the specified account.

Parameters:
- `account string` - Account name

Returns:
- `map[string]map[string]*StratJob` - Information job mapping grouped by trading pair and strategy

### AddOdSub
Add order change subscription.

Parameters:
- `acc string` - Account name
- `cb FnOdChange` - Order change callback function

### FireOdChange
Trigger order change event.

Parameters:
- `acc string` - Account name
- `od *ormo.InOutOrder` - Order object
- `evt int` - Event type

## Performance Calculation Methods

### CalcJobScores
Calculate strategy job scores.

Parameters:
- `pair string` - Trading pair name
- `tf string` - Time frame
- `stgy string` - Strategy name

Returns:
- `*errs.Error` - Error information

### CalcJobPerfs
Calculate strategy job performance.

Parameters:
- `cfg *config.StratPerfConfig` - Performance configuration
- `p *core.PerfSta` - Performance statistics object
- `perfs []*core.JobPerf` - Performance data list

### CalcDrawDownExitRate
Calculate drawdown exit rate.

Parameters:
- `maxChg float64` - Maximum change rate

Returns:
- `float64` - Calculated exit rate

## System Management Methods

### LoadStratJobs
Load strategy jobs.

Parameters:
- `pairs []string` - Trading pair list
- `tfScores map[string]map[string]float64` - Time frame score mapping

Returns:
- `map[string]map[string]int` - Job status mapping
- `map[string][]*ormo.InOutOrder` - Order mapping
- `*errs.Error` - Error information

### ExitStratJobs
Exit all strategy jobs. 