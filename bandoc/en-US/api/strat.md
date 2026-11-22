# strat Package

The strat package provides trading strategy-related functionality definitions and implementations.

## Main Structures

### TradeStrat
Core structure for trading strategy, defines basic properties and behaviors of a strategy.

Public fields:
- `Name string` - Strategy name
- `Version int` - Strategy version number
- `WarmupNum int` - Number of candlesticks needed for warmup
- `OdBarMax int` - Max bars a backtest simulates forward to find unfinished positions, default 500; `run_policy.order_bar_max` takes precedence
- `MinTfScore float64` - Minimum time frame quality, default 0.75
- `WsSubs map[string]string` - WsSubs        map[string]string    // websocket subscription: core.WsSubKLine, core.WsSubTrade, core.WsSubDepth
- `DrawDownExit bool` - Whether to enable drawdown exit
- `HedgeOff bool` - turn off future hedge mode
- `BatchInOut bool` - Whether to batch execute entry/exit after main `OnData(RoleMain)`/`OnBar`
- `BatchInfo bool` - Whether to batch process after auxiliary `OnData(RoleInfo)`/`OnInfoBar`
- `StakeRate float64` - Relative basic amount billing rate
- `StopLoss float64` - Stop loss rate for all orders opened by this strategy (without leverage)
- `StopEnterBars int` - Timeout candlestick number for limit entry orders
- `OrderOnRotation string` - Old job after its symbol is rotated out: `open` keeps allowing new orders while it holds orders; whether positions are closed is set by `pairmgr.pos_on_rotation`
- `EachMaxLong int` - Maximum number of long orders per trading pair, -1 to disable
- `EachMaxShort int` - Maximum number of short orders per trading pair, -1 to disable
- `RunTimeFrames []string` - Allowed running time periods; when omitted, the default comes from the current Runtime configuration snapshot
- `RefineTF interface{}` - Strategy default order-matching timeframe, e.g. `"5m"`, `12`, `"3-6"`; an explicit `run_policy[].refine_tf` takes precedence
- `Outputs []string` - Content of the text file output by the strategy, where each string is one line
- `Policy *config.RunPolicyConfig` - Strategy running configuration
- `OnDataSubs` - Declares K-line extension columns or custom time-series subscriptions
- `OnData` - Receives a unified `strat.DataEvent`, which embeds updated Series/latest values from `*strat.DataFields` and identifies primary K-lines, auxiliary K-lines, or custom time-series
- `OnWsData` - Receives websocket time-series events

Assign `OnData` directly when there are no auxiliary or custom subscriptions. Use `RouteData(DataHandlers{...})` when event filtering is required. `OnData` cannot be configured together with `OnBar` or `OnInfoBar`.

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
- `ExgStopLoss bool` - Default true: stop loss is submitted to the exchange as a conditional order; when false no conditional order is placed and the stop is monitored locally by price (backtest and live); a `CallbackPct` trailing stop behaves the same
- `LongSLPrice float64` - Default long stop loss price when opening position
- `ShortSLPrice float64` - Default short stop loss price when opening position
- `ExgTakeProfit bool` - Default true: take profit is submitted to the exchange as a conditional order; when false it is monitored locally by price
- `LongTPPrice float64` - Default long take-profit price when opening: used when `EnterReq` does not specify `TakeProfit/TakeProfitVal`, ignored when `TakeProfits` is set; must be above the entry price
- `ShortTPPrice float64` - Default short take-profit price when opening; must be below the entry price
- `IsWarmUp bool` - Whether currently in warmup state
- `More interface{}` - Additional information for strategy customization
- `DataHub` - Caches the latest events and window data by `(source, sid, timeframe)`

Public methods (see the [strategy guide](../guide/strat_custom.md) for usage of order and stop/take-profit methods):
- `CanOpen(short bool) bool`, `OpenOrder(req *EnterReq) *errs.Error`, `CloseOrders(req *ExitReq) *errs.Error`
- `GetOrderNum(dirt float64) int`, `GetOrders(dirt float64) []*ormo.InOutOrder`, `Position(dirt float64, enterTag string) float64`, `PositionAvgPrice(dirt float64, enterTag string) float64`, `GetAvgCostPrice(odList ...*ormo.InOutOrder) float64`
- `SetAllStopLoss(dirt float64, args *ormo.ExitTrigger) *errs.Error`, `SetAllTakeProfit(...)`: nil `args` cancels; panics when `dirt` is 0 (both) and long and short orders exist at once
- `AccountEquity() float64`: account equity (balance + unrealized PnL); during warmup or before the wallet is ready it falls back to the initial capital / default stake amount and is never 0 or NaN
- `AvailableBalance() float64`: available balance / margin, 0 when unavailable
- `AccountDrawdown() float64`: drawdown ratio versus the equity peak since the run started, in `[0,1]`, 0 during warmup
- `RefKlineSub(symbol, timeframe string, warmupNum ...int) *DataSub`: creates a read-only K-line reference subscription (empty or `_cur_` symbol means the current symbol; an omitted `warmupNum` uses `TradeStrat.WarmupNum`); the package-level `NewRefKlineSub(s, symbol, timeframe, warmupNum...)` is equivalent
- `RefEnv(symbol, timeframe string) *ta.BarEnv`: synchronized K-line environment of a reference stream, nil until its first bar arrives
- `Data(sub *DataSub) *DataFields`: subscription data view; a nil `sub.ExSymbol` means the current symbol
- `OpeningRange(anchorMinutesUTC, rangeMinutes int) OpeningRangeSnapshot`, `OpeningRangeWithOpts(anchorMinutesUTC, rangeMinutes int, opts OpeningRangeOptions) OpeningRangeSnapshot`, `SessionRange(startMinUTC, endMinUTC int) SessionRangeSnapshot`
- `PrevDayOHLC(anchorMinutesUTC int) (PrevOHLC, bool)`, `PrevNDaysStats(anchorMinutesUTC, nDays int) (PrevNDaysSummary, bool)` (`nDays` is 1-366)
- `DualThrustBands(anchorMinutesUTC, nDays int, k1, k2 float64) (upper, lower, dayOpen float64, ok bool)`
- `RBreakerLevels(anchorMinutesUTC int) (RBreakerSnap, bool)`, `RBreakerLevelsBy(anchorMinutesUTC int, f1, f2, f3 float64) (RBreakerSnap, bool)`
- `ProcessRuntimeOrders()`, `ExecutionSnapshot()`, `IsWarmUpState() bool`, `OrderCounts() (orderNum, enteredNum int)`, `PendingEntryCount() int`

Intraday methods take anchors as UTC minutes of the day; state is kept per job and has no lookahead: range values such as `High/Low` are 0 before `Ready`/`Completed`, and `Applicable=false` means the timeframe does not fit (1d or coarser, not aligned to the range, or coarser than the range without `AllowSingleBarFallback`).

### DataSub
Time-series subscription information.

Public fields:
- `Source string` - Data source name; an empty value is equivalent to `kline`
- `ExSymbol *orm.ExSymbol` - Bound trading symbol
- `TimeFrame string` - Data source base timeframe
- `WarmupNum int` - Initial warmup count
- `Fields []string` - Fields to read
- `SeriesFields []string` - Fields maintained as `banta.Series` (also added to the read projection); see [Custom Time-Series Data](../guide/custom_data.md)

### DataHub
Runtime data cache. `Get(tf, source, sid)` returns processed `DataFields`, and `AllReady()` determines whether all subscriptions that should be closed at the current event time have been updated. See [Custom Time-Series Data](../guide/custom_data.md) for subscription and consumption examples.

`OnBatchJobs` receives main-timeframe jobs only and is enabled by `BatchInOut`; auxiliary-timeframe jobs go to `OnBatchInfos`, enabled by `BatchInfo`.

### JobEnv
Represents a job for additional product data in OnBatchInfos.

Public fields:
- `Job *StratJob` - Strategy job instance
- `Env *ta.BarEnv` - Indicator runtime environment
- `Symbol string` - Trading pair name

### BatchState / BatchMap

Use `NewBatchState`, `AddTask`, `TakeReady(currMS, deterministic)`, `PendingCount` and `Reset`. All backtests use deterministic batches: within one bar, main jobs are ordered by the neutral hash `BarPriority(barMS, symbol, seed)` (not alphabetically; key order is used only when no valid bar time exists). The seed is the `bt_bar_order_seed` setting (default 0); `SetBarOrderSeed(seed uint64)` pins one on that instance and `BarOrderSeed() (uint64, bool)` reads it. Live batch order is not guaranteed; sort by strategy score when entries compete for funds. `LegacyBatchState` serves compatibility calls only.

`func BarPriority(barMS int64, symbol string, seed uint64) uint64` returns a symbol's neutral, reproducible priority at a bar time (lower runs first); it can be used to break ties when sorting in a callback.

`BatchMap` represents one timeframe task group inside the queue, rather than process-shared market state.

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
- `CommandID string` - Stable request identity; callback events can derive it. Idempotent execution depends on the order manager.
- `Tag string` - Entry signal (required)
- `StratName string` - Strategy name
- `Short bool` - Whether to short sell
- `OrderType int` - Order type
- `Limit float64` - Entry price for limit order, order will be submitted as limit order when specified; with `Stop` it makes a Stop-Limit entry
- `Stop float64` - Entry trigger price (not a stop loss): buy orders enter when the price rises to it (vice versa for sell orders)
- `OCOGroup string` - OCO pending-entry group: when one entry in the group fills, the other unfilled entries of the same (symbol, strategy, group) are cancelled
- `CostRate float64` - Opening ratio, default 1x according to configuration, used for calculating LegalCost
- `RiskRate float64` - Share of `AccountEquity()` risked at the stop loss, `[0,1]`; when `Amount`/`LegalCost` are unset and a stop price exists, `LegalCost = AccountEquity() x RiskRate / (|entry - stop| / entry)`, capped by leverage (1x for spot) and `max_stake_amt`; without a stop it falls back to the default stake amount x `CostRate`
- `LegalCost float64` - Amount in fiat currency, ignores CostRate when specified
- `Leverage float64` - Leverage ratio
- `Amount float64` - Entry target amount, calculated from LegalCost and price
- `StopLossVal float64` - Distance from entry price to stop loss price, used to calculate StopLoss
- `StopLoss float64` - Stop loss trigger price; submitted as an exchange conditional order by default, only monitored locally when `ExgStopLoss=false`
- `StopLossLimit float64` - Limit price used after the stop triggers (0 = market); also the trigger price when `StopLoss` is 0
- `StopLossRate float64` - Stop loss exit ratio, 0 means full exit, must be between (0,1]
- `StopLossTag string` - Reason for stop loss
- `TakeProfitVal float64` - Distance from entry price to take profit price, used to calculate TakeProfit
- `TakeProfit float64` - Take profit trigger price; submitted as an exchange order by default, only monitored locally when `ExgTakeProfit=false`
- `TakeProfitLimit float64` - Limit price used after the take profit triggers (0 = market); also the trigger price when `TakeProfit` is 0
- `TakeProfitRate float64` - Take profit exit ratio, 0 means full exit, must be between (0,1]
- `TakeProfitTag string` - Reason for take profit
- `TakeProfits []TakeProfitTarget` - Multi-stage take-profit ladder; cannot be combined with `TakeProfit`/`TakeProfitVal`/`TakeProfitLimit`, and `LongTPPrice/ShortTPPrice` no longer apply once set
- `StopBars int` - Number of bars after which unfilled entry limit order will be cancelled
- `ClientID string` - Used to set the trailing part of the ClientOrderID submitted to the exchange.
- `ActivationPrice float64` - Trailing stop activation price, 0 = active immediately; only used when `CallbackPct>0`
- `CallbackPct float64` - Trailing stop callback percent (unit %, 1.5 means 1.5%), 0 = off, a nonzero value must be within `[0.05, 50]`; live prefers the exchange's native trailing order and falls back to local monitoring when it is unsupported or rejected
- `Infos map[string]string` - Used to store additional information for the order.
- `Log bool` - Automatically log failures

Setting both `Stop` and `Limit` makes a Stop-Limit entry: once price reaches `Stop`, a limit order rests at `Limit`; if `Stop` has already been crossed, the limit order is placed directly. `Limit` cannot be set when `OrderType` is `core.OrderTypeMarket`. `StopLossVal`/`TakeProfitVal` are price differences, not ratios. Warmup does not enqueue requests; nil does not mean a fill. `CloseOrders` queues an intent; an exit limit on the favorable side is converted to an order stop-loss limit rather than filled immediately.

### TakeProfitTarget
One stage of a take-profit ladder.

Public fields:
- `Price float64` / `Val float64` / `Rate float64` - Exactly one is needed (greater than 0): absolute trigger price; price distance from the entry fill average; ratio relative to the entry fill average. `Price` wins when several are given, then `Val`; `Price` must be on the profit side
- `Limit float64` - Limit price after the trigger, 0 = market
- `ExitRate float64` - Share of the initial position closed at this stage, `(0,1]`; 0 is treated as 1 with a single stage; the stages must sum to at most 1
- `MoveSLToBreakeven bool` - After this stage fills, tighten the remaining position's stop loss to no worse than the entry price (a tighter stop is not loosened)
- `Tag string` - Exit reason for this stage

Stages trigger from nearest to farthest from the entry price.

### ExitReq
Close position request structure.

Public fields:
- `CommandID string` - Stable request identity; replaying the same ID and payload does not submit a second action
- `Tag string` - Exit signal
- `StratName string` - Strategy name
- `EnterTag string` - Only exit orders with EnterTag as entry signal
- `Dirt int` - Direction (core.OdDirt*), long/short/both; the zero value means both. Must be set explicitly when both long and short orders exist and `Limit` is set
- `OrderType int` - Order type
- `Limit float64` - Exit price for limit order, order will be submitted as limit order when specified
- `ExitRate float64` - Exit ratio, default 100% means all orders exit
- `Amount float64` - Target amount to exit, ExitRate invalid when specified
- `OrderID int64` - Only exit specified order
- `UnFillOnly bool` - When True, only exit unfilled portion
- `FilledOnly bool` - When True, only exit filled orders
- `Force bool` - Whether to force exit
- `Log bool` - Automatically log failures

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

## Factor-engine integration

TradeStrat is time-series; graphs and combination live in factor/runner. DataHub rawMap NULL/missing semantics coexist with numerical views.

[Factor API](factor.md) / [Guide](../guide/factor.md)

## Callbacks and job methods

```go
OnStartUp     func(s *StratJob)
OnDataSubs    func(s *StratJob) []*DataSub
OnData        func(s *StratJob, data DataEvent)
OnSymbols     func(items []string) []string
OnCheckExit   func(s *StratJob, od *ormo.InOutOrder) *ExitReq
OnOrderChange func(s *StratJob, od *ormo.InOutOrder, chgType int)
OnBatchJobs   func(jobs []*StratJob)
OnBatchInfos  func(tf string, jobs map[string]*JobEnv)
OnWsData      func(s *StratJob, evt *orm.DataSeries)
OnPostApi     FnOnPostApi
OnShutDown    func(s *StratJob)
OnStratExit   func()
```

Compatibility callbacks: `OnBar func(s *StratJob)`, `OnInfoBar func(s *StratJob, e *ta.BarEnv, pair, tf string)`, `OnPairInfos func(s *StratJob) []*PairSub`, `OnWsTrades`, `OnWsDepth`, `OnWsKline`; also `GetDrawDownExitRate CalcDDExitRate` and `PickTimeFrame PickTimeFrameFunc`.

These are callback field signatures; other configuration fields are omitted. `OnData` cannot coexist with `OnBar`/`OnInfoBar`: construction panics. Use `RouteData(DataHandlers{Main: ..., Info: ..., Custom: ...})` to route events whose `DataFields` are already updated. `DataSub` aliases `orm.Subscription`.

`job.Data(sub)` returns a subscription view; `DataHub.Get(tf, source, sid)` addresses the same identity. Check for nil before data arrives. `RawValue(name) (any, bool)` distinguishes missing from explicit NULL; `Series(name)` provides numeric indicator windows. Preserve custom field types instead of converting everything to float64.

`job.OpenOrder(req)`, `CloseOrders(req)`, `SetAllStopLoss(dirt, trigger)` and `SetAllTakeProfit(dirt, trigger)` return `*errs.Error`. `ProcessRuntimeOrders()` returns `(entered, exited []*ormo.InOutOrder, err *errs.Error, bound bool)` and handles only this job; bound=false means no explicit Runtime is bound. Do not append to Entrys/Exits directly. `ExecutionSnapshot()` supports reads across event boundaries; its order pointers remain borrowed.

Open/close requests made in `OnBatchJobs` are submitted automatically by the framework after the callback (except during warmup); requests made in `OnBatchInfos` are not submitted automatically and only log a WARN.

`RegisterStrategy(name, factory)` registers a full name; `AddStratGroup` registers a group. Get running jobs from `rt.Strategies.CollectJobs()` or `LookupJob(account, pairTF, strategy)`. Package-level `New/Get/GetJobs/GetInfoJobs/AddOdSub/FireOdChange/LoadStratJobs/ExitStratJobs` are compatibility APIs and do not query independent Runtimes. For dynamic pairs pass the owning `rt.Strategies` in `PairUpdateReq.StrategyState`; see the [strategy guide](../guide/strat_custom.md).

### UpdatePairs
`func (s *TradeStrat) UpdatePairs(req PairUpdateReq) (*PairUpdateResult, *errs.Error)` adds or removes symbols at runtime. `PairUpdateReq{Strat, StrategyState, Core, Exchange, Add, Remove, CloseOnRemove, ForceAdd, Reason}`; an empty `Strat` defaults to the receiver, and a different one returns an error. `PairUpdateResult{Added, Removed, Skipped, ExitOrders, Warnings}`.

### Parameter validation
In a strategy constructor, `pol.Require(cond bool, msg string)` and `pol.Requiref(cond bool, format string, args ...any)` validate parameters; when the condition fails, construction of that strategy fails and is reported as BadConfig (a configuration error) without being cached; ordinary runtime errors are not reclassified.
