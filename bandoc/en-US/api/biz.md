# biz Package

The biz package provides business logic layer functionality implementation.

## Task Dependencies

Current traders receive one Runtime's core state, clock, market, strategies, orders, wallets, configuration, storage, and exchange dependencies through `biz.RuntimeDeps`; `NewTraderWithRuntimeDeps` never falls back to dynamically finding missing state. Mutable business state such as order processing and wallets therefore belongs to the task that created it.

`SetupComs` and older constructors without RuntimeDeps remain for compatibility and must not be used by new execution flows that need to run alongside other tasks.

## Main Structures

### LiveOrderMgr
Live order manager for managing orders in live trading. Inherits from `OrderMgr`.

### OrderMgr
Base order manager class that provides basic order management functionality.

Main fields:
- `Account`: Account name
- `BarMS`: Current candlestick timestamp (milliseconds)

### IOrderMgr
Order manager interface that defines basic order management methods.

Main methods:
- `ProcessOrders`: Process order requests
- `EnterOrder`: Process entry orders
- `ExitOpenOrders`: Process exit orders
- `ExitOrder`: Process single order exit
- `UpdateByBar`: Update order status based on candlestick
- `OnEnvEnd`: Handle environment end
- `CleanUp`: Clean up resources

### IOrderMgrLive
Live order manager interface, inherits from `IOrderMgr`.

Additional methods:
- `SyncExgOrders`: Synchronize exchange orders
- `WatchMyTrades`: Monitor account trades
- `TrialUnMatchesForever`: Continuously monitor unmatched trades
- `ConsumeOrderQueue`: Consume order queue

### ItemWallet
Single currency wallet.

Main fields:
- `Coin`: Currency code, not trading pair
- `Available`: Available balance
- `Pendings`: Locked amounts during buy/sell, key can be order ID
- `Frozens`: Long-term frozen amounts for short positions, key can be order ID
- `UnrealizedPOL`: Public unrealized profit/loss for this currency, used in contracts, can offset margin for other orders
- `UsedUPol`: Used unrealized profit/loss (used as margin for other orders)
- `Withdraw`: Withdrawn from balance, not available for trading

### BanWallets
Account wallet manager.

Main fields:
- `Items`: Mapping from currency to wallet
- `Account`: Account name
- `IsWatch`: Whether balance changes are being monitored

### Trader
交易管理器,负责处理交易策略和订单执行。

主要方法:
- `OnEnvJobs`: 处理环境任务
- `FeedKline`: 处理K线数据
- `ExecOrders`: 执行订单

## Public Methods

### SetupComs (Compatibility API)
Initialize basic components.

Parameters:
- `args`: *config.CmdArgs - Command line arguments

Returns:
- `*errs.Error` - Error information

Implementation Details:
- Set error printing function
- Create context and cancellation function
- Initialize data directory
- Load configuration file
- Set up logging system
- Initialize core components, exchanges, ORM, and goods modules
- Mainly used for basic infrastructure initialization during system startup

### SetupComsExg (Compatibility API)
Initialize exchange-related basic components.

Parameters:
- `args`: *config.CmdArgs - Command line arguments

Returns:
- `*errs.Error` - Error information

Implementation Details:
- Call `SetupComs` to complete basic initialization
- Initialize exchange ORM module
- Mainly used for initialization when exchange functionality is needed

### InitOdSubs
Initialize order subscriptions.

Implementation Details:
- Collect strategies that need to monitor order changes
- Add order subscription callbacks for each account
- Notify relevant strategies when order status changes
- Used for real-time order status monitoring

### AddBatchJob
Add batch job.

Parameters:
- `account`: string - Account name
- `tf`: string - Time frame
- `job`: *strat.StratJob - Strategy job
- `isInfo`: bool - Whether it's an information type

### TryFireBatches
Try to trigger batch jobs.

Parameters:
- `currMS`: int64 - Current timestamp in milliseconds

Returns:
- `int` - Number of triggered jobs

### InitDataDir
Initialize data directory.

Returns:
- `*errs.Error` - Error information

### GetOdMgr
Get order manager.

Parameters:
- `account`: string - Account name

Returns:
- `IOrderMgr` - Order manager interface

### GetAllOdMgr
Get all order managers.

Returns:
- `map[string]IOrderMgr` - Mapping of account names to order managers

### GetLiveOdMgr
Get live order manager.

Parameters:
- `account`: string - Account name

Returns:
- `*LiveOrderMgr` - Live order manager

### CleanUpOdMgr
Clean up order manager.

Returns:
- `*errs.Error` - Error information

### InitLiveOrderMgr
Initialize live order manager.

Parameters:
- `callBack`: func(od *ormo.InOutOrder, isEnter bool) - Order callback function

Implementation Details:
- Create live order managers for each account
- Set order callback function
- Used for managing real-time trading orders

### InitLocalOrderMgr
Initialize local order manager.

Parameters:
- `callBack`: func(od *ormo.InOutOrder, isEnter bool) - Order callback function
- `showLog`: bool - Whether to display logs

### VerifyTriggerOds
Verify trigger orders.

### StartLiveOdMgr
Start live order manager.

## Factor-engine integration

Trader.FeedDataSeries drives time-series jobs. Shared-account bridges project execution state rather than implementing another account; AccountSink submits Full/Patch targets.

[Factor API](factor.md) / [Guide](../guide/factor.md)


## Current construction, tools and state boundaries

The old names below are no longer callable same-name public entries. Use the actual RuntimeDeps/explicit store/clock/logger APIs rather than nonexistent compatibility aliases:

| Prior documentation name | Current entry |
| --- | --- |
| LoadRefreshPairs / AutoRefreshPairs | RefreshPairsWithRuntimeDeps, RefreshJobsWithRuntimeDeps |
| RunDataServer | entry spider command |
| LoadZipKline | LoadZipSeriesWithRuntimeDeps |
| LoadCalendars | LoadCalendarsWithDeps |
| ExportKlines / PurgeKlines | ExportKlinesWithRuntimeDeps / PurgeKlinesWithRuntimeDeps |
| ExportAdjFactors | ExportAdjFactorsWithRuntimeDeps |
| CalcCorrelation | CalcCorrelationWithRuntimeDeps |
| RunHistKline | RunHistSeries / RunHistSeriesWithRuntimeDeps |

Trader, wallets and jobs use the same Runtime.Accounts/AccountsMu. Shared-account bridges preserve TS projections; execution owns physical sends/ledger. Manager/wallet facades are legacy only; explicit business paths fail on missing dependencies rather than falling back to globals.

See [runtime](runtime.md).
