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
- `UpdateByDataSeries`: Update order status based on candlestick
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

`NewTraderWithRuntimeDeps(deps RuntimeDeps) (Trader, *errs.Error)` constructs a trader. `FeedDataSeries(evt *orm.DataSeries) *errs.Error` drives strategy, indicators and order handling; `FeedSeries` is a compatibility alias. Preserve extension fields and NULL/types in DataSeries.Values rather than reducing events to fixed OHLCV.

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
- `infoEnv`: *ta.BarEnv - Auxiliary environment; nil for main data

### TryFireBatches

`TryFireBatches(currMS int64, isWarmUp bool) int`
Try to trigger batch jobs.

Parameters:
- `currMS`: int64 - Current timestamp in milliseconds

Returns:
- `int` - Number of jobs still waiting

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

### InitFakeWallets
Initialize simulation wallets: `InitFakeWallets(symbols ...string)`.

### GetWallets
`GetWallets(account string) *BanWallets` reads the compatibility registry. For an explicit task use `rt.Trading.Wallet(account)`.

### WatchLiveBalances
`WatchLiveBalances(lifecycles ...WalletRuntimeLifecycle)` monitors live balance updates; standard live assembly supplies its owning lifecycle.

## Factor-engine integration

Trader.FeedDataSeries drives time-series jobs. Shared-account bridges project execution state rather than implementing another account; AccountSink submits Full/Patch targets.

[Factor API](factor.md) / [Guide](../guide/factor.md)


## Strategy orders and account state

Strategies normally call only `job.OpenOrder`/`CloseOrders`. They queue requests; the manager validates, matches or sends them. Use `job.ProcessRuntimeOrders()` for explicit handling, rather than finding a package-level manager by account name in an independent Runtime.

```go
// IOrderMgr method signatures:
ProcessOrders(job *strat.StratJob) ([]*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error)
EnterOrder(exs *orm.ExSymbol, tf string, req *strat.EnterReq) (*ormo.InOutOrder, *errs.Error)
ExitOpenOrders(pairs string, req *strat.ExitReq) ([]*ormo.InOutOrder, *errs.Error)
ExitOrder(od *ormo.InOutOrder, req *strat.ExitReq) (*ormo.InOutOrder, *errs.Error)
UpdateByDataSeries(allOpens []*ormo.InOutOrder, evt *orm.DataSeries) *errs.Error
OnEnvEnd(evt *orm.DataSeries) *errs.Error
```

Embedded applications use `GetOdMgrWithState(rt.Trading, account)`, `GetAllOdMgrWithState(rt.Trading)`, `GetLiveOdMgrWithState(rt.Trading, account)` and `rt.Trading.Wallet(account)` for task state. Check nil: disabled accounts may have no manager. Package-level versions without State query only compatibility registries.

InitOdSubs, InitLiveOrderMgr, InitLocalOrderMgr, InitFakeWallets and StartLiveOdMgr have WithRuntimeDeps variants for new assembly paths. WatchLiveBalances accepts WalletRuntimeLifecycle; standard live entry assembly binds the owning Runtime and manages stop/join. InitLocalOrderMgr also accepts `stops ...func()`. Runtime.Accounts/AccountsMu provides mutable execution configuration shared by manager, wallet and job; the Config snapshot is read-only.

Live wallet balances come from the exchange; simulation/backtest balances do not certify real available funds. Only the compatibility `LiveOrderMgr` direct-send branch closes an order locally on CreateOrder errors, including timeouts; later reconciliation may recover the actual order. This does not describe the default Hub/shared-account execution path: shared execution retains `Unknown` or other unresolved states, blocks ordinary position changes and rejects blind retries with new commands. Recover with the original command ID and identical content, then verify exchange evidence. See [Shared accounts and Hub](../guide/execution.md).

See [Runtime API](runtime.md) and the [strategy guide](../guide/strat_custom.md).
