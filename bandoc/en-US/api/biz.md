# biz Package

The biz package provides business logic layer functionality implementation.

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

### SetupComs
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

### SetupComsExg
Initialize exchange-related basic components.

Parameters:
- `args`: *config.CmdArgs - Command line arguments

Returns:
- `*errs.Error` - Error information

Implementation Details:
- Call `SetupComs` to complete basic initialization
- Initialize exchange ORM module
- Mainly used for initialization when exchange functionality is needed

### LoadRefreshPairs
Load and refresh trading pair information.

Parameters:
- `dp`: data.IProvider - Data provider interface
- `showLog`: bool - Whether to display logs
- `pBar`: *utils.StagedPrg - Progress bar object

Returns:
- `*errs.Error` - Error information

Implementation Details:
- Refresh trading pair list
- Calculate trading pair time period scores
- Load strategy tasks
- Process incomplete orders
- Subscribe to trading pairs that need preheating
- Used during system startup or periodic trading pair information updates

### AutoRefreshPairs
Automatically refresh trading pair information.

Parameters:
- `dp`: data.IProvider - Data provider interface
- `showLog`: bool - Whether to display logs

Implementation Details:
- Automatically call `LoadRefreshPairs`
- Handle refresh failure error logs
- Used for scheduled automatic trading pair information updates

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

### ResetVars
Reset variables.

Implementation Details:
- Reset various global mappings and variables
- Including order managers, wallets, candlestick environments, etc.
- Used for cleaning up state during system restart or reset

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

### RunDataServer
Run data server.

Parameters:
- `args`: *config.CmdArgs - Command line arguments

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

### LoadZipKline
Load candlestick data from ZIP file.

Parameters:
- `inPath`: string - Input path
- `fid`: int - File ID
- `file`: *zip.File - ZIP file object
- `arg`: interface{} - Additional parameters

Returns:
- `*errs.Error` - Error information

Implementation Details:
- Parse candlestick data from ZIP file
- Support multiple data formats
- Handle timestamps and price data
- Used for historical data import

### LoadCalendars
Load calendar data.

Parameters:
- `args`: *config.CmdArgs - Command line arguments

Returns:
- `*errs.Error` - Error information

Implementation Details:
- Initialize basic components
- Read calendar data in CSV format
- Save calendar information grouped by exchange
- Used for managing trading calendars

### ExportKlines
Export candlestick data.

Parameters:
- `args`: *config.CmdArgs - Command line arguments
- `prg`: utils.PrgCB - Progress callback function

Returns:
- `*errs.Error` - Error information

Implementation Details:
- Export candlestick data for specified trading pairs
- Support multiple time periods
- Support adjustment factor processing
- Used for data analysis and backup

### PurgeKlines
Clean up candlestick data.

Parameters:
- `args`: *config.CmdArgs - Command line arguments

Returns:
- `*errs.Error` - Error information

Implementation Details:
- Delete candlestick data based on specified conditions
- Support filtering by trading pair and time period
- Requires user confirmation before execution
- Used for data cleanup and maintenance

### ExportAdjFactors
Export adjustment factors.

Parameters:
- `args`: *config.CmdArgs - Command line arguments

Returns:
- `*errs.Error` - Error information

Implementation Details:
- Export price adjustment factor data
- Include start time, factor values, etc.
- Support timezone settings 