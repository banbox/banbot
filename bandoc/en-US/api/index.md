* In GO, a folder containing GO source code is called a package, which is the smallest unit for organizing code;
* Packages can import each other, but they cannot form dependency cycles;
* The dependency relationship between packages is independent of folder inclusion, and all packages are equal in terms of dependencies; (A package corresponding to a deeply nested folder can even serve as the entry point of the project)
* The `import` statement for packages must be placed at the top of the file; it cannot be imported within functions, nor can it be dynamically imported;
* The dependency relationships of all packages within the entire Go project should form a directed acyclic tree.

::: tip DeepWiki
It is highly recommended that you quickly get familiar with banbot through [deepwiki](https://deepwiki.com/banbox/banbot); It provides complete flowcharts, design concepts, architecture, conversational research, and so on.
:::

## Go Package Introduction & Dependencies
Banbot is divided into several different packages based on functional characteristics and dependencies. Common global variables are mostly scattered across multiple packages. Below are the dependencies of all packages:

#### [core](core.md)
Types, methods, constants, and variables referenced by all other packages; for example, hyperparameter definitions, prices, simple EMA, error codes, etc.

#### [btime](btime.md)
Time utility package, for getting the current simulated time, getting UTC time, and time conversion.

&emsp;core

#### [utils](utils.md)
Utility package, including BanIO TCP inter-process communication, progress bars, correlation calculations, clustering, file read/write, Sharpe ratio calculations, and other utility functions.

&emsp;core btime

#### [config](config.md)
For parsing yml configurations and accessing several yml variables.

&emsp;core btime utils

#### [exg](exg.md)
Exchange object access management.

&emsp;config utils core

#### [orm](orm.md)
The orm package is referenced by its sub-packages ormo and ormu.
- orm includes read/write access to time-series databases such as K-line data (kline), trading symbols (exsymbol), gaps (kHole), ranges (kInfo), trading calendars, and adjustment factors.
- ormo includes order-related components: trading tasks (BotTask), position records (InOutOrder), and exchange orders (ExOrder).
- ormu includes WebUI-related components: backtest task records.

&emsp;exg config

#### [data](data.md)
For backtest and live K-line data reading, preheating, and subscription.
- IProvider is a multi-asset K-line data provider for a particular exchange and market, while IKlineFeeder is a single-asset K-line data provider. An IProvider can contain multiple IKlineFeeders, and an IKlineFeeder can contain data for multiple timeframes.
- HistProvider corresponds to the backtest data provider, while LiveProvider corresponds to the live data provider (which subscribes to data from the spider process).
- DBKlineFeeder corresponds to backtesting in IKlineFeeder, while KlineFeeder corresponds to live trading; TfKlineLoader can be used to load K-line data for a specific asset and timeframe in batches and then read it one by one. A KlineFeeder can contain multiple TfKlineLoaders.
- Spider is a real-time data subscription spider process for public data. It can subscribe to price, order book, and K-line data for multiple exchanges, markets, and assets simultaneously. A Spider can be connected to and accessed by multiple live trading robot processes.
- KLineWatcher is a client for receiving data from Spider and is used by LiveProvider.

&emsp;orm exg config

#### [strat](strat.md)
Trading strategies, trading task management, and trading task initialization.
- TradeStrat is a classic time-series strategy structure, and StratJob is a trading task for TradeStrat on a particular asset.
- After refreshing the trading assets, StratJobs can be initialized using LoadStratJobs.

&emsp;orm utils

#### [goods](goods.md)
Asset filter, corresponding to pairlists in yml; it is used only when `pairs` is empty. It can use predefined filters to filter all tradable assets based on trading volume, price, volatility, correlation, listing time, offset, etc.

&emsp;orm exg

#### rpc
Social app message notifications.

&emsp;btime, core, config, utils

#### [biz](biz.md)
The core business logic package. It includes the backtest/live order manager, wallet, Grpc data server, basic Trader for processing K-line updates, technical indicators, and StratJob-related callbacks; it also contains utility functions for K-line import/export, correlation calculations, and exchange order downloads.

&emsp;exg orm strat goods data rpc

#### [opt](opt.md)
Includes backtesting and hyperparameter optimization. BackTestLite is used for simple backtesting, while BackTest is used for complex logic backtesting and can include BackTestLite for follow-up entry of unfinished orders.
- The hyper_opt package uses six optimizers, including Bayesian and CMA-ES, to search for strategy hyperparameters.
- sim_bt performs rolling simulation backtesting using the log results from hyperparameter optimization. The final output is a true backtest report without future functions.

&emsp;biz data orm goods strat

### web
The server-side and front-end resources for WebUI and Dashboard UI.

&emsp;config core utils orm data orm exg btime strat opt biz

#### [live](live.md)
Live trading (live/simulation), starting related cron jobs, and monitoring exchange order changes.

&emsp;biz data orm goods strat opt rpc

#### [entry](entry.md)
The entry point for all cmd sub-commands and business logic.

&emsp;optimize live data

## Important Global Variables in Each Go Package
```text
core
    Ctx context.Context // Global context, can select <- core.Ctx to respond to global exit events
    StopAll func() // Trigger a global exit event
    NoEnterUntil map[string]int64 // Prohibit opening orders until the given timestamp
    RunMode string // RunModeLive / RunModeBackTest / RunModeOther
    RunEnv string // RunEnvProd / RunEnvTest / RunEnvDryRun
    StartAt int64 // Start time, 13-digit timestamp
    LiveMode bool // Whether it is live mode: live trading + simulation
    BackTestMode bool // Whether it is backtest mode
    EnvReal bool // LiveMode && RunEnv != RunEnvDryRun, submit orders to the exchange (run_env: prod/test)
    ExgName string // Current exchange name
    Market string // Current market
    IsContract bool // Whether the current market is a contract market (linear/inverse/option)
    Pairs []string // All global trading pairs, in the order refreshed
    OdBooks map[string]*banexg.OrderBook // Cache all received order books

btime
    CurTimeMS int64 // Current timestamp, used only in backtest mode
    LocShow *time.Location // Timezone for display
    UTCLocale *time.Location

biz
    AccOdMgrs // Order book objects, never empty
    AccLiveOdMgrs // Live trading order book objects, not empty in live trading
    AccWallets // Wallets

data
    Spider // Spider

orm
    HistODs // Closed orders: fully exited, used only in backtesting
    AccOpenODs // Open orders: not submitted, submitted but not entered, partially entered, fully entered, partially exited
    AccTriggerODs // Limit entry orders not yet submitted, waiting for polling submission to the exchange, used only in live trading
    AccTaskIDs // Current task IDs
    AccTasks // Current tasks

exg
    Default  // Exchange object

strat
    StagyMap // Strategy registration map
    Versions // Strategy versions
    Envs // All involved K-line environments: Pair+TimeFrame
    PairTFStags // Pair+TF+Strategy pair:[stratID]TradeStrat
    AccJobs // All involved pairs account: pair_tf: [stratID]StratJob
    AccInfoJobs // All involved auxiliary information pairs account: pair_tf: [stratID]StratJob
    ForbidJobs // Prohibited strategy tasks pair_TF: stratID: empty
```
Note: All variables starting with Acc are maps that support multiple accounts.