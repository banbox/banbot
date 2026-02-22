# config Package

The config package provides system configuration-related structures and methods.

## Important Structures

### CmdArgs
Command line argument structure, containing the following fields:
- `Configs`: []string - List of configuration file paths
- `Logfile`: string - Log file path
- `DataDir`: string - Data directory path
- `NoCompress`: bool - Whether to disable compression
- `NoDefault`: bool - Whether to disable default configuration
- `LogLevel`: string - Log level
- `TimeRange`: string - Time range in "YYYYMMDD-YYYYMMDD" format
- `TimeFrames`: []string - List of time frames
- `StakeAmount`: float64 - Single order amount
- `StakePct`: float64 - Single order amount percentage
- `Pairs`: []string - List of trading pairs
- `Force`: bool - Whether to force execution
- `WithSpider`: bool - Whether to use crawler
- `MaxPoolSize`: int - Maximum connection pool size
- `CPUProfile`: bool - Whether to enable CPU profiling
- `MemProfile`: bool - Whether to enable memory profiling
- `TimeZone`: string - Time zone
- `OptRounds`: int - Number of rounds for hyperparameter optimization single task
- `Concur`: int - Number of concurrent processes for hyperparameter optimization
- `Sampler`: string - Hyperparameter optimization method (tpe/bayes/random/cmaes etc.)
- `EachPairs`: bool - Whether to execute for each trading pair individually
- `ReviewPeriod`: string - Review period for continuous parameter optimization backtesting
- `RunPeriod`: string - Effective running period after parameter optimization
- `Alpha`: float64 - Smoothing factor for EMA calculation
- `Separate`: bool - Whether to test strategy combinations separately during backtesting

### Config
Root configuration structure, containing the following fields:
- `Name`: string - Configuration name
- `Env`: string - Running environment
- `Leverage`: float64 - Leverage multiplier
- `LimitVolSecs`: int - Expected waiting time for limit orders (seconds)
- `PutLimitSecs`: int - Expected time for submitting limit orders to exchange
- `MarketType`: string - Market type
- `ContractType`: string - Contract type
- `OdBookTtl`: int64 - Order book time to live
- `StopEnterBars`: int - Number of candles after which to cancel unfilled entry limit orders
- `ConcurNum`: int - Concurrency number
- `OrderType`: string - Order type
- `PreFire`: float64 - Pre-firing
- `MarginAddRate`: float64 - Margin addition rate
- `ChargeOnBomb`: bool - Whether to charge on liquidation
- `TakeOverStgy`: string - Takeover strategy
- `StakeAmount`: float64 - Single order amount
- `StakePct`: float64 - Single order amount percentage
- `MaxStakeAmt`: float64 - Maximum single order amount
- `OpenVolRate`: float64 - Open position volume rate
- `MinOpenRate`: float64 - Minimum open position rate
- `BTNetCost`: float64 - Backtesting network delay (seconds)
- `MaxOpenOrders`: int - Maximum number of open orders
- `MaxSimulOpen`: int - Maximum number of simultaneous open orders
- `WalletAmounts`: map[string]float64 - Wallet amounts
- `DrawBalanceOver`: float64 - Balance withdrawal threshold
- `StakeCurrency`: []string - Order currencies
- `FatalStop`: map[string]float64 - Fatal stop conditions
- `FatalStopHours`: int - Fatal stop hours
- `TimeRange`: *TimeTuple - Time range
- `RunTimeframes`: []string - Running time frames
- `KlineSource`: string - Kline data source
- `WatchJobs`: map[string][]string - Monitoring jobs
- `RunPolicy`: []*RunPolicyConfig - Running policy configuration
- `StratPerf`: *StratPerfConfig - Strategy performance configuration
- `Pairs`: []string - Trading pairs list
- `PairMgr`: *PairMgrConfig - Pair manager configuration
- `PairFilters`: []*CommonPairFilter - Pair filters
- `Exchange`: *ExchangeConfig - Exchange configuration
- `Database`: *DatabaseConfig - Database configuration
- `APIServer`: *APIServerConfig - API server configuration
- `RPCChannels`: map[string]map[string]interface{} - RPC channel configuration
- `Webhook`: map[string]map[string]string - Webhook configuration

### RunPolicyConfig
Running policy configuration, allowing multiple strategies to run simultaneously, containing the following fields:
- `Name`: string - Strategy name
- `Filters`: []*CommonPairFilter - Pair filters
- `RunTimeframes`: []string - Running time frames
- `MaxPair`: int - Maximum number of trading pairs
- `MaxOpen`: int - Maximum number of open orders
- `MaxSimulOpen`: int - Maximum number of simultaneous open orders
- `Dirt`: string - Trading direction (long/short)
- `StratPerf`: *StratPerfConfig - Strategy performance configuration
- `Pairs`: []string - Trading pairs list
- `Params`: map[string]float64 - Strategy parameters
- `PairParams`: map[string]map[string]float64 - Pair parameters

### StratPerfConfig
Strategy performance configuration, containing the following fields:
- `Enable`: bool - Whether to enable
- `MinOdNum`: int - Minimum number of orders
- `MaxOdNum`: int - Maximum number of orders
- `MinJobNum`: int - Minimum number of tasks
- `MidWeight`: float64 - Medium weight
- `BadWeight`: float64 - Bad weight

### DatabaseConfig
Database configuration, containing the following fields:
- `Url`: string - Database connection URL
- `Retention`: string - Data retention time
- `MaxPoolSize`: int - Maximum connection pool size
- `AutoCreate`: bool - Whether to automatically create the database

### APIServerConfig
API server configuration, containing the following fields:
- `Enable`: bool - Whether to enable
- `BindIPAddr`: string - Binding IP address
- `Port`: int - Listening port
- `Verbosity`: string - Log verbosity
- `JWTSecretKey`: string - JWT secret key
- `CORSOrigins`: []string - CORS allowed sources
- `Users`: []*UserConfig - User configurations

### UserConfig
User configuration, containing the following fields:
- `Username`: string - Username
- `Password`: string - Password
- `AccRoles`: map[string]string - Account role permissions
- `ExpireHours`: float64 - Token expiration time (hours)

### WeWorkChannel
WeWork channel configuration, containing the following fields:
- `Enable`: bool - Whether to enable
- `Type`: string - Type
- `MsgTypes`: []string - Message types
- `AgentId`: string - Application ID
- `CorpId`: string - Enterprise ID
- `CorpSecret`: string - Application secret
- `Keywords`: string - Keywords

### PairMgrConfig
Pair manager configuration, containing the following fields:
- `Cron`: string - Cron task expression
- `Offset`: int - Offset
- `Limit`: int - Limit
- `ForceFilters`: bool - Whether to force filtering

### AccountConfig
Account configuration, containing the following fields:
- `APIKey`: string - API key
- `APISecret`: string - API secret
- `NoTrade`: bool - Whether to prohibit trading
- `MaxStakeAmt`: float64 - Maximum single order amount
- `StakeRate`: float64 - Open position amount multiplier
- `StakePctAmt`: float64 - Amount when opening positions based on percentage
- `Leverage`: float64 - Leverage multiplier

## Public Methods

### GetDataDir
Get data directory path.

Returns:
- `string` - Absolute path of the data directory. Returns empty string if environment variable `BanDataDir` is not set.

### GetStratDir
Get strategy directory path.

Returns:
- `string` - Absolute path of the strategy directory. Returns empty string if environment variable `BanStratDir` is not set.

### LoadConfig
Load and apply configuration.

Parameters:
- `args`: *CmdArgs - Command line argument object

Returns:
- `*errs.Error` - Error information, returns nil if successful

### GetConfig
Get configuration based on command line arguments.

Parameters:
- `args`: *CmdArgs - Command line argument object
- `showLog`: bool - Whether to display logs

Returns:
- `*Config` - Configuration object
- `*errs.Error` - Error information

### ParseConfig
Parse configuration file at specified path.

Parameters:
- `path`: string - Configuration file path

Returns:
- `*Config` - Configuration object
- `*errs.Error` - Error information

### ApplyConfig
Apply configuration to global state.

Parameters:
- `args`: *CmdArgs - Command line argument object
- `c`: *Config - Configuration object

Returns:
- `*errs.Error` - Error information

### GetExgConfig
Get current exchange configuration.

Returns:
- `*ExgItemConfig` - Exchange configuration object

### GetTakeOverTF
Get takeover time frame for specified trading pair.

Parameters:
- `pair`: string - Trading pair name
- `defTF`: string - Default time frame

Returns:
- `string` - Time frame

### GetAccLeverage
Get leverage multiplier for specified account.

Parameters:
- `account`: string - Account name

Returns:
- `float64` - Leverage multiplier

### ParsePath
Parse path, replacing paths starting with `$` or `@` with absolute path of data directory.

Parameters:
- `path`: string - Original path

Returns:
- `string` - Parsed path

### GetStakeAmount
Get stake amount for specified account.

Parameters:
- `accName`: string - Account name

Returns:
- `float64` - Stake amount

### DumpYaml
Export current configuration in YAML format.

Returns:
- `[]byte` - Configuration data in YAML format
- `*errs.Error` - Error information

### LoadPerfs
Load strategy performance data from specified directory.

Parameters:
- `inDir`: string - Input directory path

### ParseTimeRange
Parse time range string.

Parameters:
- `timeRange`: string - Time range string in format "YYYYMMDD-YYYYMMDD"

Returns:
- `int64` - Start timestamp (milliseconds)
- `int64` - End timestamp (milliseconds)
- `error` - Error information

### GetExportConfig
Get export configuration.

Parameters:
- `path`: string - Configuration file path

Returns:
- `*ExportConfig` - Export configuration object
- `*errs.Error` - Error information 