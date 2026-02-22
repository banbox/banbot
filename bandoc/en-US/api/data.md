# data Package

The data package provides functionality for data processing and management.

The important concepts of this package are as follows:
* **Provider**: A candlestick data provider, which can contain multiple Feeders with the same start and end times.
* **Feeder**: Corresponds to a data source for a specific instrument and can include data for multiple timeframes.
* **Spider**: Real-time monitoring of multiple exchange data, storing it in a database, and notifying subscribers via TCP.
* **Miner**: Each exchange + market pair corresponds to one Miner.
* **KLineWatcher**: A client used to communicate with the Spider.

## Provider and Feeder
Both backtesting and live trading require subscribing to candlestick data, and multiple strategies may run simultaneously, each subscribing to multiple timeframes. Therefore, the `IProvider` interface is introduced to support both backtesting (`HistProvider`) and live trading (`LiveProvider`).

candlestick data for a single instrument may be used by multiple strategies simultaneously. To avoid redundant data fetching by each strategy, the `IKlineFeeder` interface is introduced to support backtesting (`DBKlineFeeder`) and live trading (`KlineFeeder` + `KLineWatcher`).

Each `KlineFeeder` corresponds to one instrument and can include data for multiple timeframes. For example, the instrument BTC might be used by multiple strategies, subscribing to the 5m, 1h, and 1d timeframes. To avoid redundant data reading, only the smallest timeframe (in this case, 5m) will fetch the candlestick data; larger timeframe data will be aggregated from the smallest timeframe.

#### Suitable Scenarios for Provider and Feeder
**Fetching data for multiple instruments and timeframes with consistent start and end times**. For example, during backtesting or live trading, a set of strategies is run over a specific period, involving multiple instruments and different timeframes. It is recommended to use `Provider` + `Feeder` in such cases. If data needs to be fetched for different time periods separately, multiple `Provider` + `Feeder` instances should be initialized.

#### Unsuitable Scenarios for Provider and Feeder
**Fetching data for different timeframes with inconsistent start and end times**. For example, if you want to fetch the last 1,000 candlesticks for both 1m and 1h timeframes for BTC for backtesting or other tasks, forcing the use of `Feeder` would result in the 1m timeframe fetching 60 * 1,000 candlesticks. In such cases, it is better to directly call `orm.GetOHLCV` to fetch data for each timeframe separately or initialize `Feeder` twice to fetch the data separately.

## Important Structures

### Feeder
Each Feeder corresponds to a trading pair and can contain multiple time dimensions.

Public fields:
- `ExSymbol *orm.ExSymbol` - Exchange trading pair information
- `States []*PairTFCache` - Cache states for each time dimension
- `WaitBar *banexg.Kline` - candlestick data waiting to be processed
- `CallBack FnPairKline` - candlestick data callback function
- `OnEnvEnd FuncEnvEnd` - Environment end callback function (requires position closing when futures main contract switches or stock has ex-rights)
- `isWarmUp bool` - Whether currently in warm-up state

### IKlineFeeder
candlestick data feeder interface.

Public methods:
- `getSymbol() string` - Get trading pair name
- `getWaitBar() *banexg.Kline` - Get waiting candlestick
- `setWaitBar(bar *banexg.Kline)` - Set waiting candlestick
- `SubTfs(timeFrames []string, delOther bool) []string` - Subscribe to specified time periods
- `WarmTfs(curMS int64, tfNums map[string]int, pBar *utils.PrgBar) (int64, *errs.Error)` - Warm up time periods
- `onNewBars(barTfMSecs int64, bars []*banexg.Kline) (bool, *errs.Error)` - Process new candlestick data
- `getStates() []*PairTFCache` - Get cache states

### KlineFeeder
Each Feeder corresponds to a trading pair and can contain multiple time dimensions. Used in live trading.

Public fields:
- `Feeder` - Inherits from Feeder
- `PreFire float64` - Ratio for triggering bar early
- `showLog bool` - Whether to display logs

### IHistKlineFeeder
Historical candlestick data feeder interface, inherits from IKlineFeeder.

Additional public methods:
- `getNextMS() int64` - Get end timestamp of next bar
- `DownIfNeed(sess *orm.Queries, exchange banexg.BanExchange, pBar *utils.PrgBar) *errs.Error` - Download candlesticks for entire range
- `SetSeek(since int64)` - Set reading position
- `GetBar() *banexg.Kline` - Get current candlestick
- `RunBar(bar *banexg.Kline) *errs.Error` - Run callback function for candlestick
- `CallNext()` - Move pointer to next candlestick

### HistKLineFeeder
Historical data feeder, base class for file feeder and database feeder.

Public fields:
- `KlineFeeder` - Inherits from KlineFeeder
- `TimeRange *config.TimeTuple` - Time range
- `TradeTimes [][2]int64` - Trading times

### DBKlineFeeder
Database candlestick feeder for backtesting.

Public fields:
- `HistKLineFeeder` - Inherits from HistKLineFeeder
- `offsetMS int64` - Offset timestamp

### IProvider
Data provider interface.

Public methods:
- `LoopMain() *errs.Error` - Main loop
- `SubWarmPairs(items map[string]map[string]int, delOther bool) *errs.Error` - Subscribe and warm up trading pairs
- `UnSubPairs(pairs ...string) *errs.Error` - Unsubscribe trading pairs
- `SetDirty()` - Set dirty flag

### Provider
Data provider base class.

Public fields:
- `holders map[string]T` - Map of held Feeders
- `newFeeder func(pair string, tfs []string) (T, *errs.Error)` - Function to create new Feeder
- `dirtyVers chan int` - Dirty version channel
- `showLog bool` - Whether to display logs

### HistProvider
Historical data provider.

Public fields:
- `Provider[IHistKlineFeeder]` - Inherits from Provider
- `pBar *utils.StagedPrg` - Progress bar

### LiveProvider
Real-time data provider.

Public fields:
- `Provider[IKlineFeeder]` - Inherits from Provider
- `*KLineWatcher` - candlestick watcher

### NotifyKLines
candlestick notification message.

Public fields:
- `TFSecs int` - Time period (seconds)
- `Interval int` - Update interval (seconds)
- `Arr []*banexg.Kline` - candlestick array

### KLineMsg
candlestick message.

Public fields:
- `ExgName string` - Exchange name
- `Market string` - Market type
- `Pair string` - Trading pair
- `TFSecs int` - Time period (seconds)
- `Interval int` - Update interval (seconds)
- `Arr []*banexg.Kline` - candlestick array

### SaveKline
Task for saving candlesticks.

Public fields:
- `Sid int32` - Trading pair ID
- `TimeFrame string` - Time period
- `Arr []*banexg.Kline` - candlestick array
- `SkipFirst bool` - Whether to skip first
- `MsgAction string` - Message action

### FetchJob
candlestick fetching task.

Public fields:
- `PairTFCache` - candlestick cache
- `Pair string` - Trading pair
- `CheckSecs int` - Check interval (seconds)
- `Since int64` - Start timestamp
- `NextRun int64` - Next run timestamp

### Miner
Data miner.

Public fields:
- `ExgName string` - Exchange name
- `Market string` - Market type
- `Fetchs map[string]*FetchJob` - Map of fetch tasks
- `KlineReady bool` - Whether candlestick is ready
- `KlinePairs map[string]bool` - Map of candlestick trading pairs
- `TradeReady bool` - Whether trade is ready
- `TradePairs map[string]bool` - Map of trade trading pairs
- `BookReady bool` - Whether order book is ready
- `BookPairs map[string]bool` - Map of order book trading pairs
- `IsWatchPrice bool` - Whether to monitor price

### LiveSpider
Real-time data spider.

Public fields:
- `*utils.ServerIO` - Server IO
- `miners map[string]*Miner` - Map of miners

### SubKLineState
candlestick subscription state.

Public fields:
- `Sid int32` - Trading pair ID
- `NextNotify float64` - Next notification time
- `PrevBar *banexg.Kline` - Previous candlestick

### KLineWatcher
candlestick watcher.

Public fields:
- `*utils.ClientIO` - Client IO
- `jobs map[string]*PairTFCache` - Map of jobs
- `OnKLineMsg func(msg *KLineMsg)` - Callback for receiving candlestick message
- `OnTrade func(exgName, market string, trade *banexg.Trade)` - Callback for receiving trade

### WatchJob
Watch task.

Public fields:
- `Symbol string` - Trading pair
- `TimeFrame string` - Time period
- `Since int64` - Start timestamp

## candlestick Data Related

### NewKlineFeeder
Create a new candlestick data feeder for handling real-time candlestick data.

Parameters:
- `exs *orm.ExSymbol` - Exchange trading pair information
- `callBack FnPairKline` - candlestick data callback function
- `showLog bool` - Whether to display logs

Returns:
- `*KlineFeeder` - candlestick feeder instance
- `*errs.Error` - Error information

### NewDBKlineFeeder
Create a new database candlestick feeder for reading historical candlestick data from database.

Parameters:
- `exs *orm.ExSymbol` - Exchange trading pair information
- `callBack FnPairKline` - candlestick data callback function
- `showLog bool` - Whether to display logs

Returns:
- `*DBKlineFeeder` - Database candlestick feeder instance
- `*errs.Error` - Error information

### NewHistProvider
Create a new historical data provider for managing historical candlestick data retrieval and processing.

Parameters:
- `callBack FnPairKline` - candlestick data callback function
- `envEnd FuncEnvEnd` - Environment end callback function
- `showLog bool` - Whether to display logs
- `pBar *utils.StagedPrg` - Progress bar object

Returns:
- `*HistProvider` - Historical data provider instance

### RunHistFeeders
Run historical candlestick feeder collection for batch processing of historical data.

Parameters:
- `makeFeeders func() []IHistKlineFeeder` - Function to create feeder list
- `versions chan int` - Version control channel
- `pBar *utils.PrgBar` - Progress bar object

Returns:
- `*errs.Error` - Error information

### SortFeeders
Sort or insert candlestick feeders.

Parameters:
- `holds []IHistKlineFeeder` - Existing feeder list
- `hold IHistKlineFeeder` - Feeder to process
- `insert bool` - Whether it's an insert operation

Returns:
- `[]IHistKlineFeeder` - Processed feeder list

### NewLiveProvider
Create a new real-time data provider for handling real-time candlestick data.

Parameters:
- `callBack FnPairKline` - candlestick data callback function
- `envEnd FuncEnvEnd` - Environment end callback function

Returns:
- `*LiveProvider` - Real-time data provider instance
- `*errs.Error` - Error information

## Data Tools Related

### FindPathNames
Find all files with specified suffix in the given path.

Parameters:
- `inPath string` - Input path
- `suffix string` - File suffix

Returns:
- `[]string` - List of file paths
- `*errs.Error` - Error information

### ReadZipCSVs
Read CSV files from ZIP archive.

Parameters:
- `inPath string` - ZIP file path
- `pBar *utils.PrgBar` - Progress bar object
- `handle FuncReadZipItem` - Callback function for processing each CSV file
- `arg interface{}` - Arguments passed to callback function

Returns:
- `*errs.Error` - Error information

### RunSpider
Run data spider service.

Parameters:
- `addr string` - Service listening address

Returns:
- `*errs.Error` - Error information

### NewKlineWatcher
Create a new candlestick data monitor.

Parameters:
- `addr string` - Connection address

Returns:
- `*KLineWatcher` - candlestick monitor instance
- `*errs.Error` - Error information

### RunFormatTick
Run Tick data formatting tool.

Parameters:
- `args *config.CmdArgs` - Command line arguments

Returns:
- `*errs.Error` - Error information

### Build1mWithTicks
Build 1-minute candlesticks using Tick data.

Parameters:
- `args *config.CmdArgs` - Command line arguments

Returns:
- `*errs.Error` - Error information

### CalcFilePerfs
Calculate file performance metrics.

Parameters:
- `args *config.CmdArgs` - Command line arguments

Returns:
- `*errs.Error` - Error information 