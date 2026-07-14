# orm Package

The orm package provides database access and data model definition functionality.

## Important Structures

### AdjFactor
Price adjustment factor structure, used for handling forward and backward price adjustments.

Fields:
- `Sid int32` - Trading pair ID
- `SubID int32` - Sub trading pair ID
- `StartMs int64` - Start timestamp (milliseconds)
- `Factor float64` - Adjustment factor value

### Calendar
Trading calendar structure, used to record trading time periods.

Fields:
- `Market string` - Market or calendar name
- `StartMs int64` - Start timestamp (milliseconds)
- `StopMs int64` - End timestamp (milliseconds)

### ExSymbol
Trading pair information structure, containing basic information about exchange and trading pair.

Fields:
- `ID int32` - Trading pair ID
- `Exchange string` - Exchange name
- `ExgReal string` - Actual exchange identifier
- `Market string` - Market type
- `Symbol string` - Trading pair symbol
- `Combined bool` - Whether it's a combined trading pair
- `ListMs int64` - Listing timestamp (milliseconds)
- `DelistMs int64` - Delisting timestamp (milliseconds)
- `AggRules string` - JSON aggregation rules for custom fields

### InsKline
candlestick insertion task structure, used to manage candlestick data insertion operations.

Fields:
- `Sid int32` - Trading pair ID
- `Timeframe string` - Time period
- `Ts time.Time` - Task creation time
- `StartMs int64` - Start timestamp (milliseconds)
- `StopMs int64` - End timestamp (milliseconds)

### SRange
Time-series coverage range structure, used to record covered or confirmed no-data ranges for K-lines and custom series.

Fields:
- `Sid int32` - Trading pair ID
- `Table string` - Physical time-series table name
- `Timeframe string` - Time period
- `StartMs int64` - Start timestamp (milliseconds)
- `StopMs int64` - End timestamp (milliseconds)
- `HasData bool` - Whether this range contains valid data

### KlineUn
Unadjusted candlestick data structure, containing raw candlestick data.

Fields:
- `Sid int32` - Trading pair ID
- `ExpireMs int64` - Expiration timestamp for unfinished K-lines (milliseconds)
- `StartMs int64` - Start timestamp (milliseconds)
- `StopMs int64` - End timestamp (milliseconds)
- `Timeframe string` - Time period
- `Open float64` - Opening price
- `High float64` - Highest price
- `Low float64` - Lowest price
- `Close float64` - Closing price
- `Volume float64` - Trading volume
- `Quote float64` - Quote volume
- `BuyVolume float64` - Taker buy volume
- `TradeNum int64` - Number of trades

### InfoKline
candlestick data structure with additional information.

Fields:
- `PairTFKline *banexg.PairTFKline` - Base candlestick data
- `Sid int32` - Trading pair ID
- `Adj *AdjInfo` - Price adjustment information
- `IsWarmUp bool` - Whether it's warm-up data

### AdjInfo
Price adjustment information structure, containing detailed information about price adjustments.

Fields:
- `ExSymbol *ExSymbol` - Trading pair information
- `Factor float64` - Original adjacent adjustment factor
- `CumFactor float64` - Cumulative adjustment factor
- `StartMS int64` - Start timestamp (milliseconds)
- `StopMS int64` - End timestamp (milliseconds)

### KlineAgg
candlestick data aggregation configuration structure, used to manage candlestick aggregation for different time periods.

Fields:
- `TimeFrame string` - Time period
- `MSecs int64` - Period in milliseconds
- `Table string` - Data table name
- `AggFrom string` - Aggregation source
- `AggStart string` - Aggregation start time
- `AggEnd string` - Aggregation end time
- `AggEvery string` - Aggregation interval
- `CpsBefore string` - Completion deadline
- `Retention string` - Data retention time

### SeriesInfo, DataRecord, and DataSeries

Generic time-series data is no longer limited to K-lines. `SeriesInfo` defines the source, timeframe, table binding, and fields; `DataRecord` stores rows; and `DataSeries` is the shared event model for backtesting and live trading. `SeriesStore` provides high-level `Write`, `WriteBatch`, `Read`, `Missing`, `Delete`, and `Coverage` operations, while `SeriesRepo` adapts them to TimescaleDB and QuestDB.

Supported field types are `float`, `int`, `string`, `bool`, and `json`. `NewSeriesInfo(name, timeframe, fields)` creates a table binding using the default rules. To write extension columns to an existing K-line table, use `NewKLineSeriesInfo` and `KLineSeriesStore`.

See [Custom Time-Series Data](../guide/custom_data.md) for the complete registration, storage, and strategy-consumption workflow.

## Database Connection Related

### Setup
Initialize database connection pool.

Returns:
- `*errs.Error` - Error information during initialization

### Conn
Get database connection and query object.

Parameters:
- `ctx context.Context` - Context object for controlling request lifecycle

Returns:
- `*Queries` - Database query object
- `*pgxpool.Conn` - Database connection object
- `*errs.Error` - Error information

### SetDbPath
Set database path.

Parameters:
- `key string` - Database identifier key
- `path string` - Database file path

### DbLite
Create a local banbot SQLite auxiliary database connection. It is only used for local auxiliary state; market and generic time-series data use the configured QuestDB or TimescaleDB through `Setup`/`Conn`.

Parameters:
- `src string` - Data source name
- `path string` - Database file path
- `write bool` - Whether writable

Returns:
- `*sql.DB` - Database connection object
- `*errs.Error` - Error information

### NewDbErr
Create database error object.

Parameters:
- `code int` - Error code
- `err_ error` - Original error

Returns:
- `*errs.Error` - Formatted error information

## Exchange Related

### LoadMarkets
Load exchange market data.

Parameters:
- `exchange banexg.BanExchange` - Exchange interface
- `reload bool` - Whether to force reload

Returns:
- `banexg.MarketMap` - Market data mapping
- `*errs.Error` - Error information

### InitExg
Initialize exchange configuration.

Parameters:
- `exchange banexg.BanExchange` - Exchange interface

Returns:
- `*errs.Error` - Error information

## Trading Pair Related

### GetExSymbols
Get all trading pair information for specified exchange and market.

Parameters:
- `exgName string` - Exchange name
- `market string` - Market name

Returns:
- `map[int32]*ExSymbol` - Mapping from trading pair ID to trading pair information

### GetExSymbolMap
Get all trading pair information for specified exchange and market (keyed by trading pair name).

Parameters:
- `exgName string` - Exchange name
- `market string` - Market name

Returns:
- `map[string]*ExSymbol` - Mapping from trading pair name to trading pair information

### GetSymbolByID
Get trading pair information by ID.

Parameters:
- `id int32` - Trading pair ID

Returns:
- `*ExSymbol` - Trading pair information

### GetExSymbolCur
Get trading pair information for current default exchange.

Parameters:
- `symbol string` - Trading pair name

Returns:
- `*ExSymbol` - Trading pair information
- `*errs.Error` - Error information

### GetExSymbol
Get trading pair information for specified exchange.

Parameters:
- `exchange banexg.BanExchange` - Exchange interface
- `symbol string` - Trading pair name

Returns:
- `*ExSymbol` - Trading pair information
- `*errs.Error` - Error information

### EnsureExgSymbols
Ensure trading pair information for exchange is loaded.

Parameters:
- `exchange banexg.BanExchange` - Exchange interface

Returns:
- `*errs.Error` - Error information

### EnsureCurSymbols
Ensure trading pair information for current exchange is loaded.

Parameters:
- `symbols []string` - List of trading pair names

Returns:
- `*errs.Error` - Error information

### EnsureSymbols
Ensure trading pair information for specified exchanges is loaded.

Parameters:
- `symbols []*ExSymbol` - List of trading pair information
- `exchanges ...string` - List of exchange names

Returns:
- `*errs.Error` - Error information

### LoadAllExSymbols
Load all trading pair information.

Returns:
- `*errs.Error` - Error information

### GetAllExSymbols
Get all loaded trading pair information.

Returns:
- `map[int32]*ExSymbol` - Mapping from trading pair ID to trading pair information

### InitListDates
Initialize listing date information for trading pairs.

Returns:
- `*errs.Error` - Error information

### EnsureListDates
Ensure listing date information for trading pairs is loaded.

Parameters:
- `sess *Queries` - Database query object
- `exchange banexg.BanExchange` - Exchange interface
- `exsMap map[int32]*ExSymbol` - Trading pair mapping
- `exsList []*ExSymbol` - Trading pair list

Returns:
- `*errs.Error` - Error information

### ParseShort
Parse short format trading pair name.

Parameters:
- `exgName string` - Exchange name
- `short string` - Short format trading pair name

Returns:
- `*ExSymbol` - Trading pair information
- `*errs.Error` - Error information

### MapExSymbols
Map trading pair name list to trading pair information mapping.

Parameters:
- `exchange banexg.BanExchange` - Exchange interface
- `symbols []string` - List of trading pair names

Returns:
- `map[int32]*ExSymbol` - Mapping from trading pair ID to trading pair information
- `*errs.Error` - Error information

## candlestick Data Related

### AutoFetchOHLCV
Automatically fetch candlestick data, supporting data completion and unfinished candlesticks.

Parameters:
- `exchange banexg.BanExchange` - Exchange interface
- `exs *ExSymbol` - Trading pair information
- `timeFrame string` - Time frame
- `startMS int64` - Start time (milliseconds)
- `endMS int64` - End time (milliseconds)
- `limit int` - Limit count
- `withUnFinish bool` - Whether to include unfinished candlesticks
- `pBar *utils.PrgBar` - Progress bar

Returns:
- `[]*AdjInfo` - Price adjustment information
- `[]*banexg.Kline` - candlestick data
- `*errs.Error` - Error information

### GetOHLCV
Get candlestick data.

Parameters:
- `exs *ExSymbol` - Trading pair information
- `timeFrame string` - Time frame
- `startMS int64` - Start time (milliseconds)
- `endMS int64` - End time (milliseconds)
- `limit int` - Limit count
- `withUnFinish bool` - Whether to include unfinished candlesticks

Returns:
- `[]*AdjInfo` - Price adjustment information
- `[]*banexg.Kline` - candlestick data
- `*errs.Error` - Error information

### BulkDownOHLCV
Bulk download candlestick data.

Parameters:
- `exchange banexg.BanExchange` - Exchange interface
- `exsList map[int32]*ExSymbol` - Trading pair list
- `timeFrame string` - Time frame
- `startMS int64` - Start time (milliseconds)
