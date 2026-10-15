# orm Package

The orm package provides database access and data model definition functionality.

## Runtime Storage and Symbol State

Explicit runners inject `orm.Storage` and `orm.SymbolState` into a Runtime. Storage is an explicit connection and backend-capability dependency, while SymbolState keeps the task's own symbol index, subscriptions, and recovery directory; the entry point that creates shared external resources is responsible for closing them. Package-level APIs such as `Setup` and `Conn` remain for legacy call chains and cannot obtain another Runtime's task state.

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
Unfinished candlestick data, containing raw values before the period closes.

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

`SeriesInfo{Name, TimeFrame, Binding}` describes the source, timeframe, and physical table. `SeriesBinding` contains `Table/TimeColumn/EndColumn/SIDColumn/Fields`; omitted SIDColumn defaults to `sid`. `NewSeriesInfo` uses `ts/end_ms/sid` and a `name_timeframe` table. Logical field types are `float/int/string/bool/json`.

```go
type DataRecord struct {
    Sid int32
    TimeMS, EndMS int64
    Closed bool
    Values map[string]any
}
type DataSeries struct {
    Source string
    Sid int32
    TimeMS, EndMS int64
    TimeFrame string
    Closed, IsWarmUp bool
    Values map[string]any
    ExSymbol *ExSymbol
    Adj *AdjInfo
}
```

DataRecord stores rows; DataSeries carries backtest/live events. Times are milliseconds with `[TimeMS,EndMS)` intervals. Values retain concrete Go types and explicit nil; map lookup distinguishes NULL from an absent key. Storage writes declared schema columns only: int normalizes to int64, float to float64, and JSON uses database encoding. Fixed-column round trips cannot reconstruct original absent keys or every Go type.

### Subscription

`orm.Subscription` declares engine-independent data dependencies; `strat.DataSub` is its alias. Besides Source/ExSymbol/TimeFrame/WarmupNum/Fields/SeriesFields, it has Frequency (bar/event) and Projection (default/all/selected). `NormalizeSubscription` validates declarations; a valid event declaration does not mean every reader supports it. `StreamKey{Source,SID,TimeFrame}` excludes consumer/account; isolation belongs to its catalog/repository.

### SeriesStore and BoundSeriesStore

```go
store := orm.NewSeriesStore(orm.NewSeriesRepo(storage))
rates := store.Bind(info, target)
err := rates.WriteBatch(ctx, rows) // []*orm.DataRecord
events, err := rates.Read(ctx, startMS, endMS, limit) // []*orm.DataSeries
```

The snippet assumes initialized Storage, schema, target, ctx, and time bounds. `Bind` retains definition/target pointers without I/O. Supply info/target on SeriesStore, or omit them on the bound store:

| Method | Purpose |
| --- | --- |
| `Ensure` | Ensure schema |
| `Write/WriteBatch` | Write one/batched persistent rows |
| `WriteSeries/WriteSeriesBatch` | Write runtime events |
| `Read` | Read DataSeries |
| `Missing/FillMissing` | Find gaps and fetch missing data |
| `Coverage/UpdateCoverage` | Coverage and confirmed no-data ranges |
| `Delete` | Delete a time range |

`NormalizeDataRecords` skips nil rows, fills Sid=0, rejects foreign SIDs/invalid intervals, and sorts. `RecordToSeries` normalizes source and borrows Values; `RecordsToSeries` skips nil rows. `SeriesToRecord` and `CloneWithExSymbol` do not recursively clone ownership; asynchronous queues must isolate mutable input. `WithSeriesReadByteLimit` bounds decoded pages, not process RSS.

### KLineSeriesStore and K-Line Field Reads

Independent SeriesStore writes complete records. `NewKLineSeriesInfo` / `NewKLineSeriesStoreWithStorage(info, storage)` define/write extension columns on existing K-lines. `Write(ctx,target,rows)` updates existing `(sid,time)` rows and fails when a target K-line is missing. An extension's Name is a label, not an independent source; subscribe with `Source: "kline"`.

`GetSeries/AutoFetchSeries` return `[]*DataSeries`; explicit Queries provide `GetSeriesFields`, `QuerySeriesFields`, and batched projections. Requested extensions travel with standard OHLCV through Values, feeders, and callbacks. GetOHLCV/AutoFetchOHLCV below are default K-line compatibility views and cannot transport arbitrary extension columns.

### Aggregation and Adjustment

`ResampleSeriesRecords` and `ResampleDataSeries` aggregate fields. `ExSymbol.AggRules` holds JSON field rules; `RegisterAggRule` adds custom rules. Supported rules include first/last/min/max/sum/avg/mid; unconfigured extension fields use last. first/last retain the selected raw value; numeric rules convert types and apply rule-specific NULL/missing-field validation. An aggregate is not a complete copy of input.

`SeriesOHLCV`, `DataSeries.OHLCV`, and `AsKline` are local compatibility views, not replacements for Values. Feeder adjustment copies the field map and currently adjusts open/high/low/close/volume/buy_volume. Other custom fields, quote, and trade_num retain their values; they are not automatically multiplied by a price factor.

### QuestDB WAL

Successful INSERT/CTAS may still be unreadable. Dependent flows wait for expected rows, timestamps, ranges, or counts; timeouts retain recovery markers. Verify the replacement table snapshot before a swap; never DROP the old table based on one empty read. Same-process metadata read-after-write should use targeted visibility waits or owner-local caches/locks.

See [Custom Time-Series Data](../guide/custom_data.md) and [Database](../guide/database.md).

## Database Connection Related

### Setup (Compatibility API)
Initialize database connection pool.

Returns:
- `*errs.Error` - Error information during initialization

### Conn (Compatibility API)
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

## Factor-engine integration

DataSeries.Values retains arbitrary types/NULL/missing keys; RecordToSeries conversion is not asynchronous deep copying. QuestDB visibility waits and replacement verification remain.

[Factor API](factor.md) / [Guide](../guide/factor.md)
