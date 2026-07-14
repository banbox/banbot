# Custom Time-Series Data

banbot supports more than K-lines. Funding rates, open interest, on-chain indicators, macro data, and calculated indicators can all be stored as time series and consumed consistently by strategies in backtests and live trading.

## Two Integration Methods

| Scenario | Recommended method |
| --- | --- |
| Data aligns one-to-one with existing K-lines, such as open interest for every K-line | Use `KLineSeriesStore` to add extension columns to `kline_<timeframe>` |
| Data has an independent period or fields, such as an 8-hour funding rate | Create an independent `SeriesInfo` and register a `DataSource` |

Both methods use `DataSeries` at runtime. New strategies should declare requirements with `OnDataSubs` and consume data in `OnData`; `OnBar` remains available for compatibility with traditional OHLCV strategies.

## Data Model

Each independent series is described by `orm.SeriesInfo`. `name + timeframe` generates a table name by default, such as `funding_rate_8h`. Each row is associated with the `sid` of an `ExSymbol` and has start and end times plus field values.

```go
info := orm.NewSeriesInfo("funding_rate", "8h", []orm.SeriesField{
    {Name: "rate", Type: "float", Role: "value"},
    {Name: "next_rate", Type: "float", Role: "value"},
})
```

Field types include `float`, `int`, `string`, `bool`, and `json`. TimescaleDB stores `json` as `JSONB`; QuestDB stores it as a string. Keep fields and timeframes fixed for the same source, and ensure that source names are unique.

The core runtime event fields are:

- `Source`: Data source name, such as `funding_rate`
- `Sid` / `ExSymbol`: Bound trading symbol
- `TimeMS`, `EndMS`, `TimeFrame`: Data coverage interval and base timeframe
- `Values`: Values stored by field name

## Registering a Data Source

The simplest method is a function-based data source. The historical fetch function returns data for a specified time range; the optional live subscription function can be omitted when the source is only used for backtesting and startup backfill.

```go
func init() {
    info := orm.NewSeriesInfo("funding_rate", "8h", []orm.SeriesField{
        {Name: "rate", Type: "float", Role: "value"},
    })
    err := data.RegisterFuncDataSource(info,
        func(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error) {
            // Fetch data from the provider and convert timestamps to milliseconds.
            return []*orm.DataRecord{{
                TimeMS: startMS,
                EndMS:  startMS + 8*60*60*1000,
                Closed: true,
                Values: map[string]any{"rate": 0.0001},
            }}, nil
        },
        func(ctx context.Context, subs []*strat.DataSub, sink data.DataSink) error {
            // Send a batch of DataRecord values to the matching subscription.
            // return sink.Emit(subs[0], rows)
            return nil
        },
    )
    if err != nil {
        panic(err)
    }
}
```

Historical data is automatically filled and written to the database by banbot. A live data source must call `sink.Emit(sub, rows)` for every received subscription; do not bypass this entry point to send data directly to strategies. Registration occurs while the strategy package is initialized, so the strategy package containing the registration code must be compiled into the executable before starting the bot.

When automatic backfill and live subscriptions are not needed, use `Write`, `WriteBatch`, `Read`, `Missing`, and `Delete` on `orm.DefaultSeriesStore()` for manual access. Abstract indicators must also be bound to an `ExSymbol`; use `orm.EnsureExSymbol(...)` to create or reuse one.

## Subscribing and Consuming Data in a Strategy

Return subscriptions from `OnDataSubs` and process the fields in `OnData`:

```go
strat.AddStrat(&strat.TradeStrat{
    Name: "funding_demo",
    OnDataSubs: func(job *strat.StratJob) []*strat.DataSub {
        return []*strat.DataSub{{
            Source:    "funding_rate",
            ExSymbol:  job.Symbol,
            TimeFrame: "8h",
            WarmupNum: 30,
            Fields:       []string{"rate"},
            SeriesFields: []string{"rate"},
        }}
    },
    OnData: func(job *strat.StratJob, data *strat.DataFields) {
        if data.Source() != "funding_rate" {
            return
        }
        rate := data.Float64("rate")
        if !job.DataHub.AllReady() {
            return
        }
        latest := job.DataHub.Get(data.TimeFrame(), data.Source(), data.Sid())
        _, _ = rate, latest
    },
})
```

`TimeFrame` must match the data source's `SeriesInfo`. At startup, banbot merges duplicate subscriptions by `(source, sid, timeframe)`, merges their field lists, and uses the largest `WarmupNum`. During backtesting, it fills history first, then replays data together with K-lines in time order. During live trading, it fills history first and then activates live subscriptions. An unregistered source, a mismatched timeframe, or a declared subscription without `OnData` or a compatible callback causes startup to fail.

`Fields` controls the read projection. `SeriesFields` controls the fields maintained as `banta.Series` and is automatically added to the read projection. When `SeriesFields` is not configured, `float*` fields are converted to Series by default and other fields are kept as the latest value.

Use `job.DataHub.Get(timeframe, source, sid)` to obtain the corresponding `DataFields`. `job.DataHub.AllReady()` only checks periods that should be closed at the current event time and can be used to wait until multiple data sources for the same time have all updated before running strategy logic.

## K-Line Extension Fields

When data is strictly aligned with K-line timestamps, use `orm.NewKLineSeriesInfo(...)` and `orm.NewKLineSeriesStore(...)` to write extension columns. Strategies still read them through the same subscription entry point:

```go
&strat.DataSub{
    Source: "kline", ExSymbol: job.Symbol, TimeFrame: "1h",
    SeriesFields: []string{"open_interest"},
}
```

The default OHLCV fields are preserved. Extension fields can be read with `data.Series("open_interest")` or `data.Float64("open_interest")`. Extension columns cannot use built-in field names such as `sid`, `time`, `ts`, `open`, `high`, `low`, `close`, `volume`, `quote`, `buy_volume`, or `trade_num`.

## Viewing Data

Registered custom data can be viewed on the Data page in WebUI/Dashboard. The viewer is read-only and supports filtering by source, sid, timeframe, time range, and fields. Continue to use `SeriesStore` or the data-source runtime for writing, backfilling, and deleting data.

See [Database](./database.md) for database backend deployment and configuration.
