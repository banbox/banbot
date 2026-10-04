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
- `Values map[string]any`: Raw values by field name, including integers, strings, booleans, JSON, explicit nil, and absent keys; not limited to OHLCV

Read original values with `DataFields.RawValue(name)` and its presence flag. `Series(name)` is a derived numeric history with NaN for NULL/missing points. Explicitly selecting large integers for Series converts them to float64 and may lose precision. Databases store declared columns only and can turn absent keys into NULL. JSON network transport also cannot preserve Go integer widths automatically; these boundaries are not lossless copies of the original map.

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

The function bodies above are integration scaffolding: fetch the requested range and start a producer canceled by ctx in the live function. Returning nil alone does not create a subscription. `RegisterFuncDataSource` already registers an instance factory. Complex sources should use `data.RegisterDataSourceFactory(name, factory)` to return independent instances; registering a shared instance alone does not meet explicit Runtime isolation requirements.

Historical data is automatically filled and written to the database by banbot. A live data source must call `sink.Emit(sub, rows)` for every received subscription; do not bypass this entry point to send data directly to strategies. Registration occurs while the strategy package is initialized, so the strategy package containing the registration code must be compiled into the executable before starting the bot.

Bind the definition and target once with `store.Bind(info, target)`, then call `Write/Read/Missing/FillMissing/Coverage`. Binding performs no I/O and does not clone schema, target, or Values; do not mutate definitions concurrently after setup. `strat.NewDataSub(info)` derives a subscription template for an independent series, and `job.Data(sub)` reads its current job-local DataHub view without a database query or waiting. Omitting ExSymbol uses the job's symbol.

When automatic backfill and live subscriptions are not needed, use `Write`, `WriteBatch`, `Read`, `Missing`, and `Delete` on `orm.DefaultSeriesStore()` for manual access. Abstract indicators must also be bound to an `ExSymbol`; use `orm.EnsureExSymbol(...)` to create or reuse one.

## Subscribing and Consuming Data in a Strategy

Return subscriptions from `OnDataSubs` and process the fields in `OnData`:

```go
func init() {
	strat.RegisterStrategy("funding_demo", func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			Name: "funding_demo",
			OnDataSubs: func(job *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{{
					Source:       "funding_rate",
					ExSymbol:     job.Symbol,
					TimeFrame:    "8h",
					WarmupNum:    30,
					Fields:       []string{"rate"},
					SeriesFields: []string{"rate"},
				}}
			},
			OnData: strat.RouteData(strat.DataHandlers{
				Custom: func(job *strat.StratJob, data strat.DataEvent) {
					if data.Source != "funding_rate" {
						return
					}
					rate := data.Float64("rate")
					if !job.DataHub.AllReady() {
						return
					}
					latest := job.DataHub.Get(data.TimeFrame, data.Source, data.Sid)
					_, _ = rate, latest
				},
			}),
		}
	})
}
```

`DataEvent` embeds `*DataFields`, so existing field methods such as `Series` and `Float64` remain directly available. Assign `OnData` directly when the strategy has no auxiliary or custom subscriptions. Use `RouteData(DataHandlers{Main: ..., Info: ..., Custom: ...})` to filter or handle each event class separately. `OnData` cannot be configured together with `OnBar` or `OnInfoBar`; strategy construction fails immediately so primary K-line logic cannot be silently skipped or executed twice.

`TimeFrame` must match the source's `SeriesInfo`. Regular periods use FrequencyBar; irregular streams may declare `TimeFrame: "event"` and `Frequency: orm.FrequencyEvent`, but still need a capable reader. Event WarmupNum counts observations: the source must implement `ObservationWarmupSource.WarmupStart` to resolve a real history start. Missing capability fails instead of backfilling with an invented event interval. At startup, banbot merges duplicate subscriptions by `(source, sid, timeframe)`, merges their field lists, and uses the largest `WarmupNum`. During backtesting, it fills history first, then replays data together with K-lines in time order. During live trading, it fills history first and then activates live subscriptions. An unregistered source, a mismatched timeframe, or a declared subscription without `OnData` or a compatible callback causes startup to fail.

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

## Aggregation, Adjustment, and Lifecycle

Custom fields travel through feeders and serialization in Values; do not reduce them to K-lines and reconstruct them later. `ExSymbol.AggRules` configures first/last/min/max/sum/avg/mid per column, with last as the extension default; register custom rules through `orm.RegisterAggRule`. first/last preserve the selected original value; numeric rules convert and validate NULL according to their semantics. Adjustment changes supported open/high/low/close/volume/buy_volume fields only. Custom values remain present without automatic price-factor scaling.

Both engines merge requirements through SubscriptionPlan. A live candidate generation warms history and buffers live batches, recursively cloning Values and metadata. Budget or validation failures roll back the candidate. Managed sources return Stop/Join handles; the owner seals intake and waits for admitted callbacks. Stopping one source instance must not stop another task.

QuestDB WAL writes are asynchronously visible. Wait for expected rows/ranges before dependent reads, verify replacement snapshots before swapping tables, and retain recovery markers on timeout. See [Database](./database.md) for backend deployment and configuration.

## Factor and cross-sectional engine

Custom columns feed time-series DataHub and factor expression bindings/field nodes. Source/schema/version, event and visibility/reception times must agree; candle timestamps alone do not prove PIT. Preserve concrete types, NULL and missing-key distinctions; do not cast text/NULL silently to zero.

See [Multi-factor strategies](./factor.md) and [Factor API](../api/factor.md).
