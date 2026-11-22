# Custom Time-Series Data

banbot supports more than K-lines. Funding rates, open interest, on-chain indicators, macro data, and calculated indicators can all be stored as time series and consumed consistently by strategies in backtests and live trading.

## Two Integration Methods

| Scenario | Recommended method |
| --- | --- |
| Data aligns one-to-one with existing K-lines, such as open interest for every K-line | Use `KLineSeriesStore` to add extension columns to `kline_<timeframe>` |
| Data has an independent period or fields, such as an 8-hour funding rate | Create an independent `SeriesInfo` and register a `DataSource` |

Both methods use `DataSeries` at runtime. New strategies should declare requirements with `OnDataSubs` and consume data in `OnData`; `OnBar` remains available for compatibility with traditional OHLCV strategies.

## Data Model

Each independent series is described by `orm.SeriesInfo`. `name + timeframe` generates a table name by default, such as `my_metric_8h`. Each row is associated with the `sid` of an `ExSymbol` and has start and end times plus field values.

```go
info := orm.NewSeriesInfo("my_metric", "8h", []orm.SeriesField{
    {Name: "value", Type: "float", Role: "value"},
    {Name: "extra", Type: "float", Role: "value"},
})
```

Field types include `float`, `int`, `string`, `bool`, and `json`. TimescaleDB stores `json` as `JSONB`; QuestDB stores it as a string. Keep fields and timeframes fixed for the same source, and ensure that source names are unique.

The core runtime event fields are:

- `Source`: Data source name, such as `my_metric`
- `Sid` / `ExSymbol`: Bound trading symbol
- `TimeMS`, `EndMS`, `TimeFrame`: Data coverage interval and base timeframe
- `Values map[string]any`: Raw values by field name, including integers, strings, booleans, JSON, explicit nil, and absent keys; not limited to OHLCV

Read original values with `DataFields.RawValue(name)` and its presence flag. `Series(name)` is a derived numeric history with NaN for NULL/missing points. Explicitly selecting large integers for Series converts them to float64 and may lose precision. Databases store declared columns only and can turn absent keys into NULL. JSON network transport also cannot preserve Go integer widths automatically; these boundaries are not lossless copies of the original map.

```go
value, exists := fields.RawValue("value") // fields: *strat.DataFields
if !exists {
    // The field is absent from this event.
} else if value == nil {
    // The field is present with a NULL value.
}
```

Database NULL values are read as present keys with nil values; do not convert them to zero. For full K-line row writes, missing `quote`/`buy_volume` default to 0.0, missing `trade_num` defaults to 0, and other missing fields become NULL. Explicit nil differs from omitting these defaulted fields.

## Built-in Data Sources

banbot ships the five sources below. They need no registration; strategies subscribe to them by name. `bot series list` lists them. A custom source cannot reuse these names; registering one fails with `already registered`.

| source | Default timeframe | Supported timeframes | Fields (all float) | Markets |
| --- | --- | --- | --- | --- |
| `funding_rate` | `8h` | `8h` only | `rate` | Perpetual contracts; not spot, delivery futures, or options |
| `open_interest` | `5m` | `5m` `15m` `30m` `1h` `2h` `4h` `6h` `12h` `1d` | `oi`, `oi_value`, `top_ls_pos_ratio`, `top_ls_acc_ratio`, `global_ls_ratio`, `taker_ls_vol_ratio` | Same as `funding_rate` |
| `mark_kline` | `1m` | `1m` `3m` `5m` `15m` `30m` `1h` `2h` `4h` `6h` `8h` `12h` `1d` `3d` `1w` `1M` | `open`, `high`, `low`, `close` | Contracts (including delivery futures and options); not spot |
| `index_kline` | `1m` | Same as `mark_kline` | Same as `mark_kline` | Perpetual and delivery contracts; not spot or options |
| `premium_kline` | `1m` | Same as `mark_kline` | Same as `mark_kline` (basis can be negative) | Perpetual contracts; not spot, delivery futures, or options |

- The subscription `TimeFrame` must be one the source supports, otherwise startup fails. Symbols in unsupported markets return no data.
- Each record becomes visible to strategies at its closing time: a funding rate is visible from its settlement time. Missing `open_interest` metrics are stored as NULL and appear as NaN in `Series()`.
- Live trading backfills history first, then polls the exchange: every 5 minutes for `funding_rate` and `open_interest`, every 1 minute for the three price K-line sources.
- History comes from exchange archives first, with recent data fetched over REST. With `--net-off` or `bt_no_kline_download`, no remote download is made and only local data is read.

Subscription example. A complete runnable strategy is in [banstrats/examples/tsfeatures](https://github.com/banbox/banstrats/tree/main/examples/tsfeatures) (`derivatives.go`):

```go
OnDataSubs: func(job *strat.StratJob) []*strat.DataSub {
    // An omitted ExSymbol means the current job's symbol
    return []*strat.DataSub{
        {Source: orm.SeriesSourceFundingRate, TimeFrame: "8h", WarmupNum: 3},
        {Source: orm.SeriesSourceOpenInterest, TimeFrame: "1h", WarmupNum: 25,
            SeriesFields: []string{orm.FieldOpenInterest}},
    }
},
OnData: strat.RouteData(strat.DataHandlers{
    Custom: func(job *strat.StratJob, d strat.DataEvent) {
        switch d.Source {
        case orm.SeriesSourceFundingRate:
            rate := d.Float64("rate")
            _ = rate
        case orm.SeriesSourceOpenInterest:
            if oi := d.Series(orm.FieldOpenInterest); oi != nil {
                _ = oi.Get(0) // latest open interest; NaN when missing
            }
        }
    },
    Main: func(job *strat.StratJob, _ strat.DataEvent) { /* main K-line logic */ },
}),
```

Backtests backfill subscribed data automatically by gap and write it to the database. You can also download it beforehand:

```shell
bot series down --tables funding_rate,open_interest --pairs BTC --timerange 20240101-20250101
```

`series down` downloads each source only at its default timeframe (`funding_rate` 8h, `open_interest` 5m, price K-lines 1m). Omitting `--tables` processes every registered source, including all built-in ones. Other timeframes are backfilled when a strategy subscribes to them.

### Backtest Funding Settlement

When a backtest order closes, if the database holds `funding_rate` records for the symbol (or the source's events were received during replay), funding is settled both ways from the position notional and the real rate at each settlement time held through: with a positive rate longs pay and shorts receive, and the reverse for a negative rate. Settlement does not require the strategy to subscribe to `funding_rate`, but the data must already be stored (run `series down` above first). With no rate records, it falls back to the annualized approximation from `bt_funding_apr`. Inverse contracts skip settlement when real records exist and use the approximation otherwise. See the [Backtest guide](./backtest.md) and [Configuration guide](./configuration.md).

## Registering a Data Source

The simplest method is a function-based data source. The historical fetch function returns data for a specified time range; the optional live subscription function can be omitted when the source is only used for backtesting and startup backfill.

This example assumes you implement a provider adapter named `fetchMetricRecords(ctx, sub, startMS, endMS)` returning `([]*orm.DataRecord, error)`. It registers historical access only and does not generate live data:

```go
func init() {
    info := orm.NewSeriesInfo("my_metric", "8h", []orm.SeriesField{
        {Name: "value", Type: "float", Role: "value"},
    })
    err := data.RegisterFuncDataSource(info,
        func(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error) {
            return fetchMetricRecords(ctx, sub, startMS, endMS)
        },
        nil,
    )
    if err != nil {
        panic(err)
    }
}
```

The adapter must query `[startMS,endMS)` and return real intervals, millisecond timestamps, `Closed`, and `Values`. The framework fills a zero Sid. Return an error for provider failures; return empty rows with nil only for genuinely queried intervals with no observations. Otherwise failures may be recorded as confirmed no-data coverage and prevent later backfill. Implement `PagedHistorySource` to bound fetches for large ranges.

`RegisterFuncDataSource` registers a factory automatically. For complex sources, use `data.RegisterDataSourceFactory(name, factory)` and return an independent instance each time. A shared instance alone cannot serve independent runtimes. Live sources need a real subscription function that responds to ctx cancellation and calls `sink.Emit(sub, rows)`. Omitting it does not poll the provider and cannot satisfy installations requiring managed live handles.

Historical data is automatically filled and written to the database by banbot. A live data source must call `sink.Emit(sub, rows)` for every received subscription; do not bypass this entry point to send data directly to strategies. Registration occurs while the strategy package is initialized, so the strategy package containing the registration code must be compiled into the executable before starting the bot.

Bind the definition and target once with `store.Bind(info, target)`, then call `Write/Read/Missing/FillMissing/Coverage`. Binding performs no I/O and does not clone schema, target, or Values; do not mutate definitions concurrently after setup. `strat.NewDataSub(info)` derives a subscription template for an independent series, and `job.Data(sub)` reads its current job-local DataHub view without a database query or waiting. Omitting ExSymbol uses the job's symbol.

When automatic backfill and live subscriptions are not needed, use `Write`, `WriteBatch`, `Read`, `Missing`, and `Delete` on `orm.DefaultSeriesStore()` for manual access. Abstract indicators must also be bound to an `ExSymbol`; use `orm.EnsureExSymbol(...)` to create or reuse one.

## Subscribing and Consuming Data in a Strategy

Return subscriptions from `OnDataSubs` and process the fields in `OnData`:

```go
func init() {
	strat.RegisterStrategy("metric_demo", func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			Name: "metric_demo",
			OnDataSubs: func(job *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{{
					Source:       "my_metric",
					ExSymbol:     job.Symbol,
					TimeFrame:    "8h",
					WarmupNum:    30,
					Fields:       []string{"value"},
					SeriesFields: []string{"value"},
				}}
			},
			OnData: strat.RouteData(strat.DataHandlers{
				Custom: func(job *strat.StratJob, data strat.DataEvent) {
					if data.Source != "my_metric" {
						return
					}
					value := data.Float64("value")
					if !job.DataHub.AllReady() {
						return
					}
					latest := job.DataHub.Get(data.TimeFrame, data.Source, data.Sid)
					_, _ = value, latest
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

When data is strictly aligned with K-line timestamps, use `orm.NewKLineSeriesInfo(...)` and `orm.NewKLineSeriesStore(...)` to write extension columns and read them through the same subscription entry point.

Download or write the target K-lines first. Extension writes only UPDATE existing `(sid,time)` rows and fail when a target row is missing; they never create K-lines. Use `NewKLineSeriesInfo` rather than binding a physical K-line table with the independent-series `NewSeriesInfo("kline", ...)`:

```go
info := orm.NewKLineSeriesInfo("oi_ext", "1h", []orm.SeriesField{
    {Name: "oi_ext", Type: "float"},
})
store := orm.NewKLineSeriesStoreWithStorage(info, storage)
if err := store.Write(ctx, target, []*orm.DataRecord{{
    TimeMS: barStartMS,
    Values: map[string]any{"oi_ext": 12345.0},
}}); err != nil {
    return err
}
```

The example assumes `storage`, `target`, `ctx`, and the K-line start timestamp `barStartMS` are already available. The subscription source remains `kline`, rather than the extension definition Name:

```go
&strat.DataSub{
    Source: "kline", ExSymbol: job.Symbol, TimeFrame: "1h",
    SeriesFields: []string{"oi_ext"},
}
```

The default OHLCV fields are preserved. Extension fields can be read with `data.Series("oi_ext")` or `data.Float64("oi_ext")`. Extension columns cannot use built-in field names such as `sid`, `time`, `ts`, `open`, `high`, `low`, `close`, `volume`, `quote`, `buy_volume`, or `trade_num`.

## Manual Backfill and Inspection

```shell
bot series list
bot series down --tables funding_rate,open_interest --pairs BTC --timerange 20240101-20250101
```

First check that the JSON from `list` contains your source definition. It lists compiled-in sources without scanning the database. `down --tables` selects source names (comma-separated), not physical tables such as `funding_rate_8h`; omitting it selects all registered sources, including the built-in `funding_rate`, `open_interest`, `mark_kline`, `index_kline`, and `premium_kline`, so pass `--tables` to limit the scope. Each source is downloaded at its default timeframe only. The current CLI supports regular periods only and clips the end to a closed boundary no later than now. Use a capable source/runtime interface for `event` streams.

## Viewing Data

Registered custom data can be viewed on the Data page in WebUI/Dashboard. The viewer is read-only and supports filtering by source, sid, timeframe, time range, and fields. Without a start time it reads the last 30 days, returning 100 rows by default and at most 1000. Continue to use `SeriesStore` or the data-source runtime for writing, backfilling, and deleting data.

## Aggregation, Adjustment, and Lifecycle

Custom fields travel through feeders and serialization in Values; do not reduce them to K-lines and reconstruct them later. `ExSymbol.AggRules` configures first/last/min/max/sum/avg/mid per column, with last as the extension default; register custom rules through `orm.RegisterAggRule`. first/last preserve the selected original value; numeric rules convert and validate NULL according to their semantics. Adjustment changes supported open/high/low/close/volume/buy_volume fields only. Custom values remain present without automatic price-factor scaling.

Live sources must own their subscription resources. When managed lifecycle is required, implement `ManagedLiveSource` and return independent `Stop()`/`Join() error` handles. Stop production and wait for shutdown on cancellation; stopping one instance must not close another task. Warmup or live startup failure prevents the new subscription from activating.

QuestDB WAL writes are asynchronously visible. Wait for expected rows/ranges before dependent reads, verify replacement snapshots before swapping tables, and retain recovery markers on timeout. See [Database](./database.md) for backend deployment and configuration.

## Factor and cross-sectional engine

Custom columns feed time-series DataHub and factor expression bindings/field nodes. Source/schema/version, event and visibility/reception times must agree; candle timestamps alone do not prove PIT. Preserve concrete types, NULL and missing-key distinctions; do not cast text/NULL silently to zero.

See [Multi-factor strategies](./factor.md) and [Factor API](../api/factor.md).
