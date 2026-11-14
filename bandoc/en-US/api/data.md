# data Package

`data` manages historical replay, live ingestion, subscription requirements, and source lifecycles. Providers, feeders, and watchers carry `orm.DataSeries`; both standard K-line fields and extension fields use `Values map[string]any`. `FnDataSeries` and `FuncEnvEnd` are `func(*orm.DataSeries)`.

## Generic Time-Series Data

### DataSource and DataSink

```go
type DataSink interface {
    Emit(sub *orm.Subscription, rows []*orm.DataRecord) error
}
type DataSource interface {
    Info() *orm.SeriesInfo
    FetchHistory(ctx context.Context, sub *orm.Subscription, startMS, endMS int64) ([]*orm.DataRecord, error)
    SubscribeLive(ctx context.Context, subs []*orm.Subscription, sink DataSink) error
}
```

`strat.DataSub` is an alias of `orm.Subscription`; function-source callbacks may use it. `FetchHistory` returns records with real intervals for `[startMS,endMS)`; do not invent records to cover unqueried ranges. Emit live batches through `sink.Emit`. Omitting a live function does not enable automatic polling.

Return an error when a fetch fails. Empty rows with nil mean the interval was genuinely queried and contains no data; it may be recorded as covered. Confusing these outcomes prevents later backfill. Keep the Info definition and base timeframe stable after registration.

### PagedHistorySource

```go
type PagedHistorySource interface {
    FetchHistoryPage(ctx context.Context, sub *orm.Subscription,
        startMS, endMS int64, limit int) ([]*orm.DataRecord, error)
}
```

Each page must have strictly increasing timestamps within `[startMS,endMS)` and at most limit rows. An empty page ends the range. `ReadSourceHistory` validates pages before passing them to the consumer. With PageBytes enabled, a source without this interface fails before fetching the entire range; without it, full-range `FetchHistory` remains available. Adapters must still bound network responses and decoder buffers themselves; a page budget is not a process memory limit.

### DataSourceCatalog

`NewDataSourceCatalog()` creates an independent registry. `RegisterDataSource` registers an instance; `RegisterDataSourceFactory(name, factory)` registers a factory for independent instances; `RegisterFuncDataSource(info, fetch, subscribe)` registers a function-source factory automatically. Source names are unique within each catalog.

Explicit Runtimes use `RuntimeCatalogFromRegisteredSources()` to create local instances and status from process registrations. A source registered only through package-level `RegisterDataSource`, without a factory, is rejected. Register a factory for a complex source and return a fresh instance each time so stopping one task does not stop another.

`GetDataSource`, `ListDataSources`, and `ListDataSourceStatus` have catalog methods and package-level compatibility entry points. The latter cannot retrieve another Runtime's state.

### SeriesRuntime / SeriesPlan

Traditional TS tasks use `NewSeriesRuntimeWithRuntimeDeps(deps, sink)` and `Plan/Ensure/Apply/ActivateNew` to collect non-primary K-line subscriptions, fill coverage, and activate new sources. `HistSeriesFeeder` replays independent series with K-lines in visibility order. `ThirdPartySeriesBootstrap` is a compatibility alias of `SeriesPlan`.

### SubscriptionPlan

Both engines use `catalog.CompileSubscriptionPlan(ctx, requests, options)` to merge `(source,sid,timeframe)` requirements, preserving consumer required/freshness/warmup requirements, unioning field projections, and taking the maximum warmup.

```go
type SubscriptionPlanOptions struct {
    Namespace string
    AnchorMS, EndMS int64
    PageRows, PrefetchRows int
    PageBytes int64
    RequireManagedLive bool
}
```

`Streams/Subscriptions/SourceMetadata/BudgetReport` expose plan views; `Bootstrap(ctx, repo)` fills history. PageBytes bounds decoded input pages, not process heap memory. Event streams use `FrequencyEvent` and `TimeFrame: "event"`; count-based warmup requires `ObservationWarmupSource.WarmupStart`, rather than multiplying observations by an invented interval.

### Live Installation

`PrepareLivePlan` prepares independent subscriptions, warmup, and startup buffering. `SubscriptionInstallation.Activate` activates them; `CommitPrepared` supplies a commit callback boundary. `InstallLivePlan` prepares and activates. Sources requiring managed live operation implement `ManagedLiveSource.SubscribeManaged` and return an independent handle with `Stop()` and `Join() error`; `LiveSourceErrors` can report asynchronous failure.

Startup buffering preserves Values fields, types, and NULL/absent-key distinctions. Do not mutate inputs concurrently after passing them to the sink. Required-source failure, budget overflow, or cancellation fails installation; optional-source failure can instead degrade the installation, so inspect its status. Stop managed subscriptions with `Stop`, then `Join` to wait for producers and admitted callbacks; a callback must not wait for itself. Managed handles use `Stop()` / `Join() error`, unlike `LiveProvider.Stop() *errs.Error` / `Join()`.

## Providers and Feeders

| Current type | Responsibility |
| --- | --- |
| `Feeder` / `SeriesFeeder` | Multi-period input, warmup, aggregation, and callbacks for one symbol |
| `DBSeriesFeeder` | Database replay batches advanced with `GetBatch/RunBatch/CallNext` |
| `TfSeriesLoader` | Reads and seeks one symbol/timeframe |
| `HistSeriesFeeder` | Historical replay of an independent source |
| `HistProvider` | Owns historical feeders and advances by end time |
| `LiveProvider` | Owns live feeders and a `SeriesWatcher` |

Interfaces are `IDataFeeder`, `IHistFeeder`, and `IHistDataFeeder`. Main constructors:

```go
NewSeriesFeeder(exs *orm.ExSymbol, callback FnDataSeries, showLog bool) (*SeriesFeeder, *errs.Error)
NewDBSeriesFeeder(exs *orm.ExSymbol, callback FnDataSeries, showLog bool) (*DBSeriesFeeder, *errs.Error)
NewHistProvider(callback FnDataSeries, envEnd FuncEnvEnd, getEnd FnGetInt64, showLog bool, progress *utils.StagedPrg) *HistProvider
NewLiveProvider(callback FnDataSeries, envEnd FuncEnvEnd) (*LiveProvider, *errs.Error)
```

Use `WithRuntimeDeps` constructors for explicit tasks, supplying the Runtime's configuration, symbols, storage, clock, strategies, and callback boundary. Providers support `SubWarmPairs/UnSubPairs/LoopMain`; live resources support `Stop/Join`. `Feeder.CallBack` and `Feeder.OnEnvEnd` both receive DataSeries; the pending value is `WaitData`.

```go
func NewHistProviderWithRuntimeDeps(deps *RuntimeDeps, callback FnDataSeries,
    envEnd FuncEnvEnd, getEnd FnGetInt64, showLog bool,
    progress *utils.StagedPrg) (*HistProvider, *errs.Error)
func NewLiveProviderWithRuntimeDeps(deps *RuntimeDeps, callback FnDataSeries,
    envEnd FuncEnvEnd) (*LiveProvider, *errs.Error)
```

`NewHistProviderWithRuntimeDeps` returns initialization errors, unlike the package-level compatibility constructor; callers must handle them. Strategy authors normally declare `OnDataSubs` and `OnData` without creating providers manually. `DataHub` belongs to the `strat` package and is owned by the job; there is no `data.DataHub` type.

Built-in K-lines read the smallest physical period and aggregate derived periods. Merged projections pass extension fields through the same Values map. Storage enrichment fills absent keys without overwriting explicit NULL. Aggregation follows field rules, and adjustment changes only supported fields without dropping custom fields. For different time ranges, create separate providers or read through SeriesStore/Queries.GetSeriesFields.

## Spider and Watcher

`LiveSpider` manages a `Miner` for each exchange/market, collecting, storing, and broadcasting data. The current message is `NotifySeries{TFSecs, Interval, Rows []*orm.DataSeries}`; `SeriesMsg` embeds it and adds `ExgName/Market/Pair`. The storage task is `SaveSeries`.

`NewSeriesWatcherWithRuntimeDeps(deps, addr)` creates the task's TCP client, with `OnDataMsg func(*SeriesMsg)` receiving series. `WatchJobs` declares exchange, market, type, symbol, and timeframe; `UnWatchJobs` cancels them. Spider/Watcher send complete Rows through BanIO. Network payloads use JSON, which does not guarantee integer widths or arbitrary Go types inside maps survive automatically. Strictly typed sources need a schema and decode validation.

## Data Tools

`FindPathNames` and `ReadZipCSVs` handle files; `RunFormatTick/Build1mWithTicks` handle trades; `CalcFilePerfs` analyzes files. See [Custom Time-Series Data](../guide/custom_data.md) for storage and registration and [Factor API](factor.md) for native factor graphs and backtest/live composition.

Use `bot series list` to inspect registered definitions and `bot series down --tables funding_rate --pairs BTC --timerange 20240101-20250101` for explicit backfill. Here `--tables` selects source names; this CLI does not support event timeframes. See [Series Tools](../advanced/kline_tools.md) for K-lines and coverage repair.
