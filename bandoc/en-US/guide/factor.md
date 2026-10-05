# Multi-factor and cross-sectional strategies

Banbot has two engines: `time_series` drives per-symbol `TradeStrat` callbacks; `factor` freezes a Universe each decision round, evaluates factor nodes and cross-sectional transforms, combines scores and builds a target portfolio. Both use the same YAML and the ordinary backtest/trade entries. Omitting engine retains time-series behavior.

## Data, factors and portfolios

A native graph contains field, time-series and cross-sectional nodes. Named outputs combine using equal, fixed or historical-IC weights before a default top/bottom-k or registered portfolio builder produces TargetPortfolio. Full replaces a complete target; Patch changes only declared instruments and retains omitted holdings.

Universe distinguishes investable, reference, tradable, evaluation and tracked sets. Reference membership does not grant trading permission. Compatible namespace, clock, sampling, plan and snapshot consumers can share computation while targets, budgets and accounts remain independent.

All input travels through `orm.DataSeries.Values map[string]any`, retaining custom columns, concrete types, NULL and missing-key distinctions. Numerical indicator views do not replace raw Values.


`backtest`/`trade`/`research` locate base market configuration through `--datadir` or `BanDataDir` by default. To skip default files, pass `--no-default --config /absolute/config.yml`; `@`/`$` configuration paths still require a data directory. Archive, output and account resource requirements still apply.

## Unified commands

`banbot backtest` and `banbot trade` load one `run_policy` and select the time-series, factor or mixed engine path. There is no separate factor startup command to select. Backtest mode comes from `--mode weights|events`, then `execution.mode`, then `events`; mixed replay requires `events`. A time-series-only run keeps its existing backtest behavior.

Use root `research --config strategy.yml` for factor diagnostics, `data archive --input records.jsonl --out chunk.gob` for version archives, and root `validate --spec formula.yml` / `explain --spec formula.yml` for standalone expressions. Strategy configuration uses unified YAML only.

For factor or mixed configurations, `trade --dry-run` selects historical `events` replay, not live paper trading. Time-series-only live simulation continues to use YAML `env: dry_run`; do not use the historical replay flag for that workflow. Real factor or mixed trading accepts `--live-provider` and requires a verified current-session binding. Strategy configuration uses YAML `run_policy`.

Each task owns its explicit `runtime.Runtime`, configuration snapshot, clock, strategy state and cancellation context. Runtime isolation does not create an independent exchange balance: strategies intentionally bound to the same account share its execution coordinator and retain strategy-level attribution and capital budgets. Independent tasks should use distinct account/resource identities when they must not share execution. Cancellation stops intake, then joins in-flight callbacks and computation before releasing resources; releasing one consumer preserves other active borrowers.

## Configure the built-in definition

Add this overlay to an existing database, exchange, account, symbol-pool and time_range configuration. It is not a complete standalone market configuration:

```yaml
data:
  pit_policy: static-approximation
execution:
  mode: weights
  funding_policy: explicit-zero
run_policy:
  - name: momentum-vol
    id: momentum
    engine: factor
    run_timeframes: [1h]
    params: {window: 24, k: 10}
```

A factor policy accepts one decision timeframe. A 24-period momentum window needs at least 25 closed observations. Latest-value storage cannot reconstruct historical revisions/publication times: static-approximation is an explicit research assumption. Strict PIT requires an immutable version archive or an attested version-aware historical provider.

No version marker is required; ordinary loading leaves the original file unchanged. Explicit `engine: time_series` or `engine: factor` selects the new engine semantics; legacy policies without an engine retain custom parameters. id is a stable strategy identity; account chooses an account; capital_weight allocates capital among same-account strategies and is independent of stake_rate.

## Write multiple expressions

Expressions compile to native nodes at startup. Replace the policy above with this two-output definition; outputs share underlying field and temporal nodes:

```yaml
run_policy:
  - name: MyFactors
    id: my_factors
    engine: factor
    run_timeframes: [1h]
    params: {k: 3}
    portfolio: {long_notional: 0.5, short_notional: 0.5, mode: full}
    expressions:
      schema_version: 1
      timeframe: 1h
      bindings:
        kline: {source: kline, timeframe: 1h}
      params: {window: 24}
      outputs:
        momentum: 'cs.zscore(ts.return(positive(kline.close), param.window))'
        momentum_rank: 'cs.rank(ts.return(positive(kline.close), param.window))'
      combine: {method: equal}
```

Policy params.k controls portfolio selection; expressions.params.window controls formula history. They are not copied into one another. Expression policies need no same-name Go definition; do not also specify definition. Historical-IC uses only matured, decision-visible history; the current live driver rejects history-ic.

For Go strategies register `runner.RegisterDefinition(name, builder)`, where builder is `func(runner.Config) (*factor.Plan, research.ComboSpec, error)`. Register a uniquely versioned portfolio builder with RegisterPortfolioBuilder. Declare node versions and missing-data policies. See [Factor API](../api/factor.md).

## Backtest and research

```sh
./bot backtest --config base.yml --config factors.yml
./bot research --config base.yml --config factors.yml
./bot backtest --mode weights --config base.yml --config factors.yml
./bot backtest --mode events --config base.yml --config factors.yml
```

| Mode | Behavior |
| --- | --- |
| research | Panels, matured labels and diagnostics; no execution account |
| weights | Quantity retention, changed-notional costs and approximate portfolio book |
| events | Shared accounts, discrete quantity/price constraints, margin/risk validation and paper fills |

Execution prices are separate from factor inputs. Events needs tick/event or an explicit observable 1m price stream, normalized instrument units for every SID and account risk limits. A completed coarse candle cannot reconstruct intrabar fills. Prices must be strictly after decision completion plus LatencyMS and before exclusive expiry. explicit-zero declares zero funding; required-stream requires settlement streams for every required instrument, including explicit known-zero records.

For archive input set archive and match Universe, SIDMap, schemas, source versions and price streams to the file:

```sh
./bot data archive --input records.jsonl --out chunk.gob --max-records 100000
```

Input is factor.VersionRecord JSON lines. Use --schema fields.yml when concrete integer widths matter. Raw revisions remain available; round snapshots apply event-time, available/publication and local-reception gates. Archive DecisionDelayMS advances visibility cutoff while retaining decision cadence. Live uses actual receipt time.

The default executable-return label spans one decision interval. Override research.labels as needed; the archive driver currently supports one executable-return horizon. Only matured labels enter IC/diagnostics, never current inference. Result.Unresolved counts labels beyond the available data. Fixed/equal trading may disable labels with research: {labels: []}; research/history-ic may not.

## Mixed time-series and factor replay

Place both engines in one run_policy list. Mixed replay requires events. Give stable id/account and explicit capital_weight to multiple same-account strategies, and supply each factor policy's execution-price, funding and risk evidence:

```yaml
execution: {mode: events, funding_policy: explicit-zero}
run_policy:
  - name: YourRegisteredTS
    engine: time_series
    id: ts_alpha
    account: default
    capital_weight: 0.5
    run_timeframes: [1h]
  - name: momentum-vol
    engine: factor
    id: cs_alpha
    account: default
    capital_weight: 0.5
    run_timeframes: [1h]
    params: {window: 24, k: 3}
```

Replace YourRegisteredTS with a registered project strategy. Net execution retains strategy attribution and sent allocations. It does not merge mutable strategy state. Stop/Join releases only the caller's borrow, preserving other account/session consumers.

## Live integration and limits

verified-session is an application example name, not a built-in provider. Register entry.RegisterFactorLiveBinding("verified-session", factory) with real session verification first. Empty/banexg selects the built-in adapter, which still fails on missing capabilities.

Provider precedence is explicit CLI selection, `accounts.<name>.live_provider`, `execution.live_provider`, then the built-in default. Keep account overrides in root `accounts`; each account can select its own registered binding. Startup validates every provider before opening sessions.

Remove archive input and select execution.live_provider: verified-session. Start with `./bot trade --config live.yml`. An embedding program registers entry.RegisterFactorLiveBinding using the current session's verified banexg transport, canonical symbol metadata, publication/revision mapping and funding-policy verification.

**A stock session without this verified binding fails startup explicitly. YAML alone does not enable real factor trading; there is no automatic paper fallback. This guide does not claim end-to-end real-venue acceptance.** trade --dry-run is a separate historical paper replay.

Startup compiles requirements/plans, prepares history warmup and current subscriptions, reconciles the account and commits the generation. Round barriers consume closed and locally visible records, then Flush after completion/timeout according to missing-data policies. A failed candidate preserves the old generation. Stop cancels intake; Join waits for callbacks/computation before shared services are released.

Budgets come from reconciled account state; InitialNAV deposits no live funds. Existing cash/positions need ledger attribution and startup reconciliation. Required live funding records contain exact decimal strings mark, rate, account_amount and stable settlement_id. Duplicate settlements are idempotent; settlements arriving after position changes need historical reconciliation. explicit-zero requires verified absence of funding obligations.

## Results and troubleshooting

Factor-engine runs stream panels, decisions, matured diagnostics and final scalar summaries as JSON lines. Ordinary backtests also write resolved.json with defaults/origins, account-&lt;account&gt;/manifest.json and versioned event/posting Gob chunks. Completion follows output sync/close and resource cleanup; primary and cleanup errors remain visible.

execution.history: cold/history.sqlite optionally archives settled simulated history. The path must be new; it is not a resume snapshot and is refused for real trade/durable execution. Small replays default to MemoryStore. page_bytes bounds decoded logical payload rather than process RSS.

Check version, single timeframe, registered definition, Universe/SID/schema, price timeframe, funding and account units when startup fails. Preflight creates no exchange, storage, account or output directory. bot tool bt_factor is the legacy orders.gob rolling-selection tool, separate from the factor runner.

See [Configuration](./configuration.md), [Backtesting](./backtest.md), [Live Trading](./live_trading.md) and [Custom Data](./custom_data.md).

## Complete storage replay configuration

Save this structurally complete weights example as factors.yml. Replace the local database URL, market and symbols; execution needs actual readable storage, imported data and matching metadata. Resource-free preflight proves configuration/static assembly, not data availability or provider capabilities.

```yaml
time_start: '20240101'
time_end: '20240201'
exchange: {name: binance}
market_type: linear
pairs: ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT']
stake_currency: [USDT]
wallet_amounts: {USDT: 10000}
database:
  db_type: questdb
  url: postgresql://admin:quest@127.0.0.1:8812/qdb?sslmode=disable
  auto_create: false
data:
  pit_policy: static-approximation
  page_rows: 1000
  max_records: 100000
execution:
  mode: weights
  funding_policy: explicit-zero
run_policy:
  - name: momentum-vol
    id: momentum
    engine: factor
    run_timeframes: [1h]
    params: {window: 24, k: 1}
```

```sh
./bot backtest --config factors.yml
```

For expressions replace run_policy with the earlier multi-output policy, retaining the root settings. Events/live additionally need account risk, units and verified capability evidence; changing mode alone does not establish readiness.
