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

## Independent rebalancing and holding lifecycle

Enable `portfolio.policy: lifecycle-v1` to configure rebalancing, selection, holding, transitions and allocation independently. Omitting policy retains the original builder and weight targets.

| Omitted setting | lifecycle-v1 default |
| --- | --- |
| rebalance | every_bars: 1; civil calendars use UTC unless specified and require calendar_version |
| selection | Omitting the entire selection uses params.k for enabled long/short sides; missing_scores: skip |
| holding | No minimum/maximum or cooldown; age limits require first-fill evidence or explicit adopt for existing positions |
| transition | direct; linear-exit/geometric use on_reselect: restore and require an explicit basis |
| cohort | startup: gradual, sizing: entry-nav, entry_window_bars equal to every_bars |
| allocation | equal, no reserve or extra caps; explicit builders retain weights when allocation is omitted |

K and quantile selection are exclusive on each side; quantiles do not require legacy params.k. missing_scores accepts skip/shrink/cash: skip ordinary selection with insufficient scores, reduce selection counts, or request cash. retain_rank adds a rank buffer; dropout limits dropped holdings exiting per side per round, starting with the worst ranks. Pure research has no default position book. The following policy examples apply to weights/events or verified live; ordinary CLI research uses a configuration without policy. An embedding enabling a research policy must supply `runner.PolicySink`. adopt starts age at the current grid rather than recovering an unknown historical fill time.

This overlay uses a 1h base clock and rebalances every two bars. After the first actual fill has aged at least 16 bars, a position that drops out exits over eight eligible rounds:

```yaml
run_policy:
  - name: momentum-vol
    id: rotation
    engine: factor
    run_timeframes: [1h]
    params: {window: 24, k: 10}
    portfolio:
      long_notional: 1.0
      short_notional: 0.0
      policy: lifecycle-v1
      rebalance: {every_bars: 2}
      holding: {min_bars: 16}
      transition: {mode: linear-exit, exit_steps: 8, basis: quantity}
      allocation: {method: equal, reserve_ratio: 0.02}
```

Retain your market/data/account settings. Quantity basis anchors the actual quantity when exit starts, so a NAV increase does not buy back the exiting position. Weight basis decays weights and values them against current NAV. New entries use current strategy NAV; tail positions consume budget first. Target admission and fills are distinct, and an unfilled reduction does not release cash.

For a new cohort every two hours, each with a planned sixteen-hour lifetime, replace holding/transition with:

```yaml
rebalance: {every_bars: 2}
transition: {mode: cohort, period_bars: 16, startup: gradual, sizing: entry-nav}
```

gradual initially funds one cohort; seed-all creates all initial cohorts from current rankings. entry-nav preserves entry quantities; current-nav revalues active cohorts. Cohorts expire on their planned windows and require period_bars to be a multiple of every_bars. This produces a different path from starting eight exit steps after a sixteen-hour minimum holding period.

| Dimension | Built-in capabilities |
| --- | --- |
| rebalance | every_bars, anchor/phase, duration; daily/weekly/monthly civil calendars with timezone/version |
| selection | Independent long/short K or quantile, retain_rank, dropout, group_quota and missing scores |
| holding | Minimum/maximum bars or duration, by_asset, cooldown_bars and explicit adopt |
| transition | direct, linear-exit, cohort, geometric, target-step; mode-specific reselection and asset rules |
| allocation | equal, score, inverse-volatility, fixed-notional, vol-target; reserve, asset/group/net/beta caps and turnover_limit |

direct requests zero once exit is allowed. geometric multiplies the exit anchor by ratio each round and zeros it at final_threshold; both parameters must be in (0,1). target-step moves toward the new target using alpha in (0,1]. For linear-exit/geometric, on_reselect accepts restore (return to the ideal), resume (hold the current exit level, then continue on later dropout), or finish/new-cohort (finish exiting first). Reversal waits for flat. Forced risk exits bypass ordinary turnover budgets.

holding.max_bars requests zero at the first expired monitoring grid, bypassing ordinary rebalance gates and exit curves; execution can finish later. Missing scores do not advance ordinary exit steps. Assets leaving the selection Universe retain execution price/funding subscriptions while positions/orders/cohorts remain, and release them after zero and settlement.

After its entry window closes, a cohort does not refill its original plan; late fills retain their original plan provenance. Contributions in the same asset transfer internally before net execution without invented fills or fees. These are aggregate execution contributions. Separate per-batch execution lots, strict fill-based batch deadlines and per-batch PnL remain future extensions.

For example, `holding.by_asset: {'BTC/USDT:USDT': {min_bars: 24}}` overrides the default; explicit zero removes the corresponding restriction. Asset names use canonical data SIDMap symbols. Go Config.PolicyContext supplies visible groups, volatility, beta, schedule/risk decisions and rule resolvers; missing required inputs produce diagnostics or errors. Explicit by_asset overrides take precedence over resolver rules, then defaults.

RegisterPortfolioPolicy creates independent instances using versioned names and policy_params. RegisterPortfolioBuilder still produces the ideal portfolio. Full custom policies validate their own schema and may use bounded opaque JSON checkpoints. Owner admission atomically stores plan, target and checkpoint with version fences; recovery retains sequence and actual fill provenance. Admission remains valid when subsequent sending fails. See [Factor API](../api/factor.md#lifecycle-proposals-and-allocation-targets).

swapPerBars/holdBars are not global aliases: use every_bars, min/max_bars or period_bars. Unknown, fractional, conflicting or unused fields fail validation. Geometric requires ratio/final_threshold in (0,1); target-step requires alpha in (0,1]. Exit transitions require quantity or weight basis; other transitions reject basis. Quantity outputs use versioned allocation-decision/allocation-accepted records and require a compatible consumer.

Implementation boundaries are documented in repository paths `doc/factor_opt_implementation.md` and `doc/factor_research_extensions.md`. Holidays and custom schedules use RebalanceDue callbacks; heterogeneous or fill-based cohort lifetimes require a custom policy.

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

Policy params.k controls portfolio selection; expressions.params.window controls formula history. They are not copied into one another. Expression policies need no same-name Go definition; do not also specify definition. Combination methods include equal, fixed, history-ic, history-rank-ic, history-icir, history-rank-icir and history-ewma. Historical methods use only mature, decision-visible samples; live rejects all history methods.

For Go strategies register `runner.RegisterDefinition(name, builder)`, where builder is `func(runner.Config) (*factor.Plan, research.ComboSpec, error)`. Register a uniquely versioned portfolio builder with RegisterPortfolioBuilder. Declare node versions and missing-data policies. See [Factor API](../api/factor.md).

## Robust expressions and research extensions

Outputs can use robust cross-sectional transforms and multi-exposure neutralization:

```text
cs.mad_winsorize(kline.close, 3)
cs.robust_zscore(kline.close)
group.ols(factor.signal, style.size, style.beta)
group.wls(factor.signal, style.weight, style.size, style.beta)
group.demean(factor.signal, "style", "industry")
group.zscore(factor.signal, "style", "industry")
```

Declare factor.signal and the style binding; slower exposure sources use asof sampling and max_age_ms. Industries can be raw string fields with NULL preserved. Fitting uses the frozen Reference pool; Evaluation never participates. MAD scale is 1.4826 times median absolute deviation. At zero scale, robust zscore returns zero and winsorize returns the median; NULL remains missing. OLS/WLS includes an intercept, WLS requires positive weights, collinear exposures are dropped in declaration order, and insufficient samples return Warmup.

ComboSpec.Label selects the historical label. MinSamples/MinPairs constrain historical sections/valid asset pairs. Direction defaults to signed; Fallback defaults to equal and accepts fixed/error. A zero EWMA Decay selects 0.2. MinConfidence does not adjust for overlapping returns and cannot establish independent significance.

Advanced research uses native Go APIs. ScanPortfolioTrials isolates weights books and bounds trial counts; SelectParameters/ResolveParameters supply group/global shrinkage and PIT artifacts. Models support ridge/OLS, rolling windows, purge/embargo and hash/PIT publication/recovery. Risk APIs support diagonal/shrinkage covariance and projected optimization. Registered model/risk builders can be selected by portfolio.builder; no visible model skips new targets, and infeasible risk results cannot be ordinary targets.

FactorRegistry, JSONL TrialLedger, out-of-sample comparisons, random baselines and bounded LRU preserve experiment evidence. Lifecycle, capital, capacity/funding and attribution reports consume caller-supplied verifiable events; CLI does not automatically produce complete per-batch fill attribution. No automatic online training platform or global-optimum guarantee is provided. See [Research APIs](../api/factor.md#native-go-research-extensions) and repository path `doc/factor_research_extensions.md` for fuller examples.

## Backtest and research

```sh
./bot backtest --config base.yml --config factors.yml
./bot research --config base.yml --config factors.yml
./bot backtest --mode weights --config base.yml --config factors.yml
./bot backtest --mode events --config base.yml --config factors.yml
```

| Mode | Behavior |
| --- | --- |
| research | Panels, mature labels and diagnostics; no default account/position evidence; omit policy in CLI configuration |
| weights | Quantity retention, changed-notional costs and approximate portfolio book |
| events | Shared accounts, discrete quantity/price constraints, margin/risk validation and paper fills |

Execution prices are separate from factor inputs. Events needs tick/event or an explicit observable 1m price stream, normalized instrument units for every SID and account risk limits. A completed coarse candle cannot reconstruct intrabar fills. Prices must be strictly after decision completion plus LatencyMS and before exclusive expiry. explicit-zero declares zero funding; required-stream requires settlement streams for every required instrument, including explicit known-zero records.

For archive input set archive and match Universe, SIDMap, schemas, source versions and price streams to the file:

```sh
./bot data archive --input records.jsonl --out chunk.gob --max-records 100000
```

Input is factor.VersionRecord JSON lines. Use --schema fields.yml when concrete integer widths matter. Raw revisions remain available; round snapshots apply event-time, available/publication and local-reception gates. Archive DecisionDelayMS advances visibility cutoff while retaining decision cadence. Live uses actual receipt time.

The default executable-return label spans one decision interval. research.labels accepts multiple horizons in milliseconds, with independent execution-price capture, maturity and unresolved counts, sharing a frozen Frame and bounded queue. For example, add this to the strategy:

```yaml
research:
  labels:
    - {Name: 2h, Kind: executable-return, Horizon: 7200000, PeriodsPerYear: 4383}
    - {Name: 16h, Kind: executable-return, Horizon: 57600000, PeriodsPerYear: 547.875}
```

Only mature labels enter IC/diagnostics. Future returns cannot enter current inference. Omitting historical ComboSpec.Label selects the shortest horizon, breaking ties by name, and records this choice in the manifest. Single-label implicit identity remains compatible. Result.Unresolved counts labels beyond available data. Fixed/equal trading may disable labels with research: {labels: []}; research and every history method require labels.

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
