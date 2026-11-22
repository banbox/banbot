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

For factor or mixed configurations, `trade --dry-run` selects historical `events` replay, not live paper trading. Live simulation for all engines uses YAML `env: dry_run`, a durable Hub paper account and `funding_policy: explicit-zero`. `--live-provider` overrides execution only for `trade` configurations containing factors; time-series-only configurations select it through YAML. See [Shared accounts and Hub](./execution.md).

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

## Policy keys, price stream and fill expiry

These keys sit directly under a `run_policy[]` entry, next to `name`, `engine`, `id`, `account`, `capital_weight`, `run_timeframes`, `params` and `pairs`:

| Key | Purpose |
| --- | --- |
| `expressions` / `definition` | Expression or registered Go definition; choose one |
| `portfolio` | Selection, notionals, rebalancing, holding, exits and allocation; see below |
| `combo` | Outer combination config; a non-empty `method` replaces `expressions.combine` entirely |
| `prices` | Fill price stream `{source, timeframe, field}` |
| `decision` | Decision and fill timing: `latency_ms` (default 1), `expiry_ms` (default 60000, must exceed `latency_ms`), `max_pending` (default 64), `interval_ms`, `delay_ms`, `source_event_decisions`/`source`/`timeframe` |
| `universe` (alias `rolling_universe`) | Point-in-time rolling liquidity universe; see below |
| `research` | `labels`, `label_wait_ms`, `panel` |
| `manifest.costs` | `fee_rate`, `slippage_rate`, `funding_policy`, `funding_apr` |
| `funding_source`, `initial_nav`, `max_records`, `archive` | Funding stream, initial NAV, record cap, immutable archive input |
| `pit_min_universe_ratio`, `pit_delist_penalty_bps` | Point-in-time removal threshold and delisting penalty; see below |

Unknown keys are rejected at startup. `funding_policy` (under `execution` or `manifest.costs`) is `explicit-zero`, `required-stream` or `storage`; storage replay requires an explicit choice. `storage` reads the built-in `funding_rate` source by default and settles contract legs only.

The built-in `momentum-vol` scores `z(momentum) - z(volatility)`: momentum is the `window`-period return, volatility is the `window`-period standard deviation of 1-period returns, each winsorized at 1% and standardized; the default `window` is 24. It is momentum with a low-volatility tilt, not pure momentum.

### The zero-trade trap

Without `prices`, weights replay uses the decision-timeframe close stream. A target fills only on a quote after the decision time plus `latency_ms` and within `expiry_ms`. With a 1h decision and a 1h close stream the next quote arrives after the default 60 s expiry, so every non-empty target expires and nothing executes. For 1h and coarser strategies declare a 1m or tick price stream:

```yaml
prices: {source: kline, timeframe: 1m, field: close}
decision: {latency_ms: 1, expiry_ms: 120000}
```

Events replay and live accept only `event` (tick) or `1m` price streams. Raising `expiry_ms` alone fills at the next bar's close, which is usually not what you want.

### Aggregated decision timeframes

`run_timeframes` (and the expression `timeframe` and `kline` binding `timeframe`) may be a stored timeframe (1m, 5m, 15m, 1h, 1d) or any timeframe divisible by one of them, such as 30m, 2h, 4h, 8h, 12h, 3d or 1w. Aggregated bars are built from finer bars at read time, aligned in UTC (3d/1w use the same alignment anchors as the time-series engine). A bucket is invisible before it closes and an incomplete trailing bucket is dropped. `every_bars`, windows and `ts.lag` count aggregated bars. A timeframe not divisible by a stored one fails before the database opens and the error lists the stored timeframes; `prices.timeframe` is checked as well.

### Point-in-time rolling liquidity universe

The root `pairs` list is the candidate superset; the investable set is re-selected periodically from turnover visible at that time:

```yaml
universe:
  mode: rolling_liquid_top
  top_n: 20
  lookback_days: 30
  min_listing_days: 14
  reconstitute_days: 30
  always_include: ['BTC/USDT:USDT', 'ETH/USDT:USDT']
```

| Key | Description |
| --- | --- |
| `mode` | `rolling_liquid_top` (alias `liquid_top`) or `static` (off) |
| `top_n` | Names selected each time, 1-10000 |
| `lookback_days` / `lookback_bars` | Turnover lookback, choose one; default 30 days, using `quote` and falling back to `close * volume` |
| `min_listing_days` / `min_age_bars` | Minimum visible listing span, choose one; default 14 days, `-1` disables |
| `reconstitute_days` / `reconstitute_bars` / `schedule` | Reselection interval, choose one; default 30 days; `schedule` is `daily`, `weekly` or `monthly` (UTC calendar boundaries) |
| `always_include` | Permanent members; full symbols |

At each boundary only bars visible at that instant are used; ranking is by turnover descending, ties by symbol. Listed candidates that are not selected keep warming up indicators in the background but stay out of the cross-sectional reference pool. A held name that leaves the universe and still has quotes is closed at the next executable step. `universe` and `rolling_universe` cannot both be set; either may also sit under `portfolio`.

Missing data: in weights replay, an investable symbol with missing or invalid input on the grid (delisted, not yet listed, gap) is removed and closed, and the decision continues on the reduced universe. If the remaining investable names fall below `pit_min_universe_ratio` (default 0.5; `< 0` disables removal) of the survivors at the last accepted decision, the whole round is skipped. Removed positions close at the last valid price plus a `pit_delist_penalty_bps` penalty (default 100; `< 0` disables).

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
| holding | Minimum/maximum bars or duration, by_asset, cooldown_bars, stop_loss_pct, trailing_stop_pct and explicit adopt |
| transition | direct, linear-exit, cohort, geometric, target-step; mode-specific reselection and asset rules |
| allocation | equal, score, inverse-volatility, fixed-notional, vol-target, erc, hrp, min-variance, mean-variance; risk_window, shrinkage, reserve, asset/group/net/beta caps and turnover_limit |

direct requests zero once exit is allowed. geometric multiplies the exit anchor by ratio each round and zeros it at final_threshold; both parameters must be in (0,1). target-step moves toward the new target using alpha in (0,1]. For linear-exit/geometric, on_reselect accepts restore (return to the ideal), resume (hold the current exit level, then continue on later dropout), or finish/new-cohort (finish exiting first). Reversal waits for flat. Forced risk exits bypass ordinary turnover budgets.

holding.max_bars requests zero at the first expired monitoring grid, bypassing ordinary rebalance gates and exit curves; execution can finish later. Missing scores do not advance ordinary exit steps. Assets leaving the selection Universe retain execution price/funding subscriptions while positions/orders/cohorts remain, and release them after zero and settlement.

After its entry window closes, a cohort does not refill its original plan; late fills retain their original plan provenance. Contributions in the same asset transfer internally before net execution without invented fills or fees. These are aggregate execution contributions. Separate per-batch execution lots, strict fill-based batch deadlines and per-batch PnL remain future extensions.

For example, `holding.by_asset: {'BTC/USDT:USDT': {min_bars: 24}}` overrides the default; explicit zero removes the corresponding restriction. Asset names use canonical data SIDMap symbols. Go Config.PolicyContext supplies visible groups, volatility, beta, schedule/risk decisions and rule resolvers; missing required inputs produce diagnostics or errors. Explicit by_asset overrides take precedence over resolver rules, then defaults.

RegisterPortfolioPolicy creates independent instances using versioned names and policy_params. RegisterPortfolioBuilder still produces the ideal portfolio. Full custom policies validate their own schema and may use bounded opaque JSON checkpoints. Owner admission atomically stores plan, target and checkpoint with version fences; recovery retains sequence and actual fill provenance. Admission remains valid when subsequent sending fails. See [Factor API](../api/factor.md#lifecycle-proposals-and-allocation-targets).

swapPerBars/holdBars are not global aliases: use every_bars, min/max_bars or period_bars. Unknown, fractional, conflicting or unused fields fail validation. Geometric requires ratio/final_threshold in (0,1); target-step requires alpha in (0,1]. Exit transitions require quantity or weight basis; other transitions reject basis. Quantity outputs use versioned allocation-decision/allocation-accepted records and require a compatible consumer.

Multi-horizon, robust neutralization, model and risk builders are documented in `doc/factor_research_extensions.md`. Holidays and custom schedules use RebalanceDue callbacks; heterogeneous or fill-based cohort lifetimes require a custom policy.

### Absolute score gates and dynamic exposure

Both `portfolio` (the default top/bottom-k path) and `selection` (`lifecycle-v1`) accept:

| Key | Description |
| --- | --- |
| `long_min_score` / `short_max_score` | Optional finite numbers; omitted differs from an explicit `0`. Long candidates need `score >= long_min_score`, short candidates `score <= short_max_score`; `retain_rank` never keeps a holding that fails the gate |
| `unfilled_slot_policy` | When the number of names passing the gate `m < K`: `cash` (default) gives each selected name `side_notional / K` and leaves the rest in cash, and `m == 0` emits an explicit empty portfolio that flattens the book; `rescale` concentrates the budget on passing names, subject to `asset_cap` |
| `asset_cap` | Per-asset weight cap |
| `drift_threshold` | Drift buffer in `[0,1)`; see below |

A factor output named `exposure_scale`, `long_scale` or `short_scale` is excluded from the default combination. Its valid cross-sectional mean, clamped to `[0,1]` (all invalid counts as 1), dynamically scales the current long and short notional (`long_scale`/`short_scale` scale one side only). For example, market breadth controlling total exposure:

```yaml
outputs:
  score: 'cs.pct_rank(ts.return(positive(kline.close), 42)) - 0.5'
  exposure_scale: 'cs.mean(kline.close > ts.sma(kline.close, 50))'
```

See [regime-gate.yml](https://github.com/banbox/banstrats/blob/main/examples/crosssection/advanced/regime-gate.yml) for the full example, which also adds a BTC daily trend gate through `mtf.asof` and `ref.field`.

### Risk columns and allocation

`allocation.method` is `equal` (default), `score`, `fixed-notional` (needs `fixed_notional`), `inverse-volatility`, `vol-target` (needs `vol_target`), `erc` (alias `risk-parity`), `hrp`, `min-variance` or `mean-variance` (optional `risk_aversion`). Related keys:

| Key | Description |
| --- | --- |
| `risk_window` | Rolling return window, default 20 |
| `shrinkage` | Off-diagonal covariance shrinkage in `[0,1]` |
| `reserve_ratio`, `asset_cap`, `group_caps`, `turnover_limit` | Cash reserve and caps |
| `net_cap` / `beta_cap` | Portfolio net exposure / market beta caps |
| `volatility_column` / `beta_column` | Risk column names, default `volatility` / `beta` |

`inverse-volatility`, `vol-target`, `beta_cap` and the covariance methods read the reserved factor outputs `volatility` / `beta` (the default combination ignores them). When they are absent, the runner maintains a `risk_window` return matrix, volatility and market beta from decision-grid closes visible at decision time; while the covariance is still warming up it falls back smoothly to `inverse-volatility` or `equal`. Group caps still need Go `Config.PolicyContext` to supply groups. A `portfolio` block that sets `rebalance`/`selection`/`holding`/`transition`/`allocation` without `policy` enables `lifecycle-v1` automatically. Examples: [risk-hrp-exits.yml](https://github.com/banbox/banstrats/blob/main/examples/crosssection/advanced/risk-hrp-exits.yml), [risk-minvar-beta.yml](https://github.com/banbox/banstrats/blob/main/examples/crosssection/advanced/risk-minvar-beta.yml), [liquid-top-erc.yml](https://github.com/banbox/banstrats/blob/main/examples/crosssection/advanced/liquid-top-erc.yml).

### Stop loss, trailing stop and drift buffer

```yaml
portfolio:
  policy: lifecycle-v1
  rebalance: {every_bars: 5, drift_threshold: 0.03}
  holding:
    stop_loss_pct: 0.1        # stop after a 10% adverse move from entry
    trailing_stop_pct: 0.15   # exit after a 15% pullback from the visible favourable extreme
    cooldown_bars: 2          # no re-entry for 2 decision bars after a trigger
    by_asset:
      'BTC/USDT:USDT': {trailing_stop_pct: 0}   # explicit 0 removes the limit for this asset
```

`stop_loss_pct` and `trailing_stop_pct` take values in `[0,1)` and are checked per holding on every decision grid, including non-rebalance steps when `every_bars > 1`; the trailing extreme excludes the entry bar. A triggered asset goes to zero immediately; on non-rebalance steps only the triggered asset is closed while other holdings drift with the market without turnover. `drift_threshold` (valid under `portfolio`, `rebalance` or `allocation`): when the current weight and target weight have the same sign, are non-zero and differ by less than the threshold, the existing position is kept; exits, flips and new entries run as usual.

## Writing strategies in Go: three entry points

Place registration functions in a Go package imported by your project, then rebuild the robot. Import `github.com/banbox/banbot/factor`, `factor/runner` and `factor/statistics` as needed (the latter two share the banbot module prefix). Pick the engine first. All three share the same YAML and `DataSeries.Values`; they differ in how a signal becomes a position:

| Need | Engine | Entry point |
| --- | --- | --- |
| Independent timing per symbol | time series | `strat.AddStratGroup` + `TradeStrat` (see [custom strategies](./strat_custom.md)) |
| Score the whole market, long/short top/bottom-k | factor | `runner.RegisterFactor` |
| Spread regression between two or more symbols (stat-arb) | factor | `runner.RegisterPairs` and related; see [Statistical Arbitrage & Carry](./statarb.md) |
| Custom portfolio or lifecycle | factor | `RegisterDefinition` + `RegisterPortfolioFactory` |

### Cross-section factor in one call

```go
func init() {
	if err := runner.RegisterFactor("my-mom", func(c runner.FactorContext) (runner.FactorSpec, error) {
		w := c.Params.Int("window", 24)
		return runner.FactorSpec{Outputs: map[string]*factor.Node{
			"mom": factor.ZScore(factor.Return(c.Close(), w)),
		}}, c.Params.Err()
	}); err != nil {
		panic(err)
	}
}
```

```yaml
run_policy:
  - {name: my-mom, id: mom, engine: factor, run_timeframes: [1h], params: {window: 24, k: 10}}
```

`c.Params` works like `pol.Param` in time-series strategies: it reads `params` with defaults. `Int` with a fractional value or `Bool` with a value other than 0/1 reports through `Err()` instead of truncating; `Unused()` lists parameter names nobody read (typos). Several outputs are mixed with equal weights unless `FactorSpec.Combine` says otherwise.

`FactorContext` also provides these helpers:

| Method | Description |
| --- | --- |
| `Open/High/Low/Close/Volume/Quote/BuyVolume()` | Kline field nodes on the decision timeframe (`Quote` is quote-currency turnover) |
| `Field(name)` | Any kline field |
| `SourceField(source, name)` | A non-kline source (`funding_rate`, `open_interest`, `premium_kline`, `index_kline`, `mark_kline`); applies causal AsOf alignment automatically when the native timeframe differs from the decision timeframe |
| `RefField(symbol, name)` / `RefSourceField(symbol, source, name)` | A field of a named benchmark symbol |
| `SubTF(sourceTF, build, maxAge...)` | Evaluates a source-timeframe subgraph and aligns it to the decision timeframe |
| `SectorCode()` | Built-in sector code |

### Statistical arbitrage

Pairs, rolling pairs, cointegration baskets, residual s-score and spot-perpetual carry use dedicated registration functions such as `RegisterPairs`; a single declaration is enough to run them. See [Statistical Arbitrage & Carry](./statarb.md).

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

### Expression operator reference

| Family | Operators |
| --- | --- |
| Pointwise | `+ - * /`, comparisons `> >= < <=` (or `gt/ge/lt/le`; true is 1, false is 0), `where(cond,a,b)` (alias `if_else`; when `cond` is valid only the chosen branch must be valid), `sign`, `clip(x,lo,hi)`, `positive`, `abs`, `log`, `sqrt`, `pow`, `min`, `max` |
| Time-series basics | `ts.lag`, `ts.return` (`ts.ret`), `ts.ema`, `ts.std(x,n,ddof)`, `ts.sum`, `ts.product`, `ts.rank`, `ts.zscore`, `ts.argmax`, `ts.argmin`, `ts.corr`, `ts.cov`, `ts.beta` |
| Rolling statistics | `ts.skew` (n>=3), `ts.kurt` (n>=4), `ts.slope`, `ts.rsquare`, `ts.decay_linear` |
| Cross-section | `cs.rank`, `cs.pct_rank` (`[0,1]`), `cs.zscore`, `cs.robust_zscore`, `cs.winsorize`, `cs.mad_winsorize`, `cs.quantile`, `cs.mean`, `cs.sum` (the last three output a cross-sectional constant) |
| Regression and groups | `group.residual`, `group.ols`, `group.wls`, `group.demean`, `group.zscore` |
| Crypto sectors | `cs.sector_demean`, `cs.sector_zscore` (aliases `group.sector_*`), using 8 built-in sectors (`L1`, `L2`, `DeFi`, `AI_DePin`, `Meme`, `Gaming`, `Infra`, `Other`) derived from the symbol with no external table; `sector_code()` outputs a 0-7 code |
| Quantile buckets | `group.quantile_demean/zscore/rank/pct_rank(x, metric, n)`: each period the reference pool is split into `n` (2-1000) equal-count buckets by a continuous `metric` (e.g. `ts.sma(kline.quote,30)`), and `x` is processed within each bucket |
| Cross-timeframe and benchmark | `mtf.asof("1d", expr[, max_stale_ms])` (alias `asof_tf`); `ref.field("BTC/USDT:USDT", "close")` (an optional third argument names the source) |
| Technical indicators | `ts.sma/rma/wma/dema/tema/hma/kama/alma/trima/er/kalman1d/hurst`, `ts.rsi/roc/mom/cci/macd*/bbands_*/stoch/willr`, `ts.tr/atr/natr/adx/dmi_*/chop/aroon_*/vortex_*/donchian_*/keltner_*/supertrend*/sar/stc/ichimoku_*`, `ts.highest/lowest/vwma/obv/mfi/cmf/adl/adosc`, order flow `ts.volume_delta/cvd/rolling_cvd/taker_buy_ratio`, volatility `ts.parkinson_vol/garman_klass_vol/yang_zhang_vol` |

With a `kline` binding you can write `open`, `high`, `low`, `close`, `volume` and `buy_volume` directly, and OHLC indicators may omit field arguments, as in `ts.adx(14)` or `ts.volume_delta()`. Binding aliases cannot be `factor`, `param`, `label`, `ts`, `cs`, `group`, `mtf` or `ref`. Windows must be integer constants or `param.name`, in the range 1-10000.

The source timeframe of `mtf.asof` must not be finer than the outer decision timeframe. Inner indicators advance only on closed source bars and are then causally aligned to the decision grid, so a 4h decision can use a true daily moving average. `ref.field` reads a named benchmark symbol and broadcasts it across the pool; the benchmark need not be tradable. Expressions allow any acyclic CS→TS→CS nesting, for example `cs.zscore(ts.sma(cs.pct_rank(ts.return(kline.close, 30)), 5))` (rank, smooth over time, standardize to cut turnover); warm-up lengths accumulate automatically. Full examples: [operators.yml](https://github.com/banbox/banstrats/blob/main/examples/crosssection/advanced/operators.yml), [neutral-sector-quantile.yml](https://github.com/banbox/banstrats/blob/main/examples/crosssection/advanced/neutral-sector-quantile.yml).

## Add common technical indicators

Replace or extend `expressions.outputs` with explicit scalar indicators, then combine or rank their results:

```yaml
outputs:
  rsi_rank: 'cs.rank(ts.rsi(kline.close,14))'
  atr: 'ts.atr(kline.high,kline.low,kline.close,14)'
  cci: 'ts.cci((kline.high+kline.low+kline.close)/3,20)'
  macd_hist: 'ts.macd_hist(kline.close,12,26,9)'
  band_width: 'ts.bbands_upper(kline.close,20,2,2)-ts.bbands_lower(kline.close,20,2,2)'
```

Technical, order-flow and volatility-estimator functions (see the operator reference above) support batch and incremental execution. Periods count complete valid input tuples; missing or NULL fields preserve their validity and pause indicator state. `ts.roc` uses percentage units; `ts.return` uses fractional units. Reuse one Session across historical chunks, and use explicit asof bindings for mixed source timeframes. The [indicator guide](https://github.com/banbox/banbot/blob/main/doc/factor_indicators.md) gives lookback, retention and edge-case rules.

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
    - {name: 2h, kind: executable-return, horizon: 7200000, periods_per_year: 4383}
    - {name: 16h, kind: executable-return, horizon: 57600000, periods_per_year: 547.875}
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

Default `trade` in `env: prod` or `dry_run` executes through the local Hub. An omitted `execution.live_provider`, `banexg` or `hub` all select Hub. Start `./bot hub --datadir /ban/data --config hub.yml`, then `./bot trade --datadir /ban/data --config live.yml`. A missing Hub fails without a local-sender fallback. See [Shared accounts and Hub](./execution.md) for account configuration, persistence and restart steps.

Provider precedence is `--live-provider`, `accounts.<name>.live_provider`, `execution.live_provider`, then Hub. Custom in-process providers are explicit production embedding options: register `entry.RegisterFactorLiveBinding("verified-session", factory)` and supply session transport, canonical metadata, publication/revision, funding and account evidence. `verified-session` is only an example name. One robot cannot mix Hub and embedded providers; `env: dry_run` rejects embedded real-execution adapters.

Remove historical `archive` input and configure market-matching instrument units for every SID, risk limits and funding policy. The default path currently supports linear perpetuals. Production requires `required-stream` funding and authoritative cash/settlement evidence; live simulation requires `explicit-zero`. Budgets come from reconciled accounts; `InitialNAV` deposits no live funds. Existing cash and positions require explicit attribution. Changing `env` or a provider name cannot establish execution readiness. **This documentation does not establish completed end-to-end real-venue production acceptance.**

Startup compiles data requirements, warms history and installs current subscriptions; execution admission follows reconciliation. Rounds use closed, decision-visible records. An external source drives `Observe` and `Flush`/`FlushAt`; the calculator has no independent market timer. Failed candidates preserve the old generation; shutdown uses Stop then Join. Live rejects every history combination method; use equal or fixed. Source-event decisions, option expiry and terminal-settlement replay features do not establish live support.

Required live funding records contain exact decimal strings `mark`, `rate`, `account_amount` and stable `settlement_id`. Duplicate settlements are idempotent; settlements arriving across position changes need historical reconciliation. `trade --dry-run` remains historical paper replay, distinct from live simulation.

## Results and troubleshooting

Factor-engine runs stream panels, decisions, matured diagnostics and final scalar summaries as JSON lines. Ordinary backtests also write resolved.json with defaults/origins, account-&lt;account&gt;/manifest.json and versioned event/posting Gob chunks. Completion follows output sync/close and resource cleanup; primary and cleanup errors remain visible.

### Long/short attribution and large-pool output

Per-pair results, reconciliation fields and `Result.Pairs` for statistical arbitrage are described in [Statistical Arbitrage & Carry](./statarb.md#reading-the-results).

`Result.Attribution` is available only with the weights book: long/short price PnL, average exposure, fees, slippage, funding, PIT costs and up to 20 `TopSymbols`. Events/live do not output it, and it is not a per-batch fill report. The weights `funding_apr` approximation charges account net-long notional; net shorts neither pay nor receive funding. Funding is account-level `Funding`, excluded from `TopSymbols[].Net`.

For large research pools set `research: {panel: none}` on the factor policy to omit per-symbol/column panel rows. Decision events, summaries and book values remain unchanged.

execution.history: cold/history.sqlite optionally archives settled simulated history. The path must be new; it is not a resume snapshot and is refused for real trade/durable execution. Small replays default to MemoryStore. page_bytes bounds decoded logical payload rather than process RSS.

Check version, single timeframe, registered definition, Universe/SID/schema, price timeframe, funding and account units when startup fails. Preflight creates no exchange, storage, account or output directory. bot tool bt-factor is the legacy orders.gob rolling-selection tool, separate from the factor runner.

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
