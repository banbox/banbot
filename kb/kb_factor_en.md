# banbot Cross-Sectional Factor / Multi-Factor Strategy Knowledge Base

Scope: `engine: factor` cross-sectional factors, multi-factor combination, portfolio construction, research, replay, live trading, settlement. Prerequisite: the time-series strategy doc has been read (DataSeries/Kline, banexg types, common config, banta); not repeated here. For pairs statistical arbitrage see the statarb doc. Baseline: banbot v0.7.1.

## 0. Overview

Entry points: cross-symbol ranking/neutralization/multi-factor/unified rebalancing = `engine: factor` + YAML `expressions`, or Go `runner.RegisterFactor` (simplest: one Go factor definition + default top/bottom-K) / `RegisterDefinition`; custom ideal weights = `runner.RegisterPortfolioBuilder` / YAML `portfolio.builder`; custom full portfolio strategy (rebalance/holdings/exit/state) = `runner.RegisterPortfolioPolicy("name-vN", f)` / YAML `portfolio.policy`.

- A factor strategy does not return `*strat.TradeStrat` and does not place orders per symbol; the runner generates a target portfolio each decision cycle and hands it to the account for execution.
- One `run_policy` entry = one factor strategy = one decision timeframe (multiple `run_timeframes` values report `one decision timeframe is required`).
- `engine` defaults to `time_series`; factor entries must write `engine: factor`. Optional root key `config_version: 1|2`; when 2, entries without `engine` are treated as time_series and other engine values are rejected.
- When an account with factor strategies has multiple strategies, each must set `capital_weight` explicitly (0-1, sum <= 1, share of the account budget, not the time-series `stake_rate`); a single strategy without it takes the whole account; pure time-series accounts are unaffected.
- `id` is globally unique (duplicates across accounts also report `duplicate strategy id`); defaults to `name`; `name` is required. `account` default: use `default` account if present, otherwise the first (sorted by name) account without `no_trade`, otherwise `default`.
- Legacy `factor: {...}` is still readable (keys are lifted to the same level as `run_policy`; name conflicts report `X is also set in factor`; requires `engine: factor`).

## 1. YAML Configuration

### 1.1 `run_policy` entry keys (engine: factor)

| Key | Meaning/default |
| --- | --- |
| `name` | Without `definition`/`expressions` it is the Go definition name (`MomentumVol` -> `momentum-vol`; unregistered reports `unregistered definition`), otherwise display name only |
| `engine` `id` `account` `capital_weight` | See section 0; `capital_weight` scales `initial_nav` proportionally |
| `run_timeframes` | Decision timeframe, e.g. `[1h]`; defaults to root-level `run_timeframes`, then `1h`; determines `DecisionInterval` and the default label |
| `params` | `{name: float}`; `window`, `k` must be integers; other keys are read by Go `Params()`; all enter manifest.parameters (changing params = new identity). `swapPerBars`/`holdBars` error (use `portfolio.rebalance`/`holding`/`transition`) |
| `archive` | Archive `.gob` path; relative paths are relative to the config file declaring it, `$`/`@` prefix is relative to DataDir; when written together with `chunks`, `archive` wins |
| `chunks` | `[{path, from, to}]`, `from/to` in ms, 0 = use the EventTime range within the archive; batches whose `max(EventTime, AvailableAt)` (for receive-type sources such as price/funding, also IngestedAt) falls in `[from,to]` are replayed; multiple chunks must be strictly increasing and non-overlapping |
| `definition` | Registered Go definition name; mutually exclusive with `expressions`/`Plan` (explicitly writing both reports `mutually exclusive`) |
| `prices` | `{source, timeframe, field}` fill/valuation price stream, see 1.7 |
| `funding_source` | Funding stream source name (required for `required-stream`; storage replay subscribes `{funding_source, event, rate}`) |
| `option_expiry_source` `terminal_settlement_source` | Option expiry / terminal settlement stream sources, see 7.2 |
| `pit_min_universe_ratio` `pit_delist_penalty_bps` | Backtest PIT parameters: `pit_min_universe_ratio` (<= 1, 0 = default 0.5, < 0 disables the PIT reduction) is the share of Investable symbols that must remain in one round, relative to the survivors at the last accepted decision (rolling, nominal pool for the first one); `pit_delist_penalty_bps` (finite, 0 = default 100, < 0 off) is the extra adverse penalty in bp when a held symbol is closed by a drop, refunded and replaced by the worse-of-gap price if its data returns |
| `initial_nav` | Strategy initial NAV (positive); defaults to `wallet_amounts[manifest.currency]`, else 10000; then multiplied by `capital_weight` |
| `max_records` | Max records per batch, default 100000; overridden by `data.max_records` |
| `expressions` `combo` `decision` `research` `portfolio` `manifest` `snapshot` | See section 2, 1.4-1.6, 4.2, 1.9 |
| `pairs` `time_range` `stake_currency` | Same as common config; in storage replay, strategy-level `pairs` determines the static symbol pool |

Validation behavior:
- Root-level appearance (case/underscore-insensitive) of `artifact_path definition expressions computation_context mode chunks max_records max_pending decision_interval decision_delay_ms latency_ms expiry_ms label_wait_ms snapshot factor combo manifest strategy_id account_id initial_nav account_initial_nav prices funding_source` reports `not a unified YAML configuration field`; `config` inside an entry reports `unknown advanced field`.
- **Unrecognized keys inside an entry are silently ignored** (including `artifact_path`, misspelled keys, and entry-top-level `holding`/`transition`/`rebalance`, which are only valid under `portfolio`); only unknown keys inside `data/execution/accounts` and advanced blocks such as `decision/portfolio/research/manifest/snapshot/combo/prices/expressions` report `unknown advanced field`, and values must not be null.
- Integer fields written as floats (`k: 2.5`) error; positive integers/non-negatives/enums in advanced blocks are validated at load time.

### 1.2 `data` / `execution` / `accounts`

| Location | Keys |
| --- | --- |
| `data` | `namespace page_rows prefetch_rows page_bytes archive max_records pit_policy` |
| `execution` | `mode store history sender_lease_dir hub_socket live_provider funding_policy protection_mode order_mode near_limit_bps paper_fill_model instruments margin_rate max_account_margin max_virtual_gross strategy_gross_limit` |
| `accounts.<name>` | All the same keys as `execution`, overriding the global per key; `execution.accounts.<name>` is an alias (same-key conflict with `accounts.<name>` errors; the account must be declared in root `accounts`, otherwise `unknown account`; for multiple strategies on one account, paper currency/costs/risk limits/store/lease/history/fill model must be identical) |

`data`:
- `page_rows` default 20000; `prefetch_rows` defaults to the minimum of `max_records/2` across storage strategies; `page_bytes` default 0 (unlimited); storage replay requires `max_records >= prefetch + number of streams`; `archive` is the global archive used when a strategy-level `archive/chunks` is not written (`@`/`$` relative to DataDir).
- Under the archive driver, `data` only allows `max_records/archive/pit_policy`; others report `archive driver cannot apply a storage subscription override`.
- `pit_policy`: archive takes `available-at` (default) or `strict`, **validation only, does not change snapshot visibility labels** (labels are set by `snapshot.visibility_policy`, default `available-at`); storage replay must explicitly use `static-approximation`, `strict` errors directly.

`execution`:
- `mode`: backtest only accepts `weights`/`events` (default `events`; CLI `--mode` overrides; mixed time-series/factor must use events).
- `funding_policy`: `explicit-zero` (explicitly ignore funding) or `required-stream` (requires `funding_source`), written to `manifest.costs.funding_policy`; for archive replay when undeclared, if the archive contains a `funding` source it automatically becomes `required-stream` + `funding_source=funding`, otherwise `explicit-zero` must be declared; storage replay must declare explicitly.
- `paper_fill_model`: `midpoint` (default), `bid-ask`, `mid-entry-market-exit`; Events only; strategies on the same account must agree. `midpoint` takes the midpoint of `bid`/`ask` (default = price) rounded down to `price_tick`, which must lie within the bid/ask, so with no spread the price must be an integer multiple of `price_tick` (otherwise `no quote-compatible midpoint tick`); `bid-ask` types require visible finite `bid`/`ask`, rejected if missing.
- `order_mode`: `local` (default) | `exchange_first`; `protection_mode`: `software` | `native_preferred` (by default only `exchange_first` prefers native); `near_limit_bps` in [0,10000], 0 is treated as 10.
- `store` + `sender_lease_dir`: Events persistent paper ledger (SQLite), must be paired; `history` see 7.3; `live_provider`, `hub_socket` see 7.4.
- Risk fields are **absolute settlement-currency amounts** (not leverage multiples): `margin_rate` in (0,1], `max_account_margin`, `max_virtual_gross` (account exposure cap), `strategy_gross_limit` (strategy exposure cap) (non-negative decimal). For Events archive replay these four and `instruments` must be explicit (otherwise `paper execution requires instrument units and risk limits`); Events storage replay defaults: `margin_rate=1/leverage` (root `leverage`, default 1), `max_account_margin=account NAV`, `max_virtual_gross=account NAV x leverage`, `strategy_gross_limit=strategy NAV x leverage`, and `instruments` are derived from exchange market metadata (settlement currency must equal `manifest.currency`).
- `instruments`: key is a numeric SID, value `id, version, valuation(linear_perpetual|cash_european_option), settlement_currency, quantity_step, contract_size, price_tick, money_scale(0-18), min_steps(>=0), min_notional(>=0)`; options add `option: {underlying_id, right(call|put), strike, expiry_ms, underlying_stress_max}` (all must be positive; `underlying_id` must not equal its own `id`). `id` must equal the name of that SID in `snapshot.sid_map` (archive default `sid:<N>`, storage uses exchange symbol) and be unique, `settlement_currency` must equal `manifest.currency`; in Events/live every SID in Investable and Tracked must have units.

Minimal Events archive replay example: `execution: {mode: events, funding_policy: explicit-zero, margin_rate: 0.1, max_account_margin: 10000, max_virtual_gross: 20000, strategy_gross_limit: 20000, instruments: {1: {id: "sid:1", version: v1, valuation: linear_perpetual, settlement_currency: USDT, quantity_step: '0.01', contract_size: '1', price_tick: '0.01', money_scale: 8}}}` (one entry per SID, decimals as strings; prices in the archive must be integer multiples of 0.01).

### 1.3 `decision`

`interval_ms` (decision interval, positive, default derived from `run_timeframes`; mismatch with expression `timeframe` errors), `delay_ms` (default 0, shifts the data-visibility cutoff later, >=0), `latency_ms` (default 1, delay from decision completion to a fillable price, must be >0), `expiry_ms` (default 60000, target validity period, must be >latency_ms), `max_pending` (default 64, pending proposal cap, positive integer), `source_event_decisions` (default false, see 1.6), `source`/`timeframe` (required only when `source_event_decisions: true`, otherwise their presence errors).

### 1.4 `combo` (same fields as `expressions.combine`)

`method` (`equal|fixed|history-ic|history-rank-ic|history-icir|history-rank-icir|history-ewma`), `columns`, `weights` (map, required for `fixed` and must cover all columns), `label`, `min_samples`, `min_pairs`, `min_confidence`, `decay` (EWMA alpha, [0,1], 0 is treated as 0.2), `direction` (`signed|positive`), `fallback` (`equal|fixed|error`).

- `columns` must not repeat and must be output names; **`expressions.combine` may omit columns (defaults to all outputs), the outer `combo` must write it (otherwise `expression combine requires columns`)** (when outer `combo.method` is non-empty it overrides the inner one entirely).
- `equal`: equal weight across columns; `fixed`: weights are linearly summed directly, negatives allowed, not normalized. If a non-zero-weight column is invalid for an asset -> that asset's score is invalid, no reallocation; zero-weight columns do not affect validity.
- History methods only use IC samples that are matured, visible at decision time, and with `DecisionTime<asof` (window keeps the latest 256 cross-sections, samples must be >=2 pairs; `min_pairs` can raise this); `rank` variants use RankIC, `icir` uses mean/std, `ewma` uses `decay`; `min_samples` at least 1; `min_confidence` is a |mean|/standard-error threshold (needs >=2 samples, non-zero mean, no multiple-testing correction, ICIR on overlapping samples is descriptive only); `direction: positive` clips negative weights to 0; weights normalized to sum|w|=1; with no qualifying history follow `fallback`: `equal` (default, diagnostic `ic-equal-fallback`), `fixed` (use `weights`), `error`.
- History methods require labels (`research.labels` non-empty); with multiple labels, omitting `label` takes the shortest horizon (ties by name), `label` must be declared.
- Live rejects history methods, only `fixed`/`equal`.

### 1.5 `research`

- `labels`: `[{name, kind, horizon, overlapping, periods_per_year}]`. For the archive pipeline `kind` only accepts `executable-return` (`close-to-close` rejected); `horizon` (ms) and `periods_per_year` positive; `overlapping` boolean (default false).
- When `research` is not written, one label is generated automatically: `name=run_timeframes value`, `executable-return`, `horizon=decision interval`, `periods_per_year=365.25*86400000/decision interval ms`; writing `research.labels` overrides `manifest.labels`.
- `labels: []`: backtest generates no labels; `research`/history IC require labels. `label_wait_ms` >=0, default 0. `research.panel`: `all` (default) or `none`.
- Example: `labels: [{name: forward_16h, kind: executable-return, horizon: 57600000, periods_per_year: 547.875, overlapping: true}]`. Immature labels are recorded as unresolved (`Result.Unresolved`), must not be filled with zero; future labels must not enter current-period selection.

### 1.6 Source-event decisions (irregular observations such as Information bars)

`decision: {source_event_decisions: true, source: information, timeframe: event, delay_ms: 0}` (source = real source of the archive stream; timeframe = exact identifier of the stream, not a duration)

- `GridTime` takes the `EventTime` of the real closed record of the selected stream; decision time is the publish/receive cutoff of that batch. Only Research/Weights/Events historical replay; `trade`/live rejected; `delay_ms` must be 0.
- Two different source event times within one publish batch -> rejected, the batch must be split.

### 1.7 `prices` (price stream)

- Archive: with a `tick` stream -> `{tick, event, price}`; otherwise with expressions -> `{kline, expression timeframe, close}`; otherwise `{factor.source, factor.timeframe, factor.field}` (error if the archive has no kline/tick price stream and has expressions).
- Storage replay: Events -> `{kline, 1m, close}`; with expressions -> `{kline, expression timeframe, close}`; otherwise same as above.
- Live: **no default**, must explicitly use an `event` or `1m` stream.

Events and live only accept `event` (tick) or `1m` prices, `1h` etc. error. When Weights default price is the decision-timeframe close (e.g. archive has only kline 1h), if the next price is later than `expiry_ms` -> all targets expire (`Executions=0`, `Skipped=Decisions`); configure `{tick,event,price}` or a 1m stream.

### 1.8 Replay visibility (archive)

Visible when `max(AvailableAt, IngestedAt) <= current time` and `EventTime <= GridTime`; within the same stream the latest by EventTime/Revision is taken; when `snapshot.replay_time!=0` it is driven by the ingest clock.

### 1.9 Where `manifest` and `snapshot` sub-keys end up

`manifest` allowed keys `currency code_revision factor_plan_hash universe_version visibility_policy execution_mode latency_assumption static_universe combo portfolio labels parameters costs{fee_rate,slippage_rate,funding_policy} snapshots` (`fee_rate`/`slippage_rate` non-negative, default 0).

- `currency`, `code_revision`, `costs.fee_rate/slippage_rate`: **user value is used** (defaults: `stake_currency[0]`, else `USD`; `code_revision` = banbot version).
- `parameters` is merged with `params`, `params` wins on same name; `portfolio` is layered field by field: default -> `params.k` -> `manifest.portfolio` -> `portfolio` block (later wins).
- `labels`: overridden if `research.labels` is written (including `[]`); `costs.funding_policy`: overridden if `execution.funding_policy` is present, otherwise derived from the archive.
- `combo`, `factor_plan_hash`, `universe_version`, `static_universe`, `visibility_policy`: **always overridden** (from the actual combination/plan/`snapshot`); `execution_mode`, `latency_assumption` always overridden by the runner; `snapshots` appends archive SHA-256 references.

`snapshot` keys `grid_time decision_time replay_time universe sid_map schemas source_versions adjustment_version visibility_policy` (`universe` sub-keys `version investable reference tradable evaluation tracked static`):

- `grid_time`, `decision_time`: overridden every decision round (writing them has no effect); `replay_time`: only non-zero is meaningful = enables ingest-clock replay (driven by IngestedAt), value overridden every round.
- `universe`: if `version` is non-empty it is used (may set `static: false`); if empty, archive derives `archive-static` / storage derives `storage-static:<hash>`.
- `sid_map`: archive fills missing SIDs with `sid:<N>`; storage errors on mismatch with exchange symbols, fills missing ones.
- `schemas`, `source_versions`: for archive, if all three (including `sid_map`) are given, inference is skipped, otherwise missing items are derived from the archive, and `source_versions` mismatching a record's `SourceVersion` errors; for storage they are overridden by storage metadata.
- `adjustment_version` is used, default `raw`; `visibility_policy`: archive default `available-at` (label only), storage forced to equal `data.pit_policy`, live must be non-empty.

## 2. Expressions (expressions)

### 2.1 Structure and rules

```yaml
expressions:
  schema_version: 1          # must be 1
  timeframe: 1h              # must equal run_timeframes (required for standalone validate)
  bindings:                  # alias -> data stream
    kline: {source: kline, timeframe: 1h}
    info:  {source: info, timeframe: 1d, sampling: asof, max_age_ms: 172800000}
  params: {window: 24}       # only for param.name (finite numbers), does not inherit outer params
  lets: {price: 'positive(kline.close)'}   # intermediate factors
  outputs: {momentum: 'cs.zscore(ts.return(factor.price, param.window))'}   # at least one
  combine: {method: equal}   # optional, same as 1.4
```

- `banbot validate|explain --spec f.yml` produce the same output, JSON keys `hash timeframe outputs nodes warmup retention inputs combine`; data/accounts are not validated. The file must be a single YAML document, <=1 MiB, unknown top-level keys error.
- Identifiers `[A-Za-z_][A-Za-z0-9_]*`; binding names must not use `factor param label ts cs group`. Names may be forward-referenced, cycles not allowed; unused declarations are also validated; `label.*` is forbidden.
- Limits: single expression <=16 KiB, total <=256 KiB, AST <=8192 nodes, depth <=64, declarations <=512, window <=10000. Expressions containing `"` use YAML single quotes: `'group.demean(x, "info", "sector")'`.
- binding: with `sampling` default or `source-events`, `timeframe` must equal the decision timeframe and `max_age_ms=0`; `asof`/`asof-latest` require `max_age_ms>0`, slow-frequency/event sources must use it to sample onto the decision grid; after `asof`, windows count **decision observations** (24 hourly observations != 24 trading days). Cross-timeframe without asof reports `cross-timeframe data requires asof sampling and positive max_age_ms`.
- `alias.field` reads a field; `field("alias","name")` is for special field names (both arguments must be string literals).

### 2.2 Syntax and functions

Operators `+ - * /` (`*/` higher precedence, left-associative), unary `+ -`, parentheses; numbers decimal (scientific notation `1e-8` allowed), no underscores/hex; no comparison, conditional, `%`, `**`; strings only for `field`/`group.demean/zscore`. Window arguments are integer constants (signed allowed) or `param.name` (cannot write `param.window+1`), range [1,10000], `ts.lag` additionally allows 0.

- Pointwise: `positive(x)` (x>0 yields x otherwise invalid, **no lower-bound clamping**), `abs log sqrt (x)`, `pow min max (a,b)`.
- Time-series (x is input; n window): `ts.lag(x,n)` (n=0 returns as is, not counted in window rules), `ts.return(x,n)`=x_t/x_{t-n}-1, `ts.ema/sma/rma/wma(x,n)` (exponential/simple/Wilder/weighted, wma latest weight n), `ts.std(x,n,ddof)` (integer `0<=ddof<n`), `ts.rsi(x,n)` (Wilder, 0-100), `ts.roc(x,n)` (percent, x100), `ts.mom(x,n)` (difference from n valid samples ago), `ts.cci(x,n)`, `ts.highest/lowest(x,n)`, `ts.vwma(c,v,n)`, `ts.tr(h,l,c)` (first valid previous close is warmup), `ts.atr(h,l,c,n)` (Wilder), `ts.stoch(h,l,c,n)` (raw %K without D smoothing), `ts.willr(h,l,c,n)` (-100 to 0), `ts.obv(c,v)` (first value takes the first valid volume, unchanged on equal close), `ts.mfi(h,l,c,v,n)`, `ts.macd/macd_signal/macd_hist(x,f,s,g)` (f<s, f/s/g in [1,10000]), `ts.bbands_upper/middle/lower(x,n,up,down)` (population std, multipliers non-negative and explicit).
- Cross-sectional: `cs.rank(x)` (ascending rank, 0-based, ties averaged, not percentile), `cs.zscore(x)` (population std; all-equal outputs 0), `cs.robust_zscore(x)`=(x-median)/(1.4826*MAD) (MAD=0 outputs 0), `cs.winsorize(x,tail)` (tail in [0,0.5), bounds are interpolated quantiles of the reference pool), `cs.mad_winsorize(x,k)` (median +/- k*1.4826*MAD, k>0; MAD=0 clips all to median), `cs.quantile(x,q)` (q in [0,1]; **outputs a cross-sectional constant**, the reference pool's q-quantile broadcast, only suitable for thresholds/grouping, cannot rank).
- Regression/grouping: `group.residual(y,x)` (univariate cross-sectional OLS residual with intercept, fitted on the reference pool, needs >=2 valid pairs), `group.ols(y,x1,...)` (multivariate, at least one exposure), `group.wls(y,w,x1,...)` (weights must be positive and valid), `group.demean(x,"alias","field")` / `group.zscore(...)` (within-group demean/z-score (population std) by a categorical field; field original type/NULL preserved, sampled via binding; missing field = Missing, NULL = Null).

- Go equivalents: `ts.x`/`cs.x` -> `factor.X` (MACD/BBands return three nodes; `group.ols/wls` -> `MultiResidual/WeightedResidual`; `group.demean/zscore` -> `...ByField`), see section 6.
- Time-series operators (including indicators) cannot act on cross-sectional/regression outputs: `TS windows over cross-section results are not supported` (exception `ts.lag(x,0)`); when needed, do time-series first then cross-section: `cs.rank(ts.rsi(kline.close,14))`.
- Lower bounds such as `max(x,1e-8)` only apply to valid values, they do not fill NULL/missing fields/insufficient warmup.
- Cross-sectional/regression statistics are fitted on the frozen `Universe.Reference`; outputs are only meaningful for `Investable` valid assets.
- Warmup example: `ts.macd_hist(..,12,26,9)`=33; missing input tuples extend warmup but do not advance state.

## 3. Pipeline, Inputs, and Snapshots

- Archive: `banbot data archive --input records.jsonl --out chunk.gob [--schema fields.yml] [--max-records N]`, outputs `{"archive":path,"digest":content hash}`. Each JSON line is a `factor.VersionRecord` (unknown fields forbidden, keys case-sensitive):
  `{"Series":{"Source":"kline","Sid":1,"TimeMS":0,"EndMS":3600000,"TimeFrame":"1h","Closed":true,"Values":{"close":100.5}},"EventTime":3600000,"Revision":1,"AvailableAt":3600000,"IngestedAt":3600000,"SourceVersion":"v1"}`
  `Series` is a DataSeries (`Source Sid TimeMS EndMS TimeFrame Closed IsWarmUp Values`). If `--out` already exists: identical hash succeeds idempotently, different reports `immutable archive content conflict`.
- Tick stream convention: `Source:"tick"`, `TimeFrame:"event"`, `Values:{price}` (`TimeMS=EventTime`, usually close +1ms so fills occur after the decision); funding stream `Source:"funding"`, `TimeFrame:"event"`, `Values:{rate}`.
- `--schema` YAML is `{source: {field: type}}`, types `int int8 int16 int32 int64 uint uint8 uint16 uint32 uint64 float float32 float64 string bool json`. Large integers need an explicit type (untyped integers beyond float64 exact range are rejected), type conflicts on the same field error; missing keys/NULL/strings/custom fields are preserved in `Values`.
- Archive identity derivation: SID set, schema digest, source versions, static `Universe` (`archive-static`); `visibility_policy` defaults to `available-at`; `adjustment_version` defaults to `raw`; `SourceVersion` of the same source must be consistent within the archive.
- Storage replay: `pit_policy: static-approximation` must be declared explicitly; the symbol pool takes strategy-level `pairs` (default configured pairs), all five pools are identical and static, version `storage-static:<hash>`; K-line schema change errors; requires exchange/database.
- Snapshot requirement: every symbol in Reference and Investable must have visible closed records for all planned inputs at the current decision grid. In Weights backtests an Investable symbol without data (before listing, after delisting, data gap) is dropped by the point-in-time (PIT) universe and decisions go on with the reduced universe (every pool including Reference loses it; a held position is closed at its last valid price; `lifecycle-v1` is treated the same, the policy state is scrubbed too, and the symbol re-enters as a new one when data returns); drops are counted in `UniverseDrops`/`DelistExits`/`DroppedBySID`, not in `Incomplete`. Missing data of a Reference-only symbol (not Investable), fewer than `pit_min_universe_ratio x survivors at the last accepted decision` symbols remaining in one round (rolling denominator, default 0.5; gradual delistings are always accepted, a mass loss within one round is refused; when the gate passes but fewer than 2 symbols survive the dropped positions are closed and the period is paused, counted in `PausedPeriods`), `pit_min_universe_ratio < 0`, or Events/Trade mode skip the whole round and are counted in `IncompleteStreams` (SID, Source, TimeFrame, Missing, Invalid, FirstGridTime, LastGridTime).
- `Universe` five pools: `Investable` candidates; `Reference` statistics pool; `Tradable` tradable pool (required for both scoring and selection); `Evaluation` research pool; `Tracked` still monitored after exit.
- Snapshots must not mix different revisions (conflicts need explicit reconciliation); late funding is rejected rather than counted into subsequent positions.

## 4. Portfolio Construction, Constraints, and Holding Lifecycle

### 4.1 Default top/bottom-K (`top-bottom-k-v1`) and basic `portfolio` fields

- Input score column defaults to `score` (the combined output). If valid scores in `Investable ∩ Tradable` are `<2K` -> returns nil target + diagnostic `insufficient-scores`; all scores equal -> `constant-scores`; the runner skips replacement and keeps the old portfolio in both cases. Callers invoking the Go function directly must check for non-nil.
- The top K go long at `long_notional/K` each, the bottom K go short at `-short_notional/K` each, the two sides do not overlap, ties broken by SID. `short_notional: 0` makes short weights 0 (still requires >=2K valid scores); for long-only use `selection.long_k` or a custom builder.

| `portfolio` key | Description (default) |
| --- | --- |
| `builder` | PortfolioBuilder name (`top-bottom-k-v1`; unregistered errors) |
| `k` | Positive integer, defaults to `params.k`, then 10 (`portfolio.k` wins); under lifecycle may be replaced by `selection` |
| `long_notional` / `short_notional` | Fraction of strategy NAV, non-negative (0.5 / 0.5) |
| `mode` | `full` (default; assets omitted from this strategy's old target are zeroed) or `patch` (omitted ones are kept, only explicit 0 closes) |
| `policy` | Empty by default (legacy path); `lifecycle-v1` or custom `name-vN`; `rebalance/selection/holding/transition/allocation/policy_params` **require an explicit `policy`**, otherwise `lifecycle fields require an explicit policy` |
| `policy_params` | Valid JSON for the custom policy (bounded) |

`TargetPortfolio` is ideal weights; `PortfolioTarget` can mix NAV weights and absolute quantities (`AbsoluteQuantity`, standard asset quantity not contract count, signed decimal string).

### 4.2 `lifecycle-v1`

Example (compute hourly, rebalance every two hours; after 16 hours since first fill, only dropped-out assets enter an eight-step linear exit):

`portfolio: {long_notional: 1, short_notional: 0, policy: lifecycle-v1, selection: {long_k: 3}, rebalance: {every_bars: 2}, holding: {min_bars: 16}, transition: {mode: linear-exit, exit_steps: 8, basis: quantity}, allocation: {method: equal, reserve_ratio: 0.02}}`

Fixed-lifetime cohorts (replace `transition`, drop `holding.min_bars`): `transition: {mode: cohort, period_bars: 16, startup: gradual, sizing: entry-nav}`. gradual builds a 1/8-budget cohort each round; seed-all uses the current ranking to build eight cohorts with different expiry windows at once. Cohorts of the same coin first aggregate into a net target; expiry and new-build internal transfers count as confirmed contribution, with no fabricated fills or fees; if a planned 8 units only fills 4, after the first cohort expires 3.5 remain, with no top-up to the old cohort.

**selection**: `long_k`/`short_k` are the selection counts for the round (not holding caps; default `long_k=k` (long_notional>0), `short_k=k` (short_notional>0)); or `long_quantile`/`short_quantile` in [0,1] (mutually exclusive with the corresponding K); `retain_rank` (>=0, holding rank buffer); `dropout` (>=0, replacement count in ordinary rounds); `group_quota` (map, values >=0, groups supplied by the caller); `missing_scores`: `skip` (default) | `shrink` | `cash`. Requires `long_notional+short_notional>0`.

**rebalance**: `every_bars` (positive integer, default 1), `phase` (<every_bars, in bars), `anchor` (absolute ms anchor, default 0), `duration` (e.g. `4h`), `calendar` (`daily|weekly|monthly`, requires `calendar_version`, anchor/phase forbidden, `timezone` default UTC; rounds change by timezone date/ISO week/month). Pick one of the three. Round number = `floor((grid-anchor-phase*bar)/interval)`, rebalance only when it increases, the first decision always triggers; holiday/score-change triggers need a Go callback.

**holding**: `min_bars`/`max_bars` or `min_duration`/`max_duration` (Go duration, positive; bars and duration not mixed; `min<=max`).
- `min_bars`: ordinary exit (exit only on dropout) is allowed only once the age since first fill reaches this. `max_bars`/`max_duration`: at expiry the first monitoring grid directly produces a zero target, bypassing the rebalance gate and the ordinary exit curve.
- `by_asset`: overrides by `snapshot.sid_map` symbol, distinguishing "omitted" from "explicit 0"; priority: explicit by_asset -> visible resolver rule -> default.
- `cooldown_bars` (>=0), `adopt` (`adopt`|`reject`, default reject): for existing positions without age evidence the default is to refuse to guess; `adopt` counts age from takeover.

**transition** (`mode`: `direct` (default) | `linear-exit` | `cohort` | `geometric` | `target-step`):

| mode | Fields/behavior |
| --- | --- |
| `direct` | Converge directly to the target at rebalance |
| `linear-exit` | After dropout, go to zero in N steps from the original base; `exit_steps>0`, `basis: quantity|weight` required, `on_reselect` defaults to `restore`. quantity anchors to the confirmed quantity at exit start, rounds toward zero, no top-up on price rise; weight multiplies the latest frozen NAV each round, may top up; the Nth step target is explicit zero |
| `geometric` | Each step exits by a remaining ratio: `ratio∈(0,1)`, `final_threshold∈(0,1)`, `basis` required as above, `on_reselect` optional |
| `cohort` | Fixed-lifetime cohorts: `period_bars` must be a positive integer multiple of `every_bars` (must use `every_bars` scheduling); `startup: gradual|seed-all` (default gradual); `sizing: entry-nav|current-nav` (default entry-nav); `entry_window_bars` (default = every_bars); `holding.min_bars<=period_bars`, `min_duration` cannot be used |
| `target-step` | Each step moves `alpha∈(0,1]` toward the new target, new assets interpolate from zero; progressive exit with alpha<1 may keep a tail position for a long time |

- `on_reselect`: `restore` (restore the ideal share) | `resume` (keep the current smaller share) | `finish` (continue the original exit) | `new-cohort`; only writable for linear-exit/geometric. `by_asset`: per-asset overrides of `exit_steps` (linear-exit) or `ratio` (geometric, [0,1)), not allowed in other modes.
- Fields not matching the mode (e.g. `alpha` with linear-exit, `basis` with direct/cohort/target-step) fail preflight.

**allocation**: `method`: `equal` (default) | `score` | `fixed-notional` (`fixed_notional>0`, writable only here) | `inverse-volatility` | `vol-target` (`vol_target>0`, writable only here). Common: `reserve_ratio∈[0,1)`, `asset_cap`, `group_caps` (groups need injection via `Config.PolicyContext`), `turnover_limit` (limits rebalance amount, not holding count, forced risk exits bypass it), `net_cap`, `beta_cap` (the latter two need beta/group evidence, both >=0). inverse-volatility and vol-target need injected `Volatility` (does not take a same-named factor column).

### 4.3 Lifecycle semantics highlights

- The number of holdings can exceed K (minimum holding period/progressive exit retains dropped-out old positions); there is no `max_long_positions`.
- Missing scores do not advance ordinary exit steps, but max-age and cohort-expiry are still checked. Reversal by default waits for the original direction to actually reach zero (unknown in-flight does not count as released); re-entry builds a new lifecycle; adding to a position does not reset first-fill age.
- First-fill evidence comes from `FillEvents`; `RebalanceDue/ForceExit/HoldingRules/TransitionRules` can be injected via `Config.PolicyContext`.
- Position evidence: Weights uses Book, Events uses the shared paper account, live uses the real account; pure CLI `research` has no position evidence, enabling a stateful policy fails.
- The policy reads only the frozen context and the accepted JSON state (<=1 MiB, <=4096 active assets), returns target/next state/diagnostics, and does not place orders directly; state advances only after the account atomically accepts; accepted-but-failed-to-send does not roll back and does not regenerate the plan.

### 4.4 Go portfolio extensions

```go
type PortfolioBuilder func(factor.Frame, factor.Universe, factor.PortfolioSpec, research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error)
func RegisterPortfolioBuilder(name string, builder PortfolioBuilder) error          // name unique, must not be top-bottom-k-v1
func RegisterPortfolioBuilderIdentity(name, hash string) error                    // builder must be registered first
type PortfolioFactory func(Config) (PortfolioBuilder, error)
type PortfolioValidator func(Config) error
func RegisterPortfolioFactory(name string, factory PortfolioFactory, validators ...PortfolioValidator) error // name contains "-v", at most one validator
func RegisterPortfolioPolicy(name string, factory factor.PortfolioPolicyFactory) error   // name contains "-v"; only lifecycle-v1 built in
type PortfolioPolicy interface { Propose(factor.PortfolioContext, json.RawMessage) (factor.PortfolioProposal, error) }
type PortfolioPolicyFactory func(factor.PortfolioPolicyConfig) (PortfolioPolicy, error)
func RegisterModelPortfolioBuilder(name string, config ModelBuilderConfig) error  // ModelBuilderConfig{ManifestID, Artifacts []research.ModelArtifact}
func RegisterRiskPortfolioBuilder(name string, config RiskBuilderConfig) error  
```

- Types: `Frame{GridTime, SnapshotID, PlanHash, DecisionTime, Values map[column name]map[SID]Numeric}` (includes output columns and the combined column `score`); `Universe{Version, Investable, Reference, Tradable, Evaluation, Tracked []int32, Static}`; `Numeric{Value, Validity}` (`valid missing null not-numeric non-finite warmup`); `Diagnostic{Code, Detail}`; `PortfolioSpec` includes StrategyID/AccountID/DecisionTime/ExecutableAt/ExpireAt/Budget{Version,Currency,NAV}/Mode etc.
- `PortfolioContext` includes Frame, Universe, Ideal, Spec, ScoreName, Positions (`map[SID]PositionEvidence{Quantity,PendingQuantity,FirstFillTime,FillEvents..}`), Marks, Groups, Volatility, Beta, AssetNames, ForceExit, Previous, HoldingRules, TransitionRules, RebalanceDue, CapitalLimit, etc.; `PortfolioProposal{Target, NextState, Reasons, AcceptanceID, ReconcileSIDs, PlanSequence, DecisionTime, ExpireAt}`.
- `TargetPortfolio`: `NewTargetPortfolio(spec, targets)`, `Spec/ID/Targets/EffectiveTargets(prev)/Notional(sid)`. `PortfolioTarget`: `NewPortfolioTarget(spec, map[SID]Allocation)`; `Allocation{Basis: nav-fraction|absolute-quantity, Value string}`; `Allocations()`, `AsWeightPortfolio()` (errors on quantity targets), `PortfolioTargetFromWeights`, `EffectiveAllocations(prev)`.
- Lifecycle: `factor.SelectPortfolio(frame, universe, spec, cfg)`, `SelectPortfolioScore(..., scoreName, groups)`, `AllocateSelected(selected, cfg, nav, scores, vol)`, `NewLifecyclePolicy`.
- Registered names must carry a version (e.g. `rotation-v1`); when parameters/implementation change, bump the version or change the identity hash.

## 5. Go Registration Entry Points and Interfaces

### 5.1 `runner.RegisterFactor`

```go
type FactorContext struct { TimeFrame string; Params *Params }
func (f FactorContext) Close() *factor.Node          // factor.Field("kline","close", TimeFrame)
func (f FactorContext) Field(name string) *factor.Node
type FactorSpec struct { Outputs map[string]*factor.Node; Combine research.ComboSpec }
func RegisterFactor(name string, build func(FactorContext) (FactorSpec, error)) error
```
- `Outputs` must be non-empty; defaults to `equal` + all outputs, can be specified via `Combine` (YAML `combo` overrides).

### 5.2 `runner.RegisterDefinition`

```go
type DefinitionBuilder func(runner.Config) (*factor.Plan, research.ComboSpec, error)
func RegisterDefinition(name string, builder DefinitionBuilder) error   
func CompileDefinition(c runner.Config) (*factor.Plan, research.ComboSpec, error)
func DefinitionCatalog() (builders, portfolios []string)
```
- Built-in `momentum-vol` (5.5); `definition` defaults to `run_policy.name`; `expressions`/`Plan`/`definition` are mutually exclusive (passing `Plan` requires explicit `Combo`).

### 5.3 `runner.Config`

```go
type Config struct {
    Definition string; Expressions *expr.Spec; Plan *factor.Plan
    Combo research.ComboSpec          // outer override
    Manifest research.ManifestSpec    // Parameters, Labels, Portfolio, Costs, Currency
    Factor research.MomentumVolConfig // Source, Field, TimeFrame, Window, DDOF, WinsorTail, Standardize
    PortfolioBuilder PortfolioBuilder // overrides YAML when non-nil
    PolicyContext func(context.Context, *factor.PortfolioContext) error
    Prices PriceStream; FundingSource, OptionExpirySource, TerminalSettlementSource string
    DecisionInterval, LatencyMS, ExpiryMS, LabelWaitMS, DecisionDelayMS int64
    MaxRecords, MaxPending int; InitialNAV, AccountInitialNAV float64
    SourceEventDecisions bool; Execution ExecutionConfig; StrategyID, AccountID string
}
func (c Config) Params() *Params   // reads Manifest.Parameters (private copy)
```
- `Params`: `Float/Int/Bool(name, def)` (Int errors on fractions, Bool takes 0/1), `Err()` (first error), `Unused()` (unread params, to check typos; `window`, `k` not counted).

### 5.4 Research and label interfaces (`factor/research`)

```go
type LabelSpec struct{ /* same as 1.5 labels; Kind: executable-return|close-to-close */ }
func ReturnLabel(spec LabelSpec, sid int32, decision, begin, end, available int64, beginPrice, endPrice factor.Numeric) (Label, error)
type ComboSpec struct{ /* fields same as 1.4, Method is ComboMethod */ }
func Combine(frame factor.Frame, universe factor.Universe, spec ComboSpec, history *ICHistory) (map[int32]factor.Numeric, []factor.Diagnostic, error)
func NewICHistory(window int, label string, columns []string) (*ICHistory, error)
func Evaluate(frame factor.Frame, universe factor.Universe, labels []Label, spec EvaluationSpec) (Report, error)
type ManifestSpec struct{ /* fields same as 1.9 manifest keys */ }
func BuildManifest(spec ManifestSpec) (*Manifest, error)
type PortfolioDefinition struct{ /* fields same as 4.1 portfolio keys; Rebalance/Selection/Holding/Transition/Allocation are *factor.XxxConfig */ }
```
- `Report`: per-column Coverage, missing-reason counts, per-label IC/RankIC/quintile returns/monotonicity, turnover, rank autocorrelation, risk exposures; cross-sectional correlation matrix; `Performance` (gross/net return, one-way turnover, cost); `EvaluationSpec`: `AsOf PrimaryLabel CostRate CurrentWeights PreviousWeights PreviousColumns Exposures`.
- Rolling training: `VisibleTrainingRows(rows, window)`, `RollingWindows(start, end, trainMS, testMS, stepMS, embargoMS, minSamples)`; models: `RegisterModel`, `FitModel`, `RestoreModel/PublishModel/LoadModel`, `LinearModel`; trials: `OpenTrialLedger(path, max)`, `CompareTrials`, `SelectParameters`, `ResolveParameters`, `ResearchCache`; risk: `EstimateCovariance(returns, shrinkage, diagonal)`, `OptimizePortfolio(OptimizationSpec)` (over-limit results cannot be used as ordinary targets).
- Others: `HACMean(values, lag)` (Newey-West for overlapping returns), `RankAutocorrelation`, `SummarizeLifecycles`, `Attribute`, `EstimateCapacityCost`.

### 5.5 Built-in `momentum-vol`

Price `kline.close`; `Window=24` (`params.window`, integer >=2), `DDOF=1`, `WinsorTail=0.01` (winsorizes only if >0), `Standardize=true` (zscore only if true). Momentum = `zscore(winsorize(ts.return(close,window)))`; volatility = `zscore(winsorize(ts.std(ts.return(close,1),window,ddof)))`; `fixed`: momentum +1, volatility -1. Params `window`, `k`.

### 5.6 Go example

```go
package factors

import (
    "github.com/banbox/banbot/factor"
    "github.com/banbox/banbot/factor/runner"
)

func init() {
    if err := runner.RegisterFactor("my-mom-v1", func(c runner.FactorContext) (runner.FactorSpec, error) {
        ret := factor.Return(c.Close(), c.Params.Int("window", 24))
        return runner.FactorSpec{Outputs: map[string]*factor.Node{"mom": factor.ZScore(ret)}}, c.Params.Err()
    }); err != nil {
        panic(err)
    }
}
```
Corresponding YAML (no `expressions`): `run_policy: [{name: my-mom-v1, engine: factor, run_timeframes: [1h], params: {window: 24, k: 10}, archive: data.gob, research: {labels: []}}]`
The entry program must `import _` this package.

## 6. Node API (`factor`)

```go
type Numeric struct { Value float64; Validity Validity }  // Valid, Missing, Null, NotNumeric, NonFinite, Warmup
func Number(values map[string]any, field string) Numeric
func Field(source, field, timeframe string) *Node
func AsOfField(source, field, sourceTF, decisionTF string, maxAge int64) *Node
func Constant(v float64, tf string) *Node
func Add/Sub/Mul/Div/Pow/Min/Max(a, b *Node) *Node
func Neg/Abs/Log/Sqrt/Positive(x *Node) *Node
func Lag/Return/EMA/SMA/RMA/WMA/RSI/ROC/MOM/CCI/Highest/Lowest(x *Node, period int) *Node
func StdDev(x *Node, period, ddof int) *Node
func VWMA(p, v *Node, period int) *Node; ATR(h,l,c *Node, period int); Stoch/WillR(h,l,c *Node, period int)
func TR(h,l,c *Node) *Node; OBV(c,v *Node) *Node; MFI(h,l,c,v *Node, period int) *Node
func MACD(x *Node, fast, slow, signal int) (line, signalLine, hist *Node)
func BBands(x *Node, period int, stdUp, stdDown float64) (upper, middle, lower *Node)
func Rank/ZScore/RobustZScore(x *Node) *Node
func Winsorize(x *Node, tail float64) *Node; MADWinsorize(x *Node, k float64) *Node; Quantile(x *Node, q float64) *Node
func GroupDemeanByField(x, field *Node) *Node; GroupZScoreByField(x, field *Node) *Node
func Residual(y, x *Node) *Node; MultiResidual(y *Node, xs ...*Node) *Node; WeightedResidual(y, w *Node, xs ...*Node) *Node
func Linear(inputs []*Node, weights []float64) *Node
func Custom(version string, inputs []*Node, evaluate func([]Numeric) Numeric) *Node   // purely pointwise, must propagate invalid reasons
func New() *Builder; (*Builder).Add(name string, n *Node) *Builder; (*Builder).Compile() (*Plan, error)
func Compile(outputs map[string]*Node) (*Plan, error)
func (p *Plan) Hash/TimeFrame/Outputs/Inputs/WarmupLength/StateRetention/NodeCount
```
- `GroupDemean`/`GroupZScore` (without ByField) are legacy interfaces with no age limit; new code uses `...ByField`, and the categorical field must be `factor.Field` or `AsOfField`.
- `Custom` must not keep rolling state in the closure; history-dependent operators require extending the kernel.
- Indicator windows [1,10000]; MACD requires fast<slow; BBands multipliers non-negative. `Session`/`Plan.Batch` are held by the runtime, strategies do not call them directly.

## 7. Commands, Replay, Settlement, Live

### 7.1 Commands

- `banbot validate|explain --spec f.yml`: standalone expression compilation (no data, no accounts).
- `banbot data archive --input records.jsonl --out chunk.gob [--schema s.yml] [--max-records N]`: freeze VersionRecord JSON lines into an archive.
- `banbot research --config cfg.yml`: factor research, outputs panel rows, `decision`, `evaluation` (maturity diagnostics), summary JSON lines; creates no accounts.
- `banbot backtest [--mode weights|events] --config cfg.yml [--out DIR]`: factor backtest; `--separate` does not support factors; `--mode weights` requires a factor strategy.
- `banbot trade --dry-run --config cfg.yml`: factor/mixed Events historical replay (forces events; pure time-series simulation still uses `env: dry_run`).
- `banbot trade --config cfg.yml [--live-provider NAME]`: live trading.
- `--no-default` skips `config.yml`, `config.local.yml`; `@`/`$` paths need a data directory; archive replay can be fully offline.
- Event JSON lines are written to stdout and `events.jsonl`. If `--out DIR` already exists, `DIR_1`, `DIR_2`... is used; default `<DataDir|BanDataDir|archive dir>/backtest/factor`. Artifacts: `config.yml` (effective config), `resolved.json` (actual values and sources of each field), `events.jsonl`, `strategy-N.json`, `run.json` (`{Version, Status(complete|incomplete), Errors, Results}`; any Unresolved makes it incomplete); Events additionally has `account-<account>/manifest.json`, `chunk-NNNNNN.gob`. `Result` includes Decisions, Executions, Skipped, Incomplete, Unresolved, TargetsAccepted, Fills, Book, Account, Summary, IncompleteStreams, etc.; Weights backtests also carry `Attribution` (long/short leg PnL, turnover, average exposure, fee/slippage/funding/PIT costs, `TopSymbols` with per-instrument net contribution and share; `FinalNAV−InitialNAV = LongPnL+ShortPnL−costs+Residual`, Residual is float rounding only); pairs strategies also carry `Pairs` (per pair Episodes, PnL, Trades, WinRate, InMarketBars, PausedBars, AvgHoldBars, LastBeta, Turnover) and the reconciliation fields `PairsGrossPnL/PairsNAVReturn/PairsResidual` (per-pair PnL is approximated at decision-time prices, before costs; residual ≈ costs + execution approximation). `research.panel: none` skips the per-decision panel rows (event lines, Summary and ledger unchanged; use it for million-row outputs).

### 7.2 Replay semantics

- Fills occur after decision completion, before expiry (exclusive), at an observable price strictly later than the decision and >= `decision+latency_ms`.
- Fees/slippage come from `manifest.costs`. Events paper: all-market fills, directional slippage, fees; no queueing/partial fills/network failures simulated; limit orders fill when a later quote crosses the limit. Weights: retained quantity persists between explicit decisions, fees charged on the actual notional change.
- Terminal settlement `terminal_settlement_source` (Weights/Events) and option expiry `option_expiry_source` (Events only) must be explicit sources different from both the price and funding sources.
  - Terminal settlement record: `Values["terminal_cash_value"]` a non-negative decimal **string**, `TimeFrame:"event"`, `TimeMS==EventTime`, `SourceVersion` non-empty, `AvailableAt>=EventTime`, `IngestedAt>=AvailableAt`, SID is a declared paper instrument; a non-zero target after terminal settlement errors.
  - Option expiry record: SID = the option, `EventTime` = expiry time, `AvailableAt` = publication time, `Values["fixing"]` a decimal string (rejected if above `underlying_stress_max`).
  - Options are available only in Events paper (cash-settled European, no early exercise/physical delivery); Weights reports `cash options require Events paper accounting`, live rejects; risk reserve: long = premium; short put = `strike x units`; short call = `max(stress_max-strike,0) x units`; different contracts do not offset; orders only before expiry.

### 7.3 Cold history (`execution.history`)

Cold-history SQLite archive, Events only: after resolution the path must be absolute and the file must **not exist** (otherwise `file exists`), mutually exclusive with `store`/`sender_lease_dir`, live rejects, each account uses a different path; silently ignored under Weights.

### 7.4 Live (`trade`)

- Live YAML **does no archive/storage derivation**, must be explicit: `prices` (event|1m), `snapshot` (`universe`, `sid_map`, `source_versions`, non-empty `visibility_policy`; `universe.version` non-empty), `instruments` (`id` must equal the `sid_map` name), the four risk limits, `funding_policy` (`explicit-zero` (requires verified absence of funding obligations) | `required-stream` (requires `funding_source`)). `archive/chunks`, `history`, `paper_fill_model`, `source_event_decisions`, options, terminal-settlement stream, and history IC methods are rejected at validation.
- `live_provider` (priority `--live-provider` > `accounts.<name>` > `execution` > default): default/`banexg`/`hub` = **Hub mode**: a separate `banbot hub` process holds credentials, the bot connects via Unix socket (`execution.hub_socket`, default `<datadir>/execution/hub.sock`) and only does public market data and computation; requires `env` to be `prod` or `dry_run` and `market_type: linear`, `dry_run` requires `funding_policy: explicit-zero`; Hub and embedded providers cannot be mixed, embedded does not allow `dry_run`. Other names must be registered by calling `entry.RegisterFactorLiveBinding(name, factory)` (otherwise `unsupported live capability`); the factory provides Banexg transport, symbol metadata, publish/revision mapping and a funding validator, and `store`/`sender_lease_dir` must be absolute paths.
- Built-in banexg embedded binding (non-Hub path): `env: prod`; linear perpetuals only (`Linear && Swap`, units step/tick/contract size/precision must match exchange metadata, `min_steps`/`min_notional` not lower than the exchange); one-way net positions only (hedge positions rejected); `funding_policy` must be `required-stream` and the exchange must have authoritative funding cash capability. Missing capabilities fail directly, no degradation, no fallback to paper.
- Existing positions must first have ledger attribution and startup reconciliation; `InitialNAV` does not deposit into the real account; account/strategy/currency identity is validated against the committed checkpoint.

## 8. Research Notes and Statistical Tools

- Do not back-infer historical performance from full-sample optimal parameters.
- `factor/statistics`:
  - Labels: `TripleBarrierLabel`, `CalendarBarrierLabels`, `EventUniqueness`, `SequentialBootstrap`, `FractionalDifference`, `SelectStationaryFFD`.
  - Cross-validation: `PurgedKFold(spans, folds, embargoBars)`, `CombinatorialPurgedCV`, `RunCPCVResearch`; models: `TrainRandomForest`, `TrainSequentialBaggedClassifier`, `PurgedParameterSearch`, `RefitPurgedSearch`, `PurgedFeatureImportance` (fitting must be done inside the training fold).
  - Dimensionality reduction/combination/tests: `FitPCA`, `FitFactorAnalysis`, `FactorNeutralWeights`, `HierarchicalRiskParity`, `LongOnlyMinimumVariance`, `PCARiskWeights`, `ProbabilisticSharpe`, `DeflatedSharpe`, `CombinatorialSymmetricCV`.
- Iterative fitting and portfolio experiments must explicitly set iteration/work/trial limits.

## 9. Complete Example

Momentum + low-volatility expressions, Weights backtest, archive input (`banbot backtest --no-default --mode weights --config s.yml --out res`):

```yaml
stake_currency: [USDT]
wallet_amounts: {USDT: 10000}
execution: {mode: weights, funding_policy: explicit-zero}
run_policy:
  - name: MultiFactor
    id: multi_v1
    engine: factor
    run_timeframes: [1h]
    params: {k: 3}
    archive: chunk.gob
    prices: {source: tick, timeframe: event, field: price}
    portfolio: {long_notional: 0.5, short_notional: 0.5, mode: full}
    research: {labels: []}
    expressions:
      schema_version: 1
      timeframe: 1h
      bindings:
        kline: {source: kline, timeframe: 1h}
      params: {window: 24}
      lets:
        price: 'positive(kline.close)'
        ret1: 'ts.return(factor.price, 1)'
      outputs:
        momentum: 'cs.zscore(ts.return(factor.price, param.window))'
        low_vol: 'cs.zscore(-ts.std(factor.ret1, param.window, 0))'
      combine:
        method: fixed
        weights: {momentum: 0.7, low_vol: 0.3}
```

- Storage replay: remove `archive`, add `exchange: {name: binance}`, `pairs`, `timerange: 20240101-20240601`, `data: {pit_policy: static-approximation}`.
- Research: remove `research: {labels: []}`; lifecycle/cohorts: replace `portfolio` with the 4.2 example.

## 10. Unconfirmed Points

Verified by code reading only, not tested live: storage replay and Hub live (Hub's restrictions on funding strategies unknown), embedded banexg binding and custom providers, terminal/option-expiry record formats, numeric behavior of `retain_rank`/`dropout`/`group_quota`/`on_reselect`/`adopt`/`cooldown_bars`/`geometric`/`target-step`, history IC methods (only config validation checked); live was verified only up to session initialization.
