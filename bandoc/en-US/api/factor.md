# Factor and cross-sectional API


This page covers factor, factor/expr, factor/research and factor/runner. See [Multi-factor strategies](../guide/factor.md) for configuration and workflows.

CLI startup is unified: root `backtest` / `trade` dispatch factor, time-series or mixed policies from one YAML `RunSpec`. Root `research`, `validate --spec`, `explain --spec` and `data archive` provide research, standalone compilation and archive conversion. Embedding APIs below retain their existing contracts.

| Package | Components | Responsibility |
| --- | --- | --- |
| factor | Plan, Session, Batch, VersionStore, Snapshot, Universe, RoundBarrier, TargetPortfolio, PortfolioTarget, PortfolioPolicy | Visibility snapshots, computation and lifecycle proposals |
| factor/expr | Spec, Compile, CloneSpec | Startup compilation and complete declaration validation |
| factor/research | ComboSpec, ICHistory, LabelQueue, ManifestSpec, model/risk/experiment APIs | Matured research and artifacts separated from current inference |
| factor/backtest | Book | Approximate quantity accounting with allocation support |
| factor/runner | Config, Run, NewLive, ComputationGroup, AccountSink | Replay/live drivers and account targets |

## Registration and compilation

runner.RegisterDefinition(name string, builder runner.DefinitionBuilder) error registers a Go graph. DefinitionBuilder is func(runner.Config) (*factor.Plan, research.ComboSpec, error). Duplicate names fail; momentum-vol is built in.

runner.RegisterPortfolioBuilder registers a uniquely versioned PortfolioBuilder accepting Frame, Universe, PortfolioSpec and PortfolioDefinition and returning a target, diagnostics and error. top-bottom-k-v1 is reserved.

`runner.RegisterPortfolioPolicy(name string, factory runner.PortfolioPolicyFactory) error` registers a complete stateful policy. Names include a version, such as `my-policy-v1`; duplicates fail. Each run/strategy receives a private instance from its factory. `lifecycle-v1` is built in. A builder supplies the ideal portfolio; a policy uses actual positions and pending evidence to propose an admissible target. Explicit builders retain their ideal weights when allocation is omitted.

runner.CompileDefinition uses the same definition resolution as replay/live. Config.Expressions is exclusive with Plan/Definition. Explicit schema, missing-data policy and operator versions define reproducible graph identity.

## A compilable Go definition

Compile this entry in your strategy project before selecting CodeMomentumV1. It declares two native factor outputs and handles registration failure:

```go
package main

import (
    "fmt"
    "github.com/banbox/banbot/entry"
    "github.com/banbox/banbot/factor"
    "github.com/banbox/banbot/factor/research"
    "github.com/banbox/banbot/factor/runner"
)

func buildMomentum(c runner.Config) (*factor.Plan, research.ComboSpec, error) {
    if c.Factor.Source == "" || c.Factor.Field == "" || c.Factor.TimeFrame == "" || c.Factor.Window < 2 {
        return nil, research.ComboSpec{}, fmt.Errorf("source, field, timeframe and window >= 2 are required")
    }
    price := factor.Positive(factor.Field(c.Factor.Source, c.Factor.Field, c.Factor.TimeFrame))
    returns := factor.Return(price, c.Factor.Window)
    plan, err := factor.New().
        Add("momentum", factor.ZScore(returns)).
        Add("momentum_rank", factor.Rank(returns)).
        Compile()
    return plan, research.ComboSpec{
        Method: research.Equal,
        Columns: []string{"momentum", "momentum_rank"},
    }, err
}

func main() {
    if err := runner.RegisterDefinition("CodeMomentumV1", buildMomentum); err != nil {
        panic(err)
    }
    entry.RunCmd()
}
```

Replace the built-in policy with this declaration; retain the market/history/execution settings. definition and expressions are exclusive:

```yaml
run_policy:
  - name: custom_momentum
    engine: factor
    run_timeframes: [1h]
    params: {window: 24, k: 1}
    definition: CodeMomentumV1
```

## Technical indicators

All 22 scalar operators below support `Plan.Batch`, incremental `Session` and `factor/expr`. Inputs are ordinary field/expression nodes; custom columns continue through `DataSeries.Values`. The input names h/l/c/v below mean explicit high/low/close/volume nodes.

| Go constructor | DSL functions | Meaning |
| --- | --- | --- |
| `SMA(x,n)` | `ts.sma(x,n)` | Arithmetic moving mean |
| `RMA(x,n)` | `ts.rma(x,n)` | Wilder smoothing, seeded with an n-observation mean |
| `WMA(x,n)` | `ts.wma(x,n)` | Weights 1 through n, newest weight n |
| `VWMA(c,v,n)` | `ts.vwma(c,v,n)` | `sum(c*v)/sum(v)` |
| `RSI(x,n)` | `ts.rsi(x,n)` | Wilder relative strength, 0–100 |
| `ROC(x,n)` | `ts.roc(x,n)` | Percentage change, `100*(x-prev)/prev` |
| `MOM(x,n)` | `ts.mom(x,n)` | Absolute change, `x-prev` |
| `TR(h,l,c)` | `ts.tr(h,l,c)` | True range with previous valid close |
| `ATR(h,l,c,n)` | `ts.atr(h,l,c,n)` | Wilder smoothing of true range |
| `CCI(x,n)` | `ts.cci(x,n)` | Commodity channel index of the explicit price expression |
| `Stoch(h,l,c,n)` | `ts.stoch(h,l,c,n)` | Raw stochastic %K, no extra smoothing |
| `WillR(h,l,c,n)` | `ts.willr(h,l,c,n)` | Williams %R, normally -100–0 |
| `OBV(c,v)` | `ts.obv(c,v)` | On-balance volume, initial value is first volume |
| `MFI(h,l,c,v,n)` | `ts.mfi(h,l,c,v,n)` | Typical-price money flow index |
| `Highest(x,n)`, `Lowest(x,n)` | `ts.highest(x,n)`, `ts.lowest(x,n)` | Rolling extremes |
| `MACD(x,fast,slow,signal)` → line, signalLine, hist | `ts.macd(...)`, `ts.macd_signal(...)`, `ts.macd_hist(...)` | Fast-minus-slow EMA, signal EMA, line-minus-signal; histogram has no factor of 2 |
| `BBands(x,n,up,down)` → upper, middle, lower | `ts.bbands_upper(...)`, `ts.bbands_middle(...)`, `ts.bbands_lower(...)` | SMA ± explicit multiples of population standard deviation |

Every DSL call is scalar. MACD calls all take `(x,fast,slow,signal)`; every Bollinger call takes `(x,n,up,down)`, including middle. Periods must be integers in `[1,10000]`, MACD requires `fast < slow`, and band multipliers must be finite and nonnegative. `ROC` returns 10 for a move from 100 to 110; existing `Return` returns 0.1.

New indicators advance only on complete valid input tuples. An invalid dependency propagates its original validity and skips that observation without resetting state. ROC/MOM periods count valid tuples; existing Return/Lag keep their original observation-position behavior. Flat Stoch is 50; flat RSI follows tav at 100; zero negative MFI flow remains undefined. Undefined NaN results retain `Warmup` validity; infinite results have `NonFinite` validity.

Lookback is n-1 for rolling averages/extremes/bands, CCI, Stoch, WillR and MFI; n for RSI/ROC/MOM/ATR; 1 for TR; 0 for OBV; slow-1 for MACD line; slow+signal-2 for signal/hist. Upstream lookbacks accumulate, and missing observations can extend elapsed warmup. Continue the same Session across chunks for recursive state. See the repository [indicator guide](https://github.com/banbox/banbot/blob/main/doc/factor_indicators.md) for examples and retention formulas.

## Simple registration and statistical arbitrage

`runner.RegisterFactor(name string, build func(runner.FactorContext) (runner.FactorSpec, error)) error` registers a simple cross-sectional factor. `FactorContext.Close()` supplies a price node and `Params` reads parameters. Check both the registration error and `Params.Err()` in the builder. `FactorSpec.Outputs` contains named nodes; omitted Combine uses equal weights.

`runner.RegisterPairs(name string, build func(*runner.Params) (runner.PairsSpec, error)) error` registers rolling log-regression pairs and creates private builder/statistics state for each run. `PairsSpec` contains `Pairs []runner.Pair` (canonical Y/X symbols), Field (default close), Window (at least 3), Band and Gross (default 1/pair count, total budget at most 1). Entry β stays fixed while holding. After a Band Stop/MaxHold exit, re-entry waits until the Exit band is reached.

`statistics.NewRollingSpread(window)` exposes `Observe(x, y float64) (statistics.SpreadPoint, bool)` for two log prices. `statistics.NewBandTrader(config)` exposes `Step(z float64) int` returning -1/0/+1. Use separate instances per spread. These compute statistics/direction; they do not place orders or prove cointegration, synchronized fills or profitability. See the [pairs example](../guide/factor.md#statistical-arbitrage-declare-the-pairs).

`RegisterPortfolioFactory(name, factory, validator)` supports mutable custom portfolio state. The validator only checks configuration and must not borrow accounts or start work. `PreparePortfolioConfig` creates a single-run instance after preflight; do not reuse it in another backtest. A missing leg requests pair closure and pauses it. Pairs explicitly marked unavailable for the entire window are skipped without reallocating their budgets.

## Configuration ownership

runner.CloneConfig(c Config) (Config, error) owns chunks, snapshot Universe lists/maps, expressions, combo, manifest and execution instruments. It preserves list order, duplicates and nil/empty distinctions. One JSON serialization check retains NaN/Inf rejection.

Plan, ComputationGroup, PortfolioBuilder, PolicyContext, HistoricalInput, ObserveBatch and the internal timeline remain borrowed. Copying opens no input/account and does not replace ValidateReplayConfig/ValidateLiveConfig. Owners keep borrowed services usable until all drivers Join. expr.CloneSpec, research.CloneComboSpec and research.CloneManifestSpec expose the corresponding container copies; policy maps, override pointers and policy_params are also copied.

## Lifecycle proposals and allocation targets

```go
type PortfolioPolicy interface {
    Propose(factor.PortfolioContext, json.RawMessage) (factor.PortfolioProposal, error)
}
```

This is the signature of `factor.PortfolioPolicy`. PortfolioContext supplies a frozen Frame/Universe, ideal portfolio, PortfolioSpec, strategy NAV, read-only Positions/Marks, StateVersion, LedgerCursor and Previous. `Config.PolicyContext` can add visible Groups, Volatility, Beta, ForceExit, RebalanceDue, HoldingRules and TransitionRules. Explicit by_asset overrides take precedence over resolver rules, then defaults. Validate parameter artifacts with `research.ResolveParameters` before using their hash, manifest, training cutoff and availability.

`PositionEvidence.FirstFillTime` and FillEvents represent actual fills. Generating/admitting a target or submitting an order does not start holding age. PortfolioProposal returns Target, NextState, Reasons, AcceptanceID and ReconcileSIDs for cancellation of increasing orders. NextState must be valid JSON within `factor.MaxPortfolioStateBytes`; custom opaque state does not use the built-in LifecycleState schema. A state-only proposal that changes its checkpoint also requires AcceptanceID.

`factor.NewPortfolioTarget(spec, allocations)` creates an immutable, content-addressed version 1 target. Getters return owned copies. Each SID has an Allocation with a finite plain decimal string Value and one of these bases:

| Basis | Meaning |
| --- | --- |
| `nav-fraction` | Signed fraction of strategy NAV |
| `absolute-quantity` | Signed standard asset quantity, rather than contract count |

Full zeros omitted previous assets; Patch retains omitted targets. Identity includes strategy, account, budget, sequence, frozen snapshot and validity window; policies must preserve it. `PortfolioTargetFromWeights` adapts legacy TargetPortfolio. `AsWeightPortfolio` accepts weight-only targets and rejects absolute quantities. Outputs consuming quantities implement `runner.AllocationOutput`; JSONL uses `allocation-decision` / `allocation-accepted`.

`runner.PolicySink` supplies PolicyEvidence and AcceptProposal. The weights Book supplies its own evidence/admission; events/live use the account owner. Pure research has no default position book: an embedding that enables a policy must supply an allocation-capable PolicySink. Ordinary CLI `research` cannot acquire this capability through YAML alone.

The owner atomically admits the plan, target and checkpoint with StateVersion/LedgerCursor fences. Changed evidence triggers a new proposal using the same frozen Frame. State-only changes also use a transaction. Repeated admission verifies content; conflicts fail. Recovery restores persisted sequences, checkpoint, previous target and fill provenance. Sending can fail after admission; the accepted receipt remains valid and cannot be rolled back or treated as a fill.

Live execution scope retains price/funding subscriptions for actual positions, pending orders and active cohorts. Leaving the selection Universe retains tail positions until zero and settled. Cohorts track aggregate execution contributions and conserve internal net transfers. Separate batch execution lots, strict per-batch fill deadlines and per-batch PnL are future extensions.

See the [lifecycle guide](../guide/factor.md#independent-rebalancing-and-holding-lifecycle) for defaults, modes and examples. Omitting policy preserves the legacy builder, weight contract and configuration identity.

## Multiple horizons and historical combination

Manifest.Labels accepts multiple executable-return horizons. Each captures execution prices, matures and reports unresolved labels independently, sharing a frozen Frame and bounded queues. ComboSpec.Label selects the label used for historical weights. If omitted with multiple labels, the shortest horizon wins, with name ordering for ties, and the choice enters the manifest. An omitted single label retains its existing identity.

Alongside equal/fixed/history-ic, methods include history-rank-ic, history-icir, history-rank-icir and history-ewma. MinSamples counts historical cross sections; MinPairs counts valid asset pairs per section. MinConfidence is an unadjusted mean/standard-error threshold. Decay is EWMA alpha, with zero selecting 0.2; Direction defaults to signed and accepts positive; Fallback defaults to equal and accepts fixed/error. Both insertion and weight selection check decision time, maturity and visibility. Live has no matured-history provider and rejects every history method.

ColumnMetrics.RankAutocorrelation compares adjacent reference sections; Decay() reports IC by horizon. Accumulator tracks column/label order independently, allowing older long-horizon decisions to mature after newer short-horizon ones. Use `research.HACMean(values, lag)` for overlapping returns, choosing lag from the actual sampling/holding period.

## Native Go research extensions

| API | Purpose and boundary |
| --- | --- |
| `factor.RobustZScore` / `MADWinsorize` / `MultiResidual` / `WeightedResidual` | Reference-fitted robust transforms and multi-exposure OLS/WLS; see [expressions](../guide/factor.md#robust-expressions-and-research-extensions) |
| `runner.ScanPortfolioTrials` | Bounded scans with isolated configuration/state; full-sample winners cannot be used in the past |
| `research.SelectParameters` / `ResolveParameters` | Independent episodes, group/global shrinkage and PIT artifacts, including candidate parameters in the content hash |
| `research.RegisterModel` / `FitModel` / `RestoreModel` | Private trainers per fit and feature-only Predictors; built-in ridge/OLS includes an unpenalized intercept |
| `VisibleTrainingRows` / `RollingWindows` | Decision-visible features, mature labels, purge/embargo and bounded rolling windows |
| `PublishModel` / `LoadModel` | Temporary file, Sync/Rename publication and hash/PIT recovery; no automatic online training service |
| `runner.RegisterModelPortfolioBuilder` | Published-model selection builder with its config hash in StrategyHash; no visible model skips new targets |
| `research.EstimateCovariance` / `OptimizePortfolio` | Diagonal/shrinkage covariance, PSD checks and bounded projected optimization; reports feasibility, convergence and violations separately |
| `runner.RegisterRiskPortfolioBuilder` | Frozen risk builder configuration/hash; fitted matrices require SIDs, TrainingEnd and AvailableAt |
| `SummarizeLifecycles` / `SummarizeCapital` / `EstimateCapacityCost` / `Attribute` | Holding/exit delay, capital, cost and benchmark/stage attribution from caller-supplied verifiable events |
| `FactorRegistry` / `OpenTrialLedger` / `CompareTrials` / `RandomBaselineScores` | Versioned metadata, JSONL trials, separate training/out-of-sample metrics and repeatable random baselines |
| `NewResearchCache` | Byte-budget LRU with copied reads/writes and manifest/algorithm/schema/universe/revision/window identity |

Model builders produce predicted ideal portfolios. Lifecycle retain/dropout still uses the base score by default; supply matching predicted scores through PolicyContext when needed. Risk builders have no history state; turnover limits require a policy or OptimizePortfolio.Previous. Infeasible results must not become targets, and global optimality is not promised. Third-party ML/solver backends require registered extensions; no default dependency is added.

Reports do not infer fill age from target admission. Do not invent BatchID attribution when aggregate lots cannot prove it. Ordinary CLI does not automatically collect complete per-batch fill reports. Trial corruption/content conflicts refuse recovery and preserve the file; multiple processes require external owner serialization. CompareTrials does not select models by sorting out-of-sample performance.

Fuller Go examples are in `doc/factor_research_extensions.md`. Costs, capacity and model validity require independent out-of-sample validation.

## Replay and live

`runner.Run` drives historical input. `runner.NewLive(c Config, sink Sink, clock func() int64, out Output) (*Live, error)` uses a caller-injected clock and reconciled account sink; it creates no market timer. `Observe(ctx, record)` accepts current versioned records and advances processing synchronously. An external source drives `Flush(ctx, decision)` / `FlushAt(ctx, decision, cutoff)` to complete rounds. `Stop` prevents new input; `Join` waits for accepted callbacks/computation before releasing shared Sessions. Shared computation does not merge targets, budgets or research state.

Embedded data assembly uses `runtime.CompileFactorsLivePlan`, `SubscribeFactorsLive`, `InstallFactorsLive` and `BindFactorsLive`. CLI defaults to Hub: omitted `execution.live_provider`, `banexg` or `hub` selects the local private socket. `runner.NewHubAccountSink` uses the Hub account application interface; robots do not own accounts.

Custom production embedding uses `entry.RegisterFactorLiveBinding` to provide transport, metadata, revision/funding and account evidence, and cannot mix with Hub in one robot. Live `env: dry_run` permits only Hub paper; `trade --dry-run` is historical replay. Live rejects all history combinations, source-event decisions and replay-only option/terminal settlement features. There is no completed real-venue production acceptance guarantee. See [Shared accounts](../guide/execution.md) for operation and recovery.

## Data and output

VersionRecord retains event/visibility/reception times, revisions and typed Values. Snapshot getter copies are ownership boundaries; arbitrary columns and NULL must remain. Session and Batch differ in recursive-history semantics.

Manifest records code, plan, Universe, visibility, input references, labels and execution assumptions. It is not a raw-record clone. Result.Unresolved retains immature labels. Account event/posting Gob and legacy time-series orders.gob have different audit/report contracts.

## Result fields and report boundaries

`runner.Result` uses Go field names in `run.json` `Results[]` and `run-completed`. `Pairs` is collected at the end of `RegisterPairs` replay only. `PairResult` contains Y/X, Episodes, EpisodeCount, PnL, Trades, Wins, WinRate, InMarketBars, Bars, PausedBars, AvgHoldBars, LastBeta, Turnover, Unavailable and UnavailableLeg. Episodes share a 400-record budget with at least 10 per pair, retaining earliest records; they may omit later history. `PairEpisode` contains StartMS, EndMS, Dir, EntryZ, ExitZ, Beta, Bars, Reason and PnL; Reason is exit/stop/maxhold/leg_missing/end.

Pair PnL uses each pair's decision weights and prices as a fraction of initial NAV, without allocated costs. `PairsGrossPnL` sums it. `PairsResidual = PairsNAVReturn - PairsGrossPnL` also includes delayed prices, weight drift and netting shared symbols. `AllPairsUnavailable` identifies a run with no tradable pairs; its flat zero-trade NAV is not valid strategy evidence.

`Attribution` is generated only by the weights book, not events/live. Price changes produce LongPnL/ShortPnL; costs include Fees, Slippage, Funding and PITCost, with `FinalNAV - InitialNAV = LongPnL + ShortPnL - Fees - Slippage - Funding - PITCost + Residual`. Up to 20 TopSymbols are sorted by absolute Net. Net excludes account-level funding, so all symbol Net values sum to NAV change plus Funding. Pairs/Attribution can be absent when a run returns early with an error.

Setting `panel: none` under the policy's `research` only suppresses panel output, preserving events, in-memory summaries and book values; the default is all. See [Results](../guide/factor.md#statistical-arbitrage-and-long-and-short-attribution) for examples and interpretation.
