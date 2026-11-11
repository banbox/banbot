# banbot Statistical Arbitrage: Pairs Strategy, Numerical API, and Research Run Contract

Prerequisite: you have already read the time-series strategy doc (DataSeries/Kline, public types, common config) and the cross-sectional factor doc (runner, common YAML fields, Frame, portfolio, Full/Patch, `Params`, `RegisterDefinition`/`RegisterPortfolioFactory`, common live-trading constraints, price streams and expiry defaults). This document covers only statistical-arbitrage-specific content.

## 0. Overview and Terminology

The single pipeline: **visible data → training (offline, frozen) → current-period signal → target portfolio → fill and account evidence**.

| Stage | Output |
| --- | --- |
| Training (caller Go code, reads training interval only) | β/α, residual scale, thresholds, holding period, candidate parameters |
| Freeze (write YAML `params`, enters manifest hash; or ParameterArtifact/ModelArtifact) | Immutable parameters |
| Signal (runner computes online each decision round) | Per-pair z-score, stateful position ±1/0 |
| Target (`RegisterPairs` builder outputs an immutable `factor.TargetPortfolio`: `ID()` is a content hash, `Targets()` returns a SID→NAV-fraction copy) | Target weights, diagnostic codes |
| Evidence (Weights/Events ledger and its output files) | Fills, fees, funding |

Terms: `Y` is the explained leg (bought when going long the spread); `X` is the explanatory leg (sold when going long the spread); `z` = spread residual / residual standard deviation; `β` is the log-regression slope (elasticity); decision bar = the closed K-line corresponding to `run_timeframes`. Statistical functions do not read the clock, account, or exchange; data is passed in via `DataSeries.Values`, preserving type and NULL; missing values must be explicitly excluded and must not be treated as 0.

## 1. Look-ahead Prevention and Visibility Rules

1. Selecting pairs, selecting direction, and fitting β/α/residual scale/thresholds/holding period/parameter candidates: use only observations with `EndAt <= TrainEnd` and `AvailableAt <= freeze time`. `statistics` functions do no time filtering; the caller must trim.
2. **Pair-pool selection is part of training**: backtesting "cointegrated pairs" picked from the full sample is selection bias.
3. `research.SelectParameters` keeps only observations with `BeginAt >= TrainStart`, `EndAt <= TrainEnd`, `AvailableAt <= spec.AvailableAt`, and validates only the kept rows for `SID>0`, non-empty `Candidate`, `EndAt > BeginAt`, `AvailableAt >= EndAt`, finite `NetReturn`. `research.ResolveParameters(artifacts, asof, manifestID)` accepts only artifacts whose `ManifestID` matches, `AvailableAt <= asof`, `TrainEnd < asof`, and whose ID is a content hash; it picks the one with the latest AvailableAt (ties go to the larger ID); no visible artifact or a corrupt one yields an error.
4. Online `RollingSpread` uses only the current and past closed bars; the window fit includes the current bar (closed at decision time) and excludes future bars.
5. Execution uses only prices after the decision, after `LatencyMS`, and strictly later than the decision time; Events needs a tick or 1m stream (K-line OHLC cannot reconstruct the intrabar path); `DecisionDelayMS` postpones the visibility cutoff.
6. `KalmanPair.Observe`'s `Error`/`Variance` are the **pre-update** predictive innovation and can be used directly as the current-period standardized residual; `FitKalmanNoise`'s Q/R may only be fit on training samples and frozen.
7. A static SQL latest-value table may only declare `data.pit_policy: static-approximation`; strict PIT requires an immutable archive or a versioned-page provider with evidence.
8. Test-window results must not be used for parameter selection; `asof` only guarantees visibility and does not prevent manual peeking followed by parameter changes; such changes count toward the number of trials.
9. Cointegration test statistics must be compared with critical values matching the deterministic terms (3.1), not ordinary ADF/normal tables.
10. Numerically valid (finite, identifiable, converged) ≠ significant or profitable.

## 2. Pairs Strategy: `runner.RegisterPairs`

### 2.1 Signature and Types

```go
func RegisterPairs(name string, build func(*Params) (PairsSpec, error)) error
type Pair struct{ Y, X string }          // exactly the symbol in the run SIDMap, e.g. "ETH/USDT:USDT"
type PairsSpec struct {
    Pairs  []Pair
    Field  string                        // kline field, default "close"
    Window int                           // regression window (decision bars), >= 3
    Band   statistics.BandConfig         // see 2.3
    Gross  float64                       // total notional per pair as fraction of strategy NAV; 0 = 1/len(Pairs)
}
```

`Params` (`Float/Int/Bool/Err/Unused`) is described in the cross-sectional factor doc; related behavior: `Int` errors on fractional values rather than truncating; `Bool` accepts only 0/1; `Unused()` ignores `window` and `k`, and **is not executed automatically**: you must check it in build and return an error, otherwise misspelled parameter names silently fall back to defaults. **There is no string-reading method**, so `Pairs` and `Field` cannot be parameterized via YAML; different symbol combinations must register different names.

`build` is called multiple times in the same run (definition compilation, pre-check, factory creation); it must be a pure function with no IO/global side effects.

Registration occupies the definition name `name` and the factory name `name-pairs-v1` (neither may already be registered; the definition name is registered first, so if the factory fails the definition name is already taken). The builder is bound automatically; YAML needs only `name`; an explicit `portfolio.builder` takes precedence.

### 2.2 Validation (after build returns)

Empty `build`/`name` → registration fails; non-empty `Params.Err()` → that error is returned; `Field==""` → `"close"`; `len(Pairs)==0`, `Window<3`, `Band.Validate()` failure → error; `Gross==0` → `1/len(Pairs)`; `Gross<=0` or `Gross*len(Pairs) > 1+1e-9` → error (total exposure must not exceed NAV); `Y==""`/`X==""`/`Y==X` → error; Y/X not in the run `SIDMap` → error when constructing the portfolio builder (`PreparePortfolioConfig`, before resource assembly).

Tradability is not checked at construction time (see 2.4).

### 2.3 Stateful Signal Machine: `statistics.BandConfig` / `BandTrader`

```go
type BandConfig struct {
    Entry, Exit, Stop float64
    MaxHold           int   // decision rounds, 0 disables
    LongOnly          bool  // forbid shorting the spread (z >= Entry)
    ShortOnly         bool  // forbid going long the spread (z <= -Entry)
}
func (c BandConfig) Validate() error
func NewBandTrader(cfg BandConfig) (*BandTrader, error)   // one instance per spread
func (b *BandTrader) Step(z float64) int                  // -1/0/+1
func (b *BandTrader) Position() int
```

Validation: thresholds finite; `Entry>0`; `0<=Exit<Entry`; `Stop==0 or Stop>Entry`; `MaxHold>=0`; LongOnly and ShortOnly mutually exclusive. **No built-in threshold defaults** (the 2 in `p.Float("entry", 2)` is the caller's choice).

State transitions:
1. Each `Step` first: if `blocked` and `|z|<=Exit` → clear it.
2. Non-finite z: return the current position, state unchanged.
3. Holding: `held++`, then by priority: `Stop>0 and |z|>=Stop` → close and set `blocked`; `|z|<=Exit` → close (does not set blocked); `MaxHold>0 and held>=MaxHold` → close and set `blocked`.
4. Flat: `blocked` or `|z|>=Stop` (Stop>0) → stay 0; otherwise `z<=-Entry` → +1 (long spread), `z>=Entry` → -1 (short spread), subject to LongOnly/ShortOnly; on entry `held=0`; a position never flips direction in a single step.

`blocked`: after a stop-loss/expiry close, re-entry is allowed only once z returns to `[-Exit,Exit]`.

### 2.4 Runtime Semantics (each decision round)

Each pair holds a private `RollingSpread`, `BandTrader`, frozen β, and position. Each round:

| Step | Condition | Behavior | Diagnostic code |
| --- | --- | --- | --- |
| a | Y/X `pairs-log-v1` value missing or not Valid (including price<=0) | Skip the pair; do not advance window or state machine; existing position is kept | `pair-input-missing` |
| b | `RollingSpread.Observe` not ready (window not full, x variance<=1e-12, residual std<=1e-12, non-finite) | Skip; do not advance BandTrader | `pair-warmup` |
| c | Ready, a leg is not tradable, current position 0 | Do not open; do not advance BandTrader | none |
| d | Ready, a leg is not tradable, current position non-zero | Still advance BandTrader (may close per rules) | none |
| e | After `Step(z)` position≠0 and previous position was 0 or direction changed | Freeze β = this period's `SpreadPoint.Beta` | none |
| f | No pair is ready (all in a or b) | Return nil target + `pairs-not-ready` | `pairs-not-ready` |
| g | Otherwise | Compute weight output for every pair with position≠0 (including those with missing input this round, using their state); non-empty diagnostics attach to the target | this round's diagnostics |

Weights (`pos∈{-1,0,+1}`, `G` = that pair's Gross, `β` the frozen value, `N=1+|β|`): `w_Y += pos·G/N`, `w_X += -pos·G·β/N`. When the same SID appears in several pairs the weights **accumulate**, nets can offset, and zero weights are deleted. For a single pair |w_Y|+|w_X| is exactly G, and total exposure ≤ ΣG ≤ NAV. `pos=+1` buys Y and sells X; X notional = β × Y notional (elasticity hedge under the log model).

- **When β is negative both legs go the same direction**: the code does not reject it; filter it at the training/parameter layer.
- **Signal/hedge inconsistency**: z uses the rolling α/β each round, while held-position weights use the β **frozen at entry**; β drift during the holding period changes z but not the hedge ratio.
- **Window** = the most recent N **usable observations**, not calendar bars; a bar with an invalid leg does not enter the window, so the covered time stretches.
- **Full/Patch**: the target is a NAV fraction; under Full, SIDs not in the target are treated as 0. Closed pairs are removed from the target, so **`portfolio.mode: patch` must not be set** (Patch only changes SIDs inside the target; unlisted positions are never closed); default is full.
- **nil target vs empty target**: nil (f) = no new target this round; the engine does not replace the pending target, the Weights ledger count is unchanged (prior holdings carry on), `Result.Skipped++`, and evaluation weights are not updated; an already-submitted unfilled target still executes within its window or expires. A non-nil empty map (all pairs closed) = under Full, everything is zeroed, and it executes without needing a quote. The warm-up period (window not full) is nil; once ready, having no positions yields an empty target.
- If a leg's entire kline is missing: Weights backtests handle it with the per-pair PIT (next bullet: the leg is dropped, its pair closed and paused); on paths without PIT (Events/Trade) there is no decision callback that round (`pair-input-missing` corresponds to an invalid value / price<=0).
- PIT in Weights backtests is decided per pair: when a leg has no data at the decision grid (delisting, not yet listed, gap) that leg is dropped from the universe, the pair holding it is closed and paused (diagnostic `pair-closed-missing-leg`), and the other pairs keep trading; once both legs are back the pair can reopen. It applies to the static pool with identical five pools as well; periods in which every pair is paused count in `Result.PausedPeriods`, not in `Incomplete`. `pit_delist_penalty_bps` is the delist penalty of the closed leg (0 = default 100, < 0 off).

### 2.5 YAML

Write only `name` and parameters; root config (`data.pit_policy`, `execution.funding_policy`, `execution.mode`, `prices`, etc.) follows the common doc.

```yaml
pairs: ['ETH/USDT:USDT', 'BTC/USDT:USDT']
run_policy:
  - name: eth-btc
    id: pair1
    engine: factor
    run_timeframes: [1h]
    params: {window: 120, entry: 2, exit: 0.5, stop: 4, maxHold: 0, gross: 0}
```

- `params` keys are the keys build reads; unknown keys error only if build calls `Unused()`; changing any parameter creates a new strategy identity (manifest hash).
- The loader writes `params.window` into `Factor.Window` and `params.k` into `portfolio.K` (not used by pairs); the pairs window is determined by `PairsSpec.Window`, which should match `window`. `swapPerBars`/`holdBars` are rejected by the loader in factor strategies (`maxHold` is allowed).
- For pairs, the `research` command outputs only the `logp` panel and label diagnostics; performance comes from `backtest --mode weights|events`. A Weights backtest's `Result.Pairs` gives each pair's own result (Episodes: entry/exit time, Dir, EntryZ/ExitZ, frozen Beta, Bars, Reason=exit|stop|maxhold|leg_missing|end, PnL; totals PnL, Trades, WinRate, InMarketBars, PausedBars, AvgHoldBars, LastBeta, Turnover; Episodes are capped at 400 in total, shared equally by the pairs (at least 10 per pair); see EpisodeCount for the real count). Per-pair PnL is an approximate attribution using each pair's own target weights and decision-time prices (before costs; Turnover is the cost proxy); reconcile with `PairsGrossPnL`, `PairsNAVReturn`, `PairsResidual` (residual ≈ costs + execution approximation). `Result.Attribution` adds long/short leg PnL, exposure, costs and `TopSymbols` (single-instrument contribution share).

### 2.6 Price Stream, Fill Timing and Costs (Weights)

**Default price stream**: with Weights, no `prices` written and no expression = decision-timeframe K-line close (Events store replay defaults to `kline 1m close`; when the archive has a tick stream the default is `{tick,event,price}`).

**Observable timing**: a target executes only at a quote with `AtMS > DecisionTime`, `AtMS >= DecisionTime+latency_ms`, `AvailableAt <= now`, and within `expiry_ms` (default 60000). Hence **1h decision + default 1h close price stream + default expiry**: the next quote is 1h later, every non-empty target expires, and there are zero fills (measured: no positions in the ledger, fees 0). Fix: configure `prices: {source: tick, timeframe: event, field: price}` or `kline/1m/close` (measured: the tick stream fills 1ms after the decision). Merely raising `expiry_ms` so it fills at the next K-line is untested, and the fill price would be the next close.

**The Weights ledger charges for drift**: each time the ledger executes a target it recomputes quantity as `qty = w × spec.Budget.NAV / price` (NAV = ledger NAV at decision time, compounding with PnL), and charges `fee_rate`+`slippage_rate` on the difference from existing quantity. Pairs output a target every decision round, so **price drift during the holding period is rebalanced back to target weights every round and charged** (measured: fees still accumulate round by round when weights are constant). Gross is a fraction of current NAV, not initial NAV. `research.Evaluate`'s `Costs = CostRate × Σ|w−w_prev|` counts only weight differences and excludes drift, a different basis from the ledger (`Result.Book` is the ledger basis). Whether the Events path also generates drift-rebalance fills is untested.

### 2.7 Custom Paths (static β, Kalman, multi-leg baskets)

`RegisterPairs` takes β from rolling OLS and **cannot inject a static β or Kalman state**; use the general interfaces:
1. `runner.RegisterDefinition(name, func(c runner.Config) (*factor.Plan, research.ComboSpec, error))`: `factor.New().Add(name, node).Compile()` compiles log-price nodes (`factor.Field("kline", field, c.Factor.TimeFrame)` + `factor.Custom(version, inputs, fn)`, same form as `pairs-log-v1`), combined as `research.ComboSpec{Method: research.Equal, Columns: [...]}`.
2. `runner.RegisterPortfolioFactory(name, func(c runner.Config) (runner.PortfolioBuilder, error), validator)`: the name must contain `-v`; the validator does pure parameter checks only and does not construct state.
3. Builder: `func(frame factor.Frame, universe factor.Universe, spec factor.PortfolioSpec, def research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error)`; output `factor.NewTargetPortfolio(spec, weights)`, using the incoming `spec`.
4. YAML: `portfolio: {builder: <name>-v1}` (explicit value takes precedence).

A stateful builder may be called only once per decision round (repeats would advance the filter state again), and state must be private to each run.

## 3. Numerical API (`factor/statistics`)

Conventions: observations in ascending time order; matrix rows = observations, columns = variables, with column order identical for fitting and prediction. Non-finite input, non-identifiable models, dimension mismatch, and exceeding budget all return an error.

### 3.1 Regression, Spread Models and Stationarity

```go
func MeanStd(values []float64, ddof int) (mean, std float64, err error)   // errors if len<=ddof; constant sample gives std=0
type Regression struct { Intercept float64; Beta []float64; Residuals []float64; Rank int }
func Regress(y []float64, exposures [][]float64, intercept bool) (Regression, error)  // errors on rank deficiency / too few samples

type OU struct {
    Intercept, Slope, Equilibrium, InnovationStd float64
    LinearHalfLife, DiscreteHalfLife             float64  // linear -ln2/b; discrete -ln2/ln(1+b)
}
func FitOU(values []float64) (OU, error)  // Δx = a + b·x_{t-1}; len>=4; requires -1<b<0 else error; Equilibrium=-a/b; InnovationStd uses ddof=2

type ADFSpec struct { Lags int; Intercept, Trend bool }
type ADFResult struct { DeltaCoefficient, StandardError, TStatistic float64; Samples, DegreesOfFreedom int }
func AugmentedDickeyFuller(values []float64, spec ADFSpec) (ADFResult, error)   // len >= Lags+4
func EngleGranger(y []float64, x [][]float64, intercept bool, lags int) (Regression, ADFResult, error)

type JohansenSpec struct { DifferenceLags int; Intercept, Trend bool }  // Trend requires Intercept; lags 0..128
type JohansenResult struct {
    Eigenvalues, Trace, MaximumEigenvalue []float64   // eigenvalues descending; Trace[r] tests H0: rank<=r
    CointegratingVectors, AdjustmentVectors [][]float64 // [component][asset]; beta'S_kk beta=I; the largest first loading is positive
    Samples, NuisanceRank int
}
func Johansen(levels [][]float64, spec JohansenSpec) (JohansenResult, error)   // 2<=assets<=64
func (j JohansenResult) TraceRank(criticalValues []float64) (int, error)       // length must equal len(Trace) (= number of assets), values>0; sequential rejection, stops at the first non-rejection
```

- **Critical values (the code is authoritative)**: `statistics` has **no critical-value table, interpolation function, or p-value**; `AugmentedDickeyFuller`/`EngleGranger` return only `TStatistic`, and `TraceRank` takes caller-supplied critical values with no default table.
- **EngleGranger**: after `Regress(y,x,intercept)`, runs `ADFSpec{Lags: lags}` on the residual (**residual test has no intercept, no trend**). The more negative the statistic, the more it favors rejecting "no cointegration"; it must be compared with Engle–Granger/MacKinnon critical values matching the deterministic terms/number of regressors. External reference (**not provided by the code**): MacKinnon asymptotic values, N=2 with constant approx. 1%:-3.90, 5%:-3.34, 10%:-3.04; with trend, more regressors, or finite samples they differ, so use response surfaces or simulate your own.
- **FitOU**: when counting in bars use `DiscreteHalfLife`.
- **Johansen** eigenvectors must be normalized by one leg before deriving leg weights; `TraceRank` critical values must correspond to the same lags, deterministic terms, and significance level.

### 3.2 Online Rolling Spread (used by `RegisterPairs`)

```go
type SpreadPoint struct {
    Alpha, Beta float64   // current-window OLS: y = alpha + beta*x
    Residual    float64   // current observation y - alpha - beta*x
    Std         float64   // window residual standard deviation, ddof=2
    Z           float64   // Residual / Std
}
func NewRollingSpread(window int) (*RollingSpread, error)  // window >= 3
func (r *RollingSpread) Window() int
func (r *RollingSpread) Observe(x, y float64) (SpreadPoint, bool)   // x is the X leg, y the Y leg (log prices)
```

`ok=false`: window not full, non-finite input (sample skipped), x window variance<=1e-12, residual std<=1e-12. Each call runs a full OLS on the current window (not frozen); under log prices β is an elasticity.

### 3.3 Kalman Filter (random-walk β and intercept)

```go
type KalmanPair struct {
    Beta                                 [2]float64     // [slope, intercept]
    Covariance                           [2][2]float64  // must be PSD
    ProcessVariance, ObservationVariance float64        // ProcessVariance>=0 acts as a diagonal Q on both slope and intercept; ObservationVariance>0
    ProcessCovariance                    [2][2]float64  // used when non-zero; mutually exclusive with non-zero ProcessVariance
}
type KalmanObservation struct {
    Prediction, Error, Variance float64  // pre-update prediction, innovation, innovation variance S
    PriorBeta, PosteriorBeta    [2]float64
}
func (k *KalmanPair) Observe(x, y float64) (KalmanObservation, error)       // mutates state; call in time order
func KalmanLikelihood(x, y []float64, initial KalmanPair) (float64, error)  // negative log-likelihood
type KalmanFitSpec struct {
    Initial                       KalmanPair
    MaxEvaluations, MaxIterations int             // default 4000 / 2000
    Runtime                       time.Duration   // default 30s
    DiagonalProcessNoise          bool
}
type KalmanNoiseFit struct {
    Initial                                              KalmanPair  // fitted Q (written to ProcessCovariance, ProcessVariance=0) and R
    InitialNegativeLogLikelihood, NegativeLogLikelihood  float64
    Evaluations                                          int
    Status                                               string
    Converged                                            bool
}
func FitKalmanNoise(ctx context.Context, x, y []float64, spec KalmanFitSpec) (KalmanNoiseFit, error)
```

- `Observe`: random-walk state, observation `y = β·x + intercept`; the standardized residual `Error/sqrt(Variance)` can replace the rolling z fed to `BandTrader`.
- `FitKalmanNoise`: `len>=8`, Nelder–Mead maximizes the likelihood, initial state fixed; initial Q must be positive definite (diagonal when `DiagonalProcessNoise`), `ObservationVariance>0`. Budget exhaustion or cancellation returns an error; `Converged` requires no early termination and a final negative log-likelihood no higher than the initial one (tolerance 1e-8).

### 3.4 Local Level, EMA and AR

```go
type LocalLevel struct{ Mean, Variance, ProcessVariance, ObservationVariance float64 }  // ObservationVariance>0
type LevelObservation struct{ Prediction, PredictionVariance, Innovation, Gain float64 }
func (k *LocalLevel) Observe(value float64) (LevelObservation, error)
type LocalLevelFitSpec struct { Initial LocalLevel; MaxEvaluations, MaxIterations int; Runtime time.Duration }  // default 3000 / 2000 / 15s
type LocalLevelFit struct { Initial LocalLevel; NegativeLogLikelihood, InitialNegativeLogLikelihood float64; Evaluations int }
func FitLocalLevel(y []float64, s LocalLevelFitSpec) (LocalLevelFit, error)   // >=8 observations, initial Q>0
func LocalLevelEffectiveEMA(q, r float64) (gain, period float64, err error)   // steady-state gain 2/(1+sqrt(1+4r/q)), period=2/gain-1
func LocalLevelForEMA(mean, period, r float64) (LocalLevel, error)            // period>1; sets Q/R=4/(N²-1), uses the exact EMA gain each step
type ARModel struct{ Intercept float64; Coefficients []float64 }             // lag1 first
func FitAR(values []float64, lags int) (ARModel, error)                     // conditional OLS (not ARMA MLE); len>=2*lags+2
func (m ARModel) Forecast(history []float64) (float64, error)
```

The steady-state EMA gain is only an asymptotic result.

### 3.5 Vidyamurthy Filter, Lag Selection and Fill Verification

```go
type SpreadObservation struct{ Prediction, PredictionVariance, Filtered, Variance, SlopeWeight float64 }
func NewLaggedSpreadFilter(lag int, processVariance, initialVariance, initialValue float64) (*LaggedSpreadFilter, error)       // lag 1..64; processVariance>=0; initialVariance>0
func NewSourceLaggedSpreadFilter(lag int, processVariance, initialVariance, initialValue float64) (*LaggedSpreadFilter, error) // lag>=3 uses the source book's uncorrelated approximation
func (k *LaggedSpreadFilter) Observe(value, observationVariance float64) (SpreadObservation, error)                           // observationVariance>0
func SelectSpreadLag(values, observationVariances []float64, candidates []int, processVariance, initialVariance float64) (lag int, score float64, filter *LaggedSpreadFilter, err error)
func SelectSourceSpreadLag(values, observationVariances []float64, candidates []int, processVariance, initialVariance float64) (int, float64, *LaggedSpreadFilter, error)  // uses the Source constructor, otherwise the same as above

type PairedFill struct{ Price, Quantity float64 }                  // both must be >0
type PairedExecution struct {
    Pairs []FillPair                                               // FillPair{BidderIndex, TargetIndex int; TargetQuantity float64}
    MatchedTarget, BidderEquivalent, TargetTotal, AverageSpread float64
    AverageSatisfies, FullyPaired bool
}
func PairExecutions(bidder, target []PairedFill, ratio, cash, limit float64, putOn bool) (PairedExecution, error)
func CompareCommonFactors(a, b []float64, covariance [][]float64) (CommonFactorPair, error)
func InterpolateMonotoneTail(counts []float64) ([]float64, error)   // non-increasing, non-negative; linear interpolation between change points to remove plateaus
```

- `SelectSpreadLag`: `len(values)>=2`; starts from `values[0]` as the initial value and accumulates SSE (measurement error² + prediction error²) from the 2nd observation; candidate order decides ties; the returned filter can keep being used online.
- `PairExecutions` is a **post-fill verifier** (it places no orders and does not guarantee simultaneous fills): `ratio>0`; bidder price×ratio and quantity÷ratio are converted to target units; put-on requires `ratio*bidder+cash-target >= limit`, unwind requires `<= limit`; `AverageSpread` is the weighted-average spread; pairing saturates by "ask ascending, bid descending". Each side ≤10000 fills, product ≤1e6.
- `CompareCommonFactors` (Vidyamurthy ch6): `a`, `b` are the two assets' exposure vectors to n common factors, `covariance` is the **n×n factor covariance matrix** (n=len(a)=len(b), must be symmetric positive semi-definite; **not the asset covariance, and excludes specific variance**). Returns `CommonFactorPair{Correlation, AbsoluteCorrelation, GammaAB, GammaBA, ResidualVarianceAB, ResidualVarianceBA}`: aa=a'Σa, ab=a'Σb, bb=b'Σb; `Correlation=ab/√(aa·bb)`; `GammaAB=ab/bb` (B hedges A), `GammaBA=ab/aa`; `ResidualVarianceAB=aa−ab²/bb`, `ResidualVarianceBA=bb−ab²/aa` (floored at 0); aa or bb<=0 errors.

### 3.6 Thresholds, Holding Periods and Regularization

```go
func GaussianThreshold(sigma float64) (float64, error)  // maximizes δ(1-Φ(δ/σ)), ≈0.75179σ; sigma>0
func SmoothThresholdCounts(counts []float64, lambda float64) ([]float64, error)  // min ||c-z||² + λΣ(Δz)²
type ThresholdFit struct { Threshold, Objective float64; Counts, Smoothed []float64 }
func EmpiricalThreshold(spreads, grid []float64, lambda float64) (ThresholdFit, error)   // spreads must already be centered
type RegularizationChoice struct { Lambda float64; FitErrors, Smoothed []float64 }
func RegularizationHeel(counts, lambdas []float64) (RegularizationChoice, error)         // len(counts)>=2, len(lambdas)>=3 and strictly increasing positive
func CrossingDurations(spreads []float64) ([]int, error)            // intervals between full zero crossings (in observations)
func DurationQuantile(durations []int, q float64) (float64, error)  // q∈[0,1], linear interpolation, intervals must be >0
type DurationPopulation struct { Lower, Median, Upper float64; Samples int }  // Samples=draws
func BootstrapDurations(intervals []int, draws int, seed uint64, lower, upper float64) (DurationPopulation, error)
```

- `GaussianThreshold` excludes costs and funding; σ=1 gives about 0.7518, well below the commonly used 2; the actual Entry must be recomputed with costs (7.1).
- `EmpiricalThreshold`: `Counts[i]` is the average of two-sided over-threshold counts, smoothed by `SmoothThresholdCounts`, then the `threshold·z` maximizer is chosen (with no positive value, `Threshold=grid[0]`); `grid` must be strictly increasing and non-negative. `RegularizationHeel` picks the interior λ point with the largest normalized curvature and **returns `lambdas[0]` when there is no positive curvature**, which the caller should check.
- `CrossingDurations`: a crossing is confirmed only when a non-zero value of opposite sign appears (zero values are skipped); head and tail censored intervals are not counted.
- `BootstrapDurations`: `len(intervals)>=2`, values>0, resamples to estimate population quantiles; `lower∈[0,0.5)`, `upper∈(0.5,1]`, `draws∈[2,1e6]`; assumes i.i.d., intervals are too narrow under serial correlation.

### 3.7 Portfolios, Factors and Rotation

```go
type NeutralAllocation struct { Weights []float64; ConstraintRank int; Gross float64; Scaled, NoResidualAlpha bool }
func FactorNeutralWeights(alpha, variance []float64, exposures [][]float64, risk, grossLimit float64) (NeutralAllocation, error)
```
Maximizes `α'w − (risk/2)·w'Dw` subject to `exposures'w=0`; `variance` = idiosyncratic variance (>0); `risk>0`; an all-ones column means dollar neutral; when `grossLimit>0` it scales proportionally (stays neutral, not an exact L1).

```go
type PCAModel struct { Mean, Scale, Eigenvalues []float64; Loadings [][]float64 }  // Loadings[component][feature], eigenvalues descending
func FitPCA(rows [][]float64, components int, standardize bool) (PCAModel, error)   // rows>=2, 1<=components<=number of features
func (m PCAModel) Transform(values []float64) ([]float64, error)
func (m PCAModel) Residual(values []float64) ([]float64, error)
type FactorAnalysisConfig struct { Components, MaxIterations int; Tolerance, MinUniqueness float64; ZeroMean bool }  // MaxIterations∈[1,1e4], Tolerance>0, MinUniqueness>0 have no defaults and must be given
type FactorAnalysisModel struct { Mean []float64; Loadings [][]float64; Uniqueness []float64; NegativeLogLikelihood float64; Iterations int; FloorActive bool }  // Loadings rows = assets
func FitFactorAnalysis(ctx context.Context, rows [][]float64, config FactorAnalysisConfig) (FactorAnalysisModel, error)  // 1<=Components<assets<=256, rows>=Components+2
func (m FactorAnalysisModel) FactorScores(row []float64, posterior bool) (FactorScoreResult, error)   // Scores, Residuals
func VarimaxLoadings(rows [][]float64, kaiser bool, maxIterations int, tolerance float64) ([][]float64, error)   // rows is the loading matrix (assets×factors), assets<=256, factors<=assets; rotation preserves LL'
func WinnerMinusLoserReturn(previous, current, lookback []float64) (float64, error)
func HierarchicalRiskParity(cov [][]float64) (HRPResult, error)   // HRPResult{Weights []float64; Order []int}; marginal variance>0, symmetric PSD
func LongOnlyMinimumVariance(cov [][]float64, maxIterations int, tolerance float64) (MinimumVarianceResult, error)
func ChanLongOnlyFrontier(mean []float64, cov [][]float64) (ChanFrontierResult, error)   // 2..12 assets; 21 target points; cov must be positive definite; result contains Tangency and MinimumVariance indices
func ChanInverseRiskWeights(risk []float64) ([]float64, error)
func ChanSignedTangency(mean []float64, cov [][]float64) ([]float64, error)
```
The above output numbers only and **do not generate a trading schedule**; selection rules and rebalancing cadence are the caller's. For multi-asset portfolios, HRP/minimum variance can serve as a weight baseline, while the hedge-leg β structure still comes from the Section 4 workflow.

### 3.8 Modules Outside the Main Line

AFML labels/CPCV, order flow and PIN/VPIN, VID risk pricing, HMM (`FitBinaryHMM`), GARCH/ARMA/ARIMA/VAR (`FitVAR`, `SelectVAR`, `FitARMA`, `FitARIMA`, `FitGARCH`), SVM/trees/neural networks, etc.; usually not needed; check the function comments before use.

## 4. Training → Freeze → Signal → Target → Fill Evidence

### 4.1 Training Workflow (offline, training interval only)

1. Training log prices: decision-timeframe `log(close)`, trimmed by `EndAt <= TrainEnd` and `AvailableAt`, legs aligned, rows with any missing value dropped entirely, no forward fill.
2. For each candidate pair run `EngleGranger` in both Y/X directions (explicit intercept), compare residual ADF statistics and β stability, and choose the direction from training results.
3. `FitOU(residual)` and take `DiscreteHalfLife`. An empirical starting Window is about 3–5 times the discrete half-life and not less than 30 bars; this is a **pure rule of thumb with no source-book/code/test basis**, so do a sensitivity check.
4. Standardize the residual to z (training mean/std), choose Entry with `EmpiricalThreshold`+`RegularizationHeel`; Exit/Stop are chosen by the caller (e.g. 1/2 and 2 times Entry), satisfying `Stop > Entry > Exit >= 0`.
5. Quantiles from `CrossingDurations`+`BootstrapDurations` are used to estimate the order of magnitude of `MaxHold`, not as a hard deadline.
6. Cost gate: per 7.1, confirm `(|Entry|-|Exit|)·σ/(1+|β|)` covers round-trip cost, otherwise raise Entry or abandon.
7. Multi-leg baskets: `Johansen` (needs matching critical values); `RegisterPairs` has only two legs, multi-leg goes through 2.7.

### 4.2 Frozen Artifacts

- **Pairs strategy**: write results directly into YAML `params` (`window/entry/exit/stop/maxHold/gross`); training records (interval, data version, statistics, λ, candidates) are saved by the caller.
- **Statistical selection of parameter candidates**: `research.SelectParameters(observations []ParameterObservation, spec ParameterSelectionSpec) (ParameterArtifact, error)`.
  - `ParameterObservation{SID, Group, Candidate, BeginAt, EndAt, AvailableAt, NetReturn}` (net return, costs already deducted).
  - `ParameterSelectionSpec{TrainStart, TrainEnd, AvailableAt, MinIndependentSamples(>=2), PriorSamples(>=0), ConfidencePenalty(>=0), HACLag(>=0), ManifestID, AlgorithmVersion(both non-empty), Candidates map[string]map[string]float64}`; requires `TrainStart>0`, `TrainEnd>=TrainStart`, `AvailableAt>=TrainEnd`.
  - `Score = Mean − ConfidencePenalty·SE` (HAC standard error); the asset level shrinks toward the group and global levels by `n/(n+PriorSamples)`; non-overlapping episodes count as independent samples.
  - **No runtime consumer in the library**: the results of `SelectParameters`/`ResolveParameters` are not injected into `RegisterPairs` parameters automatically; the caller must map them back to YAML/Go code.
- **Nonlinear/regression models**: `research.FitModel(name, manifest string, features []string, params json.RawMessage, rows []TrainingSample, window TrainingWindow, availableAt int64) (ModelArtifact, Predictor, error)` (`availableAt >= window.FitAsOf`), `PublishModel(dir, artifact)` writes an immutable content-addressed file, `LoadModel(path, asof)` restores it; `RegisterModel(name, factory)` must be called first. `TrainingWindow{Start, End, FitAsOf, TestStart, TestEnd, EmbargoMS, MinSamples}`; `VisibleTrainingRows` keeps only samples with `DecisionTime∈[Start,End]`, `MatureAt/AvailableAt <= FitAsOf`, `LabelEnd < TestStart-EmbargoMS` and validates ordering.

### 4.3 Acceptance Order

1. `go test ./factor/statistics ./factor/research ./factor/runner ./execution` and `go vet`: prove only the software contract.
2. `banbot backtest --mode weights --config runtime.yml`: confirm signals, targets, diagnostic codes, holding days, β sign, and the notional ratio of the two legs; **confirm the price stream lets non-empty targets actually fill** (2.6).
3. `banbot backtest --mode events --config runtime.yml`: add an explicit price stream (tick or 1m), fill model, and contract units.
4. `banbot trade --dry-run --config runtime.yml`: check visibility, diagnostics, and pre-order checks, with no real fills.
5. Live trading (Section 6) requires venue-level evidence.

## 5. Execution: Cross-Venue Opportunities and the Paper Ledger

### 5.1 Opportunity Estimation (pure function, `statistics`)

```go
type CrossVenueQuote struct {
    Venue, Asset, Currency                string
    Bid, Ask, BidSize, AskSize, FeeRate   float64
    AvailableCash, AvailableInventory     float64
    ObservedAtMS                          int64
}
type CrossVenueOpportunity struct {
    BuyVenue, SellVenue                               string
    Units, GrossProfit, Fees, TransferCost, NetProfit float64
}
func CrossVenueArbitrage(quotes []CrossVenueQuote, nowMS, maxAgeMS int64,
    units, transferRate, fixedTransferCost, minProfit float64) (CrossVenueOpportunity, bool, error)
```

- 2–1000 quotes; `nowMS>0`, `maxAgeMS>=0`, `units>0`; `transferRate∈[0,1)`, `fixedTransferCost>=0`, `minProfit>=0`; each quote needs non-empty `Venue/Asset/Currency`, `0<ObservedAtMS<=nowMS`, `Bid>0`, `Ask>=Bid`, `FeeRate∈[0,1)`, size/cash/inventory >=0, otherwise an error is returned.
- Only quotes from **different venues, same asset/currency, not stale** (`now-ObservedAtMS<=maxAgeMS`) are paired (differing assets are skipped without error).
- The buy side needs `AskSize>=units` and `AvailableCash >= units·Ask·(1+fee)`; the sell side needs `BidSize>=units` and `AvailableInventory>=units`.
- `gross=units·(sellBid-buyAsk)`; `fees=units·(sellBid·feeS+buyAsk·feeB)`; `transfer=units·buyAsk·transferRate+fixedTransferCost`; returns the direction with the highest net profit that is **strictly greater than** `minProfit`.
- Opportunity calculator only: no atomic settlement guarantee, and it excludes venue credit risk and FX.

### 5.2 Paper Ledger (`execution.CrossVenuePaper`, independent of the single-account coordinator)

```go
type CrossVenueBalance struct{ Venue, Asset, Currency string; Cash, Units decimal.Decimal }
type CrossVenueLevel struct{ Price, Units decimal.Decimal }
type CrossVenueBook struct {
    Venue, Asset, Currency        string
    AtMS, PublishedMS, ReceivedMS int64               // AtMS <= PublishedMS <= ReceivedMS <= now; now-AtMS <= MaxAgeMS
    Bids, Asks                    []CrossVenueLevel    // 1..1024 levels each, prices strictly ordered (bids descending, asks ascending), best bid<=best ask
    FeeRate                       decimal.Decimal      // [0,1)
}
type CrossVenueRequest struct {
    ID                                        string   // canonical ID
    Buy, Sell                                 CrossVenueBook
    Units, MinProfit, EstimatedRebalanceCash  decimal.Decimal   // Units>0; the rest >=0
    NowMS, MaxAgeMS                           int64
}
type CrossVenueEstimate struct{ BuyNotional, SellNotional, BuyFee, SellFee, NetProfit decimal.Decimal }
type CrossVenueTransfer struct {
    ID, From, To                  string
    Units, Cash, UnitFee, CashFee decimal.Decimal
    AtMS, ArrivalMS               int64
    CompletedMS                   int64; Completed bool
}
func OpenCrossVenuePaper(absPath string, initial []CrossVenueBalance) (*CrossVenuePaper, error) // absolute path; 2–1000 venues; SQLite; on reopen the genesis balances must match
func (p *CrossVenuePaper) Close() error
func (p *CrossVenuePaper) Snapshot(ctx context.Context) (CrossVenueSnapshot, error)
func EstimateCrossVenue(r CrossVenueRequest) (CrossVenueEstimate, error)       // read-only, computed along depth; NetProfit already net of EstimatedRebalanceCash; insufficient depth returns ErrCrossVenueDepth
func (p *CrossVenuePaper) Begin(ctx context.Context, r CrossVenueRequest) (CrossVenueTrade, error) // commits only the buy leg, status "bought"
func (p *CrossVenuePaper) Exit(ctx context.Context, id string, book CrossVenueBook, now int64, unwind bool) (CrossVenueTrade, error)
func (p *CrossVenuePaper) StartTransfer(ctx context.Context, r CrossVenueTransfer) error
func (p *CrossVenuePaper) CompleteTransfer(ctx context.Context, id string, now int64) error
func (s CrossVenueSnapshot) Equity(mark decimal.Decimal) (decimal.Decimal, error)
```

- `Begin` checks the pre-positioned cash at the buy venue, the spot inventory at the sell venue, and the net margin after deducting the rebalance subsidy.
- `Exit(unwind=false)` sells at the sell venue using the caller-supplied visible book, status `complete`; `unwind=true` sells the inventory back at the buy venue using a fresh bid book, status `unwound`. `book` must belong to the corresponding venue; repeating a call with identical arguments is idempotent, conflicting arguments error.
- The two legs are non-atomic: if the sell leg fails after the buy leg fills there is no rollback, and the trade stays in the exposed `bought` state, blocking new trades and transfers out until completed or unwound (transfer-in completion is still allowed); the sale may lose money.
- Transfers: a transfer-out immediately deducts cash/inventory and fees from the source account (cash/coin fees are both deducted once at the source); the destination account is credited only via `CompleteTransfer` after `ArrivalMS`; in-transit assets count in `Equity` but cannot be traded, and fees already deducted are not counted in equity; the estimator's rebalance subsidy is not charged twice, and the actual transfer determines the realized cost.
- Displayed depth is consumed persistently per venue and `AtMS`; changing only Published/Received does not replenish it, a new observation time does.
- Uses decimal, does not infer venue tick/lot/withdrawal rules (supplied by research inputs); does not model network latency or queues; does not represent live trading. The log is capped at 100k fill/transfer identifiers.

## 6. Live-trading Boundaries (Pairs Strategy)

For common live-trading constraints and Hub/embedded-mode details see the cross-sectional factor doc; pairs-related:

| Item | Backtest / paper | Live |
| --- | --- | --- |
| Contract | `linear_perpetual` (execution valuation supports only this class and cash European options) | Linear perpetuals only (`Contract/Linear/Swap`, consistent settlement currency); spot/inverse/delivery/options rejected; hedge-mode positions rejected |
| Shorting | Linear perpetual, open both directions | Same as left |
| Funding | `explicit-zero` or `required-stream` | Default/`banexg`/`hub` use Hub mode: the strategy side accepts `explicit-zero` and `required-stream` (the latter needs `funding_source`), and one must be declared explicitly; `dry_run` Hub forces `explicit-zero`; **embedded banexg binding only (non-Hub) must use `required-stream` with authoritative funding cash capability** |
| Price | Events: tick or 1m stream; spread-type fill models need bid/ask | Real-time subscription, requires startup reconciliation |
| Precision | `QuantityStep/PriceTick/MinSteps/MinNotional` come from normalized metadata; rejected if missing | Same as left |
| Fills | No queue priority/partial fills/network failures; limit orders wait for a later quote to cross | Real fills, no fallback to paper |
| Two legs | Generated simultaneously in the same round, ledger processes them one by one | Asynchronous; an unfilled leg creates hedge error |
| Existing positions | - | Ledger attribution and startup reconciliation required first |

`explicit-zero` is only an explicit zero-fee simplification; using it for perpetual pairs underestimates funding cost. A config switch or custom provider name cannot replace real transport, metadata, account snapshot, data version, and funding evidence.

## 7. Risk, Cost, and the Backtest → Live Quantity Relationship

### 7.1 Return and Cost Approximation

Let each unit of z correspond to residual log σ; the single-pair gross return is approximately:
- Full mean reversion (`|z|` from `|Entry|` to `|Exit|`): `gross return ≈ G·σ·(|Entry|-|Exit|)/(1+|β|)`.
- One round trip turns over about `2G`, with cost about `2G·c` (`c` = one-way fee rate + slippage, measured on leg notional).
- Break-even: `σ·(|Entry|-|Exit|)/(1+|β|) > 2c`.

Ignores β drift, funding, impact cost, exit spread deviation, and drift-rebalance cost (2.6); for screening only.

### 7.2 Key Risks

- **No volatility targeting**: each pair's notional is a fixed `G`.
- **Stop = closing, not protection**: `|z|>=Stop` realizes the loss and blocks re-entry; it does not cap single-trade loss.
- **No default time stop**: with `MaxHold:0` a spread that does not revert occupies exposure indefinitely.
- **Structural breaks** (mergers, delistings, etc.): need external invalidation detection, which the library does not provide.
- **Rebalancing cost**: see 2.6; Events must be verified separately.
- **Capacity**: `research.EstimateCapacityCost(spec CapacityCostSpec{FeeRate, SlippageRate, ImpactCoefficient, MaxParticipation}, tradedNotional, adv, volatility, heldNotional, fundingRate) (CapacityCost{Participation, Fee, Slippage, Impact, Funding, Total, Feasible}, error)`: `Impact = traded·coef·vol·√(traded/adv)`; `adv>0`, `MaxParticipation∈[0,1]` (0 = unlimited); estimate only.
- **Account risk limits**: Gross and margin limits are **absolute amounts in the settlement currency**, not leverage multiples; they do not substitute for the strategy NAV fraction `Gross`.
- **Leg imbalance**: each leg is rounded by its own step size and minimum quantity (inferred), which changes the actual β hedge at small notionals.

### 7.3 Options: Capability Boundaries

```go
type EuropeanOptionSpec struct { Spot, Strike, Years, Volatility, Rate, DividendYield float64; Call bool }
type EuropeanOptionValue struct { Price, Delta, Gamma, Vega, Theta, Rho float64; GreeksValid bool }
func PriceEuropeanOption(s EuropeanOptionSpec) (EuropeanOptionValue, error)  // BSM constant volatility; Vega/Rho are per unit volatility/rate; Theta is per year
func EuropeanImpliedVolatility(s EuropeanOptionSpec, price, tolerance, maxVolatility float64, maxIterations int) (float64, error)  // Years>0
func PutCallImpliedSpot(call, put, strike, years, rate, dividendYield float64) (float64, error)
func AmericanOptionPrice(s EuropeanOptionSpec, steps int) (float64, error)  // CRR tree, steps 1..5000, continuous dividend yield, early exercise at every node
func AmericanImpliedVolatility(s EuropeanOptionSpec, premium, low, high float64, steps, iterations int, tolerance float64) (float64, error) // 0<low<high<=10, iterations<=1000, Years>0
type SurfaceOption struct { Strike, TenorDays, IV, OpenInterest float64; ExpiryMS int64; Call bool }
func OpenInterestWeightedIVSpread(quotes []SurfaceOption) (float64, error)
func VolatilitySmirk(spot float64, quotes []SurfaceOption) (float64, error)   // quotes<=10000
```

- Valuation/diagnostic tools only (no volatility smile or discrete dividends); they do not mean options can be executed.
- On the execution side only paper Events supports cash-settled European options: `Instrument.Valuation="cash_european_option"`, `Option{UnderlyingID, Right, Strike, ExpiryMS, UnderlyingStressMax}`; no early exercise, no physical delivery.
- Expiry settlement is provided by `option_expiry_source`: `SID` is the option, `EventTime` is the exact expiry, `AvailableAt` is the publication time, `Values["fixing"]` is a decimal string; values above `UnderlyingStressMax` are rejected.
- Risk reserve: long = mark value; short put = `Strike×units`; short call = `max(StressMax-Strike,0)×units`; different strikes/expiries do not offset.
- Weights, live `Trade`, and the Banexg adapter all reject cash options.

## 8. Example

For training see 4.1 (`EngleGranger`'s x is a [][]float64 with each row = [logX_i]); `hedge.Beta/Intercept` are static results, and using them with `RegisterPairs` requires the 2.7 custom path; the other parameters can be written directly in YAML.

```go
package pairs

import (
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/factor/statistics"
)

func init() {
	if err := runner.RegisterPairs("eth-btc", func(p *runner.Params) (runner.PairsSpec, error) {
		return runner.PairsSpec{
			Pairs:  []runner.Pair{{Y: "ETH/USDT:USDT", X: "BTC/USDT:USDT"}},
			Field:  "close",
			Window: p.Int("window", 120),
			Band: statistics.BandConfig{
				Entry:   p.Float("entry", 2),
				Exit:    p.Float("exit", 0.5),
				Stop:    p.Float("stop", 4),
				MaxHold: p.Int("maxHold", 0),
			},
			Gross: p.Float("gross", 0),
		}, p.Err()
	}); err != nil {
		panic(err)
	}
}
```

YAML is the same as 2.5, plus `prices: {source: tick, timeframe: event, field: price}` (2.6).
To reject misspelled parameters, add before build returns: `if u := p.Unused(); len(u) > 0 { return runner.PairsSpec{}, fmt.Errorf("unused params: %v", u) }`.

## 9. Still Uncertain (untested)

- Whether Events produces drift-rebalance fills; fill behavior when only `expiry_ms` is raised; the real trigger scenarios for `pair-input-missing`.
- Live trading, options, cold history, and the banexg adapter are based on source code only; the harm of Patch for pairs is confirmed from code, not run end to end.
- Window = 3–5 times the half-life is a rule of thumb; the MacKinnon numbers come from memory and must be checked against authoritative tables.
