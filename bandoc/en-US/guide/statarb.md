# Statistical Arbitrage & Carry

Statistical arbitrage strategies run on the factor engine (`engine: factor`). You declare pairs, baskets or residual models through dedicated registration functions, and the runner handles rolling estimation, the state machine, target portfolios and the ledger. Symbols and structure live in Go code; thresholds and windows live in YAML `params`. For the general `run_policy`, price stream, portfolio and result conventions see [Multi-factor and cross-sectional strategies](./factor.md).

| Need | Registration | Notes |
| --- | --- | --- |
| Fixed two-leg spread regression | `runner.RegisterPairs` | Symbols are written in Go |
| Periodically re-select pairs from a candidate pool | `runner.RegisterRollingPairs` | Symbols come from the YAML `pairs` |
| 2 to 12-leg cointegration baskets | `runner.RegisterBaskets` / `RegisterBasketsParams` | Johansen or multivariate OLS weights |
| Cross-sectional PCA / benchmark residual s-score | `runner.RegisterResidualStatArb` / `RegisterResidualStatArbParams` | Dollar-neutral and factor-beta-neutral |
| Spot plus perpetual basis reversion (carry) | `RegisterPairs` + `SpreadModel: "ratio"` | Needs `extra_markets: [spot]` |

Runnable examples live in the banstrats repository: [examples/statarb](https://github.com/banbox/banstrats/tree/main/examples/statarb) and [examples/carry](https://github.com/banbox/banstrats/tree/main/examples/carry). The example thresholds are demonstration starting points; they are not optimized and imply no returns.

## Prerequisites

- Use `bot backtest` (`execution.mode: weights`) for performance; for pairs and baskets `bot research` only outputs the log-price panel and label diagnostics.
- The decision timeframe defaults to `1h` and needs klines for every symbol in the pool. Use the price stream `prices: {source: kline, timeframe: 1m, field: close}` and set `decision.expiry_ms` to 120000; a 1h close price stream alone expires every target and produces zero fills (see [The zero-trade trap](./factor.md#the-zero-trade-trap)).
- All windows count decision bars. When you change `run_timeframes` (aggregated timeframes such as 4h, 8h and 3d are supported), adjust `window`, `formation` and `reselect` too.
- Keep `portfolio.mode: full`: a closed pair is removed from the target, and `patch` mode never closes positions that are not listed.
- Declare `funding_policy` explicitly. `explicit-zero` treats funding as zero and understates perpetual pair costs; use `storage` or `required-stream` for real funding.

An overlay file (loaded together with your own database, exchange and `time_range` base config):

```yaml
pairs: ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT']
data:
  pit_policy: static-approximation
execution:
  mode: weights
  funding_policy: explicit-zero
run_policy:
  - name: SAPairsEthBtc
    engine: factor
    run_timeframes: [1h]
    params: {window: 120, entry: 2, exit: 0.5, stop: 4, gross: 0.5}
    prices: {source: kline, timeframe: 1m, field: close}
    decision: {latency_ms: 1, expiry_ms: 120000}
    portfolio: {mode: full}
    manifest:
      costs: {fee_rate: 0.0005, slippage_rate: 0.0005, funding_policy: explicit-zero}
    research: {labels: []}
```

```sh
./bot backtest --config your-runtime.yml --config examples/statarb/pairs.yml
```

## Fixed pairs: RegisterPairs

```go
func init() {
	if err := runner.RegisterPairs("SAPairsEthBtc", func(p *runner.Params) (runner.PairsSpec, error) {
		return runner.PairsSpec{
			Pairs:  []runner.Pair{{Y: "ETH/USDT:USDT", X: "BTC/USDT:USDT"}},
			Window: p.Int("window", 120),
			Band: statistics.BandConfig{
				Entry: p.Float("entry", 2), Exit: p.Float("exit", 0.5),
				Stop: p.Float("stop", 4), MaxHold: p.Int("maxHold", 0),
			},
			Gross: p.Float("gross", 0),
		}, p.Err()
	}); err != nil {
		panic(err)
	}
}
```

The `Pair{Y, X}` symbols must match the runtime symbols exactly. `build` is called several times per run and must be a side-effect-free pure function; `Params` has no string reader, so symbols cannot be parameterized from YAML. A misspelled parameter name is not reported automatically: check `p.Unused()` before returning. After registration the YAML only needs `name`; the runner binds the matching portfolio builder automatically.

### Signals and positions

On each decision bar the spread is fitted on the last `Window` valid observations and `z = residual / residual std`:

- `z <= -Entry` goes long the spread (buy Y, sell X), `z >= Entry` goes short, and `|z| <= Exit` takes profit.
- `|z| >= Stop` (optional) stops out; `MaxHold` (optional, in decision bars) closes at expiry. After a stop or expiry, z must return inside `[-Exit, Exit]` before the pair may re-enter.
- `BandConfig.Mode` is `reversion` (default) or `momentum` (breakout following); `LongOnly`/`ShortOnly` restrict direction. Validation requires `Entry > 0` and `0 <= Exit < Entry`, and in reversion mode `Stop` is 0 or greater than `Entry`. There are no built-in threshold defaults.
- Each pair's gross notional is `Gross` (fraction of strategy NAV; default `1/pair count`; the total must not exceed 1). The Y leg weight is `s·G/(1+|β|)` and the X leg `-s·G·β/(1+|β|)`. Weights add up when a symbol appears in several pairs.

### Hedge ratio and exit z

- **Positive-beta guard**: entries with `β <= 0` are rejected by default (it would leave both legs on the same side); `MinBeta` sets a floor and `AllowNegativeBeta: true` permits negative slopes.
- **Frozen exit z**: while holding, the hedge ratio stays at its entry value, and exit and stop z are computed with that frozen β on the current window so rolling-β drift cannot cause false reversion exits. `DynamicExitZ: true` uses the refitted z instead, and `DynamicHedge: true` lets the held hedge ratio follow the online estimate.
- **Drift band**: `DriftBand` (L1 weight distance). While holding, natural weight drift below this value keeps the current position to avoid needless turnover; at 0 the book rebalances to the target weights every round and pays costs.
- **Inverse-volatility sizing**: `WeightMode: "inverse_vol"` splits the total gross `Gross × pair count` across held pairs by the inverse of the residual standard deviation, with a per-pair cap `MaxPairGross` (default `min(total gross, 2×Gross)`).

### Spread models

| Value | Meaning |
| --- | --- |
| `""` / `log_ols` (default) | Rolling OLS on log prices; β is an elasticity |
| `raw_ols` | Rolling OLS on raw prices, converted to a dollar-notional hedge ratio |
| `tls` | Orthogonal regression on log prices; symmetric when Y and X are swapped |
| `kalman` | Random-walk state space (`KalmanDelta`, `KalmanVe`; 0 uses defaults), with the pre-update innovation as a look-ahead-free z |
| `ratio` | Rolling Bollinger z of the price ratio `Y/X`, fixed equal-dollar hedge (β=1) |
| `return_diff` | z of the windowed cumulative return difference, fixed equal-dollar hedge |
| `copula` | Entry by conditional probability after a rank transform; `Copula.Family` is `auto` (default), `empirical`, `gaussian`, `clayton`, `gumbel` or `frank`, with `LowerProb` (default 0.05), `UpperProb` (default 0.95), `ExitBand`, `StopProb` |

### Entry gates and filter hooks

These only block new entries; exits and stops of open positions are never affected:

| Field | Effect |
| --- | --- |
| `MinHalfLife` / `MaxHalfLife` | `FitOU` on the spread window; the discrete half-life (in bars) must lie in range, so spreads that do not revert open nothing |
| `MinRollingCorr` / `CorrWindow` | Lower bound on the 1-bar log-return correlation |
| `MinHurst` / `MaxHurst` | Hurst range of the spread window (reversion usually sets `MaxHurst: 0.5`) |
| `SpreadRSIPeriod` / `SpreadRSIUpper` / `SpreadRSILower` | Wilder RSI gate on the spread, default 70/30 |
| `FeatureExprs` / `ExtraFeatures` | Per-leg feature columns (expressions or factor nodes; binding alias `kline`) |
| `EntryFilter` | `func(PairFilterContext) bool`; reads point-in-time features (`FeatureY/FeatureX`) at entry and blocks when it returns false |

[pairs.go](https://github.com/banbox/banstrats/blob/main/examples/statarb/pairs.go) in banstrats uses `FeatureExprs` plus `EntryFilter` to refuse entries while either leg's volatility spikes. These gates, together with the optional parameters of `BandMode`, the Kalman and the copula models, are controlled through `params`; changing any parameter produces a new strategy identity.

### Missing data

No signal is produced until the window is full. If either leg of an open pair goes missing, a close is requested and the pair pauses; it may re-enter once data returns. A leg with no data at all in the backtest window (delisted or not yet listed) makes every pair containing it skip the whole run, while other pairs run normally and their budget is not scaled up; if all pairs are unavailable the backtest ends normally with zero trades and flat equity. A misspelled leg or one outside the run's symbol pool is still an error.

## Rolling re-selection: RegisterRollingPairs

No symbol literals: the YAML root `pairs` is the candidate pool, and pairs are re-selected periodically in a formation plus trading period scheme using only closed data:

```go
runner.RegisterRollingPairs("SARollingPairs", func(p *runner.Params) (runner.RollingPairsSpec, error) {
	return runner.RollingPairsSpec{
		FormationBars: p.Int("formation", 240),
		ReselectBars:  p.Int("reselect", 120),
		PairCount:     p.Int("pairCount", 4),
		MaxPerSymbol:  p.Int("maxPerSymbol", 2),
		Window:        p.Int("window", 120),
		WeightMode:    "inverse_vol",
		Band: statistics.BandConfig{
			Entry: p.Float("entry", 2), Exit: p.Float("exit", 0.5),
			Stop: p.Float("stop", 4), MaxHold: p.Int("maxHold", 60),
		},
	}, p.Err()
})
```

- `SelectionMethod`: `cointegration` (default; Engle-Granger residual ADF `<= MaxADF`, default -2.86, plus a half-life range) or `distance` (Gatev minimum distance). Coverage (`MinCoverage`) and correlation (`MinCorr`) requirements also apply.
- At each reselection the best `PairCount` pairs are chosen greedily by ascending score, each symbol appearing at most `MaxPerSymbol` times; ties break deterministically by symbol, so identical inputs give bit-identical results.
- There is no target until `FormationBars` bars are available; newly selected pairs are warmed up from previously visible history and can trade in the same round.
- `HandoffMode`: `graceful` (default) lets pairs that were not re-selected only exit until they close normally; `immediate` closes them on the rotation bar.
- The online gates (`OnlineHalfLifeGate`, `MinRollingCorr`, `MinHurst`/`MaxHurst`, `SpreadRSI*`) and `EntryFilter` mean the same as the same-named `PairsSpec` fields.

## Cointegration baskets: RegisterBaskets

```go
runner.RegisterBasketsParams("SABasketJohansen", func(p *runner.Params) (runner.BasketsSpec, error) {
	return runner.BasketsSpec{
		Baskets: []runner.BasketDef{
			{Name: "majors", Symbols: []string{"BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT"}},
		},
		Window:               p.Int("window", 240),
		DifferenceLags:       p.Int("differenceLags", 1),
		WeightMethod:         "johansen",
		RequireCointegration: true,
		Confidence:           0.95,
		Band: statistics.BandConfig{Entry: 2, Exit: 0.5, Stop: 4, MaxHold: 96},
	}, p.Err()
})
```

Each basket has 2 to 12 distinct symbols. `WeightMethod` is `johansen` (first cointegrating vector; the window must be at least `legs×(DifferenceLags+1)+3`), `multivariate_ols` (leg 0 regressed on the others) or `fixed` (explicit `Weights`). `RequireCointegration` requires the current Johansen rank test (`Confidence` 0.90/0.95/0.99) to pass before opening. While holding, entry weights are frozen by default; `DynamicWeights: true` keeps refreshing them. `GrossPerBasket`, `MaxGross`, `DriftBand` and `WeightMode` mean the same as for pairs. `RegisterBaskets(name, spec)` takes a fixed spec, while `RegisterBasketsParams` can read from `params`.

## Residual s-score: RegisterResidualStatArb

Following Avellaneda and Lee (2010), systematic factors are extracted over the investable universe and an OU fit of each asset's cumulative residual yields a centered s-score:

```go
runner.RegisterResidualStatArbParams("SAResidualPCA", func(p *runner.Params) (runner.ResidualStatArbSpec, error) {
	return runner.ResidualStatArbSpec{
		FactorModel: "pca", NumFactors: p.Int("numFactors", 3),
		Window: p.Int("window", 60), EntryS: 1.25, StopS: 3, MaxHoldBars: 48,
		WeightMode: "inverse_sigma", MaxWeightPerAsset: 0.15,
	}, p.Err()
})
```

- `FactorModel`: `pca` (the first `NumFactors` principal components of standardized returns) or `benchmarks` (`Benchmarks` lists 1 to 16 benchmark symbols such as BTC and ETH, which must be in `pairs`).
- `s <= -EntryS` goes long, `s >= EntryS` goes short, the position takes profit inside `ExitLongS`/`ExitShortS`, and `|s| >= StopS` or `MaxHoldBars` closes it; `MinHalfLife`/`MaxHalfLife` are an OU half-life entry gate.
- The target is projected to be strictly dollar-neutral (weights sum to 0) and factor-beta-neutral; `TargetGross`, `MaxWeightPerAsset` and `DriftBand` control total exposure, per-asset cap and drift buffer.
- Commonly combined with the [rolling liquidity universe](./factor.md#point-in-time-rolling-liquidity-universe) (YAML `universe`) so estimation runs only on the most active names at the time.

## Spot plus perpetual: carry

Spot and perpetual markets of one exchange can share a run. Declare `market_type: linear` and `extra_markets: [spot]` at the root, list both spot `BTC/USDT` and perpetual `BTC/USDT:USDT` in `pairs`, and use `funding_policy: storage` (funding settles on the perpetual leg only; the spot leg has none):

```go
runner.RegisterPairs("CarryBasis", func(p *runner.Params) (runner.PairsSpec, error) {
	return runner.PairsSpec{
		Pairs:       []runner.Pair{{Y: "BTC/USDT:USDT", X: "BTC/USDT"}},
		Window:      p.Int("window", 168),
		SpreadModel: "ratio",
		Band: statistics.BandConfig{
			Entry: p.Float("entry", 2), Exit: p.Float("exit", 0.5),
			Stop: p.Float("stop", 5), MaxHold: p.Int("maxHold", 72), ShortOnly: true,
		},
		LongOnlySpot: true,
		Gross:        p.Float("gross", 0.5),
	}, p.Err()
})
```

```yaml
market_type: linear
extra_markets: [spot]
pairs: ['BTC/USDT', 'BTC/USDT:USDT']
execution: {mode: weights, funding_policy: storage}
```

When the perpetual trades at a premium to spot (ratio z above the entry threshold) the strategy sells the perpetual, buys the same dollar amount of spot, and closes when the spread reverts. The ledger forbids naked spot shorts; `ShortOnly` forbids the opposite spread and `LongOnlySpot` blocks any entry that would short the spot leg (diagnostic `pair-spot-short-blocked`). The database must cover spot klines, perpetual klines (1h and 1m) and `funding_rate` data for the backtest window. This is basis-reversion trading, not hold-to-expiry arbitrage: returns come from basis convergence and funding on the short perpetual leg, and both can be negative. See the full configuration in the [carry example](https://github.com/banbox/banstrats/tree/main/examples/carry).

## Reading the results

Check the completion status in `run.json` first, then `Results[]` (Go field names with capital first letters; the same result appears in `run-completed` events).

- `Result.Pairs` is collected only by historical `RegisterPairs`, `RegisterRollingPairs` and basket runs. Each entry has PnL, Trades, Wins, WinRate, InMarketBars, PausedBars, AvgHoldBars, LastBeta, Turnover and Episodes. Episodes are capped; `EpisodeCount` holds the full count. Exit reasons are `exit`, `stop`, `maxhold`, `leg_missing` and `end` (`reselect` for an immediate rolling handoff).
- `Unavailable`/`UnavailableLeg` identify pairs skipped for the whole run; `AllPairsUnavailable` means no pair could run, and a flat NAV is not evidence of performance; `PausedPeriods` counts rounds in which all pairs were paused.
- Pair `PnL` is a fraction of initial NAV computed from decision prices and each pair's own target weights, without allocated costs; it is not fill PnL. Reconcile with `PairsGrossPnL` (sum over pairs), `PairsNAVReturn` (book return) and `PairsResidual = PairsNAVReturn - PairsGrossPnL` (costs plus approximations from delay, weight drift and netting of shared symbols).
- `Result.Attribution` is available only with the weights book and gives long/short leg PnL, exposure, fees, slippage, funding and `TopSymbols`, which helps spot a single dominant contributor.

## Limits and caveats

- **Backtest versus live**: the registrations above are demonstrated on weights backtests. Live trading supports linear perpetuals with net positions only; spot legs and options are rejected on the live path, two legs fill asynchronously and can leave hedge error, and existing positions need attribution and reconciliation first. See [live integration](./factor.md#live-integration-and-limits). This page does not claim that pairs strategies have passed end-to-end validation on a real exchange.
- **Selection bias**: backtesting “cointegrated pairs” picked on the full sample is selection bias. The fixed pairs in the examples are just common combinations; rolling re-selection is closer to a tradable process. Passing a numerical test does not mean significance or profitability.
- **Costs**: every round trip pays fees and slippage, and perpetuals also pay funding; check that `(Entry - Exit) × residual volatility` clearly exceeds round-trip cost.
- **Drift charging**: with `DriftBand` at 0 the weights book rebalances to target every round and charges for the drift.
- **Windows and gaps**: a window is the last N valid observations, not calendar bars; bars where a leg is missing do not enter the window.
- The example tests cover config parsing, parameter validation and behaviour on synthetic data only; they have not been run on real market data.

Related pages: [Multi-factor and cross-sectional strategies](./factor.md), [Backtesting](./backtest.md), [Factor API](../api/factor.md).
