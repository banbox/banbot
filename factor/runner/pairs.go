package runner

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/expr"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
)

// Pair names two symbols exactly as they appear in the run's SIDMap
// (for example "ETH/USDT:USDT"). The regression is ln(Y) = alpha + beta*ln(X).
type Pair struct{ Y, X string }

// PairFilterContext exposes point-in-time spread diagnostics and per-leg factor DAG
// features at decision step t (zero lookahead) when evaluating whether a flat pair
// may open ProposedSide (+1 long spread: buy Y / sell X; -1 short spread: sell Y / buy X).
type PairFilterContext struct {
	DecisionTime      int64
	Pair              Pair
	ProposedSide      int                // +1 (long Y / short X) or -1 (short Y / long X)
	Z                 float64            // current spread z-score (or symmetric probit score under "copula")
	Beta              float64            // current dollar-notional hedge ratio (-wX / wY)
	HalfLife          float64            // FitOU DiscreteHalfLife in bars on current spread window (NaN if non-reverting / < 4 bars)
	RollingCorr       float64            // Pearson correlation of 1-bar log returns over CorrWindow (NaN if unavailable)
	Hurst             float64            // variance-scaling Hurst exponent in [0, 1] on current spread window (NaN if < 8 bars)
	SpreadRSI         float64            // Wilder RSI in [0, 100] on current spread window (NaN if unavailable)
	CopulaProbYGivenX float64            // P(U <= u | V = v) in (0, 1) when SpreadModel == "copula" (NaN otherwise)
	CopulaProbXGivenY float64            // P(V <= v | U = u) in (0, 1) when SpreadModel == "copula" (NaN otherwise)
	FeaturesY         map[string]float64 // valid finite feature values for leg Y at decision step t
	FeaturesX         map[string]float64 // valid finite feature values for leg X at decision step t
}

// FeatureY returns leg Y's feature value and true if present and finite at step t.
func (c PairFilterContext) FeatureY(name string) (float64, bool) {
	v, ok := c.FeaturesY[name]
	return v, ok && finitePairFloat(v)
}

// FeatureX returns leg X's feature value and true if present and finite at step t.
func (c PairFilterContext) FeatureX(name string) (float64, bool) {
	v, ok := c.FeaturesX[name]
	return v, ok && finitePairFloat(v)
}

// PairsSpec declares a two-leg statistical-arbitrage strategy.
type PairsSpec struct {
	Pairs  []Pair
	Field  string                // kline field, default "close"
	Window int                   // regression/copula window in decision bars, at least 3
	Band   statistics.BandConfig // entry/exit/stop/max-hold thresholds
	// Gross is the gross notional of one open pair as a fraction of strategy NAV
	// (or per-pair budget unit under WeightMode "inverse_vol").
	// Default 1/len(Pairs), so all pairs open at once never exceed 100% gross.
	Gross float64
	// LongOnlySpot blocks spread entries that would short a spot leg (identified
	// via Config.SpotSIDs or Execution.Instruments), enabling one-sided spot-perp
	// cash-and-carry without triggering spot short-sale rejection.
	LongOnlySpot bool
	// MinBeta is the minimum rolling slope required when opening a new position.
	// By default (!AllowNegativeBeta) entry requires NotionalBeta > max(0, MinBeta);
	// when AllowNegativeBeta is true and MinBeta > 0, entry requires |NotionalBeta| > MinBeta.
	MinBeta float64
	// AllowNegativeBeta disables the default positive-beta entry guard (NotionalBeta > 0).
	AllowNegativeBeta bool
	// DynamicExitZ opts out of frozen-beta exit/stop evaluation: by default (false),
	// while a position is open its exit/stop z-score is evaluated against the entry-frozen
	// hedge ratio via EvalFrozenZ so exit signals match held PnL.
	DynamicExitZ bool
	// DriftBand buffers minor weight drift while holding: when > 0 and a pair stays open
	// in the same direction, its pre-trade drifted weights are kept if their L1 distance
	// to this round's ideal target weights is strictly below DriftBand (subject to total gross <= 1).
	DriftBand float64
	// WeightMode selects cross-pair sizing: "" or "equal" (default) assigns fixed Gross per open pair;
	// "inverse_vol" allocates TotalGross = Gross * availablePairs across open pairs inversely
	// proportional to each pair's spread residual Std, capped per pair by MaxPairGross.
	WeightMode string
	// MaxPairGross caps any single pair's gross notional under WeightMode "inverse_vol".
	// Default 0 uses min(TotalGross, 2*Gross). Must be in [0, 1].
	MaxPairGross float64
	// SpreadModel selects the online two-leg spread estimator:
	// "" or "log_ols" (default): rolling OLS on log prices ln(Y) = alpha + beta*ln(X).
	// "raw_ols": rolling OLS on raw prices Y = alpha + beta*X with notional hedge beta*(X/Y).
	// "tls": symmetric orthogonal (Deming / Total Least Squares) regression on log prices.
	// "kalman": state-space random-walk slope/intercept filter (statistics.KalmanPair) with pre-update innovation Z.
	// "ratio": rolling price ratio R = Y/X Bollinger z-score with equal dollar-neutral hedge (beta = 1).
	// "return_diff": rolling cumulative return spread (Y/Y_0) - (X/X_0) z-score with equal dollar-neutral hedge (beta = 1).
	// "copula": rolling rank-uniform copula conditional probabilities P(U <= u | V = v) and P(V <= v | U = u).
	SpreadModel string
	// Copula configures the copula family ("auto", "empirical", "gaussian", "clayton", "gumbel", "frank")
	// and conditional probability thresholds (LowerProb default 0.05, UpperProb default 0.95, ExitBand default 0)
	// when SpreadModel == "copula".
	Copula statistics.CopulaConfig
	// KalmanDelta is the state-transition noise ratio in (0, 1) under SpreadModel "kalman" (default 1e-4 when 0).
	KalmanDelta float64
	// KalmanVe is the observation noise variance (> 0) under SpreadModel "kalman" (default 1e-3 when 0).
	KalmanVe float64
	// DynamicHedge continuously updates an open position's hedge ratio to the latest online estimate
	// instead of freezing it at entry.
	DynamicHedge bool
	// MinHalfLife and MaxHalfLife optionally gate new entries online using statistics.FitOU
	// DiscreteHalfLife (in decision bars) on the current rolling spread window.
	// When either is > 0, non-reverting windows (FitOU error or DiscreteHalfLife out of range) block entry.
	MinHalfLife float64
	MaxHalfLife float64

	// MinRollingCorr optionally gates new entries by requiring Pearson correlation of 1-bar log
	// returns over CorrWindow (defaults to Window when MinRollingCorr > 0 and CorrWindow == 0; >= 4)
	// to be >= MinRollingCorr (in [0, 1]). Never blocks closing an open position.
	MinRollingCorr float64
	CorrWindow     int
	// MinHurst and MaxHurst optionally gate new entries using statistics.HurstExponent on the
	// current spread window (Window >= 8), requiring Hurst in [MinHurst, MaxHurst].
	MinHurst float64
	MaxHurst float64
	// SpreadRSIPeriod, SpreadRSIUpper, and SpreadRSILower optionally gate new entries using
	// Wilder RSI on the current spread window (Window >= SpreadRSIPeriod+1):
	// in reversion mode, short spread (-1) requires RSI >= SpreadRSIUpper (default 70) and
	// long spread (+1) requires RSI <= SpreadRSILower (default 30); reversed in momentum mode.
	SpreadRSIPeriod int
	SpreadRSIUpper  float64
	SpreadRSILower  float64

	// ExtraFeatures and FeatureExprs optionally attach custom per-leg factor DAG feature
	// columns (evaluated causally on the decision grid) exposed to EntryFilter via FeaturesY/FeaturesX.
	// When nil/empty, the compiled factor.Plan is identical to the standard log-price plan (preserving CSE).
	ExtraFeatures func(FactorContext) map[string]*factor.Node
	FeatureExprs  map[string]string
	// EntryFilter is an optional point-in-time programmable gate evaluated only when opening
	// a new position (+1 or -1). Returning false blocks entry; exits/stops are never blocked.
	EntryFilter func(PairFilterContext) bool
}

const (
	pairsLogOutput     = "logp"
	pairsFeaturePrefix = "feat:"
)

// RegisterPairs registers a statistical-arbitrage strategy in one call. The
// build function reads params once per run and returns a declarative spec:
//
//	runner.RegisterPairs("eth-btc", func(p *runner.Params) (runner.PairsSpec, error) {
//	    return runner.PairsSpec{
//	        Pairs:  []runner.Pair{{Y: "ETH/USDT:USDT", X: "BTC/USDT:USDT"}},
//	        Window: p.Int("window", 120),
//	        Band:   statistics.BandConfig{Entry: p.Float("entry", 2), Exit: p.Float("exit", 0.5), Stop: p.Float("stop", 4)},
//	    }, p.Err()
//	})
//
// Both legs must be in the run's Tradable universe. Each run gets private
// state: one SpreadEstimator and one BandTrader/CopulaTrader per pair, so replay
// and live runs never share positions. The hedge ratio is frozen at entry while a
// pair is open (unless DynamicHedge is set) and refreshed for the next entry.
//
// The strategy binds its own portfolio builder, so YAML only needs
// `name: <name>`. Wire it with `run_timeframes` for the decision bar.
func RegisterPairs(name string, build func(*Params) (PairsSpec, error)) error {
	if name == "" || build == nil {
		return errors.New("runner: pairs strategy needs name and build function")
	}
	builderName := name + "-pairs-v1"
	pairsLegResolvers.Store(builderName, build)
	err := RegisterDefinition(name, func(c Config) (*factor.Plan, research.ComboSpec, error) {
		spec, err := resolvePairsSpec(build, c)
		if err != nil {
			return nil, research.ComboSpec{}, err
		}
		return compilePairsPlan(spec.Field, c.Factor.TimeFrame, c.Params(), spec.ExtraFeatures, spec.FeatureExprs)
	})
	if err != nil {
		return err
	}
	err = RegisterPortfolioFactory(builderName, func(c Config) (PortfolioBuilder, error) {
		spec, err := resolvePairsSpec(build, c)
		if err != nil {
			return nil, err
		}
		spotSet := make(map[int32]bool)
		for _, sid := range c.spotSIDList() {
			spotSet[sid] = true
		}
		builder, tracker, err := newTrackedPairsBuilderWithSpots(spec, c.Snapshot.SIDMap, c.UnavailableSymbols, spotSet)
		if err == nil && c.observe != nil {
			c.observe.pairs, c.observe.reset = tracker.report, tracker.resetStats
		}
		return builder, err
	}, func(c Config) error {
		spec, err := resolvePairsSpec(build, c)
		if err != nil {
			return err
		}
		if spec.ExtraFeatures != nil || len(spec.FeatureExprs) > 0 {
			tf := c.Factor.TimeFrame
			if tf == "" {
				tf = "1h"
			}
			_, _, err = compilePairsPlan(spec.Field, tf, c.Params(), spec.ExtraFeatures, spec.FeatureExprs)
		}
		return err
	})
	if err != nil {
		return err
	}
	bindDefinitionBuilder(name, builderName)
	return nil
}

func resolvePairsSpec(build func(*Params) (PairsSpec, error), c Config) (PairsSpec, error) {
	p := c.Params()
	spec, err := build(p)
	if err != nil {
		return spec, err
	}
	if err := p.Err(); err != nil {
		return spec, err
	}
	return normalizePairsSpec(spec)
}

func finitePairFloat(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

type pairGateConfig struct {
	window          int
	spreadModel     string
	copula          statistics.CopulaConfig
	band            statistics.BandConfig
	minBeta         float64
	allowNegBeta    bool
	enableHalfLife  bool
	minHalfLife     float64
	maxHalfLife     float64
	minRollingCorr  float64
	corrWindow      int
	minHurst        float64
	maxHurst        float64
	spreadRSIPeriod int
	spreadRSIUpper  float64
	spreadRSILower  float64
	entryFilter     func(PairFilterContext) bool
}

func (g pairGateConfig) needsCorrTracker() bool {
	return g.minRollingCorr > 0 || g.corrWindow > 0 || (g.entryFilter != nil && g.window >= 4)
}

func (g pairGateConfig) effectiveCorrWindow() int {
	if g.corrWindow > 0 {
		return g.corrWindow
	}
	return g.window
}

// normalizeSpreadAndBand validates SpreadModel, Copula, and Band together so SpreadModel "copula"
// may omit Band.Entry/Exit (deriving probit equivalents from Copula.LowerProb/UpperProb/ExitBand)
// while still enforcing all BandConfig invariants for every model.
func normalizeSpreadAndBand(
	model string,
	window int,
	kalmanDelta, kalmanVe float64,
	copula statistics.CopulaConfig,
	band statistics.BandConfig,
) (statistics.SpreadModelConfig, statistics.BandConfig, error) {
	smCfg, err := (statistics.SpreadModelConfig{
		Model:       model,
		Window:      window,
		KalmanDelta: kalmanDelta,
		KalmanVe:    kalmanVe,
		Copula:      copula,
	}).Normalize()
	if err != nil {
		return smCfg, band, err
	}
	if smCfg.Model == statistics.SpreadModelCopula {
		if band.Entry == 0 && band.Exit == 0 {
			entryZ, errE := statistics.NormInv(math.Max(1.0-smCfg.Copula.LowerProb, smCfg.Copula.UpperProb))
			exitZ, errX := statistics.NormInv(0.5 + smCfg.Copula.ExitBand)
			if errE != nil || errX != nil {
				return smCfg, band, errors.New("runner: invalid copula band probability thresholds")
			}
			band.Entry = entryZ
			band.Exit = math.Max(0, exitZ)
		}
		if _, err := statistics.NewCopulaTrader(smCfg.Copula, band); err != nil {
			return smCfg, band, err
		}
	}
	if err := band.Validate(); err != nil {
		return smCfg, band, err
	}
	return smCfg, band, nil
}

func normalizeRegimeGates(
	window int,
	minCorr float64,
	corrWin int,
	minHurst, maxHurst float64,
	rsiPeriod int,
	rsiUpper, rsiLower float64,
) (int, int, float64, float64, error) {
	if !finitePairFloat(minCorr) || minCorr < 0 || minCorr > 1 {
		return corrWin, rsiPeriod, rsiUpper, rsiLower, errors.New("runner: pairs min rolling correlation must be in [0, 1]")
	}
	if corrWin < 0 {
		return corrWin, rsiPeriod, rsiUpper, rsiLower, errors.New("runner: pairs correlation window must be non-negative")
	}
	if minCorr > 0 && corrWin == 0 {
		corrWin = window
	}
	if (minCorr > 0 || corrWin > 0) && corrWin < 4 {
		return corrWin, rsiPeriod, rsiUpper, rsiLower, errors.New("runner: pairs correlation window must be at least 4")
	}
	if !finitePairFloat(minHurst) || minHurst < 0 || minHurst > 1 {
		return corrWin, rsiPeriod, rsiUpper, rsiLower, errors.New("runner: pairs min Hurst must be in [0, 1]")
	}
	if !finitePairFloat(maxHurst) || maxHurst < 0 || maxHurst > 1 {
		return corrWin, rsiPeriod, rsiUpper, rsiLower, errors.New("runner: pairs max Hurst must be in [0, 1]")
	}
	if minHurst > 0 && maxHurst > 0 && maxHurst < minHurst {
		return corrWin, rsiPeriod, rsiUpper, rsiLower, errors.New("runner: pairs max Hurst must be >= min Hurst")
	}
	if (minHurst > 0 || maxHurst > 0) && window < 8 {
		return corrWin, rsiPeriod, rsiUpper, rsiLower, errors.New("runner: pairs window must be at least 8 when Hurst gate is enabled")
	}
	if rsiPeriod < 0 || rsiPeriod == 1 {
		return corrWin, rsiPeriod, rsiUpper, rsiLower, errors.New("runner: pairs spread RSI period must be 0 or >= 2")
	}
	if !finitePairFloat(rsiUpper) || rsiUpper < 0 || rsiUpper >= 100 {
		return corrWin, rsiPeriod, rsiUpper, rsiLower, errors.New("runner: pairs spread RSI upper threshold must be in [0, 100)")
	}
	if !finitePairFloat(rsiLower) || rsiLower < 0 || rsiLower >= 100 {
		return corrWin, rsiPeriod, rsiUpper, rsiLower, errors.New("runner: pairs spread RSI lower threshold must be in [0, 100)")
	}
	if (rsiUpper > 0 || rsiLower > 0) && rsiPeriod == 0 {
		rsiPeriod = 14
	}
	if rsiPeriod > 0 {
		if rsiUpper == 0 && rsiLower == 0 {
			rsiUpper, rsiLower = 70, 30
		}
		if rsiLower > 0 && rsiUpper > 0 && rsiLower >= rsiUpper {
			return corrWin, rsiPeriod, rsiUpper, rsiLower, errors.New("runner: pairs spread RSI lower threshold must be < upper threshold")
		}
		if window < rsiPeriod+1 {
			return corrWin, rsiPeriod, rsiUpper, rsiLower, errors.New("runner: pairs window must be at least SpreadRSIPeriod+1")
		}
	}
	return corrWin, rsiPeriod, rsiUpper, rsiLower, nil
}

func normalizePairsSpec(spec PairsSpec) (PairsSpec, error) {
	if spec.Field == "" {
		spec.Field = "close"
	}
	if len(spec.Pairs) == 0 {
		return spec, errors.New("runner: pairs strategy declares no pairs")
	}
	if spec.Window < 3 {
		return spec, errors.New("runner: pairs window must be at least 3")
	}
	smCfg, bandCfg, err := normalizeSpreadAndBand(spec.SpreadModel, spec.Window, spec.KalmanDelta, spec.KalmanVe, spec.Copula, spec.Band)
	if err != nil {
		return spec, err
	}
	spec.SpreadModel, spec.KalmanDelta, spec.KalmanVe = smCfg.Model, smCfg.KalmanDelta, smCfg.KalmanVe
	spec.Copula, spec.Band = smCfg.Copula, bandCfg

	if spec.Gross == 0 {
		spec.Gross = 1 / float64(len(spec.Pairs))
	}
	if !finitePairFloat(spec.Gross) || !(spec.Gross > 0) || spec.Gross*float64(len(spec.Pairs)) > 1+1e-9 {
		return spec, errors.New("runner: pairs gross per pair must be positive and keep total gross within 1")
	}
	if !finitePairFloat(spec.MinBeta) || spec.MinBeta < 0 {
		return spec, errors.New("runner: pairs min beta must be finite and non-negative")
	}
	if !finitePairFloat(spec.DriftBand) || spec.DriftBand < 0 {
		return spec, errors.New("runner: pairs drift band must be finite and non-negative")
	}
	switch spec.WeightMode {
	case "", "equal":
		spec.WeightMode = "equal"
	case "inverse_vol":
	default:
		return spec, fmt.Errorf("runner: unknown pairs weight mode %q", spec.WeightMode)
	}
	if !finitePairFloat(spec.MaxPairGross) || spec.MaxPairGross < 0 || spec.MaxPairGross > 1+1e-9 {
		return spec, errors.New("runner: pairs max pair gross must be in [0, 1]")
	}
	if !finitePairFloat(spec.MinHalfLife) || spec.MinHalfLife < 0 {
		return spec, errors.New("runner: pairs min half-life must be finite and non-negative")
	}
	if !finitePairFloat(spec.MaxHalfLife) || spec.MaxHalfLife < 0 || (spec.MaxHalfLife > 0 && spec.MaxHalfLife < spec.MinHalfLife) {
		return spec, errors.New("runner: pairs max half-life must be finite, non-negative, and >= min half-life")
	}
	if (spec.MinHalfLife > 0 || spec.MaxHalfLife > 0) && spec.Window < 4 {
		return spec, errors.New("runner: pairs window must be at least 4 when half-life gate is enabled")
	}
	corrWin, rsiPeriod, rsiUpper, rsiLower, err := normalizeRegimeGates(
		spec.Window, spec.MinRollingCorr, spec.CorrWindow,
		spec.MinHurst, spec.MaxHurst,
		spec.SpreadRSIPeriod, spec.SpreadRSIUpper, spec.SpreadRSILower,
	)
	if err != nil {
		return spec, err
	}
	spec.CorrWindow = corrWin
	spec.SpreadRSIPeriod, spec.SpreadRSIUpper, spec.SpreadRSILower = rsiPeriod, rsiUpper, rsiLower

	for featName := range spec.FeatureExprs {
		if strings.TrimSpace(featName) == "" {
			return spec, errors.New("runner: empty feature expression name")
		}
	}
	for _, pair := range spec.Pairs {
		if pair.Y == "" || pair.X == "" || pair.Y == pair.X {
			return spec, errors.New("runner: each pair needs two distinct symbols")
		}
	}
	return spec, nil
}

func (s PairsSpec) gateConfig() pairGateConfig {
	return pairGateConfig{
		window:          s.Window,
		spreadModel:     s.SpreadModel,
		copula:          s.Copula,
		band:            s.Band,
		minBeta:         s.MinBeta,
		allowNegBeta:    s.AllowNegativeBeta,
		enableHalfLife:  s.MinHalfLife > 0 || s.MaxHalfLife > 0,
		minHalfLife:     s.MinHalfLife,
		maxHalfLife:     s.MaxHalfLife,
		minRollingCorr:  s.MinRollingCorr,
		corrWindow:      s.CorrWindow,
		minHurst:        s.MinHurst,
		maxHurst:        s.MaxHurst,
		spreadRSIPeriod: s.SpreadRSIPeriod,
		spreadRSIUpper:  s.SpreadRSIUpper,
		spreadRSILower:  s.SpreadRSILower,
		entryFilter:     s.EntryFilter,
	}
}

func makePairsLogNode(fieldName, timeframe string) *factor.Node {
	field := factor.Field("kline", fieldName, timeframe)
	return factor.Custom("pairs-log-v1", []*factor.Node{field}, func(v []factor.Numeric) factor.Numeric {
		if v[0].Validity != factor.Valid {
			return v[0]
		}
		if v[0].Value <= 0 {
			return factor.Numeric{Value: math.NaN(), Validity: factor.NonFinite}
		}
		return factor.Numeric{Value: math.Log(v[0].Value), Validity: factor.Valid}
	})
}

func compilePairsLogPlan(fieldName, timeframe string) (*factor.Plan, research.ComboSpec, error) {
	logp := makePairsLogNode(fieldName, timeframe)
	plan, err := factor.New().Add(pairsLogOutput, logp).Compile()
	return plan, research.ComboSpec{Method: research.Equal, Columns: []string{pairsLogOutput}}, err
}

func compilePairsPlan(
	fieldName, timeframe string,
	params *Params,
	extraFn func(FactorContext) map[string]*factor.Node,
	featureExprs map[string]string,
) (*factor.Plan, research.ComboSpec, error) {
	if extraFn == nil && len(featureExprs) == 0 {
		return compilePairsLogPlan(fieldName, timeframe)
	}
	logp := makePairsLogNode(fieldName, timeframe)
	outputs := map[string]*factor.Node{pairsLogOutput: logp}

	if len(featureExprs) > 0 {
		var pMap map[string]float64
		if params != nil && len(params.values) > 0 {
			pMap = maps.Clone(params.values)
		}
		exprNodes, err := expr.CompileNodes(expr.Spec{
			SchemaVersion: 1,
			TimeFrame:     timeframe,
			Bindings: map[string]expr.Binding{
				"kline": {Source: "kline", TimeFrame: timeframe},
			},
			Params:  pMap,
			Outputs: featureExprs,
		})
		if err != nil {
			return nil, research.ComboSpec{}, err
		}
		for name, n := range exprNodes {
			outputs[pairsFeaturePrefix+name] = n
		}
	}
	if extraFn != nil {
		nodes := extraFn(FactorContext{TimeFrame: timeframe, Params: params})
		if params != nil {
			if err := params.Err(); err != nil {
				return nil, research.ComboSpec{}, err
			}
		}
		names := make([]string, 0, len(nodes))
		for name := range nodes {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if strings.TrimSpace(name) == "" {
				return nil, research.ComboSpec{}, errors.New("runner: pairs extra feature name must not be empty")
			}
			n := nodes[name]
			if n == nil {
				return nil, research.ComboSpec{}, fmt.Errorf("runner: pairs extra feature %q node is nil", name)
			}
			outKey := pairsFeaturePrefix + name
			if _, exists := outputs[outKey]; exists {
				return nil, research.ComboSpec{}, fmt.Errorf("runner: duplicate pairs feature %q", name)
			}
			outputs[outKey] = n
		}
	}
	if len(outputs) == 1 {
		return compilePairsLogPlan(fieldName, timeframe)
	}
	plan, err := factor.Compile(outputs)
	return plan, research.ComboSpec{Method: research.Equal, Columns: []string{pairsLogOutput}}, err
}

func extractPairLegFeatures(frame factor.Frame, ySID, xSID int32) (map[string]float64, map[string]float64) {
	var featY, featX map[string]float64
	for colName, col := range frame.Values {
		var name string
		switch {
		case strings.HasPrefix(colName, pairsFeaturePrefix):
			name = strings.TrimPrefix(colName, pairsFeaturePrefix)
		case colName != pairsLogOutput && colName != "score":
			name = colName
		default:
			continue
		}
		if name == "" {
			continue
		}
		if vy, ok := col[ySID]; ok && vy.Validity == factor.Valid && finitePairFloat(vy.Value) {
			if featY == nil {
				featY = make(map[string]float64, 4)
			}
			featY[name] = vy.Value
		}
		if vx, ok := col[xSID]; ok && vx.Validity == factor.Valid && finitePairFloat(vx.Value) {
			if featX == nil {
				featX = make(map[string]float64, 4)
			}
			featX[name] = vx.Value
		}
	}
	return featY, featX
}

func betaAllowed(minBeta float64, allowNegative bool, beta float64) bool {
	if !finitePairFloat(beta) {
		return false
	}
	if !allowNegative {
		return beta > math.Max(0, minBeta)
	}
	if minBeta > 0 {
		return math.Abs(beta) > minBeta
	}
	return true
}

func entryBetaAllowed(spec PairsSpec, beta float64) bool {
	return betaAllowed(spec.MinBeta, spec.AllowNegativeBeta, beta)
}

func entryHalfLifeAllowed(est *statistics.SpreadEstimator, buf *[]float64, minHL, maxHL float64) bool {
	if minHL <= 0 && maxHL <= 0 {
		return true
	}
	series, ok := est.WindowSeries((*buf)[:0])
	if !ok {
		return false
	}
	*buf = series
	ou, err := statistics.FitOU(series)
	if err != nil || !finitePairFloat(ou.DiscreteHalfLife) || ou.DiscreteHalfLife <= 0 {
		return false
	}
	if minHL > 0 && ou.DiscreteHalfLife < minHL {
		return false
	}
	if maxHL > 0 && ou.DiscreteHalfLife > maxHL {
		return false
	}
	return true
}

func checkPairEntryAllowed(
	gates pairGateConfig,
	pair Pair,
	st *pairState,
	est statistics.SpreadEstimate,
	proposed int,
	decisionTime int64,
	frame factor.Frame,
) bool {
	if !betaAllowed(gates.minBeta, gates.allowNegBeta, est.NotionalBeta) {
		return false
	}
	corrVal := math.NaN()
	if st.corr != nil {
		if c, ok := st.corr.Last(); ok {
			corrVal = c
		}
	}
	if gates.minRollingCorr > 0 && (!finitePairFloat(corrVal) || corrVal < gates.minRollingCorr) {
		return false
	}

	needSeries := gates.enableHalfLife || gates.minHurst > 0 || gates.maxHurst > 0 || gates.spreadRSIPeriod > 0 || gates.entryFilter != nil
	var series []float64
	var seriesOK bool
	if needSeries {
		series, seriesOK = st.spread.WindowSeries(st.ouBuf[:0])
		if seriesOK {
			st.ouBuf = series
		}
	}

	hlVal := math.NaN()
	if gates.enableHalfLife || gates.entryFilter != nil {
		if seriesOK && len(series) >= 4 {
			if ou, err := statistics.FitOU(series); err == nil && finitePairFloat(ou.DiscreteHalfLife) && ou.DiscreteHalfLife > 0 {
				hlVal = ou.DiscreteHalfLife
			}
		}
		if gates.enableHalfLife {
			if !finitePairFloat(hlVal) {
				return false
			}
			if gates.minHalfLife > 0 && hlVal < gates.minHalfLife {
				return false
			}
			if gates.maxHalfLife > 0 && hlVal > gates.maxHalfLife {
				return false
			}
		}
	}

	hurstVal := math.NaN()
	if gates.minHurst > 0 || gates.maxHurst > 0 || gates.entryFilter != nil {
		if seriesOK && len(series) >= 8 {
			if h, err := statistics.HurstExponent(series); err == nil && finitePairFloat(h) {
				hurstVal = h
			}
		}
		if gates.minHurst > 0 || gates.maxHurst > 0 {
			if !finitePairFloat(hurstVal) {
				return false
			}
			if gates.minHurst > 0 && hurstVal < gates.minHurst {
				return false
			}
			if gates.maxHurst > 0 && hurstVal > gates.maxHurst {
				return false
			}
		}
	}

	rsiVal := math.NaN()
	rsiPeriod := gates.spreadRSIPeriod
	if rsiPeriod == 0 && gates.entryFilter != nil && seriesOK && len(series) >= 15 {
		rsiPeriod = 14
	}
	if rsiPeriod > 0 {
		if seriesOK && len(series) >= rsiPeriod+1 {
			if r, err := statistics.WilderRSI(series, rsiPeriod); err == nil && finitePairFloat(r) {
				rsiVal = r
			}
		}
		if gates.spreadRSIPeriod > 0 {
			if !finitePairFloat(rsiVal) {
				return false
			}
			if gates.band.Mode == statistics.BandModeMomentum {
				if proposed > 0 && gates.spreadRSIUpper > 0 && rsiVal < gates.spreadRSIUpper {
					return false
				}
				if proposed < 0 && gates.spreadRSILower > 0 && rsiVal > gates.spreadRSILower {
					return false
				}
			} else {
				if proposed < 0 && gates.spreadRSIUpper > 0 && rsiVal < gates.spreadRSIUpper {
					return false
				}
				if proposed > 0 && gates.spreadRSILower > 0 && rsiVal > gates.spreadRSILower {
					return false
				}
			}
		}
	}

	if gates.entryFilter != nil {
		featY, featX := extractPairLegFeatures(frame, st.y, st.x)
		ctx := PairFilterContext{
			DecisionTime:      decisionTime,
			Pair:              pair,
			ProposedSide:      proposed,
			Z:                 est.Z,
			Beta:              est.NotionalBeta,
			HalfLife:          hlVal,
			RollingCorr:       corrVal,
			Hurst:             hurstVal,
			SpreadRSI:         rsiVal,
			CopulaProbYGivenX: est.CopulaProbYGivenX,
			CopulaProbXGivenY: est.CopulaProbXGivenY,
			FeaturesY:         featY,
			FeaturesX:         featX,
		}
		if !gates.entryFilter(ctx) {
			return false
		}
	}
	return true
}

// allocateInverseVolGross distributes totalGross across open pairs inversely proportional
// to each pair's spread residual Std, applying water-filling caps at maxPairGross while
// keeping total gross strictly within 1.
func allocateInverseVolGross(states []*pairState, totalGross, maxPairGross float64) []float64 {
	out := make([]float64, len(states))
	active := make([]int, 0, len(states))
	inv := make([]float64, len(states))
	for i, st := range states {
		if st.pos == 0 {
			continue
		}
		if st.lastStd > 1e-12 && finitePairFloat(st.lastStd) {
			inv[i] = 1 / st.lastStd
			active = append(active, i)
		}
	}
	remaining := math.Min(1.0, totalGross)
	capPerPair := math.Min(1.0, maxPairGross)
	if len(active) == 0 || remaining <= 0 || capPerPair <= 0 {
		return out
	}
	capped := make([]bool, len(states))
	for remaining > 1e-15 {
		sumInv := 0.0
		for _, idx := range active {
			if !capped[idx] {
				sumInv += inv[idx]
			}
		}
		if sumInv <= 0 {
			break
		}
		iterRem := remaining
		clipped := false
		for _, idx := range active {
			if capped[idx] {
				continue
			}
			if iterRem*(inv[idx]/sumInv) > capPerPair+1e-15 {
				out[idx] = capPerPair
				remaining -= capPerPair
				capped[idx] = true
				clipped = true
			}
		}
		if !clipped {
			for _, idx := range active {
				if !capped[idx] {
					out[idx] = remaining * (inv[idx] / sumInv)
				}
			}
			break
		}
	}
	return out
}

type pairState struct {
	y, x int32
	// unavailable is the leg (symbol) that has no data at all inside this run's window and was left out of its
	// universe by the engine (BtFeatureLevel 11); the pair is skipped for the whole run: never traded, no spread.
	unavailable    string
	spread         *statistics.SpreadEstimator
	trader         *statistics.BandTrader
	copulaTrader   *statistics.CopulaTrader
	corr           *statistics.RollingReturnCorr
	beta           float64 // notional hedge ratio (-wX / wY) frozen at entry (or updated under DynamicHedge)
	modelBeta      float64 // model-coordinate slope frozen at entry for EvalFrozenZ
	lastStd        float64 // latest valid dimensionless spread residual Std
	ouBuf          []float64
	pos            int
	prevPos        int
	curLY, curLX   float64
	curOK          bool
	holdWY, holdWX float64
	holdLY, holdLX float64
	hasHold        bool
	stats          pairStats // result bookkeeping only; never read by the trading logic
}

func newPairState(y, x int32, gates pairGateConfig, kalmanDelta, kalmanVe float64) (pairState, error) {
	rs, err := statistics.NewSpreadEstimator(statistics.SpreadModelConfig{
		Model:       gates.spreadModel,
		Window:      gates.window,
		KalmanDelta: kalmanDelta,
		KalmanVe:    kalmanVe,
		Copula:      gates.copula,
	})
	if err != nil {
		return pairState{}, err
	}
	bt, err := statistics.NewBandTrader(gates.band)
	if err != nil {
		return pairState{}, err
	}
	var ct *statistics.CopulaTrader
	if gates.spreadModel == statistics.SpreadModelCopula {
		ct, err = statistics.NewCopulaTrader(gates.copula, gates.band)
		if err != nil {
			return pairState{}, err
		}
	}
	var corr *statistics.RollingReturnCorr
	if gates.needsCorrTracker() {
		corr, err = statistics.NewRollingReturnCorr(gates.effectiveCorrWindow())
		if err != nil {
			return pairState{}, err
		}
	}
	return pairState{
		y:            y,
		x:            x,
		spread:       rs,
		trader:       bt,
		copulaTrader: ct,
		corr:         corr,
	}, nil
}

func (st *pairState) resetTrader() {
	if st.trader != nil {
		st.trader.Reset()
	}
	if st.copulaTrader != nil {
		st.copulaTrader.Reset()
	}
}

func (st *pairState) previewFlat(est statistics.SpreadEstimate) int {
	if st.copulaTrader != nil {
		return st.copulaTrader.Preview(est.CopulaProbYGivenX, est.CopulaProbXGivenY)
	}
	return st.trader.Preview(est.Z)
}

func (st *pairState) stepFlat(est statistics.SpreadEstimate) int {
	if st.copulaTrader != nil {
		return st.copulaTrader.Step(est.CopulaProbYGivenX, est.CopulaProbXGivenY)
	}
	return st.trader.Step(est.Z)
}

func (st *pairState) stepHeld(stepZ float64, est statistics.SpreadEstimate, band statistics.BandConfig) (int, string) {
	if st.copulaTrader != nil {
		p := st.copulaTrader.Step(est.CopulaProbYGivenX, est.CopulaProbXGivenY)
		return p, st.copulaTrader.LastCloseReason()
	}
	prev := st.pos
	p := st.trader.Step(stepZ)
	return p, band.CloseReason(prev, stepZ)
}

func newPairsBuilder(spec PairsSpec, sidMap map[int32]string) (PortfolioBuilder, error) {
	b, _, err := newTrackedPairsBuilder(spec, sidMap)
	return b, err
}

// newTrackedPairsBuilder is newPairsBuilder plus the tracker that reports per-pair results after the run. The
// tracker is write-only state of the builder: weights are computed exactly as without it.
func newTrackedPairsBuilder(spec PairsSpec, sidMap map[int32]string) (PortfolioBuilder, *pairsTracker, error) {
	return newTrackedPairsBuilderSkipping(spec, sidMap, nil)
}

func spotShortsLeg(longOnlySpot bool, spotSIDs map[int32]bool, ySID, xSID int32, pos int, notionalBeta float64) bool {
	if !longOnlySpot || len(spotSIDs) == 0 || pos == 0 {
		return false
	}
	wy, wx := float64(pos), -float64(pos)*notionalBeta
	return (spotSIDs[ySID] && wy < 0) || (spotSIDs[xSID] && wx < 0)
}

// newTrackedPairsBuilderSkipping is newTrackedPairsBuilder for a run whose universe may lack legs the engine left out
// because they have no stored data inside the run window (unavailable: symbol -> reason). Such a pair was chosen on
// information from before the window (point in time), so it is not an error and not removed from the strategy: it is
// skipped for this run and the other pairs trade as usual. A pair with a leg that is neither in the universe nor in
// unavailable is still an error. Every pair keeps its Gross share: skipped pairs leave their exposure idle, the
// remaining pairs are never scaled up (no hidden leverage when pairs drop out).
func newTrackedPairsBuilderSkipping(spec PairsSpec, sidMap map[int32]string, unavailable map[string]string) (PortfolioBuilder, *pairsTracker, error) {
	return newTrackedPairsBuilderWithSpots(spec, sidMap, unavailable, nil)
}

func newTrackedPairsBuilderWithSpots(spec PairsSpec, sidMap map[int32]string, unavailable map[string]string, spotSIDs map[int32]bool) (PortfolioBuilder, *pairsTracker, error) {
	var err error
	spec, err = normalizePairsSpec(spec)
	if err != nil {
		return nil, nil, err
	}
	gates := spec.gateConfig()
	sids := make(map[string]int32, len(sidMap))
	for sid, symbol := range sidMap {
		sids[symbol] = sid
	}
	states := make([]*pairState, len(spec.Pairs))
	for i, pair := range spec.Pairs {
		y, okY := sids[pair.Y]
		x, okX := sids[pair.X]
		if !okY || !okX {
			var missing []string
			if !okY {
				missing = append(missing, pair.Y)
			}
			if !okX {
				missing = append(missing, pair.X)
			}
			for _, sym := range missing {
				if _, gone := unavailable[sym]; !gone {
					return nil, nil, fmt.Errorf("runner: pair %s/%s has a symbol missing from this run's universe", pair.Y, pair.X)
				}
			}
			states[i] = &pairState{y: -1, x: -1, unavailable: strings.Join(missing, ",")}
			continue
		}
		ps, err := newPairState(y, x, gates, spec.KalmanDelta, spec.KalmanVe)
		if err != nil {
			return nil, nil, err
		}
		states[i] = &ps
	}
	tracker := &pairsTracker{states: states, names: spec.Pairs}
	tracker.resetStats()
	availCount := 0
	for _, st := range states {
		if st.unavailable == "" {
			availCount++
		}
	}
	allUnavailable := availCount == 0
	var prevDecisionNAV float64
	build := func(frame factor.Frame, universe factor.Universe, target factor.PortfolioSpec, _ research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error) {
		tradable := make(map[int32]bool, len(universe.Tradable))
		for _, sid := range universe.Tradable {
			tradable[sid] = true
		}
		values := frame.Values[pairsLogOutput]
		var diags []factor.Diagnostic
		ready := false
		navPrev := tracker.lastNAV
		navNow := target.Budget.NAV
		tracker.lastNAV = navNow
		tracker.lastMS = frame.DecisionTime
		if allUnavailable {
			// No pair can trade in this run: an explicit flat target at every decision (equity stays at the initial value).
			target.Diagnostics = []factor.Diagnostic{{Code: "pairs-all-unavailable", Detail: "every pair has a leg without data in the run window"}}
			p, err := factor.NewTargetPortfolio(target, map[int32]float64{})
			return p, nil, err
		}
		for pairIdx, st := range states {
			st.prevPos = st.pos
			st.curOK = false
			if st.unavailable != "" {
				continue
			}
			ly, okY := values[st.y]
			lx, okX := values[st.x]
			if !okY || !okX || ly.Validity != factor.Valid || lx.Validity != factor.Valid {
				if st.corr != nil {
					st.corr.ResetPrev()
				}
				diags = append(diags, factor.Diagnostic{Code: "pair-input-missing", Detail: fmt.Sprintf("%d/%d", st.y, st.x)})
				st.stats.paused++
				st.stats.prevOK = false
				if st.pos != 0 {
					st.stats.closeEpisode(frame.DecisionTime, 0, "leg_missing")
					// 067: a leg has no data this period (PIT universe drop / delisting): close the pair and pause it;
					// it may reopen when both legs are back
					st.pos, st.beta, st.modelBeta = 0, 0, 0
					st.resetTrader()
					ready = true // the closed pair must reach the ledger even if no other pair is ready
					diags = append(diags, factor.Diagnostic{Code: "pair-closed-missing-leg", Detail: fmt.Sprintf("%d/%d", st.y, st.x)})
				}
				continue
			}
			st.curLY, st.curLX, st.curOK = ly.Value, lx.Value, true
			st.stats.accrue(ly.Value, lx.Value, navPrev)
			if st.corr != nil {
				st.corr.ObserveLog(lx.Value, ly.Value)
			}
			est, ok := st.spread.ObserveLog(lx.Value, ly.Value)
			if !ok {
				diags = append(diags, factor.Diagnostic{Code: "pair-warmup", Detail: fmt.Sprintf("%d/%d", st.y, st.x)})
				continue
			}
			ready = true
			st.lastStd = est.ScaleStd
			st.stats.bars++
			if !tradable[st.y] || !tradable[st.x] {
				// Never open on an untradable leg; an existing position keeps its state
				// but the target below drops to flat only through the band rules.
				if st.pos == 0 {
					continue
				}
			}
			prev := st.pos
			if prev == 0 {
				proposed := st.previewFlat(est)
				if proposed == 0 {
					st.pos = st.stepFlat(est)
					continue
				}
				if spotShortsLeg(spec.LongOnlySpot, spotSIDs, st.y, st.x, proposed, est.NotionalBeta) {
					diags = append(diags, factor.Diagnostic{Code: "pair-spot-short-blocked", Detail: fmt.Sprintf("%d/%d", st.y, st.x)})
					continue
				}
				if !checkPairEntryAllowed(gates, spec.Pairs[pairIdx], st, est, proposed, frame.DecisionTime, frame) {
					continue
				}
				st.pos = st.stepFlat(est)
				if st.pos != 0 {
					st.beta, st.modelBeta = est.NotionalBeta, est.Beta
				}
				st.stats.observeStep(prev, st.pos, est.Z, st.beta, frame.DecisionTime, spec.Band)
			} else {
				stepZ := est.Z
				if !spec.DynamicExitZ && !spec.DynamicHedge {
					if fz, okZ := st.spread.EvalFrozenZ(lx.Value, ly.Value, st.modelBeta); okZ {
						stepZ = fz
					} else {
						stepZ = math.NaN()
					}
				}
				var closeReason string
				st.pos, closeReason = st.stepHeld(stepZ, est, spec.Band)
				if st.pos != 0 && st.pos != prev {
					if spotShortsLeg(spec.LongOnlySpot, spotSIDs, st.y, st.x, st.pos, est.NotionalBeta) {
						diags = append(diags, factor.Diagnostic{Code: "pair-spot-short-blocked", Detail: fmt.Sprintf("%d/%d", st.y, st.x)})
						st.pos, st.beta, st.modelBeta = 0, 0, 0
						st.resetTrader()
					} else if !checkPairEntryAllowed(gates, spec.Pairs[pairIdx], st, est, st.pos, frame.DecisionTime, frame) {
						st.pos, st.beta, st.modelBeta = 0, 0, 0
						st.resetTrader()
					} else {
						st.beta, st.modelBeta = est.NotionalBeta, est.Beta
					}
				} else if st.pos != 0 && spec.DynamicHedge {
					if entryBetaAllowed(spec, est.NotionalBeta) && !spotShortsLeg(spec.LongOnlySpot, spotSIDs, st.y, st.x, st.pos, est.NotionalBeta) {
						st.beta, st.modelBeta = est.NotionalBeta, est.Beta
					}
				} else if st.pos == 0 {
					st.beta, st.modelBeta = 0, 0
				}
				st.stats.observeStepWithReason(prev, st.pos, stepZ, st.beta, frame.DecisionTime, closeReason)
			}
		}
		if !ready {
			return nil, append(diags, factor.Diagnostic{Code: "pairs-not-ready", Detail: "no pair has a full window; keep previous portfolio"}), nil
		}
		var pairGross []float64
		if spec.WeightMode == "inverse_vol" {
			totalGross := math.Min(1.0, spec.Gross*float64(availCount))
			capPerPair := spec.MaxPairGross
			if capPerPair == 0 {
				capPerPair = math.Min(totalGross, 2*spec.Gross)
			}
			pairGross = allocateInverseVolGross(states, totalGross, capPerPair)
		}
		wyList := make([]float64, len(states))
		wxList := make([]float64, len(states))
		idealWY := make([]float64, len(states))
		idealWX := make([]float64, len(states))
		preWY := make([]float64, len(states))
		preWX := make([]float64, len(states))
		buffered := make([]bool, len(states))
		for i, st := range states {
			if st.hasHold && st.curOK && prevDecisionNAV > 0 && navNow > 0 {
				ry := math.Exp(st.curLY - st.holdLY)
				rx := math.Exp(st.curLX - st.holdLX)
				if finitePairFloat(ry) && finitePairFloat(rx) {
					preWY[i] = st.holdWY * (prevDecisionNAV / navNow) * ry
					preWX[i] = st.holdWX * (prevDecisionNAV / navNow) * rx
				} else {
					preWY[i], preWX[i] = st.holdWY, st.holdWX
				}
			} else if st.hasHold {
				preWY[i], preWX[i] = st.holdWY, st.holdWX
			}
			if st.pos == 0 {
				continue
			}
			g := spec.Gross
			if pairGross != nil {
				g = pairGross[i]
			}
			norm := 1 + math.Abs(st.beta)
			s := float64(st.pos) * g / norm
			iwY, iwX := s, -s*st.beta
			idealWY[i], idealWX[i] = iwY, iwX
			wy, wx := iwY, iwX
			if spec.DriftBand > 0 && st.prevPos == st.pos && st.hasHold && st.curOK {
				if math.Abs(iwY-preWY[i])+math.Abs(iwX-preWX[i]) < spec.DriftBand {
					wy, wx = preWY[i], preWX[i]
					buffered[i] = true
				}
			}
			wyList[i], wxList[i] = wy, wx
		}
		totalG := 0.0
		for i := range states {
			totalG += math.Abs(wyList[i]) + math.Abs(wxList[i])
		}
		if totalG > 1+1e-12 {
			for i := range states {
				if buffered[i] && math.Abs(wyList[i])+math.Abs(wxList[i]) > math.Abs(idealWY[i])+math.Abs(idealWX[i]) {
					wyList[i], wxList[i] = idealWY[i], idealWX[i]
					buffered[i] = false
				}
			}
		}
		weights := map[int32]float64{}
		for i, st := range states {
			wy, wx := wyList[i], wxList[i]
			if wy != 0 || wx != 0 {
				weights[st.y] += wy
				weights[st.x] += wx
			}
			if spec.DriftBand > 0 {
				st.stats.setWeightsFrom(wy, wx, preWY[i], preWX[i])
			} else {
				st.stats.setWeights(wy, wx)
			}
			if st.pos != 0 && st.curOK {
				st.holdWY, st.holdWX = wy, wx
				st.holdLY, st.holdLX = st.curLY, st.curLX
				st.hasHold = true
			} else if st.pos == 0 {
				st.holdWY, st.holdWX, st.hasHold = 0, 0, false
			}
		}
		prevDecisionNAV = navNow
		for sid, w := range weights {
			if w == 0 {
				delete(weights, sid)
			}
		}
		target.Diagnostics = slices.Clone(diags)
		p, err := factor.NewTargetPortfolio(target, weights)
		return p, nil, err
	}
	return build, tracker, nil
}

// pairsLegResolvers maps a registered pairs portfolio builder name to its spec constructor, so the runner can
// derive the pair legs of a run (070: per-pair PIT).
var pairsLegResolvers sync.Map

// registeredPairLegs returns the (Y, X) SIDs of the run's pairs strategy, nil when the run is not a pairs one.
func registeredPairLegs(c Config) [][2]int32 {
	name := c.Manifest.Portfolio.Builder
	if name == "" {
		name = boundDefinitionBuilder(c.Definition)
	}
	v, ok := pairsLegResolvers.Load(name)
	if !ok {
		return nil
	}
	build, _ := v.(func(*Params) (PairsSpec, error))
	if build == nil {
		return nil
	}
	spec, err := resolvePairsSpec(build, c)
	if err != nil {
		return nil
	}
	sids := make(map[string]int32, len(c.Snapshot.SIDMap))
	for sid, symbol := range c.Snapshot.SIDMap {
		sids[symbol] = sid
	}
	var out [][2]int32
	for _, p := range spec.Pairs {
		y, okY := sids[p.Y]
		x, okX := sids[p.X]
		if okY && okX {
			out = append(out, [2]int32{y, x})
		}
	}
	return out
}
