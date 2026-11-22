package runner

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
)

// Registration bundles a factor-plan DefinitionBuilder with its per-run
// PortfolioFactory and parameter PortfolioValidator so callers can inspect or
// unit-test a compiled statistical-arbitrage assembly before installing it in
// the process-global registry.
type Registration struct {
	Name        string
	BuilderName string
	Definition  DefinitionBuilder
	Factory     PortfolioFactory
	Validator   PortfolioValidator
}

// Register installs the definition, portfolio factory, and default builder
// binding in the global runner registry.
func (r Registration) Register() error {
	if r.Name == "" || r.BuilderName == "" || r.Definition == nil || r.Factory == nil {
		return errors.New("runner: incomplete strategy registration")
	}
	if err := RegisterDefinition(r.Name, r.Definition); err != nil {
		return err
	}
	var err error
	if r.Validator != nil {
		err = RegisterPortfolioFactory(r.BuilderName, r.Factory, r.Validator)
	} else {
		err = RegisterPortfolioFactory(r.BuilderName, r.Factory)
	}
	if err != nil {
		return err
	}
	bindDefinitionBuilder(r.Name, r.BuilderName)
	return nil
}

// BasketDef defines one multi-leg cointegrated basket of 2..12 distinct symbols.
type BasketDef struct {
	Name    string
	Symbols []string  // 2..12 distinct symbols as they appear in SIDMap
	Weights []float64 // optional explicit weights for WeightMethod "fixed"
}

// BasketsSpec configures a multi-leg cointegrated basket statistical-arbitrage strategy.
type BasketsSpec struct {
	Baskets []BasketDef
	Field   string // kline field, default "close"
	Window  int    // rolling estimation window in decision bars

	// WeightMethod selects how basket spread coefficients are obtained:
	// - "" (default): "fixed" per basket when BasketDef.Weights is non-empty, otherwise "johansen"
	// - "fixed": explicit BasketDef.Weights normalized so sum_i |w_i| == 1 per unit basket gross
	// - "johansen": rolling Johansen VECM (Intercept: true, DifferenceLags), first eigenvector sign-stabilized so beta[0] > 0
	// - "multivariate_ols": regress leg 0 on [1, leg_1, ..., leg_{k-1}] via factor.LeastSquares (with ridge fallback if singular)
	WeightMethod string
	// UseRawPrice fits Johansen / multivariate OLS on raw prices instead of log prices,
	// converting unit coefficients to dollar-notional weights via beta_i * price_i.
	UseRawPrice bool
	// DifferenceLags is the Johansen VECM difference lag order (>= 0, default 0).
	DifferenceLags int
	// RequireCointegration gates new entries under "johansen" using JohansenResult.IsCointegrated.
	RequireCointegration bool
	// Confidence is the Johansen cointegration test confidence in {0.90, 0.95, 0.99}; default 0.95 when 0.
	Confidence float64
	// MinHalfLife and MaxHalfLife optionally gate new entries using statistics.FitOU DiscreteHalfLife (in bars).
	MinHalfLife float64
	MaxHalfLife float64

	// Band configures z-score entry/exit/stop/max-hold hysteresis ("reversion" or "momentum").
	Band statistics.BandConfig
	// MaxHoldBars optionally sets Band.MaxHold when Band.MaxHold == 0 (>= 0).
	MaxHoldBars int
	// DynamicWeights updates an open basket's unit weights and exit Z continuously on each bar;
	// by default (false), entry unit weights and spread vector are frozen while holding so exit Z matches held PnL.
	DynamicWeights bool
	// DriftBand buffers minor share-drift rebalances while holding in the same direction (>= 0).
	DriftBand float64
	// WeightMode selects cross-basket sizing: "" or "equal" (default), or "inverse_vol".
	WeightMode string
	// GrossPerBasket is the target gross notional of one open basket as a fraction of NAV;
	// 0 defaults to MaxGross / len(Baskets).
	GrossPerBasket float64
	// MaxGross caps aggregate portfolio gross exposure across all open baskets in (0, 1]; 0 defaults to 1.0.
	MaxGross float64
}

// BuildBasketsRegistration validates name and spec and builds a reusable Registration
// without mutating the global runner registry.
func BuildBasketsRegistration(name string, spec BasketsSpec) (Registration, error) {
	if name == "" {
		return Registration{}, errors.New("runner: baskets strategy needs non-empty name")
	}
	norm, err := normalizeBasketsSpec(spec)
	if err != nil {
		return Registration{}, err
	}
	builderName := name + "-baskets-v1"
	return Registration{
		Name:        name,
		BuilderName: builderName,
		Definition: func(c Config) (*factor.Plan, research.ComboSpec, error) {
			return compilePairsLogPlan(norm.Field, c.Factor.TimeFrame)
		},
		Factory: func(c Config) (PortfolioBuilder, error) {
			builder, tracker, err := newTrackedBasketsBuilder(norm, c.Snapshot.SIDMap, c.UnavailableSymbols)
			if err == nil && c.observe != nil {
				c.observe.pairs, c.observe.reset = tracker.report, tracker.resetStats
			}
			return builder, err
		},
		Validator: func(_ Config) error {
			return nil
		},
	}, nil
}

// RegisterBaskets validates and registers a multi-leg cointegrated basket strategy
// under definition `name` and bound portfolio factory `<name>-baskets-v1`.
func RegisterBaskets(name string, spec BasketsSpec) error {
	reg, err := BuildBasketsRegistration(name, spec)
	if err != nil {
		return err
	}
	return reg.Register()
}

func resolveBasketsSpec(build func(*Params) (BasketsSpec, error), c Config) (BasketsSpec, error) {
	params := c.Params()
	spec, err := build(params)
	if err != nil {
		return BasketsSpec{}, err
	}
	if err = params.Err(); err != nil {
		return BasketsSpec{}, err
	}
	return normalizeBasketsSpec(spec)
}

// BuildBasketsParamsRegistration builds a parameterized multi-leg cointegrated basket
// Registration whose BasketsSpec is resolved per run from Config.Params().
func BuildBasketsParamsRegistration(name string, build func(*Params) (BasketsSpec, error)) (Registration, error) {
	if name == "" || build == nil {
		return Registration{}, errors.New("runner: baskets strategy needs name and build function")
	}
	if _, err := resolveBasketsSpec(build, Config{}); err != nil {
		return Registration{}, err
	}
	builderName := name + "-baskets-v1"
	return Registration{
		Name:        name,
		BuilderName: builderName,
		Definition: func(c Config) (*factor.Plan, research.ComboSpec, error) {
			norm, err := resolveBasketsSpec(build, c)
			if err != nil {
				return nil, research.ComboSpec{}, err
			}
			return compilePairsLogPlan(norm.Field, c.Factor.TimeFrame)
		},
		Factory: func(c Config) (PortfolioBuilder, error) {
			norm, err := resolveBasketsSpec(build, c)
			if err != nil {
				return nil, err
			}
			builder, tracker, err := newTrackedBasketsBuilder(norm, c.Snapshot.SIDMap, c.UnavailableSymbols)
			if err == nil && c.observe != nil {
				c.observe.pairs, c.observe.reset = tracker.report, tracker.resetStats
			}
			return builder, err
		},
		Validator: func(c Config) error {
			_, err := resolveBasketsSpec(build, c)
			return err
		},
	}, nil
}

// RegisterBasketsParams validates default parameters and registers a parameterized
// multi-leg cointegrated basket strategy under `<name>` and `<name>-baskets-v1`.
func RegisterBasketsParams(name string, build func(*Params) (BasketsSpec, error)) error {
	reg, err := BuildBasketsParamsRegistration(name, build)
	if err != nil {
		return err
	}
	return reg.Register()
}

func normalizeBasketsSpec(spec BasketsSpec) (BasketsSpec, error) {
	if spec.Field == "" {
		spec.Field = "close"
	}
	if len(spec.Baskets) == 0 {
		return spec, errors.New("runner: baskets strategy declares no baskets")
	}
	switch spec.WeightMethod {
	case "", "fixed", "johansen", "multivariate_ols":
	default:
		return spec, fmt.Errorf("runner: unknown baskets weight method %q", spec.WeightMethod)
	}
	if spec.DifferenceLags < 0 || spec.DifferenceLags > 128 {
		return spec, errors.New("runner: baskets difference lags must be in [0, 128]")
	}
	if spec.Confidence == 0 {
		spec.Confidence = 0.95
	}
	if _, err := statistics.JohansenTraceCriticalValues(2, statistics.JohansenSpec{Intercept: true, DifferenceLags: spec.DifferenceLags}, spec.Confidence); err != nil {
		return spec, err
	}
	if spec.MaxHoldBars < 0 {
		return spec, errors.New("runner: baskets max hold bars must not be negative")
	}
	if spec.Band.MaxHold == 0 && spec.MaxHoldBars > 0 {
		spec.Band.MaxHold = spec.MaxHoldBars
	}
	if err := spec.Band.Validate(); err != nil {
		return spec, err
	}
	if !finitePairFloat(spec.MinHalfLife) || spec.MinHalfLife < 0 {
		return spec, errors.New("runner: baskets min half-life must be finite and non-negative")
	}
	if !finitePairFloat(spec.MaxHalfLife) || spec.MaxHalfLife < 0 || (spec.MaxHalfLife > 0 && spec.MaxHalfLife < spec.MinHalfLife) {
		return spec, errors.New("runner: baskets max half-life must be finite, non-negative, and >= min half-life")
	}
	if !finitePairFloat(spec.DriftBand) || spec.DriftBand < 0 {
		return spec, errors.New("runner: baskets drift band must be finite and non-negative")
	}
	switch spec.WeightMode {
	case "", "equal":
		spec.WeightMode = "equal"
	case "inverse_vol":
	default:
		return spec, fmt.Errorf("runner: unknown baskets weight mode %q", spec.WeightMode)
	}
	if spec.MaxGross == 0 {
		spec.MaxGross = 1.0
	}
	if !finitePairFloat(spec.MaxGross) || spec.MaxGross <= 0 || spec.MaxGross > 1+1e-9 {
		return spec, errors.New("runner: baskets max gross must be in (0, 1]")
	}
	if spec.GrossPerBasket == 0 {
		spec.GrossPerBasket = spec.MaxGross / float64(len(spec.Baskets))
	}
	if !finitePairFloat(spec.GrossPerBasket) || spec.GrossPerBasket <= 0 || spec.GrossPerBasket > spec.MaxGross+1e-9 {
		return spec, errors.New("runner: baskets gross per basket must be in (0, MaxGross]")
	}

	minWindow := 3
	if spec.MinHalfLife > 0 || spec.MaxHalfLife > 0 {
		minWindow = 4
	}
	cloned := make([]BasketDef, len(spec.Baskets))
	for i, b := range spec.Baskets {
		if len(b.Symbols) < 2 || len(b.Symbols) > 12 {
			return spec, fmt.Errorf("runner: basket %d must contain 2..12 symbols, got %d", i, len(b.Symbols))
		}
		seen := make(map[string]bool, len(b.Symbols))
		syms := make([]string, len(b.Symbols))
		for j, s := range b.Symbols {
			if s == "" || seen[s] {
				return spec, fmt.Errorf("runner: basket %d requires non-empty distinct symbols", i)
			}
			seen[s] = true
			syms[j] = s
		}
		method := basketEffectiveMethod(spec.WeightMethod, b.Weights)
		var wCopy []float64
		switch method {
		case "fixed":
			if len(b.Weights) != len(syms) {
				return spec, fmt.Errorf("runner: basket %d fixed weights length %d does not match symbols length %d", i, len(b.Weights), len(syms))
			}
			wCopy = make([]float64, len(b.Weights))
			absSum := 0.0
			for j, w := range b.Weights {
				if !finitePairFloat(w) {
					return spec, fmt.Errorf("runner: basket %d has nonfinite fixed weight", i)
				}
				absSum += math.Abs(w)
				wCopy[j] = w
			}
			if absSum <= 0 || !finitePairFloat(absSum) {
				return spec, fmt.Errorf("runner: basket %d fixed weights must have positive L1 norm", i)
			}
			for j := range wCopy {
				wCopy[j] /= absSum
			}
		case "johansen":
			d := len(syms)
			need := d*(spec.DifferenceLags+1) + 3
			if need > minWindow {
				minWindow = need
			}
		case "multivariate_ols":
			need := len(syms) + 2
			if need > minWindow {
				minWindow = need
			}
		}
		name := b.Name
		if name == "" {
			name = strings.Join(syms, "/")
		}
		cloned[i] = BasketDef{Name: name, Symbols: syms, Weights: wCopy}
	}
	if spec.Window < minWindow {
		return spec, fmt.Errorf("runner: baskets window %d is smaller than required minimum %d", spec.Window, minWindow)
	}
	spec.Baskets = cloned
	return spec, nil
}

func basketEffectiveMethod(globalMethod string, weights []float64) string {
	if globalMethod != "" {
		return globalMethod
	}
	if len(weights) > 0 {
		return "fixed"
	}
	return "johansen"
}

// basketStats generalizes pairStats to K >= 2 legs while producing exact PairResult reconciliation.
type basketStats struct {
	core   pairStats
	prevLP []float64
	w      []float64
}

func (s *basketStats) init(legs, epCap int) {
	s.core = pairStats{episodeCap: epCap}
	s.prevLP = make([]float64, legs)
	s.w = make([]float64, legs)
}

func (s *basketStats) accrue(curLP []float64, navPrev float64) {
	if s.core.prevOK && navPrev > 0 {
		hasW := false
		for _, w := range s.w {
			if w != 0 {
				hasW = true
				break
			}
		}
		if hasW {
			var ret float64
			for k, w := range s.w {
				if w != 0 {
					ret += w * math.Expm1(curLP[k]-s.prevLP[k])
				}
			}
			pnl := ret * navPrev
			if finitePairFloat(pnl) {
				s.core.pnl += pnl
				if s.core.open != nil {
					s.core.open.PnL += pnl
				}
			}
		}
	}
	copy(s.prevLP, curLP)
	s.core.prevOK = true
}

func (s *basketStats) setWeights(w, ref []float64) {
	var turn float64
	for k := range w {
		turn += math.Abs(w[k] - ref[k])
	}
	s.core.turnover += turn
	copy(s.w, w)
}

type basketState struct {
	def               BasketDef
	method            string
	sids              []int32
	unavailable       string
	ring              [][]float64 // Window x K coordinate levels
	ringHead          int
	ringCount         int
	trader            *statistics.BandTrader
	frozenModelBeta   []float64 // frozen spread coefficients in model coordinates
	frozenUnitWeights []float64 // frozen unit-gross notional leg weights (sum |u_k| == 1)
	lastScaleStd      float64
	pos               int
	prevPos           int
	curLP             []float64
	curOK             bool
	holdW             []float64
	holdLP            []float64
	hasHold           bool
	spreadBuf         []float64
	stats             basketStats
}

type basketsTracker struct {
	states  []*basketState
	labels  []Pair
	lastNAV float64
	lastMS  int64
}

func (t *basketsTracker) resetStats() {
	epCap := pairEpisodeCap(len(t.states))
	for _, st := range t.states {
		st.stats.init(len(st.def.Symbols), epCap)
	}
	t.lastNAV, t.lastMS = 0, 0
}

func (t *basketsTracker) report(initialNAV float64) []PairResult {
	out := make([]PairResult, len(t.states))
	for i, st := range t.states {
		out[i] = buildPairResult(t.labels[i], &st.stats.core, st.unavailable, st.stats.core.episodeCap, t.lastMS, initialNAV)
	}
	return out
}

func newTrackedBasketsBuilder(spec BasketsSpec, sidMap map[int32]string, unavailable map[string]string) (PortfolioBuilder, *basketsTracker, error) {
	var err error
	spec, err = normalizeBasketsSpec(spec)
	if err != nil {
		return nil, nil, err
	}
	symToSID := make(map[string]int32, len(sidMap))
	for sid, sym := range sidMap {
		if prev, exists := symToSID[sym]; !exists || sid < prev {
			symToSID[sym] = sid
		}
	}

	states := make([]*basketState, len(spec.Baskets))
	labels := make([]Pair, len(spec.Baskets))
	for i, b := range spec.Baskets {
		labels[i] = Pair{Y: b.Name, X: strings.Join(b.Symbols[1:], "+")}
		k := len(b.Symbols)
		sids := make([]int32, k)
		var missing []string
		for j, sym := range b.Symbols {
			sid, ok := symToSID[sym]
			if !ok {
				missing = append(missing, sym)
				continue
			}
			sids[j] = sid
		}
		if len(missing) > 0 {
			for _, sym := range missing {
				if _, gone := unavailable[sym]; !gone {
					return nil, nil, fmt.Errorf("runner: basket %q symbol %q is missing from this run's universe", b.Name, sym)
				}
			}
			st := &basketState{def: b, unavailable: strings.Join(missing, ",")}
			states[i] = st
			continue
		}
		bt, err := statistics.NewBandTrader(spec.Band)
		if err != nil {
			return nil, nil, err
		}
		ring := make([][]float64, spec.Window)
		for w := range ring {
			ring[w] = make([]float64, k)
		}
		states[i] = &basketState{
			def:               b,
			method:            basketEffectiveMethod(spec.WeightMethod, b.Weights),
			sids:              sids,
			ring:              ring,
			trader:            bt,
			frozenModelBeta:   make([]float64, k),
			frozenUnitWeights: make([]float64, k),
			curLP:             make([]float64, k),
			holdW:             make([]float64, k),
			holdLP:            make([]float64, k),
			spreadBuf:         make([]float64, 0, spec.Window),
		}
	}

	tracker := &basketsTracker{states: states, labels: labels}
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
		navPrev := tracker.lastNAV
		navNow := target.Budget.NAV
		tracker.lastNAV = navNow
		tracker.lastMS = frame.DecisionTime

		if allUnavailable {
			target.Diagnostics = []factor.Diagnostic{{Code: "baskets-all-unavailable", Detail: "every basket has a leg without data in the run window"}}
			p, err := factor.NewTargetPortfolio(target, map[int32]float64{})
			return p, nil, err
		}

		tradable := make(map[int32]bool, len(universe.Tradable))
		for _, sid := range universe.Tradable {
			tradable[sid] = true
		}
		values := frame.Values[pairsLogOutput]
		var diags []factor.Diagnostic
		ready := false

		levelsWork := make([][]float64, spec.Window)
		for _, st := range states {
			st.prevPos = st.pos
			st.curOK = false
			if st.unavailable != "" {
				continue
			}
			k := len(st.sids)
			allValid := true
			allTradable := true
			for j, sid := range st.sids {
				v, ok := values[sid]
				if !ok || v.Validity != factor.Valid || !finitePairFloat(v.Value) {
					allValid = false
					break
				}
				if spec.UseRawPrice {
					px := math.Exp(v.Value)
					if !finitePairFloat(px) || px <= 0 {
						allValid = false
						break
					}
				}
				st.curLP[j] = v.Value
				if !tradable[sid] {
					allTradable = false
				}
			}
			if !allValid {
				diags = append(diags, factor.Diagnostic{Code: "basket-input-missing", Detail: st.def.Name})
				st.stats.core.paused++
				st.stats.core.prevOK = false
				if st.pos != 0 {
					st.stats.core.closeEpisode(frame.DecisionTime, 0, "leg_missing")
					st.pos = 0
					clear(st.frozenModelBeta)
					clear(st.frozenUnitWeights)
					st.trader.Reset()
					ready = true
					diags = append(diags, factor.Diagnostic{Code: "basket-closed-missing-leg", Detail: st.def.Name})
				}
				continue
			}
			st.curOK = true
			st.stats.accrue(st.curLP, navPrev)

			slot := st.ring[st.ringHead]
			for j := range k {
				if spec.UseRawPrice {
					slot[j] = math.Exp(st.curLP[j])
				} else {
					slot[j] = st.curLP[j]
				}
			}
			st.ringHead = (st.ringHead + 1) % spec.Window
			if st.ringCount < spec.Window {
				st.ringCount++
			}
			if st.ringCount < spec.Window {
				diags = append(diags, factor.Diagnostic{Code: "basket-warmup", Detail: st.def.Name})
				continue
			}

			for w := range spec.Window {
				levelsWork[w] = st.ring[(st.ringHead+w)%spec.Window]
			}
			est, okEst := estimateBasketWindow(st, levelsWork, spec)
			if !okEst {
				if st.pos != 0 && !spec.DynamicWeights {
					// Even if rolling refit degenerates, evaluate exit/stop on the frozen entry vector if valid.
					if fz, okZ := evalBasketCombinationZ(levelsWork, st.frozenModelBeta, &st.spreadBuf); okZ {
						ready = true
						st.stats.core.bars++
						prev := st.pos
						st.pos = st.trader.Step(fz)
						if st.pos == 0 {
							clear(st.frozenModelBeta)
							clear(st.frozenUnitWeights)
						}
						st.stats.core.observeStep(prev, st.pos, fz, basketHeadlineBeta(st.frozenUnitWeights), frame.DecisionTime, spec.Band)
						continue
					}
				}
				diags = append(diags, factor.Diagnostic{Code: "basket-warmup", Detail: st.def.Name})
				continue
			}
			ready = true
			st.lastScaleStd = est.scaleStd
			st.stats.core.bars++

			if !allTradable && st.pos == 0 {
				continue
			}
			prev := st.pos
			if prev == 0 {
				if !bandWouldAttemptEntry(spec.Band, est.z) {
					st.pos = st.trader.Step(est.z)
					continue
				}
				if !est.cointAllowed || !basketHalfLifeAllowed(est.spreadSeries, spec.MinHalfLife, spec.MaxHalfLife) {
					continue
				}
				st.pos = st.trader.Step(est.z)
				if st.pos != 0 {
					copy(st.frozenModelBeta, est.modelBeta)
					copy(st.frozenUnitWeights, est.unitWeights)
				}
				st.stats.core.observeStep(prev, st.pos, est.z, basketHeadlineBeta(st.frozenUnitWeights), frame.DecisionTime, spec.Band)
			} else {
				stepZ := est.z
				if !spec.DynamicWeights {
					if fz, okZ := evalBasketCombinationZ(levelsWork, st.frozenModelBeta, &st.spreadBuf); okZ {
						stepZ = fz
					} else {
						stepZ = math.NaN()
					}
				}
				st.pos = st.trader.Step(stepZ)
				if st.pos != 0 && st.pos != prev {
					if !allTradable || !est.cointAllowed || !basketHalfLifeAllowed(est.spreadSeries, spec.MinHalfLife, spec.MaxHalfLife) {
						st.pos = 0
						clear(st.frozenModelBeta)
						clear(st.frozenUnitWeights)
						st.trader.Reset()
					} else {
						copy(st.frozenModelBeta, est.modelBeta)
						copy(st.frozenUnitWeights, est.unitWeights)
					}
				} else if st.pos != 0 && spec.DynamicWeights {
					copy(st.frozenModelBeta, est.modelBeta)
					copy(st.frozenUnitWeights, est.unitWeights)
				} else if st.pos == 0 {
					clear(st.frozenModelBeta)
					clear(st.frozenUnitWeights)
				}
				st.stats.core.observeStep(prev, st.pos, stepZ, basketHeadlineBeta(st.frozenUnitWeights), frame.DecisionTime, spec.Band)
			}
		}

		if !ready {
			return nil, append(diags, factor.Diagnostic{Code: "baskets-not-ready", Detail: "no basket has a full valid window; keep previous portfolio"}), nil
		}

		basketGross := computeBasketGrossBudgets(states, spec, availCount)
		finalW := make([][]float64, len(states))
		idealW := make([][]float64, len(states))
		preW := make([][]float64, len(states))
		buffered := make([]bool, len(states))

		for i, st := range states {
			if st.unavailable != "" {
				continue
			}
			k := len(st.sids)
			finalW[i] = make([]float64, k)
			idealW[i] = make([]float64, k)
			preW[i] = make([]float64, k)

			if st.hasHold && st.curOK && prevDecisionNAV > 0 && navNow > 0 {
				okDrift := true
				for j := range k {
					r := math.Exp(st.curLP[j] - st.holdLP[j])
					if !finitePairFloat(r) {
						okDrift = false
						break
					}
					preW[i][j] = st.holdW[j] * (prevDecisionNAV / navNow) * r
				}
				if !okDrift {
					copy(preW[i], st.holdW)
				}
			} else if st.hasHold {
				copy(preW[i], st.holdW)
			}

			if st.pos == 0 {
				continue
			}
			g := basketGross[i]
			for j := range k {
				idealW[i][j] = float64(st.pos) * g * st.frozenUnitWeights[j]
			}
			copy(finalW[i], idealW[i])
			if spec.DriftBand > 0 && st.prevPos == st.pos && st.hasHold && st.curOK {
				var l1 float64
				for j := range k {
					l1 += math.Abs(idealW[i][j] - preW[i][j])
				}
				if l1 < spec.DriftBand {
					copy(finalW[i], preW[i])
					buffered[i] = true
				}
			}
		}

		totalGross := 0.0
		for i, st := range states {
			if st.unavailable != "" {
				continue
			}
			for _, w := range finalW[i] {
				totalGross += math.Abs(w)
			}
		}
		if totalGross > spec.MaxGross+1e-12 {
			for i, st := range states {
				if st.unavailable != "" || !buffered[i] {
					continue
				}
				var curG, idG float64
				for j := range finalW[i] {
					curG += math.Abs(finalW[i][j])
					idG += math.Abs(idealW[i][j])
				}
				if curG > idG {
					copy(finalW[i], idealW[i])
					buffered[i] = false
				}
			}
			reGross := 0.0
			for i, st := range states {
				if st.unavailable != "" {
					continue
				}
				for _, w := range finalW[i] {
					reGross += math.Abs(w)
				}
			}
			if reGross > spec.MaxGross+1e-12 {
				scale := spec.MaxGross / reGross
				for i, st := range states {
					if st.unavailable != "" {
						continue
					}
					for j := range finalW[i] {
						finalW[i][j] *= scale
					}
				}
			}
		}

		weights := map[int32]float64{}
		for i, st := range states {
			if st.unavailable != "" {
				continue
			}
			for j, sid := range st.sids {
				if finalW[i][j] != 0 {
					weights[sid] += finalW[i][j]
				}
			}
			if spec.DriftBand > 0 {
				st.stats.setWeights(finalW[i], preW[i])
			} else {
				st.stats.setWeights(finalW[i], st.stats.w)
			}
			if st.pos != 0 && st.curOK {
				copy(st.holdW, finalW[i])
				copy(st.holdLP, st.curLP)
				st.hasHold = true
			} else if st.pos == 0 {
				clear(st.holdW)
				st.hasHold = false
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

type basketEstimate struct {
	modelBeta    []float64
	unitWeights  []float64
	spreadSeries []float64
	z            float64
	scaleStd     float64
	cointAllowed bool
}

func estimateBasketWindow(st *basketState, levels [][]float64, spec BasketsSpec) (basketEstimate, bool) {
	k := len(st.sids)
	modelBeta := make([]float64, k)
	cointAllowed := true

	switch st.method {
	case "fixed":
		if spec.UseRawPrice {
			lastRow := levels[len(levels)-1]
			for j := range k {
				if lastRow[j] <= 0 || !finitePairFloat(lastRow[j]) {
					return basketEstimate{}, false
				}
				modelBeta[j] = st.def.Weights[j] / lastRow[j]
			}
		} else {
			copy(modelBeta, st.def.Weights)
		}

	case "johansen":
		jSpec := statistics.JohansenSpec{Intercept: true, DifferenceLags: spec.DifferenceLags}
		res, err := statistics.Johansen(levels, jSpec)
		if err != nil || len(res.CointegratingVectors) == 0 {
			return basketEstimate{}, false
		}
		copy(modelBeta, res.CointegratingVectors[0])
		stabilizeLeadPositive(modelBeta)
		if spec.RequireCointegration {
			okC, err := res.IsCointegrated(jSpec, spec.Confidence)
			if err != nil {
				return basketEstimate{}, false
			}
			cointAllowed = okC
		}

	case "multivariate_ols":
		coeffs, ok := fitMultivariateBasketOLS(levels)
		if !ok {
			return basketEstimate{}, false
		}
		modelBeta[0] = 1.0
		for j := 1; j < k; j++ {
			modelBeta[j] = -coeffs[j-1]
		}
	default:
		return basketEstimate{}, false
	}

	unitWeights := make([]float64, k)
	var notionalNorm float64
	lastRow := levels[len(levels)-1]
	for j := range k {
		v := modelBeta[j]
		if spec.UseRawPrice {
			v *= lastRow[j]
		}
		if !finitePairFloat(v) {
			return basketEstimate{}, false
		}
		unitWeights[j] = v
		notionalNorm += math.Abs(v)
	}
	if notionalNorm <= 1e-12 || !finitePairFloat(notionalNorm) {
		return basketEstimate{}, false
	}
	for j := range k {
		unitWeights[j] /= notionalNorm
	}

	z, std, ok := evalBasketCombinationZWithStd(levels, modelBeta, &st.spreadBuf)
	if !ok {
		return basketEstimate{}, false
	}
	scaleStd := std / notionalNorm
	if !finitePairFloat(scaleStd) || scaleStd <= 0 {
		return basketEstimate{}, false
	}
	return basketEstimate{
		modelBeta:    modelBeta,
		unitWeights:  unitWeights,
		spreadSeries: st.spreadBuf,
		z:            z,
		scaleStd:     scaleStd,
		cointAllowed: cointAllowed,
	}, true
}

func stabilizeLeadPositive(vec []float64) {
	for _, v := range vec {
		if math.Abs(v) > 1e-12 {
			if v < 0 {
				for i := range vec {
					vec[i] = -vec[i]
				}
			}
			return
		}
	}
}

// fitMultivariateBasketOLS regresses centered leg 0 on centered legs 1..K-1 via
// factor.LeastSquares, applying a tiny ridge regularization fallback when the
// explanatory legs are rank-deficient.
func fitMultivariateBasketOLS(levels [][]float64) ([]float64, bool) {
	n := len(levels)
	p := len(levels[0]) - 1
	if n < p+2 || p < 1 {
		return nil, false
	}
	means := make([]float64, p+1)
	for _, row := range levels {
		for j, v := range row {
			if !finitePairFloat(v) {
				return nil, false
			}
			means[j] += v / float64(n)
		}
	}
	x := make([][]float64, n)
	y := make([]float64, n)
	var diagScale float64
	for i, row := range levels {
		y[i] = row[0] - means[0]
		xr := make([]float64, p)
		for j := range p {
			v := row[j+1] - means[j+1]
			xr[j] = v
			diagScale += v * v
		}
		x[i] = xr
	}
	if diagScale <= 1e-24 {
		return nil, false
	}
	coef, rank, err := factor.LeastSquares(x, y, nil)
	if err == nil && rank == p {
		return coef, true
	}
	// Tiny Tikhonov / ridge fallback via augmented rows sqrt(lambda)*I_p.
	ridge := math.Sqrt(1e-8 * math.Max(1e-12, diagScale/float64(p)))
	xAug := make([][]float64, n+p)
	yAug := make([]float64, n+p)
	copy(xAug, x)
	copy(yAug, y)
	for j := range p {
		row := make([]float64, p)
		row[j] = ridge
		xAug[n+j] = row
	}
	coef, _, err = factor.LeastSquares(xAug, yAug, nil)
	if err != nil {
		return nil, false
	}
	for _, c := range coef {
		if !finitePairFloat(c) {
			return nil, false
		}
	}
	return coef, true
}

func evalBasketCombinationZ(levels [][]float64, beta []float64, dst *[]float64) (float64, bool) {
	z, _, ok := evalBasketCombinationZWithStd(levels, beta, dst)
	return z, ok
}

func evalBasketCombinationZWithStd(levels [][]float64, beta []float64, dst *[]float64) (z, std float64, ok bool) {
	n := len(levels)
	if n < 2 || len(beta) == 0 {
		return 0, 0, false
	}
	series := (*dst)[:0]
	for _, row := range levels {
		if len(row) != len(beta) {
			return 0, 0, false
		}
		var s float64
		for j, b := range beta {
			s += b * row[j]
		}
		if !finitePairFloat(s) {
			return 0, 0, false
		}
		series = append(series, s)
	}
	*dst = series
	mean, sd, err := statistics.MeanStd(series, 1)
	if err != nil || sd <= 1e-12 || !finitePairFloat(sd) {
		return 0, 0, false
	}
	z = (series[n-1] - mean) / sd
	if !finitePairFloat(z) {
		return 0, 0, false
	}
	return z, sd, true
}

func basketHalfLifeAllowed(series []float64, minHL, maxHL float64) bool {
	if minHL <= 0 && maxHL <= 0 {
		return true
	}
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

func bandWouldAttemptEntry(cfg statistics.BandConfig, z float64) bool {
	if !finitePairFloat(z) {
		return false
	}
	if cfg.Mode == statistics.BandModeMomentum {
		return (z >= cfg.Entry && !cfg.ShortOnly) || (z <= -cfg.Entry && !cfg.LongOnly)
	}
	if cfg.Stop > 0 && math.Abs(z) >= cfg.Stop {
		return false
	}
	return (z <= -cfg.Entry && !cfg.ShortOnly) || (z >= cfg.Entry && !cfg.LongOnly)
}

func basketHeadlineBeta(unitWeights []float64) float64 {
	if len(unitWeights) < 2 || math.Abs(unitWeights[0]) <= 1e-12 {
		return 0
	}
	var hedge float64
	for j := 1; j < len(unitWeights); j++ {
		hedge += math.Abs(unitWeights[j])
	}
	return hedge / math.Abs(unitWeights[0])
}

func computeBasketGrossBudgets(states []*basketState, spec BasketsSpec, availCount int) []float64 {
	out := make([]float64, len(states))
	if spec.WeightMode == "equal" {
		var sumOpen float64
		for i, st := range states {
			if st.unavailable == "" && st.pos != 0 {
				out[i] = spec.GrossPerBasket
				sumOpen += out[i]
			}
		}
		if sumOpen > spec.MaxGross+1e-12 {
			scale := spec.MaxGross / sumOpen
			for i := range out {
				out[i] *= scale
			}
		}
		return out
	}

	// "inverse_vol" water-filling allocation bounded by MaxGross and per-basket cap.
	totalGross := math.Min(spec.MaxGross, spec.GrossPerBasket*float64(availCount))
	capPerBasket := math.Min(totalGross, 2*spec.GrossPerBasket)
	active := make([]int, 0, len(states))
	inv := make([]float64, len(states))
	for i, st := range states {
		if st.unavailable == "" && st.pos != 0 && st.lastScaleStd > 1e-12 && finitePairFloat(st.lastScaleStd) {
			inv[i] = 1 / st.lastScaleStd
			active = append(active, i)
		}
	}
	remaining := totalGross
	if len(active) == 0 || remaining <= 0 || capPerBasket <= 0 {
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
			if iterRem*(inv[idx]/sumInv) > capPerBasket+1e-15 {
				out[idx] = capPerBasket
				remaining -= capPerBasket
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
