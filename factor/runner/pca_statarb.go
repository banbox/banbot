package runner

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
)

// ResidualStatArbSpec declares a cross-sectional PCA or benchmark-residual
// Ornstein-Uhlenbeck s-score statistical-arbitrage strategy (Avellaneda & Lee 2010)
// with dollar-neutral and factor-beta-neutral portfolio construction.
type ResidualStatArbSpec struct {
	// Field is the kline field used for asset prices, default "close".
	Field string
	// FactorModel selects how systematic return factors are extracted over Window:
	// - "" or "pca" (default): standardized-return PCA via statistics.FitPCA (NumFactors, default 3)
	// - "benchmarks": multivariate OLS of asset returns on explicit Benchmarks returns
	FactorModel string
	// Benchmarks names the explicit factor-benchmark symbols under FactorModel "benchmarks"
	// (for example []string{"BTC/USDT:USDT", "ETH/USDT:USDT"}).
	Benchmarks []string
	// NumFactors is the number of principal components extracted under FactorModel "pca"; default 3 when 0.
	NumFactors int

	// Window is the rolling return regression window in 1-bar returns (>= 6; requires Window+1 price bars).
	Window int
	// OUWindow is the trailing residual accumulation window passed to statistics.FitOU;
	// 0 defaults to Window (must be in [4, Window]).
	OUWindow int
	// MinHalfLife and MaxHalfLife optionally constrain FitOU DiscreteHalfLife (in bars, >= 0; 0 disables bound).
	MinHalfLife float64
	MaxHalfLife float64

	// Asymmetric/symmetric Avellaneda-Lee s-score thresholds:
	// - Enter long residual (+1) when s_i <= -EntryS (default 1.25 when 0)
	// - Enter short residual (-1) when s_i >= +EntryS
	// - Exit long when s_i >= -ExitLongS (default 0.50 when 0, or 0.5*EntryS when EntryS <= 0.50)
	// - Exit short when s_i <= +ExitShortS (default 0.50 when 0, or 0.5*EntryS when EntryS <= 0.50)
	// - Stop out when |s_i| >= StopS (when StopS > 0) or held bars >= MaxHoldBars (when MaxHoldBars > 0)
	EntryS      float64
	ExitLongS   float64
	ExitShortS  float64
	StopS       float64
	MaxHoldBars int

	// WeightMode selects raw active desire scaling before neutral projection:
	// - "" or "equal" (default): unit active desires (+1 / -1) with equal uniqueness
	// - "inverse_vol" / "inverse_sigma": scale active desires by 1 / sigma_eq (uniqueness = sigma_eq^2)
	WeightMode string
	// TargetGross is the target portfolio gross notional as a fraction of NAV; 0 defaults to 1.0, in (0, 1].
	TargetGross float64
	// MaxWeightPerAsset optionally caps |w_i| per asset in (0, 1] while preserving neutrality; 0 disables.
	MaxWeightPerAsset float64
	// DriftBand buffers minor weight drift when active asset positions are unchanged (>= 0).
	DriftBand float64
}

// BuildResidualStatArbRegistration validates name and spec and returns a Registration
// without mutating the global runner registry.
func BuildResidualStatArbRegistration(name string, spec ResidualStatArbSpec) (Registration, error) {
	if name == "" {
		return Registration{}, errors.New("runner: residual statarb strategy needs non-empty name")
	}
	norm, err := normalizeResidualStatArbSpec(spec)
	if err != nil {
		return Registration{}, err
	}
	builderName := name + "-residual-statarb-v1"
	return Registration{
		Name:        name,
		BuilderName: builderName,
		Definition: func(c Config) (*factor.Plan, research.ComboSpec, error) {
			return compilePairsLogPlan(norm.Field, c.Factor.TimeFrame)
		},
		Factory: func(c Config) (PortfolioBuilder, error) {
			return newResidualStatArbBuilder(norm, c.Snapshot.Universe, c.Snapshot.SIDMap, c.UnavailableSymbols)
		},
		Validator: func(_ Config) error {
			return nil
		},
	}, nil
}

// RegisterResidualStatArb validates and registers a cross-sectional PCA or
// benchmark-residual s-score stat-arb strategy under definition `name` and bound
// portfolio factory `<name>-residual-statarb-v1`.
func RegisterResidualStatArb(name string, spec ResidualStatArbSpec) error {
	reg, err := BuildResidualStatArbRegistration(name, spec)
	if err != nil {
		return err
	}
	return reg.Register()
}

func resolveResidualStatArbSpec(build func(*Params) (ResidualStatArbSpec, error), c Config) (ResidualStatArbSpec, error) {
	params := c.Params()
	spec, err := build(params)
	if err != nil {
		return ResidualStatArbSpec{}, err
	}
	if err = params.Err(); err != nil {
		return ResidualStatArbSpec{}, err
	}
	return normalizeResidualStatArbSpec(spec)
}

// BuildResidualStatArbParamsRegistration builds a parameterized cross-sectional PCA or
// benchmark-residual stat-arb Registration resolved per run from Config.Params().
func BuildResidualStatArbParamsRegistration(name string, build func(*Params) (ResidualStatArbSpec, error)) (Registration, error) {
	if name == "" || build == nil {
		return Registration{}, errors.New("runner: residual statarb strategy needs name and build function")
	}
	if _, err := resolveResidualStatArbSpec(build, Config{}); err != nil {
		return Registration{}, err
	}
	builderName := name + "-residual-statarb-v1"
	return Registration{
		Name:        name,
		BuilderName: builderName,
		Definition: func(c Config) (*factor.Plan, research.ComboSpec, error) {
			norm, err := resolveResidualStatArbSpec(build, c)
			if err != nil {
				return nil, research.ComboSpec{}, err
			}
			return compilePairsLogPlan(norm.Field, c.Factor.TimeFrame)
		},
		Factory: func(c Config) (PortfolioBuilder, error) {
			norm, err := resolveResidualStatArbSpec(build, c)
			if err != nil {
				return nil, err
			}
			return newResidualStatArbBuilder(norm, c.Snapshot.Universe, c.Snapshot.SIDMap, c.UnavailableSymbols)
		},
		Validator: func(c Config) error {
			_, err := resolveResidualStatArbSpec(build, c)
			return err
		},
	}, nil
}

// RegisterResidualStatArbParams validates default parameters and registers a parameterized
// cross-sectional PCA or benchmark-residual stat-arb strategy under `<name>` and `<name>-residual-statarb-v1`.
func RegisterResidualStatArbParams(name string, build func(*Params) (ResidualStatArbSpec, error)) error {
	reg, err := BuildResidualStatArbParamsRegistration(name, build)
	if err != nil {
		return err
	}
	return reg.Register()
}

func normalizeResidualStatArbSpec(spec ResidualStatArbSpec) (ResidualStatArbSpec, error) {
	if spec.Field == "" {
		spec.Field = "close"
	}
	switch spec.FactorModel {
	case "", "pca":
		spec.FactorModel = "pca"
		if spec.NumFactors == 0 {
			spec.NumFactors = 3
		}
		if spec.NumFactors < 1 || spec.NumFactors > 32 {
			return spec, errors.New("runner: residual statarb PCA num_factors must be in [1, 32]")
		}
	case "benchmarks":
		if len(spec.Benchmarks) == 0 || len(spec.Benchmarks) > 16 {
			return spec, errors.New("runner: residual statarb benchmarks model requires 1..16 benchmark symbols")
		}
		seen := make(map[string]bool, len(spec.Benchmarks))
		benches := make([]string, len(spec.Benchmarks))
		for i, sym := range spec.Benchmarks {
			if sym == "" || seen[sym] {
				return spec, errors.New("runner: residual statarb benchmarks must be non-empty and distinct")
			}
			seen[sym] = true
			benches[i] = sym
		}
		spec.Benchmarks = benches
	default:
		return spec, fmt.Errorf("runner: unknown residual statarb factor model %q", spec.FactorModel)
	}

	minWin := 6
	if spec.FactorModel == "pca" && spec.NumFactors+3 > minWin {
		minWin = spec.NumFactors + 3
	}
	if spec.FactorModel == "benchmarks" && len(spec.Benchmarks)+3 > minWin {
		minWin = len(spec.Benchmarks) + 3
	}
	if spec.Window < minWin {
		return spec, fmt.Errorf("runner: residual statarb window %d must be at least %d", spec.Window, minWin)
	}
	if spec.OUWindow == 0 {
		spec.OUWindow = spec.Window
	}
	if spec.OUWindow < 4 || spec.OUWindow > spec.Window {
		return spec, errors.New("runner: residual statarb OU window must be in [4, Window]")
	}
	if !finitePairFloat(spec.MinHalfLife) || spec.MinHalfLife < 0 {
		return spec, errors.New("runner: residual statarb min half-life must be finite and non-negative")
	}
	if !finitePairFloat(spec.MaxHalfLife) || spec.MaxHalfLife < 0 || (spec.MaxHalfLife > 0 && spec.MaxHalfLife < spec.MinHalfLife) {
		return spec, errors.New("runner: residual statarb max half-life must be finite, non-negative, and >= min half-life")
	}

	if spec.EntryS == 0 {
		spec.EntryS = 1.25
	}
	if !finitePairFloat(spec.EntryS) || spec.EntryS <= 0 {
		return spec, errors.New("runner: residual statarb entry s-score must be positive and finite")
	}
	defExit := math.Min(0.50, 0.5*spec.EntryS)
	if spec.ExitLongS == 0 {
		spec.ExitLongS = defExit
	}
	if spec.ExitShortS == 0 {
		spec.ExitShortS = defExit
	}
	if !finitePairFloat(spec.ExitLongS) || spec.ExitLongS < 0 || spec.ExitLongS >= spec.EntryS {
		return spec, errors.New("runner: residual statarb exit_long_s must be in [0, entry_s)")
	}
	if !finitePairFloat(spec.ExitShortS) || spec.ExitShortS < 0 || spec.ExitShortS >= spec.EntryS {
		return spec, errors.New("runner: residual statarb exit_short_s must be in [0, entry_s)")
	}
	if !finitePairFloat(spec.StopS) || spec.StopS < 0 || (spec.StopS > 0 && spec.StopS <= spec.EntryS) {
		return spec, errors.New("runner: residual statarb stop_s must be 0 or > entry_s")
	}
	if spec.MaxHoldBars < 0 {
		return spec, errors.New("runner: residual statarb max hold bars must not be negative")
	}
	switch spec.WeightMode {
	case "", "equal":
		spec.WeightMode = "equal"
	case "inverse_vol", "inverse_sigma":
		spec.WeightMode = "inverse_vol"
	default:
		return spec, fmt.Errorf("runner: unknown residual statarb weight mode %q", spec.WeightMode)
	}
	if spec.TargetGross == 0 {
		spec.TargetGross = 1.0
	}
	if !finitePairFloat(spec.TargetGross) || spec.TargetGross <= 0 || spec.TargetGross > 1+1e-9 {
		return spec, errors.New("runner: residual statarb target gross must be in (0, 1]")
	}
	if !finitePairFloat(spec.MaxWeightPerAsset) || spec.MaxWeightPerAsset < 0 || spec.MaxWeightPerAsset > 1+1e-9 {
		return spec, errors.New("runner: residual statarb max weight per asset must be in [0, 1]")
	}
	if !finitePairFloat(spec.DriftBand) || spec.DriftBand < 0 {
		return spec, errors.New("runner: residual statarb drift band must be finite and non-negative")
	}
	return spec, nil
}

type residualAssetWindow struct {
	st      *residualAssetState
	returns []float64
	std     float64
	betas   []float64
	eps     []float64
	rawDiff float64 // X_last - m_eq
	sigmaEq float64
	ouValid bool
	hlValid bool
}

type residualAssetState struct {
	sid       int32
	symbol    string
	retRing   []float64 // rolling 1-bar simple returns of length Window
	retHead   int
	retCount  int
	prevLP    float64
	hasPrevLP bool

	pos            int
	prevPos        int
	held           int
	blocked        bool
	frozenEquilOff float64 // X_last - m_eq at entry/latest valid OU fit
	frozenSigmaEq  float64
	lastSigmaEq    float64
}

func (a *residualAssetState) resetPosition() {
	a.pos, a.held, a.blocked = 0, 0, false
	a.frozenEquilOff, a.frozenSigmaEq = 0, 0
}

func (a *residualAssetState) observeLogPrice(lp float64, window int) {
	if a.hasPrevLP {
		ret := math.Expm1(lp - a.prevLP)
		if finitePairFloat(ret) {
			a.retRing[a.retHead] = ret
		} else {
			a.retRing[a.retHead] = math.NaN()
		}
		a.retHead = (a.retHead + 1) % window
		if a.retCount < window {
			a.retCount++
		}
	}
	a.prevLP = lp
	a.hasPrevLP = true
}

func (a *residualAssetState) observeMissing(window int) {
	if a.hasPrevLP {
		a.retRing[a.retHead] = math.NaN()
		a.retHead = (a.retHead + 1) % window
		if a.retCount < window {
			a.retCount++
		}
	}
	a.hasPrevLP = false
}

func (a *residualAssetState) extractWindowReturns(dst []float64, window int) ([]float64, float64, bool) {
	if a.retCount < window {
		return dst, 0, false
	}
	out := dst[:0]
	for k := range window {
		v := a.retRing[(a.retHead+k)%window]
		if !finitePairFloat(v) {
			return out, 0, false
		}
		out = append(out, v)
	}
	_, std, err := statistics.MeanStd(out, 1)
	if err != nil || std <= 1e-12 || !finitePairFloat(std) {
		return out, 0, false
	}
	return out, std, true
}

func stepResidualBand(st *residualAssetState, s float64, hasS, allowEntry bool, spec ResidualStatArbSpec) {
	if st.pos != 0 {
		st.held++
		if spec.MaxHoldBars > 0 && st.held >= spec.MaxHoldBars {
			st.pos, st.held, st.blocked = 0, 0, true
			return
		}
		if !hasS || !finitePairFloat(s) {
			return
		}
		if spec.StopS > 0 && math.Abs(s) >= spec.StopS {
			st.pos, st.held, st.blocked = 0, 0, true
			return
		}
		if (st.pos > 0 && s >= -spec.ExitLongS) || (st.pos < 0 && s <= spec.ExitShortS) {
			st.pos, st.held, st.blocked = 0, 0, false
		}
		return
	}
	if !hasS || !finitePairFloat(s) {
		return
	}
	if st.blocked && s >= -spec.ExitLongS && s <= spec.ExitShortS {
		st.blocked = false
	}
	if st.blocked || !allowEntry || (spec.StopS > 0 && math.Abs(s) >= spec.StopS) {
		return
	}
	switch {
	case s <= -spec.EntryS:
		st.pos, st.held = 1, 0
	case s >= spec.EntryS:
		st.pos, st.held = -1, 0
	}
}

func newResidualStatArbBuilder(spec ResidualStatArbSpec, nominalU factor.Universe, sidMap map[int32]string, unavailable map[string]string) (PortfolioBuilder, error) {
	var err error
	spec, err = normalizeResidualStatArbSpec(spec)
	if err != nil {
		return nil, err
	}
	symToSID := make(map[string]int32, len(sidMap))
	for sid, sym := range sidMap {
		if prev, exists := symToSID[sym]; !exists || sid < prev {
			symToSID[sym] = sid
		}
	}

	var benchSIDs []int32
	benchSet := map[int32]int{}
	allBenchUnavailable := false
	if spec.FactorModel == "benchmarks" {
		for _, sym := range spec.Benchmarks {
			sid, ok := symToSID[sym]
			if !ok {
				if _, gone := unavailable[sym]; gone {
					continue
				}
				return nil, fmt.Errorf("runner: residual statarb benchmark %q is missing from this run's universe", sym)
			}
			benchSet[sid] = len(benchSIDs)
			benchSIDs = append(benchSIDs, sid)
		}
		if len(benchSIDs) == 0 {
			allBenchUnavailable = true
		}
	}

	states := map[int32]*residualAssetState{}
	getOrCreateState := func(sid int32) *residualAssetState {
		if st, ok := states[sid]; ok {
			return st
		}
		sym := sidMap[sid]
		if sym == "" {
			sym = fmt.Sprintf("sid:%d", sid)
		}
		st := &residualAssetState{
			sid:     sid,
			symbol:  sym,
			retRing: make([]float64, spec.Window),
		}
		for i := range st.retRing {
			st.retRing[i] = math.NaN()
		}
		states[sid] = st
		return st
	}
	for _, sid := range nominalU.Investable {
		getOrCreateState(sid)
	}
	for _, sid := range benchSIDs {
		getOrCreateState(sid)
	}

	var (
		prevDecisionNAV float64
		holdW           map[int32]float64
		holdLP          map[int32]float64
	)

	build := func(frame factor.Frame, universe factor.Universe, target factor.PortfolioSpec, _ research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error) {
		navNow := target.Budget.NAV
		if allBenchUnavailable {
			target.Diagnostics = []factor.Diagnostic{{Code: "residual-statarb-unavailable", Detail: "benchmark symbols have no data in the run window"}}
			p, err := factor.NewTargetPortfolio(target, map[int32]float64{})
			return p, nil, err
		}

		investSet := make(map[int32]bool, len(universe.Investable))
		for _, sid := range universe.Investable {
			investSet[sid] = true
			getOrCreateState(sid)
		}
		tradableSet := make(map[int32]bool, len(universe.Tradable))
		for _, sid := range universe.Tradable {
			tradableSet[sid] = true
		}
		values := frame.Values[pairsLogOutput]

		allKnown := make([]int32, 0, len(states))
		for sid := range states {
			allKnown = append(allKnown, sid)
		}
		sort.Slice(allKnown, func(i, j int) bool { return allKnown[i] < allKnown[j] })

		var diags []factor.Diagnostic
		curLP := make(map[int32]float64, len(allKnown))
		droppedOpen := false
		for _, sid := range allKnown {
			st := states[sid]
			st.prevPos = st.pos
			_, isBench := benchSet[sid]
			inInvest := investSet[sid]
			v, ok := values[sid]
			validLP := ok && v.Validity == factor.Valid && finitePairFloat(v.Value)
			if validLP && (inInvest || isBench) {
				curLP[sid] = v.Value
				st.observeLogPrice(v.Value, spec.Window)
			} else {
				st.observeMissing(spec.Window)
			}
			// Force-exit immediately if an asset leaves Investable or loses valid price data.
			if st.pos != 0 && (!inInvest || !validLP) {
				st.resetPosition()
				droppedOpen = true
				diags = append(diags, factor.Diagnostic{Code: "residual-closed-missing-asset", Detail: st.symbol})
			}
		}

		// Collect sorted eligible Investable assets with a complete valid Window of 1-bar returns.
		sortedInvest := slices.Clone(universe.Investable)
		slices.SortFunc(sortedInvest, func(a, b int32) int {
			sa, sb := states[a].symbol, states[b].symbol
			if sa != sb {
				return strings.Compare(sa, sb)
			}
			return int(a - b)
		})
		sortedInvest = slices.Compact(sortedInvest)

		var eligible []*residualAssetWindow
		for _, sid := range sortedInvest {
			st := states[sid]
			rets, sd, ok := st.extractWindowReturns(make([]float64, 0, spec.Window), spec.Window)
			if !ok {
				continue
			}
			eligible = append(eligible, &residualAssetWindow{st: st, returns: rets, std: sd})
		}

		var (
			factorReturns [][]float64 // Window x K
			numFactors    int
			modelReady    bool
		)

		switch spec.FactorModel {
		case "pca":
			if len(eligible) >= 2 {
				k := min(spec.NumFactors, len(eligible)-1)
				rows := make([][]float64, spec.Window)
				for t := range spec.Window {
					row := make([]float64, len(eligible))
					for j, aw := range eligible {
						row[j] = aw.returns[t]
					}
					rows[t] = row
				}
				pca, err := statistics.FitPCA(rows, k, true)
				if err == nil && len(pca.Loadings) == k {
					numFactors = k
					factorReturns = make([][]float64, spec.Window)
					for t := range spec.Window {
						fr := make([]float64, k)
						for c := range k {
							var sum float64
							for j, aw := range eligible {
								sum += (pca.Loadings[c][j] / aw.std) * aw.returns[t]
							}
							fr[c] = sum
						}
						factorReturns[t] = fr
					}
					modelReady = true
				}
			}

		case "benchmarks":
			var activeBench []int32
			var benchRets [][]float64
			for _, bsid := range benchSIDs {
				st := states[bsid]
				rets, _, ok := st.extractWindowReturns(make([]float64, 0, spec.Window), spec.Window)
				if !ok {
					activeBench = nil
					break
				}
				activeBench = append(activeBench, bsid)
				benchRets = append(benchRets, rets)
			}
			if len(activeBench) > 0 && len(eligible) >= 1 {
				numFactors = len(activeBench)
				factorReturns = make([][]float64, spec.Window)
				for t := range spec.Window {
					fr := make([]float64, numFactors)
					for c := range numFactors {
						fr[c] = benchRets[c][t]
					}
					factorReturns[t] = fr
				}
				modelReady = true
			}
		}

		if !modelReady {
			if droppedOpen {
				holdW, holdLP = nil, nil
				prevDecisionNAV = navNow
				target.Diagnostics = slices.Clone(diags)
				p, err := factor.NewTargetPortfolio(target, map[int32]float64{})
				return p, nil, err
			}
			return nil, append(diags, factor.Diagnostic{Code: "residual-not-ready", Detail: "return window or factor model warming up"}), nil
		}

		// Regress each eligible asset's return series on [1, F_1, ..., F_K] via factor.LeastSquares.
		design := make([][]float64, spec.Window)
		for t := range spec.Window {
			row := make([]float64, 1+numFactors)
			row[0] = 1.0
			copy(row[1:], factorReturns[t])
			design[t] = row
		}

		cumBuf := make([]float64, spec.OUWindow)
		var validOU []*residualAssetWindow
		for _, aw := range eligible {
			if bIdx, isBench := benchSet[aw.st.sid]; isBench && spec.FactorModel == "benchmarks" {
				aw.betas = make([]float64, numFactors)
				if bIdx < numFactors {
					aw.betas[bIdx] = 1.0
				}
				aw.eps = make([]float64, spec.Window)
				continue
			}
			coef, _, err := factor.LeastSquares(design, aw.returns, nil)
			if err != nil || len(coef) != 1+numFactors {
				continue
			}
			aw.betas = slices.Clone(coef[1:])
			aw.eps = make([]float64, spec.Window)
			for t := range spec.Window {
				fit := coef[0]
				for c := range numFactors {
					fit += aw.betas[c] * factorReturns[t][c]
				}
				aw.eps[t] = aw.returns[t] - fit
			}
			start := spec.Window - spec.OUWindow
			var acc float64
			for m := range spec.OUWindow {
				acc += aw.eps[start+m]
				cumBuf[m] = acc
			}
			ou, err := statistics.FitOU(cumBuf)
			if err != nil || !finitePairFloat(ou.DiscreteHalfLife) || ou.DiscreteHalfLife <= 0 || ou.DiscreteHalfLife > 1e6 {
				continue
			}
			phi := 1 + ou.Slope
			denom := 1 - phi*phi
			if denom <= 1e-12 {
				continue
			}
			sigmaEq := ou.InnovationStd / math.Sqrt(denom)
			rawDiff := cumBuf[spec.OUWindow-1] - ou.Equilibrium
			if !finitePairFloat(sigmaEq) || sigmaEq <= 1e-12 || !finitePairFloat(rawDiff) {
				continue
			}
			aw.rawDiff = rawDiff
			aw.sigmaEq = sigmaEq
			aw.ouValid = true
			hlOK := (spec.MinHalfLife <= 0 || ou.DiscreteHalfLife >= spec.MinHalfLife) &&
				(spec.MaxHalfLife <= 0 || ou.DiscreteHalfLife <= spec.MaxHalfLife)
			aw.hlValid = hlOK
			validOU = append(validOU, aw)
		}

		// Centered Avellaneda-Lee s-score: center (X_{i,last} - m_eq) across the cross-section when >= 2 OU assets exist.
		var meanRawDiff float64
		if len(validOU) >= 2 {
			for _, aw := range validOU {
				meanRawDiff += aw.rawDiff / float64(len(validOU))
			}
		}

		eligibleBySID := make(map[int32]*residualAssetWindow, len(eligible))
		for _, aw := range eligible {
			eligibleBySID[aw.st.sid] = aw
			st := aw.st
			canTrade := tradableSet[st.sid]
			if aw.ouValid {
				centeredDiff := aw.rawDiff - meanRawDiff
				sScore := centeredDiff / aw.sigmaEq
				st.lastSigmaEq = aw.sigmaEq
				allowEntry := canTrade && aw.hlValid
				prev := st.pos
				stepResidualBand(st, sScore, true, allowEntry, spec)
				if st.pos != 0 && prev == 0 {
					st.frozenEquilOff = aw.eps[spec.Window-1] // keep latest scale
					st.frozenSigmaEq = aw.sigmaEq
				}
			} else if st.pos != 0 {
				// Still enforce MaxHoldBars when an open position's rolling OU fit turns non-reverting;
				// if it holds no valid OU equilibrium anymore, evaluate stop/exit on accumulated residual vs entry scale.
				var fallbackS float64
				hasFB := false
				if st.frozenSigmaEq > 1e-12 && len(aw.eps) == spec.Window {
					var cum float64
					for m := spec.Window - spec.OUWindow; m < spec.Window; m++ {
						cum += aw.eps[m]
					}
					fallbackS = cum / st.frozenSigmaEq
					hasFB = finitePairFloat(fallbackS)
				}
				stepResidualBand(st, fallbackS, hasFB, false, spec)
			}
		}

		// Step any Investable asset that had pos != 0 but lacked a complete return window (e.g. temporary gap).
		for _, sid := range sortedInvest {
			if _, ok := eligibleBySID[sid]; ok {
				continue
			}
			st := states[sid]
			if st.pos != 0 {
				stepResidualBand(st, 0, false, false, spec)
			}
		}

		idealW := computeNeutralResidualWeights(eligible, benchSIDs, states, tradableSet, numFactors, spec)
		finalW := idealW

		// Apply DriftBand when all active positions are unchanged and all held instruments remain valid & tradable.
		if spec.DriftBand > 0 && len(holdW) > 0 && len(idealW) > 0 && prevDecisionNAV > 0 && navNow > 0 && !droppedOpen {
			sameSignals := true
			for _, st := range states {
				if st.pos != st.prevPos {
					sameSignals = false
					break
				}
			}
			if sameSignals {
				preW := make(map[int32]float64, len(holdW))
				okDrift := true
				var preGross float64
				for sid, hw := range holdW {
					lpNow, ok1 := curLP[sid]
					lpPrev, ok2 := holdLP[sid]
					if !ok1 || !ok2 || !tradableSet[sid] {
						okDrift = false
						break
					}
					r := math.Exp(lpNow - lpPrev)
					if !finitePairFloat(r) {
						okDrift = false
						break
					}
					pw := hw * (prevDecisionNAV / navNow) * r
					preW[sid] = pw
					preGross += math.Abs(pw)
				}
				if okDrift && preGross <= 1.0+1e-12 {
					allSIDs := make(map[int32]bool, len(idealW)+len(preW))
					for sid := range idealW {
						allSIDs[sid] = true
					}
					for sid := range preW {
						allSIDs[sid] = true
					}
					var l1 float64
					for sid := range allSIDs {
						l1 += math.Abs(idealW[sid] - preW[sid])
					}
					if l1 < spec.DriftBand {
						// Re-project onto exact dollar/factor neutrality if tiny price drift accumulated, or keep preW inside DriftBand.
						finalW = preW
					}
				}
			}
		}

		for sid, w := range finalW {
			if math.Abs(w) <= 1e-15 {
				delete(finalW, sid)
			}
		}
		if len(finalW) > 0 {
			holdW = make(map[int32]float64, len(finalW))
			holdLP = make(map[int32]float64, len(finalW))
			for sid, w := range finalW {
				holdW[sid] = w
				holdLP[sid] = curLP[sid]
			}
		} else {
			holdW, holdLP = nil, nil
		}
		prevDecisionNAV = navNow
		target.Diagnostics = slices.Clone(diags)
		p, err := factor.NewTargetPortfolio(target, finalW)
		return p, nil, err
	}
	return build, nil
}

type neutralCarrier struct {
	sid      int32
	alpha    float64
	variance float64
	betas    []float64
}

func computeNeutralResidualWeights(
	eligible []*residualAssetWindow,
	benchSIDs []int32,
	states map[int32]*residualAssetState,
	tradable map[int32]bool,
	numFactors int,
	spec ResidualStatArbSpec,
) map[int32]float64 {
	// Collect active signal assets and passive hedgeable assets with valid factor betas.
	type entry struct {
		sid      int32
		pos      int
		sigmaEq  float64
		std      float64
		betas    []float64
		tradable bool
	}
	bySID := make(map[int32]entry, len(eligible)+len(benchSIDs))
	var activeEntries []entry
	var passiveEntries []entry

	for _, aw := range eligible {
		if len(aw.betas) != numFactors {
			continue
		}
		isTrad := tradable[aw.st.sid]
		if !isTrad && aw.st.pos == 0 {
			continue
		}
		sig := aw.sigmaEq
		if sig <= 1e-12 {
			sig = aw.st.lastSigmaEq
		}
		if sig <= 1e-12 {
			sig = aw.std
		}
		e := entry{
			sid:      aw.st.sid,
			pos:      aw.st.pos,
			sigmaEq:  sig,
			std:      aw.std,
			betas:    aw.betas,
			tradable: isTrad,
		}
		bySID[e.sid] = e
		if e.pos != 0 {
			activeEntries = append(activeEntries, e)
		} else if isTrad {
			passiveEntries = append(passiveEntries, e)
		}
	}
	if len(activeEntries) == 0 {
		return map[int32]float64{}
	}

	// Ensure benchmark instruments are available as explicit beta hedges in "benchmarks" mode even if outside Investable.
	var benchEntries []entry
	if spec.FactorModel == "benchmarks" {
		for idx, bsid := range benchSIDs {
			if existing, ok := bySID[bsid]; ok {
				if existing.pos == 0 {
					benchEntries = append(benchEntries, existing)
				}
				continue
			}
			if !tradable[bsid] {
				continue
			}
			st := states[bsid]
			if st == nil {
				continue
			}
			_, sd, ok := st.extractWindowReturns(make([]float64, 0, spec.Window), spec.Window)
			if !ok {
				continue
			}
			b := make([]float64, numFactors)
			if idx < numFactors {
				b[idx] = 1.0
			}
			benchEntries = append(benchEntries, entry{
				sid:      bsid,
				pos:      0,
				sigmaEq:  sd,
				std:      sd,
				betas:    b,
				tradable: true,
			})
		}
	}

	toCarriers := func(list []entry) []neutralCarrier {
		out := make([]neutralCarrier, len(list))
		for i, e := range list {
			alpha := 0.0
			variance := 1.0
			if spec.WeightMode == "inverse_vol" {
				s := math.Max(e.sigmaEq, 1e-6)
				variance = s * s
				if e.pos != 0 {
					alpha = float64(e.pos) * s // alpha / variance = pos / sigma_eq
				}
			} else if e.pos != 0 {
				alpha = float64(e.pos)
			}
			out[i] = neutralCarrier{
				sid:      e.sid,
				alpha:    alpha,
				variance: variance,
				betas:    e.betas,
			}
		}
		return out
	}

	// Tier 1: Active assets only (or Active + Benchmarks under "benchmarks" mode) when degrees of freedom suffice.
	var primary []entry
	primary = append(primary, activeEntries...)
	if spec.FactorModel == "benchmarks" {
		primary = append(primary, benchEntries...)
	}
	if len(primary) >= numFactors+2 {
		if w, ok := solveConstrainedNeutral(toCarriers(primary), numFactors, spec.TargetGross, spec.MaxWeightPerAsset); ok {
			return w
		}
	}

	// Tier 2: Expand carrier set with passive eligible tradable assets so full dollar + factor-beta neutrality holds even for 1..K active signals.
	seen := make(map[int32]bool, len(primary)+len(passiveEntries))
	var expanded []entry
	for _, list := range [][]entry{primary, passiveEntries} {
		for _, e := range list {
			if !seen[e.sid] {
				seen[e.sid] = true
				expanded = append(expanded, e)
			}
		}
	}
	if len(expanded) >= 2 {
		maxFactors := min(numFactors, len(expanded)-2)
		for k := maxFactors; k >= 0; k-- {
			if w, ok := solveConstrainedNeutral(toCarriers(expanded), k, spec.TargetGross, spec.MaxWeightPerAsset); ok {
				return w
			}
		}
	}
	return map[int32]float64{}
}

func solveConstrainedNeutral(carriers []neutralCarrier, kFactors int, targetGross, maxWeight float64) (map[int32]float64, bool) {
	n := len(carriers)
	if n < kFactors+2 {
		return nil, false
	}
	alpha := make([]float64, n)
	variance := make([]float64, n)
	exposures := make([][]float64, n)
	for i, c := range carriers {
		alpha[i] = c.alpha
		variance[i] = c.variance
		row := make([]float64, 1+kFactors)
		row[0] = 1.0 // all-ones column enforces exact dollar neutrality sum_i w_i == 0
		copy(row[1:], c.betas[:kFactors])
		exposures[i] = row
	}
	alloc, err := statistics.FactorNeutralWeights(alpha, variance, exposures, 1.0, 0)
	if err != nil || alloc.NoResidualAlpha || alloc.Gross <= 1e-12 {
		return nil, false
	}
	w := make([]float64, n)
	scale := targetGross / alloc.Gross
	for i, v := range alloc.Weights {
		w[i] = v * scale
	}

	if maxWeight > 0 {
		// Alternating projection onto [-maxWeight, +maxWeight] (and L1 <= targetGross) and {w : exposures^T w == 0}.
		for iter := 0; iter < 64; iter++ {
			maxAbs := 0.0
			for _, v := range w {
				if math.Abs(v) > maxAbs {
					maxAbs = math.Abs(v)
				}
			}
			if maxAbs <= maxWeight+1e-12 {
				break
			}
			var gross float64
			for i, v := range w {
				if v > maxWeight {
					v = maxWeight
				} else if v < -maxWeight {
					v = -maxWeight
				}
				w[i] = v
				gross += math.Abs(v)
			}
			if gross > targetGross && gross > 0 {
				s := targetGross / gross
				for i := range w {
					w[i] *= s
				}
			}
			if !projectZeroExposureInPlace(w, exposures) {
				return nil, false
			}
		}
		if !projectZeroExposureInPlace(w, exposures) {
			return nil, false
		}
		// Final proportional guard preserves exact exposures^T w == 0 if maxWeight caps achievable gross.
		var maxAbs, gross float64
		for _, v := range w {
			maxAbs = math.Max(maxAbs, math.Abs(v))
			gross += math.Abs(v)
		}
		if maxAbs <= 1e-12 || gross <= 1e-12 {
			return nil, false
		}
		finalScale := 1.0
		if maxAbs > maxWeight {
			finalScale = math.Min(finalScale, maxWeight/maxAbs)
		}
		if gross*finalScale > targetGross {
			finalScale = math.Min(finalScale, targetGross/gross)
		}
		if finalScale < 1.0 {
			for i := range w {
				w[i] *= finalScale
			}
		}
	}

	out := make(map[int32]float64, n)
	for i, c := range carriers {
		if math.Abs(w[i]) > 1e-15 {
			out[c.sid] = w[i]
		}
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

func projectZeroExposureInPlace(w []float64, exposures [][]float64) bool {
	coef, _, err := factor.LeastSquares(exposures, w, nil)
	if err != nil {
		return false
	}
	for i := range w {
		for j, c := range coef {
			w[i] -= exposures[i][j] * c
		}
		if !finitePairFloat(w[i]) {
			return false
		}
	}
	return true
}
