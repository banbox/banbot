package runner

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
)

// RollingPairsSpec declares a periodic formation-plus-trading statistical-arbitrage
// strategy that re-screens candidate pairs from the point-in-time universe without
// lookahead.
type RollingPairsSpec struct {
	// Candidates optionally restricts pair screening to a subset of symbols in the
	// run's universe. When empty, all Investable symbols of the run are used.
	Candidates []string
	// Field is the kline field used for log-prices, default "close".
	Field string

	// FormationBars is the formation lookback window in decision bars (at least 20).
	FormationBars int
	// ReselectBars is the trading period length in decision bars between pair reselections (at least 1).
	ReselectBars int
	// PairCount is the maximum number of pairs selected per formation period, default 5.
	PairCount int
	// MaxPerSymbol caps how many times a single symbol may appear in one period's selected pairs, default 2.
	MaxPerSymbol int

	// SelectionMethod chooses the formation-window pair selector:
	// "" or "cointegration" (default): coverage/correlation/positive-beta pre-filter, bidirectional
	// Engle-Granger residual ADF + FitOU discrete half-life filter, greedy selection by ascending ADF t-stat.
	// "distance": Gatev minimum sum of squared deviations (SSD) on formation-start normalized prices (1.0)
	// among positively correlated candidate pairs.
	SelectionMethod string
	// MinCoverage is the minimum fraction of aligned valid bars in FormationBars, default 0.9, in (0, 1].
	MinCoverage float64
	// MinCorr is the minimum log-return Pearson correlation over the formation window;
	// default 0.5 under "cointegration"; under "distance", 0 requires strictly positive correlation (> 0).
	MinCorr float64
	// MaxADF is the maximum Engle-Granger residual ADF t-statistic under "cointegration"; default -2.86 when 0.
	MaxADF float64
	// ADFLags is the lag count passed to statistics.EngleGranger, default 0.
	ADFLags int
	// MinHalfLife is the minimum FitOU DiscreteHalfLife (in decision bars) under "cointegration", default 0.
	MinHalfLife float64
	// MaxHalfLife is the maximum FitOU DiscreteHalfLife (in decision bars) under "cointegration";
	// default float64(FormationBars) when 0.
	MaxHalfLife float64

	// Window is the online RollingSpread regression window in decision bars; 0 defaults to FormationBars (at least 3).
	Window int
	// Band configures z-score entry/exit/stop/max-hold hysteresis.
	Band statistics.BandConfig
	// Gross is the base gross notional of one pair slot as a fraction of strategy NAV;
	// 0 defaults to 1/PairCount, and Gross*PairCount must not exceed 1.
	Gross float64
	// MinBeta is the minimum OLS slope required on cointegration formation and online entry (>= 0).
	MinBeta float64
	// AllowNegativeBeta disables the default positive-beta guard when true.
	AllowNegativeBeta bool
	// DynamicExitZ opts out of frozen-beta EvalWithBeta exit/stop evaluation when true.
	DynamicExitZ bool
	// DriftBand buffers minor weight drift while holding (>= 0).
	DriftBand float64
	// WeightMode selects cross-pair sizing: "" or "equal" (default), or "inverse_vol".
	WeightMode string
	// MaxPairGross caps any single pair's gross notional under WeightMode "inverse_vol" ([0, 1]).
	MaxPairGross float64

	// HandoffMode controls how unselected open positions transition at reselection boundaries:
	// "" or "graceful" (default): unselected open pairs remain in an exit-only pool (tracking their frozen Beta
	// and BandTrader until exit/stop/maxhold without re-opening), while newly selected pairs occupy remaining slots
	// so total active gross never exceeds 1.
	// "immediate": unselected open pairs are closed immediately on the reselection bar.
	HandoffMode string
}

// RegisterRollingPairs registers a rolling formation-and-trading pairs strategy
// that periodically re-selects pairs from the run's universe without hardcoding
// symbol literals in Go code.
func RegisterRollingPairs(name string, build func(*Params) (RollingPairsSpec, error)) error {
	if name == "" || build == nil {
		return errors.New("runner: rolling pairs strategy needs name and build function")
	}
	builderName := name + "-rolling-pairs-v1"
	err := RegisterDefinition(name, func(c Config) (*factor.Plan, research.ComboSpec, error) {
		spec, err := resolveRollingPairsSpec(build, c)
		if err != nil {
			return nil, research.ComboSpec{}, err
		}
		return compilePairsLogPlan(spec.Field, c.Factor.TimeFrame)
	})
	if err != nil {
		return err
	}
	err = RegisterPortfolioFactory(builderName, func(c Config) (PortfolioBuilder, error) {
		spec, err := resolveRollingPairsSpec(build, c)
		if err != nil {
			return nil, err
		}
		builder, tracker, err := newTrackedRollingPairsBuilder(spec, c.Snapshot.Universe, c.Snapshot.SIDMap, c.UnavailableSymbols)
		if err == nil && c.observe != nil {
			c.observe.pairs, c.observe.reset = tracker.report, tracker.resetStats
		}
		return builder, err
	}, func(c Config) error {
		_, err := resolveRollingPairsSpec(build, c)
		return err
	})
	if err != nil {
		return err
	}
	bindDefinitionBuilder(name, builderName)
	return nil
}

func resolveRollingPairsSpec(build func(*Params) (RollingPairsSpec, error), c Config) (RollingPairsSpec, error) {
	p := c.Params()
	spec, err := build(p)
	if err != nil {
		return spec, err
	}
	if err := p.Err(); err != nil {
		return spec, err
	}
	return normalizeRollingPairsSpec(spec)
}

func normalizeRollingPairsSpec(spec RollingPairsSpec) (RollingPairsSpec, error) {
	if spec.Field == "" {
		spec.Field = "close"
	}
	if len(spec.Candidates) > 0 {
		seen := make(map[string]bool, len(spec.Candidates))
		unique := make([]string, 0, len(spec.Candidates))
		for _, sym := range spec.Candidates {
			if sym == "" {
				return spec, errors.New("runner: rolling pairs candidate symbol must not be empty")
			}
			if !seen[sym] {
				seen[sym] = true
				unique = append(unique, sym)
			}
		}
		if len(unique) < 2 {
			return spec, errors.New("runner: rolling pairs candidates must contain at least 2 distinct symbols")
		}
		spec.Candidates = unique
	}
	if spec.FormationBars < 20 {
		return spec, errors.New("runner: rolling pairs formation bars must be at least 20")
	}
	if spec.ReselectBars < 1 {
		return spec, errors.New("runner: rolling pairs reselect bars must be at least 1")
	}
	if spec.PairCount == 0 {
		spec.PairCount = 5
	}
	if spec.PairCount < 1 {
		return spec, errors.New("runner: rolling pairs pair count must be positive")
	}
	if spec.MaxPerSymbol == 0 {
		spec.MaxPerSymbol = 2
	}
	if spec.MaxPerSymbol < 1 {
		return spec, errors.New("runner: rolling pairs max per symbol must be positive")
	}
	switch spec.SelectionMethod {
	case "", "cointegration":
		spec.SelectionMethod = "cointegration"
	case "distance":
	default:
		return spec, fmt.Errorf("runner: unknown rolling pairs selection method %q", spec.SelectionMethod)
	}
	if spec.MinCoverage == 0 {
		spec.MinCoverage = 0.9
	}
	if !finitePairFloat(spec.MinCoverage) || spec.MinCoverage <= 0 || spec.MinCoverage > 1 {
		return spec, errors.New("runner: rolling pairs min coverage must be in (0, 1]")
	}
	if !finitePairFloat(spec.MinCorr) || spec.MinCorr < 0 || spec.MinCorr > 1 {
		return spec, errors.New("runner: rolling pairs min correlation must be in [0, 1]")
	}
	if spec.SelectionMethod == "cointegration" && spec.MinCorr == 0 {
		spec.MinCorr = 0.5
	}
	if spec.ADFLags < 0 || spec.FormationBars < spec.ADFLags+6 {
		return spec, errors.New("runner: rolling pairs invalid ADF lags for formation window")
	}
	if spec.MaxADF == 0 {
		spec.MaxADF = -2.86
	}
	if !finitePairFloat(spec.MaxADF) || (spec.SelectionMethod == "cointegration" && spec.MaxADF >= 0) {
		return spec, errors.New("runner: rolling pairs max ADF must be finite and negative")
	}
	if !finitePairFloat(spec.MinHalfLife) || spec.MinHalfLife < 0 {
		return spec, errors.New("runner: rolling pairs min half-life must be finite and non-negative")
	}
	if spec.MaxHalfLife == 0 {
		spec.MaxHalfLife = float64(spec.FormationBars)
	}
	if !finitePairFloat(spec.MaxHalfLife) || spec.MaxHalfLife <= 0 || spec.MaxHalfLife < spec.MinHalfLife {
		return spec, errors.New("runner: rolling pairs max half-life must be positive and >= min half-life")
	}
	if spec.Window == 0 {
		spec.Window = spec.FormationBars
	}
	if spec.Window < 3 {
		return spec, errors.New("runner: rolling pairs window must be at least 3")
	}
	if err := spec.Band.Validate(); err != nil {
		return spec, err
	}
	if spec.Gross == 0 {
		spec.Gross = 1 / float64(spec.PairCount)
	}
	if !finitePairFloat(spec.Gross) || !(spec.Gross > 0) || spec.Gross*float64(spec.PairCount) > 1+1e-9 {
		return spec, errors.New("runner: rolling pairs gross per pair must be positive and keep total gross within 1")
	}
	if !finitePairFloat(spec.MinBeta) || spec.MinBeta < 0 {
		return spec, errors.New("runner: rolling pairs min beta must be finite and non-negative")
	}
	if !finitePairFloat(spec.DriftBand) || spec.DriftBand < 0 {
		return spec, errors.New("runner: rolling pairs drift band must be finite and non-negative")
	}
	switch spec.WeightMode {
	case "", "equal":
		spec.WeightMode = "equal"
	case "inverse_vol":
	default:
		return spec, fmt.Errorf("runner: unknown rolling pairs weight mode %q", spec.WeightMode)
	}
	if !finitePairFloat(spec.MaxPairGross) || spec.MaxPairGross < 0 || spec.MaxPairGross > 1+1e-9 {
		return spec, errors.New("runner: rolling pairs max pair gross must be in [0, 1]")
	}
	switch spec.HandoffMode {
	case "", "graceful":
		spec.HandoffMode = "graceful"
	case "immediate":
	default:
		return spec, fmt.Errorf("runner: unknown rolling pairs handoff mode %q", spec.HandoffMode)
	}
	return spec, nil
}

type candidateAsset struct {
	sid    int32
	symbol string
}

type scoredCandidatePair struct {
	y, x       int32
	ySym, xSym string
	score      float64
}

type rollingPairRecord struct {
	pair        Pair
	unavailable string
	stats       pairStats
}

type activeRollingPair struct {
	pair            Pair
	rec             *rollingPairRecord
	state           pairState
	exitOnly        bool
	preWY, preWX    float64
	stepZ           float64
	observedThisBar bool
}

type rollingPairsTracker struct {
	records     map[Pair]*rollingPairRecord
	order       []Pair
	unavailable []*rollingPairRecord
	lastNAV     float64
	lastMS      int64
}

func newRollingPairsTracker() *rollingPairsTracker {
	return &rollingPairsTracker{records: make(map[Pair]*rollingPairRecord)}
}

func (t *rollingPairsTracker) getOrCreate(pair Pair) *rollingPairRecord {
	if rec, ok := t.records[pair]; ok {
		return rec
	}
	rec := &rollingPairRecord{
		pair:  pair,
		stats: pairStats{episodeCap: totalPairEpisodes},
	}
	t.records[pair] = rec
	t.order = append(t.order, pair)
	return rec
}

func (t *rollingPairsTracker) resetStats() {
	for _, rec := range t.records {
		rec.stats = pairStats{episodeCap: totalPairEpisodes}
	}
	t.lastNAV, t.lastMS = 0, 0
}

func (t *rollingPairsTracker) report(initialNAV float64) []PairResult {
	total := len(t.order) + len(t.unavailable)
	if total == 0 {
		return nil
	}
	epCap := pairEpisodeCap(total)
	out := make([]PairResult, 0, total)
	for _, pair := range t.order {
		rec := t.records[pair]
		out = append(out, buildPairResult(rec.pair, &rec.stats, rec.unavailable, epCap, t.lastMS, initialNAV))
	}
	for _, rec := range t.unavailable {
		out = append(out, buildPairResult(rec.pair, &rec.stats, rec.unavailable, epCap, t.lastMS, initialNAV))
	}
	return out
}

func resolveRollingCandidates(spec RollingPairsSpec, u factor.Universe, sidMap map[int32]string, unavailable map[string]string) ([]candidateAsset, []string, error) {
	symForSID := func(sid int32) string {
		if s := sidMap[sid]; s != "" {
			return s
		}
		return fmt.Sprintf("sid:%d", sid)
	}
	if len(spec.Candidates) == 0 {
		var pool []int32
		switch {
		case len(u.Investable) > 0:
			pool = u.Investable
		case len(u.Tradable) > 0:
			pool = u.Tradable
		default:
			for sid := range sidMap {
				pool = append(pool, sid)
			}
		}
		seen := make(map[int32]bool, len(pool))
		assets := make([]candidateAsset, 0, len(pool))
		for _, sid := range pool {
			if seen[sid] {
				continue
			}
			seen[sid] = true
			assets = append(assets, candidateAsset{sid: sid, symbol: symForSID(sid)})
		}
		sortCandidateAssets(assets)
		if len(assets) < 2 && len(unavailable) == 0 {
			return nil, nil, errors.New("runner: rolling pairs requires at least 2 symbols in this run's universe")
		}
		var skipped []string
		if len(assets) < 2 && len(unavailable) > 0 {
			for sym := range unavailable {
				skipped = append(skipped, sym)
			}
			sort.Strings(skipped)
		}
		return assets, skipped, nil
	}

	symToSID := make(map[string]int32, len(sidMap))
	for sid, sym := range sidMap {
		if prev, exists := symToSID[sym]; !exists || sid < prev {
			symToSID[sym] = sid
		}
	}
	var investSet map[int32]bool
	if len(u.Investable) > 0 {
		investSet = make(map[int32]bool, len(u.Investable))
		for _, sid := range u.Investable {
			investSet[sid] = true
		}
	}
	assets := make([]candidateAsset, 0, len(spec.Candidates))
	var skipped []string
	for _, sym := range spec.Candidates {
		sid, ok := symToSID[sym]
		if ok && investSet != nil && !investSet[sid] {
			ok = false
		}
		if !ok {
			if _, gone := unavailable[sym]; !gone {
				return nil, nil, fmt.Errorf("runner: rolling pairs candidate %s has a symbol missing from this run's universe", sym)
			}
			skipped = append(skipped, sym)
			continue
		}
		assets = append(assets, candidateAsset{sid: sid, symbol: sym})
	}
	sortCandidateAssets(assets)
	return assets, skipped, nil
}

func sortCandidateAssets(assets []candidateAsset) {
	sort.Slice(assets, func(i, j int) bool {
		if assets[i].symbol != assets[j].symbol {
			return assets[i].symbol < assets[j].symbol
		}
		return assets[i].sid < assets[j].sid
	})
}

func newRollingPairsBuilder(spec RollingPairsSpec, u factor.Universe, sidMap map[int32]string) (PortfolioBuilder, error) {
	b, _, err := newTrackedRollingPairsBuilder(spec, u, sidMap, nil)
	return b, err
}

func newTrackedRollingPairsBuilder(spec RollingPairsSpec, u factor.Universe, sidMap map[int32]string, unavailable map[string]string) (PortfolioBuilder, *rollingPairsTracker, error) {
	var err error
	spec, err = normalizeRollingPairsSpec(spec)
	if err != nil {
		return nil, nil, err
	}
	candidates, skippedSyms, err := resolveRollingCandidates(spec, u, sidMap, unavailable)
	if err != nil {
		return nil, nil, err
	}
	tracker := newRollingPairsTracker()
	allUnavailable := len(candidates) < 2
	if allUnavailable {
		unavailLeg := "universe"
		if len(skippedSyms) > 0 {
			unavailLeg = skippedSyms[0]
		}
		ySym, xSym := unavailLeg, unavailLeg
		if len(candidates) == 1 {
			ySym = candidates[0].symbol
		} else if len(skippedSyms) >= 2 {
			ySym, xSym = skippedSyms[0], skippedSyms[1]
		}
		tracker.unavailable = append(tracker.unavailable, &rollingPairRecord{
			pair:        Pair{Y: ySym, X: xSym},
			unavailable: unavailLeg,
			stats:       pairStats{episodeCap: totalPairEpisodes},
		})
	}

	sidToIdx := make(map[int32]int, len(candidates))
	for i, c := range candidates {
		sidToIdx[c.sid] = i
	}
	histCap := max(spec.FormationBars, spec.Window)
	hist := make([][]float64, len(candidates))
	for i := range hist {
		row := make([]float64, histCap)
		for j := range row {
			row[j] = math.NaN()
		}
		hist[i] = row
	}

	var (
		histHead        int
		histCount       int
		decisionBars    int
		prevDecisionNAV float64
		retiring        []*activeRollingPair
		current         []*activeRollingPair
	)

	// prewarmSpread feeds up to Window-1 aligned valid observations strictly prior to lastSlot in chronological order.
	prewarmSpread := func(rs *statistics.RollingSpread, yIdx, xIdx, lastSlot int) {
		need := spec.Window - 1
		if need <= 0 || histCount <= 1 {
			return
		}
		type obs struct{ lx, ly float64 }
		buf := make([]obs, 0, min(need, histCount-1))
		steps := histCount - 1
		for step := 1; step <= steps && len(buf) < need; step++ {
			slot := (lastSlot - step + histCap) % histCap
			ly, lx := hist[yIdx][slot], hist[xIdx][slot]
			if finitePairFloat(lx) && finitePairFloat(ly) {
				buf = append(buf, obs{lx: lx, ly: ly})
			}
		}
		for i := len(buf) - 1; i >= 0; i-- {
			rs.Observe(buf[i].lx, buf[i].ly)
		}
	}

	recordZeroWeight := func(ap *activeRollingPair) {
		if spec.DriftBand > 0 {
			ap.rec.stats.setWeightsFrom(0, 0, ap.preWY, ap.preWX)
		} else {
			ap.rec.stats.setWeights(0, 0)
		}
		ap.state.holdWY, ap.state.holdWX, ap.state.hasHold = 0, 0, false
	}

	build := func(frame factor.Frame, universe factor.Universe, target factor.PortfolioSpec, _ research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error) {
		navPrev := tracker.lastNAV
		navNow := target.Budget.NAV
		tracker.lastNAV = navNow
		tracker.lastMS = frame.DecisionTime

		if allUnavailable {
			target.Diagnostics = []factor.Diagnostic{{Code: "pairs-all-unavailable", Detail: "fewer than 2 candidate symbols have data in the run window"}}
			p, err := factor.NewTargetPortfolio(target, map[int32]float64{})
			return p, nil, err
		}

		tradable := make(map[int32]bool, len(universe.Tradable))
		for _, sid := range universe.Tradable {
			tradable[sid] = true
		}
		values := frame.Values[pairsLogOutput]

		lastSlot := histHead
		for idx, c := range candidates {
			v, ok := values[c.sid]
			if ok && v.Validity == factor.Valid && finitePairFloat(v.Value) {
				hist[idx][lastSlot] = v.Value
			} else {
				hist[idx][lastSlot] = math.NaN()
			}
		}
		histHead = (histHead + 1) % histCap
		if histCount < histCap {
			histCount++
		}
		decisionBars++

		if decisionBars < spec.FormationBars {
			return nil, []factor.Diagnostic{{Code: "pairs-not-ready", Detail: "rolling pairs formation window warming up"}}, nil
		}

		var diags []factor.Diagnostic
		prepareAndStepOpen := func(ap *activeRollingPair) {
			st := &ap.state
			st.prevPos = st.pos
			st.curOK = false
			ap.preWY, ap.preWX = 0, 0
			ap.stepZ = 0
			ap.observedThisBar = false

			ly, okY := values[st.y]
			lx, okX := values[st.x]
			if !okY || !okX || ly.Validity != factor.Valid || lx.Validity != factor.Valid || !finitePairFloat(ly.Value) || !finitePairFloat(lx.Value) {
				if st.hasHold {
					ap.preWY, ap.preWX = st.holdWY, st.holdWX
				}
				diags = append(diags, factor.Diagnostic{Code: "pair-input-missing", Detail: fmt.Sprintf("%d/%d", st.y, st.x)})
				ap.rec.stats.paused++
				ap.rec.stats.prevOK = false
				if st.pos != 0 {
					ap.rec.stats.closeEpisode(frame.DecisionTime, 0, "leg_missing")
					st.pos, st.beta = 0, 0
					st.trader.Reset()
					diags = append(diags, factor.Diagnostic{Code: "pair-closed-missing-leg", Detail: fmt.Sprintf("%d/%d", st.y, st.x)})
				}
				return
			}
			st.curLY, st.curLX, st.curOK = ly.Value, lx.Value, true
			if st.hasHold && prevDecisionNAV > 0 && navNow > 0 {
				ry := math.Exp(st.curLY - st.holdLY)
				rx := math.Exp(st.curLX - st.holdLX)
				if finitePairFloat(ry) && finitePairFloat(rx) {
					ap.preWY = st.holdWY * (prevDecisionNAV / navNow) * ry
					ap.preWX = st.holdWX * (prevDecisionNAV / navNow) * rx
				} else {
					ap.preWY, ap.preWX = st.holdWY, st.holdWX
				}
			} else if st.hasHold {
				ap.preWY, ap.preWX = st.holdWY, st.holdWX
			}
			ap.rec.stats.accrue(ly.Value, lx.Value, navPrev)

			if st.pos == 0 {
				return
			}
			point, ok := st.spread.Observe(st.curLX, st.curLY)
			ap.observedThisBar = true
			if !ok {
				diags = append(diags, factor.Diagnostic{Code: "pair-warmup", Detail: fmt.Sprintf("%d/%d", st.y, st.x)})
				return
			}
			st.lastStd = point.Std
			ap.rec.stats.bars++
			stepZ := point.Z
			if !spec.DynamicExitZ {
				if fz, okZ := st.spread.EvalWithBeta(st.curLX, st.curLY, st.beta); okZ {
					stepZ = fz
				} else {
					stepZ = math.NaN()
				}
			}
			ap.stepZ = stepZ
			prev := st.pos
			st.pos = st.trader.Step(stepZ)
			if st.pos == 0 {
				st.beta = 0
			}
			ap.rec.stats.observeStep(prev, st.pos, stepZ, st.beta, frame.DecisionTime, spec.Band)
		}

		for _, ap := range retiring {
			prepareAndStepOpen(ap)
		}
		for _, ap := range current {
			prepareAndStepOpen(ap)
		}

		isReselect := (decisionBars-spec.FormationBars)%spec.ReselectBars == 0
		if isReselect {
			selected := selectRollingPairs(spec, candidates, hist, histHead, histCap, tradable)
			selectedMap := make(map[Pair]scoredCandidatePair, len(selected))
			for _, sc := range selected {
				selectedMap[Pair{Y: sc.ySym, X: sc.xSym}] = sc
			}
			existingMap := make(map[Pair]*activeRollingPair, len(retiring)+len(current))
			var nextRetiring []*activeRollingPair
			handleOld := func(ap *activeRollingPair) {
				if _, keep := selectedMap[ap.pair]; keep {
					ap.exitOnly = false
					existingMap[ap.pair] = ap
					return
				}
				if ap.state.pos == 0 {
					if ap.state.prevPos != 0 {
						recordZeroWeight(ap)
					}
					return
				}
				if spec.HandoffMode == "immediate" {
					ap.rec.stats.closeEpisode(frame.DecisionTime, fin(ap.stepZ), "reselect")
					ap.state.pos, ap.state.beta = 0, 0
					ap.state.trader.Reset()
					recordZeroWeight(ap)
					return
				}
				ap.exitOnly = true
				nextRetiring = append(nextRetiring, ap)
			}
			for _, ap := range retiring {
				handleOld(ap)
			}
			for _, ap := range current {
				handleOld(ap)
			}

			nextCurrent := make([]*activeRollingPair, 0, len(selected))
			for _, sc := range selected {
				pair := Pair{Y: sc.ySym, X: sc.xSym}
				if ap, exists := existingMap[pair]; exists {
					nextCurrent = append(nextCurrent, ap)
					continue
				}
				rs, err := statistics.NewRollingSpread(spec.Window)
				if err != nil {
					return nil, nil, err
				}
				bt, err := statistics.NewBandTrader(spec.Band)
				if err != nil {
					return nil, nil, err
				}
				yIdx, xIdx := sidToIdx[sc.y], sidToIdx[sc.x]
				prewarmSpread(rs, yIdx, xIdx, lastSlot)
				rec := tracker.getOrCreate(pair)
				rec.stats.prevOK = false
				ap := &activeRollingPair{
					pair:  pair,
					rec:   rec,
					state: pairState{y: sc.y, x: sc.x, spread: rs, trader: bt},
				}
				nextCurrent = append(nextCurrent, ap)
			}
			retiring = nextRetiring
			current = nextCurrent
			if len(selected) == 0 {
				diags = append(diags, factor.Diagnostic{Code: "rolling-pairs-empty-selection", Detail: "no candidate pair passed formation filters"})
			}
		}

		usedSlots := 0
		retiringUnordered := make(map[[2]int32]bool, len(retiring))
		for _, ap := range retiring {
			if ap.state.pos != 0 {
				usedSlots++
				lo, hi := min(ap.state.y, ap.state.x), max(ap.state.y, ap.state.x)
				retiringUnordered[[2]int32{lo, hi}] = true
			}
		}
		for _, ap := range current {
			if ap.state.pos != 0 {
				usedSlots++
			}
		}

		for _, ap := range current {
			if ap.observedThisBar {
				continue
			}
			st := &ap.state
			if !st.curOK {
				ly, okY := values[st.y]
				lx, okX := values[st.x]
				if !okY || !okX || ly.Validity != factor.Valid || lx.Validity != factor.Valid || !finitePairFloat(ly.Value) || !finitePairFloat(lx.Value) {
					continue
				}
				st.curLY, st.curLX, st.curOK = ly.Value, lx.Value, true
				ap.rec.stats.accrue(ly.Value, lx.Value, navPrev)
			}
			point, ok := st.spread.Observe(st.curLX, st.curLY)
			ap.observedThisBar = true
			if !ok {
				diags = append(diags, factor.Diagnostic{Code: "pair-warmup", Detail: fmt.Sprintf("%d/%d", st.y, st.x)})
				continue
			}
			st.lastStd = point.Std
			ap.rec.stats.bars++
			if !tradable[st.y] || !tradable[st.x] {
				continue
			}
			lo, hi := min(st.y, st.x), max(st.y, st.x)
			canOpen := usedSlots < spec.PairCount && !retiringUnordered[[2]int32{lo, hi}] && betaAllowed(spec.MinBeta, spec.AllowNegativeBeta, point.Beta)
			if !canOpen {
				if math.Abs(point.Z) <= spec.Band.Exit {
					st.pos = st.trader.Step(point.Z)
				}
				continue
			}
			prev := st.pos
			st.pos = st.trader.Step(point.Z)
			if st.pos != 0 {
				st.beta = point.Beta
				usedSlots++
			}
			ap.rec.stats.observeStep(prev, st.pos, point.Z, st.beta, frame.DecisionTime, spec.Band)
		}

		combined := make([]*activeRollingPair, 0, len(retiring)+len(current))
		combined = append(combined, retiring...)
		combined = append(combined, current...)

		var pairGross []float64
		if spec.WeightMode == "inverse_vol" && len(combined) > 0 {
			states := make([]*pairState, len(combined))
			retiringOpenCount := 0
			for i, ap := range combined {
				states[i] = &ap.state
				if ap.exitOnly && ap.state.pos != 0 {
					retiringOpenCount++
				}
			}
			slotBudget := min(spec.PairCount, retiringOpenCount+len(current))
			totalGross := math.Min(1.0, spec.Gross*float64(slotBudget))
			capPerPair := spec.MaxPairGross
			if capPerPair == 0 {
				capPerPair = math.Min(totalGross, 2*spec.Gross)
			}
			pairGross = allocateInverseVolGross(states, totalGross, capPerPair)
		}

		wyList := make([]float64, len(combined))
		wxList := make([]float64, len(combined))
		idealWY := make([]float64, len(combined))
		idealWX := make([]float64, len(combined))
		buffered := make([]bool, len(combined))

		for i, ap := range combined {
			st := &ap.state
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
				if math.Abs(iwY-ap.preWY)+math.Abs(iwX-ap.preWX) < spec.DriftBand {
					wy, wx = ap.preWY, ap.preWX
					buffered[i] = true
				}
			}
			wyList[i], wxList[i] = wy, wx
		}

		totalG := 0.0
		for i := range combined {
			totalG += math.Abs(wyList[i]) + math.Abs(wxList[i])
		}
		if totalG > 1+1e-12 {
			for i := range combined {
				if buffered[i] && math.Abs(wyList[i])+math.Abs(wxList[i]) > math.Abs(idealWY[i])+math.Abs(idealWX[i]) {
					wyList[i], wxList[i] = idealWY[i], idealWX[i]
					buffered[i] = false
				}
			}
		}

		weights := map[int32]float64{}
		for i, ap := range combined {
			st := &ap.state
			wy, wx := wyList[i], wxList[i]
			if wy != 0 || wx != 0 {
				weights[st.y] += wy
				weights[st.x] += wx
			}
			if spec.DriftBand > 0 {
				ap.rec.stats.setWeightsFrom(wy, wx, ap.preWY, ap.preWX)
			} else {
				ap.rec.stats.setWeights(wy, wx)
			}
			if st.pos != 0 && st.curOK {
				st.holdWY, st.holdWX = wy, wx
				st.holdLY, st.holdLX = st.curLY, st.curLX
				st.hasHold = true
			} else if st.pos == 0 {
				st.holdWY, st.holdWX, st.hasHold = 0, 0, false
			}
		}

		if len(retiring) > 0 {
			kept := retiring[:0]
			for _, ap := range retiring {
				if ap.state.pos != 0 {
					kept = append(kept, ap)
				}
			}
			retiring = kept
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

// selectRollingPairs screens all candidate pairs on the trailing FormationBars ending at
// (histHead - 1 + histCap) % histCap (inclusive, strictly <= current decision bar) and
// greedily picks up to PairCount pairs subject to MaxPerSymbol.
func selectRollingPairs(
	spec RollingPairsSpec,
	candidates []candidateAsset,
	hist [][]float64,
	histHead, histCap int,
	tradable map[int32]bool,
) []scoredCandidatePair {
	fBars := spec.FormationBars
	startSlot := (histHead - fBars + histCap) % histCap
	lastSlot := (histHead - 1 + histCap) % histCap

	// Pre-filter candidate indices: must be tradable and have a valid observation at the current decision bar.
	eligible := make([]int, 0, len(candidates))
	for idx, c := range candidates {
		if tradable[c.sid] && finitePairFloat(hist[idx][lastSlot]) {
			eligible = append(eligible, idx)
		}
	}
	if len(eligible) < 2 {
		return nil
	}

	minValidBars := max(int(math.Ceil(spec.MinCoverage*float64(fBars)-1e-12)), spec.ADFLags+6, 6)
	var scored []scoredCandidatePair
	aVals := make([]float64, 0, fBars)
	bVals := make([]float64, 0, fBars)

	for ii := 0; ii < len(eligible); ii++ {
		i := eligible[ii]
		ca := candidates[i]
		rowA := hist[i]
		for jj := ii + 1; jj < len(eligible); jj++ {
			j := eligible[jj]
			cb := candidates[j]
			rowB := hist[j]

			aVals = aVals[:0]
			bVals = bVals[:0]
			var (
				retCount                   int
				sumDA, sumDB               float64
				saa, sbb, sab              float64
				prevA, prevB               float64
				prevBothValid              bool
				firstNormA, firstNormB     float64
				hasFirstNorm               bool
				ssdSum                     float64
				ssdOverflow                bool
			)
			for k := 0; k < fBars; k++ {
				slot := (startSlot + k) % histCap
				la, lb := rowA[slot], rowB[slot]
				if !finitePairFloat(la) || !finitePairFloat(lb) {
					prevBothValid = false
					continue
				}
				aVals = append(aVals, la)
				bVals = append(bVals, lb)
				if prevBothValid {
					da := la - prevA
					db := lb - prevB
					retCount++
					dda := da - sumDA
					sumDA += dda / float64(retCount)
					ddb := db - sumDB
					sumDB += ddb / float64(retCount)
					saa += dda * (da - sumDA)
					sbb += ddb * (db - sumDB)
					sab += dda * (db - sumDB)
				}
				prevA, prevB, prevBothValid = la, lb, true

				if spec.SelectionMethod == "distance" {
					if !hasFirstNorm {
						firstNormA, firstNormB, hasFirstNorm = la, lb, true
					} else {
						na := math.Exp(la - firstNormA)
						nb := math.Exp(lb - firstNormB)
						diff := na - nb
						term := diff * diff
						if !finitePairFloat(term) {
							ssdOverflow = true
						} else {
							ssdSum += term
						}
					}
				}
			}

			if len(aVals) < minValidBars || retCount < 3 || saa <= 1e-24 || sbb <= 1e-24 {
				continue
			}
			corr := sab / math.Sqrt(saa*sbb)
			if !finitePairFloat(corr) {
				continue
			}
			if spec.SelectionMethod == "cointegration" {
				if corr < spec.MinCorr {
					continue
				}
				cand, ok := evaluateCointegrationPair(spec, ca, cb, aVals, bVals)
				if ok {
					scored = append(scored, cand)
				}
			} else {
				if ssdOverflow || !finitePairFloat(ssdSum) {
					continue
				}
				if spec.MinCorr > 0 {
					if corr < spec.MinCorr {
						continue
					}
				} else if corr <= 0 {
					continue
				}
				cand, ok := evaluateDistancePair(spec, ca, cb, aVals, bVals, ssdSum, fBars)
				if ok {
					scored = append(scored, cand)
				}
			}
		}
	}

	if len(scored) == 0 {
		return nil
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score < scored[j].score
		}
		if scored[i].ySym != scored[j].ySym {
			return scored[i].ySym < scored[j].ySym
		}
		if scored[i].xSym != scored[j].xSym {
			return scored[i].xSym < scored[j].xSym
		}
		if scored[i].y != scored[j].y {
			return scored[i].y < scored[j].y
		}
		return scored[i].x < scored[j].x
	})

	selected := make([]scoredCandidatePair, 0, min(spec.PairCount, len(scored)))
	symCounts := make(map[int32]int, spec.PairCount*2)
	for _, c := range scored {
		if len(selected) >= spec.PairCount {
			break
		}
		if symCounts[c.y] >= spec.MaxPerSymbol || symCounts[c.x] >= spec.MaxPerSymbol {
			continue
		}
		selected = append(selected, c)
		symCounts[c.y]++
		symCounts[c.x]++
	}
	return selected
}

func evaluateCointegrationPair(spec RollingPairsSpec, ca, cb candidateAsset, aVals, bVals []float64) (scoredCandidatePair, bool) {
	testDir := func(yVals, xVals []float64) (adfStat float64, ok bool) {
		exposures := make([][]float64, len(xVals))
		for k, xv := range xVals {
			exposures[k] = []float64{xv}
		}
		fit, adf, err := statistics.EngleGranger(yVals, exposures, true, spec.ADFLags)
		if err != nil || len(fit.Beta) == 0 {
			return 0, false
		}
		if !betaAllowed(spec.MinBeta, spec.AllowNegativeBeta, fit.Beta[0]) {
			return 0, false
		}
		if !finitePairFloat(adf.TStatistic) || adf.TStatistic > spec.MaxADF {
			return 0, false
		}
		ou, err := statistics.FitOU(fit.Residuals)
		if err != nil || !finitePairFloat(ou.DiscreteHalfLife) {
			return 0, false
		}
		if ou.DiscreteHalfLife < spec.MinHalfLife || ou.DiscreteHalfLife > spec.MaxHalfLife {
			return 0, false
		}
		return adf.TStatistic, true
	}

	statAB, okAB := testDir(aVals, bVals)
	statBA, okBA := testDir(bVals, aVals)
	switch {
	case okAB && okBA:
		if statBA < statAB {
			return scoredCandidatePair{y: cb.sid, x: ca.sid, ySym: cb.symbol, xSym: ca.symbol, score: statBA}, true
		}
		return scoredCandidatePair{y: ca.sid, x: cb.sid, ySym: ca.symbol, xSym: cb.symbol, score: statAB}, true
	case okAB:
		return scoredCandidatePair{y: ca.sid, x: cb.sid, ySym: ca.symbol, xSym: cb.symbol, score: statAB}, true
	case okBA:
		return scoredCandidatePair{y: cb.sid, x: ca.sid, ySym: cb.symbol, xSym: ca.symbol, score: statBA}, true
	default:
		return scoredCandidatePair{}, false
	}
}

func evaluateDistancePair(spec RollingPairsSpec, ca, cb candidateAsset, aVals, bVals []float64, ssdSum float64, fBars int) (scoredCandidatePair, bool) {
	olsSlopeAndStd := func(yVals, xVals []float64) (beta, std float64, ok bool) {
		n := len(xVals)
		if n < 3 {
			return 0, 0, false
		}
		ox, oy := xVals[0], yVals[0]
		var sx, sy, sxx, sxy, syy float64
		for k := 0; k < n; k++ {
			cx, cy := xVals[k]-ox, yVals[k]-oy
			sx += cx
			sy += cy
			sxx += cx * cx
			sxy += cx * cy
			syy += cy * cy
		}
		nf := float64(n)
		mx, my := sx/nf, sy/nf
		vx := sxx - nf*mx*mx
		if vx <= 1e-12 {
			return 0, 0, false
		}
		cov := sxy - nf*mx*my
		beta = cov / vx
		ssr := (syy - nf*my*my) - beta*cov
		if ssr < 0 {
			ssr = 0
		}
		std = math.Sqrt(ssr / (nf - 2))
		if !finitePairFloat(beta) || !finitePairFloat(std) || std <= 1e-12 {
			return 0, 0, false
		}
		if !betaAllowed(spec.MinBeta, spec.AllowNegativeBeta, beta) {
			return 0, 0, false
		}
		return beta, std, true
	}

	betaAB, _, okAB := olsSlopeAndStd(aVals, bVals)
	betaBA, _, okBA := olsSlopeAndStd(bVals, aVals)
	if !okAB && !okBA {
		return scoredCandidatePair{}, false
	}
	scaledSSD := ssdSum * (float64(fBars) / float64(len(aVals)))
	if !finitePairFloat(scaledSSD) {
		return scoredCandidatePair{}, false
	}
	switch {
	case okAB && okBA:
		if math.Abs(betaBA-1) < math.Abs(betaAB-1) {
			return scoredCandidatePair{y: cb.sid, x: ca.sid, ySym: cb.symbol, xSym: ca.symbol, score: scaledSSD}, true
		}
		return scoredCandidatePair{y: ca.sid, x: cb.sid, ySym: ca.symbol, xSym: cb.symbol, score: scaledSSD}, true
	case okAB:
		return scoredCandidatePair{y: ca.sid, x: cb.sid, ySym: ca.symbol, xSym: cb.symbol, score: scaledSSD}, true
	default:
		return scoredCandidatePair{y: cb.sid, x: ca.sid, ySym: cb.symbol, xSym: ca.symbol, score: scaledSSD}, true
	}
}
