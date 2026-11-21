package runner

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
	"github.com/banbox/banbot/orm"
)

func TestPairsFilterAndCopulaSpecValidation(t *testing.T) {
	base := PairsSpec{
		Pairs:  []Pair{{Y: "Y", X: "X"}},
		Window: 20,
		Band:   statistics.BandConfig{Entry: 1.5, Exit: 0.3},
	}
	sids := map[int32]string{1: "Y", 2: "X"}
	bad := []PairsSpec{
		func() PairsSpec {
			s := base
			s.SpreadModel = "copula"
			s.Copula = statistics.CopulaConfig{Family: "student_t"}
			return s
		}(),
		func() PairsSpec {
			s := base
			s.SpreadModel = "copula"
			s.Copula = statistics.CopulaConfig{LowerProb: 0.6}
			return s
		}(),
		func() PairsSpec {
			s := base
			s.SpreadModel = "copula"
			s.Copula = statistics.CopulaConfig{LowerProb: 0.10, UpperProb: 0.90, ExitBand: 0.42}
			return s
		}(),
		func() PairsSpec { s := base; s.MinRollingCorr = -0.1; return s }(),
		func() PairsSpec { s := base; s.MinRollingCorr = 1.1; return s }(),
		func() PairsSpec { s := base; s.MinRollingCorr = 0.5; s.CorrWindow = 3; return s }(),
		func() PairsSpec { s := base; s.MinHurst = -0.1; return s }(),
		func() PairsSpec { s := base; s.MaxHurst = 1.2; return s }(),
		func() PairsSpec { s := base; s.MinHurst = 0.6; s.MaxHurst = 0.4; return s }(),
		func() PairsSpec { s := base; s.Window = 6; s.MaxHurst = 0.5; return s }(),
		func() PairsSpec { s := base; s.SpreadRSIPeriod = 1; return s }(),
		func() PairsSpec { s := base; s.SpreadRSIPeriod = 14; s.Window = 14; return s }(),
		func() PairsSpec {
			s := base
			s.SpreadRSIPeriod = 10
			s.SpreadRSILower = 70
			s.SpreadRSIUpper = 30
			return s
		}(),
		func() PairsSpec { s := base; s.FeatureExprs = map[string]string{"": "kline.close"}; return s }(),
	}
	for i, spec := range bad {
		if _, err := newPairsBuilder(spec, sids); err == nil {
			t.Fatalf("case %d accepted invalid spec: %+v", i, spec)
		}
	}
}

// TestPairsCopulaAllFamiliesAndPnLReconciliation verifies SpreadModel: "copula" across
// empirical, gaussian, clayton, gumbel, frank, and auto copulas in full Weights Run()
// replays (including omitted Band.Entry/Exit defaults and RollingPairsSpec integration),
// asserting exact Attribution.Residual and PairsResidual == -costs reconciliation.
func TestPairsCopulaAllFamiliesAndPnLReconciliation(t *testing.T) {
	sids := map[int32]string{1: "ETH", 2: "BTC"}
	rng := rand.New(rand.NewSource(202611))
	const totalBars = 160
	prices := map[int32][]float64{
		1: make([]float64, totalBars+1),
		2: make([]float64, totalBars+1),
	}
	lBtc, u := math.Log(50000), 0.0
	for bar := 1; bar <= totalBars; bar++ {
		lBtc += 0.04*math.Sin(float64(bar)/7.0) + rng.NormFloat64()*0.015
		if bar%18 == 0 {
			u = 0.14 // periodic extreme mispricing shock triggers copula tail entry
		} else if bar%18 == 9 {
			u = -0.14
		} else {
			u = 0.35*u + rng.NormFloat64()*0.01
		}
		prices[1][bar] = math.Exp(1.2*(lBtc-math.Log(50000)) + math.Log(3000) + u)
		prices[2][bar] = math.Exp(lBtc)
	}

	families := []statistics.CopulaFamily{
		statistics.CopulaEmpirical,
		statistics.CopulaGaussian,
		statistics.CopulaClayton,
		statistics.CopulaGumbel,
		statistics.CopulaFrank,
		statistics.CopulaAuto,
	}
	for _, fam := range families {
		stratName := fmt.Sprintf("test-pairs-copula-%s", fam)
		famCopy := fam
		if err := RegisterPairs(stratName, func(p *Params) (PairsSpec, error) {
			return PairsSpec{
				Pairs:       []Pair{{Y: "ETH", X: "BTC"}},
				Window:      24,
				SpreadModel: "copula",
				DriftBand:   0.04,
				Copula: statistics.CopulaConfig{
					Family:    famCopy,
					LowerProb: 0.08,
					UpperProb: 0.92,
					ExitBand:  0.05,
				},
				// Band.Entry and Band.Exit intentionally omitted (0) to verify CopulaConfig-driven defaults!
				Band: statistics.BandConfig{MaxHold: 15},
			}, p.Err()
		}); err != nil {
			t.Fatalf("%s register: %v", fam, err)
		}
		cfg := makeMultiSymbolReplayConfig(t, stratName, sids, totalBars, func(bar int64, sid int32) (float64, bool) {
			return prices[sid][bar], true
		})
		res, err := Run(context.Background(), cfg, nil, nil)
		if err != nil {
			t.Fatalf("%s Run: %v", fam, err)
		}
		if len(res.Pairs) != 1 || res.Pairs[0].Trades == 0 {
			t.Fatalf("%s expected copula pair trades, got %+v", fam, res.Pairs)
		}
		if math.Abs(res.Attribution.Residual) > 1e-9*res.Attribution.InitialNAV {
			t.Fatalf("%s attribution residual out of tolerance: %v", fam, res.Attribution.Residual)
		}
		costs := (res.Attribution.Fees + res.Attribution.Slippage + res.Attribution.Funding + res.Attribution.PITCost) / res.Attribution.InitialNAV
		if math.Abs(res.PairsResidual+costs) > 1e-6 {
			t.Fatalf("%s PairsResidual (%v) failed reconciliation with -costs (%v)", fam, res.PairsResidual, -costs)
		}
	}

	// Verify RegisterRollingPairs with SpreadModel: "copula" + CopulaAuto.
	rollSIDs := map[int32]string{1: "ETH", 2: "BTC", 3: "SOL"}
	solPrices := make([]float64, totalBars+1)
	lSol := math.Log(150)
	for bar := 1; bar <= totalBars; bar++ {
		lSol += rng.NormFloat64() * 0.04
		solPrices[bar] = math.Exp(lSol)
	}
	const rollName = "test-rolling-copula-auto"
	if err := RegisterRollingPairs(rollName, func(p *Params) (RollingPairsSpec, error) {
		return RollingPairsSpec{
			FormationBars: 40,
			ReselectBars:  30,
			PairCount:     1,
			MaxPerSymbol:  1,
			Window:        24,
			SpreadModel:   "copula",
			Copula:        statistics.CopulaConfig{Family: statistics.CopulaAuto, LowerProb: 0.08, UpperProb: 0.92, ExitBand: 0.05},
			Band:          statistics.BandConfig{MaxHold: 15},
		}, p.Err()
	}); err != nil {
		t.Fatal(err)
	}
	rCfg := makeMultiSymbolReplayConfig(t, rollName, rollSIDs, totalBars, func(bar int64, sid int32) (float64, bool) {
		if sid == 3 {
			return solPrices[bar], true
		}
		return prices[sid][bar], true
	})
	rRes, err := Run(context.Background(), rCfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rRes.Pairs) == 0 || rRes.Pairs[0].Trades == 0 {
		t.Fatalf("expected rolling copula trades, got %+v", rRes.Pairs)
	}
	if math.Abs(rRes.Attribution.Residual) > 1e-9*rRes.Attribution.InitialNAV {
		t.Fatalf("rolling copula attribution residual: %v", rRes.Attribution.Residual)
	}
}

// TestPairsMinRollingCorrEntryGateNeverBlocksExit verifies MinRollingCorr + CorrWindow:
// 1. Opens normally when 1-bar return correlation >= MinRollingCorr;
// 2. When correlation collapses mid-trade, NEVER blocks exiting/stopping the open position;
// 3. Blocks new entries while correlation remains below MinRollingCorr;
// 4. Resumes opening once correlation recovers >= MinRollingCorr.
func TestPairsMinRollingCorrEntryGateNeverBlocksExit(t *testing.T) {
	sids := map[int32]string{1: "Y", 2: "X"}
	band := statistics.BandConfig{Entry: 1.3, Exit: 0.25}

	bGated, trGated, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:          []Pair{{Y: "Y", X: "X"}},
		Window:         10,
		MinRollingCorr: 0.75,
		CorrWindow:     8,
		Gross:          1.0,
		Band:           band,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}
	bUngated, _, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:  []Pair{{Y: "Y", X: "X"}},
		Window: 10,
		Gross:  1.0,
		Band:   band,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}

	// Phase 1 (bars 1..10): strongly correlated 1-bar log returns (corr > 0.90), dislocation at bar 10 -> opens long spread (+1).
	var wGate, wUn map[int32]float64
	lx := 3.0
	for i := 1; i <= 10; i++ {
		step := 0.08 * float64((i%2)*2-1)
		lx += step
		ly := lx + 0.002*float64(i%3)
		if i == 10 {
			ly -= 0.045 // step on bar 10 is -0.08, so dy = -0.125 keeps 1-bar return corr > 0.92 while z <= -Entry!
		}
		lp := map[int32]float64{1: ly, 2: lx}
		wGate, _ = feedPairsBuilder(t, bGated, uint64(i), 1000, lp, []int32{1, 2})
		wUn, _ = feedPairsBuilder(t, bUngated, uint64(i), 1000, lp, []int32{1, 2})
	}
	if len(wGate) != 2 || len(wUn) != 2 {
		t.Fatalf("Phase 1 expected both builders to open under high corr: gate=%v un=%v", wGate, wUn)
	}

	// Phase 2 (bars 11..18): returns move in opposite directions (8-bar return corr < 0 < 0.75)
	// while keeping frozen-beta residual outside Exit until bar 18, then reverting to mean -> MUST EXIT!
	for i := 11; i <= 18; i++ {
		dx := 0.01 * float64((i%2)*2-1)
		lx += dx
		var ly float64
		if i < 18 {
			ly = (lx - 2*dx) - 0.045
		} else {
			ly = lx
		}
		lp := map[int32]float64{1: ly, 2: lx}
		wGate, _ = feedPairsBuilder(t, bGated, uint64(i), 1000, lp, []int32{1, 2})
		wUn, _ = feedPairsBuilder(t, bUngated, uint64(i), 1000, lp, []int32{1, 2})
	}
	if len(wGate) != 0 {
		t.Fatalf("MinRollingCorr gate must NEVER block exiting an open position, got w=%v", wGate)
	}
	if trGated.report(1000)[0].Trades != 1 {
		t.Fatalf("expected 1 completed episode after exit, got %+v", trGated.report(1000)[0])
	}

	// Phase 3: establish positive 10-bar OLS level slope (anchors at 3.00 and 3.30), then feed 7 anti-correlated
	// 1-bar returns (rolling out the initial jump from the 8-bar CorrWindow) ending with a spread spike -> GATED!
	xLevels := []float64{3.00, 3.30, 3.33, 3.30, 3.33, 3.30, 3.33, 3.30, 3.31}
	yLevels := []float64{3.00, 3.30, 3.27, 3.30, 3.27, 3.30, 3.27, 3.30, 3.39}
	for j := range xLevels {
		lp := map[int32]float64{1: yLevels[j], 2: xLevels[j]}
		wGate, _ = feedPairsBuilder(t, bGated, uint64(19+j), 1000, lp, []int32{1, 2})
		wUn, _ = feedPairsBuilder(t, bUngated, uint64(19+j), 1000, lp, []int32{1, 2})
	}
	if len(wUn) != 2 {
		t.Fatalf("expected ungated builder to open on bar 27 dislocation, got %v", wUn)
	}
	if len(wGate) != 0 {
		t.Fatalf("expected MinRollingCorr=0.75 to block new entry under low return correlation, got %v", wGate)
	}

	// Phase 4: restore strongly positive 1-bar return correlation (> 0.9) and trigger dislocation -> opens again!
	for j := 1; j <= 10; j++ {
		step := 0.06 * float64((j%2)*2-1)
		x := 3.25 + step
		y := x
		if j == 10 {
			y -= 0.05 // same sign as negative step on bar 10 -> corr > 0.95 and z <= -2.5!
		}
		wGate, _ = feedPairsBuilder(t, bGated, uint64(28+j), 1000, map[int32]float64{1: y, 2: x}, []int32{1, 2})
	}
	if len(wGate) != 2 {
		t.Fatalf("expected MinRollingCorr builder to reopen once return correlation recovered, got %v", wGate)
	}
}

// TestPairsHurstAndSpreadRSIEntryGatesNeverBlockExit verifies MaxHurst/MinHurst and
// SpreadRSIPeriod/SpreadRSIUpper/SpreadRSILower entry gating and unobstructed exits.
func TestPairsHurstAndSpreadRSIEntryGatesNeverBlockExit(t *testing.T) {
	sids := map[int32]string{1: "Y", 2: "X"}
	band := statistics.BandConfig{Entry: 1.25, Exit: 0.25, Stop: 4.5}

	// 1. MaxHurst = 0.45 gate: blocks smooth trending divergence (Hurst > 0.7), allows fast-reverting spread (Hurst < 0.35),
	//    and never blocks closing an open trade when spread subsequently trends.
	bHurst, trHurst, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:    []Pair{{Y: "Y", X: "X"}},
		Window:   20,
		MaxHurst: 0.45,
		Gross:    1.0,
		Band:     band,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}
	bNoHurst, _, _ := newTrackedPairsBuilder(PairsSpec{
		Pairs: []Pair{{Y: "Y", X: "X"}}, Window: 20, Gross: 1.0, Band: band,
	}, sids)

	var wH, wNoH map[int32]float64
	for i := 1; i <= 20; i++ {
		x := 4.0 + 0.05*math.Sin(float64(i))
		u := 0.0012 * math.Pow(float64(i), 1.7) // smooth persistent upward trend -> Hurst > 0.8
		lp := map[int32]float64{1: x + u, 2: x}
		wH, _ = feedPairsBuilder(t, bHurst, uint64(i), 1000, lp, []int32{1, 2})
		wNoH, _ = feedPairsBuilder(t, bNoHurst, uint64(i), 1000, lp, []int32{1, 2})
	}
	if len(wNoH) != 2 || len(wH) != 0 {
		t.Fatalf("MaxHurst gate failed on trending spread: noHurst=%v hurst=%v", wNoH, wH)
	}

	// Feed 20 alternating mean-reverting bars (Hurst < 0.25) orthogonal to x, ending with a negative excursion -> opens +1!
	for i := 21; i <= 40; i++ {
		x := 4.0 + 0.08*math.Sin(float64(i)*1.3)
		u := 0.012 * float64((i%2)*2-1)
		if i == 40 {
			u = -0.05 // sharp 1-bar drop while 20-bar alternating spread keeps Hurst < 0.45
		}
		wH, _ = feedPairsBuilder(t, bHurst, uint64(i), 1000, map[int32]float64{1: x + u, 2: x}, []int32{1, 2})
	}
	if len(wH) != 2 || wH[1] <= 0 {
		t.Fatalf("expected MaxHurst builder to open long spread on mean-reverting window, got %v", wH)
	}
	tradesBeforeExit := trHurst.report(1000)[0].Trades
	// Drive spread back to exact equilibrium y == x -> exit is NEVER blocked!
	wExit, _ := feedPairsBuilder(t, bHurst, 41, 1000, map[int32]float64{1: 4.00, 2: 4.00}, []int32{1, 2})
	if len(wExit) != 0 || trHurst.report(1000)[0].Trades < 1 || trHurst.states[0].pos != 0 {
		t.Fatalf("MaxHurst must never block exit (before=%d): w=%v rep=%+v", tradesBeforeExit, wExit, trHurst.report(1000)[0])
	}

	// 2. SpreadRSI gate (reversion vs momentum).
	bRSI, trRSI, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:           []Pair{{Y: "Y", X: "X"}},
		Window:          12,
		SpreadRSIPeriod: 5,
		SpreadRSIUpper:  80,
		SpreadRSILower:  20,
		Gross:           1.0,
		Band:            statistics.BandConfig{Entry: 1.1, Exit: 0.2},
	}, sids)
	if err != nil {
		t.Fatal(err)
	}
	// Construct 12 bars where bar 12 has z >= 1.1 (above Entry), but RSI(5) is moderated (~62 < 80) because of a dip at bar 11!
	modResiduals := []float64{0, -0.01, 0, -0.01, 0, -0.01, 0.01, 0.02, 0.03, 0.035, 0.005, 0.032}
	for i, r := range modResiduals {
		x := 2.0 + 0.04*float64(i%2)
		w, _ := feedPairsBuilder(t, bRSI, uint64(i+1), 1000, map[int32]float64{1: x + r, 2: x}, []int32{1, 2})
		if i == len(modResiduals)-1 && len(w) != 0 {
			t.Fatalf("expected SpreadRSIUpper=80 to block entry when RSI < 80, got %v", w)
		}
	}
	// Consecutive upward spread gains push RSI(5) > 85 and z > Entry -> opens short spread (-1)!
	for j, r := range []float64{0.045, 0.060, 0.080} {
		x := 2.0 + 0.04*float64(j%2)
		wH, _ = feedPairsBuilder(t, bRSI, uint64(20+j), 1000, map[int32]float64{1: x + r, 2: x}, []int32{1, 2})
	}
	if len(wH) != 2 || wH[1] >= 0 {
		t.Fatalf("expected short spread entry once SpreadRSI >= 80, got %v", wH)
	}
	// Reversion to window mean closes position regardless of RSI!
	wClose, _ := feedPairsBuilder(t, bRSI, 30, 1000, map[int32]float64{1: 2.02 + 0.025, 2: 2.02}, []int32{1, 2})
	if len(wClose) != 0 || trRSI.report(1000)[0].Trades != 1 {
		t.Fatalf("SpreadRSI must never block exit: w=%v rep=%+v", wClose, trRSI.report(1000)[0])
	}
}

// TestPairsExtraFeaturesAndEntryFilterZeroLookahead verifies:
// 1. compilePairsPlan preserves exact Plan.Hash() and CSE when ExtraFeatures == nil && FeatureExprs == nil;
// 2. ExtraFeatures (custom per-leg volume/RSI nodes) + FeatureExprs + EntryFilter work end-to-end
//    in both RegisterPairs and RegisterRollingPairs;
// 3. Strict zero lookahead: mutating future bars > t leaves PairFilterContext and TargetPortfolio
//    weights on bars <= t bit-for-bit identical.
func TestPairsExtraFeaturesAndEntryFilterZeroLookahead(t *testing.T) {
	// 1. CSE / Plan.Hash() preservation when ExtraFeatures == nil.
	basePlan, _, err := compilePairsLogPlan("close", "1h")
	if err != nil {
		t.Fatal(err)
	}
	samePlan, _, err := compilePairsPlan("close", "1h", nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if basePlan.Hash() != samePlan.Hash() || basePlan.NodeCount() != samePlan.NodeCount() {
		t.Fatalf("nil ExtraFeatures altered Plan identity: %s (%d) vs %s (%d)",
			basePlan.Hash(), basePlan.NodeCount(), samePlan.Hash(), samePlan.NodeCount())
	}

	// Verify CSE deduplicates f.Close() with the internal logp kline.close node!
	featPlan, _, err := compilePairsPlan("close", "1h", nil, func(f FactorContext) map[string]*factor.Node {
		return map[string]*factor.Node{
			"rsi":     factor.RSI(f.Close(), 6),
			"vol_sma": factor.SMA(f.Field("volume"), 4),
			"vol":     f.Field("volume"),
		}
	}, map[string]string{
		"mom3": "ts.roc(kline.close, 3)",
	})
	if err != nil {
		t.Fatal(err)
	}
	inputs := featPlan.Inputs()
	if len(inputs) != 1 || !reflect.DeepEqual(inputs[0].Fields, []string{"close", "volume"}) {
		t.Fatalf("unexpected compiled Plan.Inputs(): %+v", inputs)
	}

	// 2. End-to-end Run() + strict zero-lookahead test for RegisterPairs with ExtraFeatures + FeatureExprs + EntryFilter.
	type filterObs struct {
		At    int64
		Side  int
		VolY  float64
		RSIY  float64
		MomX  float64
		Corr  float64
		Hurst float64
	}
	runWithFutureScale := func(stratName string, futureMult float64, gateLowVol bool) (Result, []filterObs, map[int64]map[int32]float64) {
		var observed []filterObs
		err := RegisterPairs(stratName, func(p *Params) (PairsSpec, error) {
			return PairsSpec{
				Pairs:  []Pair{{Y: "ETH", X: "BTC"}},
				Window: 16,
				Band:   statistics.BandConfig{Entry: 1.3, Exit: 0.25, MaxHold: 10},
				ExtraFeatures: func(f FactorContext) map[string]*factor.Node {
					return map[string]*factor.Node{
						"rsi":     factor.RSI(f.Close(), 6),
						"vol":     f.Field("volume"),
						"vol_sma": factor.SMA(f.Field("volume"), 5),
					}
				},
				FeatureExprs: map[string]string{
					"roc3": "ts.roc(kline.close, 3)",
				},
				EntryFilter: func(ctx PairFilterContext) bool {
					volY, okVY := ctx.FeatureY("vol")
					volSmaY, okSY := ctx.FeatureY("vol_sma")
					rsiY, okRY := ctx.FeatureY("rsi")
					rocX, okRX := ctx.FeatureX("roc3")
					if !okVY || !okSY || !okRY || !okRX {
						return false
					}
					if ctx.DecisionTime <= 40*3600000 {
						observed = append(observed, filterObs{
							At:    ctx.DecisionTime,
							Side:  ctx.ProposedSide,
							VolY:  volY,
							RSIY:  rsiY,
							MomX:  rocX,
							Corr:  ctx.RollingCorr,
							Hurst: ctx.Hurst,
						})
					}
					if gateLowVol && volY < 1.5*volSmaY {
						return false // only enter on leg Y volume surge >= 1.5x 5-bar SMA!
					}
					return true
				},
			}, p.Err()
		})
		if err != nil {
			t.Fatal(err)
		}

		const hour int64 = 3600000
		const totalBars = 60
		store, err := factor.NewVersionStore(2000)
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(707))
		lx, u := math.Log(2000), 0.0
		for bar := int64(1); bar <= totalBars; bar++ {
			lx += 0.025*math.Sin(float64(bar)/3.0) + rng.NormFloat64()*0.01
			volETH := 100.0
			if bar == 24 || bar == 38 || bar == 52 {
				u = 0.09 * float64((bar%4)-1)
				if u == 0 {
					u = 0.09
				}
				if bar != 24 {
					volETH = 500.0 // volume surge on bar 38 & 52; bar 24 has normal low volume (100) so gateLowVol blocks bar 24!
				}
			} else {
				u = 0.3*u + rng.NormFloat64()*0.008
			}
			py := math.Exp(lx + u)
			px := math.Exp(lx)
			volBTC := 200.0
			if bar > 40 {
				py *= futureMult
				px *= (1 + 0.5*(futureMult-1))
				volETH *= futureMult
				volBTC *= futureMult
			}
			at := bar * hour
			for _, leg := range []struct {
				sid   int32
				price float64
				vol   float64
			}{{1, py, volETH}, {2, px, volBTC}} {
				if err := store.Put(factor.VersionRecord{
					Series: orm.DataSeries{
						Source: "kline", TimeFrame: "1h", Sid: leg.sid,
						TimeMS: at - hour, EndMS: at, Closed: true,
						Values: map[string]any{"close": leg.price, "volume": leg.vol},
					},
					EventTime: at, AvailableAt: at, IngestedAt: at, Revision: 1, SourceVersion: "v1",
				}); err != nil {
					t.Fatal(err)
				}
				if err := store.Put(factor.VersionRecord{
					Series: orm.DataSeries{
						Source: "tick", TimeFrame: "event", Sid: leg.sid,
						TimeMS: at + 1, EndMS: at + 1, Closed: true,
						Values: map[string]any{"price": leg.price},
					},
					EventTime: at + 1, AvailableAt: at + 1, IngestedAt: at + 1, Revision: 1, SourceVersion: "v1",
				}); err != nil {
					t.Fatal(err)
				}
			}
		}
		path := filepath.Join(t.TempDir(), "feat_pairs.gob")
		if _, err := store.Export(path); err != nil {
			t.Fatal(err)
		}
		c := pitConfig(t, map[int32]int64{1: 1, 2: 1}, 1, 0)
		c.Plan, c.Combo, c.Definition = nil, research.ComboSpec{}, stratName
		c.Factor = research.DefaultMomentumVolConfig()
		c.Factor.TimeFrame = "1h"
		c.Snapshot.Universe = factor.Universe{Version: "two", Static: true, Investable: []int32{1, 2}, Tradable: []int32{1, 2}, Evaluation: []int32{1, 2}, Tracked: []int32{1, 2}}
		c.Snapshot.SIDMap = map[int32]string{1: "ETH", 2: "BTC"}
		c.MaxRecords, c.MaxPending, c.DecisionInterval, c.ExpiryMS = 2000, 10, hour, 60000
		c.Chunks = []Chunk{{Path: path, From: hour, To: totalBars*hour + 2}}

		out := &capture{targets: map[int64]map[int32]float64{}}
		res, err := Run(context.Background(), c, nil, out)
		if err != nil {
			t.Fatal(err)
		}
		prefixTargets := map[int64]map[int32]float64{}
		for ts, w := range out.targets {
			if ts <= 40*hour {
				prefixTargets[ts] = w
			}
		}
		return res, observed, prefixTargets
	}

	resBase, obsBase, tgtBase := runWithFutureScale("test-pairs-feat-base", 1.0, true)
	_, obsShock, tgtShock := runWithFutureScale("test-pairs-feat-shock", 50.0, true)
	resNoVolGate, _, _ := runWithFutureScale("test-pairs-feat-novol", 1.0, false)

	if len(obsBase) == 0 {
		t.Fatal("expected EntryFilter to be invoked during prefix bars 1..40")
	}
	for _, ob := range obsBase {
		if math.IsNaN(ob.Corr) || math.IsNaN(ob.Hurst) {
			t.Fatalf("expected RollingCorr and Hurst populated in PairFilterContext: %+v", ob)
		}
	}
	if !reflect.DeepEqual(obsBase, obsShock) {
		t.Fatalf("lookahead detected in PairFilterContext: %+v vs %+v", obsBase, obsShock)
	}
	if !reflect.DeepEqual(tgtBase, tgtShock) {
		t.Fatalf("lookahead detected in prefix TargetPortfolio weights: %+v vs %+v", tgtBase, tgtShock)
	}
	if resBase.Pairs[0].Trades == 0 || resBase.Pairs[0].Trades >= resNoVolGate.Pairs[0].Trades {
		t.Fatalf("expected volume surge EntryFilter to selectively filter trades: gated=%d ungated=%d",
			resBase.Pairs[0].Trades, resNoVolGate.Pairs[0].Trades)
	}

	// 3. Also verify ExtraFeatures + EntryFilter on RegisterRollingPairs.
	var rollFilterCalls int
	const rollFeatStrat = "test-rolling-pairs-extra-feat"
	if err := RegisterRollingPairs(rollFeatStrat, func(p *Params) (RollingPairsSpec, error) {
		return RollingPairsSpec{
			FormationBars: 24,
			ReselectBars:  12,
			PairCount:     1,
			MaxPerSymbol:  1,
			Window:        16,
			Band:          statistics.BandConfig{Entry: 1.2, Exit: 0.25, MaxHold: 10},
			ExtraFeatures: func(f FactorContext) map[string]*factor.Node {
				return map[string]*factor.Node{
					"rsi": factor.RSI(f.Close(), 6),
				}
			},
			EntryFilter: func(ctx PairFilterContext) bool {
				rollFilterCalls++
				ry, okY := ctx.FeatureY("rsi")
				rx, okX := ctx.FeatureX("rsi")
				return okY && okX && math.Abs(ry-rx) > 5.0
			},
		}, p.Err()
	}); err != nil {
		t.Fatal(err)
	}
	rSIDs := map[int32]string{1: "A", 2: "B", 3: "C"}
	rngR := rand.New(rand.NewSource(909))
	rPrices := map[int32][]float64{1: make([]float64, 91), 2: make([]float64, 91), 3: make([]float64, 91)}
	lb, lc, ua := math.Log(100), math.Log(60), 0.0
	for bar := 1; bar <= 90; bar++ {
		lb += rngR.NormFloat64() * 0.025
		lc += rngR.NormFloat64() * 0.03
		if bar%14 == 0 {
			ua = 0.08
		} else {
			ua = 0.35*ua + rngR.NormFloat64()*0.01
		}
		rPrices[1][bar] = math.Exp(lb + ua)
		rPrices[2][bar] = math.Exp(lb)
		rPrices[3][bar] = math.Exp(lc)
	}
	rCfg := makeMultiSymbolReplayConfig(t, rollFeatStrat, rSIDs, 90, func(bar int64, sid int32) (float64, bool) {
		return rPrices[sid][bar], true
	})
	rRes, err := Run(context.Background(), rCfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rollFilterCalls == 0 || len(rRes.Pairs) == 0 || rRes.Pairs[0].Trades == 0 {
		t.Fatalf("expected RollingPairs EntryFilter calls (%d) and trades (%+v)", rollFilterCalls, rRes.Pairs)
	}
}
