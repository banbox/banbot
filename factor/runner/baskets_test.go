package runner

import (
	"context"
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
)

func TestBasketsSpecAndRegistrationValidation(t *testing.T) {
	base := BasketsSpec{
		Baskets: []BasketDef{{Name: "L1", Symbols: []string{"A", "B", "C"}}},
		Window:  30,
		Band:    statistics.BandConfig{Entry: 1.5, Exit: 0.3, Stop: 4.0},
	}

	bad := []BasketsSpec{
		func() BasketsSpec { s := base; s.Baskets = nil; return s }(),
		func() BasketsSpec { s := base; s.Baskets = []BasketDef{{Symbols: []string{"A"}}}; return s }(),
		func() BasketsSpec {
			s := base
			s.Baskets = []BasketDef{{Symbols: []string{"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10", "S11", "S12", "S13"}}}
			return s
		}(),
		func() BasketsSpec { s := base; s.Baskets = []BasketDef{{Symbols: []string{"A", "A", "C"}}}; return s }(),
		func() BasketsSpec {
			s := base
			s.WeightMethod = "fixed"
			s.Baskets = []BasketDef{{Symbols: []string{"A", "B", "C"}, Weights: []float64{1, -1}}}
			return s
		}(),
		func() BasketsSpec {
			s := base
			s.WeightMethod = "fixed"
			s.Baskets = []BasketDef{{Symbols: []string{"A", "B", "C"}, Weights: []float64{0, 0, 0}}}
			return s
		}(),
		func() BasketsSpec { s := base; s.WeightMethod = "pca"; return s }(),
		func() BasketsSpec { s := base; s.Confidence = 0.85; return s }(),
		func() BasketsSpec { s := base; s.DifferenceLags = 2; s.Window = 10; return s }(), // need 3*(2+1)+3 = 12
		func() BasketsSpec { s := base; s.MaxGross = 1.2; return s }(),
		func() BasketsSpec { s := base; s.MaxGross = 0.6; s.GrossPerBasket = 0.8; return s }(),
		func() BasketsSpec { s := base; s.DriftBand = -0.01; return s }(),
		func() BasketsSpec { s := base; s.WeightMode = "kelly"; return s }(),
	}
	for i, spec := range bad {
		if _, err := BuildBasketsRegistration("bad-basket", spec); err == nil {
			t.Fatalf("case %d accepted invalid BasketsSpec: %+v", i, spec)
		}
	}
	if _, err := BuildBasketsRegistration("", base); err == nil {
		t.Fatal("empty strategy name accepted")
	}

	// BuildBasketsRegistration must be pure and not mutate global definitions until Register() is called.
	reg, err := BuildBasketsRegistration("unit-pure-basket", base)
	if err != nil {
		t.Fatal(err)
	}
	if boundDefinitionBuilder("unit-pure-basket") != "" {
		t.Fatal("BuildBasketsRegistration mutated global registry before Register()")
	}
	plan, combo, err := reg.Definition(Config{Factor: research.MomentumVolConfig{TimeFrame: "1h"}})
	if err != nil || plan == nil || len(combo.Columns) != 1 || combo.Columns[0] != pairsLogOutput {
		t.Fatalf("unexpected compiled basket plan: plan=%v combo=%+v err=%v", plan, combo, err)
	}
}

func TestBasketsFixedJohansenAndMultivariateOLSModes(t *testing.T) {
	sids := map[int32]string{1: "A", 2: "B", 3: "C"}
	tradable := []int32{1, 2, 3}

	// 1. "fixed" mode normalizes weights so sum |w_i| == GrossPerBasket, supporting both reversion and momentum.
	fixedB, _, err := newTrackedBasketsBuilder(BasketsSpec{
		Baskets:        []BasketDef{{Name: "ABC", Symbols: []string{"A", "B", "C"}, Weights: []float64{2.0, -1.0, -1.0}}},
		Window:         6,
		GrossPerBasket: 0.8,
		MaxGross:       0.8,
		Band:           statistics.BandConfig{Entry: 1.3, Exit: 0.2, Mode: statistics.BandModeReversion},
	}, sids, nil)
	if err != nil {
		t.Fatal(err)
	}
	osc := []float64{0.005, -0.005, 0.005, -0.005, 0.005, -0.04} // negative spread shock -> long basket (+1)
	var wFixed map[int32]float64
	for i, d := range osc {
		wFixed, _ = feedPairsBuilder(t, fixedB, uint64(i+1), 1000, map[int32]float64{
			1: 4.0 + d, 2: 3.5, 3: 3.0,
		}, tradable)
	}
	// Unit weights: [2/4, -1/4, -1/4] = [+0.5, -0.25, -0.25]; scaled by GrossPerBasket=0.8 -> [+0.4, -0.2, -0.2].
	if math.Abs(wFixed[1]-0.4) > 1e-9 || math.Abs(wFixed[2]+0.2) > 1e-9 || math.Abs(wFixed[3]+0.2) > 1e-9 {
		t.Fatalf("unexpected fixed basket weights: %v", wFixed)
	}

	// 2. "johansen" mode on 3-leg cointegrated series vs 3 independent random walks under RequireCointegration + HalfLife gate.
	jCointSpec := BasketsSpec{
		Baskets:              []BasketDef{{Name: "J3", Symbols: []string{"A", "B", "C"}}},
		Window:               120,
		WeightMethod:         "johansen",
		RequireCointegration: true,
		Confidence:           0.99,
		MinHalfLife:          0.3,
		MaxHalfLife:          5.0,
		Band:                 statistics.BandConfig{Entry: 1.2, Exit: 0.25, Stop: 4.5},
	}
	jBuilderCoint, jTracker, err := newTrackedBasketsBuilder(jCointSpec, sids, nil)
	if err != nil {
		t.Fatal(err)
	}
	jBuilderRW, _, err := newTrackedBasketsBuilder(jCointSpec, sids, nil)
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(1337))
	var f1, f2, u float64
	var rw1, rw2, rw3 float64
	enteredCoint := false
	enteredRW := false
	for bar := 1; bar <= 150; bar++ {
		f1 += rng.NormFloat64() * 0.025
		f2 += rng.NormFloat64() * 0.025
		u = 0.45*u + rng.NormFloat64()*0.008
		if bar == 135 {
			u = -0.026 // sharp negative spread dislocation inside Stop -> opens long basket (+A, -B, -C)
		}
		wC, _ := feedPairsBuilder(t, jBuilderCoint, uint64(bar), 1000, map[int32]float64{
			1: 4.0 + 0.6*f1 + 0.4*f2 + u,
			2: 4.0 + f1,
			3: 4.0 + f2,
		}, tradable)
		if len(wC) == 3 {
			enteredCoint = true
			gross := math.Abs(wC[1]) + math.Abs(wC[2]) + math.Abs(wC[3])
			if math.Abs(gross-1.0) > 1e-9 {
				t.Fatalf("johansen basket gross=%v want 1.0 (%v)", gross, wC)
			}
			if bar == 135 && (wC[1] <= 0 || wC[2] >= 0 || wC[3] >= 0) {
				t.Fatalf("expected sign-stabilized long 3-leg spread (+A, -B, -C) at bar 135, got %v", wC)
			}
		}

		rw1 += rng.NormFloat64() * 0.025
		rw2 += rng.NormFloat64() * 0.025
		rw3 += rng.NormFloat64() * 0.025
		wRW, _ := feedPairsBuilder(t, jBuilderRW, uint64(bar), 1000, map[int32]float64{
			1: 4.0 + rw1, 2: 4.0 + rw2, 3: 4.0 + rw3,
		}, tradable)
		if len(wRW) > 0 {
			enteredRW = true
		}
	}
	if !enteredCoint {
		t.Fatal("expected Johansen cointegrated 3-leg basket to enter")
	}
	if enteredRW {
		t.Fatal("RequireCointegration + HalfLife gate failed to block independent random walks")
	}
	if rep := jTracker.report(1000); len(rep) != 1 || rep[0].Trades == 0 {
		t.Fatalf("expected tracked basket trades, got %+v", rep)
	}

	// 3. "multivariate_ols" with exact collinearity between legs B and C (tests ridge fallback) + UseRawPrice + inverse_vol.
	sids6 := map[int32]string{1: "A1", 2: "B1", 3: "C1", 4: "A2", 5: "B2", 6: "C2"}
	olsB, _, err := newTrackedBasketsBuilder(BasketsSpec{
		Baskets: []BasketDef{
			{Name: "B1_LOWVOL", Symbols: []string{"A1", "B1", "C1"}},
			{Name: "B2_HIGHVOL", Symbols: []string{"A2", "B2", "C2"}},
		},
		Window:       12,
		WeightMethod: "multivariate_ols",
		UseRawPrice:  true,
		WeightMode:   "inverse_vol",
		Band:         statistics.BandConfig{Entry: 1.2, Exit: 0.2},
	}, sids6, nil)
	if err != nil {
		t.Fatal(err)
	}
	var wOLS map[int32]float64
	for bar := 1; bar <= 12; bar++ {
		common := 100.0 + 4.0*math.Sin(float64(bar)*0.9)
		res := 0.5 * math.Cos(float64(bar)*1.7)
		if bar == 12 {
			res = -3.0 // negative dislocation on both baskets
		}
		// B1 and C1 are perfectly collinear (C1 == 2*B1) -> exercises singular ridge fallback cleanly!
		p1 := common + res
		p2 := common + 3.0*res // 3x higher spread volatility on basket 2
		wOLS, _ = feedPairsBuilder(t, olsB, uint64(bar), 1000, map[int32]float64{
			1: math.Log(p1), 2: math.Log(common), 3: math.Log(2 * common),
			4: math.Log(p2), 5: math.Log(common), 6: math.Log(2 * common),
		}, []int32{1, 2, 3, 4, 5, 6})
	}
	g1 := math.Abs(wOLS[1]) + math.Abs(wOLS[2]) + math.Abs(wOLS[3])
	g2 := math.Abs(wOLS[4]) + math.Abs(wOLS[5]) + math.Abs(wOLS[6])
	if g1 <= 0 || g2 <= 0 || g1 <= g2 {
		t.Fatalf("expected both OLS baskets open with low-vol g1 > high-vol g2, got g1=%v g2=%v w=%v", g1, g2, wOLS)
	}
	if math.Abs((g1+g2)-1.0) > 1e-9 {
		t.Fatalf("expected inverse_vol total gross 1.0, got %v", g1+g2)
	}
}

func TestBasketsBacktestPnLReconciliationDriftBandAndNoLookahead(t *testing.T) {
	const totalBars = 180
	const splitBar = 120
	sidMap := map[int32]string{1: "SOL/USDT:USDT", 2: "ETH/USDT:USDT", 3: "BTC/USDT:USDT"}

	genPrices := func(alterFuture bool) map[int32][]float64 {
		rng := rand.New(rand.NewSource(90210))
		p := make(map[int32][]float64, 3)
		for sid := int32(1); sid <= 3; sid++ {
			p[sid] = make([]float64, totalBars+1)
		}
		lEth, lBtc, u := math.Log(200), math.Log(1000), 0.0
		for bar := 1; bar <= totalBars; bar++ {
			dEth := rng.NormFloat64() * 0.022
			dBtc := rng.NormFloat64() * 0.020
			du := rng.NormFloat64() * 0.015
			if alterFuture && bar > splitBar {
				dEth = 0.09
				dBtc = -0.07
				du = 0.25
			}
			lEth += dEth
			lBtc += dBtc
			u = 0.55*u + du
			lSol := 0.6*lEth + 0.4*lBtc + u
			p[1][bar] = math.Exp(lSol)
			p[2][bar] = math.Exp(lEth)
			p[3][bar] = math.Exp(lBtc)
		}
		return p
	}

	pBase := genPrices(false)
	pAltered := genPrices(true)

	makeSpec := func(driftBand float64) BasketsSpec {
		return BasketsSpec{
			Baskets:              []BasketDef{{Name: "SOL-ETH-BTC", Symbols: []string{"SOL/USDT:USDT", "ETH/USDT:USDT", "BTC/USDT:USDT"}}},
			Window:               45,
			WeightMethod:         "johansen",
			RequireCointegration: false,
			DriftBand:            driftBand,
			MaxHoldBars:          25,
			Band:                 statistics.BandConfig{Entry: 1.25, Exit: 0.20, Stop: 4.0},
		}
	}

	if err := RegisterBaskets("test-baskets-drift-0", makeSpec(0)); err != nil {
		t.Fatal(err)
	}
	if err := RegisterBaskets("test-baskets-drift-pos", makeSpec(0.05)); err != nil {
		t.Fatal(err)
	}

	outBase := &captureTargetsOutput{}
	resNoBand, err := Run(context.Background(), makeMultiSymbolReplayConfig(t, "test-baskets-drift-0", sidMap, totalBars, func(bar int64, sid int32) (float64, bool) {
		return pBase[sid][bar], true
	}), nil, outBase)
	if err != nil {
		t.Fatal(err)
	}

	resBand, err := Run(context.Background(), makeMultiSymbolReplayConfig(t, "test-baskets-drift-pos", sidMap, totalBars, func(bar int64, sid int32) (float64, bool) {
		return pBase[sid][bar], true
	}), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(resNoBand.Pairs) != 1 || resNoBand.Pairs[0].Trades == 0 {
		t.Fatalf("expected 3-leg basket trades in backtest, got %+v", resNoBand.Pairs)
	}
	if resBand.Book.Fees >= resNoBand.Book.Fees || resBand.Book.Turnover >= resNoBand.Book.Turnover {
		t.Fatalf("expected DriftBand to reduce ledger fees & turnover: fees %v vs %v, turnover %v vs %v",
			resBand.Book.Fees, resNoBand.Book.Fees, resBand.Book.Turnover, resNoBand.Book.Turnover)
	}
	for _, res := range []Result{resNoBand, resBand} {
		if math.Abs(res.Attribution.Residual) > 1e-9*res.Attribution.InitialNAV {
			t.Fatalf("attribution residual out of tolerance: %v", res.Attribution.Residual)
		}
		if math.Abs(res.PairsNAVReturn-res.PairsGrossPnL-res.PairsResidual) > 1e-12 {
			t.Fatalf("basket PnL reconciliation broken: nav=%v gross=%v resid=%v", res.PairsNAVReturn, res.PairsGrossPnL, res.PairsResidual)
		}
		costs := (res.Attribution.Fees + res.Attribution.Slippage + res.Attribution.Funding + res.Attribution.PITCost) / res.Attribution.InitialNAV
		if math.Abs(res.PairsResidual+costs) > 1e-6 {
			t.Fatalf("expected PairsResidual (%v) to match -costs (%v)", res.PairsResidual, -costs)
		}
	}

	// Strict no-lookahead check up to splitBar.
	outAltered := &captureTargetsOutput{}
	_, err = Run(context.Background(), makeMultiSymbolReplayConfig(t, "test-baskets-drift-0", sidMap, totalBars, func(bar int64, sid int32) (float64, bool) {
		return pAltered[sid][bar], true
	}), nil, outAltered)
	if err != nil {
		t.Fatal(err)
	}
	nonEmpty := 0
	for i := 0; i < splitBar; i++ {
		w1, w2 := outBase.targets[i], outAltered.targets[i]
		if len(w1) > 0 {
			nonEmpty++
		}
		if (w1 == nil) != (w2 == nil) || len(w1) != len(w2) {
			t.Fatalf("lookahead leak at bar %d: %v vs %v", i+1, w1, w2)
		}
		for sid, v := range w1 {
			if math.Float64bits(v) != math.Float64bits(w2[sid]) {
				t.Fatalf("lookahead bit mismatch at bar %d sid %d: %v vs %v", i+1, sid, v, w2[sid])
			}
		}
	}
	if nonEmpty == 0 {
		t.Fatal("expected active basket targets prior to splitBar")
	}

	// UnavailableSymbols graceful skip check.
	bUnavail, trUnavail, err := newTrackedBasketsBuilder(makeSpec(0), map[int32]string{1: "SOL/USDT:USDT", 2: "ETH/USDT:USDT"}, map[string]string{"BTC/USDT:USDT": "no_data"})
	if err != nil {
		t.Fatalf("UnavailableSymbols leg must not error: %v", err)
	}
	wU, diagsU := feedPairsBuilder(t, bUnavail, 1, 1000, map[int32]float64{1: 5.0, 2: 5.0}, []int32{1, 2})
	if wU == nil || len(wU) != 0 || len(diagsU) == 0 || !strings.Contains(diagsU[0].Code, "baskets-all-unavailable") {
		t.Fatalf("expected baskets-all-unavailable flat target, got w=%v diags=%v", wU, diagsU)
	}
	if rep := trUnavail.report(1000); len(rep) != 1 || rep[0].Unavailable != "leg_unavailable" {
		t.Fatalf("expected unavailable PairResult, got %+v", rep)
	}

	// Mid-run PIT leg delisting check on a 2-basket run (basket 1 loses a leg mid-run, basket 2 keeps trading).
	sidMap4 := map[int32]string{1: "SOL/USDT:USDT", 2: "ETH/USDT:USDT", 3: "BTC/USDT:USDT", 4: "AVAX/USDT:USDT"}
	if err := RegisterBaskets("test-baskets-pit-delist", BasketsSpec{
		Baskets: []BasketDef{
			{Name: "B1", Symbols: []string{"SOL/USDT:USDT", "ETH/USDT:USDT", "BTC/USDT:USDT"}},
			{Name: "B2", Symbols: []string{"AVAX/USDT:USDT", "ETH/USDT:USDT", "BTC/USDT:USDT"}},
		},
		Window:       35,
		WeightMethod: "multivariate_ols",
		Band:         statistics.BandConfig{Entry: 1.1, Exit: 0.2, Stop: 4.5},
	}); err != nil {
		t.Fatal(err)
	}
	resPIT, err := Run(context.Background(), makeMultiSymbolReplayConfig(t, "test-baskets-pit-delist", sidMap4, totalBars, func(bar int64, sid int32) (float64, bool) {
		if sid == 1 && bar >= 110 {
			return 0, false // SOL delists at bar 110; B1 closes/pauses while B2 continues!
		}
		if sid == 4 {
			return pBase[1][bar] * (1 + 0.005*math.Sin(float64(bar))), true
		}
		return pBase[sid][bar], true
	}), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resPIT.Incomplete != 0 || resPIT.UniverseDrops == 0 {
		t.Fatalf("expected 0 Incomplete and positive UniverseDrops on basket leg delist, got incomplete=%d drops=%d", resPIT.Incomplete, resPIT.UniverseDrops)
	}
}
