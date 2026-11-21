package runner

import (
	"context"
	"math"
	"math/rand"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/statistics"
)

func TestPairsSpreadModelsAndHalfLifeValidation(t *testing.T) {
	base := PairsSpec{
		Pairs:  []Pair{{Y: "Y", X: "X"}},
		Window: 6,
		Band:   statistics.BandConfig{Entry: 2, Exit: 0.5},
	}
	sids := map[int32]string{1: "Y", 2: "X"}
	bad := []PairsSpec{
		func() PairsSpec { s := base; s.SpreadModel = "coint_vecm"; return s }(),
		func() PairsSpec { s := base; s.SpreadModel = "kalman"; s.KalmanDelta = -1e-4; return s }(),
		func() PairsSpec { s := base; s.SpreadModel = "kalman"; s.KalmanDelta = 1.0; return s }(),
		func() PairsSpec { s := base; s.SpreadModel = "kalman"; s.KalmanDelta = math.NaN(); return s }(),
		func() PairsSpec { s := base; s.SpreadModel = "kalman"; s.KalmanVe = -1e-3; return s }(),
		func() PairsSpec { s := base; s.SpreadModel = "kalman"; s.KalmanVe = math.Inf(1); return s }(),
		func() PairsSpec { s := base; s.MinHalfLife = -1; return s }(),
		func() PairsSpec { s := base; s.MaxHalfLife = -1; return s }(),
		func() PairsSpec { s := base; s.MinHalfLife = 10; s.MaxHalfLife = 5; return s }(),
		func() PairsSpec { s := base; s.Window = 3; s.MaxHalfLife = 10; return s }(),
		func() PairsSpec { s := base; s.Band.Mode = "breakout"; return s }(),
		func() PairsSpec {
			s := base
			s.Band = statistics.BandConfig{Entry: 2, Exit: 0.5, Stop: 0.4, Mode: "momentum"}
			return s
		}(),
	}
	for i, spec := range bad {
		if _, err := newPairsBuilder(spec, sids); err == nil {
			t.Fatalf("case %d accepted invalid spec: %+v", i, spec)
		}
	}

	rBase := RollingPairsSpec{
		FormationBars: 24,
		ReselectBars:  12,
		PairCount:     1,
		MaxPerSymbol:  1,
		Band:          statistics.BandConfig{Entry: 1.5, Exit: 0.3},
	}
	u := factor.Universe{Version: "u", Investable: []int32{1, 2}, Tradable: []int32{1, 2}}
	rBad := []RollingPairsSpec{
		func() RollingPairsSpec { s := rBase; s.SpreadModel = "unknown"; return s }(),
		func() RollingPairsSpec { s := rBase; s.KalmanDelta = 1.2; return s }(),
		func() RollingPairsSpec { s := rBase; s.KalmanVe = -0.1; return s }(),
		func() RollingPairsSpec { s := rBase; s.Window = 3; s.OnlineHalfLifeGate = true; return s }(),
	}
	for i, spec := range rBad {
		if _, err := newRollingPairsBuilder(spec, u, sids); err == nil {
			t.Fatalf("rolling case %d accepted invalid spec: %+v", i, spec)
		}
	}
}

// TestPairsRawOLSNotionalHedgeAndPnLReconciliation verifies "raw_ols" under vastly different
// nominal price levels (BTC ~ 60,000 vs ETH ~ 3,000, in both Y=ETH/X=BTC and Y=BTC/X=ETH
// directions):
// 1. Dollar notional hedge ratio equals beta_raw * (priceX / priceY), so 1 share of Y is paired
//    with exactly -beta_raw shares of X (never raw slope 0.05 or 20 used directly as NAV weight!).
// 2. A pure hedged BTC jump (+2,000 USDT on BTC, +100 USDT on ETH) yields zero net PnL in both
//    pairsTracker and the Weights ledger.
// 3. Full Run() Weights backtest reconciles Attribution.Residual and PairsResidual cleanly.
func TestPairsRawOLSNotionalHedgeAndPnLReconciliation(t *testing.T) {
	sids := map[int32]string{1: "ETH", 2: "BTC"}
	band := statistics.BandConfig{Entry: 1.5, Exit: 0.2, Stop: 5.0}

	bETHBTC, trETHBTC, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:       []Pair{{Y: "ETH", X: "BTC"}},
		Window:      8,
		Gross:       1.0,
		SpreadModel: "raw_ols",
		Band:        band,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}
	bBTCETH, trBTCETH, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:       []Pair{{Y: "BTC", X: "ETH"}},
		Window:      8,
		Gross:       1.0,
		SpreadModel: "raw_ols",
		Band:        band,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}

	btcPrices := []float64{60000, 61000, 59000, 62000, 58500, 60500, 59500, 60000}
	noise := []float64{6, -6, 5, -5, 6, -6, 4, 80} // +$80 dislocation on bar 8 opens trade
	var wEB, wBE map[int32]float64
	for i, px := range btcPrices {
		py := 0.05*px + noise[i] // ETH ~ 3,000, raw slope d(ETH)/d(BTC) ~ 0.05
		lp := map[int32]float64{1: math.Log(py), 2: math.Log(px)}
		wEB, _ = feedPairsBuilder(t, bETHBTC, uint64(i+1), 10000, lp, []int32{1, 2})
		wBE, _ = feedPairsBuilder(t, bBTCETH, uint64(i+1), 10000, lp, []int32{1, 2})
	}
	if len(wEB) != 2 || len(wBE) != 2 {
		t.Fatalf("expected both raw_ols directions to open: EB=%v BE=%v", wEB, wBE)
	}
	// At entry: BTC = 60,000, ETH = 3,080, raw slope ~ 0.05 -> notional beta = 0.05 * (60000/3080) ~ 0.974 (near 1:1 dollars!).
	// A buggy implementation using raw slope 0.05 directly would put ~95% in ETH and ~5% in BTC!
	if math.Abs(wEB[1]) < 0.45 || math.Abs(wEB[1]) > 0.55 || math.Abs(wEB[2]) < 0.45 || math.Abs(wEB[2]) > 0.55 {
		t.Fatalf("ETH/BTC raw_ols notional weights not dollar-hedged: %v (beta=%v, rawBeta=%v)",
			wEB, trETHBTC.states[0].beta, trETHBTC.states[0].modelBeta)
	}
	if math.Abs(wBE[1]) < 0.45 || math.Abs(wBE[1]) > 0.55 || math.Abs(wBE[2]) < 0.45 || math.Abs(wBE[2]) > 0.55 {
		t.Fatalf("BTC/ETH raw_ols notional weights not dollar-hedged: %v (beta=%v, rawBeta=%v)",
			wBE, trBTCETH.states[0].beta, trBTCETH.states[0].modelBeta)
	}
	// Verify exact share hedge ratio: qty_BTC / (-qty_ETH) == modelBeta (~0.05).
	ethEntry, btcEntry := 3080.0, 60000.0
	qtyETH := wEB[1] * 10000 / ethEntry
	qtyBTC := wEB[2] * 10000 / btcEntry
	wantRawBeta := trETHBTC.states[0].modelBeta
	if math.Abs((-qtyBTC/qtyETH)-wantRawBeta) > 1e-10 {
		t.Fatalf("share hedge ratio mismatch: got %v want %v", -qtyBTC/qtyETH, wantRawBeta)
	}

	// Bar 9: BTC jumps +$2,000 (60,000 -> 62,000) and ETH moves by exact rawBeta*$2,000 (spread unchanged!).
	// Net PnL booked by pairsTracker over bar 8 -> bar 9 must be identically 0!
	btc9 := 62000.0
	eth9 := ethEntry + wantRawBeta*(btc9-btcEntry)
	feedPairsBuilder(t, bETHBTC, 9, 10000, map[int32]float64{1: math.Log(eth9), 2: math.Log(btc9)}, []int32{1, 2})
	if gotPnL := trETHBTC.report(10000)[0].PnL; math.Abs(gotPnL) > 1e-12 {
		t.Fatalf("pure hedged move must yield 0 PnL in pairsTracker, got %v", gotPnL)
	}

	// Bar 10: ETH spread reverts from +$80 back to +$20 (the window mean, |Z| <= Exit) -> closes cleanly at a profit!
	eth10 := 0.05*btc9 + 20
	wClose, _ := feedPairsBuilder(t, bETHBTC, 10, 10000, map[int32]float64{1: math.Log(eth10), 2: math.Log(btc9)}, []int32{1, 2})
	rep := trETHBTC.report(10000)[0]
	if len(wClose) != 0 || len(rep.Episodes) != 1 || rep.Episodes[0].Reason != "exit" || rep.PnL <= 0 {
		t.Fatalf("expected profitable raw_ols mean-reversion exit, got w=%v rep=%+v", wClose, rep)
	}

	// Full Weights replay reconciliation at 60,000 / 3,000 price scale.
	const stratName = "test-pairs-raw-ols-btc-eth"
	if err := RegisterPairs(stratName, func(p *Params) (PairsSpec, error) {
		return PairsSpec{
			Pairs:       []Pair{{Y: "ETH", X: "BTC"}},
			Window:      20,
			SpreadModel: "raw_ols",
			DriftBand:   0.05,
			Band:        statistics.BandConfig{Entry: 1.4, Exit: 0.2, MaxHold: 25},
		}, p.Err()
	}); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(101))
	px, spread := 60000.0, 0.0
	prices := make(map[int32][]float64, 2)
	prices[1], prices[2] = make([]float64, 141), make([]float64, 141)
	for bar := 1; bar <= 140; bar++ {
		px = math.Max(20000, px+rng.NormFloat64()*600)
		spread = 0.6*spread + rng.NormFloat64()*25
		prices[1][bar] = 100 + 0.05*px + spread
		prices[2][bar] = px
	}
	cfg := makeMultiSymbolReplayConfig(t, stratName, sids, 140, func(bar int64, sid int32) (float64, bool) {
		return prices[sid][bar], true
	})
	res, err := Run(context.Background(), cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Pairs) != 1 || res.Pairs[0].Trades == 0 {
		t.Fatalf("expected raw_ols trades in full Run(), got %+v", res.Pairs)
	}
	if math.Abs(res.Attribution.Residual) > 1e-9*res.Attribution.InitialNAV {
		t.Fatalf("attribution residual out of tolerance: %v", res.Attribution.Residual)
	}
	costs := (res.Attribution.Fees + res.Attribution.Slippage + res.Attribution.Funding + res.Attribution.PITCost) / res.Attribution.InitialNAV
	if math.Abs(res.PairsResidual+costs) > 1e-6 {
		t.Fatalf("raw_ols PairsResidual (%v) failed exact reconciliation with -costs (%v)", res.PairsResidual, -costs)
	}
}

// TestPairsTLSReciprocalSymmetry verifies that Total Least Squares ("tls") eliminates
// dependent/independent leg ordering bias:
// 1. beta_YX * beta_XY == 1 across both positive and negative covariance windows;
// 2. Z_YX == -Z_XY;
// 3. TargetPortfolio weights (w_A, w_B) are identical regardless of whether Pair{Y:A, X:B} or Pair{Y:B, X:A} is declared!
func TestPairsTLSReciprocalSymmetry(t *testing.T) {
	estAB, err := statistics.NewSpreadEstimator(statistics.SpreadModelConfig{Model: "tls", Window: 20})
	if err != nil {
		t.Fatal(err)
	}
	estBA, err := statistics.NewSpreadEstimator(statistics.SpreadModelConfig{Model: "tls", Window: 20})
	if err != nil {
		t.Fatal(err)
	}
	olsAB, _ := statistics.NewRollingSpread(20)
	olsBA, _ := statistics.NewRollingSpread(20)

	rng := rand.New(rand.NewSource(42))
	for step := 0; step < 120; step++ {
		factorVal := rng.NormFloat64() * 0.08
		sign := 1.0
		if step >= 60 {
			sign = -1.0 // test negative covariance symmetry too
		}
		la := 4.5 + sign*1.7*factorVal + rng.NormFloat64()*0.03
		lb := 8.2 + 0.9*factorVal + rng.NormFloat64()*0.03

		pAB, okAB := estAB.ObserveLog(lb, la)
		pBA, okBA := estBA.ObserveLog(la, lb)
		oAB, _ := olsAB.Observe(lb, la)
		oBA, _ := olsBA.Observe(la, lb)
		if step < 19 {
			if okAB || okBA {
				t.Fatalf("ready before warmup at step %d", step)
			}
			continue
		}
		if !okAB || !okBA {
			t.Fatalf("TLS failed at step %d", step)
		}
		// OLS has attenuation bias: oAB.Beta * oBA.Beta == R^2 < 1.
		if math.Abs(oAB.Beta*oBA.Beta-1.0) < 1e-3 {
			continue
		}
		// TLS satisfies exact reciprocal symmetry: pAB.Beta * pBA.Beta == 1 and Z_BA == -sign(Beta)*Z_AB!
		if math.Abs(pAB.Beta*pBA.Beta-1.0) > 1e-11 {
			t.Fatalf("step %d TLS symmetry broken: beta_AB=%v * beta_BA=%v = %v", step, pAB.Beta, pBA.Beta, pAB.Beta*pBA.Beta)
		}
		wantZBA := -math.Copysign(1, pAB.Beta) * pAB.Z
		if math.Abs(pBA.Z-wantZBA) > 1e-11 {
			t.Fatalf("step %d TLS Z symmetry broken: Z_AB=%v Z_BA=%v want %v", step, pAB.Z, pBA.Z, wantZBA)
		}
	}

	// Verify identical TargetPortfolio weights when swapping Y and X in newPairsBuilder!
	sids := map[int32]string{1: "A", 2: "B"}
	band := statistics.BandConfig{Entry: 1.2, Exit: 0.25}
	bAB, _ := newPairsBuilder(PairsSpec{Pairs: []Pair{{Y: "A", X: "B"}}, Window: 12, SpreadModel: "tls", Gross: 0.8, Band: band}, sids)
	bBA, _ := newPairsBuilder(PairsSpec{Pairs: []Pair{{Y: "B", X: "A"}}, Window: 12, SpreadModel: "tls", Gross: 0.8, Band: band}, sids)

	rng2 := rand.New(rand.NewSource(99))
	traded := false
	for i := 1; i <= 60; i++ {
		f := rng2.NormFloat64() * 0.06
		la := 3.0 + 1.8*f + rng2.NormFloat64()*0.025
		lb := 6.0 + 0.8*f + rng2.NormFloat64()*0.025
		lp := map[int32]float64{1: la, 2: lb}
		w1, _ := feedPairsBuilder(t, bAB, uint64(i), 1000, lp, []int32{1, 2})
		w2, _ := feedPairsBuilder(t, bBA, uint64(i), 1000, lp, []int32{1, 2})
		if len(w1) != len(w2) {
			t.Fatalf("bar %d asymmetric trade state: wAB=%v vs wBA=%v", i, w1, w2)
		}
		if len(w1) == 2 {
			traded = true
			if math.Abs(w1[1]-w2[1]) > 1e-11 || math.Abs(w1[2]-w2[2]) > 1e-11 {
				t.Fatalf("bar %d asymmetric TLS weights: wAB=%v vs wBA=%v", i, w1, w2)
			}
		}
	}
	if !traded {
		t.Fatal("expected TLS builders to execute at least one trade")
	}
}

// TestPairsKalmanDynamicSlopeTrackingAndNoLookahead verifies "kalman":
// 1. Pre-update innovation Z matches direct statistics.KalmanPair one-step prediction (zero lookahead);
// 2. Warmup blocks trading for the first Window-1 bars;
// 3. Time-varying slope tracking adapts from beta ~ 0.8 to beta ~ 2.0, and DynamicHedge: true
//    continuously updates open portfolio weights while DynamicHedge: false freezes entry beta.
func TestPairsKalmanDynamicSlopeTrackingAndNoLookahead(t *testing.T) {
	// 1. Prefix invariance (strict no-lookahead) + warmup check.
	estShort, err := statistics.NewSpreadEstimator(statistics.SpreadModelConfig{
		Model: "kalman", Window: 10, KalmanDelta: 1e-3, KalmanVe: 1e-3,
	})
	if err != nil {
		t.Fatal(err)
	}
	estLong, _ := statistics.NewSpreadEstimator(statistics.SpreadModelConfig{
		Model: "kalman", Window: 10, KalmanDelta: 1e-3, KalmanVe: 1e-3,
	})
	rng := rand.New(rand.NewSource(77))
	xs, ys := make([]float64, 40), make([]float64, 40)
	lx := 10.5 // e.g., high log-price asset vs negative log-price asset (tests centering stability too!)
	for i := range xs {
		lx += rng.NormFloat64() * 0.03
		xs[i] = lx
		ys[i] = -2.4 + 1.3*lx + rng.NormFloat64()*0.01
	}
	var zAt19 float64
	for i := 0; i < 20; i++ {
		p, ok := estShort.ObserveLog(xs[i], ys[i])
		if i < 9 && ok {
			t.Fatalf("kalman ready during warmup at %d", i)
		}
		if i == 9 && !ok {
			t.Fatal("kalman not ready at Window")
		}
		if i == 19 {
			zAt19 = p.Z
		}
	}
	// Mutate future bars 20..39 violently in estLong and verify bar 19 output is bit-for-bit identical!
	for i := 0; i < 40; i++ {
		yv := ys[i]
		if i >= 20 {
			yv += 5.0
		}
		p, _ := estLong.ObserveLog(xs[i], yv)
		if i == 19 && p.Z != zAt19 {
			t.Fatalf("lookahead detected: zAt19=%v vs %v", p.Z, zAt19)
		}
	}

	// 2. Time-varying slope adaptation & DynamicHedge vs frozen hedge in builder.
	sids := map[int32]string{1: "Y", 2: "X"}
	band := statistics.BandConfig{Entry: 1.2, Exit: 0.05, Stop: 20.0}
	bFrozen, trFrozen, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:       []Pair{{Y: "Y", X: "X"}},
		Window:      12,
		SpreadModel: "kalman",
		KalmanDelta: 5e-4,
		KalmanVe:    5e-4,
		Gross:       1.0,
		Band:        band,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}
	bDyn, trDyn, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:        []Pair{{Y: "Y", X: "X"}},
		Window:       12,
		SpreadModel:  "kalman",
		KalmanDelta:  5e-4,
		KalmanVe:     5e-4,
		DynamicHedge: true,
		Gross:        1.0,
		Band:         band,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}

	rng3 := rand.New(rand.NewSource(314))
	x := 4.0
	for i := 1; i <= 30; i++ {
		x += (float64(i%2)*2 - 1) * 0.08
		y := 1.0 + 0.8*(x-4.0) + rng3.NormFloat64()*0.002
		lp := map[int32]float64{1: y, 2: x}
		feedPairsBuilder(t, bFrozen, uint64(i), 1000, lp, []int32{1, 2})
		feedPairsBuilder(t, bDyn, uint64(i), 1000, lp, []int32{1, 2})
	}
	// Trigger short-spread entry at bar 31 while slope ~ 0.8.
	y31 := 1.0 + 0.8*(x-4.0) + 0.08
	wF31, _ := feedPairsBuilder(t, bFrozen, 31, 1000, map[int32]float64{1: y31, 2: x}, []int32{1, 2})
	wD31, _ := feedPairsBuilder(t, bDyn, 31, 1000, map[int32]float64{1: y31, 2: x}, []int32{1, 2})
	if len(wF31) != 2 || len(wD31) != 2 {
		t.Fatalf("expected kalman entry at bar 31: frozen=%v dyn=%v", wF31, wD31)
	}
	entryBeta := trFrozen.states[0].beta
	if math.Abs(entryBeta-0.8) > 0.25 {
		t.Fatalf("expected initial kalman beta near 0.8, got %v", entryBeta)
	}

	// Now shift true slope upward toward ~2.2 on monotonically increasing x so every bar's pre-update
	// innovation stays strictly positive (single continuous open episode, zero close/reopen)!
	var wFLast, wDLast map[int32]float64
	for i := 32; i <= 50; i++ {
		x += 0.04
		y := y31 + 2.2*(x-4.0)
		lp := map[int32]float64{1: y, 2: x}
		wFLast, _ = feedPairsBuilder(t, bFrozen, uint64(i), 1000, lp, []int32{1, 2})
		wDLast, _ = feedPairsBuilder(t, bDyn, uint64(i), 1000, lp, []int32{1, 2})
	}
	if trFrozen.report(1000)[0].EpisodeCount != 1 || trDyn.report(1000)[0].EpisodeCount != 1 {
		t.Fatalf("expected single continuous episode: frozen=%d dyn=%d", trFrozen.report(1000)[0].EpisodeCount, trDyn.report(1000)[0].EpisodeCount)
	}
	if len(wFLast) != 2 || len(wDLast) != 2 {
		t.Fatalf("expected both positions still open: frozen=%v dyn=%v", wFLast, wDLast)
	}
	if math.Abs(trFrozen.states[0].beta-entryBeta) > 1e-12 {
		t.Fatalf("frozen kalman builder must hold entry beta %v, got %v", entryBeta, trFrozen.states[0].beta)
	}
	if trDyn.states[0].beta <= entryBeta+0.5 {
		t.Fatalf("DynamicHedge kalman builder failed to adapt slope upward: entry=%v now=%v", entryBeta, trDyn.states[0].beta)
	}
	// Verify DynamicHedge actually shifted X notional weight larger than Y notional weight (|wX| > |wY| since beta > 1.3)!
	if math.Abs(wDLast[2]) <= math.Abs(wDLast[1]) {
		t.Fatalf("expected |wX| > |wY| after slope adapted > 1, got %v", wDLast)
	}
}

// TestPairsRatioAndReturnDiffModels verifies "ratio" and "return_diff" Z-scores and equal dollar-neutral weights.
func TestPairsRatioAndReturnDiffModels(t *testing.T) {
	sids := map[int32]string{1: "Y", 2: "X"}
	band := statistics.BandConfig{Entry: 1.4, Exit: 0.2}

	bRatio, trRatio, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:       []Pair{{Y: "Y", X: "X"}},
		Window:      6,
		Gross:       0.8,
		SpreadModel: "ratio",
		Band:        band,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}
	bRetDiff, trRetDiff, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:       []Pair{{Y: "Y", X: "X"}},
		Window:      6,
		Gross:       0.8,
		SpreadModel: "return_diff",
		Band:        band,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}

	// X around 2000, Y around 100 (ratio ~ 0.05), then Y drops sharply on bar 6 (z <= -Entry -> long spread +1).
	xs := []float64{2000, 2040, 1980, 2020, 2010, 2000}
	ratios := []float64{0.0500, 0.0505, 0.0495, 0.0504, 0.0496, 0.0460}
	var wRatio, wRet map[int32]float64
	for i := range xs {
		py := xs[i] * ratios[i]
		lp := map[int32]float64{1: math.Log(py), 2: math.Log(xs[i])}
		wRatio, _ = feedPairsBuilder(t, bRatio, uint64(i+1), 1000, lp, []int32{1, 2})
		wRet, _ = feedPairsBuilder(t, bRetDiff, uint64(i+1), 1000, lp, []int32{1, 2})
	}
	// Equal dollar-neutral hedge beta = 1.0 -> wy = +0.5*0.8 = +0.4, wx = -0.5*0.8 = -0.4!
	if math.Abs(wRatio[1]-0.4) > 1e-12 || math.Abs(wRatio[2]+0.4) > 1e-12 || trRatio.states[0].beta != 1.0 {
		t.Fatalf("unexpected ratio model weights/beta: w=%v beta=%v", wRatio, trRatio.states[0].beta)
	}
	if math.Abs(wRet[1]-0.4) > 1e-12 || math.Abs(wRet[2]+0.4) > 1e-12 || trRetDiff.states[0].beta != 1.0 {
		t.Fatalf("unexpected return_diff model weights/beta: w=%v beta=%v", wRet, trRetDiff.states[0].beta)
	}
	// Revert back to window mean (~0.04935) -> both exit cleanly!
	wRatioExit, _ := feedPairsBuilder(t, bRatio, 7, 1000, map[int32]float64{1: math.Log(2000.0 * 0.04935), 2: math.Log(2000.0)}, []int32{1, 2})
	wRetExit, _ := feedPairsBuilder(t, bRetDiff, 7, 1000, map[int32]float64{1: math.Log(2000.0 * 0.04935), 2: math.Log(2000.0)}, []int32{1, 2})
	if len(wRatioExit) != 0 || len(wRetExit) != 0 {
		t.Fatalf("expected ratio and return_diff to exit on mean reversion: ratio=%v ret=%v", wRatioExit, wRetExit)
	}
}

// TestPairsOUHalfLifeOnlineGateBlocksDivergingSpread verifies that MaxHalfLife/MinHalfLife
// calls FitOU on the online spread window and blocks entry when residuals are trending/diverging
// or mean-reverting too slowly, while permitting fast-reverting OU windows.
func TestPairsOUHalfLifeOnlineGateBlocksDivergingSpread(t *testing.T) {
	sids := map[int32]string{1: "Y", 2: "X"}
	band := statistics.BandConfig{Entry: 1.3, Exit: 0.25}

	bUngated, _ := newPairsBuilder(PairsSpec{
		Pairs: []Pair{{Y: "Y", X: "X"}}, Window: 20, Band: band,
	}, sids)
	bGated, trGated, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:       []Pair{{Y: "Y", X: "X"}},
		Window:      20,
		MaxHalfLife: 6.0,
		Band:        band,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}

	// Phase 1 (bars 1..20): Explosive / trending divergence u_t = 0.004 * t^1.3 around y = x.
	// At bar 20, OLS residual Z exceeds Entry, so ungated enters, whereas FitOU sees non-reverting / slow drift and blocks bGated!
	var wUn, wGate map[int32]float64
	for i := 1; i <= 20; i++ {
		x := 3.0 + 0.04*math.Sin(float64(i))
		u := 0.0015 * math.Pow(float64(i), 1.6) // convex upward divergence -> terminal residual >> 0 and AR(1) b >= 0!
		lp := map[int32]float64{1: x + u, 2: x}
		wUn, _ = feedPairsBuilder(t, bUngated, uint64(i), 1000, lp, []int32{1, 2})
		wGate, _ = feedPairsBuilder(t, bGated, uint64(i), 1000, lp, []int32{1, 2})
	}
	if len(wUn) != 2 {
		t.Fatalf("expected ungated builder to enter on diverging window Z, got %v", wUn)
	}
	if len(wGate) != 0 || trGated.report(1000)[0].Trades != 0 {
		t.Fatalf("expected FitOU MaxHalfLife gate to block diverging spread entry, got w=%v", wGate)
	}

	// Phase 2 (bars 21..45): Stationary fast-reverting OU residuals (phi = 0.4 -> DiscreteHalfLife ~ 0.76 <= 6.0)
	// followed by a sharp 1-bar dislocation at bar 45 -> passes FitOU gate and opens!
	rng := rand.New(rand.NewSource(55))
	u := 0.0
	for i := 21; i <= 45; i++ {
		x := 3.0 + 0.05*math.Sin(float64(i)*0.7)
		if i < 45 {
			u = 0.35*u + rng.NormFloat64()*0.015
		} else {
			u = 0.055 // Z > Entry while trailing 20-bar AR(1) slope remains strongly mean-reverting
		}
		lp := map[int32]float64{1: x + u, 2: x}
		wGate, _ = feedPairsBuilder(t, bGated, uint64(i), 1000, lp, []int32{1, 2})
	}
	if trGated.report(1000)[0].Trades == 0 {
		t.Fatalf("expected FitOU gate to allow fast-reverting OU entries in Phase 2, got trades=0 (w=%v)", wGate)
	}
}

// TestPairsMomentumModeBreakoutAndReconciliation verifies BandConfig{Mode: "momentum"}:
// 1. Enters +1 (long Y, short X) on positive breakout z >= Entry, and -1 on negative breakdown z <= -Entry;
// 2. Exits on reversion to |z| <= Exit ("exit") or reverse breakdown ("stop");
// 3. Full Weights replay Run() reconciles Attribution.Residual and PairsResidual cleanly.
func TestPairsMomentumModeBreakoutAndReconciliation(t *testing.T) {
	sids := map[int32]string{1: "Y", 2: "X"}
	bMom, trMom, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:  []Pair{{Y: "Y", X: "X"}},
		Window: 6,
		Gross:  1.0,
		Band:   statistics.BandConfig{Entry: 1.5, Exit: 0.3, Stop: 1.2, Mode: "momentum"},
	}, sids)
	if err != nil {
		t.Fatal(err)
	}

	baseX := []float64{2.00, 2.03, 1.97, 2.04, 1.98, 2.01}
	noise := []float64{0.005, -0.005, 0.004, -0.004, 0.005, 0.05} // bar 6 breaks upward z >= 1.5
	var w map[int32]float64
	for i, x := range baseX {
		w, _ = feedPairsBuilder(t, bMom, uint64(i+1), 1000, map[int32]float64{1: x + noise[i], 2: x}, []int32{1, 2})
	}
	// Unlike reversion (which shorts Y on z >= Entry), momentum goes LONG the spread (+Y, -X)!
	if w[1] <= 0 || w[2] >= 0 {
		t.Fatalf("momentum mode must go LONG spread (+Y, -X) on z >= Entry, got %v", w)
	}

	// Spread continues trending higher to +0.12 (z > 2.5) -> momentum rides the trend (does NOT stop out on upside)!
	wRide, _ := feedPairsBuilder(t, bMom, 7, 1000, map[int32]float64{1: 2.02 + 0.12, 2: 2.02}, []int32{1, 2})
	if wRide[1] <= 0 {
		t.Fatalf("momentum mode must keep riding positive breakout, got %v", wRide)
	}

	// Violent reverse crash (spread plunges far negative so z <= -Stop) -> stops out with Reason == "stop"!
	wStop, _ := feedPairsBuilder(t, bMom, 8, 1000, map[int32]float64{1: 2.00 - 0.15, 2: 2.00}, []int32{1, 2})
	rep := trMom.report(1000)[0]
	if len(wStop) != 0 || len(rep.Episodes) != 1 || rep.Episodes[0].Dir != 1 || rep.Episodes[0].Reason != "stop" {
		t.Fatalf("expected momentum reverse breakdown stop, got w=%v episodes=%+v", wStop, rep.Episodes)
	}

	// Full Run() Weights backtest on a regime-trending pair with BandConfig{Mode: "momentum"} + DriftBand.
	const stratName = "test-pairs-momentum-run"
	if err := RegisterPairs(stratName, func(p *Params) (PairsSpec, error) {
		return PairsSpec{
			Pairs:       []Pair{{Y: "SOL", X: "ETH"}},
			Window:      20,
			SpreadModel: "return_diff",
			DriftBand:   0.04,
			Band:        statistics.BandConfig{Entry: 1.4, Exit: 0.25, Stop: 1.5, MaxHold: 20, Mode: "momentum"},
		}, p.Err()
	}); err != nil {
		t.Fatal(err)
	}
	runSIDs := map[int32]string{1: "SOL", 2: "ETH"}
	rng := rand.New(rand.NewSource(2026))
	lEth, relTrend := math.Log(3000), 0.0
	prices := map[int32][]float64{1: make([]float64, 161), 2: make([]float64, 161)}
	for bar := 1; bar <= 160; bar++ {
		lEth += rng.NormFloat64() * 0.02
		// Persistent momentum waves in relative strength
		relTrend = 0.85*relTrend + 0.015*math.Sin(float64(bar)/9.0) + rng.NormFloat64()*0.008
		prices[1][bar] = math.Exp(lEth - math.Log(20) + relTrend)
		prices[2][bar] = math.Exp(lEth)
	}
	cfg := makeMultiSymbolReplayConfig(t, stratName, runSIDs, 160, func(bar int64, sid int32) (float64, bool) {
		return prices[sid][bar], true
	})
	res, err := Run(context.Background(), cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Pairs) != 1 || res.Pairs[0].Trades == 0 {
		t.Fatalf("expected momentum trades in full Run(), got %+v", res.Pairs)
	}
	if math.Abs(res.Attribution.Residual) > 1e-9*res.Attribution.InitialNAV {
		t.Fatalf("attribution residual out of tolerance: %v", res.Attribution.Residual)
	}
	if math.Abs(res.PairsNAVReturn-res.PairsGrossPnL-res.PairsResidual) > 1e-12 {
		t.Fatalf("pairs reconciliation identity broken: %+v", res)
	}
	costs := (res.Attribution.Fees + res.Attribution.Slippage + res.Attribution.Funding + res.Attribution.PITCost) / res.Attribution.InitialNAV
	if math.Abs(res.PairsResidual+costs) > 1e-6 {
		t.Fatalf("momentum PairsResidual (%v) failed exact reconciliation with -costs (%v)", res.PairsResidual, -costs)
	}
}

// TestRollingPairsWithKalmanAndTLSModels verifies that RegisterRollingPairs seamlessly supports
// alternative online SpreadModel ("kalman" + DynamicHedge, "tls") and OnlineHalfLifeGate.
func TestRollingPairsWithKalmanAndTLSModels(t *testing.T) {
	const totalBars = 140
	rng := rand.New(rand.NewSource(808))
	sidMap := map[int32]string{1: "A/USDT:USDT", 2: "B/USDT:USDT", 3: "C/USDT:USDT"}
	prices := map[int32][]float64{
		1: make([]float64, totalBars+1),
		2: make([]float64, totalBars+1),
		3: make([]float64, totalBars+1),
	}
	lB, lC, uAB := math.Log(100), math.Log(50), 0.0
	for bar := 1; bar <= totalBars; bar++ {
		lB += rng.NormFloat64() * 0.03
		lC += rng.NormFloat64() * 0.03
		uAB = 0.45*uAB + rng.NormFloat64()*0.015
		prices[1][bar] = math.Exp(1.15*lB + uAB)
		prices[2][bar] = math.Exp(lB)
		prices[3][bar] = math.Exp(lC)
	}

	const stratName = "test-rolling-kalman-tls"
	if err := RegisterRollingPairs(stratName, func(p *Params) (RollingPairsSpec, error) {
		return RollingPairsSpec{
			FormationBars:      50,
			ReselectBars:       40,
			PairCount:          1,
			MaxPerSymbol:       1,
			Window:             20,
			SpreadModel:        "kalman",
			KalmanDelta:        1e-4,
			KalmanVe:           1e-4,
			DynamicHedge:       true,
			OnlineHalfLifeGate: true,
			Band:               statistics.BandConfig{Entry: 1.2, Exit: 0.25, Stop: 4.5, MaxHold: 20},
		}, p.Err()
	}); err != nil {
		t.Fatal(err)
	}
	cfg := makeMultiSymbolReplayConfig(t, stratName, sidMap, totalBars, func(bar int64, sid int32) (float64, bool) {
		return prices[sid][bar], true
	})
	res, err := Run(context.Background(), cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Pairs) == 0 || res.Pairs[0].Trades == 0 {
		t.Fatalf("expected rolling kalman pair trades, got %+v", res.Pairs)
	}
	if math.Abs(res.Attribution.Residual) > 1e-9*res.Attribution.InitialNAV {
		t.Fatalf("attribution residual out of tolerance: %v", res.Attribution.Residual)
	}
}
