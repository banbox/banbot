package runner

import (
	"context"
	"math"
	"math/rand"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
)

func feedResidualBuilder(t *testing.T, b PortfolioBuilder, seq uint64, nav float64, logPrices map[int32]float64, u factor.Universe) (map[int32]float64, []factor.Diagnostic) {
	t.Helper()
	vals := make(map[int32]factor.Numeric, len(logPrices))
	for sid, lp := range logPrices {
		vals[sid] = factor.Numeric{Value: lp, Validity: factor.Valid}
	}
	at := int64(seq) * 3600000
	frame := factor.Frame{DecisionTime: at, Values: map[string]map[int32]factor.Numeric{pairsLogOutput: vals}}
	pSpec := factor.PortfolioSpec{
		StrategyID: "s", AccountID: "a", SnapshotID: "snap", PlanHash: "p", FactorPlanHash: "f", UniverseVersion: u.Version,
		Budget:       factor.FrozenBudget{Version: "b", Currency: "USDT", NAV: nav},
		PlanSequence: seq, DecisionTime: at, ExecutableAt: at + 1, ExpireAt: at + 60000, Mode: factor.Full,
	}
	tp, diags, err := b(frame, u, pSpec, research.PortfolioDefinition{})
	if err != nil {
		t.Fatal(err)
	}
	if tp == nil {
		return nil, diags
	}
	return tp.Targets(), tp.Spec().Diagnostics
}

func TestResidualStatArbValidationAndRegistration(t *testing.T) {
	base := ResidualStatArbSpec{
		FactorModel: "pca",
		NumFactors:  2,
		Window:      30,
		EntryS:      1.25,
		ExitLongS:   0.50,
		ExitShortS:  0.50,
		StopS:       3.5,
	}

	bad := []ResidualStatArbSpec{
		func() ResidualStatArbSpec { s := base; s.FactorModel = "ica"; return s }(),
		func() ResidualStatArbSpec { s := base; s.FactorModel = "benchmarks"; s.Benchmarks = nil; return s }(),
		func() ResidualStatArbSpec {
			s := base
			s.FactorModel = "benchmarks"
			s.Benchmarks = []string{"BTC", "BTC"}
			return s
		}(),
		func() ResidualStatArbSpec { s := base; s.NumFactors = -1; return s }(),
		func() ResidualStatArbSpec { s := base; s.Window = 4; return s }(),
		func() ResidualStatArbSpec { s := base; s.OUWindow = 3; return s }(),
		func() ResidualStatArbSpec { s := base; s.OUWindow = 35; return s }(),
		func() ResidualStatArbSpec { s := base; s.EntryS = -1.0; return s }(),
		func() ResidualStatArbSpec { s := base; s.ExitLongS = 1.25; return s }(),
		func() ResidualStatArbSpec { s := base; s.ExitShortS = 1.5; return s }(),
		func() ResidualStatArbSpec { s := base; s.StopS = 1.0; return s }(),
		func() ResidualStatArbSpec { s := base; s.TargetGross = 1.1; return s }(),
		func() ResidualStatArbSpec { s := base; s.MaxWeightPerAsset = 1.2; return s }(),
		func() ResidualStatArbSpec { s := base; s.WeightMode = "martingale"; return s }(),
		func() ResidualStatArbSpec { s := base; s.DriftBand = -0.1; return s }(),
	}
	for i, spec := range bad {
		if _, err := BuildResidualStatArbRegistration("bad-res-statarb", spec); err == nil {
			t.Fatalf("case %d accepted invalid ResidualStatArbSpec: %+v", i, spec)
		}
	}
	if _, err := BuildResidualStatArbRegistration("", base); err == nil {
		t.Fatal("empty name accepted")
	}

	reg, err := BuildResidualStatArbRegistration("pure-res-statarb", base)
	if err != nil {
		t.Fatal(err)
	}
	if boundDefinitionBuilder("pure-res-statarb") != "" {
		t.Fatal("BuildResidualStatArbRegistration mutated global registry before Register()")
	}
	if plan, combo, err := reg.Definition(Config{Factor: research.MomentumVolConfig{TimeFrame: "1h"}}); err != nil || plan == nil || len(combo.Columns) != 1 {
		t.Fatalf("unexpected compiled plan: %v %+v %v", plan, combo, err)
	}
}

func TestResidualStatArbPCADollarAndBetaNeutrality(t *testing.T) {
	// 8 assets driven by 2 common factors + mean-reverting OU idiosyncratic cumulative residuals.
	const nAssets = 8
	const window = 45
	const totalBars = 120
	sids := map[int32]string{
		1: "C1", 2: "C2", 3: "C3", 4: "C4",
		5: "C5", 6: "C6", 7: "C7", 8: "C8",
	}
	allSIDs := []int32{1, 2, 3, 4, 5, 6, 7, 8}
	u := factor.Universe{Version: "u8", Investable: allSIDs, Tradable: allSIDs}

	spec := ResidualStatArbSpec{
		FactorModel:       "pca",
		NumFactors:        2,
		Window:            window,
		OUWindow:          window,
		MinHalfLife:       0.2,
		MaxHalfLife:       30,
		EntryS:            1.15,
		ExitLongS:         0.40,
		ExitShortS:        0.40,
		StopS:             4.5,
		MaxHoldBars:       20,
		WeightMode:        "inverse_vol",
		TargetGross:       0.90,
		MaxWeightPerAsset: 0.28,
	}
	builder, err := newResidualStatArbBuilder(spec, u, sids, nil)
	if err != nil {
		t.Fatal(err)
	}

	load1 := [nAssets]float64{1.2, 0.9, 1.1, 0.8, 1.3, 0.7, 1.0, 1.15}
	load2 := [nAssets]float64{0.6, 0.5, -0.5, -0.6, 0.4, -0.4, 0.3, -0.35}

	rng := rand.New(rand.NewSource(202608))
	logP := make([][]float64, totalBars+1)
	logP[0] = make([]float64, nAssets)
	for j := range nAssets {
		logP[0][j] = math.Log(100 + 10*float64(j))
	}
	idio := make([]float64, nAssets)
	for bar := 1; bar <= totalBars; bar++ {
		logP[bar] = make([]float64, nAssets)
		f1 := rng.NormFloat64() * 0.022
		f2 := rng.NormFloat64() * 0.015
		for j := range nAssets {
			prevIdio := idio[j]
			idio[j] = 0.55*idio[j] + rng.NormFloat64()*0.006
			if bar == 60 && j == 0 {
				idio[j] += 0.028 // single-asset dislocation -> exercises Tier-2 small active set neutralization
			}
			if bar == 85 {
				switch j {
				case 0, 2, 4:
					idio[j] += 0.026
				case 1, 3, 5:
					idio[j] -= 0.026
				}
			}
			epsRet := idio[j] - prevIdio
			totRet := load1[j]*f1 + load2[j]*f2 + epsRet
			logP[bar][j] = logP[bar-1][j] + math.Log1p(totRet)
		}
	}

	activeBars := 0
	for bar := 1; bar <= totalBars; bar++ {
		lpMap := make(map[int32]float64, nAssets)
		for j := range nAssets {
			lpMap[int32(j+1)] = logP[bar][j]
		}
		w, _ := feedResidualBuilder(t, builder, uint64(bar), 1000, lpMap, u)
		if bar <= window {
			if w != nil {
				t.Fatalf("bar %d expected nil warmup target, got %v", bar, w)
			}
			continue
		}
		if len(w) == 0 {
			continue
		}
		activeBars++

		// 1. Verify exact dollar neutrality |sum(w)| < 1e-9, TargetGross bound, and MaxWeightPerAsset cap.
		var netSum, grossSum float64
		for sid, v := range w {
			netSum += v
			grossSum += math.Abs(v)
			if math.Abs(v) > spec.MaxWeightPerAsset+1e-9 {
				t.Fatalf("bar %d sid %d weight %v exceeded MaxWeightPerAsset %v", bar, sid, v, spec.MaxWeightPerAsset)
			}
		}
		if math.Abs(netSum) >= 1e-9 {
			t.Fatalf("bar %d violated dollar neutrality: sum(w)=%v (w=%v)", bar, netSum, w)
		}
		if grossSum > spec.TargetGross+1e-9 {
			t.Fatalf("bar %d exceeded TargetGross %v: got %v", bar, spec.TargetGross, grossSum)
		}

		// 2. Recompute rolling PCA eigenportfolio betas independently and verify |B^T w| < 1e-9!
		rows := make([][]float64, window)
		stds := make([]float64, nAssets)
		assetRets := make([][]float64, nAssets)
		for j := range nAssets {
			assetRets[j] = make([]float64, window)
		}
		for k := range window {
			bIdx := bar - window + 1 + k
			row := make([]float64, nAssets)
			for j := range nAssets {
				r := math.Expm1(logP[bIdx][j] - logP[bIdx-1][j])
				row[j] = r
				assetRets[j][k] = r
			}
			rows[k] = row
		}
		for j := range nAssets {
			_, sd, err := statistics.MeanStd(assetRets[j], 1)
			if err != nil {
				t.Fatal(err)
			}
			stds[j] = sd
		}
		pca, err := statistics.FitPCA(rows, spec.NumFactors, true)
		if err != nil {
			t.Fatal(err)
		}
		design := make([][]float64, window)
		for k := range window {
			dRow := make([]float64, 1+spec.NumFactors)
			dRow[0] = 1
			for c := range spec.NumFactors {
				var fr float64
				for j := range nAssets {
					fr += (pca.Loadings[c][j] / stds[j]) * rows[k][j]
				}
				dRow[1+c] = fr
			}
			design[k] = dRow
		}
		for c := range spec.NumFactors {
			var betaExp float64
			for j := range nAssets {
				coef, _, err := factor.LeastSquares(design, assetRets[j], nil)
				if err != nil {
					t.Fatal(err)
				}
				betaExp += coef[1+c] * w[int32(j+1)]
			}
			if math.Abs(betaExp) >= 1e-9 {
				t.Fatalf("bar %d violated PCA factor-%d beta neutrality: exposure=%v (w=%v)", bar, c, betaExp, w)
			}
		}
	}
	if activeBars == 0 {
		t.Fatal("expected active PCA residual statarb trading bars")
	}
}

func TestResidualStatArbBenchmarksModePITDropoutAndNoLookahead(t *testing.T) {
	// 2 benchmarks (BTC, ETH) + 5 altcoins with known benchmark loadings and reverting OU residuals.
	const totalBars = 140
	const splitBar = 95
	const delistBar = 105
	const window = 35
	sidMap := map[int32]string{
		1: "BTC/USDT:USDT",
		2: "ETH/USDT:USDT",
		3: "SOL/USDT:USDT",
		4: "AVAX/USDT:USDT",
		5: "LINK/USDT:USDT",
		6: "UNI/USDT:USDT",
		7: "AAVE/USDT:USDT",
	}
	betaBTC := map[int32]float64{3: 1.3, 4: 1.1, 5: 0.8, 6: 0.9, 7: 1.0}
	betaETH := map[int32]float64{3: 0.4, 4: 0.6, 5: 0.7, 6: 0.5, 7: 0.8}

	genPrices := func(alterFuture bool) map[int32][]float64 {
		rng := rand.New(rand.NewSource(424242))
		p := make(map[int32][]float64, 7)
		logP := make(map[int32][]float64, 7)
		for sid := int32(1); sid <= 7; sid++ {
			p[sid] = make([]float64, totalBars+1)
			logP[sid] = make([]float64, totalBars+1)
			logP[sid][0] = math.Log(50 + 20*float64(sid))
			p[sid][0] = math.Exp(logP[sid][0])
		}
		idio := make(map[int32]float64, 5)
		for bar := 1; bar <= totalBars; bar++ {
			rBTC := rng.NormFloat64() * 0.020
			rETH := rng.NormFloat64() * 0.024
			if alterFuture && bar > splitBar {
				rBTC = 0.06
				rETH = -0.05
			}
			logP[1][bar] = logP[1][bar-1] + math.Log1p(rBTC)
			logP[2][bar] = logP[2][bar-1] + math.Log1p(rETH)
			p[1][bar] = math.Exp(logP[1][bar])
			p[2][bar] = math.Exp(logP[2][bar])

			for sid := int32(3); sid <= 7; sid++ {
				prev := idio[sid]
				shock := rng.NormFloat64() * 0.007
				if alterFuture && bar > splitBar {
					shock = 0.04 * float64(sid-4)
				}
				idio[sid] = 0.50*prev + shock
				// Force sid=3 (SOL) into a clean multi-bar dislocation right before it delists at bar 105!
				if !alterFuture && (bar == delistBar-2 || bar == delistBar-1) && sid == 3 {
					idio[sid] = -0.020
				}
				rAlt := betaBTC[sid]*rBTC + betaETH[sid]*rETH + (idio[sid] - prev)
				logP[sid][bar] = logP[sid][bar-1] + math.Log1p(rAlt)
				p[sid][bar] = math.Exp(logP[sid][bar])
			}
		}
		return p
	}

	spec := ResidualStatArbSpec{
		FactorModel: "benchmarks",
		Benchmarks:  []string{"BTC/USDT:USDT", "ETH/USDT:USDT"},
		Window:      window,
		EntryS:      1.15,
		ExitLongS:   0.40,
		ExitShortS:  0.40,
		StopS:       4.5,
		MaxHoldBars: 25,
		TargetGross: 0.85,
	}

	// 1. Direct builder verification of exact dollar neutrality AND exact BTC/ETH benchmark-beta neutrality!
	allSIDs := []int32{1, 2, 3, 4, 5, 6, 7}
	u := factor.Universe{Version: "ubench", Investable: allSIDs, Tradable: allSIDs}
	directB, err := newResidualStatArbBuilder(spec, u, sidMap, nil)
	if err != nil {
		t.Fatal(err)
	}
	pBase := genPrices(false)
	checkedBars := 0
	for bar := 1; bar <= totalBars; bar++ {
		lpMap := make(map[int32]float64, 7)
		for sid := int32(1); sid <= 7; sid++ {
			lpMap[sid] = math.Log(pBase[sid][bar])
		}
		w, _ := feedResidualBuilder(t, directB, uint64(bar), 1000, lpMap, u)
		if len(w) == 0 {
			continue
		}
		checkedBars++
		var sumW, grossW float64
		for _, v := range w {
			sumW += v
			grossW += math.Abs(v)
		}
		if math.Abs(sumW) >= 1e-9 {
			t.Fatalf("bar %d benchmarks dollar-neutrality broken: sum(w)=%v (%v)", bar, sumW, w)
		}
		if math.Abs(grossW-spec.TargetGross) > 1e-6 {
			t.Fatalf("bar %d expected gross=%v, got %v", bar, spec.TargetGross, grossW)
		}

		// Recompute exact rolling OLS benchmark betas on [bar-window+1 .. bar] and check B^T w == 0!
		design := make([][]float64, window)
		for k := range window {
			bIdx := bar - window + 1 + k
			r1 := pBase[1][bIdx]/pBase[1][bIdx-1] - 1
			r2 := pBase[2][bIdx]/pBase[2][bIdx-1] - 1
			design[k] = []float64{1, r1, r2}
		}
		expBTC := w[1] * 1.0
		expETH := w[2] * 1.0
		for sid := int32(3); sid <= 7; sid++ {
			if w[sid] == 0 {
				continue
			}
			y := make([]float64, window)
			for k := range window {
				bIdx := bar - window + 1 + k
				y[k] = pBase[sid][bIdx]/pBase[sid][bIdx-1] - 1
			}
			coef, _, err := factor.LeastSquares(design, y, nil)
			if err != nil {
				t.Fatal(err)
			}
			expBTC += coef[1] * w[sid]
			expETH += coef[2] * w[sid]
		}
		if math.Abs(expBTC) >= 1e-9 || math.Abs(expETH) >= 1e-9 {
			t.Fatalf("bar %d benchmark-beta neutrality broken: expBTC=%v expETH=%v (w=%v)", bar, expBTC, expETH, w)
		}
	}
	if checkedBars == 0 {
		t.Fatal("expected active bars in benchmarks mode")
	}

	// 2. Full Weights backtest with PIT mid-run delisting of SOL (sid=3) at delistBar + No-Lookahead verification!
	const stratName = "test-res-statarb-bench-pit"
	if err := RegisterResidualStatArb(stratName, spec); err != nil {
		t.Fatal(err)
	}

	outDelist := &captureTargetsOutput{}
	resDelist, err := Run(context.Background(), makeMultiSymbolReplayConfig(t, stratName, sidMap, totalBars, func(bar int64, sid int32) (float64, bool) {
		if sid == 3 && bar >= delistBar {
			return 0, false // SOL delists mid-run!
		}
		return pBase[sid][bar], true
	}), nil, outDelist)
	if err != nil {
		t.Fatal(err)
	}
	if resDelist.Incomplete != 0 {
		t.Fatalf("expected 0 Incomplete periods under PIT reduction, got %d", resDelist.Incomplete)
	}
	if resDelist.UniverseDrops == 0 || resDelist.DelistExits == 0 {
		t.Fatalf("expected PIT UniverseDrops > 0 and DelistExits > 0, got drops=%d exits=%d", resDelist.UniverseDrops, resDelist.DelistExits)
	}
	if math.Abs(resDelist.Attribution.Residual) > 1e-9*resDelist.Attribution.InitialNAV {
		t.Fatalf("attribution residual out of tolerance: %v", resDelist.Attribution.Residual)
	}
	postDelistActive := 0
	for i := delistBar - 1; i < len(outDelist.targets); i++ {
		w := outDelist.targets[i]
		if w[3] != 0 {
			t.Fatalf("delisted sid=3 must have zero weight at/after bar %d: %v", i+1, w)
		}
		if len(w) > 0 {
			postDelistActive++
			var sum float64
			for _, v := range w {
				sum += v
			}
			if math.Abs(sum) >= 1e-9 {
				t.Fatalf("post-delist bar %d broke dollar neutrality: %v", i+1, sum)
			}
		}
	}
	if postDelistActive == 0 {
		t.Fatal("expected strategy to continue trading remaining coins after PIT delist")
	}

	// 3. Strict No-Lookahead: altered prices after splitBar must leave bars 1..splitBar bit-identical!
	pAltered := genPrices(true)
	outFullBase := &captureTargetsOutput{}
	if _, err := Run(context.Background(), makeMultiSymbolReplayConfig(t, stratName, sidMap, totalBars, func(bar int64, sid int32) (float64, bool) {
		return pBase[sid][bar], true
	}), nil, outFullBase); err != nil {
		t.Fatal(err)
	}
	outFullAltered := &captureTargetsOutput{}
	if _, err := Run(context.Background(), makeMultiSymbolReplayConfig(t, stratName, sidMap, totalBars, func(bar int64, sid int32) (float64, bool) {
		return pAltered[sid][bar], true
	}), nil, outFullAltered); err != nil {
		t.Fatal(err)
	}
	preSplitActive := 0
	for i := 0; i < splitBar; i++ {
		w1, w2 := outFullBase.targets[i], outFullAltered.targets[i]
		if len(w1) > 0 {
			preSplitActive++
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
	if preSplitActive == 0 {
		t.Fatal("expected active targets prior to splitBar")
	}
}
