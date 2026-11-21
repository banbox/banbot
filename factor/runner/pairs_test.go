package runner

import (
	"context"
	"math"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
	"github.com/banbox/banbot/orm"
)

func feedPairsBuilder(t *testing.T, b PortfolioBuilder, seq uint64, nav float64, logPrices map[int32]float64, tradable []int32) (map[int32]float64, []factor.Diagnostic) {
	t.Helper()
	vals := make(map[int32]factor.Numeric, len(logPrices))
	for sid, lp := range logPrices {
		vals[sid] = factor.Numeric{Value: lp, Validity: factor.Valid}
	}
	at := int64(seq) * 3600000
	frame := factor.Frame{DecisionTime: at, Values: map[string]map[int32]factor.Numeric{pairsLogOutput: vals}}
	spec := factor.PortfolioSpec{
		StrategyID: "s", AccountID: "a", SnapshotID: "snap", PlanHash: "p", FactorPlanHash: "f", UniverseVersion: "u",
		Budget: factor.FrozenBudget{Version: "b", Currency: "USDT", NAV: nav},
		PlanSequence: seq, DecisionTime: at, ExecutableAt: at + 1, ExpireAt: at + 60000, Mode: factor.Full,
	}
	p, diags, err := b(frame, factor.Universe{Version: "u", Tradable: tradable}, spec, research.PortfolioDefinition{})
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		return nil, diags
	}
	return p.Targets(), p.Spec().Diagnostics
}

func TestPairsSpecValidationNewOptions(t *testing.T) {
	base := PairsSpec{
		Pairs:  []Pair{{Y: "Y", X: "X"}},
		Window: 5,
		Band:   statistics.BandConfig{Entry: 2, Exit: 0.5},
	}
	sids := map[int32]string{1: "Y", 2: "X"}
	bad := []PairsSpec{
		func() PairsSpec { s := base; s.MinBeta = -0.1; return s }(),
		func() PairsSpec { s := base; s.MinBeta = math.NaN(); return s }(),
		func() PairsSpec { s := base; s.DriftBand = -0.01; return s }(),
		func() PairsSpec { s := base; s.DriftBand = math.Inf(1); return s }(),
		func() PairsSpec { s := base; s.WeightMode = "risk_parity"; return s }(),
		func() PairsSpec { s := base; s.MaxPairGross = -0.1; return s }(),
		func() PairsSpec { s := base; s.MaxPairGross = 1.01; return s }(),
	}
	for i, spec := range bad {
		if _, err := newPairsBuilder(spec, sids); err == nil {
			t.Fatalf("case %d accepted invalid spec: %+v", i, spec)
		}
	}
}

func TestPairsEntryBetaGuardBlocksNegativeAndSmallBeta(t *testing.T) {
	sids := map[int32]string{1: "Y", 2: "X"}
	band := statistics.BandConfig{Entry: 1.0, Exit: 0.2}

	defaultBuilder, err := newPairsBuilder(PairsSpec{
		Pairs: []Pair{{Y: "Y", X: "X"}}, Window: 6, Band: band,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}
	allowNegBuilder, err := newPairsBuilder(PairsSpec{
		Pairs: []Pair{{Y: "Y", X: "X"}}, Window: 6, Band: band, AllowNegativeBeta: true,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}

	// Construct negatively correlated window (y = -0.8 * x + noise) followed by a z > Entry dislocation while slope < 0.
	xs := []float64{1.00, 1.04, 0.97, 1.05, 1.02, 0.98}
	ys := []float64{-0.80, -0.832, -0.775, -0.841, -0.816, -0.72} // positive jump at x < mean(x) keeps slope strictly < 0
	var wDef, wAllow map[int32]float64
	for i := range xs {
		wDef, _ = feedPairsBuilder(t, defaultBuilder, uint64(i+1), 1000, map[int32]float64{1: ys[i], 2: xs[i]}, []int32{1, 2})
		wAllow, _ = feedPairsBuilder(t, allowNegBuilder, uint64(i+1), 1000, map[int32]float64{1: ys[i], 2: xs[i]}, []int32{1, 2})
	}
	if len(wDef) != 0 {
		t.Fatalf("default builder must block entry when beta <= 0, got %v", wDef)
	}
	if len(wAllow) != 2 {
		t.Fatalf("AllowNegativeBeta builder must enter, got %v", wAllow)
	}

	// MinBeta guard: positive slope ~0.25 blocked when MinBeta=0.5, allowed when MinBeta=0.1;
	// also verify an already-open position still exits cleanly even if rolling beta later turns negative.
	minHigh, err := newPairsBuilder(PairsSpec{
		Pairs: []Pair{{Y: "Y", X: "X"}}, Window: 6, Band: band, MinBeta: 0.5,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}
	minLow, trLow, err := newTrackedPairsBuilder(PairsSpec{
		Pairs: []Pair{{Y: "Y", X: "X"}}, Window: 6, Band: band, MinBeta: 0.1,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}
	// Place x_6 at the exact window mean (1.017) so the +0.08 y-excursion has zero leverage on OLS beta (~0.25).
	pxs := []float64{1.00, 1.05, 0.96, 1.06, 1.015, 1.017}
	for i := range pxs {
		y := 0.25*pxs[i] + 0.001*float64(i%2)
		if i == len(pxs)-1 {
			y += 0.08 // positive excursion at high x keeps beta > 0 (~0.35)
		}
		wH, _ := feedPairsBuilder(t, minHigh, uint64(i+1), 1000, map[int32]float64{1: y, 2: pxs[i]}, []int32{1, 2})
		wL, _ := feedPairsBuilder(t, minLow, uint64(i+1), 1000, map[int32]float64{1: y, 2: pxs[i]}, []int32{1, 2})
		if i == len(pxs)-1 {
			if len(wH) != 0 {
				t.Fatalf("MinBeta=0.5 must reject beta ~0.35, got %v", wH)
			}
			if len(wL) != 2 {
				t.Fatalf("MinBeta=0.1 must accept beta ~0.35, got %v", wL)
			}
		}
	}
	entryBeta := trLow.states[0].beta
	if entryBeta <= 0.1 || entryBeta >= 0.5 {
		t.Fatalf("unexpected entry beta %v", entryBeta)
	}
	// Feed bars whose rolling OLS slope turns negative while the frozen-beta spread reverts to window mean -> existing position still closes.
	closed := false
	negSlopeX := []float64{1.00, 1.10, 0.90, 1.10, 0.90, 1.00}
	negSlopeY := []float64{0.35, 0.25, 0.45, 0.25, 0.45, 0.35} // slope is -1.0 < 0, while at x=1.0, y-entryBeta*x equals exact window mean!
	for j := range negSlopeX {
		wL, _ := feedPairsBuilder(t, minLow, uint64(10+j), 1000, map[int32]float64{1: negSlopeY[j], 2: negSlopeX[j]}, []int32{1, 2})
		if j == len(negSlopeX)-1 && len(wL) == 0 {
			closed = true
		}
	}
	if !closed {
		t.Fatal("open position failed to exit when rolling beta turned negative")
	}
}

func TestPairsFrozenExitZMatchesHeldHedgePnL(t *testing.T) {
	sids := map[int32]string{1: "Y", 2: "X"}
	band := statistics.BandConfig{Entry: 1.5, Exit: 0.3, Stop: 3.5}

	frozenB, frozenTr, err := newTrackedPairsBuilder(PairsSpec{
		Pairs: []Pair{{Y: "Y", X: "X"}}, Window: 8, Band: band,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}
	dynB, dynTr, err := newTrackedPairsBuilder(PairsSpec{
		Pairs: []Pair{{Y: "Y", X: "X"}}, Window: 8, Band: band, DynamicExitZ: true,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}

	// Phase 1: cointegrated around y = 1.0 * x with small oscillation, then open LONG spread (y drops below x) at seq=8.
	xs := []float64{2.00, 2.02, 1.98, 2.03, 1.97, 2.01, 1.99, 1.98}
	for i, x := range xs {
		noise := 0.004 * float64((i%3)-1)
		y := x + noise
		if i == len(xs)-1 {
			y = x - 0.05 // z << -1.5, opens long spread (+Y, -X) with frozen beta ~ 1.0
		}
		wf, _ := feedPairsBuilder(t, frozenB, uint64(i+1), 1000, map[int32]float64{1: y, 2: x}, []int32{1, 2})
		wd, _ := feedPairsBuilder(t, dynB, uint64(i+1), 1000, map[int32]float64{1: y, 2: x}, []int32{1, 2})
		if i == len(xs)-1 && (wf[1] <= 0 || wd[1] <= 0) {
			t.Fatalf("both builders must open long spread: frozen=%v dyn=%v", wf, wd)
		}
	}
	b0 := frozenTr.states[0].beta

	// Phase 2: regime shift where rolling slope steepens to 3.0 while the actual held spread (y - b0*x) keeps bleeding lower!
	// Specifically: x rises steadily and y lags behind b0*x on the final bar, OR x drops and y drops 3x faster so refit OLS
	// fits the new slope=3.0 (giving refit residual ~ 0 -> false "exit"!) whereas held spread y - b0*x diverged badly (z <= -Stop)!
	shiftX := []float64{1.95, 1.92, 1.89, 1.86, 1.83, 1.80, 1.77, 1.70}
	var wf, wd map[int32]float64
	for j, x := range shiftX {
		// Follow new line y = 3.0*(x - 1.98) + 1.93 + tiny_j so refit OLS sees near-zero residual on the new slope=3 line,
		// while actual held position (long Y, short b0~1 * X) loses heavily because Y falls 3x as fast as X!
		y := 1.93 + 3.0*(x-1.98) + 0.002*math.Sin(float64(j+1))
		wf, _ = feedPairsBuilder(t, frozenB, uint64(9+j), 1000, map[int32]float64{1: y, 2: x}, []int32{1, 2})
		wd, _ = feedPairsBuilder(t, dynB, uint64(9+j), 1000, map[int32]float64{1: y, 2: x}, []int32{1, 2})
	}
	repFrozen := frozenTr.report(1000)[0]
	repDyn := dynTr.report(1000)[0]
	_, _ = wf, wd
	// Dynamic refit Z was fooled into closing with reason "exit" on a losing trade, whereas frozen-beta Z never mistook
	// slope drift for mean reversion (it keeps tracking y - b0*x and either stops out or stays aware of divergence).
	if len(repDyn.Episodes) == 0 || repDyn.Episodes[0].Reason != "exit" || repDyn.Episodes[0].PnL >= 0 {
		t.Fatalf("expected dynamic exit Z to false-exit at a loss under slope drift, got %+v", repDyn.Episodes)
	}
	if len(repFrozen.Episodes) == 0 {
		t.Fatalf("expected episode in frozen report: %+v", repFrozen)
	}
	if repFrozen.Episodes[0].Reason == "exit" {
		t.Fatalf("frozen exit Z must not report mean-reversion exit while y - b0*x is diverging: %+v (b0=%v)", repFrozen.Episodes[0], b0)
	}
}

func TestPairsDriftBandSuppressesChurnAndReconcilesLedger(t *testing.T) {
	// 1. Direct builder check: small drift inside DriftBand holds drifted weights (0 active turnover),
	//    drift >= DriftBand snaps back to ideal weights.
	sids := map[int32]string{1: "Y", 2: "X"}
	b, tr, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:     []Pair{{Y: "Y", X: "X"}},
		Window:    5,
		Gross:     0.8,
		DriftBand: 0.05,
		Band:      statistics.BandConfig{Entry: 1.2, Exit: 0.1},
	}, sids)
	if err != nil {
		t.Fatal(err)
	}
	xs := []float64{1.00, 1.02, 0.99, 1.03, 1.01}
	for i, x := range xs {
		feedPairsBuilder(t, b, uint64(i+1), 1000, map[int32]float64{1: x, 2: x}, []int32{1, 2})
	}
	// Open short spread at seq=6
	wOpen, _ := feedPairsBuilder(t, b, 6, 1000, map[int32]float64{1: 1.06, 2: 1.02}, []int32{1, 2})
	if len(wOpen) != 2 {
		t.Fatalf("expected open position, got %v", wOpen)
	}
	turnAfterOpen := tr.report(1000)[0].Turnover
	// Tiny move (+0.5% on Y, flat X): spread stays open, weight drift ~0.002 << DriftBand(0.05) -> buffered, zero active turnover!
	nav7 := 1000 * (1 + wOpen[1]*(math.Exp(1.065-1.06)-1))
	wBuf, _ := feedPairsBuilder(t, b, 7, nav7, map[int32]float64{1: 1.065, 2: 1.02}, []int32{1, 2})
	if len(wBuf) != 2 {
		t.Fatalf("expected held position, got %v", wBuf)
	}
	if gotTurn := tr.report(1000)[0].Turnover; math.Abs(gotTurn-turnAfterOpen) > 1e-12 {
		t.Fatalf("buffered step must add zero active turnover: before=%v after=%v", turnAfterOpen, gotTurn)
	}
	// Large move (+12% on Y, spread still > Exit): weight drift exceeds 0.05 -> snaps back to ideal gross 0.8!
	nav8 := nav7 * (1 + wBuf[1]*(math.Exp(1.18-1.065)-1))
	wSnap, _ := feedPairsBuilder(t, b, 8, nav8, map[int32]float64{1: 1.18, 2: 1.02}, []int32{1, 2})
	if gross := math.Abs(wSnap[1]) + math.Abs(wSnap[2]); math.Abs(gross-0.8) > 1e-9 {
		t.Fatalf("expected snap back to ideal gross 0.8, got %v (%v)", gross, wSnap)
	}
	if gotTurn := tr.report(1000)[0].Turnover; gotTurn <= turnAfterOpen {
		t.Fatalf("snap-back rebalance must record turnover: before=%v after=%v", turnAfterOpen, gotTurn)
	}

	// 2. Full Weights backtest via Run(): DriftBand reduces ledger fees & turnover while keeping Attribution/PairsResidual reconciled.
	runWithBand := func(name string, driftBand float64) Result {
		err := RegisterPairs(name, func(p *Params) (PairsSpec, error) {
			return PairsSpec{
				Pairs:     []Pair{{Y: "ETH", X: "BTC"}},
				Window:    20,
				DriftBand: driftBand,
				Band:      statistics.BandConfig{Entry: 1.2, Exit: 0.1, MaxHold: 30},
			}, p.Err()
		})
		if err != nil {
			t.Fatal(err)
		}
		c := pitConfig(t, map[int32]int64{1: 1, 2: 1}, 1, 0)
		c.Plan, c.Combo, c.Definition = nil, research.ComboSpec{}, name
		c.Factor = research.DefaultMomentumVolConfig()
		c.Factor.TimeFrame = "1h"
		c.Snapshot.Universe = factor.Universe{Version: "two", Static: true, Investable: []int32{1, 2}, Tradable: []int32{1, 2}, Evaluation: []int32{1, 2}, Tracked: []int32{1, 2}}
		c.Snapshot.SIDMap = map[int32]string{1: "ETH", 2: "BTC"}
		c.MaxRecords, c.MaxPending, c.DecisionInterval, c.ExpiryMS = 3000, 10, 3600000, 60000
		const hour int64 = 3600000
		store, err := factor.NewVersionStore(3000)
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(23))
		lx, u := math.Log(100), 0.0
		for bar := int64(1); bar <= 220; bar++ {
			lx += rng.NormFloat64() * 0.012
			u = 0.88*u + rng.NormFloat64()*0.02
			for sid, price := range map[int32]float64{1: math.Exp(1.1*lx + u), 2: math.Exp(lx)} {
				at := bar * hour
				_ = store.Put(factor.VersionRecord{Series: orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, TimeMS: at - hour, EndMS: at, Closed: true, Values: map[string]any{"close": price}}, EventTime: at, AvailableAt: at, IngestedAt: at, Revision: 1, SourceVersion: "v1"})
				_ = store.Put(factor.VersionRecord{Series: orm.DataSeries{Source: "tick", TimeFrame: "event", Sid: sid, TimeMS: at + 1, EndMS: at + 1, Closed: true, Values: map[string]any{"price": price}}, EventTime: at + 1, AvailableAt: at + 1, IngestedAt: at + 1, Revision: 1, SourceVersion: "v1"})
			}
		}
		path := filepath.Join(t.TempDir(), "drift.gob")
		if _, err = store.Export(path); err != nil {
			t.Fatal(err)
		}
		c.Chunks = []Chunk{{Path: path, From: hour, To: 220*hour + 2}}
		res, err := Run(context.Background(), c, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	resNoBand := runWithBand("test-pairs-drift-0", 0)
	resBand := runWithBand("test-pairs-drift-pos", 0.05)
	if resBand.Book.Fees >= resNoBand.Book.Fees || resBand.Book.Turnover >= resNoBand.Book.Turnover {
		t.Fatalf("expected DriftBand to reduce ledger fees and turnover: fees %v vs %v, turnover %v vs %v",
			resBand.Book.Fees, resNoBand.Book.Fees, resBand.Book.Turnover, resNoBand.Book.Turnover)
	}
	if math.Abs(resBand.Attribution.Residual) > 1e-9*resBand.Attribution.InitialNAV {
		t.Fatalf("attribution residual out of tolerance: %v", resBand.Attribution.Residual)
	}
	if math.Abs(resBand.PairsNAVReturn-resBand.PairsGrossPnL-resBand.PairsResidual) > 1e-12 {
		t.Fatalf("pairs reconcile broken: %+v", resBand)
	}
	costs := (resBand.Attribution.Fees + resBand.Attribution.Slippage + resBand.Attribution.Funding + resBand.Attribution.PITCost) / resBand.Attribution.InitialNAV
	if math.Abs(resBand.PairsResidual+costs) > 1e-6 {
		t.Fatalf("with exact decision-close fills and DriftBand, PairsResidual (%v) must match -costs (%v)", resBand.PairsResidual, -costs)
	}
}

func TestPairsInverseVolWeightAllocationAndCap(t *testing.T) {
	sids := map[int32]string{1: "Y1", 2: "X1", 3: "Y2", 4: "X2", 5: "Y3", 6: "X3"}
	band := statistics.BandConfig{Entry: 1.2, Exit: 0.2}

	// 1. Two pairs with residual std ratio 1 : 3 -> inverse_vol allocates 3 : 1 of TotalGross = 1.0 (0.75 and 0.25).
	b2, tr2, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:      []Pair{{Y: "Y1", X: "X1"}, {Y: "Y2", X: "X2"}},
		Window:     6,
		WeightMode: "inverse_vol",
		Band:       band,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}
	baseX := []float64{2.00, 2.03, 1.97, 2.04, 1.98, 2.02}
	resPattern := []float64{0.01, -0.01, 0.01, -0.01, 0.01, 0.035} // last bar > Entry, std ratio controlled by scale
	var w map[int32]float64
	for i, x := range baseX {
		p := map[int32]float64{
			1: x + 1.0*resPattern[i], // low vol pair (scale 1)
			2: x,
			3: x + 3.0*resPattern[i], // 3x higher vol pair (scale 3)
			4: x,
			5: x, 6: x,
		}
		w, _ = feedPairsBuilder(t, b2, uint64(i+1), 1000, p, []int32{1, 2, 3, 4, 5, 6})
	}
	g1 := math.Abs(w[1]) + math.Abs(w[2])
	g2 := math.Abs(w[3]) + math.Abs(w[4])
	if g1 <= 0 || g2 <= 0 {
		t.Fatalf("both pairs must be open, got w=%v", w)
	}
	stdRatio := tr2.states[1].lastStd / tr2.states[0].lastStd
	if math.Abs(g1/g2-stdRatio) > 1e-6 || math.Abs(g1/g2-3.0) > 1e-2 {
		t.Fatalf("expected inverse-vol ratio ~3.0 (exact %v), got g1=%v g2=%v ratio=%v", stdRatio, g1, g2, g1/g2)
	}
	if math.Abs((g1+g2)-1.0) > 1e-9 {
		t.Fatalf("expected total gross 1.0, got %v", g1+g2)
	}

	// 2. Three pairs with MaxPairGross cap (0.45) and extreme low-vol pair 1:
	//    pair 1 capped at 0.45, remaining 0.55 split inversely between pair 2 & pair 3, total gross <= 1.0.
	b3, _, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:        []Pair{{Y: "Y1", X: "X1"}, {Y: "Y2", X: "X2"}, {Y: "Y3", X: "X3"}},
		Window:       6,
		WeightMode:   "inverse_vol",
		MaxPairGross: 0.45,
		Band:         band,
	}, sids)
	if err != nil {
		t.Fatal(err)
	}
	for i, x := range baseX {
		p := map[int32]float64{
			1: x + 0.1*resPattern[i], // ultra-low vol -> wants >85% uncapped, clipped to 0.45
			2: x,
			3: x + 1.0*resPattern[i], // medium vol (scale 1)
			4: x,
			5: x + 2.0*resPattern[i], // higher vol (scale 2)
			6: x,
		}
		w, _ = feedPairsBuilder(t, b3, uint64(i+1), 1000, p, []int32{1, 2, 3, 4, 5, 6})
	}
	gp1 := math.Abs(w[1]) + math.Abs(w[2])
	gp2 := math.Abs(w[3]) + math.Abs(w[4])
	gp3 := math.Abs(w[5]) + math.Abs(w[6])
	if math.Abs(gp1-0.45) > 1e-9 {
		t.Fatalf("pair 1 must be capped at MaxPairGross=0.45, got %v", gp1)
	}
	if gp2 > 0.45+1e-9 || gp3 > 0.45+1e-9 || math.Abs(gp2/gp3-2.0) > 0.02 {
		t.Fatalf("remaining budget must split 2:1 under cap, got gp2=%v gp3=%v", gp2, gp3)
	}
	if total := gp1 + gp2 + gp3; math.Abs(total-1.0) > 1e-9 || total > 1+1e-9 {
		t.Fatalf("total gross must equal 1.0 (<= 1), got %v", total)
	}

	// 3. Single open pair among 4 pairs with default MaxPairGross=0 -> capped at 2*Gross = 0.50 (never takes 100% NAV).
	sids4 := map[int32]string{1: "Y1", 2: "X1", 3: "Y2", 4: "X2", 5: "Y3", 6: "X3", 7: "Y4", 8: "X4"}
	b4, _, err := newTrackedPairsBuilder(PairsSpec{
		Pairs:      []Pair{{Y: "Y1", X: "X1"}, {Y: "Y2", X: "X2"}, {Y: "Y3", X: "X3"}, {Y: "Y4", X: "X4"}},
		Window:     6,
		WeightMode: "inverse_vol",
		Band:       band,
	}, sids4)
	if err != nil {
		t.Fatal(err)
	}
	flatNoise := []float64{0.002, -0.002, 0.002, -0.002, 0.001, 0.0} // z=0 on final bar so pairs 2..4 stay flat
	for i, x := range baseX {
		yFlat := x + flatNoise[i]
		p := map[int32]float64{
			1: x + resPattern[i], 2: x, // only pair 1 opens
			3: yFlat, 4: x,
			5: yFlat, 6: x,
			7: yFlat, 8: x,
		}
		w, _ = feedPairsBuilder(t, b4, uint64(i+1), 1000, p, []int32{1, 2, 3, 4, 5, 6, 7, 8})
	}
	if singleGross := math.Abs(w[1]) + math.Abs(w[2]); math.Abs(singleGross-0.5) > 1e-9 {
		t.Fatalf("single open pair out of 4 must be capped at 2*Gross=0.5, got %v", singleGross)
	}
}
