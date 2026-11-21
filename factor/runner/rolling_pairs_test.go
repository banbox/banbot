package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
	"github.com/banbox/banbot/orm"
)

func TestRollingPairsSpecValidation(t *testing.T) {
	base := RollingPairsSpec{
		FormationBars: 30,
		ReselectBars:  10,
		PairCount:     2,
		MaxPerSymbol:  1,
		Band:          statistics.BandConfig{Entry: 1.5, Exit: 0.3, Stop: 3.5},
	}
	u := factor.Universe{Version: "u", Investable: []int32{1, 2, 3}, Tradable: []int32{1, 2, 3}}
	sids := map[int32]string{1: "A", 2: "B", 3: "C"}

	bad := []RollingPairsSpec{
		func() RollingPairsSpec { s := base; s.FormationBars = 19; return s }(),
		func() RollingPairsSpec { s := base; s.ReselectBars = 0; return s }(),
		func() RollingPairsSpec { s := base; s.PairCount = -1; return s }(),
		func() RollingPairsSpec { s := base; s.MaxPerSymbol = -1; return s }(),
		func() RollingPairsSpec { s := base; s.SelectionMethod = "johansen"; return s }(),
		func() RollingPairsSpec { s := base; s.MinCoverage = -0.1; return s }(),
		func() RollingPairsSpec { s := base; s.MinCoverage = 1.1; return s }(),
		func() RollingPairsSpec { s := base; s.MinCorr = -0.1; return s }(),
		func() RollingPairsSpec { s := base; s.MinCorr = 1.1; return s }(),
		func() RollingPairsSpec { s := base; s.MaxADF = 2.86; return s }(),
		func() RollingPairsSpec { s := base; s.ADFLags = -1; return s }(),
		func() RollingPairsSpec { s := base; s.ADFLags = 26; return s }(),
		func() RollingPairsSpec { s := base; s.MinHalfLife = -1; return s }(),
		func() RollingPairsSpec { s := base; s.MinHalfLife = 20; s.MaxHalfLife = 10; return s }(),
		func() RollingPairsSpec { s := base; s.Window = 2; return s }(),
		func() RollingPairsSpec { s := base; s.Gross = 0.6; return s }(), // 0.6 * 2 > 1.0
		func() RollingPairsSpec { s := base; s.MinBeta = -0.1; return s }(),
		func() RollingPairsSpec { s := base; s.DriftBand = -0.01; return s }(),
		func() RollingPairsSpec { s := base; s.WeightMode = "kelly"; return s }(),
		func() RollingPairsSpec { s := base; s.MaxPairGross = 1.2; return s }(),
		func() RollingPairsSpec { s := base; s.HandoffMode = "hard"; return s }(),
		func() RollingPairsSpec { s := base; s.Candidates = []string{"A"}; return s }(),
		func() RollingPairsSpec { s := base; s.Candidates = []string{"A", ""}; return s }(),
		func() RollingPairsSpec { s := base; s.Candidates = []string{"A", "MISSING"}; return s }(),
	}
	for i, spec := range bad {
		if _, err := newRollingPairsBuilder(spec, u, sids); err == nil {
			t.Fatalf("case %d accepted invalid spec: %+v", i, spec)
		}
	}
	if err := RegisterRollingPairs("", nil); err == nil {
		t.Fatal("expected registration error on empty name/nil build")
	}
}

func makeMultiSymbolReplayConfig(t *testing.T, defName string, sidMap map[int32]string, bars int64, priceFn func(bar int64, sid int32) (float64, bool)) Config {
	t.Helper()
	const hour int64 = 3600000
	var sids []int32
	for sid := range sidMap {
		sids = append(sids, sid)
	}
	for i := 0; i < len(sids); i++ {
		for j := i + 1; j < len(sids); j++ {
			if sids[j] < sids[i] {
				sids[i], sids[j] = sids[j], sids[i]
			}
		}
	}
	maxRecs := int(bars)*len(sids)*3 + 100
	store, err := factor.NewVersionStore(maxRecs)
	if err != nil {
		t.Fatal(err)
	}
	for bar := int64(1); bar <= bars; bar++ {
		at := bar * hour
		for _, sid := range sids {
			price, ok := priceFn(bar, sid)
			if !ok {
				continue
			}
			if err := store.Put(factor.VersionRecord{
				Series:        orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, TimeMS: at - hour, EndMS: at, Closed: true, Values: map[string]any{"close": price}},
				EventTime:     at,
				AvailableAt:   at,
				IngestedAt:    at,
				Revision:      1,
				SourceVersion: "v1",
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.Put(factor.VersionRecord{
				Series:        orm.DataSeries{Source: "tick", TimeFrame: "event", Sid: sid, TimeMS: at + 1, EndMS: at + 1, Closed: true, Values: map[string]any{"price": price}},
				EventTime:     at + 1,
				AvailableAt:   at + 1,
				IngestedAt:    at + 1,
				Revision:      1,
				SourceVersion: "v1",
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	path := filepath.Join(t.TempDir(), "rolling_pairs.gob")
	if _, err := store.Export(path); err != nil {
		t.Fatal(err)
	}
	c := pitConfig(t, map[int32]int64{1: 1, 2: 1}, 1, 0)
	c.Plan, c.Combo, c.Definition = nil, research.ComboSpec{}, defName
	c.Factor = research.DefaultMomentumVolConfig()
	c.Factor.TimeFrame = "1h"
	c.Snapshot.Universe = factor.Universe{
		Version:    "rolling-u",
		Static:     true,
		Investable: sids,
		Reference:  sids,
		Tradable:   sids,
		Evaluation: sids,
		Tracked:    sids,
	}
	c.Snapshot.SIDMap = sidMap
	c.MaxRecords = maxRecs
	c.MaxPending = 32
	c.DecisionInterval = hour
	c.LatencyMS = 1
	c.ExpiryMS = 60000
	c.Chunks = []Chunk{{Path: path, From: hour, To: bars*hour + 2}}
	return c
}

type captureTargetsOutput struct {
	atMS    []int64
	targets []map[int32]float64
	diags   [][]factor.Diagnostic
}

func (o *captureTargetsOutput) Decision(f factor.Frame, p *factor.TargetPortfolio, d []factor.Diagnostic) error {
	o.atMS = append(o.atMS, f.DecisionTime)
	if p == nil {
		o.targets = append(o.targets, nil)
	} else {
		o.targets = append(o.targets, p.Targets())
	}
	o.diags = append(o.diags, d)
	return nil
}
func (o *captureTargetsOutput) Evaluation(research.Report) error                             { return nil }
func (o *captureTargetsOutput) Executed(*factor.TargetPortfolio, backtest.State, int64) error { return nil }

func isPairAB(p PairResult, s1, s2 string) bool {
	return (p.Y == s1 && p.X == s2) || (p.Y == s2 && p.X == s1)
}

// Two-stage synthetic regime shift:
// - Bars 1..120: A-B is strongly cointegrated (stationary AR(1) OU spread around shared stochastic trend), C & D are independent random walks.
// - Bars 121..240: A-B breaks cointegration and drifts apart permanently, while C-D locks into strong cointegration!
func TestRollingPairsRegimeShiftSwitchesFromABToCD(t *testing.T) {
	const totalBars = 240
	rng := rand.New(rand.NewSource(20261121))
	prices := make(map[int32][]float64, 4)
	for sid := int32(1); sid <= 4; sid++ {
		prices[sid] = make([]float64, totalBars+1)
	}
	lB, lD := math.Log(100), math.Log(50)
	lAFree, lCFree := math.Log(100), math.Log(50)
	uAB, uCD := 0.0, 0.0
	for bar := 1; bar <= totalBars; bar++ {
		dB := rng.NormFloat64() * 0.032
		dD := rng.NormFloat64() * 0.032
		lB += dB
		lD += dD
		if bar <= 120 {
			// Phase 1: A = B * exp(uAB) with true AR(1) phi=0.50 (discrete half-life = 1.0 bar, ADF << -3.5, corr > 0.9)
			uAB = 0.50*uAB + rng.NormFloat64()*0.011
			lCFree += rng.NormFloat64()*0.030 - 0.004
			prices[1][bar] = math.Exp(lB + uAB)
			prices[2][bar] = math.Exp(lB)
			prices[3][bar] = math.Exp(lCFree)
			prices[4][bar] = math.Exp(lD)
			lAFree = lB + uAB
		} else {
			// Phase 2: A breaks away from B (uncorrelated walk + steady upward drift); C becomes cointegrated with D!
			lAFree += rng.NormFloat64()*0.030 + 0.008
			uCD = 0.50*uCD + rng.NormFloat64()*0.011
			if bar == 121 {
				lCFree = lCFree - lD
			}
			prices[1][bar] = math.Exp(lAFree)
			prices[2][bar] = math.Exp(lB)
			prices[3][bar] = math.Exp(lCFree + lD + uCD)
			prices[4][bar] = math.Exp(lD)
		}
	}

	const stratName = "test-rolling-regime-shift"
	err := RegisterRollingPairs(stratName, func(p *Params) (RollingPairsSpec, error) {
		return RollingPairsSpec{
			FormationBars:   60,
			ReselectBars:    60,
			PairCount:       1,
			MaxPerSymbol:    1,
			SelectionMethod: "cointegration",
			Window:          30,
			Band:            statistics.BandConfig{Entry: 1.3, Exit: 0.25, Stop: 4.0, MaxHold: 25},
		}, p.Err()
	})
	if err != nil {
		t.Fatal(err)
	}

	sidMap := map[int32]string{1: "A/USDT:USDT", 2: "B/USDT:USDT", 3: "C/USDT:USDT", 4: "D/USDT:USDT"}
	cfg := makeMultiSymbolReplayConfig(t, stratName, sidMap, totalBars, func(bar int64, sid int32) (float64, bool) {
		return prices[sid][bar], true
	})
	out := &captureTargetsOutput{}
	res, err := Run(context.Background(), cfg, nil, out)
	if err != nil {
		t.Fatal(err)
	}

	var abRes, cdRes *PairResult
	for i := range res.Pairs {
		p := &res.Pairs[i]
		if isPairAB(*p, "A/USDT:USDT", "B/USDT:USDT") {
			abRes = p
		} else if isPairAB(*p, "C/USDT:USDT", "D/USDT:USDT") {
			cdRes = p
		} else {
			t.Fatalf("unexpected cross-regime pair selected: %+v", *p)
		}
	}
	if abRes == nil || abRes.Trades == 0 {
		t.Fatalf("expected A-B to be selected and traded in Phase 1, got Pairs=%+v", res.Pairs)
	}
	if cdRes == nil || cdRes.Trades == 0 {
		t.Fatalf("expected C-D to be selected and traded after regime shift in Phase 2, got Pairs=%+v", res.Pairs)
	}
	const hour int64 = 3600000
	for _, ep := range abRes.Episodes {
		if ep.StartMS > 180*hour {
			t.Fatalf("A-B must not open new trades after Phase 2 formation replaced it: %+v", ep)
		}
	}
	for _, ep := range cdRes.Episodes {
		if ep.StartMS < 180*hour {
			t.Fatalf("C-D must not trade before Phase 2 formation completes at bar 180: %+v", ep)
		}
	}
	for i, w := range out.targets {
		gross := 0.0
		for _, v := range w {
			gross += math.Abs(v)
		}
		if gross > 1+1e-9 {
			t.Fatalf("bar %d exceeded gross limit 1.0: %v (%v)", i+1, gross, w)
		}
	}
	if math.Abs(res.Attribution.Residual) > 1e-9*res.Attribution.InitialNAV {
		t.Fatalf("attribution residual out of tolerance: %v", res.Attribution.Residual)
	}
	if math.Abs(res.PairsNAVReturn-res.PairsGrossPnL-res.PairsResidual) > 1e-12 {
		t.Fatalf("pairs PnL reconciliation broken: nav=%v gross=%v resid=%v", res.PairsNAVReturn, res.PairsGrossPnL, res.PairsResidual)
	}
}

func TestRollingPairsDistanceMethodAndMaxPerSymbol(t *testing.T) {
	// 6 symbols:
	// Cluster 1 around base1: A (center), B (offset +0.002*s -> SSD ~ 1st), C (offset -0.007*s -> opposite side of A, so |A-C|=0.007 < |B-C|=0.009 -> SSD ~ 2nd!)
	// Cluster 2 around independent base2: D and E (offset +0.008*s -> SSD ~ 3rd, disjoint from A/B/C!)
	// NEG moves opposite to base1 (negatively correlated -> rejected!)
	u := factor.Universe{Version: "u", Investable: []int32{1, 2, 3, 4, 5, 6}, Tradable: []int32{1, 2, 3, 4, 5, 6}}
	sids := map[int32]string{1: "A", 2: "B", 3: "C", 4: "D", 5: "E", 6: "NEG"}
	band := statistics.BandConfig{Entry: 1.2, Exit: 0.2}

	bMax1, trMax1, err := newTrackedRollingPairsBuilder(RollingPairsSpec{
		FormationBars:   24,
		ReselectBars:    24,
		PairCount:       2,
		MaxPerSymbol:    1,
		SelectionMethod: "distance",
		Window:          12,
		Band:            band,
	}, u, sids, nil)
	if err != nil {
		t.Fatal(err)
	}
	bMax2, trMax2, err := newTrackedRollingPairsBuilder(RollingPairsSpec{
		FormationBars:   24,
		ReselectBars:    24,
		PairCount:       2,
		MaxPerSymbol:    2,
		SelectionMethod: "distance",
		Window:          12,
		Band:            band,
	}, u, sids, nil)
	if err != nil {
		t.Fatal(err)
	}

	base1, base2 := 0.0, 0.0
	for bar := 1; bar <= 25; bar++ {
		if bar > 1 {
			base1 += 0.03 * math.Sin(float64(bar)*0.8)
			base2 += 0.03 * math.Cos(float64(bar)*0.55) // distinct factor from base1 so cross-cluster SSD is huge
		}
		osc := math.Sin(float64(bar) * 1.3)
		if bar == 1 {
			osc = 0
		}
		dislocB := 0.002 * osc
		dislocC := -0.007 * osc // opposite sign from B so |B - C| = 0.009 > |A - C| = 0.007!
		dislocE := 0.008 * osc  // |D - E| = 0.008 > |A - C| = 0.007, but < |B - C| = 0.009!
		if bar == 25 {
			dislocB = 0.015
			dislocC = -0.04
			dislocE = 0.04
		}
		lp := map[int32]float64{
			1: math.Log(100) + base1,
			2: math.Log(200) + base1 + dislocB,
			3: math.Log(50) + base1 + dislocC,
			4: math.Log(80) + base2,
			5: math.Log(120) + base2 + dislocE,
			6: math.Log(100) - base1 + 0.001*osc,
		}
		w1, _ := feedPairsBuilder(t, bMax1, uint64(bar), 1000, lp, []int32{1, 2, 3, 4, 5, 6})
		w2, _ := feedPairsBuilder(t, bMax2, uint64(bar), 1000, lp, []int32{1, 2, 3, 4, 5, 6})
		for _, w := range []map[int32]float64{w1, w2} {
			if w[6] != 0 {
				t.Fatalf("negatively correlated symbol NEG must never be selected: %v", w)
			}
		}
	}

	rep1 := trMax1.report(1000)
	if len(rep1) != 2 || !isPairAB(rep1[0], "A", "B") || !isPairAB(rep1[1], "D", "E") {
		t.Fatalf("MaxPerSymbol=1 expected [A-B, D-E], got %+v", rep1)
	}
	rep2 := trMax2.report(1000)
	if len(rep2) != 2 || !isPairAB(rep2[0], "A", "B") || !isPairAB(rep2[1], "A", "C") {
		t.Fatalf("MaxPerSymbol=2 expected [A-B, A-C], got %+v", rep2)
	}

	// Verify Candidates subset restricts screening strictly to declared symbols (["C", "D", "E"] -> only D-E is positively close).
	bCand, trCand, err := newTrackedRollingPairsBuilder(RollingPairsSpec{
		Candidates:      []string{"C", "D", "E"},
		FormationBars:   24,
		ReselectBars:    24,
		PairCount:       1,
		SelectionMethod: "distance",
		Window:          12,
		Band:            band,
	}, u, sids, nil)
	if err != nil {
		t.Fatal(err)
	}
	b1, b2 := 0.0, 0.0
	for bar := 1; bar <= 25; bar++ {
		if bar > 1 {
			b1 += 0.03 * math.Sin(float64(bar)*0.8)
			b2 += 0.03 * math.Cos(float64(bar)*0.55)
		}
		osc := math.Sin(float64(bar) * 1.3)
		if bar == 1 {
			osc = 0
		}
		de := 0.008 * osc
		if bar == 25 {
			de = 0.04
		}
		feedPairsBuilder(t, bCand, uint64(bar), 1000, map[int32]float64{
			1: math.Log(100) + b1,
			2: math.Log(200) + b1 + 0.001*osc,
			3: math.Log(50) + b1 - 0.007*osc,
			4: math.Log(80) + b2,
			5: math.Log(120) + b2 + de,
			6: math.Log(100) - b1,
		}, []int32{1, 2, 3, 4, 5, 6})
	}
	repCand := trCand.report(1000)
	if len(repCand) != 1 || !isPairAB(repCand[0], "D", "E") {
		t.Fatalf("Candidates=[C,D,E] must ignore A-B and pick D-E, got %+v", repCand)
	}
}

func TestRollingPairsGracefulVsImmediateHandoffAndGrossCap(t *testing.T) {
	// Two distinct factor clusters so cross-pairs {A,B}x{C,D} never match:
	// - Cluster 1: A, B around baseAB
	// - Cluster 2: C, D around baseCD (diverging factor path from baseAB)
	// PairCount=1 (Gross=1.0), FormationBars=24, ReselectBars=10, Window=10.
	// - Bars 1..24: A-B is tight (selected at bar 24).
	// - At bar 33 (just before reselection at bar 34): A-B opens a position and stays dislocated across bars 34..36,
	//   then reverts inside Exit at bar 37! Meanwhile over [11..34] C-D is much tighter than A-B so bar 34 selects C-D!
	// - C-D has entry signals at bar 35 (while A-B still holds the 1 slot under graceful!) and at bar 39 (after A-B exited at bar 37!).
	u := factor.Universe{Version: "u", Investable: []int32{1, 2, 3, 4}, Tradable: []int32{1, 2, 3, 4}}
	sids := map[int32]string{1: "A", 2: "B", 3: "C", 4: "D"}
	band := statistics.BandConfig{Entry: 1.5, Exit: 0.2, Stop: 5.0}

	makeBuilder := func(handoff, weightMode string, driftBand float64) (PortfolioBuilder, *rollingPairsTracker) {
		b, tr, err := newTrackedRollingPairsBuilder(RollingPairsSpec{
			FormationBars:   24,
			ReselectBars:    10,
			PairCount:       1,
			MaxPerSymbol:    1,
			SelectionMethod: "distance",
			Window:          10,
			Band:            band,
			WeightMode:      weightMode,
			DriftBand:       driftBand,
			HandoffMode:     handoff,
		}, u, sids, nil)
		if err != nil {
			t.Fatal(err)
		}
		return b, tr
	}

	bGrace, trGrace := makeBuilder("graceful", "equal", 0.02)
	bImm, trImm := makeBuilder("immediate", "inverse_vol", 0)

	baseAB, baseCD := 0.0, 0.0
	for bar := 1; bar <= 42; bar++ {
		if bar > 1 {
			// Periodic factor oscillations centered around opposite steady slopes; at bar 35 & 39 sin(...) crosses 0 (near window mean -> zero OLS leverage!)
			baseAB = 0.12*math.Sin(float64(bar)*math.Pi/5) + 0.015*float64(bar-1)
			baseCD = 0.12*math.Sin(float64(bar-39)*math.Pi/2.5) - 0.015*float64(bar-1)
		}
		var spreadAB, spreadCD float64
		switch {
		case bar == 1:
			spreadAB, spreadCD = 0, 0
		case bar <= 10:
			// Early bars (drop out of [11..34] window!): C-D has large spread so A-B wins bar 24 [1..24]!
			spreadAB = 0.002 * math.Sin(float64(bar)*1.4)
			spreadCD = 0.18 * math.Sin(float64(bar)*1.1)
		case bar <= 24:
			// Bars 11..24: both tight, A-B still wins [1..24] because C-D had huge SSD on bars 2..10
			spreadAB = 0.002 * math.Sin(float64(bar)*1.4)
			spreadCD = 0.002 * math.Sin(float64(bar)*1.2)
		case bar <= 32:
			// Bars 25..32: A-B widens moderately (stays inside Entry=1.5), C-D stays ultra-tight -> C-D wins [11..34]!
			spreadAB = 0.018 * math.Sin(float64(bar)*1.4)
			spreadCD = 0.002 * math.Sin(float64(bar)*1.2)
		case bar == 33:
			// Bar 33: A-B dislocates strongly -> opens position right before reselection bar 34!
			spreadAB = 0.08
			spreadCD = 0.002
		case bar >= 34 && bar <= 36:
			// Bars 34..36: reselection at bar 34 picks C-D! A-B stays dislocated (0.08); C-D dislocates at bar 35!
			spreadAB = 0.08
			if bar == 35 {
				spreadCD = 0.045
			} else {
				spreadCD = 0.002
			}
		case bar == 37:
			// Bar 37: A-B reverts to the 10-bar window equilibrium (~0.035 after four 0.08 bars) -> graceful retiring position exits naturally!
			spreadAB = 0.035
			spreadCD = 0.001
		case bar == 38:
			// Bar 38: A-B spikes AGAIN -> graceful must NOT re-open retired A-B!
			spreadAB = 0.11
			spreadCD = 0.001
		case bar >= 39:
			// Bar 39+: C-D spikes at zero-leverage point -> graceful now has freed slot and opens C-D!
			spreadAB = 0.11
			if bar == 39 {
				spreadCD = 0.08
			} else {
				spreadCD = 0.0
			}
		}

		lp := map[int32]float64{
			1: 4.0 + baseAB + spreadAB,
			2: 4.0 + baseAB,
			3: 3.0 + baseCD + spreadCD,
			4: 3.0 + baseCD,
		}
		wG, _ := feedPairsBuilder(t, bGrace, uint64(bar), 1000, lp, []int32{1, 2, 3, 4})
		wI, _ := feedPairsBuilder(t, bImm, uint64(bar), 1000, lp, []int32{1, 2, 3, 4})

		for name, w := range map[string]map[int32]float64{"graceful": wG, "immediate": wI} {
			gross := 0.0
			for _, v := range w {
				gross += math.Abs(v)
			}
			if gross > 1+1e-9 {
				t.Fatalf("%s bar %d exceeded gross 1.0: %v (%v)", name, bar, gross, w)
			}
		}

		if bar == 34 {
			if wG[1] == 0 && wG[2] == 0 {
				t.Fatalf("graceful handoff must keep open A-B position at bar 34, got %v", wG)
			}
			if wI[1] != 0 || wI[2] != 0 {
				t.Fatalf("immediate handoff must flatten unselected A-B at bar 34, got %v", wI)
			}
		}
		if bar == 35 {
			if wI[3] == 0 {
				t.Fatalf("immediate handoff should open new pair C-D at bar 35, got %v", wI)
			}
			if wG[3] != 0 || wG[1] == 0 {
				t.Fatalf("graceful handoff must hold retiring A-B and defer C-D while slot is full at bar 35: %v", wG)
			}
		}
		if bar == 38 && (wG[1] != 0 || wG[2] != 0) {
			t.Fatalf("graceful retired pair A-B must never re-open after exiting: %v", wG)
		}
		if bar == 39 && wG[3] == 0 {
			t.Fatalf("graceful handoff must open C-D once retiring slot freed at bar 39, got %v", wG)
		}
	}

	repImm := trImm.report(1000)
	var immAB PairResult
	for _, r := range repImm {
		if isPairAB(r, "A", "B") {
			immAB = r
		}
	}
	if len(immAB.Episodes) == 0 || immAB.Episodes[0].Reason != "reselect" {
		t.Fatalf("expected immediate handoff episode reason 'reselect', got %+v", immAB.Episodes)
	}

	repGrace := trGrace.report(1000)
	var graceAB PairResult
	for _, r := range repGrace {
		if isPairAB(r, "A", "B") {
			graceAB = r
		}
	}
	if len(graceAB.Episodes) != 1 || graceAB.Episodes[0].Reason != "exit" {
		t.Fatalf("expected graceful handoff single episode closed by 'exit', got %+v", graceAB.Episodes)
	}
}

func TestRollingPairsZeroSelectionOutputsFlatWeights(t *testing.T) {
	u := factor.Universe{Version: "u", Investable: []int32{1, 2, 3}, Tradable: []int32{1, 2, 3}}
	sids := map[int32]string{1: "A", 2: "B", 3: "C"}
	b, tr, err := newTrackedRollingPairsBuilder(RollingPairsSpec{
		FormationBars:   20,
		ReselectBars:    10,
		PairCount:       2,
		SelectionMethod: "cointegration",
		MinCorr:         0.85,
		MaxADF:          -3.5,
		Band:            statistics.BandConfig{Entry: 1.5, Exit: 0.3},
	}, u, sids, nil)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(99))
	l1, l2, l3 := 4.0, 4.0, 4.0
	for bar := 1; bar <= 35; bar++ {
		l1 += rng.NormFloat64() * 0.02
		l2 -= rng.NormFloat64() * 0.02
		l3 += rng.NormFloat64() * 0.02
		w, diags := feedPairsBuilder(t, b, uint64(bar), 1000, map[int32]float64{1: l1, 2: l2, 3: l3}, []int32{1, 2, 3})
		if bar < 20 {
			if w != nil {
				t.Fatalf("bar %d expected warmup nil target, got %v", bar, w)
			}
			continue
		}
		if w == nil || len(w) != 0 {
			t.Fatalf("bar %d expected explicit empty map[int32]float64{}, got %v", bar, w)
		}
		if bar == 20 || bar == 30 {
			foundEmptyDiag := false
			for _, d := range diags {
				if d.Code == "rolling-pairs-empty-selection" {
					foundEmptyDiag = true
				}
			}
			if !foundEmptyDiag {
				t.Fatalf("bar %d expected rolling-pairs-empty-selection diagnostic, got %v", bar, diags)
			}
		}
	}
	if rep := tr.report(1000); len(rep) != 0 {
		t.Fatalf("expected empty PairResult report when zero pairs ever selected, got %+v", rep)
	}
}

func TestRollingPairsPITMissingLegClosesPairAndContinues(t *testing.T) {
	// 4 symbols {A, B, C, D}: A-B and C-D are both strongly cointegrated on independent trends.
	// At bar 75, symbol B delists (stops emitting klines/ticks)!
	const totalBars = 130
	rng := rand.New(rand.NewSource(777))
	prices := make(map[int32][]float64, 4)
	for sid := int32(1); sid <= 4; sid++ {
		prices[sid] = make([]float64, totalBars+1)
	}
	lB, lD := math.Log(100), math.Log(60)
	uAB, uCD := 0.0, 0.0
	for bar := 1; bar <= totalBars; bar++ {
		lB += rng.NormFloat64() * 0.032
		lD += rng.NormFloat64() * 0.032
		uAB = 0.50*uAB + rng.NormFloat64()*0.011
		uCD = 0.50*uCD + rng.NormFloat64()*0.011
		if bar == 74 {
			uAB = 0.06 // ensure A-B is actively open right before B delists at bar 75!
		}
		prices[1][bar] = math.Exp(lB + uAB)
		prices[2][bar] = math.Exp(lB)
		prices[3][bar] = math.Exp(lD + uCD)
		prices[4][bar] = math.Exp(lD)
	}

	const stratName = "test-rolling-pit-delist"
	err := RegisterRollingPairs(stratName, func(p *Params) (RollingPairsSpec, error) {
		return RollingPairsSpec{
			FormationBars:   50,
			ReselectBars:    25,
			PairCount:       2,
			MaxPerSymbol:    1,
			SelectionMethod: "cointegration",
			Window:          25,
			Band:            statistics.BandConfig{Entry: 1.2, Exit: 0.2, Stop: 4.5},
		}, p.Err()
	})
	if err != nil {
		t.Fatal(err)
	}

	sidMap := map[int32]string{1: "A", 2: "B", 3: "C", 4: "D"}
	cfg := makeMultiSymbolReplayConfig(t, stratName, sidMap, totalBars, func(bar int64, sid int32) (float64, bool) {
		if sid == 2 && bar >= 75 {
			return 0, false // symbol B delists at bar 75!
		}
		return prices[sid][bar], true
	})
	out := &captureTargetsOutput{}
	res, err := Run(context.Background(), cfg, nil, out)
	if err != nil {
		t.Fatal(err)
	}
	if res.Incomplete != 0 {
		t.Fatalf("expected 0 Incomplete periods under PIT universe reduction, got %d", res.Incomplete)
	}
	if res.UniverseDrops == 0 || res.DelistExits == 0 {
		t.Fatalf("expected PIT UniverseDrops > 0 and DelistExits > 0, got drops=%d exits=%d", res.UniverseDrops, res.DelistExits)
	}
	const hour int64 = 3600000
	var abRes, cdRes *PairResult
	for i := range res.Pairs {
		p := &res.Pairs[i]
		if isPairAB(*p, "A", "B") {
			abRes = p
		}
		if isPairAB(*p, "C", "D") {
			cdRes = p
		}
	}
	if abRes == nil || cdRes == nil {
		t.Fatalf("expected both A-B and C-D in Result.Pairs, got %+v", res.Pairs)
	}
	sawLegMissing := false
	for _, ep := range abRes.Episodes {
		if ep.Reason == "leg_missing" {
			sawLegMissing = true
		}
		if ep.StartMS >= 75*hour {
			t.Fatalf("delisted B must never be selected or opened at/after bar 75: %+v", ep)
		}
	}
	if !sawLegMissing {
		t.Fatalf("expected A-B open episode to close with reason 'leg_missing' when B delisted, got %+v", abRes.Episodes)
	}
	cdTradedAfterDelist := false
	for _, ep := range cdRes.Episodes {
		if ep.StartMS >= 75*hour || ep.EndMS > 75*hour {
			cdTradedAfterDelist = true
		}
	}
	if !cdTradedAfterDelist {
		t.Fatalf("expected C-D to keep trading normally after B delisted: %+v", cdRes.Episodes)
	}

	// Also verify UnavailableSymbols behavior on Candidates subset.
	bUnavail, trUnavail, err := newTrackedRollingPairsBuilder(RollingPairsSpec{
		Candidates:    []string{"A", "GONE_COIN"},
		FormationBars: 20,
		ReselectBars:  10,
		Band:          statistics.BandConfig{Entry: 1.5, Exit: 0.3},
	}, factor.Universe{Version: "u", Investable: []int32{1}, Tradable: []int32{1}}, map[int32]string{1: "A"}, map[string]string{"GONE_COIN": "no_stored_data"})
	if err != nil {
		t.Fatalf("UnavailableSymbols candidate must not fail builder creation: %v", err)
	}
	wFlat, diagsFlat := feedPairsBuilder(t, bUnavail, 1, 1000, map[int32]float64{1: 4.0}, []int32{1})
	if wFlat == nil || len(wFlat) != 0 || len(diagsFlat) == 0 || !strings.Contains(diagsFlat[0].Code, "pairs-all-unavailable") {
		t.Fatalf("expected flat target with pairs-all-unavailable, got w=%v diags=%v", wFlat, diagsFlat)
	}
	if repU := trUnavail.report(1000); len(repU) != 1 || repU[0].Unavailable != "leg_unavailable" {
		t.Fatalf("expected unavailable PairResult entry, got %+v", repU)
	}
}

func TestRollingPairsNoLookaheadAndDeterminism(t *testing.T) {
	// 1. Strict PIT No-Lookahead: two runs identical up to bar 80 and radically different on bars 81..120
	//    must emit bit-for-bit identical target weights on every bar 1..80!
	const totalBars = 120
	const splitBar = 80
	genPrices := func(alterFuture bool) map[int32][]float64 {
		rng := rand.New(rand.NewSource(314159))
		p := make(map[int32][]float64, 4)
		for sid := int32(1); sid <= 4; sid++ {
			p[sid] = make([]float64, totalBars+1)
		}
		x1, x2, u1, u2 := 4.5, 3.8, 0.0, 0.0
		for bar := 1; bar <= totalBars; bar++ {
			dx1 := rng.NormFloat64() * 0.032
			dx2 := rng.NormFloat64() * 0.032
			du1 := rng.NormFloat64() * 0.011
			du2 := rng.NormFloat64() * 0.011
			if alterFuture && bar > splitBar {
				dx1 = 0.08
				dx2 = -0.09
				du1 = 0.25 * float64(bar-splitBar)
				du2 = -0.30 * float64(bar-splitBar)
			}
			x1 += dx1
			x2 += dx2
			u1 = 0.50*u1 + du1
			u2 = 0.50*u2 + du2
			p[1][bar] = math.Exp(x1 + u1)
			p[2][bar] = math.Exp(x1)
			p[3][bar] = math.Exp(x2 + u2)
			p[4][bar] = math.Exp(x2)
		}
		return p
	}

	const stratName = "test-rolling-pit-determinism"
	err := RegisterRollingPairs(stratName, func(p *Params) (RollingPairsSpec, error) {
		return RollingPairsSpec{
			FormationBars:   40,
			ReselectBars:    20,
			PairCount:       2,
			MaxPerSymbol:    1,
			SelectionMethod: "cointegration",
			Window:          20,
			WeightMode:      "inverse_vol",
			DriftBand:       0.01,
			Band:            statistics.BandConfig{Entry: 1.2, Exit: 0.25, Stop: 4.0},
		}, p.Err()
	})
	if err != nil {
		t.Fatal(err)
	}

	sidMap := map[int32]string{1: "SOL", 2: "ETH", 3: "AVAX", 4: "LINK"}
	pBase := genPrices(false)
	pAltered := genPrices(true)

	out1 := &captureTargetsOutput{}
	res1, err := Run(context.Background(), makeMultiSymbolReplayConfig(t, stratName, sidMap, totalBars, func(bar int64, sid int32) (float64, bool) {
		return pBase[sid][bar], true
	}), nil, out1)
	if err != nil {
		t.Fatal(err)
	}

	outAltered := &captureTargetsOutput{}
	_, err = Run(context.Background(), makeMultiSymbolReplayConfig(t, stratName, sidMap, totalBars, func(bar int64, sid int32) (float64, bool) {
		return pAltered[sid][bar], true
	}), nil, outAltered)
	if err != nil {
		t.Fatal(err)
	}

	if len(out1.targets) < splitBar || len(outAltered.targets) < splitBar {
		t.Fatalf("unexpected decision count: %d vs %d", len(out1.targets), len(outAltered.targets))
	}
	nonEmptyBeforeSplit := 0
	for i := 0; i < splitBar; i++ {
		w1, w2 := out1.targets[i], outAltered.targets[i]
		if len(w1) > 0 {
			nonEmptyBeforeSplit++
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
	if nonEmptyBeforeSplit == 0 {
		t.Fatal("expected active trades before splitBar so no-lookahead check is non-trivial")
	}

	// 2. Repeated run determinism: bit-identical JSON Result and target weights across runs.
	out2 := &captureTargetsOutput{}
	res2, err := Run(context.Background(), makeMultiSymbolReplayConfig(t, stratName, sidMap, totalBars, func(bar int64, sid int32) (float64, bool) {
		return pBase[sid][bar], true
	}), nil, out2)
	if err != nil {
		t.Fatal(err)
	}
	b1, err := json.Marshal(res1)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := json.Marshal(res2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatalf("non-deterministic Result across identical runs:\n%s\nvs\n%s", string(b1), string(b2))
	}
	for i := range out1.targets {
		for sid, v := range out1.targets[i] {
			if math.Float64bits(v) != math.Float64bits(out2.targets[i][sid]) {
				t.Fatalf("non-deterministic weight at bar %d sid %d", i+1, sid)
			}
		}
	}
}
