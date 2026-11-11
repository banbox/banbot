package runner

import (
	"math"
	"strings"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
)

func unavailableSpec(pairs ...Pair) PairsSpec {
	return PairsSpec{Pairs: pairs, Window: 5, Gross: 1 / float64(len(pairs)), Band: statistics.BandConfig{Entry: 1, Exit: .2}}
}

// a leg that is neither in the universe nor a known unavailable symbol is still an error
func TestPairsBuilderMissingLegStillErrors(t *testing.T) {
	sids := map[int32]string{1: "Y", 2: "X"}
	spec := unavailableSpec(Pair{Y: "Y", X: "GONE"})
	if _, _, err := newTrackedPairsBuilderSkipping(spec, sids, nil); err == nil || !strings.Contains(err.Error(), "missing from this run's universe") {
		t.Fatalf("no unavailable list: %v", err)
	}
	if _, _, err := newTrackedPairsBuilderSkipping(spec, sids, map[string]string{"OTHER": "no_stored_data"}); err == nil {
		t.Fatal("a leg missing for another reason must still fail the run")
	}
	if _, _, err := newTrackedPairsBuilder(spec, sids); err == nil {
		t.Fatal("the plain builder keeps the error")
	}
}

type skipHarness struct {
	t       *testing.T
	build   PortfolioBuilder
	tracker *pairsTracker
	seq     uint64
	at      int64
}

// step feeds prices of sids 1 (Y), 2 (X), 3 (Z); a non-positive price means the symbol has no data.
func (h *skipHarness) step(prices map[int32]float64) map[int32]float64 {
	h.t.Helper()
	h.seq++
	h.at += 3600000
	vals := map[int32]factor.Numeric{}
	for sid, p := range prices {
		if p > 0 {
			vals[sid] = factor.Numeric{Value: math.Log(p), Validity: factor.Valid}
		}
	}
	frame := factor.Frame{DecisionTime: h.at, Values: map[string]map[int32]factor.Numeric{pairsLogOutput: vals}}
	spec := factor.PortfolioSpec{StrategyID: "s", AccountID: "a", SnapshotID: "x", PlanHash: "p", FactorPlanHash: "f", UniverseVersion: "u",
		Budget: factor.FrozenBudget{Version: "b", Currency: "USD", NAV: 1000}, PlanSequence: h.seq, DecisionTime: h.at, ExecutableAt: h.at + 1, ExpireAt: h.at + 1000000, Mode: factor.Full}
	p, _, err := h.build(frame, factor.Universe{Version: "u", Tradable: []int32{1, 2, 3}}, spec, research.PortfolioDefinition{})
	if err != nil {
		h.t.Fatal(err)
	}
	if p == nil {
		return nil
	}
	return p.Targets()
}

// BtFeatureLevel 11: a pair whose leg has no data in the run window is skipped for the whole run, the other pair trades with
// its own Gross share (1/2 of the nominal two pairs: the exposure of the skipped pair stays idle, nothing is scaled up).
func TestPairsBuilderSkipsPairWithUnavailableLeg(t *testing.T) {
	spec := unavailableSpec(Pair{Y: "Y", X: "X"}, Pair{Y: "Z", X: "MATIC"})
	b, tr, err := newTrackedPairsBuilderSkipping(spec, map[int32]string{1: "Y", 2: "X", 3: "Z"}, map[string]string{"MATIC": "data_ends_before_window"})
	if err != nil {
		t.Fatal(err)
	}
	tr.resetStats()
	h := &skipHarness{t: t, build: b, tracker: tr}
	for _, x := range []float64{1.00, 1.03, 0.98, 1.05, 1.01} {
		h.step(map[int32]float64{1: x * 1.001, 2: x, 3: 2})
	}
	w := h.step(map[int32]float64{1: 1.41, 2: 1.01, 3: 2})
	if len(w) != 2 || w[1] >= 0 || w[3] != 0 {
		t.Fatalf("the available pair must open and the skipped one stay out: %v", w)
	}
	gross := 0.0
	for _, v := range w {
		gross += math.Abs(v)
	}
	if gross > 0.5+1e-9 {
		t.Fatalf("gross %v exceeds the one pair's share 0.5: exposure was scaled up", gross)
	}
	rep := tr.report(1000)
	if len(rep) != 2 || rep[0].Unavailable != "" || rep[1].Unavailable != "leg_unavailable" || rep[1].UnavailableLeg != "MATIC" || rep[1].Bars != 0 || rep[1].Trades != 0 {
		t.Fatalf("report: %+v", rep)
	}
	if rep[0].Bars == 0 {
		t.Fatal("the available pair must have bars")
	}
}

// every pair skipped: an explicit flat target at every decision
func TestPairsBuilderAllPairsUnavailableIsFlat(t *testing.T) {
	spec := unavailableSpec(Pair{Y: "Y", X: "MATIC"})
	b, tr, err := newTrackedPairsBuilderSkipping(spec, map[int32]string{1: "Y"}, map[string]string{"MATIC": "data_ends_before_window"})
	if err != nil {
		t.Fatal(err)
	}
	h := &skipHarness{t: t, build: b, tracker: tr}
	for i := 0; i < 10; i++ {
		w := h.step(map[int32]float64{1: 1 + float64(i)/10})
		if w == nil || len(w) != 0 {
			t.Fatalf("decision %d: want an empty target, got %v", i, w)
		}
	}
	rep := tr.report(1000)
	if len(rep) != 1 || rep[0].Unavailable != "leg_unavailable" || rep[0].Turnover != 0 || rep[0].PnL != 0 {
		t.Fatalf("%+v", rep)
	}
}

// without any unavailable symbol the builder is exactly the old one (same weights, report without the new keys)
func TestPairsBuilderSkippingNilEqualsPlain(t *testing.T) {
	spec := unavailableSpec(Pair{Y: "Y", X: "X"})
	sids := map[int32]string{1: "Y", 2: "X"}
	b1, t1, _ := newTrackedPairsBuilder(spec, sids)
	b2, t2, _ := newTrackedPairsBuilderSkipping(spec, sids, map[string]string{"UNUSED": "no_stored_data"})
	h1 := &skipHarness{t: t, build: b1, tracker: t1}
	h2 := &skipHarness{t: t, build: b2, tracker: t2}
	for _, x := range []float64{1.00, 1.03, 0.98, 1.05, 1.01, 1.02, 0.97} {
		a, b := h1.step(map[int32]float64{1: x * 1.2, 2: x}), h2.step(map[int32]float64{1: x * 1.2, 2: x})
		if len(a) != len(b) {
			t.Fatalf("%v vs %v", a, b)
		}
		for k, v := range a {
			if b[k] != v {
				t.Fatalf("%v vs %v", a, b)
			}
		}
	}
	r1, r2 := t1.report(1000), t2.report(1000)
	if r1[0].Unavailable != "" || r2[0].Unavailable != "" || r1[0].Bars != r2[0].Bars || r1[0].Turnover != r2[0].Turnover {
		t.Fatalf("%+v %+v", r1, r2)
	}
}
