package runner

import (
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
)

// 067: a pair with a leg that loses its data is closed and paused (weights empty), and reopens when the leg is
// back and the band says so.
func TestPairsBuilderClosesAndPausesPairOnMissingLeg(t *testing.T) {
	builder, err := newPairsBuilder(PairsSpec{Pairs: []Pair{{Y: "Y", X: "X"}}, Window: 5, Gross: 1, Band: statistics.BandConfig{Entry: 1, Exit: .2}}, map[int32]string{1: "Y", 2: "X"})
	if err != nil {
		t.Fatal(err)
	}
	seq := uint64(0)
	step := func(y, x float64, missingX bool) (map[int32]float64, []factor.Diagnostic) {
		seq++
		vals := map[int32]factor.Numeric{1: {Value: y, Validity: factor.Valid}}
		if !missingX {
			vals[2] = factor.Numeric{Value: x, Validity: factor.Valid}
		}
		frame := factor.Frame{Values: map[string]map[int32]factor.Numeric{pairsLogOutput: vals}}
		spec := factor.PortfolioSpec{StrategyID: "s", AccountID: "a", SnapshotID: "x", PlanHash: "p", FactorPlanHash: "f", UniverseVersion: "u",
			Budget: factor.FrozenBudget{Version: "b", Currency: "USD", NAV: 1000}, PlanSequence: seq, DecisionTime: 10, ExecutableAt: 11, ExpireAt: 20, Mode: factor.Full}
		p, diags, err := builder(frame, factor.Universe{Version: "u", Tradable: []int32{1, 2}}, spec, research.PortfolioDefinition{})
		if err != nil {
			t.Fatal(err)
		}
		if p == nil {
			return nil, diags
		}
		return p.Targets(), p.Spec().Diagnostics
	}
	xs := []float64{1.00, 1.03, 0.98, 1.05, 1.01}
	for _, x := range xs {
		step(x+0.001*x, x, false)
	}
	w, _ := step(1.01+0.4, 1.01, false) // y spikes above the hedge line: short the spread
	if len(w) != 2 || w[1] >= 0 {
		t.Fatalf("pair did not open: %v", w)
	}
	w, diags := step(1.4, 1.01, true) // leg X vanishes: close + pause
	if len(w) != 0 {
		t.Fatalf("pair must be flat while a leg is missing: %v (%v)", w, diags)
	}
	reopened := false
	for i, x := range []float64{1.02, 0.99, 1.04, 1.0} { // data back; a fresh excursion reopens
		w, _ = step(x+0.001*x, x, false)
		if i == 3 {
			w, _ = step(x+0.4, x, false)
			reopened = len(w) == 2
		}
	}
	if !reopened {
		t.Fatalf("pair did not reopen after data came back: %v", w)
	}
}
