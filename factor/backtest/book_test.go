package backtest

import (
	"github.com/banbox/banbot/factor"
	"math"
	"testing"
)

func portfolio(t *testing.T, seq uint64, w map[int32]float64) *factor.TargetPortfolio {
	t.Helper()
	p, e := factor.NewTargetPortfolio(factor.PortfolioSpec{StrategyID: "s", AccountID: "a", DecisionTime: 10, ExecutableAt: 11, ExpireAt: 100, PlanSequence: seq, SnapshotID: "snap", PlanHash: "p", FactorPlanHash: "dag", UniverseVersion: "u", Budget: factor.FrozenBudget{Version: "v", Currency: "USD", NAV: 1000}, Mode: factor.Full}, w)
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func TestQuantityDriftAndExplicitCosts(t *testing.T) {
	b, _ := NewBook(1000)
	p := portfolio(t, 1, map[int32]float64{1: .5, 2: -.5})
	q := map[int32]Quote{1: {AtMS: 11, AvailableAt: 11, Price: 100}, 2: {AtMS: 11, AvailableAt: 11, Price: 100}}
	if err := b.Execute(p, q, 11, .001, .01); err != nil {
		t.Fatal(err)
	}
	s := b.State()
	if math.Abs(s.NAV-989) > 1e-9 || s.Quantities[1] != 5 || s.Quantities[2] != -5 {
		t.Fatalf("initial: %+v", s)
	}
	if err := b.Mark(1, Quote{AtMS: 20, AvailableAt: 20, Price: 120}, 20); err != nil {
		t.Fatal(err)
	}
	s = b.State()
	if math.Abs(s.NAV-1089) > 1e-9 || s.Quantities[1] != 5 {
		t.Fatalf("free rebalance/drift: %+v", s)
	}
	if err := b.ApplyFunding(Funding{ID: "f", SID: 1, AtMS: 20, AvailableAt: 20, Rate: .01}, 20); err != nil {
		t.Fatal(err)
	}
	if b.State().NAV != 1083 {
		t.Fatal(b.State())
	}
	if err := b.ApplyFunding(Funding{ID: "late", SID: 1, AtMS: 15, AvailableAt: 21, Rate: .02}, 21); err == nil {
		t.Fatal("late funding applied to current quantity")
	}
}
func TestAtomicStrictlyLaterPricesExpiryAndFullZero(t *testing.T) {
	b, _ := NewBook(1000)
	p := portfolio(t, 1, map[int32]float64{1: 1})
	for _, q := range []Quote{{AtMS: 10, AvailableAt: 11, Price: 100}, {AtMS: 11, AvailableAt: 12, Price: 100}, {AtMS: 11, AvailableAt: 10, Price: 100}} {
		if b.Execute(p, map[int32]Quote{1: q}, 11, 0, 0) == nil {
			t.Fatal("invalid observable event accepted")
		}
		if b.State().NAV != 1000 || len(b.State().Quantities) != 0 {
			t.Fatal("partial mutation")
		}
	}
	if b.Execute(p, map[int32]Quote{1: {AtMS: 100, AvailableAt: 100, Price: 100}}, 100, 0, 0) == nil {
		t.Fatal("exclusive expiry accepted")
	}
	if err := b.Execute(p, map[int32]Quote{1: {AtMS: 11, AvailableAt: 11, Price: 100}}, 11, 0, 0); err != nil {
		t.Fatal(err)
	}
	next := portfolio(t, 2, map[int32]float64{2: 1})
	if err := b.Execute(next, map[int32]Quote{1: {AtMS: 12, AvailableAt: 12, Price: 110}, 2: {AtMS: 12, AvailableAt: 12, Price: 100}}, 12, 0, 0); err != nil {
		t.Fatal(err)
	}
	if b.State().Quantities[1] != 0 || b.State().Quantities[2] != 10 || b.State().NAV != 1100 {
		t.Fatal(b.State())
	}
}
