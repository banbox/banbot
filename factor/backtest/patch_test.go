package backtest

import (
	"github.com/banbox/banbot/factor"
	"testing"
)

func TestPatchKeepsDriftedQuantityAndFullHistory(t *testing.T) {
	b, _ := NewBook(1000)
	first := portfolio(t, 1, map[int32]float64{1: .5})
	if err := b.Execute(first, map[int32]Quote{1: {AtMS: 11, AvailableAt: 11, Price: 100}}, 11, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := b.Mark(1, Quote{AtMS: 20, AvailableAt: 20, Price: 200}, 20); err != nil {
		t.Fatal(err)
	}
	sp := first.Spec()
	sp.Mode = factor.Patch
	sp.PlanSequence = 2
	sp.Budget.NAV = b.State().NAV
	patch, err := factor.NewTargetPortfolio(sp, map[int32]float64{2: .2})
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Execute(patch, map[int32]Quote{2: {AtMS: 20, AvailableAt: 20, Price: 100}}, 20, 0, 0); err != nil {
		t.Fatal(err)
	}
	state := b.State()
	if state.Quantities[1] != 5 || state.Quantities[2] != 3 || state.NAV != 1500 || state.Turnover != 800 {
		t.Fatalf("Patch resized omitted quantity: %+v", state)
	}
	full := portfolio(t, 3, map[int32]float64{2: .2})
	if err = b.Execute(full, map[int32]Quote{1: {AtMS: 21, AvailableAt: 21, Price: 200}, 2: {AtMS: 21, AvailableAt: 21, Price: 100}}, 21, 0, 0); err != nil {
		t.Fatal(err)
	}
	if b.State().Quantities[1] != 0 {
		t.Fatal("Full lost prior Patch scope")
	}
}
