package statistics

import (
	"math"
	"testing"
)

func TestETFTrickRollCarryCostsAndAllocationTiming(t *testing.T) {
	bar := func(at int64, open, close []float64, allocation []float64, rebalance bool) ETFBar {
		return ETFBar{At: at, Open: open, Close: close, PointValue: []float64{1, 2}, Volume: []float64{100, 50}, Carry: []float64{0, 0}, Allocation: allocation, Rebalance: rebalance}
	}
	bars := []ETFBar{
		bar(1, []float64{100, 50}, []float64{100, 50}, []float64{1, -1}, true),
		bar(2, []float64{200, 80}, []float64{202, 80}, []float64{-1, 1}, true),
		bar(3, []float64{400, 160}, []float64{400, 160}, nil, false),
		bar(4, []float64{400, 160}, []float64{400, 160}, nil, false),
	}
	bars[3].Carry[1] = 1
	got, err := ETFTrick(bars, []float64{.001, .001})
	if err != nil {
		t.Fatal(err)
	}
	// The contract roll100->200 does not create profit: only202-200 does.
	if math.Abs(got[1].NAV-1.005) > 1e-14 || math.Abs(got[2].NAV-1.005) > 1e-14 {
		t.Fatalf("roll/allocation-generated fictitious profit: %v", got)
	}
	if math.Abs(got[1].Holdings[0]-.0025) > 1e-14 || math.Abs(got[1].Holdings[1]+.003125) > 1e-14 || got[2].Holdings[0] >= 0 {
		t.Fatal("close decision did not execute at next open")
	}
	if math.Abs(got[1].RebalanceCost-.001) > 1e-14 || math.Abs(got[2].RebalanceCost-.00201) > 1e-14 {
		t.Fatalf("full liquidation/rebuy source cost: %g %g", got[1].RebalanceCost, got[2].RebalanceCost)
	}
	if math.Abs(got[1].SpreadCost-.001005) > 1e-14 || math.Abs(got[1].BasketVolume-16000) > 1e-10 {
		t.Fatal("spread cost/least-liquid basket units mismatch")
	}
	if math.Abs(got[3].NAV-(1.005+got[2].Holdings[1]*2)) > 1e-14 {
		t.Fatal("FX/point-value carry was omitted")
	}
	// Changing the next close cannot alter the already priced opening holdings.
	bars[1].Close[0] = 250
	other, err := ETFTrick(bars, []float64{.001, .001})
	if err != nil || got[1].Holdings[0] != other[1].Holdings[0] {
		t.Fatal("opening holdings sized with future close")
	}
}

func TestETFTrickRejectsExhaustionAndIncompleteLegs(t *testing.T) {
	bars := []ETFBar{{At: 1, Open: []float64{1}, Close: []float64{1}, PointValue: []float64{1}, Volume: []float64{1}, Carry: []float64{0}, Allocation: []float64{-1}, Rebalance: true}, {At: 2, Open: []float64{1}, Close: []float64{3}, PointValue: []float64{1}, Volume: []float64{1}, Carry: []float64{0}}}
	if _, err := ETFTrick(bars, []float64{0}); err == nil {
		t.Fatal("negative NAV silently clamped")
	}
	bars[1].Close = nil
	if _, err := ETFTrick(bars, []float64{0}); err == nil {
		t.Fatal("missing leg compressed")
	}
}
