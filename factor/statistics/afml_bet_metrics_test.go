package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestSourceBetTimingAndQuantityWeightedHolding(t *testing.T) {
	times := []int64{0, 10, 20, 30, 40, 50, 60, 70}
	positions := []float64{0, 2, 4, 3, -2, -1, 0, 0}
	boundaries, err := BetBoundaryTimes(times, positions)
	if err != nil || !reflect.DeepEqual(boundaries, []int64{0, 40, 60, 70}) {
		t.Fatalf("source flatten/flip boundaries: %v %v", boundaries, err)
	}
	r, err := PositionHoldingPeriod(times, positions)
	// Long avgentry15: reduce1 at30, close3 at40. Short entry40,
	// close1 at50 and1 at60 => (15+75+10+20)/6=20ms.
	if err != nil || !r.Valid || r.MeanMS != 20 || r.ClosedQuantity != 6 {
		t.Fatalf("source average-entry pairing: %+v %v", r, err)
	}
	r, err = PositionHoldingPeriod([]int64{0, 10}, []float64{0, 2})
	if err != nil || r.Valid || !math.IsNaN(r.MeanMS) {
		t.Fatal("open inventory counted as completed")
	}
	r, err = PositionHoldingPeriod([]int64{0, 10}, []float64{2, 0})
	if err != nil || r.MeanMS != 10 {
		t.Fatal("initial carried quantity entry convention changed")
	}
}

func TestSourceConcentrationAndDrawdownProxy(t *testing.T) {
	for _, values := range [][]float64{{1, 1, 1}, {-1, -1, -1}, {1e300, 1e300, 1e300}} {
		got, err := NormalizedHHI(values)
		if err != nil || math.Abs(got) > 1e-14 {
			t.Fatal("equal payouts not dispersed")
		}
	}
	got, err := NormalizedHHI([]float64{0, 0, 10})
	if err != nil || got != 1 {
		t.Fatal("single concentrated payout not maximum")
	}
	if _, err = NormalizedHHI([]float64{-1, 1, 2}); err == nil {
		t.Fatal("mixed payout classes silently canceled")
	}
	episodes, err := SourceDrawdownEpisodes([]int64{0, 10, 20, 30, 40, 50, 60}, []float64{100, 80, 100, 110, 120, 90, 120}, false)
	if err != nil || len(episodes) != 2 || math.Abs(episodes[0].Drawdown-.2) > 1e-14 || episodes[0].TimeToNextDrawdownMS != 40 || !episodes[0].TimeValid || episodes[1].TimeValid || episodes[1].Drawdown != .25 {
		t.Fatalf("source HWM-group proxy: %+v %v", episodes, err)
	}
	// Recovery of episode0 is at20, but the printed proxy reports40.
	if episodes[0].TimeToNextDrawdownMS == 20 {
		t.Fatal("source proxy replaced with conventional recovery time")
	}
}
