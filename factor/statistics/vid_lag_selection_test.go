package statistics

import (
	"math"
	"testing"
)

func TestSelectSpreadLagSourceUnnormalizedCost(t *testing.T) {
	lag, cost, filter, err := SelectSpreadLag([]float64{0, 1, 2}, []float64{1, 1, 1}, []int{1}, 1, 1)
	// Frozen-vintage oracle: filtered 2/3 then 24/13, predictions0,4/3.
	want := 1./9 + 4./9 + 4./169 + 400./1521
	if err != nil || lag != 1 || math.Abs(cost-want) > 1e-12 || filter == nil {
		t.Fatal(lag, cost, want, err)
	}
	_, _, _, err = SelectSpreadLag([]float64{0, math.NaN()}, []float64{1, 1}, []int{1, 2}, 1, 1)
	if err == nil {
		t.Fatal("invalid training accepted")
	}
}
