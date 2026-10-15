package statistics

import (
	"math"
	"testing"
)

func TestSourceSpreadHigherLagVariance(t *testing.T) {
	filter, err := NewSourceLaggedSpreadFilter(3, 0, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	first, err := filter.Observe(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Physical208: (1+1/3)^2*1 + 1/9*1. Physical209 then
	// weights the observation by priorVariance/(priorVariance+R).
	if math.Abs(first.PredictionVariance-17.0/9) > 1e-12 || math.Abs(first.Filtered-17.0/26) > 1e-12 || math.Abs(first.Variance-17.0/26) > 1e-12 {
		t.Fatal(first)
	}
	second, err := filter.Observe(-1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(second.Prediction-34.0/39) > 1e-12 || math.Abs(second.PredictionVariance-149.0/117) > 1e-12 || math.Abs(second.Filtered+47.0/266) > 1e-12 || math.Abs(second.Variance-149.0/266) > 1e-12 {
		t.Fatal(second)
	}
	full, _ := NewLaggedSpreadFilter(3, 0, 1, 0)
	variant, err := full.Observe(1, 1)
	if err != nil || math.Abs(variant.PredictionVariance-1) > 1e-12 || variant.Filtered == first.Filtered {
		t.Fatal("full covariance variant lost", variant, err)
	}
}

func TestSourceSpreadLowerLagAndSelection(t *testing.T) {
	values := []float64{0, 1, -.4, 1.5, .1, -.2, 1.7}
	variances := []float64{1, .7, .3, .8, .6, .4, .9}
	for _, lag := range []int{1, 2} {
		a, _ := NewLaggedSpreadFilter(lag, .02, 1, 0)
		b, _ := NewSourceLaggedSpreadFilter(lag, .02, 1, 0)
		for i := 1; i < len(values); i++ {
			x, e1 := a.Observe(values[i], variances[i])
			y, e2 := b.Observe(values[i], variances[i])
			if e1 != nil || e2 != nil || x != y {
				t.Fatal("lag1/2 covariance recursion changed", lag, x, y, e1, e2)
			}
		}
	}
	lag, score, selected, err := SelectSourceSpreadLag(values, variances, []int{3}, 0, 1)
	if err != nil || lag != 3 || score <= 0 || !selected.sourceIndependent {
		t.Fatal(lag, score, selected, err)
	}
	if _, err := NewSourceLaggedSpreadFilter(0, 0, 1, 0); err == nil {
		t.Fatal("invalid source lag accepted")
	}
}
