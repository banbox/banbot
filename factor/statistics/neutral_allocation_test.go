package statistics

import (
	"math"
	"testing"
)

func TestNeutralAllocationAnalyticUtilityAndRedundantConstraints(t *testing.T) {
	alpha, variance := []float64{3, -2, 1}, []float64{1, 2, 3}
	exposures := [][]float64{{1, 1}, {1, 2}, {1, 3}}
	allocation, err := FactorNeutralWeights(alpha, variance, exposures, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []float64{1.0 / 3, -2.0 / 3, 1.0 / 3} {
		closeTo(t, allocation.Weights[i], want)
	}
	utility := func(tangent float64) float64 {
		value := 0.0
		for i, direction := range []float64{1, -2, 1} {
			w := allocation.Weights[i] + tangent*direction
			value += alpha[i]*w - variance[i]*w*w
		}
		return value
	}
	if utility(0) <= utility(.1) || utility(0) <= utility(-.1) {
		t.Fatal("not constrained utility maximum")
	}
	for i := range exposures {
		exposures[i] = append(exposures[i], 2*exposures[i][1])
	}
	duplicate, err := FactorNeutralWeights(alpha, variance, exposures, 2, 1)
	if err != nil || duplicate.ConstraintRank != 2 || !duplicate.Scaled {
		t.Fatalf("redundant constraints: %+v %v", duplicate, err)
	}
	for i, want := range []float64{.25, -.5, .25} {
		closeTo(t, duplicate.Weights[i], want)
	}
}

func TestNeutralAllocationDoesNotMagnifyExplainedAlphaAndRejectsInvalidRisk(t *testing.T) {
	flat, err := FactorNeutralWeights([]float64{1, 2, 3}, []float64{1, 2, 3}, [][]float64{{1, 1}, {1, 2}, {1, 3}}, 1, 1)
	if err != nil || !flat.NoResidualAlpha || flat.Gross != 0 {
		t.Fatalf("explained alpha was traded: %+v %v", flat, err)
	}
	for _, variance := range []float64{0, math.NaN(), math.SmallestNonzeroFloat64} {
		if _, err := FactorNeutralWeights([]float64{1, 2}, []float64{1, variance}, [][]float64{{1}, {1}}, 1, 1); err == nil {
			t.Fatalf("invalid uniqueness=%g", variance)
		}
	}
}
