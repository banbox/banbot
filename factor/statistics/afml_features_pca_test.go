package statistics

import (
	"math"
	"testing"
)

func TestAFMLOrthogonalFeatureThresholdAndCausality(t *testing.T) {
	train := [][]float64{{-3, -6}, {-1, -2}, {1, 2}, {3, 6}}
	model, err := FitOrthogonalFeatures(train, .95)
	if err != nil || len(model.Loadings) != 1 {
		t.Fatal(model, err)
	}
	scores, err := model.Transform([]float64{2, 4})
	if err != nil {
		t.Fatal(err)
	}
	// Fitted standardization and projection are frozen despite extreme holdout.
	train[0][0] = 1e8
	_, _ = model.Transform([]float64{1e9, -1e9})
	after, _ := model.Transform([]float64{2, 4})
	if scores[0] != after[0] {
		t.Fatal("future/training mutation changed fit")
	}
	orthogonal, err := FitOrthogonalFeatures([][]float64{{-1, -1}, {-1, 1}, {1, -1}, {1, 1}}, .95)
	if err != nil || len(orthogonal.Loadings) != 2 {
		t.Fatal(orthogonal, err)
	}
	cross := 0.
	for _, row := range [][]float64{{-1, -1}, {-1, 1}, {1, -1}, {1, 1}} {
		s, _ := orthogonal.Transform(row)
		cross += s[0] * s[1]
	}
	if math.Abs(cross) > 1e-12 {
		t.Fatal(cross)
	}
}

func TestAFMLHyperbolicWeightedKendall(t *testing.T) {
	ranks := []int{0, 1, 2}
	tau, err := HyperbolicWeightedKendall([]float64{3, 2, 1}, []float64{30, 20, 10}, ranks, 3)
	if err != nil || tau != 1 {
		t.Fatal(tau, err)
	}
	tau, err = HyperbolicWeightedKendall([]float64{3, 2, 1}, []float64{10, 20, 30}, ranks, 3)
	if err != nil || tau != -1 {
		t.Fatal(tau, err)
	}
	tau, err = HyperbolicWeightedKendall([]float64{3, 3, 1}, []float64{30, 20, 10}, ranks, 3)
	if err != nil || tau <= 0 || tau >= 1 {
		t.Fatal(tau, err)
	}
	if _, err = HyperbolicWeightedKendall([]float64{3, 2, 1}, []float64{30, 20, 10}, ranks, 2); err == nil {
		t.Fatal("ignored budget")
	}
}
