package statistics

import (
	"math"
	"testing"
)

func TestLongOnlyMinimumVarianceAnalyticKKT(t *testing.T) {
	for _, tc := range []struct {
		cov      [][]float64
		w        []float64
		variance float64
	}{{[][]float64{{1, 0}, {0, 4}}, []float64{.8, .2}, .8}, {[][]float64{{1, 2}, {2, 5}}, []float64{1, 0}, 1}, {[][]float64{{1, 0, 0}, {0, 2, 0}, {0, 0, 4}}, []float64{4. / 7, 2. / 7, 1. / 7}, 4. / 7}} {
		r, err := LongOnlyMinimumVariance(tc.cov, 100, 1e-10)
		if err != nil {
			t.Fatal(err)
		}
		if math.Abs(r.Variance-tc.variance) > 1e-12 || r.KKTResidual > 1e-10 {
			t.Fatalf("bad optimum: %+v", r)
		}
		for i, w := range r.Weights {
			if math.Abs(w-tc.w[i]) > 1e-12 {
				t.Fatalf("weights %v want %v", r.Weights, tc.w)
			}
			g := 0.
			for j := range r.Weights {
				g += tc.cov[i][j] * r.Weights[j]
			}
			if w > 0 && math.Abs(g-r.Variance) > 1e-12 || w == 0 && g < r.Variance {
				t.Fatal("independent KKT check failed")
			}
		}
	}
}
func TestLongOnlyMinimumVarianceBoundsAndScaling(t *testing.T) {
	for _, cov := range [][][]float64{nil, {{1, 1}, {1, 1}}, {{1, 2}, {2, 1}}, {{1, 0}, {1, 1}}, {{math.NaN()}}} {
		if _, err := LongOnlyMinimumVariance(cov, 100, 1e-10); err == nil {
			t.Fatalf("invalid accepted: %v", cov)
		}
	}
	if _, err := LongOnlyMinimumVariance([][]float64{{1, 2}, {2, 5}}, 1, 1e-10); err == nil {
		t.Fatal("budget ignored")
	}
	r, err := LongOnlyMinimumVariance([][]float64{{1e-12, 0}, {0, 4e-12}}, 100, 1e-10)
	if err != nil || math.Abs(r.Weights[0]-.8) > 1e-12 {
		t.Fatalf("scale changed weights: %v %v", r, err)
	}
}
