package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestPCAAnalyticEigenvaluesAndProjection(t *testing.T) {
	rows := [][]float64{{2, 0}, {-2, 0}, {0, 1}, {0, -1}}
	m, err := FitPCA(rows, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, m.Eigenvalues[0], 8.0/3)
	closeTo(t, m.Loadings[0][0], 1)
	closeTo(t, m.Loadings[0][1], 0)
	s, err := m.Transform([]float64{3, 4})
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, s[0], 3)
	r, err := m.Residual([]float64{3, 4})
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, r[0], 0)
	closeTo(t, r[1], 4)
	all, err := FitPCA(rows, 2, false)
	if err != nil {
		t.Fatal(err)
	}
	r, err = all.Residual([]float64{3, 4})
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, r[0], 0)
	closeTo(t, r[1], 0)
	if !reflect.DeepEqual(rows, [][]float64{{2, 0}, {-2, 0}, {0, 1}, {0, -1}}) {
		t.Fatal("training input mutated")
	}
}
func TestPCASingularCovarianceAndNonfiniteGuard(t *testing.T) {
	m, err := FitPCA([][]float64{{1, 2}, {2, 4}, {3, 6}}, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, m.Eigenvalues[0], 2)
	r, err := m.Residual([]float64{4, 8})
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, r[0], 0)
	closeTo(t, r[1], 0)
	if _, err = FitPCA([][]float64{{1, 2}, {1, 3}}, 1, true); err == nil {
		t.Fatal("constant standardized feature accepted")
	}
	if _, err = m.Transform([]float64{math.NaN(), 2}); err == nil {
		t.Fatal("nonfinite inference accepted")
	}
}
