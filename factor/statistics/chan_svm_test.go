package statistics

import (
	"math"
	"testing"
)

func TestChanSVMLinearAndNonlinear(t *testing.T) {
	for _, kernel := range []string{"linear", "rbf", "polynomial"} {
		x := [][]float64{{-2}, {-1}, {1}, {2}}
		y := []float64{0, 0, 1, 1}
		m, err := FitSVM(x, y, SVMConfig{Kernel: kernel, C: 100})
		if err != nil {
			t.Fatal(err)
		}
		for i, row := range x {
			v, err := m.Predict(row)
			if err != nil || v != y[i] {
				t.Fatalf("%s decision %v %v", kernel, v, err)
			}
		}
		if _, err := m.Predict([]float64{math.NaN()}); err == nil {
			t.Fatal("nonfinite prediction")
		}
	}
	x := [][]float64{{-1, -1}, {-1, 1}, {1, -1}, {1, 1}}
	y := []float64{0, 1, 1, 0}
	m, err := FitSVM(x, y, SVMConfig{Kernel: "rbf", C: 100})
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range x {
		v, _ := m.Predict(row)
		if v != y[i] {
			t.Fatal("RBF cannot learn XOR")
		}
	}
	if _, err := FitSVM(x, y, SVMConfig{Kernel: "rbf", C: 100, MaxSweeps: 1}); err == nil {
		t.Fatal("budget silently succeeded")
	}
}
