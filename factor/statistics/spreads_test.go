package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestGaussianOptimalThresholdFirstOrderAndScale(t *testing.T) {
	u, err := GaussianThreshold(1)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(u-.751791524693564) >= 1e-6 {
		t.Fatalf("threshold %g", u)
	}
	objective := func(x float64) float64 { return x * .5 * math.Erfc(x/math.Sqrt2) }
	if objective(u) <= objective(u-.05) || objective(u) <= objective(u+.05) {
		t.Fatal("not a local maximum")
	}
	v, _ := GaussianThreshold(3)
	closeTo(t, v, 3*u)
	if _, err = GaussianThreshold(0); err == nil {
		t.Fatal("zero scale accepted")
	}
}
func TestThresholdRegularizationSolvesSpecifiedObjective(t *testing.T) {
	counts := []float64{3, 1, 0}
	z, err := SmoothThresholdCounts(counts, 2)
	if err != nil {
		t.Fatal(err)
	}
	for i := range z {
		gradient := z[i] - counts[i]
		if i > 0 {
			gradient += 2 * (z[i] - z[i-1])
		}
		if i+1 < len(z) {
			gradient += 2 * (z[i] - z[i+1])
		}
		closeTo(t, gradient, 0)
	}
	plain, _ := SmoothThresholdCounts(counts, 0)
	if !reflect.DeepEqual(plain, counts) {
		t.Fatal("zero lambda changed counts")
	}
	fit, err := EmpiricalThreshold([]float64{-3, -2, -1, 1, 2, 3}, []float64{0, 1, 2, 3}, 0)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, fit.Threshold, 1)
	closeTo(t, fit.Objective, 2)
	if _, err = EmpiricalThreshold([]float64{math.NaN()}, []float64{1}, 0); err == nil {
		t.Fatal("NaN accepted")
	}
}
func TestCrossingsExcludeCensoredAndZeroPlateaus(t *testing.T) {
	d, err := CrossingDurations([]float64{1, 2, -1, -2, 0, 0, 1, 2, -1, -2, 1})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(d, []int{4, 2, 2}) {
		t.Fatalf("durations %v", d)
	}
	q, err := DurationQuantile(d, .5)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, q, 2)
	flat, _ := CrossingDurations([]float64{0, 0, 0, 0})
	if len(flat) != 0 {
		t.Fatal("flat zeros became crossings")
	}
	retreat, _ := CrossingDurations([]float64{1, 0, 1, -1, 1})
	if !reflect.DeepEqual(retreat, []int{1}) {
		t.Fatalf("touch was counted: %v", retreat)
	}
	plateau, _ := CrossingDurations([]float64{1, 0, 0, -1, 0, 1})
	if !reflect.DeepEqual(plateau, []int{2}) {
		t.Fatalf("plateau crossing confirmation: %v", plateau)
	}
}

func TestThresholdRegularizationRejectsIntermediateOverflow(t *testing.T) {
	// Checking only final values would silently accept the zero curve after
	// an infinite interior diagonal, despite a positive input curve.
	if _, err := SmoothThresholdCounts([]float64{3, 2, 1}, math.MaxFloat64); err == nil {
		t.Fatal("overflowing regularization diagonal accepted")
	}
	if _, err := SmoothThresholdCounts([]float64{math.MaxFloat64, math.MaxFloat64}, 2); err == nil {
		t.Fatal("overflowing elimination RHS accepted")
	}
	if _, err := EmpiricalThreshold([]float64{math.MaxFloat64, math.MaxFloat64, math.MaxFloat64, math.MaxFloat64, math.MaxFloat64, math.MaxFloat64}, []float64{math.MaxFloat64 / 2}, 0); err == nil {
		t.Fatal("overflowing threshold objective accepted")
	}
	identity, err := SmoothThresholdCounts([]float64{3}, math.MaxFloat64)
	if err != nil || len(identity) != 1 || identity[0] != 3 {
		t.Fatalf("single point has no difference penalty: %v %v", identity, err)
	}
}
