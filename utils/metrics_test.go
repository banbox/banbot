package utils

import "testing"

func TestCalcExpectancyReturnsExpectancyRatio(t *testing.T) {
	expectancy, ratio := CalcExpectancy([]float64{2, -1})
	if expectancy != 0.5 {
		t.Fatalf("expectancy = %v, want 0.5", expectancy)
	}
	if ratio != 0.5 {
		t.Fatalf("expectancy ratio = %v, want 0.5", ratio)
	}
}
