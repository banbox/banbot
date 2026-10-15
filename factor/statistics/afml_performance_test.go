package statistics

import (
	"math"
	"testing"
)

func TestAFMLProbabilisticSharpeProperties(t *testing.T) {
	p, err := ProbabilisticSharpe(.1, .1, 100, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, p, .5)
	base, _ := ProbabilisticSharpe(.1, 0, 100, 0, 3)
	longer, _ := ProbabilisticSharpe(.1, 0, 200, 0, 3)
	skewed, _ := ProbabilisticSharpe(.1, 0, 100, 1, 3)
	fat, _ := ProbabilisticSharpe(.1, 0, 100, 0, 10)
	if !(longer > base && skewed > base && fat < base && base > .5 && base < 1) {
		t.Fatalf("PSR direction wrong %g %g %g %g", base, longer, skewed, fat)
	}
	for _, observed := range []float64{math.NaN(), math.Inf(1)} {
		if _, err = ProbabilisticSharpe(observed, 0, 100, 0, 3); err == nil {
			t.Fatal("invalid SR accepted")
		}
	}
	if _, err = ProbabilisticSharpe(1, 0, 100, 5, 3); err == nil {
		t.Fatal("negative estimation variance accepted")
	}
}

func TestAFMLDeflatedSharpeMultiplicity(t *testing.T) {
	single, err := DeflatedSharpe(.3, .01, 260, 1, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	many, err := DeflatedSharpe(.3, .01, 260, 100, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	more, _ := DeflatedSharpe(.3, .01, 260, 1000, 0, 3)
	if !(more < many && many < single) {
		t.Fatalf("multiple testing did not reduce significance %g %g %g", single, many, more)
	}
	// Standard-normal expected max for N=100 from the book approximation.
	benchmark, _ := ExpectedMaximumSharpe(1, 100)
	if math.Abs(benchmark-2.530602893) > 1e-8 {
		t.Fatalf("expected maximum fixture %g", benchmark)
	}
	if _, err = ExpectedMaximumSharpe(-1, 100); err == nil {
		t.Fatal("negative trial variance accepted")
	}
}
