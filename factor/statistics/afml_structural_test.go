package statistics

import (
	"math/rand"
	"testing"
)

func TestAFMLBackwardADFPreservesLagSeedsAndPrefix(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	values := make([]float64, 60)
	for i := 1; i < len(values); i++ {
		values[i] = .6*values[i-1] + rng.NormFloat64()
	}
	got, err := BackwardSupADF(values, 20, 1, "c", 40)
	if err != nil {
		t.Fatal(err)
	}
	full, err := AugmentedDickeyFuller(values, ADFSpec{Lags: 1, Intercept: true})
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, got.Statistics[0], full.TStatistic)
	if len(got.Statistics) != 40 || got.Statistic < got.Statistics[0] {
		t.Fatalf("inner-loop supremum mismatch %+v", got)
	}
	series, err := BackwardSupADFSeries(values, 20, 1, "c", 1000)
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := BackwardSupADF(values[:35], 20, 1, "c", 15)
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, series[34], prefix.Statistic)
	if _, err = BackwardSupADFSeries(values, 20, 1, "c", 10); err == nil {
		t.Fatal("fit budget ignored")
	}
}

func TestAFMLBackwardADFExplosiveVsMeanReverting(t *testing.T) {
	rng := rand.New(rand.NewSource(13))
	stationary, explosive := make([]float64, 80), make([]float64, 80)
	for i := 1; i < len(stationary); i++ {
		noise := rng.NormFloat64()
		stationary[i] = .5*stationary[i-1] + noise
		explosive[i] = 1.08*explosive[i-1] + noise
	}
	s, err := BackwardSupADF(stationary, 30, 0, "c", 100)
	if err != nil {
		t.Fatal(err)
	}
	e, err := BackwardSupADF(explosive, 30, 0, "c", 100)
	if err != nil {
		t.Fatal(err)
	}
	if e.Statistic <= 3 || s.Statistic >= 0 {
		t.Fatalf("right-tail explosive distinction lost: %g %g", s.Statistic, e.Statistic)
	}
	for _, constant := range []string{"ct", "ctt", "nc"} {
		if _, err = BackwardSupADF(stationary, 30, 1, constant, 100); err != nil {
			t.Fatalf("deterministic specification %s: %v", constant, err)
		}
	}
}
