package statistics

import (
	"math"
	"math/rand"
	"testing"
)

func TestADFFixtureAndDeterministicSpecification(t *testing.T) {
	// A known stationary AR process, independent seeded innovations.
	rng := rand.New(rand.NewSource(61))
	x := []float64{0}
	for range 500 {
		x = append(x, .6*x[len(x)-1]+rng.NormFloat64())
	}
	f, err := AugmentedDickeyFuller(x, ADFSpec{Lags: 2, Intercept: true})
	if err != nil {
		t.Fatal(err)
	}
	if f.DeltaCoefficient >= -.2 || f.DeltaCoefficient <= -.6 || f.TStatistic >= -5 || f.Samples != 498 || f.DegreesOfFreedom != 494 {
		t.Fatalf("stationary fixture: %+v", f)
	}
	trend := append([]float64(nil), x...)
	for i := range trend {
		trend[i] += float64(i) * .1
	}
	with, err := AugmentedDickeyFuller(trend, ADFSpec{Lags: 2, Intercept: true, Trend: true})
	if err != nil {
		t.Fatal(err)
	}
	base, err := AugmentedDickeyFuller(x, ADFSpec{Lags: 2, Intercept: true, Trend: true})
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, with.DeltaCoefficient, base.DeltaCoefficient)
	closeTo(t, with.TStatistic, base.TStatistic)
}

func TestEngleGrangerUsesEstimatedResidualDistribution(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	var x [][]float64
	var y []float64
	walk, noise := 0.0, 0.0
	for range 500 {
		walk += rng.NormFloat64()
		noise = .3*noise + rng.NormFloat64()
		x = append(x, []float64{walk})
		y = append(y, 2+1.7*walk+noise)
	}
	f, adf, err := EngleGranger(y, x, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(f.Beta[0]-1.7) > .1 || adf.TStatistic >= -5 {
		t.Fatalf("cointegrated fixture: beta=%g adf=%g", f.Beta[0], adf.TStatistic)
	}
	for _, bad := range [][]float64{{1, 1, 1, 1, 1}, {1, 2, math.NaN(), 3}} {
		if _, err := AugmentedDickeyFuller(bad, ADFSpec{}); err == nil {
			t.Fatal("undefined test accepted")
		}
	}
}
