package statistics

import (
	"math"
	"math/rand"
	"testing"
)

func TestChanLocalLevelAnalyticAndFit(t *testing.T) {
	k := LocalLevel{Mean: 10, Variance: 2, ProcessVariance: 1, ObservationVariance: 3}
	o, err := k.Observe(14)
	if err != nil || o.Innovation != 4 || o.PredictionVariance != 3 || k.Mean != 12 || k.Variance != 1.5 {
		t.Fatalf("analytic %+v %+v %v", k, o, err)
	}
	old := k
	if _, err = k.Observe(math.NaN()); err == nil || k != old {
		t.Fatal("invalid observation mutated filter")
	}
	r := rand.New(rand.NewSource(51))
	y := make([]float64, 500)
	level := 100.0
	for i := range y {
		level += .3 * r.NormFloat64()
		y[i] = level + .7*r.NormFloat64()
	}
	fit, err := FitLocalLevel(y, LocalLevelFitSpec{Initial: LocalLevel{Mean: 100, Variance: 1, ProcessVariance: 1, ObservationVariance: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if fit.NegativeLogLikelihood >= fit.InitialNegativeLogLikelihood || math.Abs(fit.Initial.ProcessVariance-.09) > .08 || math.Abs(fit.Initial.ObservationVariance-.49) > .2 {
		t.Fatalf("fit %+v", fit)
	}
	if _, err := FitLocalLevel(y, LocalLevelFitSpec{Initial: fit.Initial, MaxEvaluations: 1}); err == nil {
		t.Fatal("budget silently succeeded")
	}
}
