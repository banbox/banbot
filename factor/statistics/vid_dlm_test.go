package statistics

import (
	"math"
	"testing"
)

func TestDynamicAR1AnalyticPriorAndJosephUpdate(t *testing.T) {
	k, err := NewDynamicAR1(.5, 1, .25)
	if err != nil {
		t.Fatal(err)
	}
	o, err := k.Observe(2, 2, 1)
	if err != nil || o.Prediction != 1 || o.Innovation != 1 || o.InnovationVariance != 6 || o.PriorBeta != .5 || math.Abs(o.PosteriorBeta-11./12) > 1e-14 || math.Abs(o.PosteriorVariance-5./24) > 1e-14 {
		t.Fatal(o, err)
	}
	before := *k
	for _, observation := range [][3]float64{{math.NaN(), 1, 1}, {1, 1, 0}, {math.MaxFloat64, 1, 1}, {1, math.Inf(1), 1}} {
		if _, err := k.Observe(observation[0], observation[1], observation[2]); err == nil || *k != before {
			t.Fatal("invalid observation mutated model", observation, err)
		}
	}
	// A zero regressor contains no beta information; process noise still grows P.
	o, err = k.Observe(0, 3, 2)
	if err != nil || o.PosteriorBeta != before.beta || o.PosteriorVariance != before.variance+.25 {
		t.Fatal(o, err)
	}
}

func TestDynamicAR1InitializationValidation(t *testing.T) {
	for _, values := range [][3]float64{{math.NaN(), 1, 1}, {1, -1, 1}, {1, 1, -1}} {
		if _, err := NewDynamicAR1(values[0], values[1], values[2]); err == nil {
			t.Fatal("invalid initialization accepted")
		}
	}
}
