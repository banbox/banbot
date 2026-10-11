package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestAFMLAdaBoostLearnsAndReweights(t *testing.T) {
	// A stump cannot represent the positive middle interval. Success requires
	// actual sequential reweighting and an ensemble of differing split rules.
	x := [][]float64{{0}, {1}, {2}, {3}, {4}, {5}}
	labels := []int{-1, -1, 1, 1, -1, -1}
	c := AdaBoostConfig{Rounds: 12, MaxAttempts: 12, LearningRate: 1}
	model, audit, err := TrainAdaBoost(x, labels, nil, c)
	if err != nil || len(audit) < 3 {
		t.Fatal(audit, err)
	}
	for i, row := range x {
		got, _, err := model.Predict(row)
		if err != nil || got != labels[i] {
			t.Fatal(i, got, err)
		}
	}
	if math.Abs(audit[0].Error-1./3) > 1e-12 || math.Abs(audit[0].Alpha-.5*math.Log(2)) > 1e-12 {
		t.Fatal(audit[0])
	}
	largest := 0.
	for _, w := range audit[1].SampleWeights {
		largest = math.Max(largest, w)
	}
	if largest <= 1./6 {
		t.Fatal("did not reweight errors")
	}
	saved, _, _ := model.Predict([]float64{2})
	x[2][0] = 99
	labels[2] = -1
	got, _, _ := model.Predict([]float64{2})
	if got != saved {
		t.Fatal("retained mutable caller training")
	}
}

func TestAFMLAdaBoostBootstrapAndChance(t *testing.T) {
	x := [][]float64{{0}, {1}, {2}, {3}}
	y := []int{-1, -1, 1, 1}
	c := AdaBoostConfig{Rounds: 5, MaxAttempts: 50, LearningRate: 1, Bootstrap: true, Seed: 18}
	a, aa, err := TrainAdaBoost(x, y, nil, c)
	if err != nil {
		t.Fatal(err)
	}
	b, bb, err := TrainAdaBoost(x, y, nil, c)
	if err != nil || !reflect.DeepEqual(aa, bb) || !reflect.DeepEqual(a, b) {
		t.Fatal("nondeterministic", err)
	}
	if _, _, err = TrainAdaBoost([][]float64{{1}, {1}}, []int{-1, 1}, nil, c); err == nil {
		t.Fatal("accepted chance learner")
	}
	v, err := EnsemblePredictionVariance(4, .5, 4)
	if err != nil || v != 2.5 {
		t.Fatal(v, err)
	}
	v, err = EnsemblePredictionVariance(4, -1./3, 4)
	if err != nil || math.Abs(v) > 1e-12 {
		t.Fatal(v, err)
	}
	if _, err = EnsemblePredictionVariance(1, -.9, 4); err == nil {
		t.Fatal("accepted nonPSD correlation")
	}
}
