package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestAFMLPurgedMDAAndSFI(t *testing.T) {
	x := make([][]float64, 60)
	y := make([]int, 60)
	w := make([]float64, 60)
	spans := make([]EventSpan, 60)
	for i := range x {
		y[i] = i % 2
		x[i] = []float64{float64(y[i]), 7}
		w[i] = float64(1 + i%3)
		spans[i] = EventSpan{2 * i, 2*i + 1}
	}
	fit := func(rows [][]float64, labels []int, weights []float64, classes []int) (AFMLProbabilityPredictor, error) {
		config := RandomForestConfig{Trees: 20, MaxDepth: 2, MinSamplesLeaf: 1, MaxFeatures: len(rows[0]), SampleFraction: 1, Seed: 20, Criterion: "entropy"}
		model, err := TrainRandomForest(rows, labels, weights, config)
		if err != nil {
			return nil, err
		}
		return func(test [][]float64) ([][]float64, error) {
			out := make([][]float64, len(test))
			for i, row := range test {
				p, err := model.PredictProbability(row)
				if err != nil {
					return nil, err
				}
				out[i] = []float64{1 - p, p}
			}
			return out, nil
		}, nil
	}
	a, err := PurgedFeatureImportance(x, y, []int{0, 1}, w, spans, 3, 1, "neg_log_loss", false, 42, fit)
	if err != nil {
		t.Fatal(err)
	}
	if a.Features[0].Mean < .9 || math.Abs(a.Features[1].Mean) > 1e-12 {
		t.Fatal(a)
	}
	b, err := PurgedFeatureImportance(x, y, []int{0, 1}, w, spans, 3, 1, "neg_log_loss", false, 42, fit)
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatal("seed reproducibility", a, b, err)
	}
	s, err := PurgedFeatureImportance(x, y, []int{0, 1}, w, spans, 3, 1, "accuracy", true, 42, fit)
	if err != nil || s.Features[0].Mean < .99 || s.Features[1].Mean > .65 {
		t.Fatal(s, err)
	}
	if x[0][1] != 7 || x[0][0] != 0 || w[0] != 1 {
		t.Fatal("mutated inputs")
	}
}

func TestAFMLNegativeImportanceAndFoldSummary(t *testing.T) {
	x := [][]float64{{0}, {1}, {0}, {1}, {0}, {1}, {0}, {1}}
	y := []int{0, 1, 0, 1, 0, 1, 0, 1}
	spans := make([]EventSpan, 8)
	for i := range spans {
		spans[i] = EventSpan{2 * i, 2*i + 1}
	}
	// A deliberately anti-signal model can improve after permutation; source
	// MDA must retain negative importance rather than clipping it to zero.
	fit := func(_ [][]float64, _ []int, _ []float64, _ []int) (AFMLProbabilityPredictor, error) {
		return func(rows [][]float64) ([][]float64, error) {
			out := make([][]float64, len(rows))
			for i, row := range rows {
				p := .9
				if row[0] > 0 {
					p = .1
				}
				out[i] = []float64{1 - p, p}
			}
			return out, nil
		}, nil
	}
	r, err := PurgedFeatureImportance(x, y, []int{0, 1}, nil, spans, 2, 0, "neg_log_loss", false, 1, fit)
	if err != nil || r.Features[0].Mean >= 0 {
		t.Fatal(r, err)
	}
	s := afmlImportanceSummary([]float64{1, 3})
	if s.Mean != 2 || math.Abs(s.StdError-1) > 1e-12 || s.ValidFolds != 2 {
		t.Fatal(s)
	}
}
