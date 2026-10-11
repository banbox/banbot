package statistics

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
)

type AFMLProbabilityPredictor func(rows [][]float64) ([][]float64, error)
type AFMLWeightedClassifierFit func(rows [][]float64, labels []int, weights []float64, classes []int) (AFMLProbabilityPredictor, error)
type FeatureImportance struct {
	Mean, StdError float64
	ValidFolds     int
	FoldValues     []float64
}
type PurgedImportanceResult struct {
	Features []FeatureImportance
	Baseline []float64
}

func afmlImportanceScore(labels, classes []int, p [][]float64, w []float64, scoring string) (float64, error) {
	r, err := MulticlassClassification(labels, classes, p, w, 1e-15)
	if err != nil {
		return 0, err
	}
	switch scoring {
	case "accuracy":
		return r.Accuracy, nil
	case "neg_log_loss":
		return -r.LogLoss, nil
	}
	return 0, errors.New("statistics: unsupported importance score")
}

func afmlImportanceSummary(values []float64) FeatureImportance {
	r := FeatureImportance{Mean: math.NaN(), StdError: math.NaN(), FoldValues: append([]float64(nil), values...)}
	mean, m2, n := 0., 0., 0.
	for _, v := range values {
		if !afmlFinite(v) {
			continue
		}
		n++
		d := v - mean
		mean += d / n
		m2 += d * (v - mean)
	}
	r.ValidFolds = int(n)
	if n > 0 {
		r.Mean = mean
	}
	if n > 1 {
		r.StdError = math.Sqrt(m2 / (n - 1) / float64(len(values)))
	}
	return r
}

func afmlImportanceRows(x [][]float64, y []int, w []float64, indices []int, column int) ([][]float64, []int, []float64) {
	rows := make([][]float64, len(indices))
	labels := make([]int, len(indices))
	weights := make([]float64, len(indices))
	for i, index := range indices {
		if column < 0 {
			rows[i] = append([]float64(nil), x[index]...)
		} else {
			rows[i] = []float64{x[index][column]}
		}
		labels[i] = y[index]
		weights[i] = 1
		if w != nil {
			weights[i] = w[index]
		}
	}
	return rows, labels, weights
}

// PurgedFeatureImportance performs source8.3 MDA or8.4 SFI. The callback owns
// actual model fitting and every training transform; all supplied train rows
// and weights are cloned. MDA fits once per fold and permutes each test column
// without moving labels/weights. SFI fits each feature alone. CV is retrospective
// scenario evaluation, not a causal deployment artifact.
func PurgedFeatureImportance(x [][]float64, y, classes []int, weights []float64, spans []EventSpan, folds, embargo int, scoring string, single bool, seed int64, fit AFMLWeightedClassifierFit) (PurgedImportanceResult, error) {
	r := PurgedImportanceResult{}
	if fit == nil || len(x) == 0 || len(x[0]) == 0 || len(x) != len(y) || len(x) != len(spans) || (weights != nil && len(weights) != len(x)) || (scoring != "accuracy" && scoring != "neg_log_loss") {
		return r, errors.New("statistics: invalid importance inputs")
	}
	k := len(x[0])
	for _, row := range x {
		if len(row) != k {
			return r, errors.New("statistics: inconsistent importance rows")
		}
		for _, v := range row {
			if !afmlFinite(v) {
				return r, errors.New("statistics: nonfinite importance feature")
			}
		}
	}
	for _, w := range weights {
		if !afmlFinite(w) || w < 0 {
			return r, errors.New("statistics: invalid importance weight")
		}
	}
	splits, err := PurgedKFold(spans, folds, embargo)
	if err != nil {
		return r, err
	}
	values := make([][]float64, k)
	for j := range values {
		values[j] = make([]float64, len(splits))
	}
	rng := rand.New(rand.NewSource(seed))
	for f, s := range splits {
		if len(s.Train) == 0 {
			return r, errors.New("statistics: empty importance training fold")
		}
		var predictor AFMLProbabilityPredictor
		var base float64
		if !single {
			rows, labels, w := afmlImportanceRows(x, y, weights, s.Train, -1)
			predictor, err = fit(rows, labels, w, append([]int(nil), classes...))
			if err != nil {
				return r, fmt.Errorf("statistics: importance fold%d fit: %w", f, err)
			}
			if predictor == nil {
				return r, errors.New("statistics: nil importance predictor")
			}
			test, ty, tw := afmlImportanceRows(x, y, weights, s.Test, -1)
			p, err := predictor(test)
			if err != nil {
				return r, err
			}
			base, err = afmlImportanceScore(ty, classes, p, tw, scoring)
			if err != nil {
				return r, err
			}
			r.Baseline = append(r.Baseline, base)
		}
		for j := 0; j < k; j++ {
			column := -1
			if single {
				column = j
				train, labels, w := afmlImportanceRows(x, y, weights, s.Train, column)
				predictor, err = fit(train, labels, w, append([]int(nil), classes...))
				if err != nil {
					return r, err
				}
				if predictor == nil {
					return r, errors.New("statistics: nil SFI predictor")
				}
			}
			test, ty, tw := afmlImportanceRows(x, y, weights, s.Test, column)
			if !single {
				perm := rng.Perm(len(test))
				original := make([]float64, len(test))
				for i := range test {
					original[i] = test[i][j]
				}
				for i := range test {
					test[i][j] = original[perm[i]]
				}
			}
			p, err := predictor(test)
			if err != nil {
				return r, err
			}
			score, err := afmlImportanceScore(ty, classes, p, tw, scoring)
			if err != nil {
				return r, err
			}
			values[j][f] = score
			if !single {
				denom := -score
				if scoring == "accuracy" {
					denom = 1 - score
				}
				if denom <= 0 {
					values[j][f] = math.NaN()
				} else {
					values[j][f] = (base - score) / denom
				}
			}
		}
	}
	r.Features = make([]FeatureImportance, k)
	for j := range values {
		r.Features[j] = afmlImportanceSummary(values[j])
	}
	return r, nil
}
