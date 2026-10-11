package statistics

import (
	"errors"
	"fmt"
	"math"
)

type FixedHorizonResult struct {
	Start, End, Label int
	Return            float64
}

// FixedHorizonLabels labels only resolved requested observations, using simple
// returns and source section3.2's strict threshold crossings. Equality is neutral.
// threshold has one entry per requested start, allowing prior-fitted volatility.
func FixedHorizonLabels(prices []float64, starts []int, horizon int, threshold []float64) ([]FixedHorizonResult, error) {
	if horizon < 1 || len(starts) == 0 || len(threshold) != len(starts) {
		return nil, errors.New("statistics: invalid fixed horizon labels")
	}
	out := make([]FixedHorizonResult, len(starts))
	for i, s := range starts {
		if s < 0 || s >= len(prices) || horizon > len(prices)-1-s || !afmlFinite(threshold[i]) || threshold[i] < 0 {
			return nil, errors.New("statistics: unresolved/invalid fixed horizon")
		}
		end := s + horizon
		if !afmlFinite(prices[s]) || prices[s] <= 0 || !afmlFinite(prices[end]) || prices[end] <= 0 {
			return nil, errors.New("statistics: invalid endpoint price")
		}
		r := prices[end]/prices[s] - 1
		if !afmlFinite(r) {
			return nil, errors.New("statistics: label return overflow")
		}
		label := 0
		if r > threshold[i] {
			label = 1
		} else if r < -threshold[i] {
			label = -1
		}
		out[i] = FixedHorizonResult{s, end, label, r}
	}
	return out, nil
}

type MulticlassReport struct {
	Classes           []int
	Confusion         [][]float64
	Accuracy, LogLoss float64
}

// MulticlassClassification keeps a declared, stable full class mapping even
// when a fold omits classes. Prediction ties choose the first declared class.
// Log-loss clips each mass then renormalizes, as sklearn log_loss does.
func MulticlassClassification(labels, classes []int, probabilities [][]float64, weights []float64, epsilon float64) (MulticlassReport, error) {
	r := MulticlassReport{Classes: append([]int(nil), classes...)}
	if len(labels) == 0 || len(labels) != len(probabilities) || (weights != nil && len(weights) != len(labels)) || len(classes) < 2 || !afmlFinite(epsilon) || epsilon <= 0 || epsilon >= .5 {
		return r, errors.New("statistics: invalid multiclass score")
	}
	lookup := map[int]int{}
	r.Confusion = make([][]float64, len(classes))
	for i, c := range classes {
		if _, ok := lookup[c]; ok {
			return r, errors.New("statistics: duplicate score class")
		}
		lookup[c] = i
		r.Confusion[i] = make([]float64, len(classes))
	}
	total, correct, loss := 0., 0., 0.
	for i, label := range labels {
		target, ok := lookup[label]
		if !ok || len(probabilities[i]) != len(classes) {
			return r, errors.New("statistics: score class/dimension mismatch")
		}
		w := 1.
		if weights != nil {
			w = weights[i]
		}
		if !afmlFinite(w) || w < 0 {
			return r, errors.New("statistics: invalid score weight")
		}
		best := 0
		sum, clipped := 0., 0.
		for j, p := range probabilities[i] {
			if !afmlFinite(p) || p < 0 || p > 1 {
				return r, errors.New("statistics: invalid probability")
			}
			sum += p
			clipped += math.Max(epsilon, math.Min(1-epsilon, p))
			if p > probabilities[i][best] {
				best = j
			}
		}
		if math.Abs(sum-1) > 1e-10 {
			return r, errors.New("statistics: probabilities must sum to one")
		}
		total += w
		r.Confusion[target][best] += w
		if target == best {
			correct += w
		}
		loss -= w * math.Log(math.Max(epsilon, math.Min(1-epsilon, probabilities[i][target]))/clipped)
	}
	if total <= 0 || !afmlFinite(total) || !afmlFinite(loss) {
		return r, errors.New("statistics: invalid score mass")
	}
	r.Accuracy = correct / total
	r.LogLoss = loss / total
	return r, nil
}

// WeightedFoldPredictor must fit all transforms/models solely on trainIndices,
// consuming their supplied weights, then return test-row probabilities in the
// declared full class order. This callback boundary makes model-family and fold
// provenance explicit; it is not a deployable historical walk-forward model.
type WeightedFoldPredictor func(trainIndices, testIndices []int, trainWeights []float64, classes []int) ([][]float64, error)

func PurgedClassificationScores(spans []EventSpan, labels, classes []int, weights []float64, folds, embargo int, epsilon float64, fit WeightedFoldPredictor) ([]MulticlassReport, error) {
	if fit == nil || len(labels) != len(spans) || (weights != nil && len(weights) != len(spans)) {
		return nil, errors.New("statistics: invalid purged scorer")
	}
	for _, w := range weights {
		if !afmlFinite(w) || w < 0 {
			return nil, errors.New("statistics: invalid CV weights")
		}
	}
	splits, err := PurgedKFold(spans, folds, embargo)
	if err != nil {
		return nil, err
	}
	out := make([]MulticlassReport, len(splits))
	for i, s := range splits {
		if len(s.Train) == 0 {
			return nil, fmt.Errorf("statistics: fold %d has no training rows", i)
		}
		tw := make([]float64, len(s.Train))
		mass := 0.
		for j, index := range s.Train {
			tw[j] = 1
			if weights != nil {
				tw[j] = weights[index]
			}
			mass += tw[j]
		}
		if mass <= 0 || !afmlFinite(mass) {
			return nil, errors.New("statistics: fold training mass invalid")
		}
		p, err := fit(append([]int(nil), s.Train...), append([]int(nil), s.Test...), tw, append([]int(nil), classes...))
		if err != nil {
			return nil, fmt.Errorf("statistics: fold %d fit: %w", i, err)
		}
		tl := make([]int, len(s.Test))
		sw := make([]float64, len(s.Test))
		for j, index := range s.Test {
			tl[j] = labels[index]
			sw[j] = 1
			if weights != nil {
				sw[j] = weights[index]
			}
		}
		out[i], err = MulticlassClassification(tl, classes, p, sw, epsilon)
		if err != nil {
			return nil, fmt.Errorf("statistics: fold %d score: %w", i, err)
		}
	}
	return out, nil
}
