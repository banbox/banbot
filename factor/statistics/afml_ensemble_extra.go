package statistics

import (
	"errors"
	"math"
	"math/rand"
	"sort"
)

type AdaBoostConfig struct {
	Rounds, MaxAttempts int
	Seed                int64
	Bootstrap           bool
	LearningRate        float64
}
type AdaBoostRound struct {
	Error, Alpha  float64
	SampleWeights []float64
}
type afmlStump struct {
	Feature     int
	Threshold   float64
	Left, Right int
}
type AdaBoost struct {
	features int
	stumps   []afmlStump
	alpha    []float64
}

func afmlStumpPrediction(s afmlStump, row []float64) int {
	if s.Feature < 0 || row[s.Feature] <= s.Threshold {
		return s.Left
	}
	return s.Right
}

// TrainAdaBoost is a bounded binary discrete AdaBoost ensemble with real
// minimum-weighted-error decision stumps. Labels are -1/+1. Bootstrap selects
// weighted replacement training samples as figure6.3 describes; false fits the
// weighted empirical dataset directly (standard discrete AdaBoost variant).
// alpha=.5*learningRate*log((1-error)/error); weighted chance learners are
// discarded, perfect learners end training. Audit retains pre-round weights.
func TrainAdaBoost(x [][]float64, labels []int, weights []float64, c AdaBoostConfig) (*AdaBoost, []AdaBoostRound, error) {
	if len(x) < 2 || len(labels) != len(x) || len(x[0]) < 1 || (weights != nil && len(weights) != len(x)) || c.Rounds < 1 || c.MaxAttempts < c.Rounds || !afmlFinite(c.LearningRate) || c.LearningRate <= 0 || c.LearningRate > 1 {
		return nil, nil, errors.New("statistics: invalid AdaBoost specification")
	}
	k := len(x[0])
	w := make([]float64, len(x))
	mass := 0.
	for i, row := range x {
		if len(row) != k || (labels[i] != -1 && labels[i] != 1) {
			return nil, nil, errors.New("statistics: AdaBoost binary rows required")
		}
		for _, v := range row {
			if !afmlFinite(v) {
				return nil, nil, errors.New("statistics: nonfinite AdaBoost feature")
			}
		}
		w[i] = 1
		if weights != nil {
			w[i] = weights[i]
		}
		if !afmlFinite(w[i]) || w[i] < 0 {
			return nil, nil, errors.New("statistics: invalid AdaBoost weight")
		}
		mass += w[i]
	}
	if mass <= 0 || !afmlFinite(mass) {
		return nil, nil, errors.New("statistics: invalid AdaBoost mass")
	}
	for i := range w {
		w[i] /= mass
	}
	model := &AdaBoost{features: k}
	audit := []AdaBoostRound{}
	rng := rand.New(rand.NewSource(c.Seed))
	for attempt := 0; attempt < c.MaxAttempts && len(model.stumps) < c.Rounds; attempt++ {
		fitW := append([]float64(nil), w...)
		if c.Bootstrap {
			cdf := make([]float64, len(w))
			sum := 0.
			for i, v := range w {
				sum += v
				cdf[i] = sum
			}
			cdf[len(cdf)-1] = 1
			clear(fitW)
			for range x {
				p := rng.Float64()
				i := sort.Search(len(cdf), func(j int) bool { return cdf[j] > p })
				fitW[i] += 1
			}
		}
		stump := afmlFitStump(x, labels, fitW)
		errRate := 0.
		for i, row := range x {
			if afmlStumpPrediction(stump, row) != labels[i] {
				errRate += w[i]
			}
		}
		if errRate >= .5 {
			continue
		}
		clipped := math.Max(1e-15, errRate)
		alpha := .5 * c.LearningRate * math.Log((1-clipped)/clipped)
		model.stumps = append(model.stumps, stump)
		model.alpha = append(model.alpha, alpha)
		audit = append(audit, AdaBoostRound{errRate, alpha, append([]float64(nil), w...)})
		if errRate == 0 {
			break
		}
		sum := 0.
		for i, row := range x {
			w[i] *= math.Exp(-alpha * float64(labels[i]*afmlStumpPrediction(stump, row)))
			sum += w[i]
		}
		if sum <= 0 || !afmlFinite(sum) {
			return nil, nil, errors.New("statistics: AdaBoost reweight overflow")
		}
		for i := range w {
			w[i] /= sum
		}
	}
	if len(model.stumps) == 0 {
		return nil, nil, errors.New("statistics: no AdaBoost learner above weighted chance")
	}
	return model, audit, nil
}

func afmlFitStump(x [][]float64, y []int, w []float64) afmlStump {
	pos, neg := 0., 0.
	for i, label := range y {
		if label == 1 {
			pos += w[i]
		} else {
			neg += w[i]
		}
	}
	side := 1
	if neg > pos {
		side = -1
	}
	best := afmlStump{Feature: -1, Left: side, Right: side}
	bestError := math.Min(pos, neg)
	for f := range x[0] {
		order := make([]int, len(x))
		for i := range order {
			order[i] = i
		}
		sort.SliceStable(order, func(i, j int) bool { return x[order[i]][f] < x[order[j]][f] })
		lp, ln := 0., 0.
		for j, index := range order {
			if y[index] == 1 {
				lp += w[index]
			} else {
				ln += w[index]
			}
			if j+1 == len(order) || x[index][f] == x[order[j+1]][f] {
				continue
			}
			rp, rn := math.Max(0, pos-lp), math.Max(0, neg-ln)
			left, right := 1, 1
			if ln > lp {
				left = -1
			}
			if rn > rp {
				right = -1
			}
			e := math.Min(lp, ln) + math.Min(rp, rn)
			if e < bestError-1e-15 {
				bestError = e
				best = afmlStump{f, x[index][f], left, right}
			}
		}
	}
	return best
}

// Predict returns the signed weighted margin and label (ties positive). Margin
// is not a calibrated probability; no confidence/profitability claim follows.
func (m *AdaBoost) Predict(row []float64) (int, float64, error) {
	if m == nil || len(m.stumps) == 0 || len(row) != m.features {
		return 0, 0, errors.New("statistics: invalid AdaBoost prediction")
	}
	for _, v := range row {
		if !afmlFinite(v) {
			return 0, 0, errors.New("statistics: nonfinite AdaBoost inference")
		}
	}
	margin := 0.
	for i, s := range m.stumps {
		margin += m.alpha[i] * float64(afmlStumpPrediction(s, row))
	}
	label := 1
	if margin < 0 {
		label = -1
	}
	return label, margin, nil
}

// EnsemblePredictionVariance gives variance of a mean of n equi-variance,
// equi-correlated estimators: sigma^2*(rho+(1-rho)/n). Negative correlations
// below the positive-semidefinite equicorrelation bound are invalid.
func EnsemblePredictionVariance(individualVariance, correlation float64, n int) (float64, error) {
	if n < 1 || !afmlFinite(individualVariance) || individualVariance < 0 || !afmlFinite(correlation) || correlation > 1 || (n == 1 && correlation != 1) || (n > 1 && correlation < -1/float64(n-1)) {
		return 0, errors.New("statistics: invalid ensemble covariance")
	}
	return individualVariance * (correlation + (1-correlation)/float64(n)), nil
}
