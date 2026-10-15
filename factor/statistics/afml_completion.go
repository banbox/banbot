package statistics

import (
	"errors"
	"math"
)

// MeanDecreaseImpurity implements snippet8.2. Each tree first normalizes its
// bootstrap-weighted impurity reductions; zero entries are excluded from the
// feature mean. Sample standard deviations divide by sqrt(ALL trees), then both
// columns divide by the sum of feature means. NaN denotes an unused feature or
// an undefined standard error. MaxFeatures=1 is the source anti-masking setup.
// Old v1 models remain usable for prediction, but lack original bootstrap mass.
func (f *RandomForest) MeanDecreaseImpurity() ([]FeatureImportance, error) {
	if f == nil || f.artifact.Version != "afml-rf-v2" {
		return nil, errors.New("statistics: forest lacks original bootstrap impurity metadata")
	}
	values := make([][]float64, f.artifact.Features)
	for _, tree := range f.artifact.Trees {
		imp := make([]float64, len(values))
		total := 0.
		for _, n := range tree {
			if n.Feature >= 0 {
				l, r := tree[n.Left], tree[n.Right]
				gain := math.Max(0, n.Mass*n.Impurity-l.Mass*l.Impurity-r.Mass*r.Impurity)
				imp[n.Feature] += gain
				total += gain
			}
		}
		for j, v := range imp {
			if total > 0 && v > 0 {
				values[j] = append(values[j], v/total)
			} else {
				values[j] = append(values[j], math.NaN())
			}
		}
	}
	result := make([]FeatureImportance, len(values))
	total := 0.
	for j, v := range values {
		result[j] = afmlImportanceSummary(v)
		if afmlFinite(result[j].Mean) {
			total += result[j].Mean
		}
	}
	if total == 0 {
		return result, nil
	}
	for j := range result {
		result[j].Mean /= total
		result[j].StdError /= total
	}
	return result, nil
}

type BiasVarianceDiagnostic struct{ MeanPrediction, BiasSquared, Variance, NoiseVariance, ExpectedMSE float64 }

// PredictionBiasVariance implements chapter6.2's expectation over independently
// trained predictions at one fixed x. truth is known f(x), not a noisy held-out
// label. Noise variance must be supplied; it cannot be inferred from predictions.
// The empirical population variance (ddof0) makes the finite-ensemble identity
// mean((prediction-truth)^2)=bias^2+variance exact up to floating-point rounding.
func PredictionBiasVariance(predictions []float64, truth, noiseVariance float64) (BiasVarianceDiagnostic, error) {
	r := BiasVarianceDiagnostic{}
	if len(predictions) == 0 || !afmlFinite(truth) || !afmlFinite(noiseVariance) || noiseVariance < 0 {
		return r, errors.New("statistics: invalid bias variance inputs")
	}
	mean, m2 := 0., 0.
	for i, v := range predictions {
		if !afmlFinite(v) {
			return r, errors.New("statistics: nonfinite prediction")
		}
		d := v - mean
		mean += d / float64(i+1)
		m2 += d * (v - mean)
	}
	r.MeanPrediction = mean
	r.BiasSquared = (mean - truth) * (mean - truth)
	r.Variance = m2 / float64(len(predictions))
	r.NoiseVariance = noiseVariance
	r.ExpectedMSE = r.BiasSquared + r.Variance + noiseVariance
	if !afmlFinite(r.ExpectedMSE) {
		return BiasVarianceDiagnostic{}, errors.New("statistics: bias variance overflow")
	}
	return r, nil
}

type BaggingAccuracyDiagnostic struct {
	ExpectedCorrect, VarianceCorrect                      float64
	NecessaryCorrectThreshold, SufficientCorrectThreshold int
	NecessaryProbability, SufficientProbability           float64
}

// IndependentBaggingAccuracy reproduces snippet6.1's P[X>floor(N/k)] and
// additionally the sufficient P[X>N/2]. For k>2 neither quantity is exact
// multiclass plurality accuracy: the wrong-vote distribution and tie rule are
// unspecified. For binary even N, ties are excluded. Classifiers must have
// independent, identical correctness probability; financial RF trees do not
// automatically satisfy that assumption. Work is bounded to one million votes.
func IndependentBaggingAccuracy(n, classes int, p float64) (BaggingAccuracyDiagnostic, error) {
	r := BaggingAccuracyDiagnostic{}
	if n < 1 || n > 1000000 || classes < 2 || !afmlFinite(p) || p < 0 || p > 1 {
		return r, errors.New("statistics: invalid independent bagging diagnostic")
	}
	r.ExpectedCorrect = float64(n) * p
	r.VarianceCorrect = float64(n) * p * (1 - p)
	r.NecessaryCorrectThreshold = n/classes + 1
	r.SufficientCorrectThreshold = n/2 + 1
	// Sum the requested upper tails directly in log space, avoiding subtraction
	// cancellation when accuracy is extremely small and factorial overflow.
	tail := func(start int) float64 {
		if p == 0 {
			return 0
		}
		if p == 1 {
			return 1
		}
		lnFact, _ := math.Lgamma(float64(n + 1))
		logP, logQ := math.Log(p), math.Log1p(-p)
		maxLog, sum := math.Inf(-1), 0.
		for i := start; i <= n; i++ {
			a, _ := math.Lgamma(float64(i + 1))
			b, _ := math.Lgamma(float64(n - i + 1))
			v := lnFact - a - b + float64(i)*logP + float64(n-i)*logQ
			if v > maxLog {
				sum = sum*math.Exp(maxLog-v) + 1
				maxLog = v
			} else {
				sum += math.Exp(v - maxLog)
			}
		}
		return math.Min(1, math.Exp(maxLog)*sum)
	}
	r.NecessaryProbability = tail(r.NecessaryCorrectThreshold)
	r.SufficientProbability = tail(r.SufficientCorrectThreshold)
	return r, nil
}
