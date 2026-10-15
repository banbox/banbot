package statistics

import (
	"errors"
	"math"
)

// ProbabilisticSharpe implements AFML 14.7.2. Observed and benchmark Sharpe
// ratios use the original return sampling frequency, NOT annualized values.
// Kurtosis is Pearson kurtosis (Gaussian=3), not excess kurtosis.
func ProbabilisticSharpe(observed, benchmark float64, samples int, skewness, kurtosis float64) (float64, error) {
	if samples < 2 || !afmlFinite(observed) || !afmlFinite(benchmark) || !afmlFinite(skewness) || !afmlFinite(kurtosis) || kurtosis < 1 {
		return 0, errors.New("statistics: invalid probabilistic Sharpe inputs")
	}
	variance := 1 - skewness*observed + (kurtosis-1)*observed*observed/4
	if variance <= 0 || !afmlFinite(variance) {
		return 0, errors.New("statistics: invalid Sharpe estimation variance")
	}
	z := (observed - benchmark) * math.Sqrt(float64(samples)-1) / math.Sqrt(variance)
	if !afmlFinite(z) {
		return 0, errors.New("statistics: probabilistic Sharpe overflow")
	}
	return .5 * math.Erfc(-z/math.Sqrt2), nil
}

// ExpectedMaximumSharpe implements AFML 14.7.3's zero-true-SR benchmark under
// independent trials. trialVariance is across trial SR estimates in the same
// sampling frequency as observed SR. Correlated trial counts must be adjusted
// by the caller; treating all correlated searches as independent is not proven.
func ExpectedMaximumSharpe(trialVariance float64, independentTrials int) (float64, error) {
	if !afmlFinite(trialVariance) || trialVariance < 0 || independentTrials < 1 {
		return 0, errors.New("statistics: invalid deflated Sharpe trial inputs")
	}
	if independentTrials == 1 || trialVariance == 0 {
		return 0, nil
	}
	const eulerGamma = .5772156649015328606
	n := float64(independentTrials)
	quantile := func(p float64) float64 { return math.Sqrt2 * math.Erfinv(2*p-1) }
	benchmark := math.Sqrt(trialVariance) * ((1-eulerGamma)*quantile(1-1/n) + eulerGamma*quantile(1-1/(n*math.E)))
	if !afmlFinite(benchmark) {
		return 0, errors.New("statistics: deflated Sharpe benchmark overflow")
	}
	return benchmark, nil
}

// DeflatedSharpe returns PSR at the multiple-trial-adjusted rejection threshold.
func DeflatedSharpe(observed, trialVariance float64, samples, independentTrials int, skewness, kurtosis float64) (float64, error) {
	benchmark, err := ExpectedMaximumSharpe(trialVariance, independentTrials)
	if err != nil {
		return 0, err
	}
	return ProbabilisticSharpe(observed, benchmark, samples, skewness, kurtosis)
}
