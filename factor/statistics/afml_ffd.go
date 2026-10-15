package statistics

import (
	"errors"
	"math"
)

type FFDSelectionSpec struct {
	DStep, Threshold, ADFCritical float64
	MaxWeights, MinSamples        int
	MaxWork                       int64
}

type FFDTrial struct {
	D, Statistic float64
	Samples      int
	Reason       string
}

type FFDSelection struct {
	D, ADFStatistic float64
	Weights         []float64
	Trials          []FFDTrial
}

// SelectStationaryFFD fits ONLY the supplied training levels, choosing the
// smallest grid d in [0,2] passing an intercept/lag1 ADF rejection threshold.
// ADFCritical must be a unit-root critical value appropriate for the declared
// sample size, not a Normal quantile. No ADF p-value is invented. Kernels that
// exceed the work/history bound or leave fewer MinSamples are audited/skipped.
// Grid endpoints include d=2 even when DStep does not divide 2.
func SelectStationaryFFD(levels []float64, spec FFDSelectionSpec) (FFDSelection, error) {
	if !afmlFinite(spec.DStep) || spec.DStep < .01 || spec.DStep > 1 || !afmlFinite(spec.ADFCritical) || spec.ADFCritical >= 0 || spec.MinSamples < 8 || spec.MaxWeights < 1 || spec.MaxWeights > 100000 || len(levels) < spec.MinSamples || spec.MaxWork < 1 {
		return FFDSelection{}, errors.New("statistics: invalid FFD stationarity selection policy")
	}
	for _, value := range levels {
		if !afmlFinite(value) {
			return FFDSelection{}, errors.New("statistics: FFD training levels must be finite")
		}
	}
	out := FFDSelection{}
	steps := int(math.Ceil(2 / spec.DStep))
	work := 0.
	for i := 0; i <= steps; i++ {
		d := math.Min(2, float64(i)*spec.DStep)
		trial := FFDTrial{D: d, Statistic: math.NaN()}
		weights, err := FractionalDiffWeights(d, spec.Threshold, spec.MaxWeights)
		if err != nil {
			trial.Reason = err.Error()
			out.Trials = append(out.Trials, trial)
			continue
		}
		trial.Samples = len(levels) - len(weights) + 1
		if trial.Samples < spec.MinSamples {
			trial.Reason = "insufficient post-kernel training observations"
			out.Trials = append(out.Trials, trial)
			continue
		}
		work += float64(len(levels)) * float64(len(weights)+20)
		if work > float64(spec.MaxWork) {
			return out, errors.New("statistics: FFD selection exceeds work budget")
		}
		values, err := FractionalDifference(levels, weights)
		if err != nil {
			return out, err
		}
		adf, err := AugmentedDickeyFuller(values[len(weights)-1:], ADFSpec{Lags: 1, Intercept: true})
		if err != nil {
			trial.Reason = err.Error()
			out.Trials = append(out.Trials, trial)
			continue
		}
		trial.Statistic = adf.TStatistic
		out.Trials = append(out.Trials, trial)
		if trial.Statistic < spec.ADFCritical {
			out.D, out.ADFStatistic, out.Weights = d, trial.Statistic, weights
			return out, nil
		}
	}
	return out, errors.New("statistics: no feasible FFD grid candidate rejects the unit root")
}
