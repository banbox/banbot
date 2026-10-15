package statistics

import (
	"errors"
	"math"
)

type ADFSpec struct {
	Lags             int
	Intercept, Trend bool
}
type ADFResult struct {
	DeltaCoefficient, StandardError, TStatistic float64
	Samples, DegreesOfFreedom                   int
}

// AugmentedDickeyFuller regresses delta x on lagged levels and differences.
// It returns a statistic, NOT a Gaussian p-value: unit-root and estimated
// cointegration residual tests require their own critical-value distributions.
// Residualizing lagged levels against nuisance regressors obtains its standard
// error by Frisch-Waugh-Lovell, avoiding inversion of normal equations.
func AugmentedDickeyFuller(values []float64, spec ADFSpec) (ADFResult, error) {
	if spec.Lags < 0 || len(values) < spec.Lags+4 {
		return ADFResult{}, errors.New("statistics: insufficient ADF sample")
	}
	for _, v := range values {
		if !finite(v) {
			return ADFResult{}, errors.New("statistics: nonfinite ADF input")
		}
	}
	n := len(values) - spec.Lags - 1
	rows := make([][]float64, n)
	y := make([]float64, n)
	levels := make([]float64, n)
	for i := range n {
		t := spec.Lags + 1 + i
		levels[i] = values[t-1]
		y[i] = values[t] - values[t-1]
		rows[i] = []float64{levels[i]}
		for lag := 1; lag <= spec.Lags; lag++ {
			rows[i] = append(rows[i], values[t-lag]-values[t-lag-1])
		}
		if spec.Trend {
			rows[i] = append(rows[i], float64(t))
		}
	}
	f, err := Regress(y, rows, spec.Intercept)
	if err != nil {
		return ADFResult{}, err
	}
	df := n - f.Rank
	if df <= 0 {
		return ADFResult{}, errors.New("statistics: ADF has no residual degrees of freedom")
	}
	residualLevels := append([]float64(nil), levels...)
	if len(rows[0]) > 1 {
		nuisance := make([][]float64, n)
		for i := range n {
			nuisance[i] = rows[i][1:]
		}
		aux, err := Regress(levels, nuisance, spec.Intercept)
		if err != nil {
			return ADFResult{}, err
		}
		residualLevels = aux.Residuals
	} else if spec.Intercept {
		mean, _, err := MeanStd(levels, 0)
		if err != nil {
			return ADFResult{}, err
		}
		for i := range residualLevels {
			residualLevels[i] -= mean
		}
	}
	sse, ssx := 0.0, 0.0
	for i := range n {
		sse += f.Residuals[i] * f.Residuals[i]
		ssx += residualLevels[i] * residualLevels[i]
	}
	se := math.Sqrt(sse / float64(df) / ssx)
	if !finite(se) || se <= 0 {
		return ADFResult{}, errors.New("statistics: undefined ADF statistic")
	}
	stat := f.Beta[0] / se
	if !finite(stat) {
		return ADFResult{}, errors.New("statistics: ADF statistic overflow")
	}
	return ADFResult{f.Beta[0], se, stat, n, df}, nil
}

// EngleGranger fits the hedge on the declared training sample then tests its
// residuals without a second intercept. Critical values depend on the number
// of fitted regressors and deterministic terms; ordinary ADF cutoffs do not
// establish cointegration here.
func EngleGranger(y []float64, x [][]float64, intercept bool, lags int) (Regression, ADFResult, error) {
	fit, err := Regress(y, x, intercept)
	if err != nil {
		return Regression{}, ADFResult{}, err
	}
	adf, err := AugmentedDickeyFuller(fit.Residuals, ADFSpec{Lags: lags})
	return fit, adf, err
}
