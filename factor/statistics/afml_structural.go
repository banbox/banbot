package statistics

import (
	"errors"
	"math"
)

type BackwardADFResult struct {
	Statistic  float64
	BestStart  int
	Statistics []float64
}

// BackwardSupADF implements AFML snippets17.1–17.4's inner-loop endpoint
// statistic on already log-transformed prices. It trims the FULL lagged design
// at successive start rows, preserving pre-start lag seeds, exactly as get_bsadf.
// Constants: nc=no constant, c=intercept, ct=linear, ctt=linear+quadratic trend.
// minLength follows the printed startPoints range: at least minLength+1 raw
// observations remain at the last candidate. No Gaussian critical values are
// assigned to this right-tail/unit-root statistic. maxFits is an explicit budget.
func BackwardSupADF(logValues []float64, minLength, lags int, constant string, maxFits int) (BackwardADFResult, error) {
	if lags < 0 || minLength < lags+3 || len(logValues) <= minLength || maxFits < 1 || (constant != "nc" && constant != "c" && constant != "ct" && constant != "ctt") {
		return BackwardADFResult{}, errors.New("statistics: invalid backward ADF specification")
	}
	fits := len(logValues) - minLength
	if fits > maxFits {
		return BackwardADFResult{}, errors.New("statistics: backward ADF exceeds fit budget")
	}
	for _, v := range logValues {
		if !afmlFinite(v) {
			return BackwardADFResult{}, errors.New("statistics: backward ADF needs finite log values")
		}
	}
	n := len(logValues) - lags - 1
	x := make([][]float64, n)
	y := make([]float64, n)
	for i := range x {
		t := i + lags + 1
		y[i] = logValues[t] - logValues[t-1]
		x[i] = []float64{logValues[t-1]}
		for lag := 1; lag <= lags; lag++ {
			x[i] = append(x[i], logValues[t-lag]-logValues[t-lag-1])
		}
		if constant != "nc" {
			x[i] = append(x[i], 1)
		}
		if constant == "ct" || constant == "ctt" {
			x[i] = append(x[i], float64(i))
		}
		if constant == "ctt" {
			x[i] = append(x[i], float64(i)*float64(i))
		}
	}
	result := BackwardADFResult{Statistic: math.Inf(-1), BestStart: -1}
	for start := 0; start < fits; start++ {
		stat, err := afmlFirstCoefficientT(y[start:], x[start:])
		if err != nil {
			return BackwardADFResult{}, err
		}
		result.Statistics = append(result.Statistics, stat)
		if stat > result.Statistic {
			result.Statistic, result.BestStart = stat, start
		}
	}
	return result, nil
}

func afmlFirstCoefficientT(y []float64, x [][]float64) (float64, error) {
	fit, err := Regress(y, x, false)
	if err != nil {
		return 0, err
	}
	df := len(y) - fit.Rank
	if df <= 0 {
		return 0, errors.New("statistics: structural regression has no residual degrees of freedom")
	}
	levels := make([]float64, len(y))
	for i := range x {
		levels[i] = x[i][0]
	}
	residualLevels := levels
	if len(x[0]) > 1 {
		nuisance := make([][]float64, len(x))
		for i := range x {
			nuisance[i] = x[i][1:]
		}
		aux, err := Regress(levels, nuisance, false)
		if err != nil {
			return 0, err
		}
		residualLevels = aux.Residuals
	}
	sse, ssx := 0., 0.
	for i := range y {
		sse += fit.Residuals[i] * fit.Residuals[i]
		ssx += residualLevels[i] * residualLevels[i]
	}
	se := math.Sqrt(sse / float64(df) / ssx)
	if !afmlFinite(se) || se <= 0 {
		return 0, errors.New("statistics: undefined structural regression standard error")
	}
	stat := fit.Beta[0] / se
	if !afmlFinite(stat) {
		return 0, errors.New("statistics: structural regression statistic overflow")
	}
	return stat, nil
}

// BackwardSupADFSeries evaluates the inner supremum at each causal endpoint.
// Prefix values match an isolated historical prefix. The result is a BSADF
// series; its maximum over endpoints is the generalized/global supremum, not
// an ordinary single-window ADF statistic. maxFits bounds all fits in the series.
func BackwardSupADFSeries(logValues []float64, minLength, lags int, constant string, maxFits int) ([]float64, error) {
	if maxFits < 1 || minLength < 1 || len(logValues) <= minLength {
		return nil, errors.New("statistics: insufficient backward ADF series")
	}
	remaining := maxFits
	for end := minLength; end < len(logValues); end++ {
		fits := end + 1 - minLength
		if fits > remaining {
			return nil, errors.New("statistics: backward ADF series exceeds fit budget")
		}
		remaining -= fits
	}
	out := make([]float64, len(logValues))
	for i := range out {
		out[i] = math.NaN()
	}
	for end := minLength; end < len(logValues); end++ {
		result, err := BackwardSupADF(logValues[:end+1], minLength, lags, constant, maxFits)
		if err != nil {
			return nil, err
		}
		out[end] = result.Statistic
	}
	return out, nil
}
