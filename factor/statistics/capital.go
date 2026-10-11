package statistics

import (
	"errors"
	"math"
)

// KellyWeights implements F=C^-1 M from Chan ch6 on SIMPLE excess returns.
// Fraction=0.5 gives half Kelly; a positive gross cap scales all legs together.
// Singular covariance is rejected rather than silently selecting a leveraged
// basic solution. This Gaussian growth approximation does not guarantee safety.
func KellyWeights(returns [][]float64, fraction, grossCap float64) ([]float64, error) {
	if len(returns) < 2 || len(returns[0]) == 0 || !finite(fraction) || fraction <= 0 || fraction > 1 || !finite(grossCap) || grossCap < 0 {
		return nil, errors.New("statistics: invalid Kelly training/allocation")
	}
	p := len(returns[0])
	mean := make([]float64, p)
	cov := make([][]float64, p)
	for i := range cov {
		cov[i] = make([]float64, p)
	}
	for i, row := range returns {
		if len(row) != p {
			return nil, errors.New("statistics: ragged Kelly returns")
		}
		for j, x := range row {
			if !finite(x) {
				return nil, errors.New("statistics: nonfinite Kelly training")
			}
			mean[j] += (x - mean[j]) / float64(i+1)
		}
	}
	for _, row := range returns {
		for i := range p {
			for j := range p {
				cov[i][j] += (row[i] - mean[i]) * (row[j] - mean[j]) / float64(len(returns)-1)
			}
		}
	}
	fit, err := Regress(mean, cov, false)
	if err != nil {
		return nil, err
	}
	w := append([]float64(nil), fit.Beta...)
	gross := 0.0
	for i := range w {
		w[i] *= fraction
		gross += math.Abs(w[i])
	}
	if !finite(gross) {
		return nil, errors.New("statistics: Kelly allocation overflow")
	}
	if grossCap > 0 && gross > grossCap {
		for i := range w {
			w[i] *= grossCap / gross
		}
	}
	return w, nil
}

type DrawdownReport struct {
	Standard, Recovery     float64
	Duration               int
	PeakIndex, TroughIndex int
}

// Drawdowns distinguishes 1-equity/HWM from the book's HWM/equity-1
// loss-to-recovery convention. Duration is the longest unrecovered bar count.
func Drawdowns(equity []float64) (DrawdownReport, error) {
	if len(equity) == 0 {
		return DrawdownReport{}, errors.New("statistics: empty equity")
	}
	r := DrawdownReport{}
	peak := equity[0]
	peakIndex, age := 0, 0
	for i, v := range equity {
		if !finite(v) || v <= 0 {
			return DrawdownReport{}, errors.New("statistics: positive finite equity required")
		}
		if v >= peak {
			peak, peakIndex, age = v, i, 0
		} else {
			age++
		}
		dd := 1 - v/peak
		if dd > r.Standard {
			r.Standard = dd
			r.Recovery = peak/v - 1
			r.PeakIndex, r.TroughIndex = peakIndex, i
		}
		r.Duration = max(r.Duration, age)
	}
	return r, nil
}
