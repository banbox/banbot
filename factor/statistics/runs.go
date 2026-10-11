package statistics

import (
	"errors"
	"math"
)

// RunsReport describes the binary Wald-Wolfowitz statistic mentioned in
// AFML19.3.1. NormalPValue is an asymptotic two-sided approximation; the book
// supplies no feature window or significance calibration for trade signs.
type RunsReport struct {
	Positive, Negative, Runs                       int
	Expected, Variance, Z, NormalPValue            float64
	HasStandardizedStatistic, ContinuityCorrection bool
}

func WaldWolfowitzRuns(signs []int, continuityCorrection bool) (RunsReport, error) {
	r := RunsReport{ContinuityCorrection: continuityCorrection}
	if len(signs) == 0 || len(signs) > 1000000 {
		return r, errors.New("statistics: invalid runs sample size")
	}
	for i, sign := range signs {
		if sign != -1 && sign != 1 {
			return RunsReport{}, errors.New("statistics: runs require binary -1/+1 signs")
		}
		if sign > 0 {
			r.Positive++
		} else {
			r.Negative++
		}
		if i == 0 || signs[i-1] != sign {
			r.Runs++
		}
	}
	n := float64(len(signs))
	product := 2 * float64(r.Positive) * float64(r.Negative)
	r.Expected = 1 + product/n
	if n > 1 {
		r.Variance = product * (product - n) / (n * n * (n - 1))
	}
	if r.Variance > 0 {
		difference := float64(r.Runs) - r.Expected
		if continuityCorrection {
			difference = math.Copysign(math.Max(0, math.Abs(difference)-.5), difference)
		}
		r.Z = difference / math.Sqrt(r.Variance)
		r.NormalPValue = math.Erfc(math.Abs(r.Z) / math.Sqrt2)
		r.HasStandardizedStatistic = true
	}
	return r, nil
}
