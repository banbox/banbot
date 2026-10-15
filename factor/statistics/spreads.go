package statistics

import (
	"errors"
	"math"
	"sort"
)

// GaussianThreshold maximizes delta*(1-Phi(delta/sigma)) (Vidyamurthy ch8).
// Its root is ~0.75179 sigma, rather than an arbitrary two-sigma entry.
func GaussianThreshold(sigma float64) (float64, error) {
	if !finite(sigma) || sigma <= 0 {
		return 0, errors.New("statistics: positive Gaussian scale required")
	}
	lo, hi := 0.0, 4.0
	for range 80 {
		u := (lo + hi) / 2
		tail := .5 * math.Erfc(u/math.Sqrt2)
		density := math.Exp(-u*u/2) / math.Sqrt(2*math.Pi)
		if tail > u*density {
			lo = u
		} else {
			hi = u
		}
	}
	return sigma * (lo + hi) / 2, nil
}

// SmoothThresholdCounts minimizes ||z-counts||^2+lambda*sum(diff(z)^2).
// Lambda is explicit: selecting the source's L-curve heel is a separate
// calibration decision. The tridiagonal system avoids dense matrix inversion.
func SmoothThresholdCounts(counts []float64, lambda float64) ([]float64, error) {
	if len(counts) == 0 || !finite(lambda) || lambda < 0 {
		return nil, errors.New("statistics: invalid threshold regularization")
	}
	n := len(counts)
	diagonal := make([]float64, n)
	rhs := append([]float64(nil), counts...)
	for i, x := range counts {
		if !finite(x) || x < 0 {
			return nil, errors.New("statistics: invalid exceedance counts")
		}
		diagonal[i] = 1
		if i > 0 {
			diagonal[i] += lambda
		}
		if i < n-1 {
			diagonal[i] += lambda
		}
		if !finite(diagonal[i]) {
			return nil, errors.New("statistics: regularization diagonal overflow")
		}
	}
	for i := 1; i < n; i++ {
		ratio := -lambda / diagonal[i-1]
		diagonal[i] -= ratio * (-lambda)
		rhs[i] -= ratio * rhs[i-1]
		if !finite(diagonal[i]) || diagonal[i] <= 0 || !finite(rhs[i]) {
			return nil, errors.New("statistics: unstable regularization elimination")
		}
	}
	z := make([]float64, n)
	z[n-1] = rhs[n-1] / diagonal[n-1]
	for i := n - 2; i >= 0; i-- {
		z[i] = (rhs[i] + lambda*z[i+1]) / diagonal[i]
	}
	for _, x := range z {
		if !finite(x) {
			return nil, errors.New("statistics: regularization overflow")
		}
	}
	return z, nil
}

type ThresholdFit struct {
	Threshold, Objective float64
	Counts, Smoothed     []float64
}

// EmpiricalThreshold estimates two-sided excursion count by averaging the
// upper/lower tails. Input spreads must already be centered using training
// observations. Grid spacing, costs and lambda remain auditable parameters.
func EmpiricalThreshold(spreads, grid []float64, lambda float64) (ThresholdFit, error) {
	if len(spreads) == 0 || len(grid) == 0 {
		return ThresholdFit{}, errors.New("statistics: empty threshold sample")
	}
	counts := make([]float64, len(grid))
	for _, x := range spreads {
		if !finite(x) {
			return ThresholdFit{}, errors.New("statistics: nonfinite spread")
		}
	}
	for i, threshold := range grid {
		if !finite(threshold) || threshold < 0 || (i > 0 && threshold <= grid[i-1]) {
			return ThresholdFit{}, errors.New("statistics: threshold grid must increase")
		}
		for _, x := range spreads {
			if x > threshold || x < -threshold {
				counts[i] += .5
			}
		}
	}
	z, err := SmoothThresholdCounts(counts, lambda)
	if err != nil {
		return ThresholdFit{}, err
	}
	out := ThresholdFit{Threshold: grid[0], Counts: counts, Smoothed: z}
	for i, threshold := range grid {
		value := threshold * z[i]
		if !finite(value) {
			return ThresholdFit{}, errors.New("statistics: threshold objective overflow")
		}
		if value > out.Objective {
			out.Threshold, out.Objective = threshold, value
		}
	}
	return out, nil
}

// CrossingDurations includes completed intervals between successive crossings
// of a fixed training equilibrium. Leading/trailing censored intervals are
// excluded. Zeros are skipped: a crossing is confirmed only when an opposite
// nonzero sign is observed, so a touch-and-retreat is never a crossing.
func CrossingDurations(spreads []float64) ([]int, error) {
	lastCross := -1
	lastSign := 0
	var durations []int
	for i, x := range spreads {
		if !finite(x) {
			return nil, errors.New("statistics: nonfinite crossing sample")
		}
		sign := 0
		if x > 0 {
			sign = 1
		} else if x < 0 {
			sign = -1
		}
		if sign == 0 {
			continue
		}
		cross := lastSign != 0 && sign != lastSign
		lastSign = sign
		if cross {
			if lastCross >= 0 {
				durations = append(durations, i-lastCross)
			}
			lastCross = i
		}
	}
	return durations, nil
}

// DurationQuantile is an empirical holding-horizon diagnostic. It does not
// pretend that a finite sample establishes a future mean crossing deadline.
func DurationQuantile(durations []int, q float64) (float64, error) {
	if len(durations) == 0 || !finite(q) || q < 0 || q > 1 {
		return 0, errors.New("statistics: invalid duration quantile")
	}
	x := append([]int(nil), durations...)
	for _, v := range x {
		if v <= 0 {
			return 0, errors.New("statistics: nonpositive crossing interval")
		}
	}
	sort.Ints(x)
	p := q * float64(len(x)-1)
	i := int(p)
	j := min(i+1, len(x)-1)
	return float64(x[i]) + (p-float64(i))*float64(x[j]-x[i]), nil
}
