// Package statistics contains numerical research tools. Callers own training
// windows and publication times; none of these functions reads future records.
package statistics

import (
	"errors"
	"math"

	"github.com/banbox/banbot/factor"
)

func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

// MeanStd returns the arithmetic mean and standard deviation, with explicit
// degrees of freedom. Constant samples return zero scale, never a fake z-score.
func MeanStd(values []float64, ddof int) (mean, std float64, err error) {
	if ddof < 0 || len(values) <= ddof || len(values) == 0 {
		return 0, 0, errors.New("statistics: insufficient sample")
	}
	var m2 float64
	for i, x := range values {
		if !finite(x) {
			return 0, 0, errors.New("statistics: nonfinite sample")
		}
		delta := x - mean
		mean += delta / float64(i+1)
		m2 += delta * (x - mean)
	}
	std = math.Sqrt(math.Max(0, m2) / float64(len(values)-ddof))
	if !finite(mean) || !finite(std) {
		return 0, 0, errors.New("statistics: sample overflow")
	}
	return
}

type Regression struct {
	Intercept float64
	Beta      []float64
	Residuals []float64
	Rank      int
}

// Regress fits y on rows of exposures. Intercept is explicit: Chan's GLD/GDX
// example uses no intercept while the OU drift regression does include one.
func Regress(y []float64, exposures [][]float64, intercept bool) (Regression, error) {
	if len(y) == 0 || len(exposures) != len(y) || len(exposures[0]) == 0 {
		return Regression{}, errors.New("statistics: invalid regression dimensions")
	}
	p := len(exposures[0])
	x := make([][]float64, len(y))
	for i, row := range exposures {
		if len(row) != p {
			return Regression{}, errors.New("statistics: ragged exposures")
		}
		if intercept {
			x[i] = append([]float64{1}, row...)
		} else {
			x[i] = append([]float64(nil), row...)
		}
	}
	coef, rank, err := factor.LeastSquares(x, y, nil)
	if err != nil {
		return Regression{}, err
	}
	if rank != len(coef) || len(y) < len(coef) {
		return Regression{}, errors.New("statistics: unidentified regression")
	}
	out := Regression{Rank: rank, Beta: append([]float64(nil), coef...), Residuals: make([]float64, len(y))}
	if intercept {
		out.Intercept, out.Beta = coef[0], append([]float64(nil), coef[1:]...)
	}
	for i, row := range exposures {
		fit := out.Intercept
		for j, value := range row {
			fit += out.Beta[j] * value
		}
		out.Residuals[i] = y[i] - fit
		if !finite(out.Residuals[i]) {
			return Regression{}, errors.New("statistics: residual overflow")
		}
	}
	return out, nil
}

type OU struct {
	Intercept, Slope, Equilibrium, InnovationStd float64
	// LinearHalfLife is -log(2)/b from the book's Euler drift regression.
	// DiscreteHalfLife is -log(2)/log(1+b), valid only for 0<1+b<1.
	LinearHalfLife, DiscreteHalfLife float64
}

// FitOU fits x[t]-x[t-1] = a + b*x[t-1] + error. It rejects non-reverting
// or oscillatory AR(1) samples rather than assigning a useful-looking half-life.
func FitOU(values []float64) (OU, error) {
	if len(values) < 4 {
		return OU{}, errors.New("statistics: OU needs four observations")
	}
	x, y := make([][]float64, len(values)-1), make([]float64, len(values)-1)
	for i := range y {
		x[i] = []float64{values[i]}
		y[i] = values[i+1] - values[i]
	}
	fit, err := Regress(y, x, true)
	if err != nil {
		return OU{}, err
	}
	b := fit.Beta[0]
	if b >= 0 || b <= -1 {
		return OU{}, errors.New("statistics: non-reverting OU sample")
	}
	_, std, err := MeanStd(fit.Residuals, 2)
	if err != nil {
		return OU{}, err
	}
	return OU{fit.Intercept, b, -fit.Intercept / b, std, -math.Ln2 / b, -math.Ln2 / math.Log1p(b)}, nil
}

type KalmanPair struct {
	Beta                                 [2]float64 // price coefficient, intercept
	Covariance                           [2][2]float64
	ProcessVariance, ObservationVariance float64
	// ProcessCovariance supports correlated beta/offset innovations. Supply
	// this matrix or scalar ProcessVariance, never both.
	ProcessCovariance [2][2]float64
}
type KalmanObservation struct {
	Prediction, Error, Variance float64
	PriorBeta, PosteriorBeta    [2]float64
}

// Observe performs a random-walk beta filter with H=[x,1]. Error and Variance
// are the PRE-update innovation used by Chan's +/-sqrt(S) trading thresholds.
// The Joseph covariance update preserves symmetry. Noise variances must be
// estimated or declared by the caller; this method does not pretend to fit MLE.
func (k *KalmanPair) Observe(x, y float64) (KalmanObservation, error) {
	if !finite(x) || !finite(y) || !finite(k.ProcessVariance) || !finite(k.ObservationVariance) || k.ProcessVariance < 0 || k.ObservationVariance <= 0 {
		return KalmanObservation{}, errors.New("statistics: invalid Kalman observation/noise")
	}
	p := k.Covariance
	for i := range 2 {
		if !finite(k.Beta[i]) {
			return KalmanObservation{}, errors.New("statistics: nonfinite Kalman state")
		}
		for j := range 2 {
			if !finite(p[i][j]) {
				return KalmanObservation{}, errors.New("statistics: nonfinite covariance")
			}
		}
	}
	if !pairPSD(p) {
		return KalmanObservation{}, errors.New("statistics: covariance is not positive semidefinite")
	}
	q := k.ProcessCovariance
	if !pairPSD(q) || (q != [2][2]float64{} && k.ProcessVariance != 0) {
		return KalmanObservation{}, errors.New("statistics: invalid or ambiguous process covariance")
	}
	if q == [2][2]float64{} {
		q[0][0] = k.ProcessVariance
		q[1][1] = k.ProcessVariance
	}
	for i := range 2 {
		for j := range 2 {
			p[i][j] += q[i][j]
		}
	}
	h := [2]float64{x, 1}
	ph := [2]float64{p[0][0]*x + p[0][1], p[1][0]*x + p[1][1]}
	s := h[0]*ph[0] + ph[1] + k.ObservationVariance
	prediction := x*k.Beta[0] + k.Beta[1]
	e := y - prediction
	if !finite(s) || s <= 0 || !finite(e) {
		return KalmanObservation{}, errors.New("statistics: innovation overflow")
	}
	gain := [2]float64{ph[0] / s, ph[1] / s}
	updated := [2]float64{k.Beta[0] + gain[0]*e, k.Beta[1] + gain[1]*e}
	a := [2][2]float64{{1 - gain[0]*x, -gain[0]}, {-gain[1] * x, 1 - gain[1]}}
	var covariance [2][2]float64
	for i := range 2 {
		for j := range 2 {
			for m := range 2 {
				for n := range 2 {
					covariance[i][j] += a[i][m] * p[m][n] * a[j][n]
				}
			}
			covariance[i][j] += gain[i] * gain[j] * k.ObservationVariance
			if !finite(covariance[i][j]) || !finite(updated[i]) {
				return KalmanObservation{}, errors.New("statistics: update overflow")
			}
		}
	}
	covariance[0][1] = (covariance[0][1] + covariance[1][0]) / 2
	covariance[1][0] = covariance[0][1]
	out := KalmanObservation{prediction, e, s, k.Beta, updated}
	k.Beta, k.Covariance = updated, covariance
	return out, nil
}

func pairPSD(p [2][2]float64) bool {
	for i := range 2 {
		for j := range 2 {
			if !finite(p[i][j]) {
				return false
			}
		}
	}
	if p[0][0] < 0 || p[1][1] < 0 || math.Abs(p[0][1]-p[1][0]) > 1e-12*math.Max(1, math.Max(math.Abs(p[0][1]), math.Abs(p[1][0]))) {
		return false
	}
	if p[0][0] == 0 || p[1][1] == 0 {
		return p[0][1] == 0 && p[1][0] == 0
	}
	return math.Abs(p[0][1]/math.Sqrt(p[0][0])/math.Sqrt(p[1][1])) <= 1+1e-12
}
