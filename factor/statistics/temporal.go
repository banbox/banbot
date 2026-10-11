package statistics

import (
	"errors"
	"math"
)

// ARModel is conditional least-squares AR(p), with lag1 first. This is not
// ARMA maximum likelihood: the estimation method remains explicit.
type ARModel struct {
	Intercept    float64
	Coefficients []float64
}

func FitAR(values []float64, lags int) (ARModel, error) {
	if lags < 1 || len(values) < 2*lags+2 {
		return ARModel{}, errors.New("statistics: insufficient AR training")
	}
	x := make([][]float64, len(values)-lags)
	y := append([]float64(nil), values[lags:]...)
	for i := range x {
		x[i] = make([]float64, lags)
		for lag := range lags {
			x[i][lag] = values[i+lags-lag-1]
		}
	}
	f, err := Regress(y, x, true)
	if err != nil {
		return ARModel{}, err
	}
	return ARModel{f.Intercept, f.Beta}, nil
}
func (m ARModel) Forecast(history []float64) (float64, error) {
	if len(m.Coefficients) == 0 || len(history) < len(m.Coefficients) || !finite(m.Intercept) {
		return 0, errors.New("statistics: invalid AR forecast")
	}
	y := m.Intercept
	for i, coef := range m.Coefficients {
		x := history[len(history)-i-1]
		if !finite(coef) || !finite(x) {
			return 0, errors.New("statistics: nonfinite AR state")
		}
		y += coef * x
	}
	if !finite(y) {
		return 0, errors.New("statistics: AR overflow")
	}
	return y, nil
}

// LocalLevel is the scalar random-walk Kalman model of Vidyamurthy ch4.
// With Q=R, gain converges to (sqrt(5)-1)/2, producing Fibonacci weights.
type LocalLevel struct{ Mean, Variance, ProcessVariance, ObservationVariance float64 }
type LevelObservation struct{ Prediction, PredictionVariance, Innovation, Gain float64 }

func (k *LocalLevel) Observe(value float64) (LevelObservation, error) {
	if !finite(value) || !finite(k.Mean) || !finite(k.Variance) || !finite(k.ProcessVariance) || !finite(k.ObservationVariance) || k.Variance < 0 || k.ProcessVariance < 0 || k.ObservationVariance <= 0 {
		return LevelObservation{}, errors.New("statistics: invalid local-level state")
	}
	p := k.Variance + k.ProcessVariance
	s := p + k.ObservationVariance
	e := value - k.Mean
	gain := p / s
	next := k.Mean + gain*e
	v := (1-gain)*(1-gain)*p + gain*gain*k.ObservationVariance
	if !finite(s) || !finite(e) || !finite(next) || !finite(v) {
		return LevelObservation{}, errors.New("statistics: local-level overflow")
	}
	out := LevelObservation{k.Mean, p, e, gain}
	k.Mean, k.Variance = next, v
	return out, nil
}

// Cuscore accumulates sum((y_t-beta_t*t)*t) (Pole ch11), with one-based t.
// Baseline drift is supplied causally by the caller, never fitted retrospectively
// on the same trend used to demonstrate detection.
type Cuscore struct {
	Time  int
	Score float64
}

func (q *Cuscore) Observe(value, drift float64) (float64, error) {
	if !finite(value) || !finite(drift) || !finite(q.Score) || q.Time < 0 || q.Time == math.MaxInt {
		return 0, errors.New("statistics: invalid Cuscore state")
	}
	t := q.Time + 1
	score := q.Score + (value-drift*float64(t))*float64(t)
	if !finite(score) {
		return 0, errors.New("statistics: Cuscore overflow")
	}
	q.Time, q.Score = t, score
	return score, nil
}

// ARCH1 predicts variance BEFORE observing the next innovation (Pole ch3).
func ARCH1(intercept, coefficient, previousError float64) (float64, error) {
	if !finite(intercept) || !finite(coefficient) || !finite(previousError) || intercept <= 0 || coefficient < 0 || coefficient >= 1 {
		return 0, errors.New("statistics: invalid stationary ARCH(1)")
	}
	v := intercept + coefficient*previousError*previousError
	if !finite(v) {
		return 0, errors.New("statistics: ARCH overflow")
	}
	return v, nil
}
