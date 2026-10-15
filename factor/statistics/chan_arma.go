package statistics

import (
	"fmt"
	"math"
	"time"

	"gonum.org/v1/gonum/optimize"
)

// ARMA uses y_t = Intercept + sum(AR_i*y_{t-i}) +
// sum(MA_i*innovation_{t-i}) + innovation_t. Fit is conditional Gaussian MLE
// with zero presample innovations, distinct from vendor exact diffuse MLE.
type ARMA struct {
	Intercept               float64
	AR, MA                  []float64
	Variance, LogLikelihood float64
	Observations            int
}
type ARMAFitSpec struct {
	P, Q, MaxEvaluations, MaxIterations int
	Runtime                             time.Duration
}

// ARMAFilter retains only the most recent AR prices and MA innovations. The
// first P prices have zero conditioned innovations, matching FitARMA/Forecast.
type ARMAFilter struct {
	Model          ARMA
	prices, errors []float64
	count          int
}

func (m ARMA) Filter(values []float64) (*ARMAFilter, error) {
	filter := &ARMAFilter{Model: m}
	for _, v := range append(append([]float64{m.Intercept}, m.AR...), m.MA...) {
		if !finite(v) {
			return nil, fmt.Errorf("statistics: nonfinite ARMA coefficients")
		}
	}
	for _, v := range values {
		if err := filter.Observe(v); err != nil {
			return nil, err
		}
	}
	return filter, nil
}
func (f *ARMAFilter) Forecast() (float64, error) {
	if len(f.prices) < len(f.Model.AR) {
		return 0, fmt.Errorf("statistics: insufficient ARMA filter history")
	}
	v := f.Model.Intercept
	for j, c := range f.Model.AR {
		v += c * f.prices[len(f.prices)-j-1]
	}
	for j, c := range f.Model.MA {
		if len(f.errors) > j {
			v += c * f.errors[len(f.errors)-j-1]
		}
	}
	if !finite(v) {
		return 0, fmt.Errorf("statistics: ARMA filter overflow")
	}
	return v, nil
}
func (f *ARMAFilter) Observe(value float64) error {
	if !finite(value) {
		return fmt.Errorf("statistics: nonfinite ARMA filter observation")
	}
	innovation := 0.0
	if f.count >= len(f.Model.AR) {
		prediction, err := f.Forecast()
		if err != nil {
			return err
		}
		innovation = value - prediction
		if !finite(innovation) {
			return fmt.Errorf("statistics: ARMA innovation overflow")
		}
	}
	f.prices = append(f.prices, value)
	f.errors = append(f.errors, innovation)
	f.count++
	if len(f.prices) > len(f.Model.AR) {
		f.prices = f.prices[1:]
	}
	if len(f.errors) > len(f.Model.MA) {
		f.errors = f.errors[1:]
	}
	return nil
}

// Stable recursion maps unconstrained partial autocorrelations into a stable AR
// polynomial. Negation of this polynomial gives invertible MA coefficients.
func armaStable(parameters []float64) []float64 {
	var coefficients []float64
	for _, raw := range parameters {
		k := math.Tanh(raw)
		next := make([]float64, len(coefficients)+1)
		for j, v := range coefficients {
			next[j] = v - k*coefficients[len(coefficients)-1-j]
		}
		next[len(coefficients)] = k
		coefficients = next
	}
	return coefficients
}
func (m ARMA) innovations(values []float64) ([]float64, error) {
	if len(values) <= len(m.AR) || !finite(m.Intercept) {
		return nil, fmt.Errorf("statistics: insufficient ARMA history")
	}
	for _, v := range values {
		if !finite(v) {
			return nil, fmt.Errorf("statistics: nonfinite ARMA history")
		}
	}
	e := make([]float64, len(values))
	for t := len(m.AR); t < len(values); t++ {
		forecast := m.Intercept
		for j, v := range m.AR {
			forecast += v * values[t-j-1]
		}
		for j, v := range m.MA {
			if t > j {
				forecast += v * e[t-j-1]
			}
		}
		e[t] = values[t] - forecast
		if !finite(e[t]) {
			return nil, fmt.Errorf("statistics: ARMA innovation overflow")
		}
	}
	return e, nil
}
func (m ARMA) Forecast(values []float64) (float64, error) {
	e, err := m.innovations(values)
	if err != nil {
		return 0, err
	}
	if len(values) < len(m.MA) {
		return 0, fmt.Errorf("statistics: insufficient MA history")
	}
	forecast := m.Intercept
	for j, v := range m.AR {
		forecast += v * values[len(values)-j-1]
	}
	for j, v := range m.MA {
		forecast += v * e[len(e)-j-1]
	}
	if !finite(forecast) {
		return 0, fmt.Errorf("statistics: ARMA forecast overflow")
	}
	return forecast, nil
}
func FitARMA(values []float64, s ARMAFitSpec) (ARMA, error) { return fitARMA(values, s, s.P) }
func fitARMA(values []float64, s ARMAFitSpec, burn int) (ARMA, error) {
	if s.P < 0 || s.Q < 0 || s.P+s.Q == 0 || s.P > 20 || s.Q > 20 || burn < s.P || len(values)-burn <= s.P+s.Q+2 || s.MaxEvaluations < 1 || s.MaxIterations < 1 || s.Runtime <= 0 {
		return ARMA{}, fmt.Errorf("statistics: invalid ARMA fit specification")
	}
	mean, scale, err := MeanStd(values, 0)
	if err != nil {
		return ARMA{}, err
	}
	if scale <= 0 {
		return ARMA{}, fmt.Errorf("statistics: constant ARMA series")
	}
	data := make([]float64, len(values))
	for i, v := range values {
		data[i] = (v - mean) / scale
	}
	decode := func(parameters []float64) ARMA {
		m := ARMA{Intercept: parameters[0], AR: armaStable(parameters[1 : 1+s.P]), MA: armaStable(parameters[1+s.P:])}
		for i := range m.MA {
			m.MA[i] = -m.MA[i]
		}
		return m
	}
	n := len(values) - burn
	objective := func(parameters []float64) float64 {
		m := decode(parameters)
		e, err := m.innovations(data)
		if err != nil {
			return 1e100
		}
		sse := 0.0
		for _, v := range e[burn:] {
			sse += v * v
		}
		variance := sse / float64(n)
		if !finite(variance) || variance <= 0 {
			return 1e100
		}
		return float64(n) / 2 * (1 + math.Log(2*math.Pi*variance))
	}
	start := make([]float64, 1+s.P+s.Q)
	if s.P > 0 {
		fit, err := FitVAR(func() [][]float64 {
			rows := make([][]float64, len(data))
			for i, v := range data {
				rows[i] = []float64{v}
			}
			return rows
		}(), 1)
		if err == nil {
			phi := math.Max(-.95, math.Min(.95, fit.Equations[0].Beta[0]))
			start[1] = math.Atanh(phi)
			start[0] = fit.Equations[0].Intercept
		}
	}
	result, err := optimize.Minimize(optimize.Problem{Func: objective}, start, &optimize.Settings{FuncEvaluations: s.MaxEvaluations, MajorIterations: s.MaxIterations, Runtime: s.Runtime, Converger: &optimize.FunctionConverge{Absolute: 1e-9, Relative: 1e-9, Iterations: 40}}, &optimize.NelderMead{})
	if err != nil || result == nil || result.Status.Early() || !finite(result.F) || result.F >= 1e99 {
		return ARMA{}, fmt.Errorf("statistics: conditional ARMA optimizer did not converge: %v", err)
	}
	m := decode(result.X)
	sum := 0.0
	for _, v := range m.AR {
		sum += v
	}
	m.Intercept = mean*(1-sum) + scale*m.Intercept
	e, err := m.innovations(values)
	if err != nil {
		return ARMA{}, err
	}
	for _, v := range e[burn:] {
		m.Variance += v * v / float64(n)
	}
	m.Observations = n
	m.LogLikelihood = -float64(n) / 2 * (1 + math.Log(2*math.Pi*m.Variance))
	return m, nil
}

// SelectARMA compares positive P/Q orders using the same conditional burn-in.
func SelectARMA(values []float64, maxP, maxQ int, budget ARMAFitSpec) (ARMA, float64, error) {
	if maxP < 1 || maxQ < 1 || maxP > 6 || maxQ > 6 {
		return ARMA{}, 0, fmt.Errorf("statistics: ARMA order grid must be within 1..6")
	}
	bestBIC := math.Inf(1)
	var best ARMA
	for p := 1; p <= maxP; p++ {
		for q := 1; q <= maxQ; q++ {
			s := budget
			s.P, s.Q = p, q
			m, err := fitARMA(values, s, maxP)
			if err != nil {
				return ARMA{}, 0, err
			}
			bic := -2*m.LogLikelihood + float64(p+q+2)*math.Log(float64(m.Observations))
			if bic < bestBIC {
				best, bestBIC = m, bic
			}
		}
	}
	return best, bestBIC, nil
}
