package statistics

import (
	"fmt"
	"math"
	"slices"
	"time"

	"gonum.org/v1/gonum/optimize"
)

// GARCH follows MATLAB/Chan's order convention: P lags of conditional
// variance (source alpha), Q lags of squared zero-mean returns (source beta).
// Presample variances and squared returns equal InitialVariance, a declared
// training-sample backcast. This is conditional Gaussian MLE, not vendor parity.
type GARCH struct {
	Omega, InitialVariance, LogLikelihood    float64
	ReturnCoefficients, VarianceCoefficients []float64
	Observations                             int
}

type GARCHFitSpec struct {
	P, Q, MaxEvaluations, MaxIterations int
	Runtime                             time.Duration
}

type GARCHFilter struct {
	model              GARCH
	squares, variances []float64
}

func (m GARCH) Filter() (*GARCHFilter, error) {
	if !finite(m.Omega) || m.Omega <= 0 || !finite(m.InitialVariance) || m.InitialVariance <= 0 || len(m.ReturnCoefficients)+len(m.VarianceCoefficients) == 0 || len(m.ReturnCoefficients) > 10 || len(m.VarianceCoefficients) > 10 {
		return nil, fmt.Errorf("statistics: invalid GARCH model")
	}
	sum := 0.
	for _, coefficients := range [][]float64{m.ReturnCoefficients, m.VarianceCoefficients} {
		for _, v := range coefficients {
			if !finite(v) || v < 0 {
				return nil, fmt.Errorf("statistics: invalid GARCH coefficients")
			}
			sum += v
		}
	}
	if sum >= 1 {
		return nil, fmt.Errorf("statistics: nonstationary GARCH model")
	}
	m.ReturnCoefficients, m.VarianceCoefficients = slices.Clone(m.ReturnCoefficients), slices.Clone(m.VarianceCoefficients)
	f := &GARCHFilter{model: m, squares: make([]float64, len(m.ReturnCoefficients)), variances: make([]float64, len(m.VarianceCoefficients))}
	for i := range f.squares {
		f.squares[i] = m.InitialVariance
	}
	for i := range f.variances {
		f.variances[i] = m.InitialVariance
	}
	return f, nil
}

// Forecast returns h_(t+1) after observing return t, before seeing t+1.
func (f *GARCHFilter) Forecast() (float64, error) {
	if f == nil {
		return 0, fmt.Errorf("statistics: nil GARCH filter")
	}
	h := f.model.Omega
	for j, a := range f.model.ReturnCoefficients {
		h += a * f.squares[len(f.squares)-j-1]
	}
	for j, b := range f.model.VarianceCoefficients {
		h += b * f.variances[len(f.variances)-j-1]
	}
	if !finite(h) || h <= 0 {
		return 0, fmt.Errorf("statistics: GARCH variance overflow")
	}
	return h, nil
}

func (f *GARCHFilter) Observe(r float64) error {
	if !finite(r) || !finite(r*r) {
		return fmt.Errorf("statistics: invalid GARCH return")
	}
	h, err := f.Forecast()
	if err != nil {
		return err
	}
	if len(f.squares) > 0 {
		copy(f.squares, f.squares[1:])
		f.squares[len(f.squares)-1] = r * r
	}
	if len(f.variances) > 0 {
		copy(f.variances, f.variances[1:])
		f.variances[len(f.variances)-1] = h
	}
	return nil
}

func (m GARCH) ConditionalVariances(returns []float64) ([]float64, float64, error) {
	f, err := m.Filter()
	if err != nil {
		return nil, 0, err
	}
	values := make([]float64, len(returns))
	ll := 0.
	for i, r := range returns {
		h, err := f.Forecast()
		if err != nil {
			return nil, 0, err
		}
		values[i] = h
		if err = f.Observe(r); err != nil {
			return nil, 0, err
		}
		ll -= .5 * (math.Log(2*math.Pi*h) + r*r/h)
	}
	if !finite(ll) {
		return nil, 0, fmt.Errorf("statistics: GARCH likelihood overflow")
	}
	return values, ll, nil
}

func FitGARCH(returns []float64, s GARCHFitSpec) (GARCH, error) {
	if s.P < 0 || s.Q < 1 || s.P > 10 || s.Q > 10 || len(returns) < max(20, 5*(s.P+s.Q+1)) || len(returns) > 1000000 || s.MaxEvaluations < 1 || s.MaxIterations < 1 || s.Runtime <= 0 {
		return GARCH{}, fmt.Errorf("statistics: invalid GARCH fit budget/order")
	}
	variance := 0.
	for _, r := range returns {
		if !finite(r) || !finite(r*r) {
			return GARCH{}, fmt.Errorf("statistics: nonfinite GARCH data")
		}
		variance += r * r / float64(len(returns))
	}
	if !finite(variance) || variance <= 0 {
		return GARCH{}, fmt.Errorf("statistics: zero GARCH return scale")
	}
	data := make([]float64, len(returns))
	scale := math.Sqrt(variance)
	for i, r := range returns {
		data[i] = r / scale
	}
	// A softmax with a fixed zero-logit slack gives positive coefficients
	// whose sum is strictly below one. Omega is positive independently.
	decode := func(x []float64) GARCH {
		m := GARCH{Omega: math.Exp(x[0]), InitialVariance: 1, ReturnCoefficients: make([]float64, s.Q), VarianceCoefficients: make([]float64, s.P)}
		largest := 0.
		for _, v := range x[1:] {
			largest = math.Max(largest, v)
		}
		denominator := math.Exp(-largest)
		for _, v := range x[1:] {
			denominator += math.Exp(v - largest)
		}
		for j := range m.ReturnCoefficients {
			m.ReturnCoefficients[j] = math.Exp(x[1+j]-largest) / denominator
		}
		for j := range m.VarianceCoefficients {
			m.VarianceCoefficients[j] = math.Exp(x[1+s.Q+j]-largest) / denominator
		}
		return m
	}
	objective := func(x []float64) float64 {
		_, ll, err := decode(x).ConditionalVariances(data)
		if err != nil {
			return 1e100
		}
		return -ll
	}
	start := make([]float64, 1+s.P+s.Q)
	start[0] = math.Log(.1)
	for j := 0; j < s.Q; j++ {
		start[1+j] = math.Log(.1 / float64(s.Q) / .1)
	}
	for j := 0; j < s.P; j++ {
		start[1+s.Q+j] = math.Log(.8 / float64(s.P) / .1)
	}
	result, err := optimize.Minimize(optimize.Problem{Func: objective}, start, &optimize.Settings{FuncEvaluations: s.MaxEvaluations, MajorIterations: s.MaxIterations, Runtime: s.Runtime, Converger: &optimize.FunctionConverge{Absolute: 1e-8, Relative: 1e-9, Iterations: 40}}, &optimize.NelderMead{})
	if err != nil || result == nil || result.Status.Early() || !finite(result.F) || result.F >= 1e99 {
		return GARCH{}, fmt.Errorf("statistics: GARCH optimizer did not converge: %v", err)
	}
	m := decode(result.X)
	m.Omega *= variance
	m.InitialVariance = variance
	m.Observations = len(returns)
	_, m.LogLikelihood, err = m.ConditionalVariances(returns)
	if err != nil {
		return GARCH{}, err
	}
	return m, nil
}

type GARCHCandidate struct {
	P, Q  int
	BIC   float64
	Error string
}

// SelectGARCH records failures as in the source try/catch loop; only genuinely
// converged candidates enter BIC, with the same sample/backcast convention.
func SelectGARCH(returns []float64, maxP, maxQ int, budget GARCHFitSpec) (GARCH, []GARCHCandidate, error) {
	if maxP < 1 || maxQ < 1 || maxP > 10 || maxQ > 9 || budget.Runtime <= 0 {
		return GARCH{}, nil, fmt.Errorf("statistics: GARCH selection grid must fit source10x9 and declare total runtime")
	}
	deadline := time.Now().Add(budget.Runtime)
	best := GARCH{}
	bestBIC := math.Inf(1)
	audit := []GARCHCandidate{}
	for p := 1; p <= maxP; p++ {
		for q := 1; q <= maxQ; q++ {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return GARCH{}, audit, fmt.Errorf("statistics: GARCH order selection total runtime exhausted")
			}
			budget.Runtime = remaining
			budget.P, budget.Q = p, q
			m, err := FitGARCH(returns, budget)
			row := GARCHCandidate{P: p, Q: q}
			if err != nil {
				row.Error = err.Error()
			} else {
				row.BIC = -2*m.LogLikelihood + float64(p+q+1)*math.Log(float64(m.Observations))
				if row.BIC < bestBIC {
					best, bestBIC = m, row.BIC
				}
			}
			audit = append(audit, row)
		}
	}
	if best.Observations == 0 {
		return GARCH{}, audit, fmt.Errorf("statistics: no converged GARCH candidate")
	}
	return best, audit, nil
}
