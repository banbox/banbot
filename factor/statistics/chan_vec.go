package statistics

import "fmt"

// ForecastVEC0 rewrites unrestricted VAR(1) as delta y=c+(A-I)y.
// This is algebraic equivalence, not a separately fitted rank-constrained VECM.
func (m VAR) ForecastVEC0(previous []float64) ([]float64, error) {
	if m.Lags != 1 {
		return nil, fmt.Errorf("statistics: VEC0 requires VAR1")
	}
	// Validate the fitted dimensions/finite inputs with the usual VAR boundary.
	if _, err := m.Forecast([][]float64{previous}); err != nil {
		return nil, err
	}
	out := make([]float64, len(previous))
	for j, equation := range m.Equations {
		delta := equation.Intercept
		for k, value := range previous {
			coefficient := equation.Beta[k]
			if j == k {
				coefficient--
			}
			delta += coefficient * value
		}
		out[j] = previous[j] + delta
		if !finite(out[j]) {
			return nil, fmt.Errorf("statistics: VEC0 overflow")
		}
	}
	return out, nil
}
