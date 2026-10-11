package statistics

import (
	"fmt"
	"math"

	"gonum.org/v1/gonum/mat"
)

// VAR predicts one vector from its most recent Lags vectors, with a fitted
// intercept and unrestricted cross-series coefficients. Rows are chronological.
type VAR struct {
	Lags         int
	Equations    []Regression
	Covariance   [][]float64
	Observations int
}

func FitVAR(rows [][]float64, lags int) (VAR, error) {
	if lags < 1 || len(rows) <= lags || len(rows[0]) == 0 {
		return VAR{}, fmt.Errorf("statistics: invalid VAR dimensions")
	}
	d := len(rows[0])
	n := len(rows) - lags
	if n < d*lags+1 {
		return VAR{}, fmt.Errorf("statistics: insufficient VAR observations")
	}
	for _, row := range rows {
		if len(row) != d {
			return VAR{}, fmt.Errorf("statistics: ragged VAR rows")
		}
		for _, v := range row {
			if !finite(v) {
				return VAR{}, fmt.Errorf("statistics: nonfinite VAR input")
			}
		}
	}
	x := make([][]float64, n)
	for i := range x {
		for lag := 1; lag <= lags; lag++ {
			x[i] = append(x[i], rows[i+lags-lag]...)
		}
	}
	out := VAR{Lags: lags, Equations: make([]Regression, d), Covariance: make([][]float64, d), Observations: n}
	for j := 0; j < d; j++ {
		y := make([]float64, n)
		for i := range y {
			y[i] = rows[i+lags][j]
		}
		fit, err := Regress(y, x, true)
		if err != nil {
			return VAR{}, err
		}
		out.Equations[j] = fit
	}
	// Gaussian maximum-likelihood covariance uses N, not residual degrees of freedom.
	for j := 0; j < d; j++ {
		out.Covariance[j] = make([]float64, d)
		for k := 0; k < d; k++ {
			for i := 0; i < n; i++ {
				out.Covariance[j][k] += out.Equations[j].Residuals[i] * out.Equations[k].Residuals[i] / float64(n)
			}
		}
	}
	return out, nil
}

func (v VAR) Forecast(history [][]float64) ([]float64, error) {
	d := len(v.Equations)
	if v.Lags < 1 || d == 0 || len(history) < v.Lags {
		return nil, fmt.Errorf("statistics: insufficient VAR forecast history")
	}
	x := make([]float64, 0, d*v.Lags)
	for lag := 1; lag <= v.Lags; lag++ {
		row := history[len(history)-lag]
		if len(row) != d {
			return nil, fmt.Errorf("statistics: VAR forecast dimension mismatch")
		}
		for _, value := range row {
			if !finite(value) {
				return nil, fmt.Errorf("statistics: nonfinite VAR forecast input")
			}
		}
		x = append(x, row...)
	}
	out := make([]float64, d)
	for j, eq := range v.Equations {
		if len(eq.Beta) != len(x) {
			return nil, fmt.Errorf("statistics: invalid VAR coefficients")
		}
		out[j] = eq.Intercept
		for k, value := range x {
			out[j] += eq.Beta[k] * value
		}
		if !finite(out[j]) {
			return nil, fmt.Errorf("statistics: VAR forecast overflow")
		}
	}
	return out, nil
}

// SelectVAR compares Gaussian BIC on a common label window across lag orders.
// The parameter count includes the unrestricted innovation covariance.
func SelectVAR(rows [][]float64, maxLag int) (VAR, float64, error) {
	if maxLag < 1 || len(rows) <= maxLag || len(rows[0]) == 0 {
		return VAR{}, 0, fmt.Errorf("statistics: invalid VAR selection dimensions")
	}
	bestBIC := math.Inf(1)
	var best VAR
	for lag := 1; lag <= maxLag; lag++ {
		candidate, err := FitVAR(rows[maxLag-lag:], lag)
		if err != nil {
			return VAR{}, 0, err
		}
		d := len(candidate.Equations)
		cov := mat.NewSymDense(d, nil)
		for j := 0; j < d; j++ {
			for k := 0; k <= j; k++ {
				cov.SetSym(j, k, candidate.Covariance[j][k])
			}
		}
		var chol mat.Cholesky
		if !chol.Factorize(cov) {
			return VAR{}, 0, fmt.Errorf("statistics: singular VAR innovation covariance")
		}
		n := float64(candidate.Observations)
		parameters := d*(d*lag+1) + d*(d+1)/2
		bic := n*(float64(d)*(1+math.Log(2*math.Pi))+chol.LogDet()) + float64(parameters)*math.Log(n)
		if !finite(bic) {
			return VAR{}, 0, fmt.Errorf("statistics: nonfinite VAR BIC")
		}
		if bic < bestBIC {
			best, bestBIC = candidate, bic
		}
	}
	return best, bestBIC, nil
}
