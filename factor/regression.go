package factor

import (
	"errors"
	"math"
)

// LeastSquares uses twice-reorthogonalized QR, dropping dependent columns in
// declaration order. It returns a deterministic basic solution for singular
// exposures instead of inverting unstable normal equations. Callers supply an
// intercept column if required; weights must be strictly positive.
func LeastSquares(x [][]float64, y, weights []float64) ([]float64, int, error) {
	if len(x) == 0 || len(x) != len(y) || len(x[0]) == 0 || (weights != nil && len(weights) != len(y)) {
		return nil, 0, errors.New("factor: invalid regression dimensions")
	}
	n, p := len(x), len(x[0])
	columns := make([][]float64, p)
	response := make([]float64, n)
	for j := range columns {
		columns[j] = make([]float64, n)
	}
	for i, row := range x {
		if len(row) != p || math.IsNaN(y[i]) || math.IsInf(y[i], 0) {
			return nil, 0, errors.New("factor: invalid regression row")
		}
		scale := 1.0
		if weights != nil {
			if weights[i] <= 0 || math.IsNaN(weights[i]) || math.IsInf(weights[i], 0) {
				return nil, 0, errors.New("factor: invalid regression weight")
			}
			scale = math.Sqrt(weights[i])
		}
		response[i] = y[i] * scale
		if math.IsNaN(response[i]) || math.IsInf(response[i], 0) {
			return nil, 0, errors.New("factor: weighted response overflow")
		}
		for j, value := range row {
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, 0, errors.New("factor: nonfinite exposure")
			}
			columns[j][i] = value * scale
			if math.IsNaN(columns[j][i]) || math.IsInf(columns[j][i], 0) {
				return nil, 0, errors.New("factor: weighted exposure overflow")
			}
		}
	}
	var q [][]float64
	var independent []int
	r := make([][]float64, p)
	for j := range r {
		r[j] = make([]float64, p)
	}
	for j, col := range columns {
		original := norm(col)
		if original == 0 {
			continue
		}
		for pass := 0; pass < 2; pass++ {
			for k, basis := range q {
				projection := dot(basis, col)
				r[k][j] += projection
				for i := range col {
					col[i] -= projection * basis[i]
				}
			}
		}
		length := norm(col)
		if length <= original*1e-12 {
			continue
		}
		k := len(q)
		r[k][j] = length
		for i := range col {
			col[i] /= length
		}
		q = append(q, col)
		independent = append(independent, j)
	}
	coef := make([]float64, p)
	for k := len(q) - 1; k >= 0; k-- {
		j := independent[k]
		value := dot(q[k], response)
		for l := k + 1; l < len(q); l++ {
			value -= r[k][independent[l]] * coef[independent[l]]
		}
		coef[j] = value / r[k][j]
		if math.IsNaN(coef[j]) || math.IsInf(coef[j], 0) {
			return nil, 0, errors.New("factor: regression overflow")
		}
	}
	return coef, len(q), nil
}
func dot(a, b []float64) float64 {
	sum := 0.0
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}
func norm(a []float64) float64 {
	result := 0.0
	for _, v := range a {
		result = math.Hypot(result, v)
	}
	return result
}
