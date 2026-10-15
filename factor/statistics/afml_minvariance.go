package statistics

import (
	"errors"
	"math"

	"gonum.org/v1/gonum/mat"
)

type MinimumVarianceResult struct {
	Weights                       []float64
	Variance, Lambda, KKTResidual float64
	Iterations                    int
}

// LongOnlyMinimumVariance solves min w'Cov w, sum(w)=1, w>=0 by a
// feasible active set. This is the minimum-variance endpoint compared with HRP
// in AFML 16.5, not a CLA efficient-frontier implementation. Covariance must
// be positive definite; no hidden ridge or pseudoinverse changes the objective.
func LongOnlyMinimumVariance(cov [][]float64, maxIterations int, tolerance float64) (MinimumVarianceResult, error) {
	n := len(cov)
	if n < 1 || n > 128 || maxIterations < 1 || maxIterations > 10000 || !afmlFinite(tolerance) || tolerance < 1e-12 || tolerance > 1e-4 {
		return MinimumVarianceResult{}, errors.New("statistics: invalid minimum-variance budget/tolerance")
	}
	scale := 0.
	for i, row := range cov {
		if len(row) != n || !afmlFinite(row[i]) || row[i] <= 0 {
			return MinimumVarianceResult{}, errors.New("statistics: invalid covariance dimensions/variance")
		}
		scale = math.Max(scale, row[i])
	}
	a := mat.NewSymDense(n, nil)
	for i := range cov {
		for j := 0; j <= i; j++ {
			if !afmlFinite(cov[i][j]) || !afmlFinite(cov[j][i]) || math.Abs(cov[i][j]-cov[j][i]) > tolerance*scale {
				return MinimumVarianceResult{}, errors.New("statistics: covariance not finite symmetric")
			}
			a.SetSym(i, j, cov[i][j]/scale)
		}
	}
	var full mat.Cholesky
	if !full.Factorize(a) {
		return MinimumVarianceResult{}, errors.New("statistics: minimum variance requires positive definite covariance")
	}
	w, free := make([]float64, n), make([]bool, n)
	for i := range w {
		w[i] = 1 / float64(n)
		free[i] = true
	}
	for iteration := 1; iteration <= maxIterations; iteration++ {
		ids := []int{}
		for i, f := range free {
			if f {
				ids = append(ids, i)
			}
		}
		sub := mat.NewSymDense(len(ids), nil)
		for i, id := range ids {
			for j := 0; j <= i; j++ {
				sub.SetSym(i, j, a.At(id, ids[j]))
			}
		}
		var chol mat.Cholesky
		if !chol.Factorize(sub) {
			return MinimumVarianceResult{}, errors.New("statistics: active covariance factorization failed")
		}
		ones := mat.NewVecDense(len(ids), nil)
		for i := range ids {
			ones.SetVec(i, 1)
		}
		var x mat.VecDense
		if err := chol.SolveVecTo(&x, ones); err != nil {
			return MinimumVarianceResult{}, err
		}
		denom := 0.
		for i := range ids {
			denom += x.AtVec(i)
		}
		if !afmlFinite(denom) || denom <= 0 {
			return MinimumVarianceResult{}, errors.New("statistics: degenerate active solution")
		}
		candidate := make([]float64, n)
		alpha := 1.
		hit := -1
		for i, id := range ids {
			candidate[id] = x.AtVec(i) / denom
			if candidate[id] < 0 {
				step := w[id] / (w[id] - candidate[id])
				if step < alpha {
					alpha = step
					hit = id
				}
			}
		}
		if hit >= 0 {
			for i := range w {
				w[i] += alpha * (candidate[i] - w[i])
				if math.Abs(w[i]) < 1e-15 {
					w[i] = 0
				}
			}
			w[hit] = 0
			free[hit] = false
			continue
		}
		w = candidate
		gradient := make([]float64, n)
		variance := 0.
		for i := range w {
			for j := range w {
				gradient[i] += a.At(i, j) * w[j]
			}
			variance += w[i] * gradient[i]
		}
		enter := -1
		violation := tolerance
		for i, f := range free {
			if !f && variance-gradient[i] > violation {
				violation = variance - gradient[i]
				enter = i
			}
		}
		if enter >= 0 {
			free[enter] = true
			continue
		}
		residual, sum := 0., 0.
		for i := range w {
			sum += w[i]
			if w[i] > tolerance {
				residual = math.Max(residual, math.Abs(gradient[i]-variance))
			} else {
				residual = math.Max(residual, math.Max(0, variance-gradient[i]))
			}
			residual = math.Max(residual, math.Max(0, -w[i]))
		}
		residual = math.Max(residual, math.Abs(sum-1))
		if residual > tolerance*10 {
			return MinimumVarianceResult{}, errors.New("statistics: minimum variance KKT verification failed")
		}
		return MinimumVarianceResult{w, variance * scale, variance * scale, residual, iteration}, nil
	}
	return MinimumVarianceResult{}, errors.New("statistics: minimum variance iteration budget exhausted")
}
