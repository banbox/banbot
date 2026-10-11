package statistics

import (
	"errors"
	"math"

	"github.com/banbox/banbot/factor"
	"gonum.org/v1/gonum/mat"
)

type JohansenSpec struct {
	DifferenceLags   int
	Intercept, Trend bool // unrestricted nuisance terms in both regressions
}

type JohansenResult struct {
	Eigenvalues, Trace, MaximumEigenvalue   []float64
	CointegratingVectors, AdjustmentVectors [][]float64
	Samples, NuisanceRank                   int
}

// Johansen computes the Gaussian reduced-rank VEC canonical correlations.
// Delta levels and lagged levels are residualized against past differences and
// explicitly selected deterministic terms. Vectors satisfy beta'Skk beta=I.
// No ordinary normal/t distribution or critical-value table is inferred here:
// callers must choose rank thresholds for these exact deterministic terms,
// lag order and significance. This operator does not select a trading policy.
func Johansen(levels [][]float64, spec JohansenSpec) (JohansenResult, error) {
	if len(levels) == 0 || len(levels[0]) < 2 || len(levels[0]) > 64 || spec.DifferenceLags < 0 || spec.DifferenceLags > 128 || spec.Trend && !spec.Intercept {
		return JohansenResult{}, errors.New("statistics: invalid Johansen dimensions/specification")
	}
	d, lag := len(levels[0]), spec.DifferenceLags
	n := len(levels) - lag - 1
	nuisanceCount := d * lag
	if spec.Intercept {
		nuisanceCount++
	}
	if spec.Trend {
		nuisanceCount++
	}
	if n <= nuisanceCount+d {
		return JohansenResult{}, errors.New("statistics: insufficient Johansen observations")
	}
	for _, row := range levels {
		if len(row) != d {
			return JohansenResult{}, errors.New("statistics: ragged Johansen levels")
		}
		for _, v := range row {
			if !finite(v) {
				return JohansenResult{}, errors.New("statistics: nonfinite Johansen levels")
			}
		}
	}
	z := make([][]float64, n)
	r0, rk := make([][]float64, n), make([][]float64, n)
	for i := range n {
		t := lag + 1 + i
		z[i] = make([]float64, 0, nuisanceCount)
		if spec.Intercept {
			z[i] = append(z[i], 1)
		}
		if spec.Trend {
			z[i] = append(z[i], float64(t))
		}
		for l := 1; l <= lag; l++ {
			for asset := range d {
				z[i] = append(z[i], levels[t-l][asset]-levels[t-l-1][asset])
			}
		}
		r0[i], rk[i] = make([]float64, d), make([]float64, d)
		for asset := range d {
			r0[i][asset] = levels[t][asset] - levels[t-1][asset]
			rk[i][asset] = levels[t-1][asset]
		}
	}
	rank := 0
	for _, matrix := range [][][]float64{r0, rk} {
		for asset := range d {
			y := make([]float64, n)
			for i := range n {
				y[i] = matrix[i][asset]
			}
			if nuisanceCount > 0 {
				coefficients, r, err := factor.LeastSquares(z, y, nil)
				if err != nil {
					return JohansenResult{}, err
				}
				rank = r
				for i := range n {
					for j, c := range coefficients {
						matrix[i][asset] -= z[i][j] * c
					}
				}
			}
		}
	}
	s00, skk := mat.NewSymDense(d, nil), mat.NewSymDense(d, nil)
	s0k := mat.NewDense(d, d, nil)
	for a := range d {
		for b := range d {
			v00, vkk, v0k := 0.0, 0.0, 0.0
			for i := range n {
				v00 += r0[i][a] * r0[i][b] / float64(n)
				vkk += rk[i][a] * rk[i][b] / float64(n)
				v0k += r0[i][a] * rk[i][b] / float64(n)
			}
			if !finite(v00) || !finite(vkk) || !finite(v0k) {
				return JohansenResult{}, errors.New("statistics: Johansen covariance overflow")
			}
			if b <= a {
				s00.SetSym(a, b, v00)
				skk.SetSym(a, b, vkk)
			}
			s0k.Set(a, b, v0k)
		}
	}
	root, err := covarianceInverseRoot(skk)
	if err != nil {
		return JohansenResult{}, err
	}
	var chol mat.Cholesky
	if !chol.Factorize(s00) {
		return JohansenResult{}, errors.New("statistics: singular Johansen difference covariance")
	}
	var solve, canonical, intermediate mat.Dense
	if err := chol.SolveTo(&solve, s0k); err != nil {
		return JohansenResult{}, err
	}
	intermediate.Mul(s0k.T(), &solve)
	canonical.Mul(root, &intermediate)
	intermediate.Mul(&canonical, root)
	sym := mat.NewSymDense(d, nil)
	for a := range d {
		for b := 0; b <= a; b++ {
			v := .5*intermediate.At(a, b) + .5*intermediate.At(b, a)
			if !finite(v) {
				return JohansenResult{}, errors.New("statistics: Johansen canonical covariance overflow")
			}
			sym.SetSym(a, b, v)
		}
	}
	var eigen mat.EigenSym
	if !eigen.Factorize(sym, true) {
		return JohansenResult{}, errors.New("statistics: Johansen eigendecomposition failed")
	}
	values := eigen.Values(nil)
	var vectors mat.Dense
	eigen.VectorsTo(&vectors)
	out := JohansenResult{Samples: n, NuisanceRank: rank, Eigenvalues: make([]float64, d), Trace: make([]float64, d), MaximumEigenvalue: make([]float64, d), CointegratingVectors: make([][]float64, d), AdjustmentVectors: make([][]float64, d)}
	for component := range d {
		column := d - 1 - component
		value := values[column]
		if value < -1e-10 || value >= 1 {
			return JohansenResult{}, errors.New("statistics: degenerate Johansen canonical correlation")
		}
		out.Eigenvalues[component] = max(0, value)
		out.MaximumEigenvalue[component] = -float64(n) * math.Log1p(-out.Eigenvalues[component])
		beta := make([]float64, d)
		for a := range d {
			for b := range d {
				beta[a] += root.At(a, b) * vectors.At(b, column)
			}
		}
		out.CointegratingVectors[component] = beta
	}
	canonicalLoadingSignsTranspose(out.CointegratingVectors)
	for component, beta := range out.CointegratingVectors {
		out.AdjustmentVectors[component] = make([]float64, d)
		for a := range d {
			for b := range d {
				out.AdjustmentVectors[component][a] += s0k.At(a, b) * beta[b]
			}
		}
	}
	for rank := d - 1; rank >= 0; rank-- {
		out.Trace[rank] = out.MaximumEigenvalue[rank]
		if rank+1 < d {
			out.Trace[rank] += out.Trace[rank+1]
		}
	}
	return out, nil
}

func covarianceInverseRoot(cov *mat.SymDense) (*mat.SymDense, error) {
	n := cov.SymmetricDim()
	var eigen mat.EigenSym
	if !eigen.Factorize(cov, true) {
		return nil, errors.New("statistics: covariance inverse-root decomposition failed")
	}
	values := eigen.Values(nil)
	var vectors mat.Dense
	eigen.VectorsTo(&vectors)
	root := mat.NewSymDense(n, nil)
	for component, v := range values {
		if v <= 1e-12*values[n-1] || !finite(v) {
			return nil, errors.New("statistics: singular Johansen level covariance")
		}
		for a := range n {
			for b := 0; b <= a; b++ {
				root.SetSym(a, b, root.At(a, b)+vectors.At(a, component)*vectors.At(b, component)/math.Sqrt(v))
			}
		}
	}
	return root, nil
}

func canonicalLoadingSignsTranspose(vectors [][]float64) {
	for _, vector := range vectors {
		pivot := 0
		for i, v := range vector {
			if math.Abs(v) > math.Abs(vector[pivot]) {
				pivot = i
			}
		}
		if vector[pivot] < 0 {
			for i := range vector {
				vector[i] = -vector[i]
			}
		}
	}
}

// TraceRank sequentially rejects the supplied rank-r critical values. A rejected
// rank0 alone does not establish full rank or choose a hedge vector automatically.
func (j JohansenResult) TraceRank(criticalValues []float64) (int, error) {
	if len(j.Trace) == 0 || len(criticalValues) != len(j.Trace) {
		return 0, errors.New("statistics: Johansen rank thresholds must match dimension")
	}
	for i, v := range criticalValues {
		if !finite(v) || v <= 0 || !finite(j.Trace[i]) || j.Trace[i] < 0 {
			return 0, errors.New("statistics: invalid Johansen rank threshold/statistic")
		}
	}
	rank := 0
	for i, v := range criticalValues {
		if j.Trace[i] <= v {
			break
		}
		rank++
	}
	return rank, nil
}
