package statistics

import (
	"errors"
	"gonum.org/v1/gonum/mat"
	"math"
)

type ChanFrontierPoint struct {
	Target, Variance, Sharpe, KKTResidual float64
	Weights                               []float64
}
type ChanFrontierResult struct {
	Points                    []ChanFrontierPoint
	Tangency, MinimumVariance int
}

// ChanLongOnlyFrontier reproduces Box 1.2's 21 inclusive mean-return targets.
// It exhaustively solves feasible active faces, bounded to 12 assets (4095 faces).
// Covariance must be SPD; no ridge, pseudoinverse or target interpolation is used.
// Ties select the first (lowest-target) grid point. MinimumVariance is the GRID
// minimum, distinct from the unconstrained-return minimum-variance endpoint.
func ChanLongOnlyFrontier(mean []float64, cov [][]float64) (ChanFrontierResult, error) {
	n := len(mean)
	if n < 2 || n > 12 || len(cov) != n {
		return ChanFrontierResult{}, errors.New("statistics: frontier requires 2..12 assets")
	}
	if _, err := LongOnlyMinimumVariance(cov, 10000, 1e-10); err != nil {
		return ChanFrontierResult{}, err
	}
	lo, hi := mean[0], mean[0]
	scale := 0.
	for i, m := range mean {
		if !afmlFinite(m) {
			return ChanFrontierResult{}, errors.New("statistics: nonfinite expected return")
		}
		lo = math.Min(lo, m)
		hi = math.Max(hi, m)
		scale = math.Max(scale, cov[i][i])
	}
	result := ChanFrontierResult{}
	for k := 0; k <= 20; k++ {
		target := lo + (hi-lo)*float64(k)/20
		if k == 20 {
			target = hi
		}
		best := ChanFrontierPoint{Target: target, Variance: math.Inf(1)}
		for mask := 1; mask < (1 << n); mask++ {
			ids := []int{}
			for i := 0; i < n; i++ {
				if mask&(1<<i) != 0 {
					ids = append(ids, i)
				}
			}
			// Endpoints permit only assets whose mean equals that endpoint; all-equal
			// means reduce to one equality, avoiding the rank-deficient two-row system.
			endpoint := hi == lo || k == 0 || k == 20
			if endpoint {
				valid := true
				for _, i := range ids {
					if mean[i] != target {
						valid = false
					}
				}
				if !valid {
					continue
				}
			}
			sub := mat.NewSymDense(len(ids), nil)
			for i, id := range ids {
				for j := 0; j <= i; j++ {
					sub.SetSym(i, j, cov[id][ids[j]]/scale)
				}
			}
			var ch mat.Cholesky
			if !ch.Factorize(sub) {
				return ChanFrontierResult{}, errors.New("statistics: frontier face factorization failed")
			}
			ones := mat.NewVecDense(len(ids), nil)
			m := mat.NewVecDense(len(ids), nil)
			for i, id := range ids {
				ones.SetVec(i, 1)
				if hi != lo {
					m.SetVec(i, (mean[id]-lo)/(hi-lo))
				}
			}
			var x, y mat.VecDense
			if err := ch.SolveVecTo(&x, ones); err != nil {
				return ChanFrontierResult{}, err
			}
			a := mat.Dot(ones, &x)
			lambda, eta := 1/a, 0.
			if !endpoint {
				if err := ch.SolveVecTo(&y, m); err != nil {
					return ChanFrontierResult{}, err
				}
				b, d := mat.Dot(ones, &y), mat.Dot(m, &y)
				det := a*d - b*b
				if det <= 1e-12*a*d {
					continue
				}
				t := (target - lo) / (hi - lo)
				lambda = (d - b*t) / det
				eta = (a*t - b) / det
			}
			w := make([]float64, n)
			sum, ret := 0., 0.
			valid := true
			for j, id := range ids {
				v := lambda * x.AtVec(j)
				if !endpoint {
					v += eta * y.AtVec(j)
				}
				if !afmlFinite(v) || v < -1e-9 {
					valid = false
					break
				}
				w[id] = math.Max(0, v)
				sum += w[id]
				ret += w[id] * mean[id]
			}
			if !valid || math.Abs(sum-1) > 1e-8 || math.Abs(ret-target) > 1e-8*math.Max(1, math.Max(math.Abs(lo), math.Abs(hi))) {
				continue
			}
			variance, residual := 0., math.Abs(sum-1)
			for i := range w {
				g := 0.
				for j := range w {
					g += cov[i][j] / scale * w[j]
				}
				variance += w[i] * g
				if endpoint {
					if mean[i] != target {
						continue
					}
					if w[i] > 1e-9 {
						residual = math.Max(residual, math.Abs(g-lambda))
					} else {
						residual = math.Max(residual, math.Max(0, lambda-g))
					}
				} else {
					dual := lambda + eta*(mean[i]-lo)/(hi-lo)
					if w[i] > 1e-9 {
						residual = math.Max(residual, math.Abs(g-dual))
					} else {
						residual = math.Max(residual, math.Max(0, dual-g))
					}
				}
			}
			// Reduced endpoint KKT excludes infeasible off-endpoint directions.
			if residual > 1e-7 {
				continue
			}
			variance *= scale
			if !afmlFinite(variance) || variance <= 0 {
				continue
			}
			if variance < best.Variance {
				best = ChanFrontierPoint{target, variance, target / math.Sqrt(variance), residual, w}
			}
		}
		if best.Weights == nil || !afmlFinite(best.Sharpe) {
			return ChanFrontierResult{}, errors.New("statistics: frontier infeasible or numerically degenerate face")
		}
		result.Points = append(result.Points, best)
		if k > 0 {
			if best.Sharpe > result.Points[result.Tangency].Sharpe {
				result.Tangency = k
			}
			if best.Variance < result.Points[result.MinimumVariance].Variance {
				result.MinimumVariance = k
			}
		}
	}
	return result, nil
}

// ChanInverseRiskWeights normalizes inverse positive supplied risk. Passing
// volatility reproduces chapter 1 risk parity; passing fixed positive maximum
// drawdowns is an explicit interpretation of Chan's suggestion, not a CPPI rule.
func ChanInverseRiskWeights(risk []float64) ([]float64, error) {
	if len(risk) == 0 {
		return nil, errors.New("statistics: empty inverse risk")
	}
	min := math.Inf(1)
	for _, r := range risk {
		if !afmlFinite(r) || r <= 0 {
			return nil, errors.New("statistics: risks must be finite positive")
		}
		min = math.Min(min, r)
	}
	w := make([]float64, len(risk))
	sum := 0.
	for i, r := range risk {
		w[i] = min / r
		sum += w[i]
	}
	for i := range w {
		w[i] /= sum
	}
	return w, nil
}

// ChanSignedTangency implements Box 1.3 C^-1 m divided by its SIGNED sum.
// The logarithmic Sharpe derivation requires positive expected portfolio return;
// negative or vanishing signed normalizers are rejected rather than presented
// as a maximizing tangency portfolio. Gross exposure may exceed one.
func ChanSignedTangency(mean []float64, cov [][]float64) ([]float64, error) {
	n := len(mean)
	if n == 0 || len(cov) != n {
		return nil, errors.New("statistics: tangent dimensions")
	}
	if _, err := LongOnlyMinimumVariance(cov, 10000, 1e-10); err != nil {
		return nil, err
	}
	scale := 0.
	for i := range mean {
		if !afmlFinite(mean[i]) {
			return nil, errors.New("statistics: nonfinite mean")
		}
		scale = math.Max(scale, cov[i][i])
	}
	a := mat.NewSymDense(n, nil)
	for i := range mean {
		for j := 0; j <= i; j++ {
			a.SetSym(i, j, cov[i][j]/scale)
		}
	}
	var ch mat.Cholesky
	ch.Factorize(a)
	var x mat.VecDense
	if err := ch.SolveVecTo(&x, mat.NewVecDense(n, mean)); err != nil {
		return nil, err
	}
	sum, gross := 0., 0.
	for i := range mean {
		sum += x.AtVec(i)
		gross += math.Abs(x.AtVec(i))
	}
	if !afmlFinite(sum) || sum <= 1e-12*gross || gross == 0 {
		return nil, errors.New("statistics: signed tangent normalization nonpositive or degenerate; positive-return derivation unavailable")
	}
	w := make([]float64, n)
	for i := range w {
		w[i] = x.AtVec(i) / sum
		if !afmlFinite(w[i]) {
			return nil, errors.New("statistics: tangent overflow")
		}
	}
	return w, nil
}
