package research

import (
	"errors"
	"math"
)

type CovarianceEstimate struct {
	Matrix    [][]float64
	Samples   int
	Shrinkage float64
}

// EstimateCovariance expects time rows with a stable declared asset order.
// Invalid rows are rejected instead of silently changing the fitted universe.
func EstimateCovariance(returns [][]float64, shrinkage float64, diagonal bool) (CovarianceEstimate, error) {
	if len(returns) < 2 || len(returns[0]) == 0 || !finiteValues(shrinkage) || shrinkage < 0 || shrinkage > 1 {
		return CovarianceEstimate{}, errors.New("research: invalid covariance inputs")
	}
	p := len(returns[0])
	means := make([]float64, p)
	for _, row := range returns {
		if len(row) != p || !finiteValues(row...) {
			return CovarianceEstimate{}, errors.New("research: invalid covariance row")
		}
		for j, v := range row {
			means[j] += v / float64(len(returns))
		}
	}
	matrix := make([][]float64, p)
	for i := range matrix {
		matrix[i] = make([]float64, p)
	}
	for _, row := range returns {
		for i := range row {
			for j := 0; j <= i; j++ {
				matrix[i][j] += (row[i] - means[i]) * (row[j] - means[j]) / float64(len(returns)-1)
			}
		}
	}
	for i := range matrix {
		for j := 0; j < i; j++ {
			if diagonal {
				matrix[i][j] = 0
			} else {
				matrix[i][j] *= 1 - shrinkage
			}
			matrix[j][i] = matrix[i][j]
		}
	}
	if diagonal {
		shrinkage = 1
	}
	return CovarianceEstimate{matrix, len(returns), shrinkage}, nil
}

type RiskConstraints struct {
	Gross, MaxWeight, MaxNet, MaxBeta, VolatilityTarget, TurnoverBudget float64
	LimitNet, LimitBeta, LimitTurnover                                  bool
	LongOnly                                                            bool
	Groups                                                              []string
	GroupCaps                                                           map[string]float64
	Beta                                                                []float64
}
type OptimizationSpec struct {
	ExpectedReturns, Previous []float64
	Covariance                [][]float64
	RiskAversion              float64
	Iterations                int
	Constraints               RiskConstraints
}
type OptimizationResult struct {
	Weights                                            []float64
	Status                                             string
	Iterations                                         int
	Converged                                          bool
	Gross, Net, Beta, Volatility, OneWayTurnover, Cash float64
	Violations                                         map[string]float64
}

// OptimizePortfolio is a bounded projected-gradient baseline. It never relaxes
// configured limits; infeasible results retain violations and cannot be used
// as ordinary targets. A production solver can replace this native interface.
func OptimizePortfolio(spec OptimizationSpec) (OptimizationResult, error) {
	n := len(spec.ExpectedReturns)
	c := spec.Constraints
	if n == 0 || len(spec.Covariance) != n || (spec.Previous != nil && len(spec.Previous) != n) || !finiteValues(spec.ExpectedReturns...) || !finiteValues(spec.Previous...) || !finiteValues(spec.RiskAversion, c.Gross, c.MaxWeight, c.MaxNet, c.MaxBeta, c.VolatilityTarget, c.TurnoverBudget) || spec.RiskAversion <= 0 || c.Gross <= 0 || min(c.MaxWeight, c.MaxNet, c.MaxBeta, c.VolatilityTarget, c.TurnoverBudget) < 0 || (len(c.Groups) > 0 && len(c.Groups) != n) || (c.LimitBeta && (len(c.Beta) != n || !finiteValues(c.Beta...))) {
		return OptimizationResult{}, errors.New("research: invalid optimization inputs")
	}
	for _, cap := range c.GroupCaps {
		if !finiteValues(cap) || cap < 0 {
			return OptimizationResult{}, errors.New("research: invalid group cap")
		}
	}
	// Verify PSD with a tolerant Cholesky factorization. Zero diagonal assets
	// are regularized only numerically and still bounded by hard position caps.
	l := make([][]float64, n)
	normBound := 0.0
	for i, row := range spec.Covariance {
		if len(row) != n || !finiteValues(row...) || row[i] < 0 {
			return OptimizationResult{}, errors.New("research: invalid covariance matrix")
		}
		l[i] = make([]float64, n)
		sum := 0.0
		for j, v := range row {
			sum += math.Abs(v)
			if len(spec.Covariance[j]) != n || math.Abs(v-spec.Covariance[j][i]) > 1e-10 {
				return OptimizationResult{}, errors.New("research: asymmetric covariance")
			}
		}
		normBound = max(normBound, sum)
	}
	epsilon := max(1e-14, normBound*1e-12)
	for i := 0; i < n; i++ {
		for j := 0; j <= i; j++ {
			v := spec.Covariance[i][j]
			for k := 0; k < j; k++ {
				v -= l[i][k] * l[j][k]
			}
			if i == j {
				if v < -epsilon {
					return OptimizationResult{}, errors.New("research: non-PSD covariance")
				}
				l[i][j] = math.Sqrt(max(v, epsilon))
			} else {
				l[i][j] = v / l[j][j]
			}
		}
	}
	iterations := spec.Iterations
	if iterations == 0 {
		iterations = 500
	}
	if iterations < 1 || iterations > 10000 {
		return OptimizationResult{}, errors.New("research: invalid optimizer iteration budget")
	}
	weights := make([]float64, n)
	previous := make([]float64, n)
	copy(previous, spec.Previous)
	step := 1 / max(spec.RiskAversion*normBound, 1e-6)
	variance := func(w []float64) float64 {
		v := 0.0
		for i := range w {
			for j := range w {
				v += w[i] * spec.Covariance[i][j] * w[j]
			}
		}
		return max(0, v)
	}
	project := func(w []float64) {
		for pass := 0; pass < 12; pass++ {
			gross, net := 0.0, 0.0
			for i, v := range w {
				if c.LongOnly {
					v = max(0, v)
				}
				if c.MaxWeight > 0 {
					v = max(-c.MaxWeight, min(c.MaxWeight, v))
				}
				w[i] = v
				gross += math.Abs(v)
				net += v
			}
			if gross > c.Gross {
				for i := range w {
					w[i] *= c.Gross / gross
				}
				net *= c.Gross / gross
			}
			if len(c.Groups) > 0 {
				totals := map[string]float64{}
				for i, v := range w {
					totals[c.Groups[i]] += math.Abs(v)
				}
				for i := range w {
					if cap, exists := c.GroupCaps[c.Groups[i]]; exists && totals[c.Groups[i]] > cap {
						w[i] *= cap / totals[c.Groups[i]]
					}
				}
			}
			if c.LimitNet {
				net = 0
				for _, v := range w {
					net += v
				}
				if math.Abs(net) > c.MaxNet {
					excess := net - math.Copysign(c.MaxNet, net)
					for i := range w {
						w[i] -= excess / float64(n)
					}
				}
			}
			if c.LimitBeta {
				beta, denom := 0.0, 0.0
				for i, v := range w {
					beta += v * c.Beta[i]
					denom += c.Beta[i] * c.Beta[i]
				}
				if math.Abs(beta) > c.MaxBeta && denom > 0 {
					excess := beta - math.Copysign(c.MaxBeta, beta)
					for i := range w {
						w[i] -= excess * c.Beta[i] / denom
					}
				}
			}
			if c.VolatilityTarget > 0 {
				volatility := math.Sqrt(variance(w))
				if volatility > c.VolatilityTarget {
					for i := range w {
						w[i] *= c.VolatilityTarget / volatility
					}
				}
			}
		}
	}
	converged := false
	for iter := 0; iter < iterations; iter++ {
		next := make([]float64, n)
		for i := range weights {
			risk := 0.0
			for j, w := range weights {
				risk += spec.Covariance[i][j] * w
			}
			next[i] = weights[i] + step*(spec.ExpectedReturns[i]-spec.RiskAversion*risk)
		}
		project(next)
		if c.LimitTurnover {
			change := 0.0
			for i := range next {
				change += math.Abs(next[i] - previous[i])
			}
			if change > 2*c.TurnoverBudget {
				ratio := 2 * c.TurnoverBudget / change
				for i := range next {
					next[i] = previous[i] + ratio*(next[i]-previous[i])
				}
			}
		}
		change := 0.0
		for i := range weights {
			change = max(change, math.Abs(next[i]-weights[i]))
		}
		weights = next
		if change < 1e-10 {
			converged = true
			iterations = iter + 1
			break
		}
	}
	if !finiteValues(weights...) {
		return OptimizationResult{}, errors.New("research: optimizer numerical overflow")
	}
	r := OptimizationResult{Weights: weights, Status: "feasible", Iterations: iterations, Converged: converged, Violations: map[string]float64{}}
	groups := map[string]float64{}
	for i, w := range weights {
		r.Gross += math.Abs(w)
		r.Net += w
		r.OneWayTurnover += math.Abs(w-previous[i]) / 2
		if c.LimitBeta {
			r.Beta += w * c.Beta[i]
		}
		if len(c.Groups) > 0 {
			groups[c.Groups[i]] += math.Abs(w)
		}
		if c.MaxWeight > 0 && math.Abs(w) > c.MaxWeight+1e-8 {
			r.Violations["single-asset"] = max(r.Violations["single-asset"], math.Abs(w)-c.MaxWeight)
		}
		if c.LongOnly && w < -1e-8 {
			r.Violations["long-only"] = max(r.Violations["long-only"], -w)
		}
	}
	r.Volatility = math.Sqrt(variance(weights))
	if !finiteValues(r.Volatility, r.Gross, r.Net, r.Beta, r.OneWayTurnover) {
		return OptimizationResult{}, errors.New("research: optimizer result overflow")
	}
	r.Cash = max(0, 1-r.Gross)
	check := func(name string, value, limit float64) {
		if value > limit+1e-8 {
			r.Violations[name] = value - limit
		}
	}
	check("gross", r.Gross, c.Gross)
	if c.LimitNet {
		check("net", math.Abs(r.Net), c.MaxNet)
	}
	if c.LimitBeta {
		check("beta", math.Abs(r.Beta), c.MaxBeta)
	}
	if c.VolatilityTarget > 0 {
		check("volatility", r.Volatility, c.VolatilityTarget)
	}
	if c.LimitTurnover {
		check("turnover", r.OneWayTurnover, c.TurnoverBudget)
	}
	for group, cap := range c.GroupCaps {
		check("group:"+group, groups[group], cap)
	}
	if len(r.Violations) > 0 {
		r.Status = "infeasible"
	}
	return r, nil
}
