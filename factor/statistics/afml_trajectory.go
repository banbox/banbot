package statistics

import (
	"context"
	"errors"
	"math"
	"math/big"

	"gonum.org/v1/gonum/mat"
)

// TrajectoryHorizon contains forecasts known at the planning cutoff. Means are
// excess returns; horizons are independent (AFML21.3). No forecasting model or
// cross-horizon covariance is inferred by this research solver.
type TrajectoryHorizon struct {
	Mean, Cost []float64
	Covariance [][]float64
}

type TrajectoryBudget struct {
	MaxAllocations, MaxTrajectories, MaxWork uint64
}

type TrajectoryResult struct {
	// Weights is horizon-major, unlike the source's N-by-H matrix.
	Weights                           [][]float64
	Costs                             []float64
	ExpectedReturn, Variance, Sharpe  float64
	Allocations, Evaluated, Undefined uint64
}

// TrajectoryMeshSize counts distinct signed full-gross allocations. Assigning
// signs to zero weights in snippet21.2 duplicates vectors; removing those
// duplicates leaves precisely sum_m C(N,m) C(K-1,m-1) 2^m vectors.
func TrajectoryMeshSize(units, assets int) (*big.Int, error) {
	if units < 1 || units > 1000000 || assets < 1 || assets > 64 {
		return nil, errors.New("statistics: invalid trajectory mesh dimensions")
	}
	total := new(big.Int)
	for m := 1; m <= assets && m <= units; m++ {
		term := new(big.Int).Binomial(int64(assets), int64(m))
		term.Mul(term, new(big.Int).Binomial(int64(units-1), int64(m-1)))
		term.Lsh(term, uint(m))
		total.Add(total, term)
	}
	return total, nil
}

func validateTrajectory(params []TrajectoryHorizon, initial []float64) error {
	n := len(initial)
	if n < 1 || n > 64 || len(params) < 1 || len(params) > 64 {
		return errors.New("statistics: invalid trajectory dimensions")
	}
	gross := 0.
	for _, v := range initial {
		if !afmlFinite(v) {
			return errors.New("statistics: invalid initial allocation")
		}
		gross += math.Abs(v)
	}
	if gross > 1+1e-12 {
		return errors.New("statistics: initial trajectory allocation exceeds full gross")
	}
	for _, p := range params {
		if len(p.Mean) != n || len(p.Cost) != n || len(p.Covariance) != n {
			return errors.New("statistics: inconsistent trajectory forecasts")
		}
		sym := mat.NewSymDense(n, nil)
		for i := 0; i < n; i++ {
			if len(p.Covariance[i]) != n || !afmlFinite(p.Mean[i]) || !afmlFinite(p.Cost[i]) || p.Cost[i] < 0 {
				return errors.New("statistics: invalid trajectory forecast")
			}
		}
		scale := 0.
		for i := 0; i < n; i++ {
			for j := 0; j <= i; j++ {
				a, b := p.Covariance[i][j], p.Covariance[j][i]
				if !afmlFinite(a) || !afmlFinite(b) || math.Abs(a-b) > 1e-12*math.Max(1, math.Max(math.Abs(a), math.Abs(b))) {
					return errors.New("statistics: invalid trajectory covariance symmetry")
				}
				sym.SetSym(i, j, (a+b)/2)
				scale = math.Max(scale, math.Abs(a))
			}
		}
		var eig mat.EigenSym
		if !eig.Factorize(sym, false) {
			return errors.New("statistics: trajectory covariance decomposition failed")
		}
		for _, v := range eig.Values(nil) {
			if v < -1e-12*scale {
				return errors.New("statistics: trajectory covariance is not positive semidefinite")
			}
		}
	}
	return nil
}

func trajectoryStep(p TrajectoryHorizon, prev, w []float64) (mean, variance, cost float64) {
	for i, v := range w {
		mean += v * p.Mean[i]
		cost += p.Cost[i] * math.Sqrt(math.Abs(v-prev[i]))
		for j, u := range w {
			variance += v * p.Covariance[i][j] * u
		}
	}
	return mean - cost, math.Max(0, variance), cost
}

// EvaluateTrajectory implements the printed SR=sum(mu'w-tau)/sqrt(sum(w'Vw)).
// Initial may be cash (all zero), as in snippet21.3. No terminal liquidation
// cost or compounding is added. Zero aggregate variance has undefined Sharpe.
func EvaluateTrajectory(params []TrajectoryHorizon, initial []float64, weights [][]float64) (TrajectoryResult, error) {
	if err := validateTrajectory(params, initial); err != nil {
		return TrajectoryResult{}, err
	}
	if len(weights) != len(params) {
		return TrajectoryResult{}, errors.New("statistics: trajectory horizon mismatch")
	}
	r := TrajectoryResult{Weights: make([][]float64, len(weights)), Costs: make([]float64, len(weights))}
	prev := initial
	for h, w := range weights {
		if len(w) != len(initial) {
			return TrajectoryResult{}, errors.New("statistics: trajectory asset mismatch")
		}
		gross := 0.
		for _, v := range w {
			if !afmlFinite(v) {
				return TrajectoryResult{}, errors.New("statistics: invalid trajectory weight")
			}
			gross += math.Abs(v)
		}
		if math.Abs(gross-1) > 1e-12 {
			return TrajectoryResult{}, errors.New("statistics: trajectory must be fully invested in gross")
		}
		m, v, c := trajectoryStep(params[h], prev, w)
		r.ExpectedReturn += m
		r.Variance += v
		r.Costs[h] = c
		r.Weights[h] = append([]float64(nil), w...)
		prev = w
	}
	if !afmlFinite(r.ExpectedReturn) || !afmlFinite(r.Variance) || r.Variance <= 0 {
		return TrajectoryResult{}, errors.New("statistics: undefined trajectory Sharpe")
	}
	r.Sharpe = r.ExpectedReturn / math.Sqrt(r.Variance)
	if !afmlFinite(r.Sharpe) {
		return TrajectoryResult{}, errors.New("statistics: nonfinite trajectory Sharpe")
	}
	return r, nil
}

// OptimizeTrajectory exhausts the Cartesian H-horizon mesh, streaming paths
// instead of materializing their exponential product. All candidate/storage
// and arithmetic-work bounds are checked using exact integers BEFORE generating
// allocations. Ties keep the first deterministic mesh path. Undefined zero-risk
// candidates are counted and excluded; cancellation returns no partial optimum.
func OptimizeTrajectory(ctx context.Context, params []TrajectoryHorizon, initial []float64, units int, budget TrajectoryBudget) (TrajectoryResult, error) {
	if ctx == nil {
		return TrajectoryResult{}, errors.New("statistics: nil trajectory context")
	}
	if err := ctx.Err(); err != nil {
		return TrajectoryResult{}, err
	}
	if err := validateTrajectory(params, initial); err != nil {
		return TrajectoryResult{}, err
	}
	n, h := len(initial), len(params)
	a, err := TrajectoryMeshSize(units, n)
	if err != nil {
		return TrajectoryResult{}, err
	}
	paths := new(big.Int).Exp(a, big.NewInt(int64(h)), nil)
	work := new(big.Int).Mul(paths, big.NewInt(int64(h*n*(n+2))))
	storage := new(big.Int).Mul(a, big.NewInt(int64(n)))
	if budget.MaxAllocations == 0 || budget.MaxTrajectories == 0 || budget.MaxWork == 0 || a.Cmp(new(big.Int).SetUint64(budget.MaxAllocations)) > 0 || paths.Cmp(new(big.Int).SetUint64(budget.MaxTrajectories)) > 0 || work.Cmp(new(big.Int).SetUint64(budget.MaxWork)) > 0 || storage.Cmp(big.NewInt(2000000)) > 0 {
		return TrajectoryResult{}, errors.New("statistics: trajectory search exceeds preflight budget")
	}
	mesh := make([][]float64, 0, int(a.Uint64()))
	w := make([]float64, n)
	var generate func(int, int)
	generate = func(i, left int) {
		if i == n {
			mesh = append(mesh, append([]float64(nil), w...))
			return
		}
		max := left
		if i == n-1 {
			max = left
		}
		start := 0
		if i == n-1 {
			start = left
		}
		for k := start; k <= max; k++ {
			w[i] = float64(k) / float64(units)
			if k == 0 {
				generate(i+1, left-k)
			} else {
				w[i] = -w[i]
				generate(i+1, left-k)
				w[i] = -w[i]
				generate(i+1, left-k)
			}
		}
	}
	generate(0, units)
	best := TrajectoryResult{Allocations: uint64(len(mesh)), Sharpe: math.Inf(-1)}
	indices := make([]int, h)
	found := false
	var search func(int, []float64, float64, float64) error
	search = func(t int, prev []float64, mean, variance float64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if t == h {
			best.Evaluated++
			if variance <= 0 {
				best.Undefined++
				return nil
			}
			sr := mean / math.Sqrt(variance)
			if !afmlFinite(sr) {
				return errors.New("statistics: trajectory arithmetic overflow")
			}
			if !found || sr > best.Sharpe {
				found = true
				best.Sharpe = sr
				best.ExpectedReturn = mean
				best.Variance = variance
				best.Weights = make([][]float64, h)
				best.Costs = make([]float64, h)
				p := initial
				for j, k := range indices {
					best.Weights[j] = append([]float64(nil), mesh[k]...)
					_, _, best.Costs[j] = trajectoryStep(params[j], p, mesh[k])
					p = mesh[k]
				}
			}
			return nil
		}
		for k, v := range mesh {
			indices[t] = k
			m, q, _ := trajectoryStep(params[t], prev, v)
			if !afmlFinite(m) || !afmlFinite(q) || !afmlFinite(mean+m) || !afmlFinite(variance+q) {
				return errors.New("statistics: trajectory arithmetic overflow")
			}
			if err := search(t+1, v, mean+m, variance+q); err != nil {
				return err
			}
		}
		return nil
	}
	if err := search(0, initial, 0, 0); err != nil {
		return TrajectoryResult{}, err
	}
	if !found {
		return TrajectoryResult{}, errors.New("statistics: no trajectory has defined Sharpe")
	}
	return best, nil
}

// StaticTrajectory implements snippet21.6: V^-1 mu, normalized by absolute
// weight sum at each horizon. It ignores costs when choosing weights but evaluates
// the final path with exactly the same turnover objective as the dynamic solver.
// Singular covariance is rejected rather than substituting a different optimizer.
func StaticTrajectory(params []TrajectoryHorizon, initial []float64) (TrajectoryResult, error) {
	if err := validateTrajectory(params, initial); err != nil {
		return TrajectoryResult{}, err
	}
	n := len(initial)
	weights := make([][]float64, len(params))
	for h, p := range params {
		s := mat.NewSymDense(n, nil)
		for i := 0; i < n; i++ {
			for j := 0; j <= i; j++ {
				s.SetSym(i, j, p.Covariance[i][j])
			}
		}
		var ch mat.Cholesky
		if !ch.Factorize(s) {
			return TrajectoryResult{}, errors.New("statistics: static trajectory requires nonsingular positive covariance")
		}
		var v mat.VecDense
		if err := ch.SolveVecTo(&v, mat.NewVecDense(n, p.Mean)); err != nil {
			return TrajectoryResult{}, err
		}
		weights[h] = append([]float64(nil), v.RawVector().Data...)
		gross := 0.
		for _, x := range weights[h] {
			gross += math.Abs(x)
		}
		if gross == 0 || !afmlFinite(gross) {
			return TrajectoryResult{}, errors.New("statistics: static trajectory has no allocation direction")
		}
		for i := range weights[h] {
			weights[h][i] /= gross
		}
	}
	return EvaluateTrajectory(params, initial, weights)
}
