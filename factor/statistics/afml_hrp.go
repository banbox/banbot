package statistics

import (
	"errors"
	"math"
	"sort"

	"gonum.org/v1/gonum/mat"
)

type HRPResult struct {
	Weights []float64
	Order   []int
}

// HierarchicalRiskParity implements AFML 16.4/snippets 16.1-16.3: correlation
// distance profiles, Euclidean single linkage, leaf order, recursive bisection.
// Positive marginal variances and PSD covariance are required, but invertibility
// is not. Ties are resolved by stable cluster ids for deterministic artifacts.
func HierarchicalRiskParity(cov [][]float64) (HRPResult, error) {
	n := len(cov)
	if n == 0 {
		return HRPResult{}, errors.New("statistics: empty HRP covariance")
	}
	for _, row := range cov {
		if len(row) != n {
			return HRPResult{}, errors.New("statistics: HRP covariance must be square")
		}
	}
	sym := mat.NewSymDense(n, nil)
	scale := 0.
	for i := 0; i < n; i++ {
		if !afmlFinite(cov[i][i]) || cov[i][i] <= 0 {
			return HRPResult{}, errors.New("statistics: HRP needs positive marginal variances")
		}
		scale = math.Max(scale, cov[i][i])
		for j := 0; j <= i; j++ {
			if !afmlFinite(cov[i][j]) || !afmlFinite(cov[j][i]) || math.Abs(cov[i][j]-cov[j][i]) > 1e-12*math.Max(1, math.Max(math.Abs(cov[i][j]), math.Abs(cov[j][i]))) {
				return HRPResult{}, errors.New("statistics: HRP covariance is not finite symmetric")
			}
			sym.SetSym(i, j, cov[i][j])
		}
	}
	// PSD and risk ratios are invariant to common scale. Normalize by division
	// before eigendecomposition; reciprocal scaling can itself overflow for
	// finite subnormal covariances.
	for i := 0; i < n; i++ {
		for j := 0; j <= i; j++ {
			sym.SetSym(i, j, cov[i][j]/scale)
		}
	}
	var eig mat.EigenSym
	if !eig.Factorize(sym, false) {
		return HRPResult{}, errors.New("statistics: HRP covariance eigendecomposition failed")
	}
	for _, v := range eig.Values(nil) {
		if !afmlFinite(v) || v < -1e-10 {
			return HRPResult{}, errors.New("statistics: HRP covariance is not PSD")
		}
	}
	distance := make([][]float64, n)
	for i := range distance {
		distance[i] = make([]float64, n)
		for j := 0; j < n; j++ {
			rho := cov[i][j] / math.Sqrt(cov[i][i]) / math.Sqrt(cov[j][j])
			if math.Abs(rho) > 1+1e-10 {
				return HRPResult{}, errors.New("statistics: invalid HRP correlation")
			}
			rho = math.Max(-1, math.Min(1, rho))
			distance[i][j] = math.Sqrt((1 - rho) / 2)
		}
	}
	// scipy.linkage(dist, "single") sees a 2D observation matrix, so the
	// source clusters Euclidean distances of profiles, not condensed D entries.
	dist := make([][]float64, 2*n-1)
	for i := range dist {
		dist[i] = make([]float64, 2*n-1)
	}
	for i := 0; i < n; i++ {
		for j := 0; j < i; j++ {
			sum := 0.
			for k := 0; k < n; k++ {
				delta := distance[i][k] - distance[j][k]
				sum += delta * delta
			}
			dist[i][j] = math.Sqrt(sum)
			dist[j][i] = dist[i][j]
		}
	}
	leaves := make([][]int, 2*n-1)
	active := make([]int, n)
	for i := 0; i < n; i++ {
		leaves[i] = []int{i}
		active[i] = i
	}
	for id := n; id < 2*n-1; id++ {
		a, b := 0, 1
		best := math.Inf(1)
		for i := 0; i < len(active); i++ {
			for j := i + 1; j < len(active); j++ {
				d := dist[active[i]][active[j]]
				if d < best {
					best = d
					a = i
					b = j
				}
			}
		}
		left, right := active[a], active[b]
		leaves[id] = append(append([]int(nil), leaves[left]...), leaves[right]...)
		var next []int
		for _, other := range active {
			if other == left || other == right {
				continue
			}
			dist[id][other] = math.Min(dist[left][other], dist[right][other])
			dist[other][id] = dist[id][other]
			next = append(next, other)
		}
		active = append(next, id)
		sort.Ints(active)
	}
	order := append([]int(nil), leaves[2*n-2]...)
	weights := make([]float64, n)
	for i := range weights {
		weights[i] = 1
	}
	var allocate func([]int) error
	allocate = func(items []int) error {
		if len(items) < 2 {
			return nil
		}
		mid := len(items) / 2
		left, right := items[:mid], items[mid:]
		lv, rv := afmlClusterVariance(cov, left), afmlClusterVariance(cov, right)
		if !afmlFinite(lv) || !afmlFinite(rv) || (lv <= 0 && rv <= 0) {
			return errors.New("statistics: HRP cluster risk is degenerate")
		}
		riskScale := math.Max(lv, rv)
		leftRisk, rightRisk := lv/riskScale, rv/riskScale
		alpha := rightRisk / (leftRisk + rightRisk)
		for _, i := range left {
			weights[i] *= alpha
		}
		for _, i := range right {
			weights[i] *= 1 - alpha
		}
		if err := allocate(left); err != nil {
			return err
		}
		return allocate(right)
	}
	if err := allocate(order); err != nil {
		return HRPResult{}, err
	}
	return HRPResult{Weights: weights, Order: order}, nil
}

func afmlClusterVariance(cov [][]float64, items []int) float64 {
	inv := make([]float64, len(items))
	total := 0.
	minVariance := math.Inf(1)
	for _, i := range items {
		minVariance = math.Min(minVariance, cov[i][i])
	}
	for k, i := range items {
		inv[k] = minVariance / cov[i][i]
		total += inv[k]
	}
	sum := 0.
	for k, i := range items {
		for l, j := range items {
			sum += (inv[k] / total) * (inv[l] / total) * cov[i][j]
		}
	}
	return math.Max(0, sum)
}
