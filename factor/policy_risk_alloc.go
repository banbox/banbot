package factor

import (
	"errors"
	"math"
	"sort"
)

func finiteFloat(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

// PopulateContextRiskFromFrame extracts valid positive "volatility" (or cfg.VolatilityColumn)
// and finite "beta" (or cfg.BetaColumn) outputs from c.Frame.Values without overwriting any
// explicit per-SID values already injected on c.Volatility or c.Beta.
func PopulateContextRiskFromFrame(c *PortfolioContext, cfg AllocationConfig) (volFromFrame, betaFromFrame bool) {
	volCol := cfg.VolatilityColumn
	if volCol == "" {
		volCol = VolatilityOutput
	}
	if col, ok := c.Frame.Values[volCol]; ok {
		volFromFrame = true
		if c.Volatility == nil {
			c.Volatility = make(map[int32]float64, len(col))
		}
		for sid, n := range col {
			if _, exists := c.Volatility[sid]; !exists && n.Validity == Valid && n.Value > 0 && finiteFloat(n.Value) {
				c.Volatility[sid] = n.Value
			}
		}
	}
	betaCol := cfg.BetaColumn
	if betaCol == "" {
		betaCol = BetaOutput
	}
	if col, ok := c.Frame.Values[betaCol]; ok {
		betaFromFrame = true
		if c.Beta == nil {
			c.Beta = make(map[int32]float64, len(col))
		}
		for sid, n := range col {
			if _, exists := c.Beta[sid]; !exists && n.Validity == Valid && finiteFloat(n.Value) {
				c.Beta[sid] = n.Value
			}
		}
	}
	return volFromFrame, betaFromFrame
}

func allocateSideCovariance(chosen []int32, sign float64, method string, cfg AllocationConfig, ctx PortfolioContext, scores map[int32]float64) ([]float64, error) {
	n := len(chosen)
	if n == 1 {
		return []float64{1}, nil
	}
	cov, ready, err := resolveSideCovariance(chosen, cfg, ctx)
	if err != nil {
		return nil, err
	}
	if !ready {
		return fallbackRiskWeights(chosen, ctx.Volatility), nil
	}
	sideScores := make([]float64, n)
	for i, sid := range chosen {
		sideScores[i] = sign * scores[sid]
	}
	optimizer := ctx.RiskOptimizer
	if optimizer == nil {
		optimizer = defaultSideRiskOptimizer
	}
	weights, optErr := optimizer(method, cov, sideScores, cfg)
	if optErr != nil || len(weights) != n {
		return fallbackRiskWeights(chosen, ctx.Volatility), nil
	}
	sum := 0.0
	for _, w := range weights {
		if !finiteFloat(w) || w < -1e-9 {
			return fallbackRiskWeights(chosen, ctx.Volatility), nil
		}
		if w > 0 {
			sum += w
		}
	}
	if sum <= 0 {
		return fallbackRiskWeights(chosen, ctx.Volatility), nil
	}
	normalized := make([]float64, n)
	for i, w := range weights {
		if w > 0 {
			normalized[i] = w / sum
		}
	}
	return normalized, nil
}

func fallbackRiskWeights(chosen []int32, vol map[int32]float64) []float64 {
	n := len(chosen)
	weights := make([]float64, n)
	invSum := 0.0
	allPos := len(vol) > 0
	if allPos {
		for i, sid := range chosen {
			sigma := vol[sid]
			if sigma <= 0 || !finiteFloat(sigma) {
				allPos = false
				break
			}
			weights[i] = 1 / sigma
			invSum += weights[i]
		}
	}
	if allPos && invSum > 0 {
		for i := range weights {
			weights[i] /= invSum
		}
		return weights
	}
	eq := 1 / float64(n)
	for i := range weights {
		weights[i] = eq
	}
	return weights
}

func resolveSideCovariance(chosen []int32, cfg AllocationConfig, ctx PortfolioContext) ([][]float64, bool, error) {
	n := len(chosen)
	if ctx.Covariance != nil {
		idx := make(map[int32]int, len(ctx.Covariance.SIDs))
		for i, sid := range ctx.Covariance.SIDs {
			idx[sid] = i
		}
		if len(ctx.Covariance.Matrix) != len(ctx.Covariance.SIDs) {
			return nil, false, errors.New("factor: invalid covariance matrix dimensions")
		}
		sub := make([][]float64, n)
		for i, sidI := range chosen {
			ri, ok := idx[sidI]
			if !ok || len(ctx.Covariance.Matrix[ri]) != len(ctx.Covariance.SIDs) {
				return nil, false, nil
			}
			sub[i] = make([]float64, n)
			for j, sidJ := range chosen {
				rj, ok := idx[sidJ]
				if !ok {
					return nil, false, nil
				}
				v := ctx.Covariance.Matrix[ri][rj]
				if !finiteFloat(v) {
					return nil, false, errors.New("factor: non-finite covariance entry")
				}
				if i != j && cfg.Shrinkage > 0 {
					v *= 1 - cfg.Shrinkage
				}
				sub[i][j] = v
			}
			if sub[i][i] <= 1e-24 {
				return nil, false, nil
			}
		}
		for i := 0; i < n; i++ {
			for j := 0; j < i; j++ {
				if math.Abs(sub[i][j]-sub[j][i]) > 1e-10*math.Max(1, math.Max(math.Abs(sub[i][j]), math.Abs(sub[j][i]))) {
					return nil, false, errors.New("factor: asymmetric covariance matrix")
				}
			}
		}
		if !isPositiveDefinite(sub) {
			sub = applyDiagonalShrinkage(sub, 0.05)
			if !isPositiveDefinite(sub) {
				return nil, false, nil
			}
		}
		return sub, true, nil
	}
	window := cfg.EffectiveRiskWindow()
	if len(ctx.ReturnHistory) < window {
		return nil, false, nil
	}
	var aligned [][]float64
	for k := len(ctx.ReturnHistory) - 1; k >= 0 && len(aligned) < window; k-- {
		rowMap := ctx.ReturnHistory[k].Returns
		row := make([]float64, n)
		complete := true
		for j, sid := range chosen {
			v, ok := rowMap[sid]
			if !ok || !finiteFloat(v) {
				complete = false
				break
			}
			row[j] = v
		}
		if complete {
			aligned = append(aligned, row)
		}
	}
	if len(aligned) < window {
		return nil, false, nil
	}
	for l, r := 0, len(aligned)-1; l < r; l, r = l+1, r-1 {
		aligned[l], aligned[r] = aligned[r], aligned[l]
	}
	cov, ok := sampleCovarianceShrunk(aligned, cfg.Shrinkage)
	if !ok {
		return nil, false, nil
	}
	if !isPositiveDefinite(cov) {
		cov = applyDiagonalShrinkage(cov, math.Max(cfg.Shrinkage, 0.05))
		if !isPositiveDefinite(cov) {
			return nil, false, nil
		}
	}
	return cov, true, nil
}

func sampleCovarianceShrunk(returns [][]float64, shrinkage float64) ([][]float64, bool) {
	t := len(returns)
	if t < 2 || len(returns[0]) == 0 {
		return nil, false
	}
	p := len(returns[0])
	means := make([]float64, p)
	for _, row := range returns {
		for j, v := range row {
			means[j] += v
		}
	}
	for j := range means {
		means[j] /= float64(t)
	}
	cov := make([][]float64, p)
	for i := range cov {
		cov[i] = make([]float64, p)
	}
	denom := float64(t - 1)
	for i := 0; i < p; i++ {
		for j := 0; j <= i; j++ {
			sum := 0.0
			for k := 0; k < t; k++ {
				sum += (returns[k][i] - means[i]) * (returns[k][j] - means[j])
			}
			val := sum / denom
			if i != j {
				val *= 1 - shrinkage
			}
			if !finiteFloat(val) {
				return nil, false
			}
			cov[i][j], cov[j][i] = val, val
		}
		if cov[i][i] <= 1e-24 {
			return nil, false
		}
	}
	return cov, true
}

func applyDiagonalShrinkage(cov [][]float64, delta float64) [][]float64 {
	n := len(cov)
	out := make([][]float64, n)
	for i := range cov {
		out[i] = make([]float64, n)
		for j := range cov[i] {
			if i == j {
				out[i][j] = cov[i][j]
			} else {
				out[i][j] = (1 - delta) * cov[i][j]
			}
		}
	}
	return out
}

func choleskyLower(cov [][]float64) ([][]float64, bool) {
	n := len(cov)
	if n == 0 {
		return nil, false
	}
	scale := 0.0
	for i := 0; i < n; i++ {
		if len(cov[i]) != n || !finiteFloat(cov[i][i]) || cov[i][i] <= 0 {
			return nil, false
		}
		scale = math.Max(scale, cov[i][i])
	}
	l := make([][]float64, n)
	for i := 0; i < n; i++ {
		l[i] = make([]float64, n)
		for j := 0; j <= i; j++ {
			v := cov[i][j] / scale
			for k := 0; k < j; k++ {
				v -= l[i][k] * l[j][k]
			}
			if i == j {
				if v <= 1e-13 {
					return nil, false
				}
				l[i][j] = math.Sqrt(v)
			} else {
				l[i][j] = v / l[j][j]
			}
		}
	}
	return l, true
}

func isPositiveDefinite(cov [][]float64) bool {
	_, ok := choleskyLower(cov)
	return ok
}

// EqualRiskContribution solves for w_i > 0, sum(w) = 1 such that every asset
// contributes equally to portfolio variance: w_i * (Cov * w)_i = w'Cov w / n
// using Spinu's strictly convex cyclical coordinate descent formulation.
func EqualRiskContribution(cov [][]float64) ([]float64, error) {
	n := len(cov)
	if n == 0 {
		return nil, errors.New("factor: empty ERC covariance")
	}
	if n == 1 {
		if len(cov[0]) != 1 || !finiteFloat(cov[0][0]) || cov[0][0] <= 0 {
			return nil, errors.New("factor: ERC requires positive variance")
		}
		return []float64{1}, nil
	}
	scale := 0.0
	for i, row := range cov {
		if len(row) != n || !finiteFloat(row[i]) || row[i] <= 0 {
			return nil, errors.New("factor: ERC requires square positive-diagonal covariance")
		}
		scale = math.Max(scale, row[i])
	}
	scaled := make([][]float64, n)
	for i := 0; i < n; i++ {
		scaled[i] = make([]float64, n)
		for j := 0; j < n; j++ {
			if !finiteFloat(cov[i][j]) || math.Abs(cov[i][j]-cov[j][i]) > 1e-10*math.Max(1, scale) {
				return nil, errors.New("factor: ERC covariance must be finite symmetric")
			}
			scaled[i][j] = cov[i][j] / scale
		}
	}
	budget := 1 / float64(n)
	y := make([]float64, n)
	invVolSum := 0.0
	for i := 0; i < n; i++ {
		y[i] = 1 / math.Sqrt(scaled[i][i])
		invVolSum += y[i]
	}
	for i := range y {
		y[i] /= invVolSum
	}
	sy := make([]float64, n)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			sy[i] += scaled[i][j] * y[j]
		}
	}
	for iter := 0; iter < 1000; iter++ {
		maxChange := 0.0
		for i := 0; i < n; i++ {
			diag := scaled[i][i]
			ci := sy[i] - diag*y[i]
			disc := math.Sqrt(ci*ci + 4*diag*budget)
			var next float64
			if ci <= 0 {
				next = (-ci + disc) / (2 * diag)
			} else {
				next = (2 * budget) / (ci + disc)
			}
			delta := next - y[i]
			if delta != 0 {
				maxChange = math.Max(maxChange, math.Abs(delta)/math.Max(next, 1e-12))
				y[i] = next
				for k := 0; k < n; k++ {
					sy[k] += scaled[k][i] * delta
				}
			}
		}
		if maxChange < 1e-13 {
			break
		}
	}
	total := 0.0
	for _, v := range y {
		if !finiteFloat(v) || v <= 0 {
			return nil, errors.New("factor: ERC solver failed to converge")
		}
		total += v
	}
	w := make([]float64, n)
	for i, v := range y {
		w[i] = v / total
	}
	return w, nil
}

func defaultSideRiskOptimizer(method string, cov [][]float64, sideScores []float64, cfg AllocationConfig) ([]float64, error) {
	switch method {
	case "erc", "risk-parity":
		return EqualRiskContribution(cov)
	case "hrp":
		return standaloneHRP(cov)
	case "min-variance":
		return standaloneMinVariance(cov)
	case "mean-variance":
		return standaloneMeanVariance(cov, sideScores, cfg.EffectiveRiskAversion())
	default:
		return nil, errors.New("factor: unknown covariance allocation method")
	}
}

func standaloneHRP(cov [][]float64) ([]float64, error) {
	n := len(cov)
	if n == 1 {
		return []float64{1}, nil
	}
	distance := make([][]float64, n)
	for i := range distance {
		distance[i] = make([]float64, n)
		for j := 0; j < n; j++ {
			rho := cov[i][j] / math.Sqrt(cov[i][i]) / math.Sqrt(cov[j][j])
			rho = math.Max(-1, math.Min(1, rho))
			distance[i][j] = math.Sqrt((1 - rho) / 2)
		}
	}
	dist := make([][]float64, 2*n-1)
	for i := range dist {
		dist[i] = make([]float64, 2*n-1)
	}
	for i := 0; i < n; i++ {
		for j := 0; j < i; j++ {
			sum := 0.0
			for k := 0; k < n; k++ {
				d := distance[i][k] - distance[j][k]
				sum += d * d
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
				if d := dist[active[i]][active[j]]; d < best {
					best, a, b = d, i, j
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
	order := leaves[2*n-2]
	weights := make([]float64, n)
	for i := range weights {
		weights[i] = 1
	}
	clusterVar := func(items []int) float64 {
		inv := make([]float64, len(items))
		minVar := math.Inf(1)
		for _, idx := range items {
			minVar = math.Min(minVar, cov[idx][idx])
		}
		tot := 0.0
		for k, idx := range items {
			inv[k] = minVar / cov[idx][idx]
			tot += inv[k]
		}
		sum := 0.0
		for k, idxI := range items {
			for l, idxJ := range items {
				sum += (inv[k] / tot) * (inv[l] / tot) * cov[idxI][idxJ]
			}
		}
		return math.Max(0, sum)
	}
	var bisect func([]int) error
	bisect = func(items []int) error {
		if len(items) < 2 {
			return nil
		}
		mid := len(items) / 2
		left, right := items[:mid], items[mid:]
		lv, rv := clusterVar(left), clusterVar(right)
		if !finiteFloat(lv) || !finiteFloat(rv) || (lv <= 0 && rv <= 0) {
			return errors.New("factor: degenerate HRP cluster variance")
		}
		sc := math.Max(lv, rv)
		lr, rr := lv/sc, rv/sc
		alpha := rr / (lr + rr)
		for _, idx := range left {
			weights[idx] *= alpha
		}
		for _, idx := range right {
			weights[idx] *= 1 - alpha
		}
		if err := bisect(left); err != nil {
			return err
		}
		return bisect(right)
	}
	if err := bisect(order); err != nil {
		return nil, err
	}
	return weights, nil
}

func solveCholeskyVec(l [][]float64, b []float64) []float64 {
	n := len(l)
	y := make([]float64, n)
	for i := 0; i < n; i++ {
		s := b[i]
		for k := 0; k < i; k++ {
			s -= l[i][k] * y[k]
		}
		y[i] = s / l[i][i]
	}
	x := make([]float64, n)
	for i := n - 1; i >= 0; i-- {
		s := y[i]
		for k := i + 1; k < n; k++ {
			s -= l[k][i] * x[k]
		}
		x[i] = s / l[i][i]
	}
	return x
}

func standaloneMinVariance(cov [][]float64) ([]float64, error) {
	n := len(cov)
	if n == 1 {
		return []float64{1}, nil
	}
	scale := 0.0
	for i := 0; i < n; i++ {
		scale = math.Max(scale, cov[i][i])
	}
	normCov := make([][]float64, n)
	for i := 0; i < n; i++ {
		normCov[i] = make([]float64, n)
		for j := 0; j < n; j++ {
			normCov[i][j] = cov[i][j] / scale
		}
	}
	w := make([]float64, n)
	free := make([]bool, n)
	for i := range w {
		w[i] = 1 / float64(n)
		free[i] = true
	}
	const tol = 1e-10
	for iter := 0; iter < 2000; iter++ {
		var ids []int
		for i, f := range free {
			if f {
				ids = append(ids, i)
			}
		}
		sub := make([][]float64, len(ids))
		ones := make([]float64, len(ids))
		for i, idI := range ids {
			sub[i] = make([]float64, len(ids))
			ones[i] = 1
			for j, idJ := range ids {
				sub[i][j] = normCov[idI][idJ]
			}
		}
		l, ok := choleskyLower(sub)
		if !ok {
			break
		}
		x := solveCholeskyVec(l, ones)
		denom := 0.0
		for _, v := range x {
			denom += v
		}
		if !finiteFloat(denom) || denom <= 0 {
			break
		}
		cand := make([]float64, n)
		alpha := 1.0
		hit := -1
		for i, id := range ids {
			cand[id] = x[i] / denom
			if cand[id] < 0 {
				step := w[id] / (w[id] - cand[id])
				if step < alpha {
					alpha = step
					hit = id
				}
			}
		}
		if hit >= 0 {
			for i := range w {
				w[i] += alpha * (cand[i] - w[i])
				if math.Abs(w[i]) < 1e-15 {
					w[i] = 0
				}
			}
			w[hit] = 0
			free[hit] = false
			continue
		}
		w = cand
		grad := make([]float64, n)
		variance := 0.0
		for i := range w {
			for j := range w {
				grad[i] += normCov[i][j] * w[j]
			}
			variance += w[i] * grad[i]
		}
		enter := -1
		viol := tol
		for i, f := range free {
			if !f && variance-grad[i] > viol {
				viol = variance - grad[i]
				enter = i
			}
		}
		if enter >= 0 {
			free[enter] = true
			continue
		}
		return w, nil
	}
	return simplexProjectedMinVariance(normCov), nil
}

func simplexProjectedMinVariance(normCov [][]float64) []float64 {
	n := len(normCov)
	normBound := 0.0
	for i := 0; i < n; i++ {
		s := 0.0
		for j := 0; j < n; j++ {
			s += math.Abs(normCov[i][j])
		}
		normBound = math.Max(normBound, s)
	}
	step := 1 / math.Max(normBound, 1e-6)
	w := make([]float64, n)
	for i := range w {
		w[i] = 1 / float64(n)
	}
	for iter := 0; iter < 2000; iter++ {
		next := make([]float64, n)
		for i := 0; i < n; i++ {
			g := 0.0
			for j := 0; j < n; j++ {
				g += normCov[i][j] * w[j]
			}
			next[i] = w[i] - step*g
		}
		projectOntoUnitSimplex(next)
		diff := 0.0
		for i := range w {
			diff = math.Max(diff, math.Abs(next[i]-w[i]))
		}
		w = next
		if diff < 1e-11 {
			break
		}
	}
	return w
}

func projectOntoUnitSimplex(v []float64) {
	n := len(v)
	u := append([]float64(nil), v...)
	sort.Slice(u, func(i, j int) bool { return u[i] > u[j] })
	cssv := 0.0
	rho := -1
	for i := 0; i < n; i++ {
		cssv += u[i]
		t := (cssv - 1) / float64(i+1)
		if u[i]-t > 0 {
			rho = i
		}
	}
	theta := (cssv - 1) / float64(n)
	if rho >= 0 {
		s := 0.0
		for i := 0; i <= rho; i++ {
			s += u[i]
		}
		theta = (s - 1) / float64(rho+1)
	}
	for i := range v {
		v[i] = math.Max(0, v[i]-theta)
	}
}

// PrepareMeanVarianceInputs normalizes covariance to unit average diagonal and
// constructs positive expected excess returns mu = max(0.05*lambda, lambda + 0.5*z)
// from side-aligned factor scores so RiskAversion lambda is scale-invariant across timeframes.
func PrepareMeanVarianceInputs(cov [][]float64, sideScores []float64, riskAversion float64) (tildeCov [][]float64, mu []float64, err error) {
	n := len(cov)
	if n == 0 || len(sideScores) != n {
		return nil, nil, errors.New("factor: invalid mean-variance dimensions")
	}
	meanVar := 0.0
	for i := 0; i < n; i++ {
		if len(cov[i]) != n || !finiteFloat(cov[i][i]) || cov[i][i] <= 0 {
			return nil, nil, errors.New("factor: mean-variance requires positive variances")
		}
		meanVar += cov[i][i]
	}
	meanVar /= float64(n)
	tildeCov = make([][]float64, n)
	for i := 0; i < n; i++ {
		tildeCov[i] = make([]float64, n)
		for j := 0; j < n; j++ {
			tildeCov[i][j] = cov[i][j] / meanVar
		}
	}
	meanScore := 0.0
	for _, s := range sideScores {
		meanScore += s
	}
	meanScore /= float64(n)
	varScore := 0.0
	for _, s := range sideScores {
		d := s - meanScore
		varScore += d * d
	}
	stdScore := math.Sqrt(varScore / float64(n))
	mu = make([]float64, n)
	floor := 0.05 * riskAversion
	for i, s := range sideScores {
		z := 0.0
		if stdScore > 1e-12 {
			z = math.Max(-2.0, math.Min(2.0, (s-meanScore)/stdScore))
		}
		mu[i] = math.Max(floor, riskAversion+0.5*z)
	}
	return tildeCov, mu, nil
}

func standaloneMeanVariance(cov [][]float64, sideScores []float64, riskAversion float64) ([]float64, error) {
	if len(cov) == 1 {
		return []float64{1}, nil
	}
	tildeCov, mu, err := PrepareMeanVarianceInputs(cov, sideScores, riskAversion)
	if err != nil {
		return nil, err
	}
	n := len(tildeCov)
	normBound := 0.0
	for i := 0; i < n; i++ {
		s := 0.0
		for j := 0; j < n; j++ {
			s += math.Abs(tildeCov[i][j])
		}
		normBound = math.Max(normBound, s)
	}
	step := 1 / math.Max(riskAversion*normBound, 1e-6)
	w := make([]float64, n)
	for iter := 0; iter < 4000; iter++ {
		next := make([]float64, n)
		diff := 0.0
		for i := 0; i < n; i++ {
			risk := 0.0
			for j := 0; j < n; j++ {
				risk += tildeCov[i][j] * w[j]
			}
			v := math.Max(0, w[i]+step*(mu[i]-riskAversion*risk))
			next[i] = v
			diff = math.Max(diff, math.Abs(v-w[i]))
		}
		w = next
		if diff < 1e-11 {
			break
		}
	}
	sum := 0.0
	for _, v := range w {
		sum += v
	}
	if !finiteFloat(sum) || sum <= 0 {
		return nil, errors.New("factor: degenerate mean-variance weights")
	}
	for i := range w {
		w[i] /= sum
	}
	return w, nil
}
