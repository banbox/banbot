package factor

import "math"

func isFiniteFloat(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

// centeredDeviations returns (w[i] - mean) and sum((w[i] - mean)^2) over finite
// observations w. Constant windows produce exact zeros without rounding drift;
// shifted centering preserves precision when |mean| >> std.
func centeredDeviations(w []float64) ([]float64, float64, bool) {
	d := len(w)
	dx := make([]float64, d)
	constant := true
	for i := 1; i < d; i++ {
		if w[i] != w[0] {
			constant = false
			break
		}
	}
	if constant {
		return dx, 0, true
	}
	base := w[0]
	var sumU float64
	shiftedOK := true
	for i, v := range w {
		u := v - base
		if !isFiniteFloat(u) {
			shiftedOK = false
			break
		}
		dx[i] = u
		sumU += u
	}
	if shiftedOK && isFiniteFloat(sumU) {
		meanU := sumU / float64(d)
		for i := range dx {
			dx[i] -= meanU
		}
	} else {
		var sum float64
		for _, v := range w {
			sum += v
		}
		if !isFiniteFloat(sum) {
			return nil, 0, false
		}
		mean := sum / float64(d)
		for i, v := range w {
			diff := v - mean
			if !isFiniteFloat(diff) {
				return nil, 0, false
			}
			dx[i] = diff
		}
	}
	var s2 float64
	for _, diff := range dx {
		s2 += diff * diff
	}
	if !isFiniteFloat(s2) {
		return nil, 0, false
	}
	return dx, s2, true
}

func evalRolling1(operator string, wx []float64) float64 {
	period := len(wx)
	switch operator {
	case "ts-sum":
		var sum float64
		for _, v := range wx {
			sum += v
		}
		if !isFiniteFloat(sum) {
			return math.Inf(1)
		}
		if sum == 0 {
			return 0
		}
		return sum
	case "ts-product":
		for _, v := range wx {
			if v == 0 {
				return 0
			}
		}
		prod := 1.0
		for _, v := range wx {
			prod *= v
		}
		if !isFiniteFloat(prod) {
			return math.Inf(1)
		}
		if prod == 0 {
			return 0
		}
		return prod
	case "ts-rank":
		if period == 1 {
			return 0.5
		}
		cur := wx[period-1]
		less, equal := 0, 0
		for _, v := range wx {
			if v < cur {
				less++
			} else if v == cur {
				equal++
			}
		}
		return (float64(less) + float64(equal-1)*0.5) / float64(period-1)
	case "ts-zscore":
		dx, s2, ok := centeredDeviations(wx)
		if !ok {
			return math.Inf(1)
		}
		if s2 == 0 {
			return 0
		}
		std := math.Sqrt(s2 / float64(period))
		if std == 0 {
			return 0
		}
		z := dx[period-1] / std
		if !isFiniteFloat(z) {
			return math.Inf(1)
		}
		if z == 0 {
			return 0
		}
		return z
	case "ts-skew":
		dx, s2, ok := centeredDeviations(wx)
		if !ok {
			return math.Inf(1)
		}
		if s2 == 0 {
			return 0
		}
		n := float64(period)
		sigma := math.Sqrt(s2 / (n - 1))
		if !isFiniteFloat(sigma) {
			return math.Inf(1)
		}
		if sigma == 0 {
			return 0
		}
		var sumZ3 float64
		for _, d := range dx {
			z := d / sigma
			sumZ3 += z * z * z
		}
		if !isFiniteFloat(sumZ3) {
			return math.Inf(1)
		}
		skew := (n / ((n - 1) * (n - 2))) * sumZ3
		if !isFiniteFloat(skew) {
			return math.Inf(1)
		}
		if skew == 0 {
			return 0
		}
		return skew
	case "ts-kurt":
		dx, s2, ok := centeredDeviations(wx)
		if !ok {
			return math.Inf(1)
		}
		if s2 == 0 {
			return 0
		}
		n := float64(period)
		sigma := math.Sqrt(s2 / (n - 1))
		if !isFiniteFloat(sigma) {
			return math.Inf(1)
		}
		if sigma == 0 {
			return 0
		}
		var sumZ4 float64
		for _, d := range dx {
			z2 := (d / sigma) * (d / sigma)
			sumZ4 += z2 * z2
		}
		if !isFiniteFloat(sumZ4) {
			return math.Inf(1)
		}
		kurt := (n*(n+1)/((n-1)*(n-2)*(n-3)))*sumZ4 - (3*(n-1)*(n-1))/((n-2)*(n-3))
		if !isFiniteFloat(kurt) {
			return math.Inf(1)
		}
		if kurt == 0 {
			return 0
		}
		return kurt
	case "ts-slope", "ts-rsquare":
		if period <= 1 {
			return 0
		}
		dy, syy, ok := centeredDeviations(wx)
		if !ok {
			return math.Inf(1)
		}
		if syy == 0 {
			return 0
		}
		n := float64(period)
		meanT := (n - 1) * 0.5
		stt := n * (n*n - 1) / 12.0
		var sty float64
		for i, d := range dy {
			sty += (float64(i) - meanT) * d
		}
		if operator == "ts-slope" {
			slope := sty / stt
			if !isFiniteFloat(slope) {
				return math.Inf(1)
			}
			if slope == 0 {
				return 0
			}
			return slope
		}
		denom := math.Sqrt(stt) * math.Sqrt(syy)
		if denom == 0 {
			return 0
		}
		r := sty / denom
		if !isFiniteFloat(r) {
			return math.Inf(1)
		}
		r2 := r * r
		if r2 > 1 {
			return 1
		}
		return r2
	default:
		return math.NaN()
	}
}

func evalRolling2(operator string, first, second []float64) float64 {
	period := len(first)
	df, sff, okF := centeredDeviations(first)
	ds, sss, okS := centeredDeviations(second)
	if !okF || !okS {
		return math.Inf(1)
	}
	var sfs float64
	for i := range df {
		sfs += df[i] * ds[i]
	}
	if !isFiniteFloat(sfs) {
		return math.Inf(1)
	}
	switch operator {
	case "ts-cov":
		cov := sfs / float64(period)
		if !isFiniteFloat(cov) {
			return math.Inf(1)
		}
		if cov == 0 {
			return 0
		}
		return cov
	case "ts-corr":
		if sff == 0 || sss == 0 {
			return 0
		}
		denom := math.Sqrt(sff) * math.Sqrt(sss)
		if denom == 0 {
			return 0
		}
		r := sfs / denom
		if !isFiniteFloat(r) {
			return math.Inf(1)
		}
		if r == 0 {
			return 0
		}
		return max(-1.0, min(1.0, r))
	case "ts-beta":
		// TsBeta(y, x, period): first is y, second is x; beta = Cov(y, x) / Var(x).
		if sss == 0 {
			return 0
		}
		beta := sfs / sss
		if !isFiniteFloat(beta) {
			return math.Inf(1)
		}
		if beta == 0 {
			return 0
		}
		return beta
	default:
		return math.NaN()
	}
}

func computeRollingBatch1(operator string, data []float64, period int) []float64 {
	out := make([]float64, len(data))
	for i := range out {
		if i < period-1 {
			out[i] = math.NaN()
		} else {
			out[i] = evalRolling1(operator, data[i-period+1:i+1])
		}
	}
	return out
}

func computeRollingBatch2(operator string, first, second []float64, period int) []float64 {
	out := make([]float64, len(first))
	for i := range out {
		if i < period-1 {
			out[i] = math.NaN()
		} else {
			out[i] = evalRolling2(operator, first[i-period+1:i+1], second[i-period+1:i+1])
		}
	}
	return out
}
