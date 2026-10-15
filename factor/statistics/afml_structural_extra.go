package statistics

import (
	"errors"
	"math"
	"sort"

	"gonum.org/v1/gonum/mat"
)

type RecursiveCUSUMResult struct {
	Residuals, CUSUM []float64
	Scale            float64
	First            int
}

// RecursiveResidualCUSUM is the Brown-Durbin-Evans expanding RLS diagnostic.
// Initial rows fit the seed model; subsequent forecasts use only earlier rows.
// Residuals remove leverage. The source's residual standard deviation is a
// batch diagnostic normalization: historical CUSUM values are NOT PIT signals.
func RecursiveResidualCUSUM(y []float64, x [][]float64, initial int) (RecursiveCUSUMResult, error) {
	r := RecursiveCUSUMResult{First: initial}
	if len(y) != len(x) || len(x) == 0 || len(x[0]) < 1 || initial < len(x[0]) || initial >= len(y)-1 {
		return r, errors.New("statistics: invalid RLS CUSUM shape")
	}
	k := len(x[0])
	for i, row := range x {
		if len(row) != k || !afmlFinite(y[i]) {
			return r, errors.New("statistics: invalid RLS CUSUM row")
		}
		for _, v := range row {
			if !afmlFinite(v) {
				return r, errors.New("statistics: nonfinite RLS design")
			}
		}
	}
	seed, err := Regress(y[:initial], x[:initial], false)
	if err != nil {
		return r, err
	}
	beta := append([]float64(nil), seed.Beta...)
	gram := mat.NewDense(k, k, nil)
	for _, row := range x[:initial] {
		for a := 0; a < k; a++ {
			for b := 0; b < k; b++ {
				gram.Set(a, b, gram.At(a, b)+row[a]*row[b])
			}
		}
	}
	var inv mat.Dense
	if err = inv.Inverse(gram); err != nil {
		return r, err
	}
	r.Residuals = make([]float64, len(y))
	r.CUSUM = make([]float64, len(y))
	for i := 0; i < initial; i++ {
		r.Residuals[i] = math.NaN()
		r.CUSUM[i] = math.NaN()
	}
	mean, m2, n := 0., 0., 0.
	for t := initial; t < len(y); t++ {
		row := x[t]
		px := make([]float64, k)
		prediction := 0.
		for a := 0; a < k; a++ {
			prediction += row[a] * beta[a]
			for b := 0; b < k; b++ {
				px[a] += inv.At(a, b) * row[b]
			}
		}
		denom := 1.
		for a := 0; a < k; a++ {
			denom += row[a] * px[a]
		}
		if denom < 1 || !afmlFinite(denom) || !afmlFinite(prediction) {
			return RecursiveCUSUMResult{}, errors.New("statistics: invalid RLS leverage")
		}
		innovation := y[t] - prediction
		w := innovation / math.Sqrt(denom)
		if !afmlFinite(w) {
			return RecursiveCUSUMResult{}, errors.New("statistics: RLS residual overflow")
		}
		r.Residuals[t] = w
		n++
		delta := w - mean
		mean += delta / n
		m2 += delta * (w - mean)
		for a := 0; a < k; a++ {
			beta[a] += px[a] * innovation / denom
			for b := 0; b < k; b++ {
				inv.Set(a, b, inv.At(a, b)-px[a]*px[b]/denom)
			}
		}
	}
	r.Scale = math.Sqrt(m2 / (n - 1))
	if !afmlFinite(r.Scale) || r.Scale <= 0 {
		return RecursiveCUSUMResult{}, errors.New("statistics: undefined recursive residual scale")
	}
	sum := 0.
	for t := initial; t < len(y); t++ {
		sum += r.Residuals[t] / r.Scale
		r.CUSUM[t] = sum
		if !afmlFinite(sum) {
			return RecursiveCUSUMResult{}, errors.New("statistics: CUSUM overflow")
		}
	}
	return r, nil
}

type LevelCUSUMResult struct {
	Statistic, Critical float64
	Reference           int
}

// LevelCUSUM is the one-sided Chu-Stinchcombe-White endpoint test on log
// levels. It returns the maximal departure and that reference's critical bound.
// b=4.6 is the cited Monte Carlo 5% constant, not a universal calibration.
func LevelCUSUM(logValues []float64, minLag int, b float64) (LevelCUSUMResult, error) {
	r := LevelCUSUMResult{Statistic: math.Inf(-1), Reference: -1}
	if len(logValues) < 3 || minLag < 1 || minLag >= len(logValues) || !afmlFinite(b) || b <= 0 {
		return r, errors.New("statistics: invalid level CUSUM")
	}
	sq := 0.
	for i, v := range logValues {
		if !afmlFinite(v) {
			return r, errors.New("statistics: nonfinite CUSUM levels")
		}
		if i > 0 {
			d := v - logValues[i-1]
			sq += d * d
		}
	}
	end := len(logValues) - 1
	sigma := math.Sqrt(sq / float64(end))
	if !afmlFinite(sigma) || sigma <= 0 {
		return r, errors.New("statistics: undefined CUSUM innovation scale")
	}
	for n := 0; n <= end-minLag; n++ {
		lag := float64(end - n)
		s := (logValues[end] - logValues[n]) / (sigma * math.Sqrt(lag))
		if s > r.Statistic {
			r = LevelCUSUMResult{s, math.Sqrt(b + math.Log(lag)), n}
		}
	}
	return r, nil
}

// ChowDickeyFuller fits delta*y[t-1]*1[t>=breakIndex] without intercept,
// as source section17.4.1. This is a right-tail break statistic, not an ordinary
// Gaussian p-value. A pre-break random walk and post-break regime are required.
func ChowDickeyFuller(values []float64, breakIndex int) (float64, error) {
	if len(values) < 5 || breakIndex < 2 || breakIndex >= len(values)-1 {
		return 0, errors.New("statistics: invalid Chow break")
	}
	for _, v := range values {
		if !afmlFinite(v) {
			return 0, errors.New("statistics: nonfinite Chow input")
		}
	}
	y := make([]float64, len(values)-1)
	x := make([][]float64, len(y))
	for i := range y {
		t := i + 1
		y[i] = values[t] - values[t-1]
		z := 0.
		if t >= breakIndex {
			z = values[t-1]
		}
		x[i] = []float64{z}
	}
	return afmlFirstCoefficientT(y, x)
}

type ChowSupremumResult struct {
	Statistic  float64
	BreakIndex int
	Statistics []float64
}

func SupChowDickeyFuller(values []float64, first, last, maxFits int) (ChowSupremumResult, error) {
	r := ChowSupremumResult{Statistic: math.Inf(-1), BreakIndex: -1}
	if first < 2 || last < first || last >= len(values)-1 || maxFits < last-first+1 {
		return r, errors.New("statistics: invalid trimmed Chow mesh/budget")
	}
	for b := first; b <= last; b++ {
		s, err := ChowDickeyFuller(values, b)
		if err != nil {
			return ChowSupremumResult{}, err
		}
		r.Statistics = append(r.Statistics, s)
		if s > r.Statistic {
			r.Statistic = s
			r.BreakIndex = b
		}
	}
	return r, nil
}

type RobustADFResult struct {
	Quantile, QuantileDispersion, TailMean, TailStd, Maximum float64
	TailCount                                                int
}

// RobustADF summarizes the complete backward-window t-statistic population,
// following sections17.4.2.4-5. Quantiles interpolate linearly; the conditional
// distribution is the empirical mass at/above Q(q), including boundary ties.
// These finite-sample conventions are explicit because the book specifies
// continuous distribution integrals, not tie/interpolation conventions.
func RobustADF(statistics []float64, q, v float64) (RobustADFResult, error) {
	r := RobustADFResult{}
	if len(statistics) == 0 || !afmlFinite(q) || q < 0 || q > 1 || !afmlFinite(v) || v <= 0 || v > math.Min(q, 1-q) {
		return r, errors.New("statistics: invalid robust ADF quantile/dispersion")
	}
	x := append([]float64(nil), statistics...)
	for _, s := range x {
		if !afmlFinite(s) {
			return r, errors.New("statistics: invalid ADF population")
		}
	}
	sort.Float64s(x)
	quantile := func(p float64) float64 {
		h := float64(len(x)-1) * p
		j := int(h)
		if j+1 == len(x) {
			return x[j]
		}
		return x[j]*(1-h+float64(j)) + x[j+1]*(h-float64(j))
	}
	r.Quantile = quantile(q)
	r.QuantileDispersion = quantile(q+v) - quantile(q-v)
	r.Maximum = x[len(x)-1]
	mean, m2, n := 0., 0., 0.
	for _, s := range x {
		if s < r.Quantile {
			continue
		}
		n++
		d := s - mean
		mean += d / n
		m2 += d * (s - mean)
	}
	r.TailCount = int(n)
	r.TailMean = mean
	r.TailStd = math.Sqrt(m2 / n)
	if !afmlFinite(r.QuantileDispersion) || !afmlFinite(r.TailMean) || !afmlFinite(r.TailStd) {
		return RobustADFResult{}, errors.New("statistics: robust ADF overflow")
	}
	return r, nil
}

type MartingaleTrendResult struct {
	Statistic  float64
	BestStart  int
	Statistics []float64
}

// SupMartingaleTrend implements section17.4.3 SM-Poly1/Poly2/Exp/Power,
// testing the squared-time coefficient for polynomials and the linear/log-time
// coefficient for Exp/Power. Time is the global 1-based observation index,
// preserved across backward windows. The source abs(t-stat)/length^phi penalty
// is applied to each window before taking its supremum. Criticals are external.
func SupMartingaleTrend(values []float64, minLength int, model string, phi float64, maxFits int) (MartingaleTrendResult, error) {
	r := MartingaleTrendResult{Statistic: math.Inf(-1), BestStart: -1}
	if minLength < 4 || len(values) < minLength || !afmlFinite(phi) || phi < 0 || phi > 1 || maxFits < len(values)-minLength+1 || (model != "poly1" && model != "poly2" && model != "exp" && model != "power") {
		return r, errors.New("statistics: invalid martingale trend specification/budget")
	}
	y := make([]float64, len(values))
	x := make([][]float64, len(values))
	for i, v := range values {
		if !afmlFinite(v) || (model != "poly1" && v <= 0) {
			return r, errors.New("statistics: invalid martingale level")
		}
		t := float64(i + 1)
		y[i] = v
		if model != "poly1" {
			y[i] = math.Log(v)
		}
		switch model {
		case "poly1", "poly2":
			x[i] = []float64{t * t, 1, t}
		case "exp":
			x[i] = []float64{t, 1}
		case "power":
			x[i] = []float64{math.Log(t), 1}
		}
	}
	for start := 0; start <= len(values)-minLength; start++ {
		stat, err := afmlFirstCoefficientT(y[start:], x[start:])
		if err != nil {
			return MartingaleTrendResult{}, err
		}
		score := math.Abs(stat) / math.Pow(float64(len(values)-start), phi)
		if !afmlFinite(score) {
			return MartingaleTrendResult{}, errors.New("statistics: martingale statistic overflow")
		}
		r.Statistics = append(r.Statistics, score)
		if score > r.Statistic {
			r.Statistic = score
			r.BestStart = start
		}
	}
	return r, nil
}
