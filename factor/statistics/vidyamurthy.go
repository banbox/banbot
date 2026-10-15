package statistics

import (
	"errors"
	"gonum.org/v1/gonum/mat"
	"math"
	"math/rand/v2"
	"sort"
)

type DurationPopulation struct {
	Lower, Median, Upper float64
	Samples              int
}

// LaggedSpreadFilter retains covariance between recent filtered log-spreads.
// Ch12 lag1 extrapolates two states; lag2 combines consecutive changes at their
// covariance-aware minimum-variance weight; lag>=3 averages the lag changes.
// Unlike the book's >=3 uncorrelated-state approximation, full covariance is
// retained consistently. Fields are private to preserve valid state ownership.
type LaggedSpreadFilter struct {
	lag               int
	sourceIndependent bool
	processVariance   float64
	means             []float64
	covariance        [][]float64
}
type SpreadObservation struct{ Prediction, PredictionVariance, Filtered, Variance, SlopeWeight float64 }

func NewLaggedSpreadFilter(lag int, processVariance, initialVariance, initialValue float64) (*LaggedSpreadFilter, error) {
	if lag < 1 || lag > 64 || !finite(processVariance) || !finite(initialVariance) || !finite(initialValue) || processVariance < 0 || initialVariance <= 0 {
		return nil, errors.New("statistics: invalid lagged-spread initialization")
	}
	n := lag + 1
	k := &LaggedSpreadFilter{lag: lag, processVariance: processVariance, means: make([]float64, n), covariance: make([][]float64, n)}
	// Until observed separately, repeated initialization states represent the
	// same uncertain value and therefore share covariance, not independent draws.
	for i := range n {
		k.means[i] = initialValue
		k.covariance[i] = make([]float64, n)
		for j := range n {
			k.covariance[i][j] = initialVariance
		}
	}
	return k, nil
}

// NewSourceLaggedSpreadFilter follows the Chapter12 appendix's uncorrelated
// historical-estimate approximation for lag>=3. Lag1/2 retain the corrected
// covariance recursions. processVariance=0 matches the appendix's omitted Q.
func NewSourceLaggedSpreadFilter(lag int, processVariance, initialVariance, initialValue float64) (*LaggedSpreadFilter, error) {
	k, err := NewLaggedSpreadFilter(lag, processVariance, initialVariance, initialValue)
	if err != nil {
		return nil, err
	}
	k.sourceIndependent = lag >= 3
	if k.sourceIndependent {
		for i := range k.covariance {
			for j := range k.covariance[i] {
				if i != j {
					k.covariance[i][j] = 0
				}
			}
		}
	}
	return k, nil
}

func (k *LaggedSpreadFilter) Observe(value, observationVariance float64) (SpreadObservation, error) {
	if k == nil || !finite(value) || !finite(observationVariance) || observationVariance <= 0 {
		return SpreadObservation{}, errors.New("statistics: invalid lagged-spread observation")
	}
	n := len(k.means)
	a := make([]float64, n)
	weight := 1.
	switch k.lag {
	case 1:
		a[0], a[1] = 2, -1
	case 2:
		p := k.covariance
		v1 := p[0][0] + p[1][1] - 2*p[0][1]
		v2 := p[1][1] + p[2][2] - 2*p[1][2]
		cross := p[0][1] - p[0][2] - p[1][1] + p[1][2]
		den := v1 + v2 - 2*cross
		weight = .5
		if den > 1e-12*math.Max(math.Abs(v1), math.Abs(v2)) {
			weight = (v2 - cross) / den
		}
		a[0], a[1], a[2] = 1+weight, 1-2*weight, -(1 - weight)
	default:
		a[0], a[k.lag] = 1+1/float64(k.lag), -1/float64(k.lag)
	}
	prior := make([]float64, n)
	for i, x := range k.means {
		prior[0] += a[i] * x
	}
	for i := 1; i < n; i++ {
		prior[i] = k.means[i-1]
	}
	covariance := make([][]float64, n)
	for i := range n {
		covariance[i] = make([]float64, n)
	}
	for i := range n {
		for j := range n {
			if k.sourceIndependent && i != j {
				continue
			}
			if i == 0 && j == 0 {
				for r := range n {
					for s := range n {
						covariance[i][j] += a[r] * k.covariance[r][s] * a[s]
					}
				}
				covariance[i][j] += k.processVariance
			} else if i == 0 {
				for r := range n {
					covariance[i][j] += a[r] * k.covariance[r][j-1]
				}
			} else if j == 0 {
				for r := range n {
					covariance[i][j] += k.covariance[i-1][r] * a[r]
				}
			} else {
				covariance[i][j] = k.covariance[i-1][j-1]
			}
		}
	}
	innovationVariance := covariance[0][0] + observationVariance
	if !finite(innovationVariance) || innovationVariance <= 0 {
		return SpreadObservation{}, errors.New("statistics: lagged-spread prediction overflow")
	}
	gain := covariance[0][0] / innovationVariance
	next := make([]float64, n)
	for i := range n {
		next[i] = prior[i]
		if i == 0 {
			next[i] += gain * (value - prior[0])
		}
		if !finite(next[i]) {
			return SpreadObservation{}, errors.New("statistics: lagged-spread update overflow")
		}
	}
	updated := make([][]float64, n)
	for i := range n {
		updated[i] = make([]float64, n)
		for j := range n {
			// Source slopes use X(t-j|t-j), not retrospectively smoothed
			// X(t-j|t). Keep historical estimates and their covariance fixed.
			updated[i][j] = covariance[i][j]
			if i == 0 && j == 0 {
				updated[i][j] = (1-gain)*(1-gain)*covariance[0][0] + gain*gain*observationVariance
			} else if i == 0 || j == 0 {
				updated[i][j] *= 1 - gain
			}
			if !finite(updated[i][j]) {
				return SpreadObservation{}, errors.New("statistics: lagged-spread covariance overflow")
			}
		}
	}
	if updated[0][0] < -1e-12 {
		return SpreadObservation{}, errors.New("statistics: negative lagged-spread variance")
	}
	out := SpreadObservation{prior[0], covariance[0][0], next[0], math.Max(0, updated[0][0]), weight}
	k.means, k.covariance = next, updated
	return out, nil
}

// BootstrapDurations follows Vidyamurthy ch7: resample completed intervals
// individually to estimate their population, not the sampling distribution of
// an estimated median. Seed/draws/percentiles are explicit calibration choices.
func BootstrapDurations(intervals []int, draws int, seed uint64, lower, upper float64) (DurationPopulation, error) {
	if len(intervals) < 2 || draws < 2 || draws > 1000000 || !finite(lower) || !finite(upper) || lower < 0 || upper > 1 || lower >= .5 || upper <= .5 {
		return DurationPopulation{}, errors.New("statistics: invalid duration bootstrap")
	}
	for _, v := range intervals {
		if v <= 0 {
			return DurationPopulation{}, errors.New("statistics: nonpositive bootstrap interval")
		}
	}
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	sample := make([]int, draws)
	for i := range sample {
		sample[i] = intervals[rng.IntN(len(intervals))]
	}
	lo, e := DurationQuantile(sample, lower)
	if e != nil {
		return DurationPopulation{}, e
	}
	median, e := DurationQuantile(sample, .5)
	if e != nil {
		return DurationPopulation{}, e
	}
	hi, e := DurationQuantile(sample, upper)
	if e != nil {
		return DurationPopulation{}, e
	}
	return DurationPopulation{lo, median, hi, draws}, nil
}

type RegularizationChoice struct {
	Lambda    float64
	FitErrors []float64
	Smoothed  []float64
}

// RegularizationHeel chooses the earliest positive-curvature heel of the
// fit-error vs log(lambda) curve described in ch8. The discrete normalized
// curvature criterion and candidate grid are explicit numerical choices; the
// book supplies a visual heel, not a unique automated selection algorithm.
func RegularizationHeel(counts, lambdas []float64) (RegularizationChoice, error) {
	if len(counts) < 2 || len(lambdas) < 3 {
		return RegularizationChoice{}, errors.New("statistics: insufficient regularization curve")
	}
	choice := RegularizationChoice{FitErrors: make([]float64, len(lambdas))}
	curves := make([][]float64, len(lambdas))
	xs := make([]float64, len(lambdas))
	for i, l := range lambdas {
		if !finite(l) || l <= 0 || i > 0 && l <= lambdas[i-1] {
			return choice, errors.New("statistics: lambda grid must increase positively")
		}
		xs[i] = math.Log(l)
		z, e := SmoothThresholdCounts(counts, l)
		if e != nil {
			return choice, e
		}
		curves[i] = z
		for j, x := range counts {
			d := x - z[j]
			choice.FitErrors[i] += d * d
		}
		if !finite(choice.FitErrors[i]) {
			return choice, errors.New("statistics: regularization fit overflow")
		}
	}
	xrange := xs[len(xs)-1] - xs[0]
	yrange := choice.FitErrors[len(xs)-1] - choice.FitErrors[0]
	best, at := 0., 0
	if yrange > 0 {
		for i := 1; i < len(xs)-1; i++ {
			left := (choice.FitErrors[i] - choice.FitErrors[i-1]) / yrange / ((xs[i] - xs[i-1]) / xrange)
			right := (choice.FitErrors[i+1] - choice.FitErrors[i]) / yrange / ((xs[i+1] - xs[i]) / xrange)
			second := 2 * (right - left) / ((xs[i+1] - xs[i-1]) / xrange)
			slope := (left + right) / 2
			curvature := second / math.Pow(1+slope*slope, 1.5)
			if curvature > best {
				best, at = curvature, i
			}
		}
	}
	choice.Lambda = lambdas[at]
	choice.Smoothed = curves[at]
	return choice, nil
}

type CommonFactorPair struct{ Correlation, AbsoluteCorrelation, GammaAB, GammaBA, ResidualVarianceAB, ResidualVarianceBA float64 }

// CompareCommonFactors excludes specific variances, as ch6 requires. GammaAB
// hedges A by B; GammaBA is the reverse orientation. A PSD covariance is
// required so correlations retain their probabilistic meaning.
func CompareCommonFactors(a, b []float64, covariance [][]float64) (CommonFactorPair, error) {
	n := len(a)
	if n == 0 || len(b) != n || len(covariance) != n {
		return CommonFactorPair{}, errors.New("statistics: invalid common-factor dimensions")
	}
	matrix := mat.NewSymDense(n, nil)
	for i, row := range covariance {
		if len(row) != n {
			return CommonFactorPair{}, errors.New("statistics: ragged common-factor covariance")
		}
		for j, x := range row {
			if !finite(x) || len(covariance[j]) != n || math.Abs(x-covariance[j][i]) > 1e-12*math.Max(1, math.Abs(x)) {
				return CommonFactorPair{}, errors.New("statistics: invalid common-factor covariance")
			}
			if j <= i {
				matrix.SetSym(i, j, x)
			}
		}
	}
	var eigen mat.EigenSym
	if !eigen.Factorize(matrix, false) {
		return CommonFactorPair{}, errors.New("statistics: factor covariance eigendecomposition failed")
	}
	values := eigen.Values(nil)
	scale := math.Max(1, slicesMax(values))
	if values[0] < -1e-12*scale {
		return CommonFactorPair{}, errors.New("statistics: factor covariance is not PSD")
	}
	aa, ab, bb := 0., 0., 0.
	for i := range n {
		if !finite(a[i]) || !finite(b[i]) {
			return CommonFactorPair{}, errors.New("statistics: nonfinite common-factor exposure")
		}
		for j := range n {
			aa += a[i] * covariance[i][j] * a[j]
			ab += a[i] * covariance[i][j] * b[j]
			bb += b[i] * covariance[i][j] * b[j]
		}
	}
	if aa <= 0 || bb <= 0 || !finite(aa) || !finite(ab) || !finite(bb) {
		return CommonFactorPair{}, errors.New("statistics: zero/overflowing factor variance")
	}
	rho := (ab / math.Sqrt(aa)) / math.Sqrt(bb)
	if !finite(rho) || math.Abs(rho) > 1+1e-10 {
		return CommonFactorPair{}, errors.New("statistics: invalid common-factor correlation")
	}
	rho = math.Max(-1, math.Min(1, rho))
	return CommonFactorPair{rho, math.Abs(rho), ab / bb, ab / aa, math.Max(0, aa-ab*(ab/bb)), math.Max(0, bb-ab*(ab/aa))}, nil
}

func slicesMax(x []float64) float64 {
	v := x[0]
	for _, z := range x {
		v = math.Max(v, z)
	}
	return v
}

// InterpolateMonotoneTail removes step plateaus using linear interpolation
// between adjacent change points, preserving nonincreasing tail counts.
func InterpolateMonotoneTail(counts []float64) ([]float64, error) {
	if len(counts) == 0 {
		return nil, errors.New("statistics: empty tail curve")
	}
	out := append([]float64(nil), counts...)
	for i, x := range counts {
		if !finite(x) || x < 0 || i > 0 && x > counts[i-1] {
			return nil, errors.New("statistics: tail counts must decrease")
		}
	}
	anchors := []int{0}
	for i := 1; i < len(counts); i++ {
		if counts[i] != counts[i-1] {
			anchors = append(anchors, i)
		}
	}
	if anchors[len(anchors)-1] != len(counts)-1 {
		anchors = append(anchors, len(counts)-1)
	}
	sort.Ints(anchors)
	for i := 1; i < len(anchors); i++ {
		lo, hi := anchors[i-1], anchors[i]
		for j := lo + 1; j < hi; j++ {
			out[j] = counts[lo] + (counts[hi]-counts[lo])*float64(j-lo)/float64(hi-lo)
		}
	}
	return out, nil
}
