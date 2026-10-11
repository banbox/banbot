package statistics

import (
	"errors"
	"math"
	"sort"
)

// ReturnEncoder retains only fitted training boundaries. Equal quantiles may
// produce unused codes when the training distribution has atoms; ties are never
// broken with future data. Sigma codes may extend beyond the training alphabet.
type ReturnEncoder struct {
	boundaries   []float64
	origin, step float64
	sigma        bool
}

func FitQuantileEncoder(training []float64, letters int) (ReturnEncoder, error) {
	if len(training) == 0 || letters < 2 || letters > len(training) {
		return ReturnEncoder{}, errors.New("statistics: invalid quantile encoding fit")
	}
	x := append([]float64(nil), training...)
	for _, v := range x {
		if !afmlFinite(v) {
			return ReturnEncoder{}, errors.New("statistics: nonfinite encoder training")
		}
	}
	sort.Float64s(x)
	b := make([]float64, letters-1)
	for i := range b {
		h := float64(len(x)-1) * float64(i+1) / float64(letters)
		j := int(h)
		b[i] = x[j]
		if j+1 < len(x) {
			b[i] = x[j]*(1-(h-float64(j))) + x[j+1]*(h-float64(j))
		}
		if !afmlFinite(b[i]) {
			return ReturnEncoder{}, errors.New("statistics: quantile overflow")
		}
	}
	return ReturnEncoder{boundaries: b}, nil
}

// FitSigmaEncoder follows section18.5.3's fixed-width [min+k*s,min+(k+1)*s)
// intervals. The caller chooses sigma from training data only.
func FitSigmaEncoder(training []float64, sigma float64) (ReturnEncoder, error) {
	if len(training) == 0 || !afmlFinite(sigma) || sigma <= 0 {
		return ReturnEncoder{}, errors.New("statistics: invalid sigma encoding fit")
	}
	low := math.Inf(1)
	for _, v := range training {
		if !afmlFinite(v) {
			return ReturnEncoder{}, errors.New("statistics: nonfinite encoder training")
		}
		low = math.Min(low, v)
	}
	return ReturnEncoder{origin: low, step: sigma, sigma: true}, nil
}

func (e ReturnEncoder) Encode(values []float64) ([]int, error) {
	if (!e.sigma && len(e.boundaries) == 0) || (e.sigma && e.step <= 0) {
		return nil, errors.New("statistics: unfitted encoder")
	}
	out := make([]int, len(values))
	for i, v := range values {
		if !afmlFinite(v) {
			return nil, errors.New("statistics: nonfinite encoding input")
		}
		if e.sigma {
			q := math.Floor((v - e.origin) / e.step)
			if !afmlFinite(q) || q >= float64(math.MaxInt) || q <= float64(math.MinInt) {
				return nil, errors.New("statistics: encoding code overflow")
			}
			out[i] = int(q)
		} else {
			out[i] = sort.Search(len(e.boundaries), func(j int) bool { return v < e.boundaries[j] })
		}
	}
	return out, nil
}

// SignEncoding removes zero returns, retaining source indices for alignment.
func SignEncoding(values []float64) (codes, indices []int, err error) {
	for i, v := range values {
		if !afmlFinite(v) {
			return nil, nil, errors.New("statistics: nonfinite sign encoding")
		}
		if v == 0 {
			continue
		}
		code := 0
		if v > 0 {
			code = 1
		}
		codes = append(codes, code)
		indices = append(indices, i)
	}
	return
}

type InformationDistance struct{ EntropyX, EntropyY, JointEntropy, MutualInformation, NormalizedVariation float64 }

// DiscreteInformation computes empirical discrete entropies in nats and
// normalized variation VI/H(X,Y). Two constant messages have distance zero.
func DiscreteInformation(x, y []int) (InformationDistance, error) {
	r := InformationDistance{}
	if len(x) == 0 || len(x) != len(y) {
		return r, errors.New("statistics: invalid information samples")
	}
	a, b, j := map[int]int{}, map[int]int{}, map[[2]int]int{}
	for i, v := range x {
		a[v]++
		b[y[i]]++
		j[[2]int{v, y[i]}]++
	}
	entropy := func(counts []int) float64 {
		h := 0.
		for _, c := range counts {
			p := float64(c) / float64(len(x))
			h -= p * math.Log(p)
		}
		return h
	}
	ca, cb, cj := []int{}, []int{}, []int{}
	// Sorting makes floating-point accumulation reproducible across map order.
	for _, c := range a {
		ca = append(ca, c)
	}
	for _, c := range b {
		cb = append(cb, c)
	}
	for _, c := range j {
		cj = append(cj, c)
	}
	sort.Ints(ca)
	sort.Ints(cb)
	sort.Ints(cj)
	r.EntropyX, r.EntropyY, r.JointEntropy = entropy(ca), entropy(cb), entropy(cj)
	r.MutualInformation = math.Max(0, r.EntropyX+r.EntropyY-r.JointEntropy)
	if r.JointEntropy > 0 {
		r.NormalizedVariation = math.Max(0, math.Min(1, (2*r.JointEntropy-r.EntropyX-r.EntropyY)/r.JointEntropy))
	}
	return r, nil
}

// KLDivergence requires normalized probability masses. Missing support in q
// returns +Inf, the mathematical result rather than arbitrary smoothing.
func KLDivergence(p, q []float64) (float64, error) {
	if len(p) == 0 || len(p) != len(q) {
		return 0, errors.New("statistics: invalid KL masses")
	}
	a, b, result := 0., 0., 0.
	for i, v := range p {
		if !afmlFinite(v) || v < 0 || !afmlFinite(q[i]) || q[i] < 0 {
			return 0, errors.New("statistics: invalid KL probability")
		}
		a += v
		b += q[i]
	}
	if math.Abs(a-1) > 1e-10 || math.Abs(b-1) > 1e-10 {
		return 0, errors.New("statistics: KL requires normalized masses")
	}
	for i, v := range p {
		if v == 0 {
			continue
		}
		if q[i] == 0 {
			return math.Inf(1), nil
		}
		result += v * (math.Log(v) - math.Log(q[i]))
	}
	return result, nil
}

// HillDiversity returns exp(Renyi entropy), the effective category count in
// section18.7. q=1 is the Shannon limit; zero-mass categories are excluded.
func HillDiversity(probabilities []float64, q float64) (float64, error) {
	if len(probabilities) == 0 || math.IsNaN(q) || q < 0 || math.IsInf(q, -1) {
		return 0, errors.New("statistics: invalid diversity order")
	}
	mass, maxP, entropy := 0., 0., 0.
	logs := []float64{}
	for _, p := range probabilities {
		if !afmlFinite(p) || p < 0 || p > 1 {
			return 0, errors.New("statistics: invalid diversity probability")
		}
		mass += p
		maxP = math.Max(maxP, p)
		if p > 0 {
			logs = append(logs, math.Log(p))
			entropy -= p * math.Log(p)
		}
	}
	if math.Abs(mass-1) > 1e-10 {
		return 0, errors.New("statistics: diversity requires normalized mass")
	}
	if math.IsInf(q, 1) {
		return 1 / maxP, nil
	}
	if q == 0 {
		return float64(len(logs)), nil
	}
	if math.Abs(q-1) < 1e-8 {
		return math.Exp(entropy), nil
	}
	largest := math.Inf(-1)
	for _, l := range logs {
		largest = math.Max(largest, q*l)
	}
	sum := 0.
	for _, l := range logs {
		sum += math.Exp(q*l - largest)
	}
	return math.Exp((largest + math.Log(sum)) / (1 - q)), nil
}

// GaussianDifferentialEntropy returns the continuous normal entropy in nats.
// It can be negative; it is not the entropy of a discretized price message.
func GaussianDifferentialEntropy(sigma float64) (float64, error) {
	if !afmlFinite(sigma) || sigma <= 0 {
		return 0, errors.New("statistics: invalid Gaussian sigma")
	}
	return .5*math.Log(2*math.Pi*math.E) + math.Log(sigma), nil
}

// GeneralizedWeightedMean implements section18.7 power means for positive
// values and normalized nonnegative weights, including geometric and extrema
// limits. Zero-weight entries never determine the extrema.
func GeneralizedWeightedMean(values, weights []float64, q float64) (float64, error) {
	if len(values) == 0 || len(values) != len(weights) || math.IsNaN(q) {
		return 0, errors.New("statistics: invalid generalized mean")
	}
	mass, lo, hi, geo := 0., math.Inf(1), 0., 0.
	logs, ws := []float64{}, []float64{}
	for i, v := range values {
		w := weights[i]
		if !afmlFinite(v) || v <= 0 || !afmlFinite(w) || w < 0 {
			return 0, errors.New("statistics: invalid generalized mean value/mass")
		}
		mass += w
		if w == 0 {
			continue
		}
		lo = math.Min(lo, v)
		hi = math.Max(hi, v)
		l := math.Log(v)
		geo += w * l
		logs = append(logs, l)
		ws = append(ws, w)
	}
	if math.Abs(mass-1) > 1e-10 {
		return 0, errors.New("statistics: mean requires normalized mass")
	}
	if math.IsInf(q, -1) {
		return lo, nil
	}
	if math.IsInf(q, 1) {
		return hi, nil
	}
	if math.Abs(q) < 1e-8 {
		return math.Exp(geo), nil
	}
	shift := math.Log(hi)
	if q < 0 {
		shift = math.Log(lo)
	}
	sum := 0.
	for i, l := range logs {
		sum += ws[i] * math.Exp(q*(l-shift))
	}
	out := math.Exp(shift + math.Log(sum)/q)
	if !afmlFinite(out) || out <= 0 {
		return 0, errors.New("statistics: mean overflow")
	}
	return out, nil
}
