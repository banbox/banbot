package statistics

import (
	"errors"
	"math"
	"slices"
	"sort"
)

// GaussianMixture2 is a frozen two-component distribution. AFML10.2 permits a
// two-Gaussian fitting method like the cited EF3M; this fitter uses bounded EM,
// not EF3M or a claim of global optimality. Parameters remain in input units.
type GaussianMixture2 struct{ Means, Sigmas, Weights [2]float64 }
type GaussianMixtureFit struct {
	Model         GaussianMixture2
	LogLikelihood float64
	Iterations    int
}
type GaussianMixtureSpec struct {
	MaxIterations       int
	MaxWork             uint64
	Tolerance, MinSigma float64
}

func (m GaussianMixture2) validate() error {
	for i := 0; i < 2; i++ {
		if !afmlFinite(m.Means[i]) || !afmlFinite(m.Sigmas[i]) || m.Sigmas[i] <= 0 || !afmlFinite(m.Weights[i]) || m.Weights[i] <= 0 || m.Weights[i] >= 1 {
			return errors.New("statistics: invalid Gaussian mixture")
		}
	}
	if math.Abs(m.Weights[0]+m.Weights[1]-1) > 1e-12 {
		return errors.New("statistics: unnormalized Gaussian mixture")
	}
	return nil
}
func (m GaussianMixture2) CDF(x float64) (float64, error) {
	if err := m.validate(); err != nil {
		return 0, err
	}
	if !afmlFinite(x) {
		return 0, errors.New("statistics: nonfinite mixture input")
	}
	p := 0.
	for i := 0; i < 2; i++ {
		p += m.Weights[i] * .5 * math.Erfc(-(x-m.Means[i])/m.Sigmas[i]/math.Sqrt2)
	}
	return math.Max(0, math.Min(1, p)), nil
}

// ReserveBetSize implements the visually checked piecewise CDF equation10.2.
// The model must be fitted on the historical training prefix, then frozen.
// Tail differences use erfc directly, preserving small positive denominators.
func (m GaussianMixture2) ReserveBetSize(netConcurrency float64) (float64, error) {
	if err := m.validate(); err != nil {
		return 0, err
	}
	if !afmlFinite(netConcurrency) {
		return 0, errors.New("statistics: invalid net concurrency")
	}
	if netConcurrency == 0 {
		return 0, nil
	}
	base, tail := 0., 0.
	for i := 0; i < 2; i++ {
		cdf := func(x float64) float64 { return .5 * math.Erfc(-(x-m.Means[i])/m.Sigmas[i]/math.Sqrt2) }
		survival := func(x float64) float64 { return .5 * math.Erfc((x-m.Means[i])/m.Sigmas[i]/math.Sqrt2) }
		if netConcurrency > 0 {
			base += m.Weights[i] * survival(0)
			tail += m.Weights[i] * survival(netConcurrency)
		} else {
			base += m.Weights[i] * cdf(0)
			tail += m.Weights[i] * cdf(netConcurrency)
		}
	}
	if base <= 0 || !afmlFinite(base) {
		return 0, errors.New("statistics: mixture has numerically unresolved zero tail")
	}
	size := math.Max(0, math.Min(1, 1-tail/base))
	if netConcurrency < 0 {
		size = -size
	}
	return size, nil
}

// FitGaussianMixture2 uses stable log responsibilities and weighted online
// centered moments. A positive declared sigma floor prevents singular mixture
// likelihoods; it is a modeling assumption, not inferred from future samples.
func FitGaussianMixture2(training []float64, spec GaussianMixtureSpec) (GaussianMixtureFit, error) {
	n := len(training)
	if n < 4 || n > 1000000 || spec.MaxIterations < 1 || spec.MaxIterations > 10000 || spec.MaxWork < uint64(n)*uint64(spec.MaxIterations) || !afmlFinite(spec.Tolerance) || spec.Tolerance <= 0 || !afmlFinite(spec.MinSigma) || spec.MinSigma <= 0 {
		return GaussianMixtureFit{}, errors.New("statistics: invalid mixture fit budget")
	}
	x := slices.Clone(training)
	for _, v := range x {
		if !afmlFinite(v) {
			return GaussianMixtureFit{}, errors.New("statistics: nonfinite mixture training")
		}
	}
	sort.Float64s(x)
	mean, m2 := 0., 0.
	for i, v := range x {
		d := v - mean
		mean += d / float64(i+1)
		m2 += d * (v - mean)
	}
	if !afmlFinite(m2) || m2 <= 0 {
		return GaussianMixtureFit{}, errors.New("statistics: constant/overflowing mixture training")
	}
	std := math.Max(spec.MinSigma, math.Sqrt(m2/float64(n)))
	m := GaussianMixture2{[2]float64{x[n/4], x[3*n/4]}, [2]float64{std, std}, [2]float64{.5, .5}}
	prev := math.Inf(-1)
	for iteration := 1; iteration <= spec.MaxIterations; iteration++ {
		mass, means, variance := [2]float64{}, [2]float64{}, [2]float64{}
		ll := 0.
		for _, v := range x {
			logs := [2]float64{}
			for k := 0; k < 2; k++ {
				z := (v - m.Means[k]) / m.Sigmas[k]
				logs[k] = math.Log(m.Weights[k]) - math.Log(m.Sigmas[k]) - .5*math.Log(2*math.Pi) - .5*z*z
			}
			hi := math.Max(logs[0], logs[1])
			z := hi + math.Log(math.Exp(logs[0]-hi)+math.Exp(logs[1]-hi))
			if !afmlFinite(z) {
				return GaussianMixtureFit{}, errors.New("statistics: mixture likelihood overflow")
			}
			ll += z
			for k := 0; k < 2; k++ {
				w := math.Exp(logs[k] - z)
				if w == 0 {
					continue
				}
				mass[k] += w
				d := v - means[k]
				means[k] += w / mass[k] * d
				variance[k] += w * d * (v - means[k])
			}
		}
		if iteration > 1 && ll < prev-1e-8*math.Max(1, math.Abs(prev)) {
			return GaussianMixtureFit{}, errors.New("statistics: mixture likelihood decreased")
		}
		if iteration > 1 && math.Abs(ll-prev) <= spec.Tolerance*math.Max(1, math.Abs(prev)) {
			return GaussianMixtureFit{m, ll, iteration}, nil
		}
		prev = ll
		for k := 0; k < 2; k++ {
			if mass[k] <= 1e-12 || !afmlFinite(variance[k]) || variance[k] < 0 {
				return GaussianMixtureFit{}, errors.New("statistics: unidentified mixture component")
			}
			m.Means[k] = means[k]
			m.Sigmas[k] = math.Max(spec.MinSigma, math.Sqrt(variance[k]/mass[k]))
			m.Weights[k] = mass[k] / float64(n)
		}
		if err := m.validate(); err != nil {
			return GaussianMixtureFit{}, err
		}
	}
	return GaussianMixtureFit{}, errors.New("statistics: mixture fit did not converge")
}

// BudgetBetSize uses frozen train-only long/short concurrency capacities.
// Exceeding either historical capacity is flagged instead of silently clipping
// the source equation into an unexplained capital reservation assumption.
func BudgetBetSize(longs, shorts, longCapacity, shortCapacity int) (float64, error) {
	if longs < 0 || shorts < 0 || longCapacity <= 0 || shortCapacity <= 0 || longs > longCapacity || shorts > shortCapacity {
		return 0, errors.New("statistics: invalid or exceeded concurrency budget")
	}
	return float64(longs)/float64(longCapacity) - float64(shorts)/float64(shortCapacity), nil
}

// BetConcurrency counts sign-only active bets on source [Start,End) intervals.
// End times must be known at the inference cutoff, as for AverageActiveBetSizes.
func BetConcurrency(bets []ActiveBet, times []int64, maxWork uint64) (longs, shorts []int, err error) {
	if len(bets) > 1000000 || len(times) > 1000000 || maxWork < uint64(len(bets))*uint64(len(times)) {
		return nil, nil, errors.New("statistics: concurrency work budget exceeded")
	}
	for _, b := range bets {
		if !afmlFinite(b.Size) || math.Abs(b.Size) > 1 || (!b.Open && b.End < b.Start) {
			return nil, nil, errors.New("statistics: invalid concurrency bet")
		}
	}
	longs, shorts = make([]int, len(times)), make([]int, len(times))
	for i, at := range times {
		if i > 0 && at <= times[i-1] {
			return nil, nil, errors.New("statistics: concurrency times must increase")
		}
		for _, b := range bets {
			if b.Start <= at && (b.Open || at < b.End) {
				if b.Size > 0 {
					longs[i]++
				} else if b.Size < 0 {
					shorts[i]++
				}
			}
		}
	}
	return
}

// PowerBetSize is the alternative printed in10.6, sign(x)*abs(x)^omega.
// The exercise restricts x to [-1,1]; callers define the price normalization.
func PowerBetSize(omega, x float64) (float64, error) {
	if !afmlFinite(omega) || omega < 0 || !afmlFinite(x) || math.Abs(x) > 1 {
		return 0, errors.New("statistics: invalid power bet input")
	}
	if x == 0 {
		return 0, nil
	}
	return math.Copysign(math.Pow(math.Abs(x), omega), x), nil
}
func CalibratePowerBet(x, size float64) (float64, error) {
	if !afmlFinite(x) || !afmlFinite(size) || x == 0 || math.Abs(x) >= 1 || size == 0 || math.Abs(size) > 1 || math.Signbit(x) != math.Signbit(size) {
		return 0, errors.New("statistics: invalid power calibration")
	}
	return math.Log(math.Abs(size)) / math.Log(math.Abs(x)), nil
}
func InversePowerBet(size, omega float64) (float64, error) {
	if !afmlFinite(size) || math.Abs(size) > 1 || !afmlFinite(omega) || omega <= 0 {
		return 0, errors.New("statistics: noninvertible power bet")
	}
	if size == 0 {
		return 0, nil
	}
	return math.Copysign(math.Pow(math.Abs(size), 1/omega), size), nil
}
