package statistics

import (
	"errors"
	"math"
)

// RollEstimate retains inadmissible moment estimates instead of silently
// turning positive serial covariance into a zero spread. Spread is in price
// units; NoiseVariance excludes the model's microstructure component.
type RollEstimate struct {
	Spread, NoiseVariance, ChangeVariance, LagCovariance float64
	SpreadValid, NoiseVarianceValid                      bool
}

// RollFromMoments implements chapter 19.3.2: spread=2*sqrt(-covariance),
// efficient-price noise variance=observed variance+2*covariance.
// A positive covariance violates the model and gives NaN spread. A negative
// noise-variance estimate remains visible and has NoiseVarianceValid=false.
func RollFromMoments(changeVariance, lagCovariance float64) (RollEstimate, error) {
	if !afmlFinite(changeVariance) || changeVariance < 0 || !afmlFinite(lagCovariance) {
		return RollEstimate{}, errors.New("statistics: invalid Roll moments")
	}
	noise := changeVariance + 2*lagCovariance
	if !afmlFinite(noise) {
		return RollEstimate{}, errors.New("statistics: Roll noise-variance overflow")
	}
	out := RollEstimate{math.NaN(), noise, changeVariance, lagCovariance, lagCovariance <= 0, noise >= 0}
	if out.SpreadValid {
		out.Spread = 2 * math.Sqrt(-lagCovariance)
	}
	return out, nil
}

// RollPriceEstimate estimates moments from successive raw price differences.
// ddof is explicitly 0 (population) or 1 (sample); the book specifies moments,
// not a finite-sample normalization. Variance uses all differences, covariance
// uses paired adjacent differences with each side separately centered.
func RollPriceEstimate(close []float64, ddof int) (RollEstimate, error) {
	if len(close) < 4 || (ddof != 0 && ddof != 1) {
		return RollEstimate{}, errors.New("statistics: Roll needs four prices and ddof zero or one")
	}
	changes := make([]float64, len(close)-1)
	mean, m2 := 0., 0.
	for i, price := range close {
		if !afmlFinite(price) {
			return RollEstimate{}, errors.New("statistics: Roll requires finite prices")
		}
		if i == 0 {
			continue
		}
		value := price - close[i-1]
		if !afmlFinite(value) {
			return RollEstimate{}, errors.New("statistics: Roll price-change overflow")
		}
		changes[i-1] = value
		delta := value - mean
		mean += delta / float64(i)
		m2 += delta * (value - mean)
	}
	meanX, meanY, covarianceSum := 0., 0., 0.
	for i := 1; i < len(changes); i++ {
		x, y := changes[i-1], changes[i]
		dx, dy := x-meanX, y-meanY
		meanX += dx / float64(i)
		meanY += dy / float64(i)
		covarianceSum += dx * (y - meanY)
	}
	return RollFromMoments(m2/float64(len(changes)-ddof), covarianceSum/float64(len(changes)-1-ddof))
}

// HighLowEstimates preserves the input bar alignment; warmup values are NaN.
// Spread is a fraction of price, Sigma is unannualized Becker-Parkinson sigma.
type HighLowEstimates struct {
	Beta, Gamma, Alpha, Spread, Sigma []float64
}

func afmlLogRanges(high, low []float64) ([]float64, error) {
	if len(high) == 0 || len(high) != len(low) {
		return nil, errors.New("statistics: invalid high-low dimensions")
	}
	out := make([]float64, len(high))
	for i := range high {
		if !afmlFinite(high[i]) || !afmlFinite(low[i]) || low[i] <= 0 || high[i] < low[i] {
			return nil, errors.New("statistics: high-low prices must be finite, positive and ordered")
		}
		// Separate logs avoid overflow in high/low for extreme valid prices.
		out[i] = math.Log(high[i]) - math.Log(low[i])
	}
	return out, nil
}

// ParkinsonVolatility is chapter 19.3.3's sqrt(mean(log(H/L)^2)/(4 log 2))
// on trailing window bars. It is not annualized and assumes continuous GBM
// observations without drift, spread or overnight jumps.
func ParkinsonVolatility(high, low []float64, window int) ([]float64, error) {
	if window < 1 {
		return nil, errors.New("statistics: Parkinson window must be positive")
	}
	ranges, err := afmlLogRanges(high, low)
	if err != nil {
		return nil, err
	}
	out := make([]float64, len(ranges))
	sum := 0.
	for i, value := range ranges {
		sum += value * value
		if i >= window {
			sum -= ranges[i-window] * ranges[i-window]
		}
		out[i] = math.NaN()
		if i >= window-1 {
			out[i] = math.Sqrt(math.Max(0, sum) / (float64(window) * 4 * math.Ln2))
		}
	}
	return out, nil
}

// CorwinSchultz implements source snippets 19.1 and 19.2. betaWindow smooths
// consecutive TWO-bar squared-log-range sums; gamma uses the current two-bar
// combined high/low, as printed (not a smoothed gamma). Alpha and sigma clamp
// negative estimates to zero. Full warmup ends at index betaWindow.
// No overnight-gap adjustment is applied: none appears in the supplied snippet.
func CorwinSchultz(high, low []float64, betaWindow int) (HighLowEstimates, error) {
	if betaWindow < 1 {
		return HighLowEstimates{}, errors.New("statistics: Corwin-Schultz window must be positive")
	}
	ranges, err := afmlLogRanges(high, low)
	if err != nil {
		return HighLowEstimates{}, err
	}
	n := len(ranges)
	out := HighLowEstimates{make([]float64, n), make([]float64, n), make([]float64, n), make([]float64, n), make([]float64, n)}
	den, k2 := 3-2*math.Sqrt2, math.Sqrt(8/math.Pi)
	sum := 0.
	for i := range ranges {
		out.Beta[i], out.Gamma[i], out.Alpha[i], out.Spread[i], out.Sigma[i] = math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()
		if i == 0 {
			continue
		}
		sum += ranges[i]*ranges[i] + ranges[i-1]*ranges[i-1]
		if i > betaWindow {
			at := i - betaWindow
			sum -= ranges[at]*ranges[at] + ranges[at-1]*ranges[at-1]
		}
		combined := math.Log(math.Max(high[i], high[i-1])) - math.Log(math.Min(low[i], low[i-1]))
		gamma := combined * combined
		out.Gamma[i] = gamma
		if i < betaWindow {
			continue
		}
		beta := math.Max(0, sum/float64(betaWindow))
		alpha := math.Max(0, (math.Sqrt2-1)*math.Sqrt(beta)/den-math.Sqrt(gamma/den))
		sigma := math.Max(0, (1/math.Sqrt2-1)*math.Sqrt(beta)/(k2*den)+math.Sqrt(gamma/(k2*k2*den)))
		out.Beta[i], out.Alpha[i] = beta, alpha
		// Algebraically identical to 2*(exp(alpha)-1)/(1+exp(alpha)),
		// avoiding overflow for large alpha.
		out.Spread[i], out.Sigma[i] = 2*math.Tanh(alpha/2), sigma
	}
	return out, nil
}
