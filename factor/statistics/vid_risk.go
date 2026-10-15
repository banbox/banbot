package statistics

import (
	"errors"
	"math"

	"gonum.org/v1/gonum/stat/distuv"
)

// VidBrownianMaximumCDF is equation 10.9 for driftless log-price increments.
// Sigma and horizon must use the same time unit. This is a one-sided maximum
// probability, not simultaneous coverage of a two-sided pricing-period band.
func VidBrownianMaximumCDF(x, sigma, horizon float64) (float64, error) {
	if !finite(x) || x < 0 || !finite(sigma) || sigma <= 0 || !finite(horizon) || horizon <= 0 {
		return 0, errors.New("statistics: invalid Brownian maximum inputs")
	}
	scale := sigma * math.Sqrt(horizon)
	if !finite(scale) || scale <= 0 {
		return 0, errors.New("statistics: Brownian scale overflow/underflow")
	}
	// The same stable normal-tail formula used by European option pricing.
	return 1 - math.Erfc(x/scale/math.Sqrt2), nil
}

type VidPricingBoundsSpec struct {
	Spot, Sigma, MaximumProbability, FixedTargetValue, BidderShares float64
	RealizedPrices                                                  []float64
	FutureHorizons                                                  []float64
}

type VidPricingBounds struct {
	FutureLower, FutureUpper                             []float64
	AverageLower, AverageUpper, TargetLower, TargetUpper float64
}

// VidFuturePricingBounds applies equations 10.9/10.10 to equal-weight closing
// fixings. FutureHorizons are strictly increasing positive distances from the
// current spot; realized fixings are already known. The maximum probability is
// explicitly calibrated; each side/day is marginal, not a joint guarantee.
// Target quantity = bidder shares * final average / fixed target value.
func VidFuturePricingBounds(s VidPricingBoundsSpec) (VidPricingBounds, error) {
	var out VidPricingBounds
	if !finite(s.Spot) || s.Spot <= 0 || !finite(s.Sigma) || s.Sigma <= 0 || !finite(s.MaximumProbability) || s.MaximumProbability <= 0 || s.MaximumProbability >= 1 || !finite(s.FixedTargetValue) || s.FixedTargetValue <= 0 || !finite(s.BidderShares) || s.BidderShares <= 0 || len(s.FutureHorizons) == 0 || len(s.FutureHorizons)+len(s.RealizedPrices) > 100000 {
		return out, errors.New("statistics: invalid future pricing bounds")
	}
	z := distuv.UnitNormal.Quantile(.5 + .5*s.MaximumProbability)
	if !finite(z) {
		return out, errors.New("statistics: pricing quantile overflow")
	}
	count := float64(len(s.RealizedPrices) + len(s.FutureHorizons))
	for _, price := range s.RealizedPrices {
		if !finite(price) || price <= 0 {
			return VidPricingBounds{}, errors.New("statistics: invalid realized fixing")
		}
		out.AverageLower += price / count
		out.AverageUpper += price / count
	}
	previous := 0.
	for _, horizon := range s.FutureHorizons {
		if !finite(horizon) || horizon <= previous {
			return VidPricingBounds{}, errors.New("statistics: future horizons must increase")
		}
		previous = horizon
		distance := z * s.Sigma * math.Sqrt(horizon)
		lower, upper := s.Spot*math.Exp(-distance), s.Spot*math.Exp(distance)
		if !finite(distance) || !finite(lower) || lower <= 0 || !finite(upper) || upper <= 0 {
			return VidPricingBounds{}, errors.New("statistics: pricing bounds overflow/underflow")
		}
		out.FutureLower = append(out.FutureLower, lower)
		out.FutureUpper = append(out.FutureUpper, upper)
		out.AverageLower += lower / count
		out.AverageUpper += upper / count
	}
	out.TargetLower = s.BidderShares * (out.AverageLower / s.FixedTargetValue)
	out.TargetUpper = s.BidderShares * (out.AverageUpper / s.FixedTargetValue)
	if !finite(out.AverageLower) || !finite(out.AverageUpper) || !finite(out.TargetLower) || !finite(out.TargetUpper) || out.TargetLower <= 0 {
		return VidPricingBounds{}, errors.New("statistics: pricing position overflow/underflow")
	}
	return out, nil
}

// VidMergerFailureProbability is equation 11.3. Spread and breakSpread EXCLUDE
// cash: S = ratio*bidder - target. BreakSpread is an externally assessed scenario
// at the common settlement horizon, not an observed future terminal price.
func VidMergerFailureProbability(spread, breakSpread, cash, rate, years float64) (float64, error) {
	if !finite(spread) || spread <= 0 || !finite(breakSpread) || breakSpread <= 0 || !finite(cash) || cash < 0 || !finite(rate) || !finite(years) || years <= 0 {
		return 0, errors.New("statistics: invalid merger failure scenario")
	}
	growth, denominator := math.Exp(rate*years), breakSpread+cash
	if !finite(growth) || growth <= 0 || !finite(denominator) || denominator <= 0 {
		return 0, errors.New("statistics: invalid merger discount/denominator")
	}
	p := (growth*spread + cash) / denominator
	if !finite(p) || p <= 0 || p > 1 {
		return 0, errors.New("statistics: merger scenario implies invalid probability")
	}
	return p, nil
}

// VidUpdateCashlessFailureProbability implements corrected 11.7/11.8:
// p2 = p1 exp(-r*deltaYears) S2/S1. It is only for zero-cash deals; the initial
// probability and forward interval interest rate are explicit calibrations.
func VidUpdateCashlessFailureProbability(p1, s1, s2, cash, rate, deltaYears float64) (float64, error) {
	if !finite(p1) || p1 <= 0 || p1 > 1 || !finite(s1) || s1 <= 0 || !finite(s2) || s2 <= 0 || cash != 0 || !finite(rate) || !finite(deltaYears) || deltaYears <= 0 {
		return 0, errors.New("statistics: invalid cashless probability update")
	}
	// Log arithmetic avoids a transient overflow in s2/s1.
	p := math.Exp(math.Log(p1) - rate*deltaYears + math.Log(s2) - math.Log(s1))
	if !finite(p) || p <= 0 || p > 1 {
		return 0, errors.New("statistics: updated merger probability outside (0,1]")
	}
	return p, nil
}

// VidMixtureReturnQuantile bisects equation 11.9, returning a signed return
// quantile, NOT a weighted average of component quantiles. CDFs are supplied
// calibrated nondecreasing distribution functions in the same dollar/unit
// convention. Bounds must bracket the requested lower-tail probability.
func VidMixtureReturnQuantile(failureProbability, tail, lower, upper, tolerance float64, iterations int, successCDF, failureCDF func(float64) float64) (float64, error) {
	if !finite(failureProbability) || failureProbability < 0 || failureProbability > 1 || !finite(tail) || tail <= 0 || tail >= 1 || !finite(lower) || !finite(upper) || lower >= upper || !finite(tolerance) || tolerance <= 0 || iterations < 1 || iterations > 10000 || successCDF == nil || failureCDF == nil {
		return 0, errors.New("statistics: invalid mixture quantile specification")
	}
	cdf := func(x float64) (float64, error) {
		a, b := successCDF(x), failureCDF(x)
		if !finite(a) || a < 0 || a > 1 || !finite(b) || b < 0 || b > 1 {
			return 0, errors.New("statistics: invalid component CDF value")
		}
		return (1-failureProbability)*a + failureProbability*b, nil
	}
	loCDF, err := cdf(lower)
	if err != nil {
		return 0, err
	}
	hiCDF, err := cdf(upper)
	if err != nil {
		return 0, err
	}
	if loCDF >= tail || hiCDF < tail {
		return 0, errors.New("statistics: mixture quantile is not bracketed")
	}
	for range iterations {
		if upper-lower <= tolerance {
			return upper, nil
		}
		mid := lower/2 + upper/2
		if mid == lower || mid == upper {
			return 0, errors.New("statistics: mixture quantile precision exhausted")
		}
		p, e := cdf(mid)
		if e != nil {
			return 0, e
		}
		if p < loCDF || p > hiCDF {
			return 0, errors.New("statistics: component mixture is not monotone")
		}
		if p < tail {
			lower, loCDF = mid, p
		} else {
			upper, hiCDF = mid, p
		}
	}
	if upper-lower <= tolerance {
		return upper, nil
	}
	return 0, errors.New("statistics: mixture quantile iteration budget exhausted")
}
