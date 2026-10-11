package statistics

import (
	"errors"
	"math"
)

type EuropeanOptionSpec struct {
	Spot, Strike, Years, Volatility, Rate, DividendYield float64
	Call                                                 bool
}

type EuropeanOptionValue struct {
	Price, Delta, Gamma, Vega, Theta, Rho float64
	GreeksValid                           bool
}

// PriceEuropeanOption supplies a constant-volatility Black-Scholes-Merton
// benchmark for synthetic European options, not an American exercise or smile
// model. Vega/rho are per UNIT volatility/rate; theta is per YEAR. Published
// market quotes and Greeks remain authoritative inputs to source strategies.
func PriceEuropeanOption(s EuropeanOptionSpec) (EuropeanOptionValue, error) {
	var out EuropeanOptionValue
	if !afmlFinite(s.Spot) || s.Spot <= 0 || !afmlFinite(s.Strike) || s.Strike <= 0 || !afmlFinite(s.Years) || s.Years < 0 || !afmlFinite(s.Volatility) || s.Volatility < 0 || !afmlFinite(s.Rate) || !afmlFinite(s.DividendYield) {
		return out, errors.New("statistics: invalid European option inputs")
	}
	if s.Years == 0 {
		difference := s.Spot - s.Strike
		if s.Call {
			out.Price = math.Max(0, difference)
			if difference > 0 {
				out.Delta = 1
			}
		} else {
			out.Price = math.Max(0, -difference)
			if difference < 0 {
				out.Delta = -1
			}
		}
		// At expiry Greeks are discontinuous/distribution-valued; delta away
		// from the strike is useful, but this is not a valid smooth Greek tuple.
		return out, nil
	}
	discountR, discountQ := math.Exp(-s.Rate*s.Years), math.Exp(-s.DividendYield*s.Years)
	a, b := s.Spot*discountQ, s.Strike*discountR
	if !afmlFinite(a) || !afmlFinite(b) || a <= 0 || b <= 0 {
		return out, errors.New("statistics: option discount overflow/underflow")
	}
	if s.Volatility == 0 {
		if s.Call {
			out.Price = math.Max(0, a-b)
			if a > b {
				out.Delta, out.Rho, out.Theta = discountQ, s.Years*b, s.DividendYield*a-s.Rate*b
			}
		} else {
			out.Price = math.Max(0, b-a)
			if b > a {
				out.Delta, out.Rho, out.Theta = -discountQ, -s.Years*b, s.Rate*b-s.DividendYield*a
			}
		}
		out.GreeksValid = a != b
		return out, nil
	}
	root := math.Sqrt(s.Years)
	sigmaT := s.Volatility * root
	d1 := ((math.Log(s.Spot) - math.Log(s.Strike)) + (s.Rate-s.DividendYield+.5*s.Volatility*s.Volatility)*s.Years) / sigmaT
	d2 := d1 - sigmaT
	if !afmlFinite(d1) || !afmlFinite(d2) {
		return out, errors.New("statistics: option normal-coordinate overflow")
	}
	phi := math.Exp(-.5*d1*d1) / math.Sqrt(2*math.Pi)
	N := func(x float64) float64 { return .5 * math.Erfc(-x/math.Sqrt2) }
	out.Gamma, out.Vega = discountQ*phi/s.Spot/sigmaT, a*phi*root
	commonTheta := -a * phi * s.Volatility / (2 * root)
	if s.Call {
		out.Price, out.Delta = a*N(d1)-b*N(d2), discountQ*N(d1)
		out.Theta, out.Rho = commonTheta-s.Rate*b*N(d2)+s.DividendYield*a*N(d1), s.Years*b*N(d2)
	} else {
		out.Price, out.Delta = b*N(-d2)-a*N(-d1), -discountQ*N(-d1)
		out.Theta, out.Rho = commonTheta+s.Rate*b*N(-d2)-s.DividendYield*a*N(-d1), -s.Years*b*N(-d2)
	}
	for _, value := range []float64{out.Price, out.Delta, out.Gamma, out.Vega, out.Theta, out.Rho} {
		if !afmlFinite(value) {
			return EuropeanOptionValue{}, errors.New("statistics: option price/Greek overflow")
		}
	}
	// Tiny cancellation errors can appear at the zero-price bound.
	out.Price = math.Max(0, out.Price)
	out.GreeksValid = true
	return out, nil
}

// EuropeanImpliedVolatility solves a European quote with declared absolute
// price tolerance, volatility cap and iteration budget. Expired/no-arbitrage
// invalid quotes and nonconverged caps/budgets are errors, not inferred smiles.
func EuropeanImpliedVolatility(s EuropeanOptionSpec, price, tolerance, maxVolatility float64, maxIterations int) (float64, error) {
	if s.Years <= 0 || !afmlFinite(price) || price < 0 || !afmlFinite(tolerance) || tolerance <= 0 || !afmlFinite(maxVolatility) || maxVolatility <= 0 || maxIterations < 1 || maxIterations > 10000 {
		return 0, errors.New("statistics: invalid implied-volatility bounds")
	}
	s.Volatility = 0
	lowValue, err := PriceEuropeanOption(s)
	if err != nil {
		return 0, err
	}
	upper := s.Spot * math.Exp(-s.DividendYield*s.Years)
	if !s.Call {
		upper = s.Strike * math.Exp(-s.Rate*s.Years)
	}
	if price < lowValue.Price-tolerance || price >= upper {
		return 0, errors.New("statistics: option price outside European arbitrage bounds")
	}
	if math.Abs(price-lowValue.Price) <= tolerance {
		return 0, nil
	}
	s.Volatility = maxVolatility
	highValue, err := PriceEuropeanOption(s)
	if err != nil {
		return 0, err
	}
	if highValue.Price < price-tolerance {
		return 0, errors.New("statistics: implied volatility exceeds configured cap")
	}
	lo, hi := 0., maxVolatility
	for i := 0; i < maxIterations; i++ {
		mid := lo + (hi-lo)/2
		s.Volatility = mid
		value, err := PriceEuropeanOption(s)
		if err != nil {
			return 0, err
		}
		if math.Abs(value.Price-price) <= tolerance {
			return mid, nil
		}
		if value.Price > price {
			hi = mid
		} else {
			lo = mid
		}
	}
	return 0, errors.New("statistics: implied volatility iteration budget exhausted")
}

// PutCallImpliedSpot uses matched European call/put quotes and known carry.
// It must not mix strikes, expiry, quote publication cutoffs or exercise styles.
func PutCallImpliedSpot(call, put, strike, years, rate, dividendYield float64) (float64, error) {
	if !afmlFinite(call) || call < 0 || !afmlFinite(put) || put < 0 || !afmlFinite(strike) || strike <= 0 || !afmlFinite(years) || years < 0 || !afmlFinite(rate) || !afmlFinite(dividendYield) {
		return 0, errors.New("statistics: invalid put-call parity inputs")
	}
	spot := (call - put + strike*math.Exp(-rate*years)) * math.Exp(dividendYield*years)
	if !afmlFinite(spot) || spot <= 0 {
		return 0, errors.New("statistics: invalid implied underlying price")
	}
	return spot, nil
}
