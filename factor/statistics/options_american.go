package statistics

import (
	"fmt"
	"math"
)

// AmericanOptionPrice uses a bounded Cox-Ross-Rubinstein tree, continuous
// dividend yield and early exercise at every node. Steps controls convergence;
// discrete cash dividends and volatility smiles require a different model.
func AmericanOptionPrice(s EuropeanOptionSpec, steps int) (float64, error) {
	if steps < 1 || steps > 5000 || !finite(s.Spot) || s.Spot <= 0 || !finite(s.Strike) || s.Strike <= 0 || !finite(s.Years) || s.Years < 0 || !finite(s.Volatility) || s.Volatility < 0 || !finite(s.Rate) || !finite(s.DividendYield) {
		return 0, fmt.Errorf("statistics: invalid American option tree specification")
	}
	intrinsic := func(spot float64) float64 {
		if s.Call {
			return math.Max(0, spot-s.Strike)
		}
		return math.Max(0, s.Strike-spot)
	}
	if s.Years == 0 {
		return intrinsic(s.Spot), nil
	}
	if s.Volatility == 0 {
		best := intrinsic(s.Spot)
		for i := 1; i <= steps; i++ {
			t := s.Years * float64(i) / float64(steps)
			spot := s.Spot * math.Exp((s.Rate-s.DividendYield)*t)
			payoff := math.Exp(-s.Rate*t) * intrinsic(spot)
			if !finite(payoff) {
				return 0, fmt.Errorf("statistics: deterministic option overflow")
			}
			best = math.Max(best, payoff)
		}
		return best, nil
	}
	dt := s.Years / float64(steps)
	logU := s.Volatility * math.Sqrt(dt)
	u, d := math.Exp(logU), math.Exp(-logU)
	growth := math.Exp((s.Rate - s.DividendYield) * dt)
	prob := (growth - d) / (u - d)
	discount := math.Exp(-s.Rate * dt)
	if !finite(prob) || prob < 0 || prob > 1 || !finite(discount) {
		return 0, fmt.Errorf("statistics: CRR tree probability inadmissible; increase steps")
	}
	values := make([]float64, steps+1)
	for j := 0; j <= steps; j++ {
		spot := s.Spot * math.Exp(float64(2*j-steps)*logU)
		if !finite(spot) {
			return 0, fmt.Errorf("statistics: CRR terminal overflow")
		}
		values[j] = intrinsic(spot)
	}
	for i := steps - 1; i >= 0; i-- {
		for j := 0; j <= i; j++ {
			spot := s.Spot * math.Exp(float64(2*j-i)*logU)
			continuation := discount * (prob*values[j+1] + (1-prob)*values[j])
			values[j] = math.Max(continuation, intrinsic(spot))
			if !finite(values[j]) {
				return 0, fmt.Errorf("statistics: CRR backward overflow")
			}
		}
	}
	return values[0], nil
}

// AmericanImpliedVolatility returns a CRR-model root on a caller-declared
// bracket. A price in a flat immediate-exercise plateau is unidentifiable and
// rejected rather than reporting an arbitrary zero volatility.
func AmericanImpliedVolatility(s EuropeanOptionSpec, premium, low, high float64, steps, iterations int, tolerance float64) (float64, error) {
	if !finite(premium) || premium < 0 || !finite(low) || !finite(high) || low <= 0 || high <= low || high > 10 || iterations < 1 || iterations > 1000 || !finite(tolerance) || tolerance <= 0 || s.Years <= 0 {
		return 0, fmt.Errorf("statistics: invalid American IV bracket/budget")
	}
	// Each evaluation visits a triangular tree. Bound combined work before
	// pricing either bracket endpoint, rather than allowing25 billion nodes.
	if steps < 1 || steps > 5000 || int64(steps+1)*int64(steps+2)/2*int64(iterations+2) > 100000000 {
		return 0, fmt.Errorf("statistics: American IV combined tree work budget exceeded")
	}
	s.Volatility = low
	a, err := AmericanOptionPrice(s, steps)
	if err != nil {
		return 0, err
	}
	s.Volatility = high
	b, err := AmericanOptionPrice(s, steps)
	if err != nil {
		return 0, err
	}
	if premium < a || premium > b || b-a <= tolerance || math.Abs(premium-a) <= tolerance && a == math.Max(0, func() float64 {
		if s.Call {
			return s.Spot - s.Strike
		}
		return s.Strike - s.Spot
	}()) {
		return 0, fmt.Errorf("statistics: American IV unbracketed or exercise-plateau unidentifiable")
	}
	for i := 0; i < iterations; i++ {
		mid := (low + high) / 2
		s.Volatility = mid
		value, err := AmericanOptionPrice(s, steps)
		if err != nil {
			return 0, err
		}
		if math.Abs(value-premium) <= tolerance {
			return mid, nil
		}
		if value < premium {
			low = mid
		} else {
			high = mid
		}
	}
	return 0, fmt.Errorf("statistics: American IV root budget exhausted")
}
