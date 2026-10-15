package statistics

import (
	"fmt"
	"math"
)

// SurfaceOption is an already published implied volatility, not an assumption
// that its source used a European model for an American contract. Identity is
// exact expiry/strike/right; the caller selects the source's current revision.
type SurfaceOption struct {
	Strike, TenorDays, IV, OpenInterest float64
	ExpiryMS                            int64
	Call                                bool
}

func OpenInterestWeightedIVSpread(quotes []SurfaceOption) (float64, error) {
	type key struct {
		expiry int64
		strike float64
	}
	type pair struct{ call, put *SurfaceOption }
	pairs := map[key]pair{}
	if len(quotes) > 10000 {
		return 0, fmt.Errorf("statistics: option surface too large")
	}
	for i := range quotes {
		q := &quotes[i]
		if !finite(q.Strike) || q.Strike <= 0 || q.ExpiryMS <= 0 || !finite(q.IV) || q.IV <= 0 || !finite(q.OpenInterest) || q.OpenInterest < 0 {
			return 0, fmt.Errorf("statistics: invalid option IV/OI identity")
		}
		k := key{q.ExpiryMS, q.Strike}
		p := pairs[k]
		if q.Call {
			if p.call != nil {
				return 0, fmt.Errorf("statistics: duplicate option call revision")
			}
			p.call = q
		} else {
			if p.put != nil {
				return 0, fmt.Errorf("statistics: duplicate option put revision")
			}
			p.put = q
		}
		pairs[k] = p
	}
	// Stable normalization avoids overflowing notional-sized open interests.
	maxWeight := 0.
	for _, p := range pairs {
		if p.call != nil && p.put != nil {
			maxWeight = math.Max(maxWeight, p.call.OpenInterest/2+p.put.OpenInterest/2)
		}
	}
	if maxWeight <= 0 {
		return 0, fmt.Errorf("statistics: no positive-OI complete option pair")
	}
	sum, weight := 0., 0.
	// Iterate input identities in source order for reproducible floating sums.
	for _, q := range quotes {
		if !q.Call {
			continue
		}
		p := pairs[key{q.ExpiryMS, q.Strike}]
		if p.put == nil {
			continue
		}
		w := (p.call.OpenInterest/2 + p.put.OpenInterest/2) / maxWeight
		sum += w * (p.call.IV - p.put.IV)
		weight += w
	}
	value := sum / weight
	if !finite(value) {
		return 0, fmt.Errorf("statistics: IV spread overflow")
	}
	return value, nil
}

// VolatilitySmirk follows MT ch2:10..60-day tenor, OTM put moneyness(.8,.95),
// ATM call [.95,1.05]; choose call closest1 and put closest.95 separately.
func VolatilitySmirk(spot float64, quotes []SurfaceOption) (float64, error) {
	if !finite(spot) || spot <= 0 || len(quotes) > 10000 {
		return 0, fmt.Errorf("statistics: invalid smirk surface")
	}
	var call, put *SurfaceOption
	callDistance, putDistance := math.Inf(1), math.Inf(1)
	for i := range quotes {
		q := &quotes[i]
		if !finite(q.Strike) || q.Strike <= 0 || !finite(q.TenorDays) || q.TenorDays < 0 || !finite(q.IV) || q.IV <= 0 || q.ExpiryMS <= 0 {
			return 0, fmt.Errorf("statistics: invalid smirk quote")
		}
		if q.TenorDays < 10 || q.TenorDays > 60 {
			continue
		}
		m := q.Strike / spot
		if q.Call && m >= .95 && m <= 1.05 {
			d := math.Abs(m - 1)
			if d < callDistance || d == callDistance && call != nil && q.ExpiryMS < call.ExpiryMS {
				call, callDistance = q, d
			}
		}
		if !q.Call && m > .8 && m < .95 {
			d := math.Abs(m - .95)
			if d < putDistance || d == putDistance && put != nil && q.ExpiryMS < put.ExpiryMS {
				put, putDistance = q, d
			}
		}
	}
	if call == nil || put == nil {
		return 0, fmt.Errorf("statistics: source ATM-call/OTM-put surface incomplete")
	}
	return put.IV - call.IV, nil
}
