package statistics

import (
	"errors"
	"math"
	"sort"
)

const afmlDayMillis int64 = 24 * 60 * 60 * 1000

// DailyVolatility implements AFML snippet 3.1 on increasing millisecond UTC
// timestamps. strictBefore=true selects the last close STRICTLY before t-1day,
// as the printed searchsorted(..., side="left")-1 does. false includes an exact
// t-1day observation. Both policies are causal, but daily regular data yields
// two-day returns in source mode and one-day returns in inclusive mode.
// Output matches input length with NaN during alignment/variance warmup.
// EW standard deviation uses pandas defaults: adjust=true, unbiased variance.
func DailyVolatility(timestamps []int64, close []float64, span int, strictBefore bool) ([]float64, error) {
	if len(timestamps) == 0 || len(timestamps) != len(close) || span < 1 {
		return nil, errors.New("statistics: invalid daily volatility dimensions/span")
	}
	for i, ts := range timestamps {
		if ts < math.MinInt64+afmlDayMillis || (i > 0 && ts <= timestamps[i-1]) || !afmlFinite(close[i]) || close[i] <= 0 {
			return nil, errors.New("statistics: daily volatility needs ordered timestamps and positive finite closes")
		}
	}
	out := make([]float64, len(close))
	decay := 1 - 2/(float64(span)+1)
	mean, m2, mass, squaredMass := 0., 0., 0., 0.
	for i, ts := range timestamps {
		out[i] = math.NaN()
		cutoff := ts - afmlDayMillis
		at := sort.Search(i, func(j int) bool {
			if strictBefore {
				return timestamps[j] >= cutoff
			}
			return timestamps[j] > cutoff
		}) - 1
		if at < 0 {
			continue
		}
		ret := close[i]/close[at] - 1
		if !afmlFinite(ret) {
			return nil, errors.New("statistics: daily volatility return overflow")
		}
		oldMass := mass * decay
		mass = oldMass + 1
		squaredMass = squaredMass*decay*decay + 1
		delta := ret - mean
		mean += delta / mass
		m2 = decay*m2 + delta*delta*oldMass/mass
		denominator := mass - squaredMass/mass
		if !afmlFinite(m2) {
			return nil, errors.New("statistics: daily volatility variance overflow")
		}
		if denominator > 0 {
			out[i] = math.Sqrt(math.Max(0, m2/denominator))
		}
	}
	return out, nil
}

// RollGaps implements AFML snippet 2.2's first occurrence of each contract id:
// gap = new contract open - immediately preceding raw close. Forward gaps start
// at zero; matchEnd=true subtracts the terminal cumulative gap (backward roll).
// Subtract these gaps from raw closes to obtain roll-adjusted prices; keep raw
// prices for capital/sizing. Backward gaps are retrospective research data.
func RollGaps(contracts []string, open, close []float64, matchEnd bool) ([]float64, error) {
	if len(contracts) == 0 || len(open) != len(contracts) || len(close) != len(contracts) {
		return nil, errors.New("statistics: invalid roll-gap dimensions")
	}
	gaps := make([]float64, len(contracts))
	seen := make(map[string]bool)
	cumulative := 0.
	for i, contract := range contracts {
		if contract == "" || !afmlFinite(open[i]) || !afmlFinite(close[i]) {
			return nil, errors.New("statistics: roll gaps need nonempty contracts and finite prices")
		}
		if !seen[contract] {
			seen[contract] = true
			if i > 0 {
				cumulative += open[i] - close[i-1]
			}
		}
		if !afmlFinite(cumulative) {
			return nil, errors.New("statistics: roll-gap overflow")
		}
		gaps[i] = cumulative
	}
	if matchEnd {
		for i := range gaps {
			gaps[i] -= cumulative
			if !afmlFinite(gaps[i]) {
				return nil, errors.New("statistics: backward roll-gap overflow")
			}
		}
	}
	return gaps, nil
}
