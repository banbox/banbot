package statistics

import "fmt"

// WinnerMinusLoserReturn uses portfolio memberships determined at the previous
// close: positive lookback returns are winners, negative returns losers, exact
// zero excluded. Each side is equal-dollar; return is winner average minus loser
// average (gross-two factor convention). Missing either side is an invalid factor.
func WinnerMinusLoserReturn(previous, current, lookback []float64) (float64, error) {
	if len(previous) == 0 || len(previous) != len(current) || len(current) != len(lookback) {
		return 0, fmt.Errorf("statistics: invalid UMD dimensions")
	}
	winners, losers := 0, 0
	up, down := 0.0, 0.0
	for i, p := range previous {
		if !finite(p) || p <= 0 || !finite(current[i]) || current[i] <= 0 || !finite(lookback[i]) || lookback[i] <= 0 {
			return 0, fmt.Errorf("statistics: positive finite UMD prices required")
		}
		ret := current[i]/p - 1
		if !finite(ret) {
			return 0, fmt.Errorf("statistics: UMD return overflow")
		}
		if p > lookback[i] {
			winners++
			up += ret
		} else if p < lookback[i] {
			losers++
			down += ret
		}
	}
	if winners == 0 || losers == 0 {
		return 0, fmt.Errorf("statistics: UMD requires both historical winners and losers")
	}
	value := up/float64(winners) - down/float64(losers)
	if !finite(value) {
		return 0, fmt.Errorf("statistics: UMD factor overflow")
	}
	return value, nil
}
