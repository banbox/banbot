package statistics

import (
	"fmt"
	"math"
)

// LocalLevelEffectiveEMA is the asymptotic scalar random-walk Kalman gain and
// its equivalent EMA period under alpha=2/(N+1). Finite-prior transient gains
// need not match this period. Increasing N requires decreasing Q/R.
func LocalLevelEffectiveEMA(q, r float64) (gain, period float64, err error) {
	if !finite(q) || !finite(r) || q <= 0 || r <= 0 {
		return 0, 0, fmt.Errorf("statistics: positive finite Q/R required")
	}
	// Stable equivalent of (sqrt((Q/R)^2+4Q/R)-Q/R)/2.
	ratio := r / q
	if !finite(ratio) {
		return 0, 0, fmt.Errorf("statistics: Q/R ratio overflow")
	}
	gain = 2 / (1 + math.Sqrt(1+4*ratio))
	period = 2/gain - 1
	if !finite(period) {
		return 0, 0, fmt.Errorf("statistics: effective EMA period overflow")
	}
	return gain, period, nil
}

// LocalLevelForEMA sets Q/R=4/(N^2-1) and posterior P=alpha*R, so
// every observation uses the exact EMA gain instead of a transient Kalman gain.
func LocalLevelForEMA(mean, period, r float64) (LocalLevel, error) {
	if !finite(mean) || !finite(period) || period <= 1 || !finite(r) || r <= 0 {
		return LocalLevel{}, fmt.Errorf("statistics: EMA period>1 and positive observation variance required")
	}
	alpha := 2 / (period + 1)
	q := r * alpha * alpha / (1 - alpha)
	if !finite(q) || q <= 0 {
		return LocalLevel{}, fmt.Errorf("statistics: EMA process variance overflow/underflow")
	}
	return LocalLevel{Mean: mean, Variance: alpha * r, ProcessVariance: q, ObservationVariance: r}, nil
}
