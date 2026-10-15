package statistics

import (
	"errors"
	"math"
)

func validatePositionSeries(times []int64, positions []float64) error {
	if len(times) == 0 || len(times) != len(positions) {
		return errors.New("statistics: invalid target-position series")
	}
	for i, at := range times {
		if at < 0 || (i > 0 && at <= times[i-1]) || !afmlFinite(positions[i]) {
			return errors.New("statistics: position series requires increasing nonnegative times and finite values")
		}
	}
	return nil
}

// BetBoundaryTimes reproduces snippet14.1: flattening from a nonzero position,
// sign flips and terminal observation. Like pandas' shift-NaN !=0 comparison,
// an initially flat observation is included. Adds/reductions are not boundaries.
func BetBoundaryTimes(times []int64, positions []float64) ([]int64, error) {
	if err := validatePositionSeries(times, positions); err != nil {
		return nil, err
	}
	var out []int64
	for i, value := range positions {
		flat := value == 0 && (i == 0 || positions[i-1] != 0)
		flip := i > 0 && value != 0 && positions[i-1] != 0 && math.Signbit(value) != math.Signbit(positions[i-1])
		if flat || flip || i == len(positions)-1 {
			out = append(out, times[i])
		}
	}
	return out, nil
}

type HoldingPeriodReport struct {
	MeanMS, ClosedQuantity float64
	Valid                  bool
}

// PositionHoldingPeriod pairs reductions with the quantity-weighted mean entry
// time as in snippet14.2. A flip closes the full old side and seeds the new side;
// unclosed terminal inventory contributes no fictitious completed holding.
func PositionHoldingPeriod(times []int64, positions []float64) (HoldingPeriodReport, error) {
	r := HoldingPeriodReport{MeanMS: math.NaN()}
	if err := validatePositionSeries(times, positions); err != nil {
		return r, err
	}
	entry, weightedDuration := 0., 0.
	for i := 1; i < len(times); i++ {
		old, current := positions[i-1], positions[i]
		elapsed := float64(times[i] - times[0])
		same := old == 0 || current == 0 || math.Signbit(old) == math.Signbit(current)
		if same && math.Abs(current) >= math.Abs(old) {
			if current != 0 {
				// Convex arithmetic avoids overflowing signed quantity products.
				fraction := math.Abs(old) / math.Abs(current)
				entry = fraction*entry + (1-fraction)*elapsed
			}
			continue
		}
		closed := math.Abs(old) - math.Abs(current)
		if !same {
			closed = math.Abs(old)
		}
		r.ClosedQuantity += closed
		weightedDuration += closed * (elapsed - entry)
		if !same {
			entry = elapsed
		}
		if !afmlFinite(r.ClosedQuantity) || !afmlFinite(weightedDuration) {
			return r, errors.New("statistics: holding period overflow")
		}
	}
	if r.ClosedQuantity > 0 {
		r.MeanMS, r.Valid = weightedDuration/r.ClosedQuantity, true
	}
	return r, nil
}

// NormalizedHHI implements snippet14.3's finite-N normalization. Source returns
// NaN for at most two observations. Inputs must be one signed payout class or
// nonnegative period counts, so positive and negative payouts cannot cancel.
func NormalizedHHI(values []float64) (float64, error) {
	if len(values) <= 2 {
		return math.NaN(), nil
	}
	scale, sign := 0., 0
	for _, value := range values {
		if !afmlFinite(value) {
			return 0, errors.New("statistics: nonfinite concentration input")
		}
		if value != 0 {
			current := 1
			if value < 0 {
				current = -1
			}
			if sign != 0 && sign != current {
				return 0, errors.New("statistics: HHI requires a single signed payout class")
			}
			sign = current
		}
		scale = math.Max(scale, math.Abs(value))
	}
	if scale == 0 {
		return math.NaN(), nil
	}
	total, squares := 0., 0.
	for _, value := range values {
		normalized := value / scale
		total += normalized
		squares += normalized * normalized
	}
	n := float64(len(values))
	return (squares/(total*total) - 1/n) / (1 - 1/n), nil
}

type SourceDrawdownEpisode struct {
	PeakAt                       int64
	HighWater, Minimum, Drawdown float64
	TimeToNextDrawdownMS         int64
	TimeValid                    bool
}

// SourceDrawdownEpisodes implements snippet14.4's HWM-group minima and the
// interval to the next DRAW-DOWN group's first HWM timestamp. This source TuW
// proxy differs from actual peak-to-recovery duration: the final episode has no
// measured duration, and high-water groups with no decline are omitted.
func SourceDrawdownEpisodes(times []int64, values []float64, dollars bool) ([]SourceDrawdownEpisode, error) {
	if err := validatePositionSeries(times, values); err != nil {
		return nil, err
	}
	var out []SourceDrawdownEpisode
	peak, minimum, peakAt := values[0], values[0], times[0]
	appendEpisode := func() error {
		if minimum >= peak {
			return nil
		}
		dd := peak - minimum
		if !dollars {
			if peak <= 0 || minimum <= 0 {
				return errors.New("statistics: relative drawdown needs positive equity")
			}
			dd = 1 - minimum/peak
		}
		if !afmlFinite(dd) {
			return errors.New("statistics: drawdown overflow")
		}
		out = append(out, SourceDrawdownEpisode{PeakAt: peakAt, HighWater: peak, Minimum: minimum, Drawdown: dd})
		return nil
	}
	for i, value := range values[1:] {
		if value > peak {
			if err := appendEpisode(); err != nil {
				return nil, err
			}
			peak, minimum, peakAt = value, value, times[i+1]
		} else {
			minimum = math.Min(minimum, value)
		}
	}
	if err := appendEpisode(); err != nil {
		return nil, err
	}
	for i := 0; i+1 < len(out); i++ {
		out[i].TimeToNextDrawdownMS, out[i].TimeValid = out[i+1].PeakAt-out[i].PeakAt, true
	}
	return out, nil
}
