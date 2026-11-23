package execution

import (
	"math"
	"sort"
)

// Backtest accounting rules shared by every historical engine. They are pure
// float functions over plain values: the caller owns order and wallet state.
// Live accounts never use them; live funding is venue-reported (ApplyFunding).

// FundingPoint is one settled perpetual funding rate.
type FundingPoint struct {
	AtMS int64
	Rate float64
}

// MergeFundingPoints returns the points of a and b ordered by settlement time.
// A point of b replaces a point of a with the same time. When b is empty the
// result is a itself, which must already be ordered.
func MergeFundingPoints(a, b []FundingPoint) []FundingPoint {
	if len(b) == 0 {
		return a
	}
	merged := make([]FundingPoint, 0, len(a)+len(b))
	merged = append(merged, a...)
	merged = append(merged, b...)
	sort.SliceStable(merged, func(i, j int) bool { return merged[i].AtMS < merged[j].AtMS })
	out := merged[:0]
	for _, point := range merged {
		if len(out) > 0 && out[len(out)-1].AtMS == point.AtMS {
			out[len(out)-1] = point
			continue
		}
		out = append(out, point)
	}
	return out
}

// RealFundingCost sums signedNotional x rate over the settlements in
// (enterMS, exitMS] of ordered points. signedNotional is negative for a short,
// so a positive result is paid by the position and a negative one is received.
// crossed is the number of settlements inside the holding period.
func RealFundingCost(points []FundingPoint, signedNotional float64, enterMS, exitMS int64) (cost float64, crossed int) {
	start := sort.Search(len(points), func(i int) bool { return points[i].AtMS > enterMS })
	for i := start; i < len(points) && points[i].AtMS <= exitMS; i++ {
		cost += signedNotional * points[i].Rate
		crossed++
	}
	return cost, crossed
}

// ApproxFundingCost is notional x aprPct% x the holding period in years, the
// constant-rate stand-in used when a symbol has no settled funding history.
func ApproxFundingCost(notional, aprPct float64, holdMS int64) float64 {
	if aprPct == 0 || math.IsNaN(aprPct) || math.IsInf(aprPct, 0) || notional <= 0 || holdMS <= 0 {
		return 0
	}
	return notional * aprPct / 100 * float64(holdMS) / (365 * 86400000)
}

// DefaultGapMinHours is the minimum hole length treated as a data gap when
// bt_gap_min_hours is unset.
const DefaultGapMinHours = 48.0

// GapConfirmDelayMS is how long after a symbol's last bar its silence counts
// as a data gap: the longer of gapBars bars and minHours, plus one millisecond.
func GapConfirmDelayMS(tfMS int64, gapBars int, minHours float64) int64 {
	if minHours <= 0 {
		minHours = DefaultGapMinHours
	}
	delay := int64(gapBars) * tfMS
	if minMS := int64(minHours * 3600 * 1000); minMS > delay {
		delay = minMS
	}
	return delay + 1
}

// GapExitPrice prices a forced exit after data resumed: the worse for the
// position of the last close before the hole and the first open after it, so a
// position never dodges the move inside the hole. nextOpen <= 0 means unknown.
func GapExitPrice(short bool, lastClose, nextOpen float64) float64 {
	if nextOpen <= 0 {
		return lastClose
	}
	if short {
		return max(lastClose, nextOpen)
	}
	return min(lastClose, nextOpen)
}

// DelistExitPrice prices a forced exit of a symbol whose data never resumes:
// the last close moved against the position by penaltyBps.
func DelistExitPrice(short bool, lastClose, penaltyBps float64) float64 {
	if penaltyBps <= 0 {
		return lastClose
	}
	if short {
		return lastClose * (1 + penaltyBps/10000)
	}
	return lastClose * (1 - penaltyBps/10000)
}

// GapFillTime is the booked time of a data-gap exit: the last bar plus the
// confirmation delay, capped by the recovery time (when known) and by now, and
// never before the last bar.
func GapFillTime(lastMS, nowMS, recoverMS, delayMS int64) int64 {
	at := lastMS + delayMS
	if recoverMS > 0 && recoverMS < at {
		at = recoverMS
	}
	if at > nowMS {
		at = nowMS
	}
	if at < lastMS {
		at = lastMS
	}
	return at
}

// LiquidationBreached reports whether an account's open loss consumed more
// than 99% of its settled balance. A zero balance with any loss is breached; a
// negative balance never is (it is handled as depleted equity by the caller).
func LiquidationBreached(unrealizedPnL, settledBalance float64) bool {
	if unrealizedPnL >= 0 {
		return false
	}
	return math.Abs(unrealizedPnL)/settledBalance > 0.99
}

// MarginWithLossTopUp returns the margin to hold for a losing position. Once
// the loss reaches addRate of the buffer between the position margin and the
// maintenance margin, the loss is added to the margin so the position is not
// force-closed.
func MarginWithLossTopUp(margin, maintMargin, profit, addRate float64) float64 {
	if math.Abs(profit) >= (margin-maintMargin)*addRate {
		return margin - profit
	}
	return margin
}
