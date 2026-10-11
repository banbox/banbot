package statistics

import (
	"errors"
	"math"
	"sort"
)

type CalendarBarrierRequest struct {
	Start                              int
	HorizonMS                          int64 // zero disables the vertical barrier
	Target, ProfitFactor, StopFactor   float64
	TargetAvailableMS, SideAvailableMS int64
	TargetReceivedMS, SideReceivedMS   int64
	Side                               int
	Meta                               bool
}
type CalendarBarrierOutcome struct {
	Request        int
	StartMS, EndMS int64
	Vertical       int  // -1 when disabled or outside the supplied observation range
	Included       bool // source minRet filtering; a missing target is excluded
	BarrierLabel
}

// CalendarBarrierLabels implements AFML3.4's bulk minRet orchestration and
// next-observation-at-or-after calendar expiry. Target is a published volatility
// estimate, e.g. DailyVolatility; fitting it on future rows is not permitted by
// this API's explicit availability contract. Future outcomes are evaluation
// metadata; inference may use them only after EndMS has matured/published.
// Disabled or unobserved vertical barriers do not manufacture an endpoint.
func CalendarBarrierLabels(times []int64, close []float64, requests []CalendarBarrierRequest, minRet float64, maxPathReads uint64) ([]CalendarBarrierOutcome, error) {
	if len(times) == 0 || len(times) > 1000000 || len(times) != len(close) || len(requests) > 1000000 || !afmlFinite(minRet) || minRet < 0 {
		return nil, errors.New("statistics: invalid bulk calendar barrier dimensions")
	}
	for i, at := range times {
		if at <= 0 || (i > 0 && at <= times[i-1]) {
			return nil, errors.New("statistics: calendar barrier times must increase")
		}
	}
	out := make([]CalendarBarrierOutcome, len(requests))
	work := uint64(0)
	for i, r := range requests {
		if r.Start < 0 || r.Start >= len(times) || r.HorizonMS < 0 || times[r.Start] > math.MaxInt64-r.HorizonMS {
			return nil, errors.New("statistics: invalid calendar barrier event")
		}
		out[i] = CalendarBarrierOutcome{Request: i, StartMS: times[r.Start], Vertical: -1, BarrierLabel: BarrierLabel{End: -1}}
		// Source getEvents drops missing/nonqualifying targets before reading
		// the path or side. Equality to minRet is excluded (strict greater).
		if math.IsInf(r.Target, 0) {
			return nil, errors.New("statistics: infinite calendar barrier target")
		}
		if math.IsNaN(r.Target) || r.Target <= minRet {
			continue
		}
		if !afmlFinite(r.Target) || r.TargetAvailableMS <= 0 || r.TargetAvailableMS > times[r.Start] || r.TargetReceivedMS < r.TargetAvailableMS || r.TargetReceivedMS > times[r.Start] || r.SideAvailableMS <= 0 || r.SideAvailableMS > times[r.Start] || r.SideReceivedMS < r.SideAvailableMS || r.SideReceivedMS > times[r.Start] || (r.Side != 1 && r.Side != -1) || !afmlFinite(r.ProfitFactor) || r.ProfitFactor < 0 || !afmlFinite(r.StopFactor) || r.StopFactor < 0 || !r.Meta && r.ProfitFactor != r.StopFactor {
			return nil, errors.New("statistics: invalid or future calendar barrier target/side")
		}
		last := len(times) - 1
		if r.HorizonMS > 0 {
			expiry := times[r.Start] + r.HorizonMS
			vertical := sort.Search(len(times), func(j int) bool { return times[j] >= expiry })
			if vertical < len(times) {
				out[i].Vertical = vertical
				last = vertical
			}
		}
		work += uint64(last - r.Start + 1)
		if work > maxPathReads {
			return nil, errors.New("statistics: calendar barrier path budget exceeded")
		}
		out[i].Included = true
	}
	for i, r := range requests {
		if !out[i].Included {
			continue
		}
		label, err := TripleBarrierLabel(close, r.Start, out[i].Vertical, r.Target, r.ProfitFactor, r.StopFactor, r.Side, r.Meta)
		if err != nil {
			return nil, err
		}
		out[i].BarrierLabel = label
		if label.Resolved {
			out[i].EndMS = times[label.End]
		}
	}
	return out, nil
}

// ExponentialUniquenessDecay is chapter4's requested exponential alternative.
// The source exercise gives no parameterization: halfLifeMass explicitly means
// cumulative event uniqueness units, newest weight1. It is fitted training
// weighting metadata and must not incorporate future event endings at inference.
func ExponentialUniquenessDecay(uniqueness []float64, halfLifeMass float64) ([]float64, error) {
	if len(uniqueness) == 0 || len(uniqueness) > 1000000 || !afmlFinite(halfLifeMass) || halfLifeMass <= 0 {
		return nil, errors.New("statistics: invalid exponential uniqueness decay")
	}
	total := 0.
	for _, v := range uniqueness {
		if !afmlFinite(v) || v < 0 || v > 1 {
			return nil, errors.New("statistics: invalid event uniqueness")
		}
		total += v
	}
	if total <= 0 || !afmlFinite(total) {
		return nil, errors.New("statistics: uniqueness mass must be positive")
	}
	out := make([]float64, len(uniqueness))
	cumulative := 0.
	for i, v := range uniqueness {
		cumulative += v
		out[i] = math.Exp2(-(total - cumulative) / halfLifeMass)
	}
	return out, nil
}
