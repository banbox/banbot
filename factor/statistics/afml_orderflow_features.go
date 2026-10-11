package statistics

import (
	"errors"
	"math"
	"sort"
)

type OrderFlowEvent struct {
	EventMS, AvailableMS, ReceivedMS int64
	// Kind is trade, market, limit or cancel. Trade size is in integer lots;
	// Side is observed aggressor +/-1 or unknown0, never inferred from prices.
	Kind string
	Lots int64
	Side int
}

type OrderFlowFeatures struct {
	Trades, RoundTrades, UnknownSideTrades, Messages int
	SizeFrequency                                    map[int64]int
	RoundFraction                                    float64
	MarketFraction, LimitFraction, CancelFraction    float64
	MarketPerSecond, LimitPerSecond, CancelPerSecond float64
	HasTradeFraction, HasMessageFractions            bool
	// Source19.6.3 hour-by-second volume surface in caller's fixed UTC offset.
	VolumeByHourSecond   [24][60]float64
	EarlyMinuteImbalance map[int64]float64
	// Volume maps include complete minutes with observed trades; absent minutes
	// mean no observation, not certified zero volume or complete market coverage.
	MinuteVolume, EarlyMinuteVolume map[int64]float64
	EarlyImbalanceLag1              float64
	HasEarlyImbalanceLag1           bool
}

// OrderFlowWindow implements AFML19.6.1-.3 descriptive features on [from,to).
// Round sizes are explicit (the source lists5,10,20,25,50,100,..., not every
// multiple of a single lot). Message fractions normalize cancel/limit/market
// counts; missing denominators have explicit flags. Early-second imbalance is
// (buy-sell)/(buy+sell) of known aggressors; unknown sides are counted separately.
// Lag1 uses consecutive complete observed minutes, not compressed missing time.
// This is research feature extraction, not a trading or manipulation classifier.
func OrderFlowWindow(events []OrderFlowEvent, roundSizes []int64, fromMS, toMS, cutoffMS int64, utcOffsetMinutes, earlySeconds int) (OrderFlowFeatures, error) {
	r := OrderFlowFeatures{SizeFrequency: map[int64]int{}, EarlyMinuteImbalance: map[int64]float64{}, MinuteVolume: map[int64]float64{}, EarlyMinuteVolume: map[int64]float64{}}
	if len(events) > 2000000 || len(roundSizes) > 10000 || fromMS < 0 || toMS <= fromMS || cutoffMS < toMS || utcOffsetMinutes < -840 || utcOffsetMinutes > 840 || earlySeconds < 1 || earlySeconds > 60 {
		return r, errors.New("statistics: invalid order-flow research window")
	}
	round := map[int64]bool{}
	for _, s := range roundSizes {
		if s <= 0 || round[s] {
			return r, errors.New("statistics: invalid round-size vocabulary")
		}
		round[s] = true
	}
	market, limit, cancel := 0, 0, 0
	type signed struct{ net, gross float64 }
	minutes := map[int64]signed{}
	for _, e := range events {
		if e.EventMS < 0 || e.AvailableMS < e.EventMS || e.ReceivedMS < e.AvailableMS || e.Lots <= 0 || e.Side < -1 || e.Side > 1 {
			return r, errors.New("statistics: invalid order-flow event")
		}
		switch e.Kind {
		case "trade", "market", "limit", "cancel":
		default:
			return r, errors.New("statistics: unknown order-flow event kind")
		}
		if e.EventMS < fromMS || e.EventMS >= toMS || e.AvailableMS > cutoffMS || e.ReceivedMS > cutoffMS {
			continue
		}
		switch e.Kind {
		case "market":
			market++
		case "limit":
			limit++
		case "cancel":
			cancel++
		case "trade":
			r.Trades++
			r.SizeFrequency[e.Lots]++
			if round[e.Lots] {
				r.RoundTrades++
			}
			if e.Side == 0 {
				r.UnknownSideTrades++
			}
			seconds := e.EventMS / 1000
			second := int(seconds % 60)
			// Offset may have a half/quarter hour; calculate hour from shifted
			// seconds within a day without risking epoch addition overflow.
			daysec := ((seconds%86400+int64(utcOffsetMinutes)*60)%86400 + 86400) % 86400
			hour := int(daysec / 3600)
			r.VolumeByHourSecond[hour][second] += float64(e.Lots)
			minute := e.EventMS / 60000
			start := e.EventMS - e.EventMS%60000
			if start >= fromMS && toMS-start >= 60000 {
				r.MinuteVolume[minute] += float64(e.Lots)
				if _, ok := r.EarlyMinuteVolume[minute]; !ok {
					r.EarlyMinuteVolume[minute] = 0
				}
				if second < earlySeconds {
					r.EarlyMinuteVolume[minute] += float64(e.Lots)
				}
			}
			if second < earlySeconds && e.Side != 0 && start >= fromMS && toMS-start >= 60000 {
				p := minutes[minute]
				p.net += float64(e.Side) * float64(e.Lots)
				p.gross += float64(e.Lots)
				minutes[minute] = p
			}
		}
	}
	r.Messages = market + limit + cancel
	duration := float64(toMS-fromMS) / 1000
	r.MarketPerSecond = float64(market) / duration
	r.LimitPerSecond = float64(limit) / duration
	r.CancelPerSecond = float64(cancel) / duration
	if r.Messages > 0 {
		r.HasMessageFractions = true
		r.MarketFraction = float64(market) / float64(r.Messages)
		r.LimitFraction = float64(limit) / float64(r.Messages)
		r.CancelFraction = float64(cancel) / float64(r.Messages)
	}
	if r.Trades > 0 {
		r.HasTradeFraction = true
		r.RoundFraction = float64(r.RoundTrades) / float64(r.Trades)
	}
	for m, p := range minutes {
		r.EarlyMinuteImbalance[m] = p.net / p.gross
	}
	x, y := []float64{}, []float64{}
	keys := make([]int64, 0, len(minutes))
	for m := range minutes {
		keys = append(keys, m)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, m := range keys {
		if a, ok := r.EarlyMinuteImbalance[m-1]; ok {
			x = append(x, a)
			y = append(y, r.EarlyMinuteImbalance[m])
		}
	}
	if len(x) >= 2 {
		mx, my := 0., 0.
		for i := range x {
			mx += x[i] / float64(len(x))
			my += y[i] / float64(len(x))
		}
		xx, yy, xy := 0., 0., 0.
		for i := range x {
			a, b := x[i]-mx, y[i]-my
			xx += a * a
			yy += b * b
			xy += a * b
		}
		if xx > 0 && yy > 0 {
			r.EarlyImbalanceLag1 = math.Max(-1, math.Min(1, xy/math.Sqrt(xx*yy)))
			r.HasEarlyImbalanceLag1 = true
		}
	}
	return r, nil
}

type KSDiagnostic struct {
	Statistic                 float64
	LeftSamples, RightSamples int
}

// TwoSampleKS computes the exact empirical supremum distance, grouping ties
// before comparing CDFs. AFML19 exercises suggest KS diagnostics, but tied lot
// sizes and dependent flows invalidate the usual continuous IID null p-values;
// no unsupported asymptotic p-value or causal significance is returned.
func TwoSampleKS(left, right []PITScalar, cutoffMS int64) (KSDiagnostic, error) {
	if len(left) == 0 || len(right) == 0 || len(left)+len(right) > 2000000 || cutoffMS < 0 {
		return KSDiagnostic{}, errors.New("statistics: invalid KS samples")
	}
	get := func(s []PITScalar) ([]float64, error) {
		v := []float64{}
		for _, p := range s {
			if !validScalar(p) {
				return nil, errors.New("statistics: invalid KS observation")
			}
			if visibleScalar(p, cutoffMS) {
				v = append(v, p.Value)
			}
		}
		if len(v) == 0 {
			return nil, errors.New("statistics: no visible KS observations")
		}
		sort.Float64s(v)
		return v, nil
	}
	a, err := get(left)
	if err != nil {
		return KSDiagnostic{}, err
	}
	b, err := get(right)
	if err != nil {
		return KSDiagnostic{}, err
	}
	r := KSDiagnostic{LeftSamples: len(a), RightSamples: len(b)}
	i, j := 0, 0
	for i < len(a) || j < len(b) {
		v := 0.
		if j == len(b) || (i < len(a) && a[i] < b[j]) {
			v = a[i]
		} else {
			v = b[j]
		}
		for i < len(a) && a[i] == v {
			i++
		}
		for j < len(b) && b[j] == v {
			j++
		}
		r.Statistic = math.Max(r.Statistic, math.Abs(float64(i)/float64(len(a))-float64(j)/float64(len(b))))
	}
	return r, nil
}
