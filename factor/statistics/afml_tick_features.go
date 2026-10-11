package statistics

import "errors"

// TickSignFeatures is a snapshot of the visible trade tape in supplied event
// order. It infers direction, not observed aggressor side. Equal-time trades
// retain input order. Availability carries the entire preceding sign history.
type TickSignFeatures struct {
	Signs, Cumulative, Fractional []PITScalar
	FractionalValid               []bool
}

// TickRuleFeatures implements AFML19.3.1: initial b0=+1, carry the previous
// sign for unchanged price, then fixed-width fractional difference of cumsum(b).
// Kernel selection is caller supplied/frozen; this does not fit future data.
// Unreceived trades are excluded, so the caller must establish tape completeness.
// Finite zero/negative prices are valid; no OHLC or volume proxy is manufactured.
func TickRuleFeatures(prices []PITScalar, weights []float64, cutoffMS int64, maxWork int64) (TickSignFeatures, error) {
	var out TickSignFeatures
	if len(prices) == 0 || len(prices) > 1000000 || cutoffMS < 0 || len(weights) == 0 || maxWork < 1 || int64(len(prices)) > maxWork/int64(len(weights)) {
		return out, errors.New("statistics: invalid tick feature bounds")
	}
	visible := make([]PITScalar, 0, len(prices))
	for i, p := range prices {
		if !validScalar(p) || (i > 0 && p.EventMS < prices[i-1].EventMS) {
			return out, errors.New("statistics: invalid trade-price chronology")
		}
		if visibleScalar(p, cutoffMS) {
			visible = append(visible, p)
		}
	}
	levels := make([]float64, len(visible))
	sign, cumulative := 1., 0.
	available, received := int64(0), int64(0)
	for i, p := range visible {
		if i > 0 {
			if p.Value > visible[i-1].Value {
				sign = 1
			} else if p.Value < visible[i-1].Value {
				sign = -1
			}
		}
		available = max(available, p.AvailableMS)
		received = max(received, p.ReceivedMS)
		p.AvailableMS, p.ReceivedMS, p.Value = available, received, sign
		out.Signs = append(out.Signs, p)
		cumulative += sign
		p.Value = cumulative
		out.Cumulative = append(out.Cumulative, p)
		levels[i] = cumulative
	}
	values, err := FractionalDifference(levels, weights)
	if err != nil {
		return TickSignFeatures{}, err
	}
	for i, p := range out.Cumulative {
		valid := i >= len(weights)-1
		// Keep undefined warmup JSON-safe: flag false, value zero (not a sample).
		p.Value = 0
		if valid {
			p.Value = values[i]
		}
		out.Fractional = append(out.Fractional, p)
		out.FractionalValid = append(out.FractionalValid, valid)
	}
	return out, nil
}
