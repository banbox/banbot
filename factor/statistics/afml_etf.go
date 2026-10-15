package statistics

import (
	"errors"
	"math"
)

type ETFBar struct {
	At                                     int64
	Open, Close, PointValue, Volume, Carry []float64
	// Allocation becomes known at this close, applied at the NEXT bar's open.
	Allocation []float64
	Rebalance  bool
}

type ETFObservation struct {
	At                                           int64
	NAV, RebalanceCost, SpreadCost, BasketVolume float64
	Holdings                                     []float64
}

// ETFTrick implements AFML2.4.1's reinvested $1 multi-product series. Source
// h(t) uses o(t+1); this API delays the corresponding holding/cost record until
// that open is observed. It never sizes yesterday's decision with today's close.
// Carry is in instrument price-point units and multiplied by current PointValue.
// RebalanceCost liquidates/rebuys all legs as in the printed equation, even when
// net quantity change is smaller. Costs remain separate from NAV, so shorting
// the synthetic series cannot turn transaction costs into fictitious profits.
// A loss exhausting NAV is an error; no artificial positive-price clamp is used.
func ETFTrick(bars []ETFBar, costs []float64) ([]ETFObservation, error) {
	if len(bars) < 2 || len(costs) == 0 || len(costs) > 10000 || len(bars) > 1000000 {
		return nil, errors.New("statistics: invalid ETF series dimensions")
	}
	n := len(costs)
	for _, cost := range costs {
		if !afmlFinite(cost) || cost < 0 {
			return nil, errors.New("statistics: invalid ETF transaction cost")
		}
	}
	for t, bar := range bars {
		if (t > 0 && bar.At <= bars[t-1].At) || len(bar.Open) != n || len(bar.Close) != n || len(bar.PointValue) != n || len(bar.Volume) != n || len(bar.Carry) != n || (bar.Rebalance && len(bar.Allocation) != n) {
			return nil, errors.New("statistics: inconsistent ETF source grid/legs")
		}
		gross := 0.
		for i := 0; i < n; i++ {
			if !afmlFinite(bar.Open[i]) || bar.Open[i] <= 0 || !afmlFinite(bar.Close[i]) || bar.Close[i] <= 0 || !afmlFinite(bar.PointValue[i]) || bar.PointValue[i] <= 0 || !afmlFinite(bar.Volume[i]) || bar.Volume[i] < 0 || !afmlFinite(bar.Carry[i]) {
				return nil, errors.New("statistics: invalid ETF source units")
			}
			if bar.Rebalance {
				if !afmlFinite(bar.Allocation[i]) {
					return nil, errors.New("statistics: nonfinite ETF allocation")
				}
				gross += math.Abs(bar.Allocation[i])
			}
		}
		if bar.Rebalance && (gross <= 0 || !afmlFinite(gross)) {
			return nil, errors.New("statistics: empty ETF rebalance allocation")
		}
	}
	if !bars[0].Rebalance {
		return nil, errors.New("statistics: ETF requires initial close allocation")
	}
	out := make([]ETFObservation, len(bars))
	out[0] = ETFObservation{At: bars[0].At, NAV: 1, BasketVolume: math.NaN()}
	holdings, nav := make([]float64, n), 1.
	for t := 1; t < len(bars); t++ {
		previous, current := bars[t-1], bars[t]
		obs := ETFObservation{At: current.At, BasketVolume: math.Inf(1)}
		if previous.Rebalance {
			gross := 0.
			for _, w := range previous.Allocation {
				gross += math.Abs(w)
			}
			for i, w := range previous.Allocation {
				h := (w / gross) * nav / current.Open[i] / previous.PointValue[i]
				obs.RebalanceCost += (math.Abs(holdings[i])*previous.Close[i] + math.Abs(h)*current.Open[i]) * previous.PointValue[i] * costs[i]
				holdings[i] = h
			}
		}
		for i, h := range holdings {
			delta := current.Close[i] - previous.Close[i]
			if previous.Rebalance {
				delta = current.Close[i] - current.Open[i]
			}
			nav += h * current.PointValue[i] * (delta + current.Carry[i])
			obs.SpreadCost += math.Abs(h) * current.Close[i] * current.PointValue[i] * costs[i]
			if h != 0 {
				obs.BasketVolume = math.Min(obs.BasketVolume, current.Volume[i]/math.Abs(h))
			}
		}
		if !afmlFinite(nav) || nav <= 0 || !afmlFinite(obs.RebalanceCost) || !afmlFinite(obs.SpreadCost) || !afmlFinite(obs.BasketVolume) {
			return nil, errors.New("statistics: exhausted/overflowed ETF investment or volume")
		}
		obs.NAV, obs.Holdings = nav, append([]float64(nil), holdings...)
		out[t] = obs
	}
	return out, nil
}
