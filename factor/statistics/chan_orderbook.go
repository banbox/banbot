package statistics

import (
	"errors"
	"math"
	"sort"
)

// ChanBookMessage is a normalized displayed-order message. Prices and sizes
// use caller-fixed integer ticks/lots; negative prices are permitted. Full fill
// must name the entire remaining quantity; cancellation can remove part or all.
// Owner is the feed's participant identity (a stable sentinel if unavailable).
type ChanBookMessage struct {
	Venue, Instrument, OrderID, Owner string
	Action, Side                      string // add/cancel/fill/partial_fill; buy/sell
	PriceTicks, Quantity              int64
	Sequence                          uint64
	EventMS, AvailableMS, ReceivedMS  int64
}

type ChanBookLimits struct {
	MaxEvents, MaxOrders, Depth    int
	MaxWork                        int64   // bounds active-order scans, including sorting
	PriceTickSize, QuantityLotSize float64 // declared units, never used to round input
}

type ChanBookLevel struct {
	PriceTicks, Quantity int64
	Orders               int
}

// Empty sides are explicit empty slices, never stale or fabricated quotes.
type ChanBookSnapshot struct {
	Sequence                         uint64
	EventMS, AvailableMS, ReceivedMS int64
	Bids, Asks                       []ChanBookLevel // best first, depth limited
	ActiveOrders                     int
}

type ChanBookReport struct {
	Venue, Instrument              string
	PriceTickSize, QuantityLotSize float64
	Snapshots                      []ChanBookSnapshot
	Processed                      int
	BlockedByReceipt               bool
}

// ChanPriceLevelMessage follows the five Coinsetter fields in Ex6.2, with
// explicit caller-scaled ticks/lots and added tape/PIT clocks. No order ID or
// participant identity is fabricated when the source supplies only price levels.
type ChanPriceLevelMessage struct {
	Venue, Instrument, Action, Side  string
	PriceTicks, Quantity             int64
	Sequence                         uint64
	EventMS, AvailableMS, ReceivedMS int64
}

// ReconstructChanPriceLevels is the literal source add/subtract price-level
// algorithm. All three removal actions subtract EventAmount from aggregate
// level quantity. Orders/ActiveOrders are zero because order counts are unknown.
func ReconstructChanPriceLevels(messages []ChanPriceLevelMessage, cutoffMS int64, limits ChanBookLimits) (ChanBookReport, error) {
	out := ChanBookReport{PriceTickSize: limits.PriceTickSize, QuantityLotSize: limits.QuantityLotSize}
	if cutoffMS < 0 || !chanBookScales(limits) || limits.MaxEvents < 1 || limits.MaxEvents > 1000000 || len(messages) > limits.MaxEvents || limits.MaxOrders < 1 || limits.MaxOrders > 1000000 || limits.Depth < 1 || limits.Depth > limits.MaxOrders || limits.MaxWork < 1 {
		return out, errors.New("statistics: invalid price-level bounds")
	}
	bids, asks := map[int64]ChanBookLevel{}, map[int64]ChanBookLevel{}
	var previous ChanPriceLevelMessage
	var available, received, work int64
	for i, m := range messages {
		if m.EventMS > cutoffMS || m.AvailableMS > cutoffMS || m.ReceivedMS > cutoffMS {
			out.BlockedByReceipt = true
			break
		}
		if m.EventMS < 0 || m.AvailableMS < m.EventMS || m.ReceivedMS < m.AvailableMS || m.Sequence == 0 || m.Venue == "" || m.Instrument == "" || (m.Side != "buy" && m.Side != "sell") || m.Quantity <= 0 {
			return ChanBookReport{}, errors.New("statistics: malformed price-level message")
		}
		if i == 0 {
			out.Venue, out.Instrument = m.Venue, m.Instrument
		} else if previous.Sequence == math.MaxUint64 || m.Sequence != previous.Sequence+1 || m.EventMS < previous.EventMS || m.Venue != out.Venue || m.Instrument != out.Instrument {
			return ChanBookReport{}, errors.New("statistics: price-level tape sequence/scope mismatch")
		}
		levels := bids
		if m.Side == "sell" {
			levels = asks
		}
		level, exists := levels[m.PriceTicks]
		switch m.Action {
		case "add":
			if m.Quantity > math.MaxInt64-level.Quantity {
				return ChanBookReport{}, errors.New("statistics: level quantity overflow")
			}
			level.PriceTicks, level.Quantity = m.PriceTicks, level.Quantity+m.Quantity
		case "cancel", "fill", "partial_fill":
			if !exists || m.Quantity > level.Quantity {
				return ChanBookReport{}, errors.New("statistics: unknown level or excessive decrement")
			}
			level.Quantity -= m.Quantity
		default:
			return ChanBookReport{}, errors.New("statistics: unknown price-level action")
		}
		if level.Quantity == 0 {
			delete(levels, m.PriceTicks)
		} else {
			levels[m.PriceTicks] = level
		}
		n, log := int64(len(bids)+len(asks)), int64(0)
		if n > int64(limits.MaxOrders) {
			return ChanBookReport{}, errors.New("statistics: price-level count bound")
		}
		for k := n; k > 1; k = (k + 1) / 2 {
			log++
		}
		cost := n * (1 + log)
		if cost > limits.MaxWork-work {
			return ChanBookReport{}, errors.New("statistics: price-level work bound")
		}
		work += cost
		available, received = max(available, m.AvailableMS), max(received, m.ReceivedMS)
		out.Snapshots = append(out.Snapshots, ChanBookSnapshot{m.Sequence, m.EventMS, available, received, chanBookLevels(bids, true, limits.Depth), chanBookLevels(asks, false, limits.Depth), 0})
		out.Processed++
		previous = m
	}
	return out, nil
}

// ReconstructChanOrderBook implements Machine Trading Ex6.2's sequential
// add/subtract and best-price replacement with strict order-ID validation.
// Input starts from an empty displayed book and is one venue/instrument tape.
// Stop at the first invisible event: processing later visible messages would
// create a fictitious book across a receipt gap. Callers establish completeness.
// No hidden liquidity, exchange queue modifiers, or hypothetical fills inferred.
func ReconstructChanOrderBook(messages []ChanBookMessage, cutoffMS int64, limits ChanBookLimits) (ChanBookReport, error) {
	out := ChanBookReport{PriceTickSize: limits.PriceTickSize, QuantityLotSize: limits.QuantityLotSize}
	if cutoffMS < 0 || !chanBookScales(limits) || limits.MaxEvents < 1 || limits.MaxEvents > 1000000 || limits.MaxOrders < 1 || limits.MaxOrders > 1000000 || limits.Depth < 1 || limits.Depth > limits.MaxOrders || limits.MaxWork < 1 || len(messages) > limits.MaxEvents {
		return out, errors.New("statistics: invalid order-book bounds")
	}
	active := make(map[string]ChanBookMessage)
	seen := make(map[string]bool)
	var previous ChanBookMessage
	var available, received, work int64
	for i, m := range messages {
		if m.EventMS > cutoffMS || m.AvailableMS > cutoffMS || m.ReceivedMS > cutoffMS {
			out.BlockedByReceipt = true
			break
		}
		if m.EventMS < 0 || m.AvailableMS < m.EventMS || m.ReceivedMS < m.AvailableMS || m.Sequence == 0 || m.Venue == "" || m.Instrument == "" || m.OrderID == "" || m.Owner == "" || (m.Side != "buy" && m.Side != "sell") || m.Quantity <= 0 {
			return ChanBookReport{}, errors.New("statistics: malformed order-book message")
		}
		if i == 0 {
			out.Venue, out.Instrument = m.Venue, m.Instrument
		} else if previous.Sequence == math.MaxUint64 || m.Sequence != previous.Sequence+1 || m.EventMS < previous.EventMS || m.Venue != out.Venue || m.Instrument != out.Instrument {
			return ChanBookReport{}, errors.New("statistics: order-book tape sequence/scope mismatch")
		}
		old, exists := active[m.OrderID]
		switch m.Action {
		case "add":
			if seen[m.OrderID] || len(active) >= limits.MaxOrders {
				return ChanBookReport{}, errors.New("statistics: duplicate order ID or active-order bound")
			}
			active[m.OrderID], seen[m.OrderID] = m, true
		case "cancel", "fill", "partial_fill":
			if !exists || old.Owner != m.Owner || old.Side != m.Side || old.PriceTicks != m.PriceTicks || m.Quantity > old.Quantity || (m.Action == "fill" && m.Quantity != old.Quantity) || (m.Action == "partial_fill" && m.Quantity == old.Quantity) {
				return ChanBookReport{}, errors.New("statistics: unknown/mismatched order or invalid decrement")
			}
			old.Quantity -= m.Quantity
			if old.Quantity == 0 {
				delete(active, m.OrderID)
			} else {
				active[m.OrderID] = old
			}
		default:
			return ChanBookReport{}, errors.New("statistics: unknown order-book action")
		}
		// n*(1+ceil(log2(n))) conservatively budgets aggregation and sorting.
		n, log := int64(len(active)), int64(0)
		for k := n; k > 1; k = (k + 1) / 2 {
			log++
		}
		cost := n * (1 + log)
		if cost > limits.MaxWork-work {
			return ChanBookReport{}, errors.New("statistics: order-book work bound")
		}
		work += cost
		bids, asks := map[int64]ChanBookLevel{}, map[int64]ChanBookLevel{}
		for _, order := range active {
			levels := bids
			if order.Side == "sell" {
				levels = asks
			}
			level := levels[order.PriceTicks]
			if order.Quantity > math.MaxInt64-level.Quantity {
				return ChanBookReport{}, errors.New("statistics: order-book level quantity overflow")
			}
			level.PriceTicks, level.Quantity, level.Orders = order.PriceTicks, level.Quantity+order.Quantity, level.Orders+1
			levels[order.PriceTicks] = level
		}
		available, received = max(available, m.AvailableMS), max(received, m.ReceivedMS)
		out.Snapshots = append(out.Snapshots, ChanBookSnapshot{m.Sequence, m.EventMS, available, received, chanBookLevels(bids, true, limits.Depth), chanBookLevels(asks, false, limits.Depth), len(active)})
		out.Processed++
		previous = m
	}
	return out, nil
}

func chanBookLevels(levels map[int64]ChanBookLevel, bid bool, depth int) []ChanBookLevel {
	result := make([]ChanBookLevel, 0, len(levels))
	for _, level := range levels {
		result = append(result, level)
	}
	sort.Slice(result, func(i, j int) bool {
		if bid {
			return result[i].PriceTicks > result[j].PriceTicks
		}
		return result[i].PriceTicks < result[j].PriceTicks
	})
	if len(result) > depth {
		result = result[:depth]
	}
	return result
}

func chanBookScales(limits ChanBookLimits) bool {
	return limits.PriceTickSize > 0 && limits.QuantityLotSize > 0 && !math.IsNaN(limits.PriceTickSize) && !math.IsNaN(limits.QuantityLotSize) && !math.IsInf(limits.PriceTickSize, 0) && !math.IsInf(limits.QuantityLotSize, 0)
}
