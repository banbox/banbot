package backtest

import (
	"math"
	"sort"
)

// Attribution accounting: a pure observer of the Book's price-change points. It only adds to private
// accumulators and never reads or writes cash, quantities, marks or costs, so enabling it cannot change any
// ledger number. Invariant (up to float rounding, see AttributionReport.Residual):
//
//	NAV - initial NAV = LongPnL + ShortPnL - Fees - Slippage - Funding - PITCost
//
// Funding (approximation and settlements) is an account-level item only: it is in AttributionReport.Funding and not in
// any symbol's Costs / Net (the net-notional charge belongs to the book, not to one instrument), so the symbols'
// Net sums to NAV change + Funding.
//
// Price-change points covered: Mark (revaluation of the held quantity between two marks), Execute /
// ExecuteAllocation (the held quantity is revalued to the fill quote before the trade; the trade then changes the
// quantity), ExitAtLastMark (quantity removed at the last mark, penalty booked), resolveRecovered (penalty refund
// and gap loss), ApplyTerminalSettlement (quantity cashed at the settlement value instead of the last mark),
// ApplyFunding and accrueFunding (costs). Those are all code paths that change a quantity, a mark or cash.

type symbolAttr struct{ pnl, fees, slippage, pit, turnover float64 }

type attribution struct {
	longPnL, shortPnL, longTurn, shortTurn float64
	expLong, expShort                      float64
	samples                                int
	bySID                                  map[int32]*symbolAttr
}

// EnableAttribution turns the long/short/per-symbol accumulators on (idempotent). Call before the first mark.
func (b *Book) EnableAttribution() {
	if b.attr == nil {
		b.attr = &attribution{bySID: map[int32]*symbolAttr{}}
	}
}

func (a *attribution) sym(sid int32) *symbolAttr {
	s := a.bySID[sid]
	if s == nil {
		s = &symbolAttr{}
		a.bySID[sid] = s
	}
	return s
}

// move books the price move of a held quantity: qty x (to - from).
func (a *attribution) move(sid int32, qty, from, to float64) {
	if a == nil || qty == 0 {
		return
	}
	v := qty * (to - from)
	if !finite(v) {
		return
	}
	if qty > 0 {
		a.longPnL += v
	} else {
		a.shortPnL += v
	}
	a.sym(sid).pnl += v
}

// trade books a quantity change old -> next at price (the unslipped quote): turnover is split by leg (the long
// leg moves from max(old,0) to max(next,0), the short leg from min(old,0) to min(next,0)); cost and slip are the
// fee and the slippage of this trade.
func (a *attribution) trade(sid int32, old, next, price, cost, slip float64) {
	if a == nil {
		return
	}
	a.longTurn += math.Abs(math.Max(next, 0)-math.Max(old, 0)) * price
	a.shortTurn += math.Abs(math.Min(next, 0)-math.Min(old, 0)) * price
	s := a.sym(sid)
	s.fees += cost
	s.slippage += slip
	s.turnover += math.Abs(next-old) * price
}

func (a *attribution) addPIT(sid int32, cost float64) {
	if a != nil {
		a.sym(sid).pit += cost
	}
}

// revalue books the move of the held quantity of sid from its previous mark to price (no-op without a mark).
func (b *Book) attrRevalue(sid int32, to float64) {
	if b.attr == nil {
		return
	}
	if old, ok := b.marks[sid]; ok {
		b.attr.move(sid, b.quantities[sid], old.Price, to)
	}
}

// SampleExposure records the long and short gross exposure (position value / NAV) at this instant; the report
// averages the samples. The runner calls it once per decision time.
func (b *Book) SampleExposure() {
	b.settleFunding()
	if b.attr == nil {
		return
	}
	nav := b.cash
	var long, short float64
	for _, sid := range sortedKeys(b.quantities) {
		v := b.quantities[sid] * b.marks[sid].Price
		nav += v
		if v > 0 {
			long += v
		} else {
			short -= v
		}
	}
	if nav <= 0 || !finite(nav) {
		return
	}
	b.attr.expLong += long / nav
	b.attr.expShort += short / nav
	b.attr.samples++
}

// SymbolAttribution is one instrument's contribution: PnL = price-move profit (before costs), Costs = fees +
// slippage + PIT exit costs (no funding), Net = PnL - Costs.
type SymbolAttribution struct {
	SID                       int32
	PnL, Costs, Net, Turnover float64
}

// AttributionReport is the cumulative attribution so far. Costs are positive numbers. Symbols is sorted by |Net|
// descending (SID ascending on ties).
type AttributionReport struct {
	InitialNAV, NAV                                float64
	LongPnL, ShortPnL, LongTurnover, ShortTurnover float64
	AvgLongExposure, AvgShortExposure              float64
	Samples                                        int
	Fees, Slippage, Funding, PITCost               float64
	Residual                                       float64
	Symbols                                        []SymbolAttribution
}

// Attribution returns the report, nil when attribution is not enabled.
func (b *Book) Attribution() *AttributionReport {
	a := b.attr
	if a == nil {
		return nil
	}
	st := b.State()
	r := &AttributionReport{InitialNAV: b.initNAV, NAV: st.NAV, LongPnL: a.longPnL, ShortPnL: a.shortPnL, LongTurnover: a.longTurn, ShortTurnover: a.shortTurn,
		Samples: a.samples, Fees: b.fees, Slippage: b.slippage, Funding: b.funding, PITCost: b.gapLoss + b.delistLoss}
	if a.samples > 0 {
		r.AvgLongExposure = a.expLong / float64(a.samples)
		r.AvgShortExposure = a.expShort / float64(a.samples)
	}
	r.Residual = (st.NAV - b.initNAV) - (r.LongPnL + r.ShortPnL - r.Fees - r.Slippage - r.Funding - r.PITCost)
	for sid, s := range a.bySID {
		costs := s.fees + s.slippage + s.pit
		r.Symbols = append(r.Symbols, SymbolAttribution{SID: sid, PnL: s.pnl, Costs: costs, Net: s.pnl - costs, Turnover: s.turnover})
	}
	sort.Slice(r.Symbols, func(i, j int) bool {
		x, y := math.Abs(r.Symbols[i].Net), math.Abs(r.Symbols[j].Net)
		if x != y {
			return x > y
		}
		return r.Symbols[i].SID < r.Symbols[j].SID
	})
	return r
}
