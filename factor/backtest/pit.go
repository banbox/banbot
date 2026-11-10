package backtest

import (
	"math"
	"sort"

	"github.com/banbox/banbot/factor"
)

// ExitAtLastMark (067, PIT universe) force-closes the positions of instruments that have dropped out of the
// point-in-time universe (data ended / is missing): each is sold (long) or bought back (short) at its last valid
// mark with the usual slippage and fee, and removed from the previous target portfolio so later full-mode
// rebalances never ask for a quote of a dead instrument. Returns the SIDs actually closed. The mark time is not
// advanced; NAV is continuous (qty x last mark -> cash minus costs).
func (b *Book) ExitAtLastMark(sids []int32, fee, slip float64) []int32 {
	sorted := append([]int32(nil), sids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var closed []int32
	for _, sid := range sorted {
		qty := b.quantities[sid]
		if qty == 0 {
			continue
		}
		mark, ok := b.marks[sid]
		if !ok || mark.Price <= 0 || !finite(mark.Price) {
			continue
		}
		price := mark.Price
		if b.pitExits == nil {
			b.pitExits = map[int32]pitExit{}
		}
		pen := math.Abs(qty) * mark.Price * b.pitPenaltyBps / 10000
		b.pitExits[sid] = pitExit{qty: qty, price: mark.Price, atMS: mark.AtMS, penalty: pen}
		b.cash -= pen
		b.delistLoss += pen
		if qty > 0 {
			price *= 1 - slip
		} else {
			price *= 1 + slip
		}
		cost := math.Abs(qty*price) * fee
		b.cash += qty*price - cost
		b.fees += cost
		b.slippage += math.Abs(qty * (price - mark.Price))
		b.turnover += math.Abs(qty * mark.Price)
		delete(b.quantities, sid)
		delete(b.firstFill, sid)
		closed = append(closed, sid)
	}
	if len(closed) > 0 {
		b.ledgerCursor++
	}
	if b.previous != nil {
		if p, err := DropTargets(b.previous, sorted); err == nil {
			b.previous = p
		}
	}
	return closed
}

// DropTargets returns p without the given SIDs (same spec/sequence). Nil-safe.
func DropTargets(p *factor.TargetPortfolio, drop []int32) (*factor.TargetPortfolio, error) {
	if p == nil || len(drop) == 0 {
		return p, nil
	}
	targets := p.Targets()
	changed := false
	for _, sid := range drop {
		if _, ok := targets[sid]; ok {
			delete(targets, sid)
			changed = true
		}
	}
	if !changed {
		return p, nil
	}
	return factor.NewTargetPortfolio(p.Spec(), targets)
}

// pitExit is a position closed by ExitAtLastMark whose outcome (delisted for good vs data gap that recovers) is
// only known later (071).
type pitExit struct {
	qty, price, penalty float64
	atMS                int64
}

// SetPITDelistPenaltyBps sets the extra adverse penalty (bp of notional) charged when a position is closed
// because its instrument dropped out of the PIT universe, assuming the data never resumes (delist). If the
// instrument's data later resumes the penalty is refunded and the gap rule applies instead.
func (b *Book) SetPITDelistPenaltyBps(bps float64) {
	if bps > 0 && finite(bps) {
		b.pitPenaltyBps = bps
	}
}

// resolveRecovered handles an instrument whose data resumes after a PIT exit (data_gap): the delist penalty is
// refunded, and the position is charged the adverse move across the hole as if it had been closed at the worse of
// the last mark before and the first mark after it (long: lower, short: higher). The first post-gap mark is the
// first quote that arrives (its close; quotes carry no open).
func (b *Book) resolveRecovered(sid int32, ex pitExit, next float64) {
	delete(b.pitExits, sid)
	b.cash += ex.penalty
	b.delistLoss -= ex.penalty
	var loss float64
	if ex.qty > 0 && next < ex.price {
		loss = ex.qty * (ex.price - next)
	} else if ex.qty < 0 && next > ex.price {
		loss = -ex.qty * (next - ex.price)
	}
	if loss > 0 {
		b.cash -= loss
		b.gapLoss += loss
	}
}

// PITExitLosses returns the cumulative adverse cost booked by PIT exits: gap = hindsight-avoiding adjustment of
// recovered gaps, delist = penalty of exits whose data never resumed (so far).
func (b *Book) PITExitLosses() (gap, delist float64) { return b.gapLoss, b.delistLoss }
