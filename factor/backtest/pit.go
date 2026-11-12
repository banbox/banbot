package backtest

import (
	"bytes"
	"encoding/json"
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
	b.settleFunding()
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
		b.attr.addPIT(sid, pen)
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
		b.attr.trade(sid, qty, 0, mark.Price, cost, math.Abs(qty*(price-mark.Price)))
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

// ForgetPolicyAssets (lifecycle policy under PIT drops) removes instruments that left the point-in-time universe
// from the policy checkpoint (scrub), the previous allocation target and the unacknowledged fill facts, and bumps the
// state version (only when something changed) so the runner reloads the policy's view from PolicyEvidence. Call it after ExitAtLastMark: the
// positions are already flat, so the policy sees the instruments as never held. A nil scrub leaves the checkpoint.
func (b *Book) ForgetPolicyAssets(sids []int32, scrub func(json.RawMessage, []int32) (json.RawMessage, error)) error {
	if len(sids) == 0 {
		return nil
	}
	changed := false
	if scrub != nil && len(b.policyState) > 0 {
		next, err := scrub(append(json.RawMessage(nil), b.policyState...), sids)
		if err != nil {
			return err
		}
		if !bytes.Equal(next, b.policyState) {
			changed = true
		}
		b.policyState = next
	}
	if b.previousAllocation != nil {
		kept := b.previousAllocation.Allocations()
		for _, sid := range sids {
			if _, ok := kept[sid]; ok {
				delete(kept, sid)
				changed = true
			}
		}
		if changed {
			next, err := factor.NewPortfolioTarget(b.previousAllocation.Spec(), kept)
			if err != nil {
				return err
			}
			b.previousAllocation = next
			changed = true
		}
	}
	gone := make(map[int32]bool, len(sids))
	for _, sid := range sids {
		gone[sid] = true
	}
	remaining := b.fillFacts[:0]
	for _, fill := range b.fillFacts {
		if !gone[fill.sid] {
			remaining = append(remaining, fill)
		}
	}
	if len(remaining) != len(b.fillFacts) {
		changed = true
	}
	b.fillFacts = remaining
	if changed {
		b.stateVersion++
	}
	return nil
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
	b.attr.addPIT(sid, -ex.penalty)
	var loss float64
	if ex.qty > 0 && next < ex.price {
		loss = ex.qty * (ex.price - next)
	} else if ex.qty < 0 && next > ex.price {
		loss = -ex.qty * (next - ex.price)
	}
	if loss > 0 {
		b.cash -= loss
		b.gapLoss += loss
		b.attr.addPIT(sid, loss)
	}
}

// PITExitLosses returns the cumulative adverse cost booked by PIT exits: gap = hindsight-avoiding adjustment of
// recovered gaps, delist = penalty of exits whose data never resumed (so far).
func (b *Book) PITExitLosses() (gap, delist float64) { return b.gapLoss, b.delistLoss }

// NeverSeenAll reports whether none of the SIDs was ever marked, held, or PIT-exited (instruments that are not
// listed yet). Dropping such SIDs from the universe cannot desynchronise any position/policy state (081).
func (b *Book) NeverSeenAll(sids []int32) bool {
	for _, sid := range sids {
		if _, ok := b.marks[sid]; ok {
			return false
		}
		if b.quantities[sid] != 0 {
			return false
		}
		if _, ok := b.pitExits[sid]; ok {
			return false
		}
		if _, ok := b.firstFill[sid]; ok {
			return false
		}
		if b.previousAllocation != nil {
			if _, ok := b.previousAllocation.Allocations()[sid]; ok {
				return false
			}
		}
	}
	return true
}

// maxFundingAccrualMS caps one accrual step so a data hole is not charged as continuous holding (positions across
// holes are PIT-exited anyway).
const maxFundingAccrualMS = 3 * 24 * 3600 * 1000

// SetFundingApr (081, net basis since BtFeatureLevel 13) enables the funding approximation: percent per year charged
// on the NET LONG notional of the whole book, max(0, long notional - short notional), for the time between marks.
// A pure long book pays on its long notional (as before); a fully hedged book pays 0; a net short book pays 0 and
// receives nothing (conservative, no credit for shorts). The charge is booked into State.Funding and reduces cash
// (an account-level item: Attribution carries it only in AttributionReport.Funding, never in a symbol's costs).
// Real per-settlement funding (ApplyFunding) is independent of it.
func (b *Book) SetFundingApr(apr float64) {
	if apr > 0 && finite(apr) {
		b.fundingApr = apr
	}
}

const fundingYearMS = 365.25 * 24 * 3600 * 1000

// fundingStep is the accumulator of one engine billing step (the `now` of the Mark calls: all quotes the runner
// delivers for one batch time). Marks of one step may carry slightly different quote times (AtMS), the step groups
// them.
//
// Each marked instrument adds its signed funding term (qty x previous mark price x apr x its own interval, capped;
// negative for a short) and the cash follows the change of max(0, sum): the sum is exactly the step's net charge once
// the step is complete, and the order the instruments are marked in does not change it. An instrument that is held
// but has no mark in the step (a missing bar) still hedges: when the book holds a short, settleFunding adds its term
// at its last valid price for the step's time (and remembers that the time is already billed, fundingBilled, so its
// next mark does not bill it again). A book without shorts never adds such terms: every long leg is charged at its own
// marks over its own interval, bit for bit as before the net basis.
func (b *Book) accrueFunding(sid int32, old Quote, atMS, now int64) {
	qty := b.quantities[sid]
	if qty == 0 {
		return
	}
	b.fundingMarked[sid] = now
	from := old.AtMS
	if t, ok := b.fundingBilled[sid]; ok {
		delete(b.fundingBilled, sid)
		from = max(from, t)
	}
	if atMS <= from || old.Price <= 0 {
		return
	}
	dt := float64(min(atMS-from, maxFundingAccrualMS))
	b.addFundingTerm(qty * old.Price * b.fundingApr / 100 * dt / fundingYearMS)
}

// addFundingTerm adds one signed term to the step's sum and books the change of max(0, sum).
func (b *Book) addFundingTerm(c float64) {
	if c == 0 {
		return
	}
	prev := b.fundingNet
	b.fundingNet += c
	var delta float64
	if prev >= 0 && b.fundingNet >= 0 {
		delta = c // no floor involved: exactly the term (bitwise the per-leg charge of a long-only book)
	} else {
		delta = math.Max(b.fundingNet, 0) - math.Max(prev, 0)
	}
	if delta != 0 {
		b.cash -= delta
		b.funding += delta
	}
}

// settleFunding completes the pending billing step: held instruments without a mark in the step take part in the
// net with their last valid price when the book holds a short (see accrueFunding). Called before anything reads or
// changes the cash or the quantities, and when the next step starts.
func (b *Book) settleFunding() {
	if !b.fundingPending {
		return
	}
	b.fundingPending = false
	var missing []int32
	short := false
	for sid, qty := range b.quantities {
		if qty < 0 {
			short = true
		}
		if qty != 0 {
			if t, ok := b.fundingMarked[sid]; !ok || t != b.fundingStep {
				missing = append(missing, sid)
			}
		}
	}
	if !short || len(missing) == 0 {
		return
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	for _, sid := range missing {
		m, ok := b.marks[sid]
		if !ok || m.Price <= 0 {
			continue
		}
		from := m.AtMS
		if t, ok := b.fundingBilled[sid]; ok {
			from = max(from, t)
		}
		if b.fundingStep <= from {
			continue
		}
		dt := float64(min(b.fundingStep-from, maxFundingAccrualMS))
		b.fundingBilled[sid] = b.fundingStep
		b.addFundingTerm(b.quantities[sid] * m.Price * b.fundingApr / 100 * dt / fundingYearMS)
	}
}
