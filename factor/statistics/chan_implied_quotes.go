package statistics

import (
	"errors"
	"fmt"
)

// CalendarBBO is an independently sourced outright or native spread quote.
// Zero quantity means that side is absent. LiquidityID identifies the underlying
// book snapshot, not a synthetic quote; it must differ across the three books.
type CalendarBBO struct {
	Instrument, LiquidityID          string
	Bid, Ask, BidQty, AskQty         float64
	EventMS, AvailableMS, ReceivedMS int64
}

type CalendarQuoteSide struct {
	Price, Quantity                  float64
	EventMS, AvailableMS, ReceivedMS int64
	Dependencies                     [2]string
	// Relation compares the candidate with the target native BBO side.
	Relation, ComparisonLiquidityID string
}

type CalendarImpliedBBO struct {
	Instrument string
	Bid, Ask   *CalendarQuoteSide
}

type CalendarImpliedQuotes struct {
	AsOfMS                   int64
	ImpliedIn                CalendarImpliedBBO
	HImpliedOut, MImpliedOut CalendarImpliedBBO
}

// ChanCalendarImpliedQuotes implements Box6.3 for spread H-M: exactly one H
// and one M contract in the same declared price units. The caller supplies
// chronological histories and distinct instrument identities; no FX/ratio
// conversion is inferred. Latest visible quotes use event order, with equal
// timestamps retaining supplied order. No stale-age limit is invented.
// Each side is an alternative capacity bound, NEVER an additive liquidity
// allocation: synthetic quotes can share spread orders and outright depth.
func ChanCalendarImpliedQuotes(hID, mID, spreadID string, h, m, spread []CalendarBBO, asOfMS int64) (CalendarImpliedQuotes, error) {
	var out CalendarImpliedQuotes
	if asOfMS < 0 || hID == "" || mID == "" || spreadID == "" || hID == mID || hID == spreadID || mID == spreadID {
		return out, errors.New("statistics: invalid calendar instrument ownership/asof")
	}
	selectQuote := func(id string, tape []CalendarBBO) (*CalendarBBO, error) {
		if len(tape) > 1000000 {
			return nil, errors.New("statistics: calendar history exceeds limit")
		}
		var selected *CalendarBBO
		for i, q := range tape {
			// Validate tape structure independently of price payload. Unreceived
			// and future quote content cannot invalidate an earlier snapshot.
			if !validScalar(PITScalar{EventMS: q.EventMS, AvailableMS: q.AvailableMS, ReceivedMS: q.ReceivedMS}) || (i > 0 && q.EventMS < tape[i-1].EventMS) {
				return nil, fmt.Errorf("statistics: malformed calendar chronology %s at %d", id, i)
			}
			if q.EventMS > asOfMS || q.AvailableMS > asOfMS || q.ReceivedMS > asOfMS {
				continue
			}
			if q.Instrument != id || q.LiquidityID == "" || !afmlFinite(q.Bid) || !afmlFinite(q.Ask) || !afmlFinite(q.BidQty) || !afmlFinite(q.AskQty) || q.BidQty < 0 || q.AskQty < 0 || q.BidQty+q.AskQty == 0 || (q.BidQty > 0 && q.AskQty > 0 && q.Bid > q.Ask) {
				return nil, fmt.Errorf("statistics: malformed calendar quote %s at %d", id, i)
			}
			copy := q
			selected = &copy
		}
		return selected, nil
	}
	qh, err := selectQuote(hID, h)
	if err != nil {
		return out, err
	}
	qm, err := selectQuote(mID, m)
	if err != nil {
		return out, err
	}
	qs, err := selectQuote(spreadID, spread)
	if err != nil {
		return out, err
	}
	if qh == nil || qm == nil || qh.BidQty == 0 || qh.AskQty == 0 || qm.BidQty == 0 || qm.AskQty == 0 {
		return out, errors.New("statistics: visible two-sided outright quotes required")
	}
	if qh.LiquidityID == qm.LiquidityID || (qs != nil && (qs.LiquidityID == qh.LiquidityID || qs.LiquidityID == qm.LiquidityID)) {
		return out, errors.New("statistics: shared calendar book identity")
	}
	makeSide := func(price, qty float64, a, b *CalendarBBO, aSide, bSide string, bid bool, target *CalendarBBO) (*CalendarQuoteSide, error) {
		if !afmlFinite(price) {
			return nil, errors.New("statistics: nonfinite implied price")
		}
		side := &CalendarQuoteSide{Price: price, Quantity: qty, EventMS: max(a.EventMS, b.EventMS), AvailableMS: max(a.AvailableMS, b.AvailableMS), ReceivedMS: max(a.ReceivedMS, b.ReceivedMS), Dependencies: [2]string{a.LiquidityID + ":" + aSide, b.LiquidityID + ":" + bSide}}
		if target != nil {
			native, targetSide := target.Ask, "ask"
			if bid {
				native, targetSide = target.Bid, "bid"
			}
			side.Relation = "at_touch"
			if (bid && price > native) || (!bid && price < native) {
				side.Relation = "improved"
			} else if price != native {
				side.Relation = "worse"
			}
			side.ComparisonLiquidityID = target.LiquidityID + ":" + targetSide
			side.EventMS = max(side.EventMS, target.EventMS)
			side.AvailableMS = max(side.AvailableMS, target.AvailableMS)
			side.ReceivedMS = max(side.ReceivedMS, target.ReceivedMS)
		}
		return side, nil
	}
	out.AsOfMS = asOfMS
	out.ImpliedIn.Instrument, out.HImpliedOut.Instrument, out.MImpliedOut.Instrument = spreadID, hID, mID
	out.ImpliedIn.Bid, err = makeSide(qh.Bid-qm.Ask, min(qh.BidQty, qm.AskQty), qh, qm, "bid", "ask", true, nil)
	if err != nil {
		return CalendarImpliedQuotes{}, err
	}
	out.ImpliedIn.Ask, err = makeSide(qh.Ask-qm.Bid, min(qh.AskQty, qm.BidQty), qh, qm, "ask", "bid", false, nil)
	if err != nil {
		return CalendarImpliedQuotes{}, err
	}
	if qs == nil {
		return out, nil
	}
	if qs.BidQty > 0 {
		out.HImpliedOut.Bid, err = makeSide(qs.Bid+qm.Bid, min(qs.BidQty, qm.BidQty), qs, qm, "bid", "bid", true, qh)
		if err != nil {
			return CalendarImpliedQuotes{}, err
		}
		out.MImpliedOut.Ask, err = makeSide(qh.Ask-qs.Bid, min(qh.AskQty, qs.BidQty), qh, qs, "ask", "bid", false, qm)
		if err != nil {
			return CalendarImpliedQuotes{}, err
		}
	}
	if qs.AskQty > 0 {
		out.HImpliedOut.Ask, err = makeSide(qs.Ask+qm.Ask, min(qs.AskQty, qm.AskQty), qs, qm, "ask", "ask", false, qh)
		if err != nil {
			return CalendarImpliedQuotes{}, err
		}
		out.MImpliedOut.Bid, err = makeSide(qh.Bid-qs.Ask, min(qh.BidQty, qs.AskQty), qh, qs, "bid", "ask", true, qm)
		if err != nil {
			return CalendarImpliedQuotes{}, err
		}
	}
	return out, nil
}
