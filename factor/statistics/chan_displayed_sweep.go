package statistics

import (
	"errors"
	"math"
)

// DisplayedDepth is one frozen opposite-side book in caller-declared price and
// quantity units. Protected means its top level is a protected displayed quote.
// The caller supplies the complete protected venue set; this is not a router.
type DisplayedDepth struct {
	Venue, Instrument                string
	EventMS, AvailableMS, ReceivedMS int64
	Protected                        bool
	Levels                           []DisplayedLevel
}

type DisplayedLevel struct{ Price, Quantity float64 }

type DisplayedSweepKind string

const (
	ISOLimitIOC    DisplayedSweepKind = "iso-limit-ioc"
	LocalMarketIOC DisplayedSweepKind = "local-market-ioc"
)

type DisplayedSweepRequest struct {
	Kind                   DisplayedSweepKind
	LocalVenue, Instrument string
	// Side is +1 to buy the supplied asks, -1 to sell the supplied bids.
	Side                 int
	Quantity, LimitPrice float64
	SubmitMS, MaxAgeMS   int64
}

type DisplayedSweepFill struct {
	Venue           string
	Price, Quantity float64
}

type ProtectedSweepObligation struct {
	Venue                       string
	Price, Quantity, LimitPrice float64
}

type DisplayedSweepPlan struct {
	SubmitMS, AvailableMS, ReceivedMS int64
	LocalFills                        []DisplayedSweepFill
	// ISO obligations are ADDITIONAL full top-quote orders, not deductions
	// from the requested local quantity. They are plans, not confirmed fills.
	ProtectedOrders                                              []ProtectedSweepObligation
	LocalFilled, LocalCanceled, LocalNotional, LocalAveragePrice float64
	AveragePriceValid                                            bool
	AdditionalProtectedQuantity, AdditionalProtectedNotional     float64
}

// PlanDisplayedSweep implements Machine Trading Ex6.1 and its local market-IOC
// contrast. ISO walks only the requested venue to the limit and identifies
// other venues' full protected top sizes superior to that limit. Deeper remote
// levels are not protected. Plain market IOC fills only at the frozen NBBO on
// the local venue. No future quote change, queue outcome, fee or live submission
// is inferred. A returned plan owns its slices and never mutates input depth.
func PlanDisplayedSweep(books []DisplayedDepth, r DisplayedSweepRequest) (DisplayedSweepPlan, error) {
	var out DisplayedSweepPlan
	if len(books) == 0 || len(books) > 10000 || r.LocalVenue == "" || r.Instrument == "" || (r.Side != 1 && r.Side != -1) || !finite(r.Quantity) || r.Quantity <= 0 || r.SubmitMS < 0 || r.MaxAgeMS < 0 || (r.Kind != ISOLimitIOC && r.Kind != LocalMarketIOC) || (r.Kind == ISOLimitIOC && !finite(r.LimitPrice)) || (r.Kind == LocalMarketIOC && r.LimitPrice != 0) {
		return out, errors.New("statistics: invalid displayed sweep request")
	}
	seen := make(map[string]bool, len(books))
	local := -1
	var nbbo float64
	hasNBBO := false
	totalLevels := 0
	out.SubmitMS = r.SubmitMS
	for i, b := range books {
		if b.Venue == "" || seen[b.Venue] || b.Instrument != r.Instrument || b.EventMS < 0 || b.AvailableMS < b.EventMS || b.ReceivedMS < b.AvailableMS || b.ReceivedMS > r.SubmitMS || b.EventMS > r.SubmitMS || r.SubmitMS-b.EventMS > r.MaxAgeMS || len(b.Levels) == 0 {
			return DisplayedSweepPlan{}, errors.New("statistics: missing, stale or unavailable displayed book")
		}
		seen[b.Venue] = true
		totalLevels += len(b.Levels)
		if totalLevels > 100000 {
			return DisplayedSweepPlan{}, errors.New("statistics: displayed depth budget exceeded")
		}
		for j, l := range b.Levels {
			if !finite(l.Price) || !finite(l.Quantity) || l.Quantity <= 0 || (j > 0 && float64(r.Side)*(l.Price-b.Levels[j-1].Price) <= 0) {
				return DisplayedSweepPlan{}, errors.New("statistics: invalid or unordered displayed depth")
			}
		}
		out.AvailableMS = max(out.AvailableMS, b.AvailableMS)
		out.ReceivedMS = max(out.ReceivedMS, b.ReceivedMS)
		if b.Venue == r.LocalVenue {
			local = i
		}
		if b.Protected && (!hasNBBO || float64(r.Side)*(b.Levels[0].Price-nbbo) < 0) {
			nbbo = b.Levels[0].Price
			hasNBBO = true
		}
	}
	if local < 0 || !hasNBBO {
		return DisplayedSweepPlan{}, errors.New("statistics: local venue or protected NBBO missing")
	}
	if r.Kind == ISOLimitIOC {
		for _, b := range books {
			l := b.Levels[0]
			if b.Venue != r.LocalVenue && b.Protected && float64(r.Side)*(l.Price-r.LimitPrice) < 0 {
				out.ProtectedOrders = append(out.ProtectedOrders, ProtectedSweepObligation{b.Venue, l.Price, l.Quantity, r.LimitPrice})
				out.AdditionalProtectedQuantity += l.Quantity
				out.AdditionalProtectedNotional += l.Price * l.Quantity
			}
		}
	}
	remaining := r.Quantity
	for _, l := range books[local].Levels {
		if r.Kind == ISOLimitIOC && float64(r.Side)*(l.Price-r.LimitPrice) > 0 {
			break
		}
		if r.Kind == LocalMarketIOC && l.Price != nbbo {
			break
		}
		q := min(remaining, l.Quantity)
		out.LocalFills = append(out.LocalFills, DisplayedSweepFill{r.LocalVenue, l.Price, q})
		out.LocalFilled += q
		out.LocalNotional += q * l.Price
		remaining -= q
		if remaining <= 0 {
			break
		}
	}
	out.LocalCanceled = remaining
	if out.LocalFilled > 0 {
		out.LocalAveragePrice = out.LocalNotional / out.LocalFilled
		out.AveragePriceValid = true
	}
	if !finite(out.LocalNotional) || !finite(out.LocalAveragePrice) || !finite(out.AdditionalProtectedQuantity) || !finite(out.AdditionalProtectedNotional) {
		return DisplayedSweepPlan{}, errors.New("statistics: displayed sweep arithmetic overflow")
	}
	return out, nil
}

type DarkPoolAllocation struct {
	Fills, Remaining                            []float64
	IncomingFilled, IncomingRemaining, MidPrice float64
}

// AllocateDarkPoolProRata implements the p172 ideal continuous pro-rata example.
// All supplied resting orders are on the opposite side and eligible at the
// caller-observed NBBO midpoint. No integer-lot residual priority is invented.
func AllocateDarkPoolProRata(resting []float64, incoming, bid, ask float64) (DarkPoolAllocation, error) {
	var out DarkPoolAllocation
	if len(resting) == 0 || len(resting) > 100000 || !finite(incoming) || incoming <= 0 || !finite(bid) || !finite(ask) || bid > ask {
		return out, errors.New("statistics: invalid dark-pool allocation")
	}
	var scale float64
	for _, q := range resting {
		if !finite(q) || q <= 0 {
			return out, errors.New("statistics: invalid resting quantity")
		}
		scale = max(scale, q)
	}
	var normalized, normalizationCompensation float64
	for _, q := range resting {
		y := q/scale - normalizationCompensation
		next := normalized + y
		normalizationCompensation = (next - normalized) - y
		normalized = next
	}
	// Capacity comparison avoids overflow in sum(resting). For partial fills,
	// multiply/divide mantissas and combine exponents before one final rounding;
	// either incoming/scale or q/scale alone can underflow although its eventual
	// per-order allocation remains representable.
	allFilled := incoming/scale >= normalized
	incomingMantissa, incomingExponent := math.Frexp(incoming)
	scaleMantissa, scaleExponent := math.Frexp(scale)
	out.MidPrice = bid/2 + ask/2
	var fillCompensation float64
	for _, q := range resting {
		f := q
		if !allFilled {
			quantityMantissa, quantityExponent := math.Frexp(q)
			f = math.Ldexp(quantityMantissa*incomingMantissa/scaleMantissa/normalized, quantityExponent+incomingExponent-scaleExponent)
		}
		out.Fills = append(out.Fills, f)
		out.Remaining = append(out.Remaining, q-f)
		y := f - fillCompensation
		next := out.IncomingFilled + y
		fillCompensation = (next - out.IncomingFilled) - y
		out.IncomingFilled = next
	}
	if !finite(out.IncomingFilled) || out.IncomingFilled == 0 {
		return DarkPoolAllocation{}, errors.New("statistics: unrepresentable dark-pool fill total")
	}
	// Independently rounded subnormal shares can overfill or lose a material
	// fraction of the incoming order. Reject that result rather than choosing
	// an unspecified residual priority or concealing it with a quantity clamp.
	ratio := out.IncomingFilled / incoming
	if ratio > 1+1e-12 || (!allFilled && math.Abs(ratio-1) > 1e-12) {
		return DarkPoolAllocation{}, errors.New("statistics: pro-rata allocation cannot conserve incoming quantity")
	}
	out.IncomingRemaining = max(0, incoming-out.IncomingFilled)
	return out, nil
}
