package statistics

import "errors"

// MatchedTradeQuote contains an actual trade and a caller-matched historical
// BBO, in explicit sequence order. The source specifies no universal quote lag.
type MatchedTradeQuote struct {
	Sequence                                        int64
	EventMS, AvailableMS, ReceivedMS                int64
	Price, Quantity                                 float64
	QuoteEventMS, QuoteAvailableMS, QuoteReceivedMS int64
	Bid, Ask                                        float64
}

type TradeDirectionEstimate struct {
	Sequence, EventMS, AvailableMS, ReceivedMS int64
	TickSign, QuoteSign, LeeReadySign          int
	TickFlow, QuoteFlow, LeeReadyFlow          float64
}

type TradeClassificationReport struct {
	Rows          []TradeDirectionEstimate
	PendingPrefix bool
}

// ClassifyMatchedTrades follows Machine Trading pp186–187: quote rule is zero
// at midpoint, and Lee–Ready instead uses tick direction there. Unchanged trade
// prices carry the prior tick sign. Initial sign and quote lag/age are explicit
// caller choices. Processing stops at the first unreceived trade or quote; it
// never skips an unseen predecessor and manufactures a different tick history.
// These estimates are not observed aggressor tags or a hidden-order simulator.
func ClassifyMatchedTrades(rows []MatchedTradeQuote, initialSign int, quoteLagMS, maxQuoteAgeMS, cutoffMS int64) (TradeClassificationReport, error) {
	var out TradeClassificationReport
	if len(rows) == 0 || len(rows) > 1000000 || (initialSign != 1 && initialSign != -1) || quoteLagMS < 0 || maxQuoteAgeMS < quoteLagMS || cutoffMS < 0 {
		return out, errors.New("statistics: invalid trade classification bounds")
	}
	sign := initialSign
	var available, received int64
	for i, r := range rows {
		if r.EventMS > cutoffMS || r.AvailableMS > cutoffMS || r.ReceivedMS > cutoffMS || r.QuoteAvailableMS > cutoffMS || r.QuoteReceivedMS > cutoffMS {
			out.PendingPrefix = true
			break
		}
		if r.Sequence < 0 || r.EventMS < 0 || r.AvailableMS < r.EventMS || r.ReceivedMS < r.AvailableMS || r.QuoteEventMS < 0 || r.QuoteAvailableMS < r.QuoteEventMS || r.QuoteReceivedMS < r.QuoteAvailableMS || r.QuoteEventMS > r.EventMS || r.EventMS-r.QuoteEventMS < quoteLagMS || r.EventMS-r.QuoteEventMS > maxQuoteAgeMS || !finite(r.Price) || !finite(r.Quantity) || r.Quantity <= 0 || !finite(r.Bid) || !finite(r.Ask) || r.Bid > r.Ask || (i > 0 && (r.Sequence <= rows[i-1].Sequence || r.Sequence-rows[i-1].Sequence != 1 || r.EventMS < rows[i-1].EventMS)) {
			return TradeClassificationReport{}, errors.New("statistics: invalid trade or matched quote chronology")
		}
		if i > 0 {
			if r.Price > rows[i-1].Price {
				sign = 1
			} else if r.Price < rows[i-1].Price {
				sign = -1
			}
		}
		quote := 0
		mid := r.Bid/2 + r.Ask/2
		if r.Price > mid {
			quote = 1
		} else if r.Price < mid {
			quote = -1
		}
		lee := quote
		if lee == 0 {
			lee = sign
		}
		available = max(available, r.AvailableMS, r.QuoteAvailableMS)
		received = max(received, r.ReceivedMS, r.QuoteReceivedMS)
		out.Rows = append(out.Rows, TradeDirectionEstimate{r.Sequence, r.EventMS, available, received, sign, quote, lee, float64(sign) * r.Quantity, float64(quote) * r.Quantity, float64(lee) * r.Quantity})
	}
	return out, nil
}
