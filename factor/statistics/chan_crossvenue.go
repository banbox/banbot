package statistics

import (
	"fmt"
)

type CrossVenueQuote struct {
	Venue, Asset, Currency                                                 string
	Bid, Ask, BidSize, AskSize, FeeRate, AvailableCash, AvailableInventory float64
	ObservedAtMS                                                           int64
}
type CrossVenueOpportunity struct {
	BuyVenue, SellVenue                               string
	Units, GrossProfit, Fees, TransferCost, NetProfit float64
}

// CrossVenueArbitrage compares actual same-asset/same-currency executable BBOs,
// capped by displayed depth and pre-funded per-venue cash/inventory. Transfer
// fees and an explicit net-profit floor are included. This is an opportunity
// calculator, not a claim of atomic settlement or eliminated venue credit risk.
func CrossVenueArbitrage(quotes []CrossVenueQuote, nowMS, maxAgeMS int64, units, transferRate, fixedTransferCost, minProfit float64) (CrossVenueOpportunity, bool, error) {
	if len(quotes) < 2 || len(quotes) > 1000 || nowMS <= 0 || maxAgeMS < 0 || !finite(units) || units <= 0 || !finite(transferRate) || transferRate < 0 || transferRate >= 1 || !finite(fixedTransferCost) || fixedTransferCost < 0 || !finite(minProfit) || minProfit < 0 {
		return CrossVenueOpportunity{}, false, fmt.Errorf("statistics: invalid cross-venue limits")
	}
	for _, q := range quotes {
		if q.Venue == "" || q.Asset == "" || q.Currency == "" || q.ObservedAtMS <= 0 || q.ObservedAtMS > nowMS || !finite(q.Bid) || !finite(q.Ask) || q.Bid <= 0 || q.Ask < q.Bid || !finite(q.BidSize) || q.BidSize < 0 || !finite(q.AskSize) || q.AskSize < 0 || !finite(q.FeeRate) || q.FeeRate < 0 || q.FeeRate >= 1 || !finite(q.AvailableCash) || q.AvailableCash < 0 || !finite(q.AvailableInventory) || q.AvailableInventory < 0 {
			return CrossVenueOpportunity{}, false, fmt.Errorf("statistics: invalid cross-venue quote")
		}
	}
	var best CrossVenueOpportunity
	found := false
	for _, buy := range quotes {
		for _, sell := range quotes {
			if buy.Venue == sell.Venue || buy.Asset != sell.Asset || buy.Currency != sell.Currency || nowMS-buy.ObservedAtMS > maxAgeMS || nowMS-sell.ObservedAtMS > maxAgeMS {
				continue
			}
			if buy.AskSize < units || sell.BidSize < units || sell.AvailableInventory < units || buy.AvailableCash < units*buy.Ask*(1+buy.FeeRate) {
				continue
			}
			gross := units * (sell.Bid - buy.Ask)
			fees := units * (sell.Bid*sell.FeeRate + buy.Ask*buy.FeeRate)
			transfer := units*buy.Ask*transferRate + fixedTransferCost
			net := gross - fees - transfer
			if !finite(net) || !finite(gross) || !finite(fees) || !finite(transfer) {
				return CrossVenueOpportunity{}, false, fmt.Errorf("statistics: cross-venue arithmetic overflow")
			}
			if net > minProfit && (!found || net > best.NetProfit) {
				best = CrossVenueOpportunity{buy.Venue, sell.Venue, units, gross, fees, transfer, net}
				found = true
			}
		}
	}
	return best, found, nil
}
