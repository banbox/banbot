package statistics

import (
	"math"
	"testing"
)

func TestChanCrossVenueBookQuotesAndPrefunding(t *testing.T) {
	q := []CrossVenueQuote{{Venue: "Bitfinex", Asset: "BTC", Currency: "USD", Bid: 239.19, Ask: 239.48, BidSize: 1, AskSize: 1, FeeRate: .002, AvailableCash: 1000, AvailableInventory: 1, ObservedAtMS: 100}, {Venue: "btc-e", Asset: "BTC", Currency: "USD", Bid: 233.232, Ask: 233.546, BidSize: 1, AskSize: 1, FeeRate: .002, AvailableCash: 1000, AvailableInventory: 1, ObservedAtMS: 100}}
	p, ok, err := CrossVenueArbitrage(q, 100, 0, 1, .01, 0, 0)
	if err != nil || !ok || p.BuyVenue != "btc-e" || p.SellVenue != "Bitfinex" || math.Abs(p.GrossProfit-5.644) > 1e-9 || math.Abs(p.NetProfit-2.363068) > 1e-9 {
		t.Fatalf("source quotes %+v %v %v", p, ok, err)
	}
	for _, change := range []func([]CrossVenueQuote){func(q []CrossVenueQuote) { q[0].AvailableInventory = 0 }, func(q []CrossVenueQuote) { q[1].AvailableCash = 233.546 }, func(q []CrossVenueQuote) { q[1].ObservedAtMS = 99 }, func(q []CrossVenueQuote) { q[1].Currency = "EUR" }, func(q []CrossVenueQuote) { q[1].AskSize = .5 }} {
		copy := append([]CrossVenueQuote(nil), q...)
		change(copy)
		_, ok, err := CrossVenueArbitrage(copy, 100, 0, 1, .01, 0, 0)
		if err != nil || ok {
			t.Fatal("invalid prefunding/quote identity still arbitrages")
		}
	}
}
