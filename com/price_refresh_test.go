package com

import (
	"testing"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type priceTickerExchange struct {
	banexg.BanExchange
	calls int
	price float64
}

func (exchange *priceTickerExchange) FetchTickers([]string, map[string]interface{}) ([]*banexg.Ticker, *errs.Error) {
	exchange.calls++
	return []*banexg.Ticker{{Symbol: "BTC/USDT", Ask: exchange.price, Bid: exchange.price}}, nil
}

func TestPriceRefreshIsolationAndThrottle(t *testing.T) {
	first, second := NewPriceState("binance"), NewPriceState("binance")
	firstExchange := &priceTickerExchange{price: 100}
	secondExchange := &priceTickerExchange{price: 200}
	for _, nowMS := range []int64{10_000, 11_000} {
		if err := first.RefreshLatestPriceAt(nowMS, firstExchange, "BTC/USDT"); err != nil {
			t.Fatal(err)
		}
		if err := second.RefreshLatestPriceAt(nowMS, secondExchange, "BTC/USDT"); err != nil {
			t.Fatal(err)
		}
	}
	if firstExchange.calls != 1 || secondExchange.calls != 1 {
		t.Fatalf("refresh counts: %d, %d", firstExchange.calls, secondExchange.calls)
	}
	if first.GetPriceSafeExpAt(11_000, "BTC/USDT", "", 60_000) != 100 || second.GetPriceSafeExpAt(11_000, "BTC/USDT", "", 60_000) != 200 {
		t.Fatal("runtime price caches interfered")
	}
}
