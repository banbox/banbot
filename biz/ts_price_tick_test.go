package biz

import (
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestRoundToTick(t *testing.T) {
	tick := decimal.RequireFromString("0.1")
	for _, tc := range []struct{ in, want float64 }{
		{61040.10654, 61040.1}, {61040.15, 61040.2}, {61040.1, 61040.1}, {0.04, 0.1}, {0, 0}, {-5, -5},
	} {
		if got := roundToTick(tc.in, tick); got != tc.want {
			t.Fatalf("roundToTick(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
	if got := roundToTick(123.456, decimal.Zero); got != 123.456 {
		t.Fatalf("unknown tick must leave the price alone, got %v", got)
	}
	if got := roundToTick(0.00012344, decimal.RequireFromString("0.0000001")); got != 0.0001234 {
		t.Fatalf("small tick rounding = %v", got)
	}
}

func TestRoundEntryLimitsRoundsOrderPricesOnly(t *testing.T) {
	rungs := []strat.TakeProfitTarget{{Price: 105.123, Limit: 105.167}, {Rate: 0.1}}
	req := &strat.EnterReq{
		Limit: 100.04, Stop: 99.987, StopLoss: 95.123, StopLossLimit: 95.06, TakeProfit: 110.456, TakeProfitLimit: 110.44,
		ActivationPrice: 108.888, TakeProfits: rungs,
	}
	roundEntryLimits(req, decimal.RequireFromString("0.1"))
	if req.Limit != 100 || req.StopLossLimit != 95.1 || req.TakeProfitLimit != 110.4 || req.TakeProfits[0].Limit != 105.2 {
		t.Fatalf("order prices not rounded: %+v", req)
	}
	// Trigger levels are thresholds, not order prices.
	if req.Stop != 99.987 || req.StopLoss != 95.123 || req.TakeProfit != 110.456 || req.ActivationPrice != 108.888 || req.TakeProfits[0].Price != 105.123 {
		t.Fatalf("trigger levels were changed: %+v", req)
	}
	if rungs[0].Limit != 105.167 {
		t.Fatal("the caller's ladder slice was modified")
	}
	roundEntryLimits(nil, decimal.RequireFromString("0.1"))
}

func TestExitTickSelectsTheInstrumentTheLimitAppliesTo(t *testing.T) {
	two := map[string]execution.Instrument{
		"BTC": {PriceTick: decimal.RequireFromString("0.1")},
		"ETH": {PriceTick: decimal.RequireFromString("0.01")},
	}
	if tick, ok := exitTick(two, "ETH", ""); !ok || !tick.Equal(decimal.RequireFromString("0.01")) {
		t.Fatalf("single pair: %v %t", tick, ok)
	}
	if tick, ok := exitTick(two, "", "BTC"); !ok || !tick.Equal(decimal.RequireFromString("0.1")) {
		t.Fatalf("order symbol: %v %t", tick, ok)
	}
	if _, ok := exitTick(two, "BTC,ETH", ""); ok {
		t.Fatal("one limit cannot be rounded for several instruments")
	}
	if _, ok := exitTick(two, "", ""); ok {
		t.Fatal("no symbol and several instruments must not guess")
	}
	one := map[string]execution.Instrument{"BTC": {PriceTick: decimal.RequireFromString("0.5")}}
	if tick, ok := exitTick(one, "", ""); !ok || !tick.Equal(decimal.RequireFromString("0.5")) {
		t.Fatalf("single instrument: %v %t", tick, ok)
	}
}
