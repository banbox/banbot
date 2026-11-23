package biz

import (
	"strings"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

// The account owner refuses a limit price that is not a multiple of the
// instrument's price tick, and a refused command stops an execution-backed
// robot. Strategies compute limits freely (average × 1.0003 …), as they always
// could: the wallet-based managers rounded through the exchange's price
// precision. These helpers give the execution-backed managers the same
// tolerance. Only prices that become ORDER prices are rounded; trigger levels
// (stop, stop-loss, take-profit, trailing activation) are thresholds compared
// with the market and may lie between ticks.

// roundToTick rounds a positive price to the nearest multiple of tick.
// Non-positive prices and ticks are returned unchanged.
func roundToTick(price float64, tick decimal.Decimal) float64 {
	if price <= 0 || !tick.IsPositive() {
		return price
	}
	rounded := decimal.NewFromFloat(price).Div(tick).Round(0).Mul(tick)
	if !rounded.IsPositive() {
		rounded = tick
	}
	return rounded.InexactFloat64()
}

// roundEntryLimits rounds every order price of an entry request in place.
func roundEntryLimits(req *strat.EnterReq, tick decimal.Decimal) {
	if req == nil {
		return
	}
	req.Limit = roundToTick(req.Limit, tick)
	req.StopLossLimit = roundToTick(req.StopLossLimit, tick)
	req.TakeProfitLimit = roundToTick(req.TakeProfitLimit, tick)
	if len(req.TakeProfits) > 0 {
		rungs := make([]strat.TakeProfitTarget, len(req.TakeProfits))
		copy(rungs, req.TakeProfits)
		for i := range rungs {
			rungs[i].Limit = roundToTick(rungs[i].Limit, tick)
		}
		req.TakeProfits = rungs
	}
}

// exitTick finds the price tick an exit request's limit applies to: the single
// symbol named by pairs, else the symbol of the addressed order, else the only
// declared instrument. ok is false when the request spans several instruments,
// where one limit price cannot be rounded for all of them.
func exitTick(instruments map[string]execution.Instrument, pairs, orderSymbol string) (decimal.Decimal, bool) {
	symbol := strings.TrimSpace(pairs)
	if symbol == "" || strings.Contains(symbol, ",") {
		symbol = orderSymbol
	}
	if instrument, found := instruments[symbol]; found {
		return instrument.PriceTick, true
	}
	if len(instruments) == 1 {
		for _, instrument := range instruments {
			return instrument.PriceTick, true
		}
	}
	return decimal.Zero, false
}
