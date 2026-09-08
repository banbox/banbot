package com

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg"
)

type legacyPriceBinding struct {
	exchange string
	state    *PriceState
}

var (
	legacyPrices           = NewPriceState("")
	legacyPriceStates      = map[string]*PriceState{"": legacyPrices}
	legacyPriceCurrent     atomic.Pointer[legacyPriceBinding]
	legacyPriceExchangeMux sync.Mutex
	PriceExpireMS          = int64(60000)
)

func init() {
	legacyPriceCurrent.Store(&legacyPriceBinding{state: legacyPrices})
}

// syncLegacyPriceParser keeps the old facade tied to the current process
// exchange while leaving unchanged-exchange calls lock-free. Each exchange
// keeps its own parser and price maps so switching back restores old data.
func syncLegacyPriceParser() *PriceState {
	exgName := core.ExgName
	current := legacyPriceCurrent.Load()
	if current != nil && current.exchange == exgName {
		return current.state
	}
	legacyPriceExchangeMux.Lock()
	current = legacyPriceCurrent.Load()
	if current == nil || current.exchange != exgName {
		state := legacyPriceStates[exgName]
		if state == nil {
			// Keep configured legacy calls on the current adapter. Exchange
			// semantics, including non-standard contract symbols, belong to
			// banexg's MapMarket implementation.
			state = NewPriceStateWithStrategy(exgName, newLegacyPriceSymbolParser(exgName, legacyExchange(exgName)))
			legacyPriceStates[exgName] = state
		}
		current = &legacyPriceBinding{exchange: exgName, state: state}
		legacyPriceCurrent.Store(current)
	}
	legacyPriceExchangeMux.Unlock()
	return current.state
}

func newLegacyPriceSymbolParser(exgName string, exchange banexg.BanExchange) core.SymbolParserStrategy {
	fallback := exg.NewLegacyPriceSymbolParser(exgName)
	if exchange == nil {
		return fallback
	}
	checked := exg.NewPriceSymbolParserWithError(exgName, exchange)
	return func(pair string) [4]string {
		parts, err := checked(pair)
		if err != nil || parts == [4]string{} {
			// The old facade accepted arbitrary synthetic symbols. Preserve that
			// compatibility without weakening explicit Runtime parser validation.
			return fallback(pair)
		}
		return parts
	}
}

func legacyExchange(exgName string) banexg.BanExchange {
	exchange := exg.Default
	if exchange == nil {
		return nil
	}
	defer func() {
		if recover() != nil {
			exchange = nil
		}
	}()
	info := exchange.Info()
	if info == nil || info.ID != exgName {
		return nil
	}
	return exchange
}

const Day10MSecs = int64(864000000)

func GetPriceSafeExp(symbol string, side string, expMS int64) float64 {
	return syncLegacyPriceParser().GetPriceSafeExpAt(btime.TimeMS(), symbol, side, expMS)
}

func GetLastBarPrice(symbol string) float64 {
	return syncLegacyPriceParser().GetLastBarPriceAt(symbol)
}

func GetPriceSafe(symbol string, side string) float64 {
	return GetPriceSafeExp(symbol, side, PriceExpireMS)
}

func GetPriceExp(symbol string, side string, expMS int64) float64 {
	price := GetPriceSafeExp(symbol, side, expMS)
	if price == -1 {
		panic(fmt.Errorf("invalid symbol for price: %s", symbol))
	}
	return price
}

func GetPrice(symbol string, side string) float64 {
	return GetPriceExp(symbol, side, 10000)
}

func SetBarPrice(pair string, price float64) {
	syncLegacyPriceParser().SetBarPriceAt(btime.TimeMS(), pair, price)
}

func IsPriceEmpty() bool {
	return syncLegacyPriceParser().IsPriceEmpty()
}

func SetPrice(pair string, ask, bid float64) {
	syncLegacyPriceParser().SetPriceAt(btime.TimeMS(), pair, ask, bid)
}

func SetPrices(data map[string]float64, side string) {
	syncLegacyPriceParser().SetPricesAt(btime.TimeMS(), data, side)
}

func IsMaker(pair, side string, price float64) bool {
	curPrice := GetPriceExp(pair, side, 10000)
	isBuy := side == banexg.OdSideBuy
	isLow := price < curPrice
	return isBuy == isLow
}
