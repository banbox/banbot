package com

import (
	"fmt"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/sasha-s/go-deadlock"
	"gonum.org/v1/gonum/floats"
	"math"
	"strings"
)

var (
	barPrices     = make(map[string]*core.Int64Flt) // Latest price of each coin from bar, only for backtesting etc. The key can be a trading pair or a coin code 来自bar的每个币的最新价格，仅用于回测等。键可以是交易对，也可以是币的code
	bidPrices     = make(map[string]*core.Int64Flt) // The latest order book price of the trading pair is only used for real-time simulation or real trading. The key can be a trading pair or a coin code 交易对的最新订单簿价格，仅用于实时模拟或实盘。键可以是交易对，也可以是币的code
	askPrices     = make(map[string]*core.Int64Flt)
	lockPrices    deadlock.RWMutex
	lockBarPrices deadlock.RWMutex
	PriceExpireMS = int64(60000)
)

const (
	Day10MSecs = int64(864000000)
)

func getPriceBySide(ask, bid map[string]*core.Int64Flt, lock *deadlock.RWMutex, symbol string, side string, expMS int64) (float64, bool) {
	lock.RLock()
	curMS := btime.TimeMS()
	priceArr := make([]float64, 0, 1)
	expMSFlt := float64(expMS)
	if side == banexg.OdSideBuy || side == "" {
		if item, ok := bid[symbol]; ok && math.Abs(float64(curMS-item.Int)) <= expMSFlt {
			priceArr = append(priceArr, item.Val)
		}
	}
	if side == banexg.OdSideSell || side == "" {
		if item, ok := ask[symbol]; ok && math.Abs(float64(curMS-item.Int)) <= expMSFlt {
			priceArr = append(priceArr, item.Val)
		}
	}
	lock.RUnlock()
	if len(priceArr) > 0 {
		if len(priceArr) == 1 {
			return priceArr[0], true
		}
		return floats.Sum(priceArr) / float64(len(priceArr)), true
	}
	return 0, false
}

func GetPriceSafeExp(symbol string, side string, expMS int64) float64 {
	if core.IsFiat(symbol) && !strings.Contains(symbol, "/") {
		return 1
	}
	price, ok := getPriceBySide(askPrices, bidPrices, &lockPrices, symbol, side, expMS)
	if ok {
		return price
	}
	lockBarPrices.RLock()
	item, ok := barPrices[symbol]
	lockBarPrices.RUnlock()
	curMS := btime.TimeMS()
	if ok && math.Abs(float64(curMS-item.Int)) <= float64(expMS) {
		return item.Val
	}
	return -1
}

// GetPriceSafe return -1 if price expired or not found
func GetPriceSafe(symbol string, side string) float64 {
	return GetPriceSafeExp(symbol, side, PriceExpireMS)
}

// GetPriceExp panic if price expired before expMS or not found
func GetPriceExp(symbol string, side string, expMS int64) float64 {
	price := GetPriceSafeExp(symbol, side, expMS)
	if price == -1 {
		panic(fmt.Errorf("invalid symbol for price: %s", symbol))
	}
	return price
}

// GetPrice panic if price expired or not found
func GetPrice(symbol string, side string) float64 {
	return GetPriceExp(symbol, side, 10000)
}

func setDataPrice(data map[string]*core.Int64Flt, pair string, price float64) {
	item := &core.Int64Flt{
		Int: btime.TimeMS(),
		Val: price,
	}
	data[pair] = item
	base, quote, settle, _ := core.SplitSymbol(pair)
	if core.IsFiat(quote) && (settle == "" || settle == quote) {
		data[base] = item
	}
}

func SetBarPrice(pair string, price float64) {
	lockBarPrices.Lock()
	setDataPrice(barPrices, pair, price)
	lockBarPrices.Unlock()
}

func IsPriceEmpty() bool {
	lockPrices.RLock()
	lockBarPrices.RLock()
	empty := len(bidPrices) == 0 && len(barPrices) == 0
	lockBarPrices.RUnlock()
	lockPrices.RUnlock()
	return empty
}

func SetPrice(pair string, ask, bid float64) {
	lockPrices.Lock()
	curMS := btime.TimeMS()
	if ask > 0 {
		askPrices[pair] = &core.Int64Flt{
			Int: curMS,
			Val: ask,
		}
	}
	if bid > 0 {
		bidPrices[pair] = &core.Int64Flt{
			Int: curMS,
			Val: bid,
		}
	}
	lockPrices.Unlock()
}

func SetPrices(data map[string]float64, side string) {
	updateAsk := side == banexg.OdSideSell || side == ""
	updateBid := side == banexg.OdSideBuy || side == ""
	if !updateBid && !updateAsk {
		panic(fmt.Sprintf("invalid side: %v, use `banexg.OdSideBuy/OdSideSell` or ''", side))
	}
	lockPrices.Lock()
	curMS := btime.TimeMS()
	for pair, price := range data {
		item := &core.Int64Flt{
			Int: curMS,
			Val: price,
		}
		if updateAsk {
			askPrices[pair] = item
		}
		if updateBid {
			bidPrices[pair] = item
		}
		base, quote, settle, _ := core.SplitSymbol(pair)
		if core.IsFiat(quote) && (settle == "" || settle == quote) {
			if updateAsk {
				askPrices[base] = item
			}
			if updateBid {
				bidPrices[base] = item
			}
		}
	}
	lockPrices.Unlock()
}

func IsMaker(pair, side string, price float64) bool {
	curPrice := GetPrice(pair, side)
	isBuy := side == banexg.OdSideBuy
	isLow := price < curPrice
	return isBuy == isLow
}
