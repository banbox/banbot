package core

import (
	"fmt"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banexg"
	"github.com/sasha-s/go-deadlock"
	"gonum.org/v1/gonum/floats"
	"strings"
)

func getPriceBySide(ask, bid map[string]*Int64Flt, lock *deadlock.RWMutex, symbol string, side string, expMS int64) (float64, bool) {
	lock.RLock()
	curMS := btime.UTCStamp()
	priceArr := make([]float64, 0, 1)
	if side == banexg.OdSideBuy || side == "" {
		if item, ok := bid[symbol]; ok && curMS-item.Int <= expMS {
			priceArr = append(priceArr, item.Val)
		}
	}
	if side == banexg.OdSideSell || side == "" {
		if item, ok := ask[symbol]; ok && curMS-item.Int <= expMS {
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
	if IsFiat(symbol) && !strings.Contains(symbol, "/") {
		return 1
	}
	price, ok := getPriceBySide(askPrices, bidPrices, &lockPrices, symbol, side, expMS)
	if ok {
		return price
	}
	lockBarPrices.RLock()
	item, ok := barPrices[symbol]
	lockBarPrices.RUnlock()
	curMS := btime.UTCStamp()
	if ok && curMS-item.Int <= expMS {
		return price
	}
	return -1
}

func GetPriceSafe(symbol string, side string) float64 {
	return GetPriceSafeExp(symbol, side, 10000)
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

func setDataPrice(data map[string]*Int64Flt, pair string, price float64) {
	item := &Int64Flt{
		Int: btime.UTCStamp(),
		Val: price,
	}
	data[pair] = item
	base, quote, settle, _ := SplitSymbol(pair)
	if IsFiat(quote) && (settle == "" || settle == quote) {
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
	curMS := btime.UTCStamp()
	if ask > 0 {
		askPrices[pair] = &Int64Flt{
			Int: curMS,
			Val: ask,
		}
	}
	if bid > 0 {
		bidPrices[pair] = &Int64Flt{
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
	curMS := btime.UTCStamp()
	for pair, price := range data {
		item := &Int64Flt{
			Int: curMS,
			Val: price,
		}
		if updateAsk {
			askPrices[pair] = item
		}
		if updateBid {
			bidPrices[pair] = item
		}
		base, quote, settle, _ := SplitSymbol(pair)
		if IsFiat(quote) && (settle == "" || settle == quote) {
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
