package com

import (
	"fmt"
	"sync"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

var (
	lockLoadPrice   sync.Mutex
	lastPriceLoadAt int64
)

// EnsureLatestPrice makes sure the cached price is valid (not expired) for the symbol.
// It only fetches from exchange when the cache is invalid.
func EnsureLatestPrice(symbol string) *errs.Error {
	if !core.LiveMode {
		return nil
	}
	if GetPriceSafe(symbol, "") > 0 {
		return nil
	}
	lockLoadPrice.Lock()
	defer lockLoadPrice.Unlock()
	if GetPriceSafe(symbol, "") > 0 {
		return nil
	}
	if btime.UTCStamp()-lastPriceLoadAt < 3000 {
		// Two requests must be at least 3s apart.
		return errs.NewMsg(errs.CodeRunTime, "no valid price for %v", symbol)
	}
	_, err := GetBookTickers()
	lastPriceLoadAt = btime.UTCStamp()
	return err
}

// RefreshLatestPrice forces a price refresh when called outside bar-complete flow.
// It respects a short throttle window and returns nil if a valid cached price exists.
func RefreshLatestPrice(symbol string) *errs.Error {
	if !core.LiveMode {
		return nil
	}
	lockLoadPrice.Lock()
	defer lockLoadPrice.Unlock()
	if btime.UTCStamp()-lastPriceLoadAt < 3000 {
		if GetPriceSafe(symbol, "") > 0 {
			return nil
		}
		return errs.NewMsg(errs.CodeRunTime, "no valid price for %v", symbol)
	}
	_, err := GetBookTickers()
	lastPriceLoadAt = btime.UTCStamp()
	return err
}

// GetBookTickers fetches latest book tickers and updates the price cache.
func GetBookTickers() (map[string]*banexg.Ticker, *errs.Error) {
	key := fmt.Sprintf("%s_%s_bookTicker", core.ExgName, core.Market)
	cacheVal, exist := core.Cache.Get(key)
	var tickerMap map[string]*banexg.Ticker
	if !exist {
		tickers, err := exg.Default.FetchTickers(nil, map[string]interface{}{
			banexg.ParamMethod: "bookTicker",
		})
		if err != nil {
			return nil, err
		}
		tickerMap = make(map[string]*banexg.Ticker)
		for _, t := range tickers {
			tickerMap[t.Symbol] = t
			SetPrice(t.Symbol, t.Ask, t.Bid)
		}
		log.Info("load book prices", zap.String("mkt", core.Market), zap.Int("num", len(tickerMap)))
		core.Cache.SetWithTTL(key, tickerMap, 0, time.Millisecond*1500)
	} else {
		tickerMap = cacheVal.(map[string]*banexg.Ticker)
	}
	return tickerMap, nil
}
