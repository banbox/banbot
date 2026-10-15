package com

import (
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type PriceSymbolParser = core.SymbolParserStrategy

// PriceState owns bar and order-book prices for one Runtime.
type PriceState struct {
	barPrices  map[string]core.Int64Flt
	bidPrices  map[string]core.Int64Flt
	askPrices  map[string]core.Int64Flt
	lockPrices sync.RWMutex
	lockBars   sync.RWMutex
	parser     *core.SymbolParser
	loadMu     sync.Mutex
	lastLoadMS int64
}

func (p *PriceState) RefreshLatestPriceAt(nowMS int64, exchange banexg.BanExchange, symbol string) *errs.Error {
	if p == nil || exchange == nil {
		return errs.NewMsg(core.ErrExgNotInit, "runtime prices and exchange are required")
	}
	p.loadMu.Lock()
	defer p.loadMu.Unlock()
	if p.lastLoadMS == 0 || nowMS-p.lastLoadMS >= 3000 {
		tickers, err := exchange.FetchTickers(nil, map[string]interface{}{banexg.ParamMethod: "bookTicker"})
		p.lastLoadMS = nowMS
		if err != nil {
			return err
		}
		for _, ticker := range tickers {
			if ticker != nil {
				p.SetPriceAt(nowMS, ticker.Symbol, ticker.Ask, ticker.Bid)
			}
		}
	}
	if p.GetPriceSafeExpAt(nowMS, symbol, "", PriceExpireMS) <= 0 {
		return errs.NewMsg(errs.CodeRunTime, "no valid price for %s", symbol)
	}
	return nil
}

func NewPriceState(exgName string) *PriceState {
	return NewPriceStateWithStrategy(exgName, exg.NewLegacyPriceSymbolParser(exgName))
}

func NewPriceStateWithStrategy(exgName string, parse PriceSymbolParser) *PriceState {
	return &PriceState{
		barPrices: make(map[string]core.Int64Flt),
		bidPrices: make(map[string]core.Int64Flt),
		askPrices: make(map[string]core.Int64Flt),
		parser:    core.NewSymbolParserWithStrategy(exgName, parse),
	}
}

func NewPriceStateWithErrorStrategy(exgName string, parse core.SymbolParserStrategyWithError) *PriceState {
	return &PriceState{
		barPrices: make(map[string]core.Int64Flt),
		bidPrices: make(map[string]core.Int64Flt),
		askPrices: make(map[string]core.Int64Flt),
		parser:    core.NewSymbolParserWithErrorStrategy(exgName, parse),
	}
}

func NewPriceStateWithExchange(exgName string, exchange banexg.BanExchange) *PriceState {
	if exchange == nil {
		return NewPriceState(exgName)
	}
	checked := exg.NewPriceSymbolParserWithError(exgName, exchange)
	return NewPriceStateWithErrorStrategy(exgName, func(pair string) ([4]string, error) {
		parts, err := checked(pair)
		if err != nil {
			return parts, err
		}
		return parts, nil
	})
}

func (p *PriceState) SetExchangeName(exgName string) {
	if p != nil {
		p.parser.SetStrategy(exgName, exg.NewLegacyPriceSymbolParser(exgName))
	}
}

func (p *PriceState) SetSymbolParser(exgName string, parse PriceSymbolParser) {
	if p != nil {
		p.parser.SetStrategy(exgName, parse)
	}
}

func (p *PriceState) GetPriceSafeExpAt(nowMS int64, symbol, side string, expMS int64) float64 {
	if p == nil {
		return -1
	}
	if core.IsFiat(symbol) && !strings.Contains(symbol, "/") {
		return 1
	}
	p.lockPrices.RLock()
	var sum float64
	var count int
	if side == banexg.OdSideBuy || side == "" {
		if item, ok := p.bidPrices[symbol]; ok && math.Abs(float64(nowMS-item.Int)) <= float64(expMS) {
			sum += item.Val
			count++
		}
	}
	if side == banexg.OdSideSell || side == "" {
		if item, ok := p.askPrices[symbol]; ok && math.Abs(float64(nowMS-item.Int)) <= float64(expMS) {
			sum += item.Val
			count++
		}
	}
	p.lockPrices.RUnlock()
	if count > 0 {
		return sum / float64(count)
	}
	p.lockBars.RLock()
	item, ok := p.barPrices[symbol]
	p.lockBars.RUnlock()
	if ok && math.Abs(float64(nowMS-item.Int)) <= float64(expMS) {
		return item.Val
	}
	return -1
}

func (p *PriceState) GetLastBarPriceAt(symbol string) float64 {
	if p == nil {
		return -1
	}
	if core.IsFiat(symbol) && !strings.Contains(symbol, "/") {
		return 1
	}
	p.lockBars.RLock()
	item, ok := p.barPrices[symbol]
	p.lockBars.RUnlock()
	if ok {
		return item.Val
	}
	return -1
}

func (p *PriceState) SetBarPriceAt(nowMS int64, pair string, price float64) {
	if p == nil {
		return
	}
	p.lockBars.Lock()
	p.setDataPrice(p.barPrices, nowMS, pair, price)
	p.lockBars.Unlock()
}

func (p *PriceState) SetPriceAt(nowMS int64, pair string, ask, bid float64) {
	if p == nil {
		return
	}
	p.lockPrices.Lock()
	var askItem, bidItem core.Int64Flt
	var hasAsk, hasBid bool
	if ask > 0 {
		askItem = core.Int64Flt{Int: nowMS, Val: ask}
		p.askPrices[pair] = askItem
		hasAsk = true
	}
	if bid > 0 {
		bidItem = core.Int64Flt{Int: nowMS, Val: bid}
		p.bidPrices[pair] = bidItem
		hasBid = true
	}
	base, quote, settle, _ := p.parser.Split(pair)
	if core.IsFiat(quote) && (settle == "" || settle == quote) {
		if hasAsk {
			p.askPrices[base] = askItem
		}
		if hasBid {
			p.bidPrices[base] = bidItem
		}
	}
	p.lockPrices.Unlock()
}

func (p *PriceState) SetPricesAt(nowMS int64, data map[string]float64, side string) {
	if p == nil {
		return
	}
	updateAsk := side == banexg.OdSideSell || side == ""
	updateBid := side == banexg.OdSideBuy || side == ""
	if !updateBid && !updateAsk {
		panic(fmt.Sprintf("invalid side: %v, use `banexg.OdSideBuy/OdSideSell` or ''", side))
	}
	p.lockPrices.Lock()
	for pair, price := range data {
		item := core.Int64Flt{Int: nowMS, Val: price}
		if updateAsk {
			p.askPrices[pair] = item
		}
		if updateBid {
			p.bidPrices[pair] = item
		}
		base, quote, settle, _ := p.parser.Split(pair)
		if core.IsFiat(quote) && (settle == "" || settle == quote) {
			if updateAsk {
				p.askPrices[base] = item
			}
			if updateBid {
				p.bidPrices[base] = item
			}
		}
	}
	p.lockPrices.Unlock()
}

func (p *PriceState) IsPriceEmpty() bool {
	if p == nil {
		return true
	}
	p.lockPrices.RLock()
	p.lockBars.RLock()
	empty := len(p.bidPrices) == 0 && len(p.barPrices) == 0
	p.lockBars.RUnlock()
	p.lockPrices.RUnlock()
	return empty
}

func (p *PriceState) Reset() {
	if p == nil {
		return
	}
	p.loadMu.Lock()
	defer p.loadMu.Unlock()
	p.lastLoadMS = 0
	p.lockPrices.Lock()
	p.bidPrices = make(map[string]core.Int64Flt)
	p.askPrices = make(map[string]core.Int64Flt)
	p.lockPrices.Unlock()
	p.lockBars.Lock()
	p.barPrices = make(map[string]core.Int64Flt)
	p.lockBars.Unlock()
}

func (p *PriceState) setDataPrice(dst map[string]core.Int64Flt, nowMS int64, pair string, price float64) {
	item := core.Int64Flt{Int: nowMS, Val: price}
	dst[pair] = item
	base, quote, settle, _ := p.parser.Split(pair)
	if core.IsFiat(quote) && (settle == "" || settle == quote) {
		dst[base] = item
	}
}
