package orm

import (
	"context"
	"strings"
	"testing"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type multiMarketStubExchange struct {
	banexg.BanExchange
	info         *banexg.ExgInfo
	archiveCalls int
	fetchCalls   []multiMarketFetchCall
}

type multiMarketFetchCall struct {
	symbol string
	market string
}

func (e *multiMarketStubExchange) Info() *banexg.ExgInfo {
	return e.info
}

// GetMarket mimics banexg.Exchange.GetMarket in contract mode: when MarketType
// is linear, looking up a spot symbol without LookupExchangeMarket rewrites
// BASE/QUOTE to BASE/QUOTE:USDT.
func (e *multiMarketStubExchange) GetMarket(symbol string) (*banexg.Market, *errs.Error) {
	if e.info == nil || e.info.Markets == nil {
		return nil, errs.NewMsg(errs.CodeNoMarketForPair, "markets not loaded")
	}
	if mar := e.info.Markets[symbol]; mar != nil {
		if mar.Spot && e.info.MarketType == banexg.MarketLinear {
			if fut := e.info.Markets[symbol+":"+mar.Quote]; fut != nil {
				return fut, nil
			}
		}
		return mar, nil
	}
	return nil, errs.NewMsg(errs.CodeNoMarketForPair, "no market for %s", symbol)
}

func (e *multiMarketStubExchange) FetchOHLCVArchive(_ context.Context, symbol, _ string, _, _ int64) ([]*banexg.Kline, bool, *errs.Error) {
	e.archiveCalls++
	return []*banexg.Kline{{Time: 1704067200000, Close: 99999}}, true, nil
}

func (e *multiMarketStubExchange) FetchOHLCV(symbol, _ string, since int64, _ int, params map[string]interface{}) ([]*banexg.Kline, *errs.Error) {
	mkt, _ := params[banexg.ParamMarket].(string)
	e.fetchCalls = append(e.fetchCalls, multiMarketFetchCall{symbol: symbol, market: mkt})
	price := 50100.0
	if mkt == banexg.MarketSpot {
		price = 50000.0
	}
	return []*banexg.Kline{{Time: since, Open: price, High: price, Low: price, Close: price, Volume: 1}}, nil
}

func TestSymbolStateSingleMarketDefaultRejectsAuxiliaryMarket(t *testing.T) {
	state := NewSymbolStateWithIdentity("binance", banexg.MarketLinear)
	if err := state.CacheExSymbolChecked(&ExSymbol{
		ID: 1, Exchange: "binance", Market: banexg.MarketLinear, Symbol: "BTC/USDT:USDT",
	}); err != nil {
		t.Fatalf("cache primary linear symbol failed: %v", err)
	}
	err := state.CacheExSymbolChecked(&ExSymbol{
		ID: 2, Exchange: "binance", Market: banexg.MarketSpot, Symbol: "BTC/USDT",
	})
	if err == nil || !strings.Contains(err.Error(), "does not belong to state identity") {
		t.Fatalf("expected single-market state to reject spot symbol, got %v", err)
	}
	if _, err := state.GetExSymbolCur("BTC/USDT"); err == nil {
		t.Fatal("expected uncached spot symbol lookup to fail")
	}
}

func TestSymbolStateAllowExtraMarketsResolvesSpotAndLinearWithoutContamination(t *testing.T) {
	spotMarket := &banexg.Market{
		ID: "BTCUSDT", Symbol: "BTC/USDT", Base: "BTC", Quote: "USDT",
		Type: banexg.MarketSpot, Spot: true, Active: true,
	}
	linearMarket := &banexg.Market{
		ID: "BTCUSDT", Symbol: "BTC/USDT:USDT", Base: "BTC", Quote: "USDT", Settle: "USDT",
		Type: banexg.MarketLinear, Contract: true, Linear: true, Swap: true, Active: true,
	}
	marMap := banexg.MarketMap{
		spotMarket.Symbol:   spotMarket,
		linearMarket.Symbol: linearMarket,
	}
	exchange := &multiMarketStubExchange{
		info: &banexg.ExgInfo{
			ID:         "binance",
			MarketType: banexg.MarketLinear,
			Markets:    marMap,
		},
	}

	strictState := NewSymbolStateWithIdentity("binance", banexg.MarketLinear)
	strictList, resErr := resolveEnsureList(strictState, marMap, "binance", banexg.MarketLinear, []string{"BTC/USDT", "BTC/USDT:USDT"}, nil, false, nil)
	if resErr != nil {
		t.Fatalf("resolveEnsureList failed: %v", resErr)
	}
	if strictList[0].Market != banexg.MarketSpot || strictList[1].Market != banexg.MarketLinear {
		t.Fatalf("expected true market types [spot, linear], got [%s, %s]", strictList[0].Market, strictList[1].Market)
	}
	if err := strictState.SetExSymbols([]*ExSymbol{
		{ID: 1, Exchange: "binance", Market: strictList[0].Market, Symbol: strictList[0].Symbol},
		{ID: 2, Exchange: "binance", Market: strictList[1].Market, Symbol: strictList[1].Symbol},
	}); err == nil {
		t.Fatal("strict single-market state accepted spot symbol")
	}

	multiState := NewSymbolStateWithIdentity("binance", banexg.MarketLinear)
	multiState.AllowExtraMarkets(banexg.MarketSpot)
	if got := multiState.ExtraMarkets(); len(got) != 1 || got[0] != banexg.MarketSpot {
		t.Fatalf("ExtraMarkets = %v, want [spot]", got)
	}
	if err := multiState.CacheExSymbolChecked(&ExSymbol{
		ID: 99, Exchange: "okx", Market: banexg.MarketSpot, Symbol: "BTC/USDT",
	}); err == nil {
		t.Fatal("multi-market state accepted foreign exchange")
	}
	if err := multiState.CacheExSymbolChecked(&ExSymbol{
		ID: 98, Exchange: "binance", Market: banexg.MarketInverse, Symbol: "BTC/USD:BTC",
	}); err == nil {
		t.Fatal("multi-market state accepted unconfigured inverse market")
	}

	if err := multiState.SetExSymbols([]*ExSymbol{
		{ID: 10, Exchange: "binance", Market: banexg.MarketSpot, Symbol: "BTC/USDT"},
		{ID: 11, Exchange: "binance", Market: banexg.MarketLinear, Symbol: "BTC/USDT:USDT"},
	}); err != nil {
		t.Fatalf("SetExSymbols with extra_markets=[spot] failed: %v", err)
	}

	gotSpot, sErr := multiState.GetExSymbolCur("BTC/USDT")
	if sErr != nil || gotSpot == nil || gotSpot.ID != 10 || gotSpot.Market != banexg.MarketSpot {
		t.Fatalf("GetExSymbolCur(BTC/USDT) = %+v, err=%v", gotSpot, sErr)
	}
	gotPerp, pErr := multiState.GetExSymbolCur("BTC/USDT:USDT")
	if pErr != nil || gotPerp == nil || gotPerp.ID != 11 || gotPerp.Market != banexg.MarketLinear {
		t.Fatalf("GetExSymbolCur(BTC/USDT:USDT) = %+v, err=%v", gotPerp, pErr)
	}

	byExgSpot, esErr := multiState.GetExSymbol(exchange, "BTC/USDT")
	if esErr != nil || byExgSpot == nil || byExgSpot.ID != 10 || byExgSpot.Market != banexg.MarketSpot {
		t.Fatalf("GetExSymbol(exchange, BTC/USDT) = %+v, err=%v", byExgSpot, esErr)
	}
	byExgPerp, epErr := multiState.GetExSymbol(exchange, "BTC/USDT:USDT")
	if epErr != nil || byExgPerp == nil || byExgPerp.ID != 11 || byExgPerp.Market != banexg.MarketLinear {
		t.Fatalf("GetExSymbol(exchange, BTC/USDT:USDT) = %+v, err=%v", byExgPerp, epErr)
	}
}

func TestFetchApiOHLCVCrossMarketPassesExplicitMarketAndSkipsPrimaryArchive(t *testing.T) {
	exchange := &multiMarketStubExchange{
		info: &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear},
	}
	startMS := int64(1704067200000)
	endMS := startMS + 60_000
	options := KlineRuntimeOptions{
		ClockValid: true,
		NowMS:      startMS + 10*24*3600*1000,
	}

	spotOut := make(chan []*banexg.Kline, 4)
	if err := fetchApiOHLCVWithMarketOptions(context.Background(), exchange, "BTC/USDT", banexg.MarketSpot, "1m", startMS, endMS, spotOut, true, options); err != nil {
		t.Fatalf("spot cross-market fetch failed: %v", err)
	}
	close(spotOut)
	var spotBars []*banexg.Kline
	for batch := range spotOut {
		spotBars = append(spotBars, batch...)
	}
	if exchange.archiveCalls != 0 {
		t.Fatalf("cross-market spot fetch invoked primary linear archive %d times", exchange.archiveCalls)
	}
	if len(exchange.fetchCalls) != 1 || exchange.fetchCalls[0].market != banexg.MarketSpot {
		t.Fatalf("expected ParamMarket=spot on FetchOHLCV, got %+v", exchange.fetchCalls)
	}
	if len(spotBars) != 1 || spotBars[0].Close != 50000.0 {
		t.Fatalf("expected spot close 50000.0, got %+v", spotBars)
	}
}
