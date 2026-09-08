package com

import (
	"sync"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type priceMarketMapperStub struct {
	banexg.BanExchange
	market *banexg.Market
	calls  int
}

func (s *priceMarketMapperStub) MapMarket(string, int) (*banexg.Market, *errs.Error) {
	s.calls++
	return s.market, nil
}

func resetLegacyPriceState(t testing.TB, exchange string) {
	t.Helper()
	oldExchange := core.ExgName
	core.ExgName = exchange
	resetLegacyPriceCaches()
	t.Cleanup(func() {
		resetLegacyPriceCaches()
		core.ExgName = oldExchange
	})
}

func resetLegacyPriceCaches() {
	legacyPriceExchangeMux.Lock()
	for _, prices := range legacyPriceStates {
		prices.Reset()
	}
	legacyPriceExchangeMux.Unlock()
}

func TestLegacyPriceStateChinaWithoutExchangeKeepsRawSymbol(t *testing.T) {
	resetLegacyPriceState(t, "china")

	SetPrice("AU2406", 102, 98)
	if got := GetPriceSafeExp("AU", banexg.OdSideBuy, Day10MSecs); got != -1 {
		t.Fatalf("unconfigured china buy base alias = %v, want -1", got)
	}
	if got := GetPriceSafeExp("AU2406", banexg.OdSideBuy, Day10MSecs); got != 98 {
		t.Fatalf("unconfigured china raw buy price = %v, want 98", got)
	}
}

func TestLegacyPriceStateChinaSetPricesWithoutExchangeKeepsRawSymbol(t *testing.T) {
	resetLegacyPriceState(t, "china")

	SetPrices(map[string]float64{"AU2406": 103}, banexg.OdSideSell)
	if got := GetPriceSafeExp("AU", banexg.OdSideSell, Day10MSecs); got != -1 {
		t.Fatalf("unconfigured china batch base alias = %v, want -1", got)
	}
	if got := GetPriceSafeExp("AU2406", banexg.OdSideSell, Day10MSecs); got != 103 {
		t.Fatalf("unconfigured china raw sell price = %v, want 103", got)
	}
}

func TestLegacyPriceStateChinaSetBarPriceWithoutExchangeKeepsRawSymbol(t *testing.T) {
	resetLegacyPriceState(t, "china")

	SetBarPrice("AU2406", 104)
	if got := GetLastBarPrice("AU"); got != -1 {
		t.Fatalf("unconfigured china bar base alias = %v, want -1", got)
	}
	if got := GetLastBarPrice("AU2406"); got != 104 {
		t.Fatalf("unconfigured china raw bar price = %v, want 104", got)
	}
}

func TestConfiguredPriceStateUsesExchangeOwnedChinaMetadata(t *testing.T) {
	exchange := &priceMarketMapperStub{market: &banexg.Market{
		Symbol: "IF/CNY:CNY:2409", Base: "IF", Quote: "CNY", Settle: "CNY",
	}}
	prices := NewPriceStateWithExchange("china", exchange)
	prices.SetBarPriceAt(1000, "IF2409", 100)

	if got := prices.GetLastBarPriceAt("IF"); got != 100 {
		t.Fatalf("china base alias = %v, want 100", got)
	}
	base, quote, settle, ident := prices.parser.Split("IF2409")
	if base != "IF" || quote != "CNY" || settle != "CNY" || ident != "2409" {
		t.Fatalf("china parts = %q/%q/%q/%q, want IF/CNY/CNY/2409", base, quote, settle, ident)
	}
}

func TestRuntimeMarketStateChinaNilExchangeKeepsGenericParser(t *testing.T) {
	resetLegacyPriceState(t, "china")

	runtimeState := NewMarketStateWithExchange("china", nil)
	runtimeState.Prices.SetBarPriceAt(1000, "IF2409", 100)
	if got := runtimeState.Prices.GetLastBarPriceAt("IF"); got != -1 {
		t.Fatalf("runtime china base alias = %v, want -1", got)
	}
	if got := runtimeState.Prices.GetLastBarPriceAt("IF2409"); got != 100 {
		t.Fatalf("runtime china raw price = %v, want 100", got)
	}

	SetBarPrice("AU2406", 104)
	if got := GetLastBarPrice("AU"); got != -1 {
		t.Fatalf("unconfigured legacy china base alias = %v, want -1", got)
	}
	if got := runtimeState.Prices.GetLastBarPriceAt("AU2406"); got != -1 {
		t.Fatalf("legacy china price leaked into runtime = %v", got)
	}
}

func TestPriceStateExchangeAdapterUsesMappedChinaBaseAndCachesIt(t *testing.T) {
	exchange := &priceMarketMapperStub{market: &banexg.Market{
		Symbol: "IF/CNY:CNY:2409", Base: "IF", Quote: "CNY", Settle: "CNY",
	}}
	prices := NewPriceStateWithExchange("china", exchange)

	base, quote, settle, ident := prices.parser.Split("if2409")
	if base != "IF" || quote != "CNY" || settle != "CNY" || ident != "2409" {
		t.Fatalf("mapped parts = %q/%q/%q/%q, want IF/CNY/CNY/2409", base, quote, settle, ident)
	}
	prices.SetPriceAt(1000, "if2409", 102, 98)
	prices.SetBarPriceAt(1000, "if2409", 100)
	if got := prices.GetPriceSafeExpAt(1000, "IF", banexg.OdSideBuy, 0); got != 98 {
		t.Fatalf("mapped base buy alias = %v, want 98", got)
	}
	if got := prices.GetLastBarPriceAt("IF"); got != 100 {
		t.Fatalf("mapped base bar alias = %v, want 100", got)
	}
	if exchange.calls != 1 {
		t.Fatalf("MapMarket calls = %d, want one cache miss", exchange.calls)
	}
}

func TestLegacyPriceStateIncompleteExchangeUsesDefaultParser(t *testing.T) {
	resetLegacyPriceState(t, "incomplete")
	oldExchange := exg.Default
	exg.Default = &priceMarketMapperStub{}
	t.Cleanup(func() { exg.Default = oldExchange })

	SetPrice("BTC/USDT", 102, 98)
	if got := GetPriceSafeExp("BTC", banexg.OdSideBuy, Day10MSecs); got != 98 {
		t.Fatalf("default parser base alias = %v, want 98", got)
	}
}

func TestLegacyPriceParserFallsBackForUnknownMappedSymbol(t *testing.T) {
	exchange := &priceMarketMapperStub{}
	parser := core.NewSymbolParserWithStrategy("binance", newLegacyPriceSymbolParser("binance", exchange))

	base, quote, settle, ident := parser.Split("DET_A/USDT")
	if base != "DET_A" || quote != "USDT" || settle != "" || ident != "" {
		t.Fatalf("legacy fallback parts = %q/%q/%q/%q", base, quote, settle, ident)
	}
}

func TestPriceStateExplicitSymbolStrategy(t *testing.T) {
	prices := NewPriceStateWithStrategy("custom", func(pair string) [4]string {
		if pair == "X1" {
			return [4]string{"X", "USD", "USD", "1"}
		}
		base, quote, settle, ident := core.SplitSymbol(pair)
		return [4]string{base, quote, settle, ident}
	})

	prices.SetBarPriceAt(1000, "X1", 12)
	if got := prices.GetLastBarPriceAt("X"); got != 12 {
		t.Fatalf("strategy base alias = %v, want 12", got)
	}
}

func TestLegacyPriceStateExchangeSwitchRestoresPrices(t *testing.T) {
	resetLegacyPriceState(t, "binance")

	SetPrice("BTC/USDT", 12, 10)
	SetBarPrice("BTC/USDT", 11)
	if got := GetPriceSafeExp("BTC/USDT", banexg.OdSideBuy, Day10MSecs); got != 10 {
		t.Fatalf("binance buy price = %v, want 10", got)
	}
	if got := GetLastBarPrice("BTC/USDT"); got != 11 {
		t.Fatalf("binance bar price = %v, want 11", got)
	}

	core.ExgName = "china"
	if got := GetPriceSafeExp("BTC/USDT", "", Day10MSecs); got != -1 {
		t.Fatalf("binance order-book price leaked into china = %v", got)
	}
	if got := GetLastBarPrice("BTC/USDT"); got != -1 {
		t.Fatalf("binance bar price leaked into china = %v", got)
	}

	// Without an exchange adapter, the legacy facade keeps the raw symbol.
	SetPrice("AU2406", 102, 98)
	SetBarPrice("AU2406", 104)
	if got := GetPriceSafeExp("AU", banexg.OdSideBuy, Day10MSecs); got != -1 {
		t.Fatalf("unconfigured china AU/CNY buy alias = %v, want -1", got)
	}
	if got := GetLastBarPrice("AU2406"); got != 104 {
		t.Fatalf("unconfigured china raw bar price = %v, want 104", got)
	}

	core.ExgName = "binance"
	if got := GetPriceSafeExp("BTC/USDT", banexg.OdSideBuy, Day10MSecs); got != 10 {
		t.Fatalf("restored binance buy price = %v, want 10", got)
	}
	if got := GetPriceSafeExp("BTC/USDT", banexg.OdSideSell, Day10MSecs); got != 12 {
		t.Fatalf("restored binance sell price = %v, want 12", got)
	}
	if got := GetLastBarPrice("BTC/USDT"); got != 11 {
		t.Fatalf("restored binance bar price = %v, want 11", got)
	}
	if got := GetPriceSafeExp("AU", banexg.OdSideBuy, Day10MSecs); got != -1 {
		t.Fatalf("unconfigured china alias leaked into binance = %v", got)
	}

	core.ExgName = "china"
	if got := GetPriceSafeExp("AU2406", banexg.OdSideBuy, Day10MSecs); got != 98 {
		t.Fatalf("restored china raw buy price = %v, want 98", got)
	}
	if got := GetLastBarPrice("AU2406"); got != 104 {
		t.Fatalf("restored china raw bar price = %v, want 104", got)
	}
	if got := GetPriceSafeExp("BTC/USDT", "", Day10MSecs); got != -1 {
		t.Fatalf("binance price leaked back into china = %v", got)
	}
}

func TestPairCopiedStateConcurrentReadersKeepMaximumBarMs(t *testing.T) {
	state := NewPairCopiedState()
	const workers = 8
	const readers = 4
	const updates = 32

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers + readers)
	for reader := 0; reader < readers; reader++ {
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < workers*updates; i++ {
				_ = state.GetPairCopieds()
				_ = state.LastBarMs()
				_ = state.LastCopiedMs()
			}
		}()
	}
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wg.Done()
			<-start
			for update := 0; update < updates; update++ {
				barMS := int64(worker*updates + update + 1)
				state.SetPairMsAt(barMS, "PAIR", barMS, 60)
			}
		}(worker)
	}
	close(start)
	wg.Wait()

	want := int64(workers * updates)
	if got := state.LastBarMs(); got != want {
		t.Fatalf("last bar ms = %d, want maximum %d", got, want)
	}
	if got := state.LastCopiedMs(); got != want {
		t.Fatalf("last copied ms = %d, want maximum %d", got, want)
	}
	if got := state.GetPairCopieds()["PAIR"][0]; got < 1 || got > want {
		t.Fatalf("pair bar ms = %d, want a submitted value in [1, %d]", got, want)
	}
}

func TestPriceStateUsesExplicitTimestampAndKeepsSourcesSeparate(t *testing.T) {
	prices := NewPriceState("binance")
	prices.SetPriceAt(1000, "BTC/USDT:USDT", 101, 99)

	if got := prices.GetPriceSafeExpAt(1000, "BTC/USDT:USDT", banexg.OdSideBuy, 0); got != 99 {
		t.Fatalf("buy price = %v, want 99", got)
	}
	if got := prices.GetPriceSafeExpAt(1000, "BTC/USDT:USDT", banexg.OdSideSell, 0); got != 101 {
		t.Fatalf("sell price = %v, want 101", got)
	}
	if got := prices.GetPriceSafeExpAt(1000, "BTC/USDT:USDT", "", 0); got != 100 {
		t.Fatalf("mid price = %v, want 100", got)
	}
	if got := prices.GetLastBarPriceAt("BTC"); got != -1 {
		t.Fatalf("bar price leaked from order book = %v", got)
	}

	prices.SetBarPriceAt(2000, "ETH/USDT", 22)
	if got := prices.GetPriceSafeExpAt(2000, "ETH/USDT", "", 0); got != 22 {
		t.Fatalf("bar fallback price = %v, want 22", got)
	}
	if got := prices.GetPriceSafeExpAt(2001, "ETH/USDT", "", 0); got != -1 {
		t.Fatalf("expired bar price = %v, want -1", got)
	}
	if got := prices.GetLastBarPriceAt("USDT"); got != 1 {
		t.Fatalf("fiat price = %v, want 1", got)
	}
}

func TestPriceStateRejectsInvalidSide(t *testing.T) {
	defer func() {
		if got := recover(); got != "invalid side: invalid, use `banexg.OdSideBuy/OdSideSell` or ''" {
			t.Fatalf("panic = %v", got)
		}
	}()
	NewPriceState("binance").SetPricesAt(1000, map[string]float64{"BTC/USDT": 1}, "invalid")
}

func BenchmarkPriceStateRead(b *testing.B) {
	prices := NewPriceState("binance")
	prices.SetPriceAt(1000, "BTC/USDT:USDT", 101, 99)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = prices.GetPriceSafeExpAt(1000, "BTC/USDT:USDT", "", 0)
	}
}

func BenchmarkPriceStateWrite(b *testing.B) {
	prices := NewPriceState("binance")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prices.SetPriceAt(int64(i), "BTC/USDT:USDT", 101, 99)
	}
}

func BenchmarkLegacyPriceFacadeRead(b *testing.B) {
	resetLegacyPriceState(b, "binance")
	SetPrice("BTC/USDT:USDT", 101, 99)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = GetPriceSafeExp("BTC/USDT:USDT", "", Day10MSecs)
	}
}

func BenchmarkLegacyPriceFacadeSetBarPrice(b *testing.B) {
	resetLegacyPriceState(b, "binance")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		SetBarPrice("BTC/USDT:USDT", 100)
	}
}
