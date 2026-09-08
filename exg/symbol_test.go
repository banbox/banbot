package exg

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/china"
	"github.com/banbox/banexg/errs"
)

type symbolMarketStub struct {
	banexg.BanExchange
	market     *banexg.Market
	mapErr     *errs.Error
	panicOnMap bool
	calls      atomic.Int32
}

type symbolCapabilityStub struct {
	symbolMarketStub
	parts        [4]string
	parseErr     *errs.Error
	panicOnParse bool
}

func (s *symbolCapabilityStub) PriceSymbolParts(string) ([4]string, *errs.Error) {
	if s.panicOnParse {
		panic("stub symbol capability failure")
	}
	if s.parseErr != nil {
		return [4]string{}, s.parseErr
	}
	return s.parts, nil
}

func (s *symbolMarketStub) MapMarket(string, int) (*banexg.Market, *errs.Error) {
	s.calls.Add(1)
	if s.panicOnMap {
		panic("stub MapMarket failure")
	}
	return s.market, s.mapErr
}

func TestNewPriceSymbolParserDoesNotInferFromExchangeName(t *testing.T) {
	parser := core.NewSymbolParserWithStrategy("china", NewPriceSymbolParser("china", nil))
	base, quote, settle, ident := parser.Split("IF2409")
	if base != "IF2409" {
		t.Fatalf("default base = %q, want IF2409", base)
	}
	if quote != "" || settle != "" || ident != "" {
		t.Fatalf("default parts = %q/%q/%q/%q, want empty quote/settle/identifier", base, quote, settle, ident)
	}
}

func TestNewPriceSymbolParserUsesMappedMarket(t *testing.T) {
	exchange := &symbolMarketStub{market: &banexg.Market{
		Symbol: "BASE/USD:SETTLE:ID",
		Base:   "BASE",
		Quote:  "USD",
		Settle: "SETTLE",
	}}
	parser := core.NewSymbolParserWithStrategy("china", NewPriceSymbolParser("china", exchange))

	base, quote, settle, ident := parser.Split("raw-id")
	if base != "BASE" || quote != "USD" || settle != "SETTLE" || ident != "ID" {
		t.Fatalf("mapped parts = %q/%q/%q/%q", base, quote, settle, ident)
	}
	parser.Split("raw-id")
	if got := exchange.calls.Load(); got != 1 {
		t.Fatalf("MapMarket calls = %d, want 1", got)
	}
}

func TestNewPriceSymbolParserUsesAdapterCapability(t *testing.T) {
	exchange := &symbolCapabilityStub{parts: [4]string{"IF", "CNY", "CNY", "2409"}}
	parser := core.NewSymbolParserWithStrategy("china", NewPriceSymbolParser("china", exchange))

	base, quote, settle, ident := parser.Split("IF2409")
	if base != "IF" || quote != "CNY" || settle != "CNY" || ident != "2409" {
		t.Fatalf("capability parts = %q/%q/%q/%q", base, quote, settle, ident)
	}
	if got := exchange.calls.Load(); got != 0 {
		t.Fatalf("MapMarket calls = %d, want capability-only parsing", got)
	}
}

func TestResolveRuntimePriceSymbolUsesGenericCanonicalFallback(t *testing.T) {
	exchange := &symbolMarketStub{mapErr: errs.NewMsg(errs.CodeNoMarketForPair, "legacy lookup must not run")}
	parts, err := ResolveRuntimePriceSymbol(exchange, "SQD/USDT:USDT")
	if err != nil {
		t.Fatalf("runtime symbol error = %v", err)
	}
	if parts != [4]string{"SQD", "USDT", "USDT", ""} {
		t.Fatalf("runtime symbol parts = %q", parts)
	}
	if got := exchange.calls.Load(); got != 0 {
		t.Fatalf("runtime fallback called MapMarket %d times", got)
	}
}

func TestResolveRuntimePriceSymbolUsesAdapterCapability(t *testing.T) {
	exchange := &symbolCapabilityStub{parts: [4]string{"IF", "CNY", "CNY", "2409"}}
	parts, err := ResolveRuntimePriceSymbol(exchange, "IF2409")
	if err != nil {
		t.Fatalf("runtime capability error = %v", err)
	}
	if parts != exchange.parts {
		t.Fatalf("runtime capability parts = %q, want %q", parts, exchange.parts)
	}
}

func TestResolveRuntimePriceSymbolUsesChinaAdapterForDelimiterFreeContract(t *testing.T) {
	exchange, err := china.New(nil)
	if err != nil {
		t.Fatalf("create China adapter: %v", err)
	}
	parts, parseErr := ResolveRuntimePriceSymbol(exchange, "IF2409")
	if parseErr != nil {
		t.Fatalf("resolve China symbol: %v", parseErr)
	}
	if parts != [4]string{"IF", "CNY", "CNY", "2409"} {
		t.Fatalf("China runtime symbol parts = %q", parts)
	}
}

func TestGetPriceSymbolCapabilityUnwrapsBotExchange(t *testing.T) {
	underlying := &symbolCapabilityStub{parts: [4]string{"IF", "CNY", "CNY", "2409"}}
	exchange := &BotExchange{BanExchange: underlying}
	capability := GetPriceSymbolCapability(exchange)
	if capability == nil {
		t.Fatal("wrapped exchange capability is nil")
	}

	parser := core.NewSymbolParserWithStrategy("china", NewPriceSymbolParser("china", exchange))
	if base, quote, settle, ident := parser.Split("IF2409"); base != "IF" || quote != "CNY" || settle != "CNY" || ident != "2409" {
		t.Fatalf("wrapped capability parts = %q/%q/%q/%q", base, quote, settle, ident)
	}
	if got := underlying.calls.Load(); got != 0 {
		t.Fatalf("MapMarket calls = %d, want capability-only parsing", got)
	}
}

func TestNewPriceSymbolParserWithErrorReturnsMapError(t *testing.T) {
	exchange := &symbolMarketStub{mapErr: errs.NewMsg(errs.CodeParamInvalid, "missing market")}
	parser := NewPriceSymbolParserWithError("china", exchange)

	parts, err := parser("BTC/USDT:USDT")
	if err == nil || err.Code != errs.CodeParamInvalid {
		t.Fatalf("MapMarket error = %v, want CodeParamInvalid", err)
	}
	if parts != [4]string{} {
		t.Fatalf("error parts = %q, want zero tuple", parts)
	}
}

func TestNewPriceSymbolParserWithErrorReturnsMapPanic(t *testing.T) {
	exchange := &symbolMarketStub{panicOnMap: true}
	parser := NewPriceSymbolParserWithError("china", exchange)

	parts, err := parser("")
	if err == nil || err.Code != errs.CodeRunTime {
		t.Fatalf("MapMarket panic error = %v, want CodeRunTime", err)
	}
	if parts != [4]string{} {
		t.Fatalf("panic parts = %q, want zero tuple", parts)
	}
}

func TestNewPriceSymbolParserWithErrorReturnsCapabilityErrorAndPanic(t *testing.T) {
	capErr := errs.NewMsg(errs.CodeParamInvalid, "bad capability")
	for _, exchange := range []*symbolCapabilityStub{
		{symbolMarketStub: symbolMarketStub{}, parseErr: capErr},
		{symbolMarketStub: symbolMarketStub{}, panicOnParse: true},
	} {
		parts, err := NewPriceSymbolParserWithError("china", exchange)("IF2409")
		if err == nil {
			t.Fatalf("capability failure returned nil error: parts=%q", parts)
		}
		if parts != [4]string{} {
			t.Fatalf("capability failure parts = %q, want zero tuple", parts)
		}
	}
}

func TestNewPriceSymbolParserUsesOnlyMappedMarketMetadata(t *testing.T) {
	exchange := &symbolMarketStub{market: &banexg.Market{Symbol: "IF2409", Base: "IF"}}
	parts, err := NewPriceSymbolParserWithError("china", exchange)("raw-id")
	if err != nil {
		t.Fatalf("mapped market error = %v", err)
	}
	if parts != [4]string{"IF", "", "", ""} {
		t.Fatalf("mapped metadata parts = %q, want IF/empty/empty/empty", parts)
	}
}

func TestNewPriceSymbolParserDoesNotInferChinaContractParts(t *testing.T) {
	exchange := &symbolMarketStub{market: &banexg.Market{
		Symbol:   "IF2409",
		Base:     "IF",
		Contract: true,
	}}
	parts, err := NewPriceSymbolParserWithError("china", exchange)("IF2409")
	if err != nil {
		t.Fatalf("China contract mapping error = %v", err)
	}
	if parts != [4]string{"IF", "", "", ""} {
		t.Fatalf("China contract parts = %q, want IF with empty suffixes", parts)
	}
}

func TestNewPriceSymbolParserDoesNotFillChinaPartsForOtherExchange(t *testing.T) {
	exchange := &symbolMarketStub{market: &banexg.Market{
		Symbol:   "IF2409",
		Base:     "IF",
		Contract: true,
	}}
	parts, err := NewPriceSymbolParserWithError("other", exchange)("IF2409")
	if err != nil {
		t.Fatalf("other contract mapping error = %v", err)
	}
	if parts != [4]string{"IF", "", "", ""} {
		t.Fatalf("other contract parts = %q, want IF with empty suffixes", parts)
	}
}

func TestValidatePriceSymbolParserReturnsAdapterError(t *testing.T) {
	exchange := &symbolMarketStub{mapErr: errs.NewMsg(errs.CodeNoMarketForPair, "missing")}
	parser := NewPriceSymbolParserWithError("binance", exchange)
	if err := ValidatePriceSymbolParser(parser, "BTC/USDT"); err == nil || err.Code != errs.CodeNoMarketForPair {
		t.Fatalf("validation error = %v, want CodeNoMarketForPair", err)
	}
}

func TestNewPriceSymbolParserAdaptersAreIsolatedConcurrently(t *testing.T) {
	first := &symbolMarketStub{market: &banexg.Market{
		Symbol: "FIRST/USD:USD",
		Base:   "FIRST",
		Quote:  "USD",
		Settle: "USD",
	}}
	second := &symbolMarketStub{market: &banexg.Market{
		Symbol: "SECOND/CNY:CNY",
		Base:   "SECOND",
		Quote:  "CNY",
		Settle: "CNY",
	}}
	firstParser := core.NewSymbolParserWithStrategy("first", NewPriceSymbolParser("first", first))
	secondParser := core.NewSymbolParserWithStrategy("second", NewPriceSymbolParser("second", second))

	const workers = 24
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers * 2)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			<-start
			if base, quote, settle, ident := firstParser.Split("same-raw-id"); base != "FIRST" || quote != "USD" || settle != "USD" || ident != "" {
				t.Errorf("first adapter parts = %q/%q/%q/%q", base, quote, settle, ident)
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			if base, quote, settle, ident := secondParser.Split("same-raw-id"); base != "SECOND" || quote != "CNY" || settle != "CNY" || ident != "" {
				t.Errorf("second adapter parts = %q/%q/%q/%q", base, quote, settle, ident)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := first.calls.Load(); got != 1 {
		t.Fatalf("first MapMarket calls = %d, want 1", got)
	}
	if got := second.calls.Load(); got != 1 {
		t.Fatalf("second MapMarket calls = %d, want 1", got)
	}
}
