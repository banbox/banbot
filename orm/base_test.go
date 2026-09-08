package orm

import (
	"context"
	"testing"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type beginDelegatingDB struct{}

func (beginDelegatingDB) Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (beginDelegatingDB) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	return nil, nil
}

func (beginDelegatingDB) QueryRow(context.Context, string, ...interface{}) pgx.Row {
	return nil
}

func (beginDelegatingDB) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

func (beginDelegatingDB) Begin(context.Context) (pgx.Tx, error) {
	return nil, context.Canceled
}

func TestSubQueriesBeginDelegatesToPoolConnection(t *testing.T) {
	_, err := (&SubQueries{db: beginDelegatingDB{}}).Begin(context.Background())
	if err != context.Canceled {
		t.Fatalf("Begin error = %v, want delegated error %v", err, context.Canceled)
	}
}

func TestNormalizeDatabaseURLLegacyBracketedIPv4(t *testing.T) {
	got := normalizeDatabaseURL("postgresql://user:pass@[127.0.0.1]:5432/ban")
	if got != "postgresql://user:pass@127.0.0.1:5432/ban" {
		t.Fatalf("normalized URL = %q", got)
	}
}

type marketLoadArgsExchange struct {
	banexg.BanExchange
	info            *banexg.ExgInfo
	contract        bool
	markets         banexg.MarketMap
	loadCalls       int
	loadParams      map[string]interface{}
	symbolLoadCalls int
	symbols         []string
}

func (e *marketLoadArgsExchange) Info() *banexg.ExgInfo { return e.info }

func (e *marketLoadArgsExchange) IsContract(string) bool { return e.contract }

func (e *marketLoadArgsExchange) LoadMarkets(_ bool, params map[string]interface{}) (banexg.MarketMap, *errs.Error) {
	e.loadCalls++
	e.loadParams = params
	return e.markets, nil
}

func (e *marketLoadArgsExchange) LoadMarketsForSymbols(_ bool, symbols []string) (banexg.MarketMap, *errs.Error) {
	e.symbolLoadCalls++
	e.symbols = append([]string(nil), symbols...)
	return e.markets, nil
}

func TestLoadMarketsUsesGenericContractClassificationForSymbolParams(t *testing.T) {
	previous := swapDefaultSymbolState(NewSymbolState())
	t.Cleanup(func() { swapDefaultSymbolState(previous) })
	cacheExSymbol(&ExSymbol{
		ID: 1, Exchange: "adapter", Market: banexg.MarketLinear, Symbol: "ABC/USDT:USDT",
	})
	exchange := &marketLoadArgsExchange{
		info:     &banexg.ExgInfo{ID: "adapter", MarketType: banexg.MarketLinear},
		contract: true,
	}
	if _, err := LoadMarkets(exchange, false); err != nil {
		t.Fatal(err)
	}
	if exchange.loadCalls != 1 || exchange.symbolLoadCalls != 1 {
		t.Fatalf("market load calls = %d/%d, want standard load plus capability reload", exchange.loadCalls, exchange.symbolLoadCalls)
	}
	if len(exchange.symbols) != 1 || exchange.symbols[0] != "ABC/USDT:USDT" {
		t.Fatalf("market load symbols = %#v, want cached contract symbol", exchange.symbols)
	}
}

type marketLoadNoSymbolCapabilityExchange struct {
	banexg.BanExchange
	info     *banexg.ExgInfo
	contract bool
	markets  banexg.MarketMap
	loads    int
}

func (e *marketLoadNoSymbolCapabilityExchange) Info() *banexg.ExgInfo  { return e.info }
func (e *marketLoadNoSymbolCapabilityExchange) IsContract(string) bool { return e.contract }
func (e *marketLoadNoSymbolCapabilityExchange) LoadMarkets(bool, map[string]interface{}) (banexg.MarketMap, *errs.Error) {
	e.loads++
	return e.markets, nil
}

func TestLoadMarketsDoesNotGuessSymbolScopedCapability(t *testing.T) {
	previous := swapDefaultSymbolState(NewSymbolState())
	t.Cleanup(func() { swapDefaultSymbolState(previous) })
	cacheExSymbol(&ExSymbol{
		ID: 1, Exchange: "adapter", Market: banexg.MarketLinear, Symbol: "ABC/USDT:USDT",
	})
	exchange := &marketLoadNoSymbolCapabilityExchange{
		info:     &banexg.ExgInfo{ID: "adapter", MarketType: banexg.MarketLinear},
		contract: true,
	}
	if _, err := LoadMarkets(exchange, false); err != nil {
		t.Fatal(err)
	}
	if exchange.loads != 1 {
		t.Fatalf("market load calls = %d, want one unscoped load", exchange.loads)
	}
}

func TestLoadMarketsDoesNotPassSymbolsToPopulatedContractLoader(t *testing.T) {
	previous := swapDefaultSymbolState(NewSymbolState())
	t.Cleanup(func() { swapDefaultSymbolState(previous) })
	cacheExSymbol(&ExSymbol{
		ID: 1, Exchange: "adapter", Market: banexg.MarketLinear, Symbol: "ABC/USDT:USDT",
	})
	exchange := &marketLoadArgsExchange{
		info:     &banexg.ExgInfo{ID: "adapter", MarketType: banexg.MarketLinear},
		contract: true,
		markets:  banexg.MarketMap{"ABC/USDT:USDT": {Symbol: "ABC/USDT:USDT"}},
	}
	if _, err := LoadMarkets(exchange, false); err != nil {
		t.Fatal(err)
	}
	if exchange.loadCalls != 1 {
		t.Fatalf("market load calls = %d, want one standard load", exchange.loadCalls)
	}
	if _, ok := exchange.loadParams[banexg.ParamSymbols]; ok {
		t.Fatalf("populated contract loader unexpectedly received symbol params: %#v", exchange.loadParams)
	}
}

func TestLoadMarketsDoesNotInjectSymbolParamsForNonContractMarkets(t *testing.T) {
	previous := swapDefaultSymbolState(NewSymbolState())
	t.Cleanup(func() { swapDefaultSymbolState(previous) })
	cacheExSymbol(&ExSymbol{
		ID: 1, Exchange: "adapter", Market: banexg.MarketSpot, Symbol: "ABC/USDT",
	})
	exchange := &marketLoadArgsExchange{
		info:     &banexg.ExgInfo{ID: "adapter", MarketType: banexg.MarketSpot},
		contract: false,
	}
	if _, err := LoadMarkets(exchange, false); err != nil {
		t.Fatal(err)
	}
	if _, ok := exchange.loadParams[banexg.ParamSymbols]; ok {
		t.Fatalf("spot market load unexpectedly received symbol params: %#v", exchange.loadParams)
	}
}
