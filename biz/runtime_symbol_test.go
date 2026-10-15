package biz

import (
	"sync/atomic"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type runtimeSymbolExchangeStub struct {
	banexg.BanExchange
	calls atomic.Int32
}

func (s *runtimeSymbolExchangeStub) MapMarket(string, int) (*banexg.Market, *errs.Error) {
	s.calls.Add(1)
	return nil, errs.NewMsg(errs.CodeNoMarketForPair, "runtime canonical symbol must not be remapped")
}

func TestRuntimeOrderMgrUsesGenericCanonicalSymbolParser(t *testing.T) {
	exchange := &runtimeSymbolExchangeStub{}
	mgr := &OrderMgr{
		runtimeDeps: true,
		runtimeCore: &core.State{},
		exchange:    exchange,
	}

	parts, err := mgr.priceSymbolParts("SQD/USDT:USDT")
	if err != nil {
		t.Fatalf("runtime symbol error = %v", err)
	}
	if parts != [4]string{"SQD", "USDT", "USDT", ""} {
		t.Fatalf("runtime symbol parts = %q", parts)
	}
	if got := exchange.calls.Load(); got != 0 {
		t.Fatalf("runtime order manager called MapMarket %d times", got)
	}
}

func TestRuntimeWalletUsesExplicitSymbolState(t *testing.T) {
	symbols := orm.NewSymbolStateWithIdentity("runtime-exchange", "linear")
	expected := &orm.ExSymbol{
		ID:       77,
		Exchange: "runtime-exchange",
		Market:   "linear",
		Symbol:   "SQD/USDT:USDT",
	}
	if err := symbols.CacheExSymbolChecked(expected); err != nil {
		t.Fatal(err)
	}

	wallets := &BanWallets{}
	wallets.bindRuntimeSymbols(symbols, nil)
	got := wallets.symbolByID(expected.ID)
	if got == nil || got.ID != expected.ID || got.Symbol != expected.Symbol {
		t.Fatalf("runtime wallet symbol = %#v, want %#v", got, expected)
	}
}
