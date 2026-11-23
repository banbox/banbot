package exg

import (
	"testing"

	"github.com/banbox/banexg"
)

type clientOrderExchangeStub struct {
	banexg.BanExchange
	id string
}

func (s *clientOrderExchangeStub) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: s.id}
}

type clientOrderCapabilityStub struct {
	clientOrderExchangeStub
	value     string
	panicCall bool
}

func (s *clientOrderCapabilityStub) BuildClientOrderID(string, int64, string, bool) string {
	if s.panicCall {
		panic("stub client-order capability failure")
	}
	return s.value
}

func TestBuildClientOrderIDUsesAdapterCapability(t *testing.T) {
	exchange := &clientOrderCapabilityStub{
		clientOrderExchangeStub: clientOrderExchangeStub{id: "adapter"},
		value:                   "adapter-owned-id",
	}
	if got := BuildClientOrderID(exchange, "ignored", "bot", 42, "entry", true); got != "adapter-owned-id" {
		t.Fatalf("capability client ID = %q, want adapter-owned-id", got)
	}

	wrapped := &BotExchange{BanExchange: exchange}
	if got := BuildClientOrderID(wrapped, "ignored", "bot", 42, "entry", false); got != "adapter-owned-id" {
		t.Fatalf("wrapped capability client ID = %q, want adapter-owned-id", got)
	}
}

func TestBuildClientOrderIDCapabilityPanicUsesLegacyFallback(t *testing.T) {
	exchange := &clientOrderCapabilityStub{
		clientOrderExchangeStub: clientOrderExchangeStub{id: "adapter"},
		panicCall:               true,
	}
	if got := BuildClientOrderID(exchange, "ignored", "bot", 42, "entry", false); got != "bot_42_entry" {
		t.Fatalf("panic fallback client ID = %q, want bot_42_entry", got)
	}
}

func TestBuildClientOrderIDUsesGenericLegacyFallback(t *testing.T) {
	exchange := &clientOrderExchangeStub{id: "adapter"}
	for _, exchangeName := range []string{"first", "second"} {
		if got := BuildClientOrderID(exchange, exchangeName, "bot", 42, "entry", false); got != "bot_42_entry" {
			t.Fatalf("legacy client ID for %q = %q, want bot_42_entry", exchangeName, got)
		}
	}
}
