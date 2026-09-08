package exg

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
)

type orderEventExchangeStub struct {
	banexg.BanExchange
}

func (*orderEventExchangeStub) ParseClientOrderID(string, string) int64 { return 1 }
func (*orderEventExchangeStub) GetOrderEventOrderID(*banexg.MyTrade) string {
	return "1"
}
func (*orderEventExchangeStub) NormalizeOrderTimestamp(*banexg.Order, int64) int64 { return 1 }

func TestRequireOrderEventCapability(t *testing.T) {
	if capability, err := RequireOrderEventCapability(&orderEventExchangeStub{}, true); err != nil || capability == nil {
		t.Fatalf("supported adapter rejected: capability=%v err=%v", capability, err)
	}
	if _, err := RequireOrderEventCapability(&symbolMarketStub{}, true); err == nil || !strings.Contains(err.Error(), "capability") {
		t.Fatalf("missing native capability error = %v", err)
	}
	if capability, err := RequireOrderEventCapability(&symbolMarketStub{}, false); err != nil || capability == nil {
		t.Fatalf("legacy adapter fallback result: capability=%v err=%v", capability, err)
	}
	if _, err := RequireOrderEventCapability(nil, true); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("missing adapter error = %v", err)
	}
}

func TestOrderEventCapabilityFallbackIsSafe(t *testing.T) {
	capability := GetOrderEventCapability(&symbolMarketStub{})
	if capability == nil {
		t.Fatal("safe capability is nil")
	}
	if got := capability.ParseClientOrderID("bot", "bot_42_7_note"); got != 0 {
		t.Fatalf("generic capability guessed client ID = %d, want 0", got)
	}
	if got := capability.GetOrderEventOrderID(&banexg.MyTrade{AlgoId: "algo:99"}); got != "" {
		t.Fatalf("generic capability guessed algo ID = %q, want empty", got)
	}
	if got := capability.NormalizeOrderTimestamp(&banexg.Order{
		Timestamp: 10, LastTradeTimestamp: 30, LastUpdateTimestamp: 20,
	}, 40); got != 40 {
		t.Fatalf("order timestamp = %d, want fallback 40", got)
	}
	if got := capability.NormalizeOrderTimestamp(&banexg.Order{
		Timestamp: 10, LastTradeTimestamp: 50, LastUpdateTimestamp: 20,
	}, 40); got != 50 {
		t.Fatalf("order timestamp = %d, want latest 50", got)
	}
}

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

func TestBuildClientOrderIDCheckedRejectsCapabilityFailure(t *testing.T) {
	exchange := &clientOrderCapabilityStub{
		clientOrderExchangeStub: clientOrderExchangeStub{id: "adapter"},
		panicCall:               true,
	}
	if got, err := BuildClientOrderIDChecked(exchange, "ignored", "bot", 42, "entry", false); err == nil || got != "" {
		t.Fatalf("checked capability failure = %q/%v, want an error and no identifier", got, err)
	}

	exchange.panicCall = false
	exchange.value = ""
	if got, err := BuildClientOrderIDChecked(exchange, "ignored", "bot", 42, "entry", false); err == nil || got != "" {
		t.Fatalf("checked empty capability = %q/%v, want an error and no identifier", got, err)
	}
}

func TestBuildClientOrderIDCheckedRequiresAdapterCapability(t *testing.T) {
	exchange := &clientOrderExchangeStub{id: "adapter"}
	if got, err := BuildClientOrderIDChecked(exchange, "ignored", "bot", 42, "entry", false); err == nil || got != "" {
		t.Fatalf("missing capability = %q/%v, want an error and no identifier", got, err)
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

func TestCreateUsesBanexgDefaultOrderEventCapability(t *testing.T) {
	oldExchangeConfig := config.Exchange
	oldRunEnv := core.RunEnv
	oldEnvReal := core.EnvReal
	t.Cleanup(func() {
		config.Exchange = oldExchangeConfig
		core.RunEnv = oldRunEnv
		core.EnvReal = oldEnvReal
	})

	config.Exchange = &config.ExchangeConfig{Name: "china"}
	core.RunEnv = core.RunEnvProd
	core.EnvReal = true
	exchange, err := create("china", banexg.MarketLinear, banexg.MarketSwap)
	if err != nil {
		t.Fatalf("v0.2.63 adapter creation failed: %v", err)
	}
	if exchange == nil {
		t.Fatal("v0.2.63 adapter creation returned nil exchange")
	}
	t.Cleanup(func() { _ = exchange.Close() })
	if !HasNativeOrderEventCapability(exchange) {
		t.Fatal("banexg adapter did not expose its default order-event capability")
	}
	capability := GetOrderEventCapability(exchange)
	if capability == nil {
		t.Fatal("banexg adapter did not receive its order-event capability")
	}
	if got := capability.ParseClientOrderID("bot", "bot_42_7_"); got != 0 {
		t.Fatalf("default adapter guessed client ID = %d, want 0", got)
	}
}
