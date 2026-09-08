package config

import (
	"testing"

	"github.com/banbox/banbot/core"
)

func TestExchangeCapabilitiesComeFromAdapter(t *testing.T) {
	if !ExchangeUsesOpaqueSymbols("china") {
		t.Fatal("china adapter did not advertise opaque symbol capability")
	}
	if ExchangeUsesOpaqueSymbols("unknown-exchange") {
		t.Fatal("unknown exchange unexpectedly advertised opaque symbols")
	}
	location := ExchangeDefaultLocation("china")
	if location == nil || location.String() != "Asia/Shanghai" {
		t.Fatalf("china adapter location = %v, want Asia/Shanghai", location)
	}
}

func TestParsePairsUsesAdapterCapability(t *testing.T) {
	oldExchange, oldName, oldMarket, oldStake := Exchange, core.ExgName, core.Market, StakeCurrency
	t.Cleanup(func() {
		Exchange, core.ExgName, core.Market, StakeCurrency = oldExchange, oldName, oldMarket, oldStake
	})

	Exchange = &ExchangeConfig{Name: "china"}
	core.ExgName, core.Market = "china", "spot"
	if got, err := ParsePairs("IF2409"); err != nil || len(got) != 1 || got[0] != "IF2409" {
		t.Fatalf("adapter-owned raw pair = %v, %v", got, err)
	}

	Exchange = &ExchangeConfig{Name: "unknown-exchange"}
	core.ExgName, core.Market, StakeCurrency = "unknown-exchange", "spot", []string{"USDT"}
	if got, err := ParsePairs("BTC"); err != nil || len(got) != 1 || got[0] != "BTC/USDT" {
		t.Fatalf("generic short pair = %v, %v", got, err)
	}
}
