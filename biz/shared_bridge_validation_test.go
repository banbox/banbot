package biz

import (
	"github.com/banbox/banbot/execution"
	"github.com/shopspring/decimal"
	"testing"
)

func TestSharedBridgeRejectsAmbiguousInstrumentAndStrategyIdentities(t *testing.T) {
	i := execution.Instrument{ID: "opaque-btc", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.NewFromInt(1), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1), MoneyScale: 3}
	quoted := false
	cfg := &SharedOrderBridgeConfig{Version: "v1", Instruments: map[string]execution.Instrument{"BTC/USD": i}, Strategies: map[string]SharedStrategyBinding{"first": {ID: "ts", MaxNotional: decimal.NewFromInt(1000)}}, Risk: execution.PortfolioRisk{StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{"ts": decimal.NewFromInt(1000)}}, IntentTTLMS: 1000, Quote: func(string, int64) (execution.VisibleQuote, error) {
		quoted = true
		return execution.VisibleQuote{}, nil
	}}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.Instruments["BTC-ALIAS/USD"] = i
	if err := cfg.Validate(); err == nil {
		t.Fatal("two symbols mapped to one execution instrument")
	}
	delete(cfg.Instruments, "BTC-ALIAS/USD")
	cfg.Strategies["second"] = cfg.Strategies["first"]
	if err := cfg.Validate(); err == nil {
		t.Fatal("two TS names mapped to one contributor")
	}
	if quoted {
		t.Fatal("ambiguous declaration reached quote IO")
	}
}
