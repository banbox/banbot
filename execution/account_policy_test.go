package execution

import (
	"context"
	"encoding/json"
	"github.com/shopspring/decimal"
	"testing"
)

func TestAccountPolicyCannotBeOverwrittenByStrategyCheckpoint(t *testing.T) {
	s, _, _, _ := testStore(t)
	p := AccountPolicy{Version: "v1", Currency: s.Account().SettlementDomain, MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(1000), MaxVirtualGross: decimal.NewFromInt(1000), StrategyGrossLimits: map[StrategyID]decimal.Decimal{"a": decimal.NewFromInt(1000)}}
	if err := s.RegisterAccountPolicy(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(p)
	if err := s.SaveStrategyCheckpoint(context.Background(), "account-risk", "policy-v1", body); err == nil {
		t.Fatal("generic checkpoint bypassed immutable policy registration")
	}
	p.StrategyGrossLimits["a"] = decimal.NewFromInt(2000)
	if err := s.RegisterAccountPolicy(context.Background(), p); err == nil {
		t.Fatal("registered cap changed")
	}
	old, err := s.AccountPolicy(context.Background())
	if err != nil || !old.StrategyGrossLimits["a"].Equal(decimal.NewFromInt(1000)) {
		t.Fatal("failed mutation changed durable policy", old, err)
	}
}
