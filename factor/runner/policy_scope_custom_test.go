package runner

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/banbox/banbot/factor"
)

type scopeCustomPolicy struct{}

func (scopeCustomPolicy) Propose(factor.PortfolioContext, json.RawMessage) (factor.PortfolioProposal, error) {
	return factor.PortfolioProposal{NextState: json.RawMessage(`[1,2]`)}, nil
}

func TestPolicyScopeRetainsOpaqueCustomCheckpoint(t *testing.T) {
	const name = "scope-custom-v1"
	if err := RegisterPortfolioPolicy(name, func(factor.PortfolioPolicyConfig) (factor.PortfolioPolicy, error) { return scopeCustomPolicy{}, nil }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		portfolioPolicies.Lock()
		delete(portfolioPolicies.factories, name)
		portfolioPolicies.Unlock()
	})
	for _, raw := range []string{`[1,2]`, `42`, `"opaque"`} {
		t.Run(raw, func(t *testing.T) {
			c := paperConfig(t, archiveConfig(t, false))
			c.Chunks = nil
			c.Manifest.Portfolio.Policy = name
			c.Manifest.Costs.FundingPolicy = "explicit-zero"
			sink, done, err := NewPaperSink(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			defer done()
			e, err := sink.PolicyEvidence(context.Background(), 11)
			if err != nil {
				t.Fatal(err)
			}
			proposal := factor.PortfolioProposal{AcceptanceID: "custom-state", NextState: json.RawMessage(raw), PlanSequence: 1, DecisionTime: 11, ExpireAt: 100}
			if receipt, err := sink.AcceptProposal(context.Background(), proposal, e.StateVersion, e.LedgerCursor, nil, 11); err != nil || !receipt.Accepted {
				t.Fatal(receipt, err)
			}
			engine, err := NewLive(c, sink, func() int64 { return 11 }, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer engine.Stop()
			snapshot, err := sink.Account.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := engine.RetainPolicyScope(nil, snapshot); err != nil {
				t.Fatal("opaque custom state decoded as lifecycle", err)
			}
			state, _, _, err := sink.RestorePolicy(context.Background(), "")
			if err != nil || string(state) != raw {
				t.Fatal("scope retention rewrote custom state", string(state), err)
			}
		})
	}
}
