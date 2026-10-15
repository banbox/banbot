package backtest

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
)

func allocationPortfolio(t *testing.T, seq uint64, mode factor.PortfolioMode, nav float64, values map[int32]factor.Allocation) *factor.PortfolioTarget {
	t.Helper()
	spec := portfolio(t, seq, nil).Spec()
	spec.Mode = mode
	spec.Budget.NAV = nav
	p, err := factor.NewPortfolioTarget(spec, values)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func TestMixedAllocationQuantityDoesNotRebuyOnNAVAndPriceRise(t *testing.T) {
	b, _ := NewBook(1000)
	q := map[int32]Quote{1: {AtMS: 11, AvailableAt: 11, Price: 100}, 2: {AtMS: 11, AvailableAt: 11, Price: 100}}
	p := allocationPortfolio(t, 1, factor.Full, 1000, map[int32]factor.Allocation{1: {Basis: factor.AbsoluteQuantity, Value: "8"}, 2: {Basis: factor.NAVFraction, Value: "0.2"}})
	if err := b.ExecuteAllocation(p, q, 11, 0, 0); err != nil {
		t.Fatal(err)
	}
	q[1] = Quote{AtMS: 12, AvailableAt: 12, Price: 200}
	q[2] = q[1]
	p = allocationPortfolio(t, 2, factor.Full, 3000, map[int32]factor.Allocation{1: {Basis: factor.AbsoluteQuantity, Value: "7"}, 2: {Basis: factor.NAVFraction, Value: "0.2"}})
	if err := b.ExecuteAllocation(p, q, 12, 0, 0); err != nil {
		t.Fatal(err)
	}
	if state := b.State(); state.Quantities[1] != 7 || state.Quantities[2] != 3 {
		t.Fatal("quantity exit rebought or weight ignored new NAV", state)
	}
	e, _ := b.PolicyEvidence(12)
	if e.Positions[1].FirstFillTime != 11 || e.Positions[2].FirstFillTime != 11 {
		t.Fatal("increase reset age", e)
	}
	p = allocationPortfolio(t, 3, factor.Patch, 5000, map[int32]factor.Allocation{1: {Basis: factor.AbsoluteQuantity, Value: "6"}})
	q[1] = Quote{AtMS: 13, AvailableAt: 13, Price: 300}
	if err := b.ExecuteAllocation(p, q, 13, 0, 0); err != nil {
		t.Fatal(err)
	}
	if b.State().Quantities[2] != 3 {
		t.Fatal("patch resized omitted quantity")
	}
	p = allocationPortfolio(t, 4, factor.Full, 5000, nil)
	q[1] = Quote{AtMS: 14, AvailableAt: 14, Price: 300}
	q[2] = q[1]
	if err := b.ExecuteAllocation(p, q, 14, 0, 0); err != nil {
		t.Fatal(err)
	}
	e, _ = b.PolicyEvidence(14)
	if b.State().Quantities[1] != 0 || b.State().Quantities[2] != 0 || e.Positions[1].FirstFillTime != 0 {
		t.Fatal("full did not clear", e)
	}
}

func TestBookPolicyAcceptanceRejectsWithoutAdvancingAndRetriesExactly(t *testing.T) {
	b, _ := NewBook(1000)
	e, _ := b.PolicyEvidence(11)
	p := allocationPortfolio(t, 1, factor.Full, 1000, map[int32]factor.Allocation{1: {Basis: factor.AbsoluteQuantity, Value: "1"}})
	proposal := factor.PortfolioProposal{Target: p, NextState: json.RawMessage(`{"step":1}`)}
	if _, err := b.AcceptProposal(proposal, e.StateVersion, e.LedgerCursor, nil, 11, 0, 0); err == nil {
		t.Fatal("missing quotes accepted")
	}
	state, version, _ := b.RestorePolicy()
	if version != 0 || state != nil || len(b.State().Quantities) != 0 {
		t.Fatal("rejection advanced state")
	}
	q := map[int32]Quote{1: {AtMS: 11, AvailableAt: 11, Price: 100}}
	receipt, err := b.AcceptProposal(proposal, e.StateVersion, e.LedgerCursor, q, 11, 0, 0)
	if err != nil || !receipt.Accepted {
		t.Fatal(receipt, err)
	}
	turnover := b.State().Turnover
	if _, err = b.AcceptProposal(proposal, e.StateVersion, e.LedgerCursor, q, 11, 0, 0); err != nil || b.State().Turnover != turnover {
		t.Fatal("retry traded again", err)
	}
	proposal.NextState = json.RawMessage(`{"step":2}`)
	if _, err = b.AcceptProposal(proposal, e.StateVersion, e.LedgerCursor, q, 11, 0, 0); err == nil {
		t.Fatal("same target changed state succeeded")
	}
	e, _ = b.PolicyEvidence(11)
	stateOnly := factor.PortfolioProposal{AcceptanceID: "state-only", NextState: json.RawMessage(`{"step":2}`)}
	if _, err = b.AcceptProposal(stateOnly, e.StateVersion, e.LedgerCursor, nil, 11, 0, 0); err != nil || b.State().Turnover != turnover {
		t.Fatal("state-only traded", err)
	}
	stateOnly.AcceptanceID = "stale"
	if _, err = b.AcceptProposal(stateOnly, e.StateVersion, e.LedgerCursor, nil, 11, 0, 0); !errors.Is(err, execution.ErrPolicyEvidenceChanged) {
		t.Fatal("stale state accepted", err)
	}
}
