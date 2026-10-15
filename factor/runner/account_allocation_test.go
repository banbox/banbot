package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/shopspring/decimal"
)

func TestAccountAllocationBaseUnitsRestoreAndStateOnly(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	c.Manifest.Costs.FeeRate = 0
	c.Manifest.Costs.SlippageRate = 0
	i := c.Execution.Instruments[1]
	i.ContractSize = decimal.NewFromInt(10)
	i.QuantityStep = decimal.RequireFromString("0.1")
	c.Execution.Instruments[1] = i
	sink, done, err := NewPaperSink(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	ctx := context.Background()
	makeTarget := func(seq uint64, mode factor.PortfolioMode, nav float64, values map[int32]factor.Allocation) *factor.PortfolioTarget {
		sp := patchPortfolio(t, seq, mode, nav, nil).Spec()
		sp.AccountID = c.AccountID
		sp.StrategyID = c.StrategyID
		p, err := factor.NewPortfolioTarget(sp, values)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	q := backtest.Quote{AtMS: 11, AvailableAt: 11, Price: 100}
	if err := sink.ObserveQuote(ctx, 1, q, 11); err != nil {
		t.Fatal(err)
	}
	e, err := sink.PolicyEvidence(ctx, 11)
	if err != nil {
		t.Fatal(err)
	}
	proposal := factor.PortfolioProposal{Target: makeTarget(1, factor.Full, 10000, map[int32]factor.Allocation{1: {Basis: factor.AbsoluteQuantity, Value: "8.9"}}), NextState: json.RawMessage(`{"step":0}`), PlanSequence: 1}
	receipt, err := sink.AcceptProposal(ctx, proposal, e.StateVersion, e.LedgerCursor, map[int32]backtest.Quote{1: q}, 11)
	if err != nil || !receipt.Accepted || receipt.SendError != nil {
		t.Fatal(receipt, err)
	}
	state, err := sink.StrategyState(ctx, 11)
	if err != nil || state.Quantities[1] != 8 {
		t.Fatal("base units not converted to contract steps", state, err)
	}
	fills := sink.Paper.Metrics().Fills
	if _, err := sink.AcceptProposal(ctx, proposal, e.StateVersion, e.LedgerCursor, map[int32]backtest.Quote{1: q}, 11); err != nil || sink.Paper.Metrics().Fills != fills {
		t.Fatal("retry rebought", err)
	}
	proposal.NextState = json.RawMessage(`{"step":1}`)
	if _, err := sink.AcceptProposal(ctx, proposal, e.StateVersion, e.LedgerCursor, map[int32]backtest.Quote{1: q}, 11); err == nil {
		t.Fatal("changed checkpoint silently accepted")
	}
	q = backtest.Quote{AtMS: 12, AvailableAt: 12, Price: 200}
	if err := sink.ObserveQuote(ctx, 1, q, 12); err != nil {
		t.Fatal(err)
	}
	e, err = sink.PolicyEvidence(ctx, 12)
	if err != nil || e.Positions[1].FirstFillTime != 11 || e.Positions[1].Quantity != "8" || e.Positions[1].Quantum != "1" {
		t.Fatal(e, err)
	}
	if fills := e.Positions[1].FillEvents; len(fills) != 1 || fills[0].Quantity != "8" || fills[0].PlanSequence != 1 {
		t.Fatal("entry fill provenance missing", fills)
	}
	proposal = factor.PortfolioProposal{Target: makeTarget(2, factor.Full, 30000, map[int32]factor.Allocation{1: {Basis: factor.AbsoluteQuantity, Value: "7"}}), NextState: json.RawMessage(`{"step":1}`), PlanSequence: 2}
	receipt, err = sink.AcceptProposal(ctx, proposal, e.StateVersion, e.LedgerCursor, map[int32]backtest.Quote{1: q}, 12)
	if err != nil || receipt.SendError != nil {
		t.Fatal(receipt, err)
	}
	state, err = sink.StrategyState(ctx, 12)
	if err != nil || state.Quantities[1] != 7 {
		t.Fatal("quantity exit rebought on higher NAV", state, err)
	}
	// A fresh sink borrows the same authoritative owner and restores accepted
	// target scope, sequence and first-fill facts without another fill.
	restored := &AccountSink{Account: sink.Account, AccountID: sink.AccountID, StrategyID: sink.StrategyID, Currency: sink.Currency, Instruments: sink.Instruments, Paper: sink.Paper}
	raw, version, seq, err := restored.RestorePolicy(ctx, execution.PolicyCheckpointName)
	if err != nil || string(raw) != `{"step":1}` || version != 2 || seq != 2 {
		t.Fatal("restore", string(raw), version, seq, err)
	}
	e, err = restored.PolicyEvidence(ctx, 12)
	if err != nil || e.Previous == nil {
		t.Fatal(e, err)
	}
	if fills := e.Positions[1].FillEvents; len(fills) != 1 || fills[0].Quantity != "-1" || fills[0].PlanSequence != 2 {
		t.Fatal("restored incremental fill provenance missing", fills)
	}
	noTrade := factor.PortfolioProposal{AcceptanceID: "state-only", NextState: json.RawMessage(`{"step":2}`), PlanSequence: 3, DecisionTime: 12, ExpireAt: 100}
	receipt, err = restored.AcceptProposal(ctx, noTrade, e.StateVersion, e.LedgerCursor, nil, 12)
	if err != nil || !receipt.Accepted || sink.Paper.Metrics().Fills != fills+1 {
		t.Fatal("state-only created a fill", receipt, err)
	}
	_, version, seq, err = restored.RestorePolicy(ctx, "")
	if err != nil || version != 3 || seq != 3 {
		t.Fatal("state-only recovery sequence", version, seq, err)
	}
}

func TestAccountPolicyCacheBoundAndHistoricalRetry(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	sink, done, err := NewPaperSink(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	ctx := context.Background()
	e, err := sink.PolicyEvidence(ctx, 11)
	if err != nil {
		t.Fatal(err)
	}
	first := factor.PortfolioProposal{AcceptanceID: "bounded-0", NextState: json.RawMessage(`{"step":0}`), PlanSequence: 1, DecisionTime: 10, ExpireAt: 20}
	for n := 0; n < maxAccountAcceptedPolicyCache+6; n++ {
		proposal := first
		proposal.AcceptanceID = fmt.Sprintf("bounded-%d", n)
		proposal.NextState = json.RawMessage(fmt.Sprintf(`{"step":%d}`, n))
		proposal.PlanSequence = uint64(n + 1)
		receipt, err := sink.AcceptProposal(ctx, proposal, uint64(n), e.LedgerCursor, nil, 11)
		if err != nil || !receipt.Accepted || receipt.Versions[execution.StrategyID(c.StrategyID)] != uint64(n+1) {
			t.Fatal(n, receipt, err)
		}
	}
	if len(sink.acceptedPolicy) != maxAccountAcceptedPolicyCache || len(sink.acceptedPolicyOrder) != maxAccountAcceptedPolicyCache {
		t.Fatal("unbounded recent acceptance cache", len(sink.acceptedPolicy), len(sink.acceptedPolicyOrder))
	}
	if _, cached := sink.acceptedPolicy[first.AcceptanceID]; cached {
		t.Fatal("oldest acceptance was not evicted")
	}
	before, err := sink.Account.PolicyEvidenceContext(ctx, execution.StrategyID(c.StrategyID))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := sink.AcceptProposal(ctx, first, 0, e.LedgerCursor, nil, 1000)
	if err != nil || !receipt.Accepted || receipt.Versions[execution.StrategyID(c.StrategyID)] != 1 {
		t.Fatal("evicted expired retry lost original receipt", receipt, err)
	}
	for _, mutate := range []func(*factor.PortfolioProposal, *uint64, *uint64){
		func(p *factor.PortfolioProposal, _, _ *uint64) { p.NextState = json.RawMessage(`{"step":999}`) },
		func(_ *factor.PortfolioProposal, version, _ *uint64) { *version = 1 },
		func(_ *factor.PortfolioProposal, _, cursor *uint64) { *cursor++ },
	} {
		proposal, version, cursor := first, uint64(0), e.LedgerCursor
		mutate(&proposal, &version, &cursor)
		if _, err := sink.AcceptProposal(ctx, proposal, version, cursor, nil, 1000); err == nil {
			t.Fatal("evicted identity accepted changed proposal evidence")
		}
	}
	after, err := sink.Account.PolicyEvidenceContext(ctx, execution.StrategyID(c.StrategyID))
	if err != nil || after.State.Version != before.State.Version || string(after.State.Payload) != string(before.State.Payload) || after.Snapshot.Checkpoint != before.Snapshot.Checkpoint {
		t.Fatal("historical retry changed current state", after, err)
	}
}

func TestAccountPolicyFreshWrapperResumesFrozenPlan(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	c.Manifest.Costs.FeeRate = 0
	c.Manifest.Costs.SlippageRate = 0
	sink, done, err := NewPaperSink(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	ctx := context.Background()
	q := backtest.Quote{AtMS: 11, AvailableAt: 11, Price: 100}
	if err := sink.ObserveQuote(ctx, 1, q, 11); err != nil {
		t.Fatal(err)
	}
	e, err := sink.PolicyEvidence(ctx, 11)
	if err != nil {
		t.Fatal(err)
	}
	sp := patchPortfolio(t, 1, factor.Full, 1000, nil).Spec()
	sp.AccountID, sp.StrategyID = c.AccountID, c.StrategyID
	target, err := factor.NewPortfolioTarget(sp, map[int32]factor.Allocation{1: {Basis: factor.AbsoluteQuantity, Value: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	proposal := factor.PortfolioProposal{Target: target, NextState: json.RawMessage(`{"step":1}`), PlanSequence: 1}
	receipt, err := sink.AcceptProposal(ctx, proposal, e.StateVersion, e.LedgerCursor, map[int32]backtest.Quote{1: q}, 11)
	if err != nil || !receipt.Accepted || receipt.SendError != nil {
		t.Fatal(receipt, err)
	}
	current, err := sink.PolicyEvidence(ctx, 11)
	if err != nil {
		t.Fatal(err)
	}
	advance := factor.PortfolioProposal{AcceptanceID: "newer-state", NextState: json.RawMessage(`{"step":2}`), PlanSequence: 2}
	if _, err := sink.AcceptProposal(ctx, advance, current.StateVersion, current.LedgerCursor, nil, 11); err != nil {
		t.Fatal(err)
	}
	before, err := sink.Account.PolicyEvidenceContext(ctx, execution.StrategyID(c.StrategyID))
	if err != nil {
		t.Fatal(err)
	}
	fills := sink.Paper.Metrics().Fills
	// A cold wrapper has neither old execution metadata, quotes nor a cache.
	fresh := &AccountSink{Account: sink.Account, AccountID: sink.AccountID, StrategyID: sink.StrategyID, Currency: sink.Currency}
	receipt, err = fresh.AcceptProposal(ctx, proposal, e.StateVersion, e.LedgerCursor, nil, sp.ExpireAt+1)
	if err != nil || !receipt.Accepted || receipt.SendError != nil || receipt.Versions[execution.StrategyID(c.StrategyID)] != 1 || sink.Paper.Metrics().Fills != fills {
		t.Fatal("cold retry rebuilt expired allocation", receipt, err)
	}
	proposal.NextState = json.RawMessage(`{"step":3}`)
	if _, err := fresh.AcceptProposal(ctx, proposal, e.StateVersion, e.LedgerCursor, nil, sp.ExpireAt+1); err == nil {
		t.Fatal("cold retry ignored changed content")
	}
	after, err := sink.Account.PolicyEvidenceContext(ctx, execution.StrategyID(c.StrategyID))
	if err != nil || after.State.Version != before.State.Version || string(after.State.Payload) != string(before.State.Payload) || after.Snapshot.Checkpoint != before.Snapshot.Checkpoint {
		t.Fatal("cold retry rewrote latest checkpoint", after, err)
	}
}

func TestAccountFullReleasesSettledSyntheticZeroWithoutOldQuote(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	c.Manifest.Costs.FeeRate = 0
	c.Manifest.Costs.SlippageRate = 0
	sink, done, err := NewPaperSink(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	ctx := context.Background()
	target := func(seq uint64, sid int32, value string) *factor.PortfolioTarget {
		sp := patchPortfolio(t, seq, factor.Full, 1000, nil).Spec()
		sp.AccountID = c.AccountID
		sp.StrategyID = c.StrategyID
		p, err := factor.NewPortfolioTarget(sp, map[int32]factor.Allocation{sid: {Basis: factor.AbsoluteQuantity, Value: value}})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	for seq, value := range []string{"1", "0"} {
		now := int64(11 + seq)
		q := backtest.Quote{AtMS: now, AvailableAt: now, Price: 100}
		if err := sink.ObserveQuote(ctx, 1, q, now); err != nil {
			t.Fatal(err)
		}
		e, err := sink.PolicyEvidence(ctx, now)
		if err != nil {
			t.Fatal(err)
		}
		proposal := factor.PortfolioProposal{Target: target(uint64(seq+1), 1, value), NextState: json.RawMessage(`{"done":true}`), PlanSequence: uint64(seq + 1)}
		receipt, err := sink.AcceptProposal(ctx, proposal, e.StateVersion, e.LedgerCursor, map[int32]backtest.Quote{1: q}, now)
		if err != nil || receipt.SendError != nil {
			t.Fatal(receipt, err)
		}
	}
	fresh := *sink
	fresh.Instruments = map[int32]execution.Instrument{2: sink.Instruments[2]}
	fresh.PolicySIDMap = map[int32]string{2: c.Snapshot.SIDMap[2]}
	fresh.acceptedPolicy = nil
	q := backtest.Quote{AtMS: 13, AvailableAt: 13, Price: 100}
	if err := fresh.ObserveQuote(ctx, 2, q, 13); err != nil {
		t.Fatal(err)
	}
	e, err := fresh.PolicyEvidence(ctx, 13)
	if err != nil {
		t.Fatal(err)
	}
	proposal := factor.PortfolioProposal{Target: target(3, 2, "1"), NextState: json.RawMessage(`{"done":true}`), PlanSequence: 3}
	if !FullZeroOmissions(proposal.Target, e.Previous, []int32{2})[1] {
		t.Fatal("settled prior zero did not qualify for omission")
	}
	receipt, err := fresh.AcceptProposal(ctx, proposal, e.StateVersion, e.LedgerCursor, map[int32]backtest.Quote{2: q}, 13)
	if err != nil || !receipt.Accepted || receipt.SendError != nil {
		t.Fatal("removed zero quote blocked Full", receipt, err)
	}
	e, err = fresh.PolicyEvidence(ctx, 13)
	if err != nil {
		t.Fatal(err)
	}
	if _, old := e.Previous.Allocations()[1]; old {
		t.Fatal("checkpoint retained released zero SID")
	}
	// Even an explicitly supplied zero cannot bypass execution metadata/quotes.
	explicit := target(4, 1, "0")
	proposal = factor.PortfolioProposal{Target: explicit, NextState: json.RawMessage(`{"done":true}`), PlanSequence: 4}
	if FullZeroOmissions(explicit, e.Previous, []int32{2})[1] {
		t.Fatal("explicit zero was pruned")
	}
	if _, err := fresh.AcceptProposal(ctx, proposal, e.StateVersion, e.LedgerCursor, map[int32]backtest.Quote{2: q}, 13); err == nil || !strings.Contains(err.Error(), "instrument") {
		t.Fatal("explicit zero skipped validation", err)
	}
}
