package runtime

import (
	"context"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
	"strings"
	"testing"
)

func TestProductionMixedAccountPolicyAndOutsideUniverseMarks(t *testing.T) {
	f := newSharedTriggerFixture(t)
	f.entry(t, &strat.EnterReq{Tag: "outside-factor", Amount: 1})
	ctx := context.Background()
	if err := f.rt.SharedExecution().CashEvent(execution.CashEvent{ID: "factor-capital", Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Strategy: "ts", Amount: decimal.NewFromInt(-500)}, {Strategy: "cs", Amount: decimal.NewFromInt(500)}}, AtMS: 101}); err != nil {
		t.Fatal(err)
	}
	eth := f.bridge.Instruments["BTC"]
	eth.ID = "ETH"
	risk := f.bridge.Risk
	risk.StrategyGrossLimits = map[execution.StrategyID]decimal.Decimal{"cs": decimal.NewFromInt(1000)}
	sinkFor := func(rt *Runtime) *runner.AccountSink {
		return &runner.AccountSink{Account: rt.SharedExecution(), AccountID: "default", StrategyID: "cs", Currency: "USDT", Instruments: map[int32]execution.Instrument{2: eth}, Paper: f.adapter, Risk: risk, QuoteTTLMS: 1000, Clock: func() int64 { return 102 }}
	}
	portfolio := func(sequence uint64) *factor.TargetPortfolio {
		p, err := factor.NewTargetPortfolio(factor.PortfolioSpec{StrategyID: "cs", AccountID: "default", DecisionTime: 101, ExecutableAt: 102, ExpireAt: 500, PlanSequence: sequence, SnapshotID: "snapshot", PlanHash: "plan", FactorPlanHash: "factor", UniverseVersion: "ETH-only", Budget: factor.FrozenBudget{Version: "capital", Currency: "USDT", NAV: 500}, Mode: factor.Full}, map[int32]float64{2: .2})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	q := backtest.Quote{AtMS: 102, AvailableAt: 102, Price: 100}
	f.rt.Clock.SetTimeMS(102)
	sink := sinkFor(f.rt)
	if err := sink.ObserveQuote(ctx, 2, q, 102); err != nil {
		t.Fatal(err)
	}
	if err := sink.ProcessSnapshot(ctx, portfolio(1), map[int32]backtest.Quote{2: q}, 102); err == nil || !strings.Contains(err.Error(), "policy missing: cs") {
		t.Fatalf("unknown factor policy not refused: %v", err)
	}
	if err := sink.RegisterExecution(); err != nil {
		t.Fatal(err)
	}
	if _, ok := sink.Risk.StrategyGrossLimits["ts"]; ok {
		t.Fatal("fixture manually injected TS policy")
	}
	if err := sink.ProcessSnapshot(ctx, portfolio(1), map[int32]backtest.Quote{2: q}, 102); err != nil {
		t.Fatal(err)
	}
	view, err := sink.Account.AccountRisk(ctx, 102)
	if err != nil {
		t.Fatal(err)
	}
	if !view.StrategyGrossLimits["ts"].Equal(decimal.NewFromInt(1000)) || !view.StrategyGrossLimits["cs"].Equal(decimal.NewFromInt(1000)) || !view.Marks["BTC"].Equal(decimal.NewFromInt(100)) {
		t.Fatal("account policy/marks not composed", view)
	}
	snap, err := sink.Account.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Lots) != 2 || f.adapter.Metrics().Fills != 2 {
		t.Fatal("mixed composition lost TS or factor", snap)
	}
	bad := risk
	bad.StrategyGrossLimits = map[execution.StrategyID]decimal.Decimal{"cs": decimal.NewFromInt(2000)}
	if err := sink.Account.RegisterRiskPolicy("shared-risk-v1", bad); err == nil {
		t.Fatal("registered cap was mutable")
	}
	f.process.Close()
	p := NewProcess()
	defer p.Close()
	rt, err := p.NewRuntime(Options{AccountOwnerKey: &f.key, SharedExecution: f.opts})
	if err != nil {
		t.Fatal(err)
	}
	rt.Clock.SetTimeMS(102)
	if err := rt.SharedExecution().Reconcile("restart", 102); err != nil {
		t.Fatal(err)
	}
	restarted := sinkFor(rt)
	if err := restarted.RegisterExecution(); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ObserveQuote(ctx, 2, q, 102); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Account.AccountRisk(ctx, 102); err == nil || !strings.Contains(err.Error(), "BTC") {
		t.Fatalf("missing outside-universe mark was accepted: %v", err)
	}
	btc := f.bridge.Instruments["BTC"]
	stale := true
	if err := restarted.Account.RegisterAccountQuotes([]execution.Instrument{btc}, func(_ context.Context, _ string, now int64) (execution.VisibleQuote, error) {
		valid := now + 1000
		if stale {
			valid = now
		}
		return execution.VisibleQuote{Bid: decimal.NewFromInt(100), Ask: decimal.NewFromInt(100), AtMS: now, ReceivedMS: now, ValidUntilMS: valid}, nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Account.AccountRisk(ctx, 102); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("stale mark was accepted: %v", err)
	}
	stale = false
	view, err = restarted.Account.AccountRisk(ctx, 102)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.StrategyGrossLimits) != 2 || !view.StrategyGrossLimits["ts"].Equal(decimal.NewFromInt(1000)) {
		t.Fatal("restart lost immutable TS policy", view)
	}
	if err := restarted.ProcessSnapshot(ctx, portfolio(2), map[int32]backtest.Quote{2: q}, 102); err != nil {
		t.Fatal(err)
	}
}
