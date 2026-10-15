package runner

import (
	"context"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
)

func TestLiveGenerationSettledScopeRemovalFillsThroughFreshAccountSink(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	c.Mode = Trade
	c.Chunks = nil
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Costs.FeeRate = 0
	c.Manifest.Costs.SlippageRate = 0
	var err error
	c.Plan, err = factor.New().Add("value", factor.Field("kline", "close", "1h")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"value"}, Weights: map[string]float64{"value": 1}}
	sink, closeAccount, err := NewPaperSink(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer closeAccount()
	ctx := context.Background()
	portfolio := func(sequence uint64, targets map[int32]float64) *factor.TargetPortfolio {
		p := patchPortfolio(t, sequence, factor.Full, 10000, targets)
		spec := p.Spec()
		spec.AccountID = c.AccountID
		spec.StrategyID = c.StrategyID
		p, err = factor.NewTargetPortfolio(spec, targets)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	quote := backtest.Quote{AtMS: 11, AvailableAt: 11, Price: 100}
	if err := sink.ObserveQuote(ctx, 1, quote, 11); err != nil {
		t.Fatal(err)
	}
	if err := sink.ProcessSnapshot(ctx, portfolio(1, map[int32]float64{1: .5}), map[int32]backtest.Quote{1: quote}, 11); err != nil {
		t.Fatal(err)
	}
	quote = backtest.Quote{AtMS: 20, AvailableAt: 20, Price: 100}
	if err := sink.ObserveQuote(ctx, 1, quote, 20); err != nil {
		t.Fatal(err)
	}
	if err := sink.ProcessSnapshot(ctx, portfolio(2, map[int32]float64{1: 0}), map[int32]backtest.Quote{1: quote}, 20); err != nil {
		t.Fatal(err)
	}
	settled, err := sink.Account.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, lot := range settled.Lots {
		if lot.Instrument.ID == sink.Instruments[1].ID && lot.SignedSteps != 0 {
			t.Fatal("old SID did not settle")
		}
	}
	old, err := NewLive(c, sink, func() int64 { return 30 }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Stop()
	old.previous = sink.previous
	old.sequence = 2
	old.lastDecision = 10
	nextCfg := c
	nextCfg.Snapshot.Universe = factor.Universe{Version: "next", Static: true, Investable: []int32{2}, Reference: []int32{2}, Evaluation: []int32{2}, Tradable: []int32{2}, Tracked: []int32{2}}
	nextCfg.Execution.Instruments = map[int32]execution.Instrument{2: sink.Instruments[2]}
	fresh := *sink
	fresh.Instruments = nextCfg.Execution.Instruments
	fresh.previous = nil
	next, err := NewLive(nextCfg, &fresh, func() int64 { return 30 }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Stop()
	if err := next.InheritAdmission(old); err != nil {
		t.Fatal(err)
	}
	if _, exists := fresh.previous.Targets()[1]; exists {
		t.Fatal("fresh AccountSink retained settled old scope")
	}
	if _, exists := sink.previous.Targets()[1]; !exists {
		t.Fatal("inheritance mutated old AccountSink scope")
	}
	quote = backtest.Quote{AtMS: 30, AvailableAt: 30, Price: 100}
	if err := fresh.ObserveQuote(ctx, 2, quote, 30); err != nil {
		t.Fatal(err)
	}
	next.quotes = map[int32]backtest.Quote{2: quote}
	next.pending = portfolio(3, map[int32]float64{2: .2})
	beforeFills := sink.Paper.Metrics().Fills
	if err := next.execute(ctx, 30); err != nil {
		t.Fatal(err)
	}
	if next.pending != nil || sink.Paper.Metrics().Fills <= beforeFills {
		t.Fatal("successor did not fill using only the retained SID quote")
	}
	after, err := sink.Account.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	filled := false
	for _, lot := range after.Lots {
		if lot.Instrument.ID == fresh.Instruments[2].ID && lot.SignedSteps != 0 {
			filled = true
		}
	}
	if !filled {
		t.Fatal("successor fill did not reach shared account ledger")
	}
}
