package runner

import (
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/orm"
	"github.com/shopspring/decimal"
)

func TestOptionSinkPremiumNAVAndPublishedExpiry(t *testing.T) {
	ctx := context.Background()
	c := paperConfig(t, archiveConfig(t, false))
	c.Mode = Events
	c.Manifest.Costs.FeeRate = 0
	c.Manifest.Costs.SlippageRate = 0
	i := c.Execution.Instruments[1]
	i.Valuation = "cash_european_option"
	i.QuantityStep = decimal.NewFromInt(1)
	i.Option = &execution.EuropeanOption{UnderlyingID: "underlying", Right: "call", Strike: decimal.NewFromInt(100), ExpiryMS: 1000, UnderlyingStressMax: decimal.NewFromInt(200)}
	c.Execution.Instruments[1] = i
	sink, done, err := NewPaperSink(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	q := backtest.Quote{AtMS: 11, AvailableAt: 12, Price: 5}
	if err := sink.ObserveQuote(ctx, 1, q, 12); err != nil {
		t.Fatal(err)
	}
	sp := factor.PortfolioSpec{StrategyID: c.StrategyID, AccountID: c.AccountID, DecisionTime: 10, ExecutableAt: 11, ExpireAt: 100, PlanSequence: 1, SnapshotID: "snapshot", PlanHash: "definition", FactorPlanHash: "dag", UniverseVersion: "u", Budget: factor.FrozenBudget{Version: "budget", Currency: "USD", NAV: 10000}, Mode: factor.Full}
	p, err := factor.NewTargetPortfolio(sp, map[int32]float64{1: .001})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.ProcessSnapshot(ctx, p, map[int32]backtest.Quote{1: q}, 12); err != nil {
		t.Fatal(err)
	}
	state, err := sink.StrategyState(ctx, 12)
	if err != nil {
		t.Fatal(err)
	}
	if state.Cash != 9990 || state.NAV != 10000 || state.Quantities[1] != 2 {
		t.Fatal(state)
	}
	// A Full flat proposal still closes another asset, and may omit the option
	// explicitly: EffectiveAllocations supplies its zero against prior exposure.
	previous, err := factor.PortfolioTargetFromWeights(p)
	if err != nil {
		t.Fatal(err)
	}
	flatSpec := sp
	flatSpec.DecisionTime = 1004
	flatSpec.ExecutableAt = 1005
	flatSpec.ExpireAt = 1100
	flatSpec.PlanSequence = 2
	flatAllocation, err := factor.NewPortfolioTarget(flatSpec, map[int32]factor.Allocation{3: {Basis: factor.AbsoluteQuantity, Value: "0"}})
	if err != nil {
		t.Fatal(err)
	}
	pending := &pendingProposal{proposal: factor.PortfolioProposal{Target: flatAllocation}}
	freshEquity := map[int32]backtest.Quote{3: {AtMS: 1005, AvailableAt: 1005, Price: 100}}
	if ready, err := proposalReady(ctx, sink, pending, previous, freshEquity, 1005, []int32{1, 3}); err != nil || ready {
		t.Fatal("elapsed expiry without settlement exempted quote", ready, err)
	}
	r := factor.VersionRecord{Series: orm.DataSeries{Source: "option-expiry", Sid: 1, TimeMS: 1000, EndMS: 1000, Closed: true, Values: map[string]any{"fixing": "110"}}, EventTime: 1000, AvailableAt: 1002, IngestedAt: 1003, Revision: 1, SourceVersion: "v1"}
	if err := sink.ObserveOptionExpiryRecord(ctx, r, 1002); err == nil {
		t.Fatal("unobserved receipt accepted")
	}
	if err := sink.ObserveOptionExpiryRecord(ctx, r, 1003); err != nil {
		t.Fatal(err)
	}
	if err := sink.ObserveOptionExpiryRecord(ctx, r, 1004); err != nil {
		t.Fatal("retry", err)
	}
	if ready, err := proposalReady(ctx, sink, pending, previous, freshEquity, 1005, []int32{1, 3}); err != nil || !ready {
		t.Fatal("settled omitted Full option blocked equity flat", ready, err)
	}
	if ready, err := proposalReady(ctx, sink, pending, previous, nil, 1005, []int32{1, 3}); err != nil || ready {
		t.Fatal("settled option exempted other asset quote", ready, err)
	}
	resurrect, err := factor.NewPortfolioTarget(flatSpec, map[int32]factor.Allocation{1: {Basis: factor.AbsoluteQuantity, Value: "1"}, 3: {Basis: factor.AbsoluteQuantity, Value: "0"}})
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := proposalReady(ctx, sink, &pendingProposal{proposal: factor.PortfolioProposal{Target: resurrect}}, previous, freshEquity, 1005, []int32{1, 3}); err == nil || ready {
		t.Fatal("settled nonzero target exempted", ready, err)
	}
	state, err = sink.StrategyState(ctx, 1004)
	if err != nil {
		t.Fatal(err)
	}
	if state.Cash != 10010 || state.NAV != 10010 || state.Quantities[1] != 0 {
		t.Fatal(state)
	}
	venue, err := sink.Paper.Snapshot(ctx)
	if err != nil || venue.Cash != "10010" {
		t.Fatal(venue, err)
	}
	sp.DecisionTime = 1004
	sp.ExecutableAt = 1005
	sp.ExpireAt = 1100
	sp.PlanSequence = 2
	flat, err := factor.NewTargetPortfolio(sp, map[int32]float64{1: 0})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.ProcessSnapshot(ctx, flat, nil, 1005); err != nil {
		t.Fatal("expired flat requires quote", err)
	}
	c.Mode = Weights
	if err := ValidateReplayConfig(c, true); err == nil {
		t.Fatal("option silently received perpetual Weights accounting")
	}
	clone, err := CloneConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	clone.Execution.Instruments[1].Option.Right = "put"
	if c.Execution.Instruments[1].Option.Right != "call" {
		t.Fatal("descriptor alias in cloned config")
	}
}

func TestOptionExpiryArchiveSourceRetainsPublicationClock(t *testing.T) {
	ctx := context.Background()
	store, err := factor.NewVersionStore(10)
	if err != nil {
		t.Fatal(err)
	}
	r := factor.VersionRecord{Series: orm.DataSeries{Source: "option-expiry", TimeFrame: "event", Sid: 1, TimeMS: 1000, EndMS: 1000, Closed: true, Values: map[string]any{"fixing": "110", "custom": int64(7), "null": nil}}, EventTime: 1000, AvailableAt: 1002, IngestedAt: 1004, Revision: 1, SourceVersion: "v1"}
	if err := store.Put(r); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "expiry.gob")
	if _, err := store.Export(path); err != nil {
		t.Fatal(err)
	}
	c := Config{MaxRecords: 10, OptionExpirySource: "option-expiry", Prices: PriceStream{Source: "prices"}, Snapshot: factor.SnapshotSpec{ReplayTime: 1}}
	input, err := openArchiveInput(ctx, c, Chunk{Path: path, From: 999, To: 1005})
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	batch, err := input.Next(ctx)
	if err != nil || batch.AtMS != 1004 || len(batch.Records) != 1 {
		t.Fatal(batch, err)
	}
	if batch.Records[0].Series.Values["fixing"] != "110" || batch.Records[0].Series.Values["custom"] != int64(7) || batch.Records[0].AvailableAt != 1002 {
		t.Fatal(batch)
	}
	if _, err := input.Next(ctx); err != io.EOF {
		t.Fatal(err)
	}
}
