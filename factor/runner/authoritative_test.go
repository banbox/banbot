package runner

import (
	"context"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
	"github.com/shopspring/decimal"
)

func TestAuthoritativeFundingUsesActualCashAndStableIdentity(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	sink, done, err := NewPaperSink(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	sink.AuthoritativeFunding = true
	ctx := context.Background()
	r := factor.VersionRecord{Series: orm.DataSeries{Sid: 1, Values: map[string]any{"settlement_id": "venue-funding-1", "mark": "100", "rate": "0.001", "account_amount": "1.23456789"}}, EventTime: 10, AvailableAt: 11, IngestedAt: 12}
	if err := sink.ObserveFundingRecord(ctx, r, 12); err != nil {
		t.Fatal(err)
	}
	snap, err := sink.Account.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !snap.AccountSettledCash.Equal(decimal.RequireFromString("10001.23456789")) || !snap.UnassignedCash.Equal(decimal.RequireFromString("1.23456789")) || !snap.SyntheticStrategyCash[execution.StrategyID(c.StrategyID)].Equal(decimal.NewFromInt(10000)) {
		t.Fatalf("actual funding amount replaced by model: %+v", snap)
	}
	if err := sink.ObserveFundingRecord(ctx, r, 13); err != nil {
		t.Fatal(err)
	}
	duplicate, err := sink.Account.Snapshot(ctx)
	if err != nil || duplicate.Checkpoint != snap.Checkpoint {
		t.Fatalf("duplicate settlement posted twice: %+v %v", duplicate, err)
	}
	delete(r.Series.Values, "account_amount")
	if err := sink.ObserveFundingRecord(ctx, r, 13); err == nil {
		t.Fatal("rate-only real cash event admitted")
	}
	if err := sink.ObserveFunding(ctx, backtest.Funding{SID: 1, AtMS: 13, Rate: .001}, 13); err == nil {
		t.Fatal("synthetic live funding admitted")
	}
}

func TestLiveFundingSourceAlsoFeedsFactorFields(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	c.FundingSource = "funding"
	c.Manifest.Costs.FundingPolicy = "required-stream"
	plan, err := factor.New().Add("signal", factor.Field("funding", "signal", "event")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Plan = plan
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"signal"}, Weights: map[string]float64{"signal": 1}}
	sink, done, err := NewPaperSink(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	sink.AuthoritativeFunding = true
	live, err := NewLive(c, sink, func() int64 { return 12 }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Stop()
	r := factor.VersionRecord{Series: orm.DataSeries{Source: "funding", TimeFrame: "event", Sid: 1, TimeMS: 10, EndMS: 10, Values: map[string]any{"settlement_id": "factor-funding", "mark": "100", "rate": "0.001", "account_amount": "0", "signal": 42.0}}, EventTime: 10, AvailableAt: 11, IngestedAt: 12, Revision: 1, SourceVersion: "v1"}
	if err = live.Observe(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	row, ok := live.rows[factor.StreamKey{SID: 1, Source: "funding", Frequency: "event"}]
	if !ok || row.Series.Values["signal"] != 42.0 {
		t.Fatal("settlement callback swallowed factor fields")
	}
}

func TestAccountSinkFetchedQuoteUsesCompletionClockAndMidpointSizing(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	sink, done, err := NewPaperSink(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	ctx := context.Background()
	clock := int64(12)
	sink.Clock = func() int64 { return clock }
	sink.VisibleQuote = func(context.Context, string, int64) (execution.VisibleQuote, error) {
		clock = 15
		return execution.VisibleQuote{Bid: decimal.NewFromInt(199), Ask: decimal.NewFromInt(201), AtMS: 14, ReceivedMS: 15, ValidUntilMS: 100, Bar: 14}, nil
	}
	quotes := map[int32]backtest.Quote{1: {AtMS: 11, AvailableAt: 12, Price: 100}}
	if err = sink.ObserveQuote(ctx, 1, quotes[1], 12); err != nil {
		t.Fatal(err)
	}
	p, err := factor.NewTargetPortfolio(factor.PortfolioSpec{StrategyID: c.StrategyID, AccountID: c.AccountID, DecisionTime: 10, ExecutableAt: 11, ExpireAt: 100, PlanSequence: 1, SnapshotID: "s", PlanHash: "p", FactorPlanHash: "f", UniverseVersion: "u", Budget: factor.FrozenBudget{Version: "b", Currency: "USD", NAV: 10000}, Mode: factor.Patch}, map[int32]float64{1: .5})
	if err != nil {
		t.Fatal(err)
	}
	if err = sink.ProcessSnapshot(ctx, p, quotes, 12); err != nil {
		t.Fatal(err)
	}
	snap, err := sink.Account.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Lots) != 1 || snap.Lots[0].SignedSteps != 2500 || sink.Paper.Metrics().LastFill.AtMS != 15 {
		t.Fatalf("source-price sizing/backdated fill: %+v %+v", snap, sink.Paper.Metrics())
	}
}
