package runner

import (
	"context"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/orm"
	"github.com/shopspring/decimal"
)

// A settled option flattened by a live proposal needs no fresh quote; an
// unsettled one must still wait for it.
func TestLiveExecuteExemptsSettledOptionQuote(t *testing.T) {
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
	sp := factor.PortfolioSpec{StrategyID: c.StrategyID, AccountID: c.AccountID, DecisionTime: 1004, ExecutableAt: 1005, ExpireAt: 1100, PlanSequence: 1, SnapshotID: "snapshot", PlanHash: "definition", FactorPlanHash: "dag", UniverseVersion: "u", Budget: factor.FrozenBudget{Version: "budget", Currency: "USD", NAV: 10000}, Mode: factor.Full}
	flat, err := factor.NewTargetPortfolio(sp, map[int32]float64{1: 0})
	if err != nil {
		t.Fatal(err)
	}
	l := &Live{sink: sink, pending: flat, quotes: map[int32]backtest.Quote{}}
	if err = l.execute(ctx, 1005); err != nil || l.pending == nil {
		t.Fatal("unsettled option must wait for a quote", err)
	}
	r := factor.VersionRecord{Series: orm.DataSeries{Source: "option-expiry", Sid: 1, TimeMS: 1000, EndMS: 1000, Closed: true, Values: map[string]any{"fixing": "110"}}, EventTime: 1000, AvailableAt: 1002, IngestedAt: 1003, Revision: 1, SourceVersion: "v1"}
	if err = sink.ObserveOptionExpiryRecord(ctx, r, 1003); err != nil {
		t.Fatal(err)
	}
	if err = l.execute(ctx, 1005); err != nil {
		t.Fatal(err)
	}
	if l.pending != nil {
		t.Fatal("settled option flat target blocked on missing quote")
	}
}
