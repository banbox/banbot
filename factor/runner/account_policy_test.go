package runner

import (
	"context"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"strings"
	"testing"
)

func TestPaperValuationMarksExpireAtBoundedGridWindow(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	c.Manifest.Costs.FeeRate = 0
	c.Manifest.Costs.SlippageRate = 0
	sink, done, err := NewPaperSink(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	q := backtest.Quote{AtMS: 11, AvailableAt: 11, Price: 100}
	if err := sink.ObserveQuote(context.Background(), 1, q, 11); err != nil {
		t.Fatal(err)
	}
	sp := patchPortfolio(t, 1, factor.Full, 10000, map[int32]float64{1: .1}).Spec()
	sp.AccountID = c.AccountID
	sp.StrategyID = c.StrategyID
	p, err := factor.NewTargetPortfolio(sp, map[int32]float64{1: .1})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.ProcessSnapshot(context.Background(), p, map[int32]backtest.Quote{1: q}, 11); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Account.AccountRisk(context.Background(), 11+c.DecisionInterval+c.ExpiryMS-1); err != nil {
		t.Fatal("bounded last-visible mark expired early", err)
	}
	if _, err := sink.Account.AccountRisk(context.Background(), 11+c.DecisionInterval+c.ExpiryMS); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("unbounded paper valuation cache: %v", err)
	}
}
