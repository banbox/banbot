package execution

import (
	"context"
	"testing"
)

func TestStrategyZeroTargetsRetireOnlyAfterSettlement(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			borrow, venue := strategyService(t, backend != "SQLite")
			eth := ledgerInstrument()
			eth.ID = "ETH"
			if err := borrow.RegisterAccountQuotes([]Instrument{ledgerInstrument(), eth}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
				return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
			}, nil); err != nil {
				t.Fatal(err)
			}
			request := func(id string, at, steps int64) StrategyRebalance {
				r := strategyRequest("a", id, at, StrategyTargetsPatch, ExecutableTarget{Lot: "eth-lot", SignedSteps: steps})
				r.Requests[0].Instrument = eth
				return r
			}
			if err := borrow.RebalanceStrategy(request("enter", 10, 4), 11); err != nil {
				t.Fatal(err)
			}
			exit := request("exit", 20, 0)
			if err := borrow.RebalanceStrategy(exit, 21); err != nil {
				t.Fatal(err)
			}
			other := strategyRequest("b", "other", 30, StrategyTargetsPatch, ExecutableTarget{Lot: "btc-lot", SignedSteps: 1})
			settled, err := borrow.PrepareStrategy(other)
			if err != nil {
				t.Fatal(err)
			}
			for _, target := range settled.Plan.Targets {
				if target.Lot == "eth-lot" {
					t.Fatal("settled zero target remained in the latest plan", target)
				}
			}
			original, err := borrow.service.store.Plan(ctx, exit.PlanID)
			if err != nil || len(original.Targets) != 1 || original.Targets[0].Lot != "eth-lot" || original.Targets[0].SignedSteps != 0 {
				t.Fatal("original exit membership changed", original, err)
			}
			fills := venue.Metrics().Fills
			if err := borrow.RebalanceStrategy(exit, 41); err != nil || venue.Metrics().Fills != fills {
				t.Fatal("old exit retry changed execution", err)
			}
			if err := borrow.RebalanceStrategy(request("reenter", 50, 4), 51); err != nil {
				t.Fatal(err)
			}
			if _, err := borrow.PrepareStrategy(request("pending-exit", 60, 0)); err != nil {
				t.Fatal(err)
			}
			other = strategyRequest("b", "while-pending", 70, StrategyTargetsPatch, ExecutableTarget{Lot: "btc-lot", SignedSteps: 1})
			pending, err := borrow.PrepareStrategy(other)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, target := range pending.Plan.CarriedTargets {
				found = found || target.Instrument == "ETH" && target.Lot == "eth-lot" && target.SignedSteps == 0
			}
			if !found {
				t.Fatal("outstanding exit lost its zero declaration", pending.Plan)
			}
		})
	}
}
