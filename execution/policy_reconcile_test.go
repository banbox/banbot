package execution

import (
	"context"
	"testing"
)

func TestPolicyEntryReconciliationPreservesSharedStrategyRemainder(t *testing.T) {
	for _, memory := range []bool{false, true} {
		name := "SQLite"
		if memory {
			name = "Memory"
		}
		t.Run(name, func(t *testing.T) {
			borrow, paper := strategyService(t, memory)
			updates := []StrategyRebalance{strategyRequest("a", "shared-entry", 10, StrategyTargetsPatch, ExecutableTarget{Lot: "cs", SignedSteps: 3}), strategyRequest("b", "shared-entry", 10, StrategyTargetsPatch, ExecutableTarget{Lot: "ts", SignedSteps: 2})}
			for n := range updates {
				u := &updates[n]
				u.Requests[0].Targets[0].SourceSequence = uint64(100 + n)
				lot := u.Requests[0].Targets[0].Lot
				steps := u.Requests[0].Targets[0].SignedSteps
				condition := contributor(borrow.service.store, u.Strategy, lot, Buy, steps, "99", "0")
				condition.Conditions.PostOnly = true
				u.Requests[0].IntentConstraints = []EligibleIntent{condition}
			}
			if err := borrow.WithState(func(s *SharedAccount) error {
				prepared, err := s.PrepareStrategiesWithCheckpoints(updates, context.Background(), nil)
				if err != nil {
					return err
				}
				return s.SendPrepared(prepared, 11, context.Background())
			}); err != nil {
				t.Fatal(err)
			}
			before, _ := borrow.Snapshot(context.Background())
			if len(before.Orders) != 1 || len(before.Orders[0].Intent.Allocations) != 2 || paper.Metrics().Fills != 0 {
				t.Fatal("fixture not a shared resting order", before)
			}
			if err := borrow.CancelPolicyEntriesContext(context.Background(), "a", []string{"BTC"}, 12); err != nil {
				t.Fatal(err)
			}
			after, _ := borrow.Snapshot(context.Background())
			if len(after.Lots) != 0 || len(after.Orders) != 1 || len(after.Orders[0].Intent.Allocations) != 1 || after.Orders[0].Intent.Allocations[0].Strategy != "b" {
				t.Fatal("reconciliation lost untouched TS intent or created CS fill", after)
			}
			quote := VisibleQuote{Bid: intentPrice("98"), Ask: intentPrice("99"), AtMS: 13, ReceivedMS: 13, ValidUntilMS: 1000, Bar: 13}
			if err := borrow.AdvancePaperQuote(context.Background(), "BTC", quote, 13); err != nil {
				t.Fatal(err)
			}
			after, _ = borrow.Snapshot(context.Background())
			if len(after.Lots) != 1 || after.Lots[0].Strategy != "b" || after.Lots[0].SignedSteps != 2 {
				t.Fatal("TS remainder not preserved", after)
			}
			if paper.Metrics().Fills != 1 {
				t.Fatal("cancellation charged fictitious fill", paper.Metrics())
			}
			evidence, err := borrow.PolicyEvidenceContext(context.Background(), "b")
			if err != nil || len(evidence.Fills) != 1 || evidence.Fills[0].PlanSequence != 101 {
				t.Fatal("replacement lost untouched source sequence", evidence.Fills, err)
			}
		})
	}
}
