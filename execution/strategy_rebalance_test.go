package execution

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func strategyService(t *testing.T, memory bool) (*SharedAccountBorrow, *PaperAdapter) {
	t.Helper()
	registry := &AccountRegistry{}
	owner, err := registry.Acquire(testIntent(Buy).Account)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewPaperAdapter(intentPrice("1000"), intentPrice("0"), intentPrice("0"))
	if err != nil {
		t.Fatal(err)
	}
	opts := SharedExecutionOptions{Memory: memory, Adapter: adapter, AuthoritativeSnapshot: true}
	if memory && strings.Contains(t.Name(), "/MemoryHistory") {
		opts.HistoryPath = filepath.Join(t.TempDir(), "history.sqlite")
	}
	if !memory {
		opts.StorePath = filepath.Join(t.TempDir(), "execution.db")
		opts.SenderLeaseDir = t.TempDir()
	}
	service, err := NewSharedAccount(owner, opts)
	if err != nil {
		t.Fatal(err)
	}
	borrow := service.Borrow()
	t.Cleanup(func() { borrow.Release(); registry.Close(); service.Close() })
	fundStrategies(t, service.store)
	if err := borrow.Reconcile("ready", 0); err != nil {
		t.Fatal(err)
	}
	if err := borrow.RegisterRiskPolicy("v1", domainRisk()); err != nil {
		t.Fatal(err)
	}
	if err := borrow.RegisterAccountQuotes([]Instrument{ledgerInstrument()}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	return borrow, adapter
}

func strategyRequest(strategy StrategyID, id string, at int64, mode TargetUpdateMode, targets ...ExecutableTarget) StrategyRebalance {
	for n := range targets {
		targets[n].Strategy = strategy
	}
	return StrategyRebalance{Strategy: strategy, PlanID: id, DecisionMS: at, ExpiresMS: 1000, Mode: mode, Requests: []InstrumentRebalance{{Instrument: ledgerInstrument(), Targets: targets, Quote: VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: at, ReceivedMS: at, ValidUntilMS: 1000, Bar: at}}}}
}

func TestStrategyTargetUpdatesAreOwnedAndIdempotent(t *testing.T) {
	for _, memory := range []bool{false, true} {
		name := "SQLite"
		if memory {
			name = "MemoryStore"
		}
		t.Run(name, func(t *testing.T) {
			borrow, venue := strategyService(t, memory)
			a := strategyRequest("a", "a-full", 10, StrategyTargetsFull, ExecutableTarget{Lot: "first", SignedSteps: 10}, ExecutableTarget{Lot: "second", SignedSteps: 4})
			if err := borrow.RebalanceStrategy(a, 11); err != nil {
				t.Fatal(err)
			}
			b := strategyRequest("b", "b-full", 20, StrategyTargetsFull, ExecutableTarget{Lot: "other", SignedSteps: -6})
			if err := borrow.RebalanceStrategy(b, 21); err != nil {
				t.Fatal(err)
			}
			patch := strategyRequest("a", "a-patch", 30, StrategyTargetsPatch, ExecutableTarget{Lot: "first", SignedSteps: 5})
			if err := borrow.RebalanceStrategy(patch, 31); err != nil {
				t.Fatal(err)
			}
			assertLots := func(first, second, other int64) {
				t.Helper()
				snapshot, err := borrow.Snapshot(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				got := map[VirtualLotID]int64{}
				for _, lot := range snapshot.Lots {
					got[lot.ID] = lot.SignedSteps
				}
				if got["first"] != first || got["second"] != second || got["other"] != other {
					t.Fatal("target update changed another lot", got)
				}
				assertNAVConservation(t, snapshot, "100")
			}
			assertLots(5, 4, -6)
			full := strategyRequest("a", "a-replace", 40, StrategyTargetsFull, ExecutableTarget{Lot: "first", SignedSteps: 2})
			if err := borrow.RebalanceStrategy(full, 41); err != nil {
				t.Fatal(err)
			}
			assertLots(2, 0, -6)
			empty := StrategyRebalance{Strategy: "a", PlanID: "a-empty", DecisionMS: 50, ExpiresMS: 1000, Mode: StrategyTargetsFull}
			if err := borrow.RebalanceStrategy(empty, 51); err != nil {
				t.Fatal(err)
			}
			assertLots(0, 0, -6)
			before := venue.Metrics().Fills
			if err := borrow.RebalanceStrategy(patch, 60); err != nil {
				t.Fatal("retry after unrelated revision lost original plan", err)
			}
			if venue.Metrics().Fills != before {
				t.Fatal("old revision re-executed targets")
			}
			assertLots(0, 0, -6)
			patch.Requests[0].Targets[0].SignedSteps = 8
			if _, err := borrow.PrepareStrategy(patch); err == nil {
				t.Fatal("reused revision changed target")
			}
			foreign := strategyRequest("a", "foreign", 60, StrategyTargetsFull, ExecutableTarget{Lot: "bad", SignedSteps: 1})
			foreign.Requests[0].Targets[0].Strategy = "b"
			if _, err := borrow.PrepareStrategy(foreign); err == nil {
				t.Fatal("caller submitted another strategy's target")
			}
		})
	}
}

func TestStrategyUpdateKeepsOriginalPendingConditions(t *testing.T) {
	for _, memory := range []bool{false, true} {
		name := "SQLite"
		if memory {
			name = "MemoryStore"
		}
		t.Run(name, func(t *testing.T) {
			borrow, _ := strategyService(t, memory)
			pending := strategyRequest("b", "pending", 10, StrategyTargetsFull, ExecutableTarget{Lot: "pending", SignedSteps: -6})
			pending.ExpiresMS = 50
			constraint := EligibleIntent{ID: "original", Account: testIntent(Buy).Account, Strategy: "b", Lot: "pending", Instrument: "BTC", Kind: EntryIntent, Side: Sell, QuantitySteps: 6, State: Eligible, Conditions: IntentConditions{Limit: intentPrice("100"), CreatedBar: 10, StopBars: 5}}
			pending.Requests[0].IntentConstraints = []EligibleIntent{constraint}
			if _, err := borrow.PrepareStrategy(pending); err != nil {
				t.Fatal(err)
			}
			request := strategyRequest("a", "later", 20, StrategyTargetsFull, ExecutableTarget{Lot: "a", SignedSteps: 1})
			if _, err := borrow.PrepareStrategy(request); err == nil {
				t.Fatal("other strategy expiry/StopBars renewed")
			}
			plan, err := borrow.LatestPlan(context.Background())
			if err != nil || plan.ID != "pending" {
				t.Fatal("rejected revision replaced pending plan", plan.ID, err)
			}
			if _, err := borrow.service.store.StrategyCheckpoint(context.Background(), "a", "target-revision:later"); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("rejected source revision escaped transaction", err)
			}
			request.DecisionMS = 12
			request.Requests[0].Quote.AtMS = 12
			request.Requests[0].Quote.ReceivedMS = 12
			request.Requests[0].Quote.Bar = 12
			prepared, err := borrow.PrepareStrategy(request)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, c := range prepared.Plan.ContributorConstraints {
				if c.Strategy == "b" {
					found = true
					if c.Conditions.CreatedBar != 10 || c.Conditions.StopBars != 5 || c.Conditions.ExpiresAtMS != 50 || !c.Conditions.Limit.Equal(intentPrice("100")) {
						t.Fatal("original conditions changed", c.Conditions)
					}
				}
			}
			if !found {
				t.Fatal("carried contributor lost original conditions")
			}
		})
	}
}
