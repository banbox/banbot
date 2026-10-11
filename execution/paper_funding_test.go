package execution

import (
	"context"
	"testing"
)

func TestPaperFundingFrozenFactsAndCash(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			borrow, paper := strategyService(t, backend != "SQLite")
			if err := borrow.RebalanceStrategy(strategyRequest("a", "enter-funding", 10, StrategyTargetsFull, ExecutableTarget{Lot: "first", SignedSteps: 10}), 11); err != nil {
				t.Fatal(err)
			}
			event := PaperFundingObservation{ID: "paper-funding", Instrument: ledgerInstrument(), Mark: intentPrice("100"), Rate: intentPrice("0.01"), AtMS: 12, AvailableAt: 12}
			before, _ := borrow.Snapshot(ctx)
			venueBefore, _ := paper.Snapshot(ctx)
			if applied, err := borrow.ApplyPaperFunding(ctx, event, 12); err != nil || !applied {
				t.Fatal(applied, err)
			}
			after, _ := borrow.Snapshot(ctx)
			venueAfter, _ := paper.Snapshot(ctx)
			delta := event.Instrument.Notional(10, event.Mark).Mul(event.Rate).Neg().Round(event.Instrument.MoneyScale)
			if !after.AccountSettledCash.Sub(before.AccountSettledCash).Equal(delta) {
				t.Fatal("account funding mismatch")
			}
			if !after.SyntheticStrategyCash["a"].Sub(before.SyntheticStrategyCash["a"]).Equal(delta) {
				t.Fatal("strategy funding attribution mismatch")
			}
			if !intentPrice(venueAfter.Cash).Sub(intentPrice(venueBefore.Cash)).Equal(delta) {
				t.Fatal("venue funding mismatch")
			}
			if err := borrow.RebalanceStrategy(strategyRequest("a", "reduce-funding", 20, StrategyTargetsFull, ExecutableTarget{Lot: "first", SignedSteps: 5}), 21); err != nil {
				t.Fatal(err)
			}
			frozen, _ := borrow.Snapshot(ctx)
			frozenVenue, _ := paper.Snapshot(ctx)
			if applied, err := borrow.ApplyPaperFunding(ctx, event, 12); err != nil || applied {
				t.Fatal("changed lots altered replay", applied, err)
			}
			mutations := []func(*PaperFundingObservation){func(e *PaperFundingObservation) { e.Rate = intentPrice("0.02") }, func(e *PaperFundingObservation) { e.Mark = intentPrice("101") }, func(e *PaperFundingObservation) { e.Instrument.Version = "v2" }, func(e *PaperFundingObservation) { e.AtMS = 13; e.AvailableAt = 13 }}
			for _, change := range mutations {
				other := event
				change(&other)
				if _, err := borrow.ApplyPaperFunding(ctx, other, other.AtMS); err == nil {
					t.Fatal("changed source identity accepted")
				}
			}
			if _, err := borrow.ApplyPaperFunding(ctx, event, 22); err == nil {
				t.Fatal("late observation accepted")
			}
			final, _ := borrow.Snapshot(ctx)
			finalVenue, _ := paper.Snapshot(ctx)
			a, _ := payload(frozen)
			b, _ := payload(final)
			if a != b || frozenVenue.Cash != finalVenue.Cash {
				t.Fatal("replay/rejection mutated account")
			}
			if backend == "MemoryHistory" {
				store := borrow.Service().Store()
				if _, hot := store.memory.records[recordsEvent][memoryKey{account: store.accountID, first: event.ID}]; hot {
					t.Fatal("settled funding fact retained in hot memory")
				}
				stats, err := borrow.Service().Store().MemoryHistoryStats(ctx)
				if err != nil || stats.ColdRecords == 0 {
					t.Fatal("funding did not enter cold history", stats, err)
				}
				page, err := borrow.Service().Store().EventsAfter(ctx, 0, 100)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, committed := range page {
					if committed.ID == event.ID {
						found = true
						if committed.Kind != "PaperFunding" || len(committed.Ledger) != 1 || committed.Ledger[0].Kind != string(Funding) || !committed.Ledger[0].CashDelta.Equal(delta) {
							t.Fatal("cold projection lost funding", committed)
						}
					}
				}
				if !found {
					t.Fatal("funding fact lost")
				}
			}
		})
	}
}

func TestPaperFundingArchiveFailureRollsBackVenueAndAccount(t *testing.T) {
	t.Run("MemoryHistory", func(t *testing.T) {
		ctx := context.Background()
		borrow, paper := strategyService(t, true)
		store := borrow.Service().Store()
		if err := borrow.RebalanceStrategy(strategyRequest("a", "enter-before-failure", 10, StrategyTargetsFull, ExecutableTarget{Lot: "first", SignedSteps: 10}), 11); err != nil {
			t.Fatal(err)
		}
		before, _ := borrow.Snapshot(ctx)
		venueBefore, _ := paper.Snapshot(ctx)
		if _, err := store.memory.history.db.Exec(`CREATE TRIGGER reject_paper_funding BEFORE INSERT ON history_record WHEN NEW.id='rejected-funding' BEGIN SELECT RAISE(ABORT,'archive failure'); END`); err != nil {
			t.Fatal(err)
		}
		e := PaperFundingObservation{ID: "rejected-funding", Instrument: ledgerInstrument(), Mark: intentPrice("100"), Rate: intentPrice("0.01"), AtMS: 12, AvailableAt: 12}
		if applied, err := borrow.ApplyPaperFunding(ctx, e, 12); err == nil || applied {
			t.Fatal("archive failure applied", applied, err)
		}
		after, _ := borrow.Snapshot(ctx)
		venueAfter, _ := paper.Snapshot(ctx)
		a, _ := payload(before)
		b, _ := payload(after)
		if a != b || venueBefore.Cash != venueAfter.Cash {
			t.Fatal("failed commit mutated cash")
		}
		if _, err := store.memory.history.db.Exec(`DROP TRIGGER reject_paper_funding`); err != nil {
			t.Fatal(err)
		}
		if applied, err := borrow.ApplyPaperFunding(ctx, e, 12); err != nil || !applied {
			t.Fatal("failed event could not retry", applied, err)
		}
	})
}

func TestPaperFundingRejectsUnassignedExposure(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(map[bool]string{false: "unassigned", true: "external"}[external], func(t *testing.T) {
			ctx := context.Background()
			borrow, paper := strategyService(t, true)
			store := borrow.Service().Store()
			if external {
				if _, err := store.ApplyExternalPosition(ctx, ExternalPositionEvent{ID: "manual-position", Kind: ExternalCashChange, Instrument: ledgerInstrument(), Side: Buy, Steps: 2, Price: intentPrice("100"), AtMS: 11}); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := store.commit(ctx, func(tx *storeTxn) error {
					return store.writeActual(tx, VirtualLot{Instrument: ledgerInstrument(), SignedSteps: 2})
				}); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := borrow.Snapshot(ctx)
			venueBefore, _ := paper.Snapshot(ctx)
			e := PaperFundingObservation{ID: "unassigned-funding", Instrument: ledgerInstrument(), Mark: intentPrice("100"), Rate: intentPrice("0.01"), AtMS: 12, AvailableAt: 12}
			if applied, err := borrow.ApplyPaperFunding(ctx, e, 12); err == nil || applied {
				t.Fatal("unattributed exposure charged", applied, err)
			}
			after, _ := borrow.Snapshot(ctx)
			venueAfter, _ := paper.Snapshot(ctx)
			a, _ := payload(before)
			b, _ := payload(after)
			if a != b || venueBefore.Cash != venueAfter.Cash {
				t.Fatal("rejected funding mutated cash")
			}
		})
	}
}
