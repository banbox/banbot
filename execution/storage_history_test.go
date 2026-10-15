package execution

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

func TestMemoryHistoryBoundsInactiveRecordsAndReadsOldFacts(t *testing.T) {
	ctx := context.Background()
	s, err := NewMemoryStoreWithHistory(testIntent(Buy).Account, filepath.Join(t.TempDir(), "history.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var hotAt100 int
	for n := 0; n < 1000; n++ {
		id := fmt.Sprintf("history-%04d", n)
		err := s.commit(ctx, func(tx *storeTxn) error {
			planBody, _ := payload(Plan{ID: id, Sequence: int64(n), DecisionMS: 10, ExpiresMS: 1000})
			if _, err := tx.Exec(opInsertPlan, s.accountID, id, n, planBody); err != nil {
				return err
			}
			orderBody, _ := payload(OrderIntent{ID: id, PlanID: id, Instrument: ledgerInstrument(), Side: Buy, Steps: 1})
			if _, err := tx.Exec(opInsertOrder, s.accountID, id, id, orderBody, "client-"+id, string(OrderFilled)); err != nil {
				return err
			}
			lotBody, _ := payload(VirtualLot{Strategy: "a", ID: VirtualLotID(id), Instrument: ledgerInstrument()})
			if _, err := tx.Exec(opPutLot, s.accountID, "a", id, 0, lotBody); err != nil {
				return err
			}
			if _, err := tx.Exec(opInsertEvent, s.accountID, id, "Audit", fmt.Sprintf(`{"n":%d}`, n)); err != nil {
				return err
			}
			if err := s.ledger(tx, LedgerEntry{EventID: id, Kind: "Audit", AtMS: int64(n), CashDelta: intentPrice("0")}); err != nil {
				return err
			}
			return s.commitAccountEvent(tx, id, intentPrice("0"), nil)
		})
		if err != nil {
			t.Fatal(n, err)
		}
		if n == 99 {
			stats, _ := s.MemoryHistoryStats(ctx)
			hotAt100 = stats.HotRecords
		}
	}
	stats, err := s.MemoryHistoryStats(ctx)
	if err != nil || stats.HotRecords != hotAt100 || stats.HotRecords > 3 || stats.ColdRecords < 5000 {
		t.Fatalf("inactive history accumulated in memory: %+v at100=%d err=%v", stats, hotAt100, err)
	}
	for _, id := range []string{"history-0000", "history-0099", "history-0999"} {
		order, err := s.Order(ctx, id)
		if err != nil || order.Intent.ID != id || order.State != OrderFilled || order.ClientID != "client-"+id {
			t.Fatalf("cold order lost: %+v %v", order, err)
		}
	}
	page, err := s.EventsAfter(ctx, 0, 3)
	if err != nil || len(page) != 3 || page[0].ID != "history-0000" || len(page[0].Ledger) != 1 || page[2].Checkpoint != 3 {
		t.Fatalf("lagging projection cannot read cold history: %+v %v", page, err)
	}
	err = s.commit(ctx, func(tx *storeTxn) error {
		fresh, err := s.recordEvent(tx, "history-0000", "Audit", `{"n":0}`)
		if fresh {
			t.Fatal("cold event applied twice")
		}
		return err
	})
	if err != nil {
		t.Fatal("old exact duplicate rejected", err)
	}
	if err := s.commit(ctx, func(tx *storeTxn) error {
		_, err := s.recordEvent(tx, "history-0000", "Audit", `{"n":1}`)
		return err
	}); err == nil {
		t.Fatal("cold identity accepted different content")
	}
	if s.db != nil || s.releaseLease != nil || s.Durability() != MemoryOnly {
		t.Fatal("cold output became a real execution store/sender")
	}
}

func TestMemoryHistoryWriteFailureRollsBackHotAndColdState(t *testing.T) {
	s, err := NewMemoryStoreWithHistory(testIntent(Buy).Account, filepath.Join(t.TempDir(), "history.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	fundStrategies(t, s)
	before, _ := s.Snapshot(ctx)
	version := s.memory.version
	if _, err := s.memory.history.db.Exec(`CREATE TRIGGER reject_event BEFORE INSERT ON history_record WHEN NEW.id='history-failure' BEGIN SELECT RAISE(ABORT,'archive failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyCashEvent(ctx, CashEvent{ID: "history-failure", Kind: CapitalTransfer, Postings: []CashPosting{{Strategy: "a", Amount: intentPrice("-10")}, {Strategy: "b", Amount: intentPrice("10")}}}); err == nil {
		t.Fatal("failed archive silently committed")
	}
	after, err := s.Snapshot(ctx)
	a, _ := payload(before)
	b, _ := payload(after)
	if err != nil || a != b || s.memory.version != version+1 {
		// Snapshot is itself a successful read commit and increments the version.
		t.Fatalf("failed archive changed domain state: before=%s after=%s version=%d err=%v", a, b, s.memory.version, err)
	}
	var count int
	if err := s.memory.history.db.QueryRow("SELECT COUNT(*) FROM history_record WHERE id='history-failure'").Scan(&count); err != nil || count != 0 {
		t.Fatal("failed cold transaction partially published", count, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.EventsAfter(canceled, 0, 3); !errors.Is(err, context.Canceled) {
		t.Fatal("cold read ignored cancellation", err)
	}
}

func TestMemoryHistoryNeverOverwritesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	s, err := NewMemoryStoreWithHistory(testIntent(Buy).Account, path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := NewMemoryStoreWithHistory(testIntent(Buy).Account, path); err == nil {
		t.Fatal("existing history overwritten")
	}
}

func TestMemoryHistoryBoundsStrategyRevisions(t *testing.T) {
	t.Run("MemoryHistory", func(t *testing.T) {
		borrow, venue := strategyService(t, true)
		ctx := context.Background()
		var hotAt80 int
		first := strategyRequest("a", "revision-000", 10, StrategyTargetsFull, ExecutableTarget{Lot: "rolling", SignedSteps: 0})
		for n := 0; n < 160; n++ {
			request := strategyRequest("a", fmt.Sprintf("revision-%03d", n), int64(10+n*2), StrategyTargetsFull, ExecutableTarget{Lot: "rolling", SignedSteps: int64(n % 2)})
			if err := borrow.RebalanceStrategy(request, request.DecisionMS+1); err != nil {
				t.Fatal(n, err)
			}
			if n == 79 {
				stats, err := borrow.service.store.MemoryHistoryStats(ctx)
				if err != nil {
					t.Fatal(err)
				}
				hotAt80 = stats.HotRecords
			}
		}
		stats, err := borrow.service.store.MemoryHistoryStats(ctx)
		if err != nil || stats.HotRecords != hotAt80 {
			t.Fatalf("strategy revisions accumulated in memory: %+v at80=%d err=%v", stats, hotAt80, err)
		}
		fills := venue.Metrics().Fills
		if err := borrow.RebalanceStrategy(first, 400); err != nil {
			t.Fatal("cold revision replay failed", err)
		}
		if venue.Metrics().Fills != fills {
			t.Fatal("old strategy revision executed twice")
		}
		first.Requests[0].Targets[0].SignedSteps = 2
		if _, err := borrow.PrepareStrategy(first); err == nil {
			t.Fatal("cold revision accepted different targets")
		}
	})
}
