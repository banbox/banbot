package execution

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecutionSQLiteOwnsFullDurabilityOnReopenedConnections(t *testing.T) {
	store, err := OpenStoreWithLeaseDir(filepath.Join(t.TempDir(), "ledger space #.db"), testIntent(Buy).Account, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.db.SetMaxIdleConns(0)
	for range 2 {
		connection, err := store.db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		var synchronous, foreignKeys int
		var journal string
		for _, check := range []struct {
			query string
			out   any
		}{{"PRAGMA synchronous", &synchronous}, {"PRAGMA foreign_keys", &foreignKeys}, {"PRAGMA journal_mode", &journal}} {
			if err := connection.QueryRowContext(context.Background(), check.query).Scan(check.out); err != nil {
				connection.Close()
				t.Fatal(err)
			}
		}
		connection.Close()
		if synchronous != 2 || foreignKeys != 1 || journal != "wal" {
			t.Fatal("fresh connection lost durable sender policy", synchronous, foreignKeys, journal)
		}
	}
	var unrelated int
	if err := store.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('bottask','inoutorder','exorder')").Scan(&unrelated); err != nil || unrelated != 0 {
		t.Fatal("execution bootstrap created legacy tables", unrelated, err)
	}
}

func TestSQLiteActiveAndTailQueriesUseHistoryIndexes(t *testing.T) {
	store, err := OpenStoreWithLeaseDir(filepath.Join(t.TempDir(), "ledger.db"), testIntent(Buy).Account, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, check := range []struct {
		operation storageOperation
		arguments []any
		index     string
	}{{opListActiveLots, []any{store.accountID}, "exec_lot_active_order"}, {opListCommittedEvents, []any{store.accountID, 10000, 16}, "account=? AND checkpoint>?"}} {
		rows, err := store.db.Query("EXPLAIN QUERY PLAN "+sqliteStatements[check.operation], check.arguments...)
		if err != nil {
			t.Fatal(err)
		}
		var plans []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			plans = append(plans, detail)
		}
		rows.Close()
		joined := strings.Join(plans, "; ")
		t.Log(joined)
		if !strings.Contains(joined, check.index) {
			t.Fatalf("history scan replaced bounded index %s: %s", check.index, joined)
		}
	}
}

func TestClosedLotReopenPreservesHistoryAndRollbackIndexes(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore"} {
		t.Run(backend, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			executor := executorFor(s, h, &fakeExecutionAdapter{})
			for n, fill := range []struct {
				id, price string
				side      OrderSide
				kind      IntentKind
				steps     int64
			}{{"open", "100", Buy, EntryIntent, 10}, {"close", "110", Sell, ExitIntent, 10}} {
				order := planOrder(t, s, fill.id, int64(n+1), fill.side, fill.kind, map[string]int64{"a": fill.steps})
				if err := executor.Send(order.ID, 11); err != nil {
					t.Fatal(err)
				}
				if _, err := s.ApplyFill(context.Background(), FillReport{EventID: fill.id + "-fill", OrderID: order.ID, Steps: fill.steps, Price: intentPrice(fill.price), Fee: intentPrice("0.1"), AtMS: 12}); err != nil {
					t.Fatal(err)
				}
			}
			closed, err := s.Lot(context.Background(), "a", "lot-a")
			if err != nil || closed.SignedSteps != 0 || !closed.RealizedPnL.Equal(intentPrice("10")) || !closed.Fees.Equal(intentPrice("0.2")) {
				t.Fatal("closed lot audit disappeared", closed, err)
			}
			if s.memory != nil && len(s.memory.activeLots) != 0 {
				t.Fatal("closed lot retained in active index")
			}
			order := planOrder(t, s, "reopen", 3, Buy, EntryIntent, map[string]int64{"a": 5})
			if err := executor.Send(order.ID, 11); err != nil {
				t.Fatal(err)
			}
			fill := FillReport{EventID: "reopen-fill", OrderID: order.ID, Steps: 5, Price: intentPrice("120"), Fee: intentPrice("0.1"), AtMS: 12}
			fault := errors.New("rollback reopened lot")
			if err := s.atomically(context.Background(), func(ctx context.Context) error {
				if _, err := s.ApplyFill(ctx, fill); err != nil {
					return err
				}
				return fault
			}); !errors.Is(err, fault) {
				t.Fatal(err)
			}
			if s.memory != nil && len(s.memory.activeLots) != 0 {
				t.Fatal("rollback leaked active lot index")
			}
			if _, err := s.ApplyFill(context.Background(), fill); err != nil {
				t.Fatal(err)
			}
			reopened, err := s.Lot(context.Background(), "a", "lot-a")
			if err != nil || reopened.SignedSteps != 5 || !reopened.CostBasis.Equal(intentPrice("60")) || !reopened.RealizedPnL.Equal(closed.RealizedPnL) || !reopened.Fees.Equal(intentPrice("0.3")) {
				t.Fatal("reopened lot lost prior realized/fee history", reopened, err)
			}
		})
	}
}

func TestStrategyBatchAcceptancePrecedesFillsAndRollsBack(t *testing.T) {
	for _, memory := range []bool{false, true} {
		name := "SQLite"
		if memory {
			name = "MemoryStore"
		}
		t.Run(name, func(t *testing.T) {
			borrow, _ := strategyService(t, memory)
			baseline, _ := borrow.Snapshot(context.Background())
			updates := []StrategyRebalance{
				strategyRequest("a", "batch", 10, StrategyTargetsFull, ExecutableTarget{Lot: "a-lot", SignedSteps: 4}),
				strategyRequest("b", "batch", 10, StrategyTargetsPatch, ExecutableTarget{Lot: "b-lot", SignedSteps: -2}),
			}
			checkpoint := StrategyCheckpoint{Strategy: "ts", Name: "requests", Payload: []byte(`{"accepted":true}`), Events: []StrategyAcceptedEvent{{Strategy: "a", Lot: "a-lot", Kind: EntryIntent, CommandID: "a-command", AtMS: 10}, {Strategy: "b", Lot: "b-lot", Kind: EntryIntent, CommandID: "b-command", AtMS: 10}}}
			var prepared PreparedRebalance
			err := borrow.WithState(func(account *SharedAccount) error {
				var err error
				prepared, err = account.PrepareStrategiesWithCheckpoint(updates, context.Background(), &checkpoint)
				return err
			})
			if err != nil || len(prepared.InternalMatchIDs) != 1 || len(prepared.OrderIDs) != 1 {
				t.Fatal(prepared, err)
			}
			events, err := borrow.service.store.EventsAfter(context.Background(), baseline.Checkpoint, 100)
			if err != nil || len(events) < 3 || events[0].Kind != "StrategyAccepted" || events[1].Kind != "StrategyAccepted" || events[2].Kind != "InternalFill" {
				t.Fatal("fill became visible before acceptance", events, err)
			}
			before, _ := borrow.Snapshot(context.Background())
			if err := borrow.WithState(func(account *SharedAccount) error {
				_, err := account.PrepareStrategiesWithCheckpoint(updates, context.Background(), &checkpoint)
				return err
			}); err != nil {
				t.Fatal("accepted batch replay failed", err)
			}
			after, _ := borrow.Snapshot(context.Background())
			if before.Checkpoint != after.Checkpoint {
				t.Fatal("replay emitted acceptance/fill twice")
			}
			for n := range updates {
				updates[n].PlanID = "bad-batch"
			}
			updates[1].Requests[0].Targets[0].Strategy = "a"
			checkpoint.Name = "rejected"
			if err := borrow.WithState(func(account *SharedAccount) error {
				_, err := account.PrepareStrategiesWithCheckpoint(updates, context.Background(), &checkpoint)
				return err
			}); err == nil {
				t.Fatal("batch accepted foreign target")
			}
			if _, err := borrow.service.store.StrategyCheckpoint(context.Background(), "ts", "rejected"); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("rejected batch committed checkpoint", err)
			}
		})
	}
}
