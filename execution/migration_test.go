package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/shopspring/decimal"
)

func migrationFixture(t *testing.T) LegacyMigration {
	t.Helper()
	backup := filepath.Join(t.TempDir(), "legacy-backup.db")
	if err := os.WriteFile(backup, []byte("preserved legacy snapshot"), 0600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("preserved legacy snapshot"))
	instrument := ledgerInstrument()
	lot := VirtualLot{Strategy: "a", ID: "legacy-lot", Instrument: instrument, SignedSteps: 10, CostBasis: intentPrice("100"), Fees: intentPrice("1")}
	actual := VirtualLot{Instrument: instrument, SignedSteps: 10, CostBasis: intentPrice("100")}
	return LegacyMigration{
		Preflight: func() error { return nil }, ID: "migration-1", SourceVersion: "0.3.8", SourceSnapshotID: "legacy-snapshot-1", StoppedOwnerProof: "stop-and-join-verified", BackupPath: backup,
		BackupSHA256: hex.EncodeToString(digest[:]),
		AccountCash:  intentPrice("999"), StrategyCash: map[StrategyID]decimal.Decimal{"a": intentPrice("999")}, Lots: []VirtualLot{lot}, ActualPositions: []VirtualLot{actual},
		RawLegacyMap:  []LegacySourceMapping{{Strategy: "a", Lot: "legacy-lot", TaskID: 1, IOrderID: 42, ExOrderIDs: []string{"old-entry"}, RawJSON: json.RawMessage(`{"request":{"tag":"legacy"},"infos":{"custom":null}}`)}},
		Checkpoints:   []LegacyCheckpoint{{Strategy: "a", Name: "legacy-state", Payload: json.RawMessage(`{"trailingAnchor":"110","custom":null}`)}},
		VenueSnapshot: LegacyVenueSnapshot{Complete: true, AccountCash: intentPrice("999"), Positions: []VirtualLot{actual}, AtMS: 10},
	}
}

func assertMigrationPendingEmpty(t *testing.T, s *Store) {
	t.Helper()
	checkpoint, err := s.Migration(context.Background(), "migration-1")
	if err != nil || checkpoint.State != "pending" {
		t.Fatal("pending recovery marker missing", checkpoint, err)
	}
	snapshot, err := s.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.RiskFrozen || len(snapshot.Lots) != 0 || len(snapshot.ActualPositions) != 0 || len(snapshot.Orders) != 0 || !snapshot.AccountSettledCash.IsZero() || snapshot.Checkpoint != 0 {
		t.Fatal("failed migration changed destination", snapshot)
	}
}

func TestLegacyMigrationMatchedSnapshotAndRestartIdempotence(t *testing.T) {
	s, _, _, path := testStore(t)
	request := migrationFixture(t)
	// Independently decoded decimal descriptors must compare by value.
	encoded, _ := json.Marshal(request.VenueSnapshot.Positions)
	if err := json.Unmarshal(encoded, &request.VenueSnapshot.Positions); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.transaction(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec("CREATE TABLE legacy_preserved(value TEXT); INSERT INTO legacy_preserved VALUES('keep')")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	applied, err := s.ImportMigration(ctx, request)
	if err != nil || !applied {
		t.Fatal(applied, err)
	}
	snapshot, err := s.Snapshot(ctx)
	if err != nil || snapshot.RiskFrozen || len(snapshot.Lots) != 1 || len(snapshot.ActualPositions) != 1 || !snapshot.AccountSettledCash.Equal(request.AccountCash) {
		t.Fatal(snapshot, err)
	}
	checkpoint, err := s.StrategyCheckpoint(ctx, "a", "legacy-state")
	if err != nil || string(checkpoint) != string(request.Checkpoints[0].Payload) {
		t.Fatal(string(checkpoint), err)
	}
	totals, err := s.StrategyTotals(ctx, "a")
	if err != nil || !totals.Fees.Equal(intentPrice("1")) {
		t.Fatal(totals, err)
	}
	key := s.key
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenStore(path, key)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	request.VenueSnapshot.AtMS = 20
	if applied, err := restarted.ImportMigration(ctx, request); err != nil || applied {
		t.Fatal("restart duplicated migration", applied, err)
	}
	status, err := restarted.Migration(ctx, request.ID)
	if err != nil || status.State != "ready" || status.SchemaVersion != LegacyMigrationSchemaVersion {
		t.Fatal(status, err)
	}
	if err := restarted.transaction(ctx, func(tx *sql.Tx) error {
		var value string
		err := tx.QueryRow("SELECT value FROM legacy_preserved").Scan(&value)
		if value != "keep" {
			t.Fatal(value)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	backup, err := os.ReadFile(request.BackupPath)
	if err != nil || string(backup) != "preserved legacy snapshot" {
		t.Fatal("backup mutated", err)
	}
	request.Lots[0].CostBasis = intentPrice("101")
	if _, err := restarted.ImportMigration(ctx, request); err == nil {
		t.Fatal("source identity reused")
	}
}

func TestLegacyMigrationInternalLotsDoNotRequireExchangeID(t *testing.T) {
	s, _, _, _ := testStore(t)
	request := migrationFixture(t)
	request.RawLegacyMap[0].ExOrderIDs = nil
	short := request.Lots[0]
	short.Strategy, short.ID, short.SignedSteps = "b", "internal-short", -10
	short.Fees = intentPrice("0")
	request.Lots = append(request.Lots, short)
	request.StrategyCash["a"], request.StrategyCash["b"] = intentPrice("499"), intentPrice("500")
	request.RawLegacyMap = append(request.RawLegacyMap, LegacySourceMapping{Strategy: "b", Lot: short.ID, TaskID: 1, IOrderID: 43, RawJSON: json.RawMessage(`{"internal":true}`)})
	request.ActualPositions = nil
	request.VenueSnapshot.Positions = nil
	if applied, err := s.ImportMigration(context.Background(), request); err != nil || !applied {
		t.Fatal(applied, err)
	}
	snapshot, err := s.Snapshot(context.Background())
	if err != nil || len(snapshot.Lots) != 2 || len(snapshot.ActualPositions) != 0 {
		t.Fatal(snapshot, err)
	}
}

func TestLegacyMigrationFailureRetainsPendingAndRollsBack(t *testing.T) {
	for _, failure := range []string{"unmatched-order", "incomplete", "positions", "equity", "mapping", "backup", "backup-digest", "instrument-version", "preflight", "sql"} {
		t.Run(failure, func(t *testing.T) {
			s, _, _, path := testStore(t)
			request := migrationFixture(t)
			switch failure {
			case "unmatched-order":
				request.VenueSnapshot.OpenOrders = []LegacyVenueOrder{{ExchangeID: "manual-order"}}
			case "incomplete":
				request.VenueSnapshot.Complete = false
			case "positions":
				request.VenueSnapshot.Positions = nil
			case "equity":
				request.StrategyCash["a"] = intentPrice("998")
			case "mapping":
				request.RawLegacyMap = nil
			case "backup":
				request.BackupPath = filepath.Join(t.TempDir(), "missing.db")
			case "backup-digest":
				request.BackupSHA256 = string(make([]byte, 64))
			case "instrument-version":
				request.Lots[0].Instrument.Version = "wrong-version"
			case "preflight":
				calls := 0
				request.Preflight = func() error {
					calls++
					if calls == 2 {
						return errors.New("legacy owner resumed")
					}
					return nil
				}
			case "sql":
				if err := s.transaction(context.Background(), func(tx *sql.Tx) error {
					_, err := tx.Exec("CREATE TRIGGER migration_fault BEFORE INSERT ON exec_lot BEGIN SELECT RAISE(ABORT,'migration failure'); END")
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			if applied, err := s.ImportMigration(context.Background(), request); err == nil || applied {
				t.Fatal("invalid migration accepted", applied, err)
			}
			assertMigrationPendingEmpty(t, s)
			if _, err := s.Reconcile(context.Background(), AccountReconciliation{ID: "unsafe-unfreeze", Positions: map[string]int64{}, AtMS: 20}); err == nil {
				t.Fatal("reconcile unfroze pending migration")
			}
			if failure != "sql" && failure != "preflight" {
				return
			}
			key := s.key
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			restarted, err := OpenStore(path, key)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			assertMigrationPendingEmpty(t, restarted)
			if failure == "sql" {
				if err := restarted.transaction(context.Background(), func(tx *sql.Tx) error { _, err := tx.Exec("DROP TRIGGER migration_fault"); return err }); err != nil {
					t.Fatal(err)
				}
			}
			request.Preflight = func() error { return nil }
			if applied, err := restarted.ImportMigration(context.Background(), request); err != nil || !applied {
				t.Fatal("pending retry failed", applied, err)
			}
		})
	}
}

func TestLegacyMigrationConfirmedPartialOrderNeverResubmits(t *testing.T) {
	s, _, h, _ := testStore(t)
	request := migrationFixture(t)
	virtual := EligibleIntent{ID: "legacy-exit-intent", Account: s.key, Strategy: "a", Lot: "legacy-lot", Instrument: "BTC", Side: Sell, Kind: ExitIntent, QuantitySteps: 5, State: Partial, FilledSteps: 2}
	plan := Plan{ID: "legacy-active-plan", Sequence: 0, DecisionMS: 0, ExpiresMS: 100, Intents: []EligibleIntent{virtual}}
	intent := OrderIntent{ID: "legacy-exit", PlanID: plan.ID, Instrument: ledgerInstrument(), Side: Sell, Steps: 5, Allocations: []FillAllocation{{ID: "legacy-exit-allocation", IntentID: virtual.ID, Strategy: "a", Lot: "legacy-lot", Side: Sell, Kind: ExitIntent, Steps: 5}}}
	order := StoredOrder{Intent: intent, ClientID: "legacy-client", ExchangeID: "legacy-exit-exchange", State: OrderPartial, FilledSteps: 2, ReportedCost: intentPrice("20"), ReportedFee: intentPrice("0.1"), AllocationFilled: map[string]int64{"legacy-exit-allocation": 2}}
	request.Plans, request.Orders = []Plan{plan}, []StoredOrder{order}
	request.RawLegacyMap[0].ExOrderIDs = append(request.RawLegacyMap[0].ExOrderIDs, order.ExchangeID)
	request.VenueSnapshot.OpenOrders = []LegacyVenueOrder{{ExchangeID: order.ExchangeID, ClientID: order.ClientID, Instrument: "BTC", Side: Sell, Steps: 5, FilledSteps: 2}}
	request.VenueSnapshot.OpenOrders[0].Cost, request.VenueSnapshot.OpenOrders[0].Fee = order.ReportedCost, order.ReportedFee
	if applied, err := s.ImportMigration(context.Background(), request); err != nil || !applied {
		t.Fatal(applied, err)
	}
	stored, err := s.Order(context.Background(), intent.ID)
	if err != nil || stored.FilledSteps != 2 || stored.AllocationFilled["legacy-exit-allocation"] != 2 || stored.ExchangeID != order.ExchangeID {
		t.Fatal(stored, err)
	}
	adapter := &fakeExecutionAdapter{}
	executor := executorFor(s, h, adapter)
	if err := executor.Send(intent.ID, 20); err == nil {
		t.Fatal("already acknowledged migration was resubmitted")
	}
	if len(adapter.calls()) != 0 {
		t.Fatal("migration sent transport", adapter.calls())
	}
	if applied, err := s.ApplyFill(context.Background(), FillReport{EventID: "legacy-late-snapshot", OrderID: intent.ID, Steps: 2, Price: intentPrice("100"), Cost: intentPrice("20"), Fee: intentPrice("0.1"), Cumulative: true, AtMS: 21}); err != nil || applied {
		t.Fatal("migration highwater replay double-accounted", applied, err)
	}
}
