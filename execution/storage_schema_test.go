package execution

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestStoreVersionedMirrorMigrationPreservesRecovery(t *testing.T) {
	s, _, h, path := testStore(t)
	o := planOrder(t, s, "upgrade", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	e := executorFor(s, h, &fakeExecutionAdapter{})
	if err := e.Send(o.ID, 11); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyFill(context.Background(), FillReport{EventID: "upgrade-fill", OrderID: o.ID, Steps: 3, Price: intentPrice("100"), AtMS: 12}); err != nil {
		t.Fatal(err)
	}
	before, err := s.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS exec_outbox(account TEXT,order_id TEXT,state TEXT); CREATE TABLE IF NOT EXISTS exec_fill(account TEXT,event_id TEXT,order_id TEXT,steps INTEGER,fee TEXT); DELETE FROM exec_schema; INSERT INTO exec_schema VALUES(1);`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("INSERT OR REPLACE INTO exec_outbox VALUES(?,?,?)", s.accountID, o.ID, string(OrderPartial)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("INSERT OR REPLACE INTO exec_fill VALUES(?,?,?,?,?)", s.accountID, "upgrade-fill", o.ID, 3, "0"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path, s.key)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := reopened.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	a, _ := payload(before)
	b, _ := payload(after)
	if a != b {
		t.Fatalf("upgrade changed recovery state: %s / %s", a, b)
	}
	for _, name := range []string{"exec_outbox", "exec_fill", "exec_attempt", "exec_projection", "exec_strategy_checkpoint", "exec_migration", "exec_legacy_source"} {
		var count int
		if err := reopened.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", name).Scan(&count); err != nil || count != 0 {
			t.Fatalf("unexpected table %s: %d %v", name, count, err)
		}
	}
	var version int
	if err := reopened.db.QueryRow("SELECT max(version) FROM exec_schema").Scan(&version); err != nil || version != 4 {
		t.Fatal(version, err)
	}
}

func TestStoreMirrorMigrationRejectsUnverifiedRows(t *testing.T) {
	key := testIntent(Buy).Account
	path := filepath.Join(t.TempDir(), "invalid.db")
	s, err := OpenStore(path, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS exec_outbox(account TEXT,order_id TEXT,state TEXT); DELETE FROM exec_schema; INSERT INTO exec_schema VALUES(1);`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("INSERT INTO exec_outbox VALUES(?,?,?)", s.accountID, "missing", "Unknown"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if reopened, err := OpenStore(path, key); err == nil {
		reopened.Close()
		t.Fatal("unverified mirror was dropped")
	}
	// Verification failure must roll back every schema change, including DROP.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow("SELECT count(*) FROM exec_outbox").Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	var version int
	if err := db.QueryRow("SELECT max(version) FROM exec_schema").Scan(&version); err != nil || version != 1 {
		t.Fatal(version, err)
	}
}

func TestCheckpointV3MigrationPreservesTypedNamespaces(t *testing.T) {
	s, _, _, path := testStore(t)
	ctx := context.Background()
	fundStrategies(t, s)
	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := []byte(`{"nullable":null,"custom":[1,"x"]}`)
	if err := s.SaveStrategyCheckpoint(ctx, "a", "same", checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := s.AdvanceProjection(ctx, "same", snapshot.Checkpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TABLE exec_projection(account TEXT NOT NULL,name TEXT NOT NULL,checkpoint INTEGER NOT NULL,PRIMARY KEY(account,name));
CREATE TABLE exec_strategy_checkpoint(account TEXT NOT NULL,strategy TEXT NOT NULL,name TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(account,strategy,name));
INSERT INTO exec_projection SELECT account,name,checkpoint FROM exec_checkpoint WHERE kind='projection';
INSERT INTO exec_strategy_checkpoint SELECT account,strategy,name,payload FROM exec_checkpoint WHERE kind='strategy';
DROP TABLE exec_checkpoint; DELETE FROM exec_schema; INSERT INTO exec_schema VALUES(3);`); err != nil {
		t.Fatal(err)
	}
	s.Close()
	reopened, err := OpenStore(path, s.key)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.StrategyCheckpoint(ctx, "a", "same")
	if err != nil || string(got) != string(checkpoint) {
		t.Fatal("snapshot changed", string(got), err)
	}
	cursor, err := reopened.ProjectionCursor(ctx, "same")
	if err != nil || cursor != snapshot.Checkpoint {
		t.Fatal("cursor changed", cursor, err)
	}
	for _, table := range []string{"exec_projection", "exec_strategy_checkpoint"} {
		var count int
		if err := reopened.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name=?", table).Scan(&count); err != nil || count != 0 {
			t.Fatal(table, count, err)
		}
	}
	for _, statement := range []string{
		"INSERT INTO exec_checkpoint VALUES('a','projection','strategy','bad',1,NULL)",
		"INSERT INTO exec_checkpoint VALUES('a','projection','','bad',1,'{}')",
		"INSERT INTO exec_checkpoint VALUES('a','strategy','','bad',NULL,'{}')",
		"INSERT INTO exec_checkpoint VALUES('a','strategy','a','bad',1,'{}')",
		"INSERT INTO exec_checkpoint VALUES('a','strategy','a','bad',NULL,'invalid')",
	} {
		if _, err := reopened.db.Exec(statement); err == nil {
			t.Fatal("typed checkpoint constraint missing", statement)
		}
	}
}

func TestCheckpointMigrationVerifiesBeforeDroppingAndRollsBack(t *testing.T) {
	for _, scenario := range []string{"invalid-json", "shadow-conflict"} {
		t.Run(scenario, func(t *testing.T) {
			s, _, _, path := testStore(t)
			if _, err := s.db.Exec(`CREATE TABLE exec_projection(account TEXT,name TEXT,checkpoint INTEGER);
CREATE TABLE exec_strategy_checkpoint(account TEXT,strategy TEXT,name TEXT,payload TEXT);
INSERT INTO exec_projection VALUES('account','cursor',1);
INSERT INTO exec_strategy_checkpoint VALUES('account','strategy','snapshot','{}');
DELETE FROM exec_schema; INSERT INTO exec_schema VALUES(3);`); err != nil {
				t.Fatal(err)
			}
			if scenario == "invalid-json" {
				if _, err := s.db.Exec("UPDATE exec_strategy_checkpoint SET payload='invalid'; DROP TABLE exec_checkpoint"); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := s.db.Exec("INSERT INTO exec_checkpoint VALUES('account','strategy','strategy','snapshot',NULL,'{\"different\":true}')"); err != nil {
					t.Fatal(err)
				}
			}
			s.Close()
			if reopened, err := OpenStore(path, s.key); err == nil {
				reopened.Close()
				t.Fatal("unverified checkpoint sources were dropped")
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, table := range []string{"exec_projection", "exec_strategy_checkpoint"} {
				var count int
				if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 1 {
					t.Fatal("original checkpoint lost", table, count, err)
				}
			}
			var version int
			if err := db.QueryRow("SELECT max(version) FROM exec_schema").Scan(&version); err != nil || version != 3 {
				t.Fatal(version, err)
			}
			var tables int
			if err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='exec_checkpoint'").Scan(&tables); err != nil {
				t.Fatal(err)
			}
			if scenario == "invalid-json" && tables != 0 {
				t.Fatal("rollback retained a partially migrated table")
			}
			if scenario == "shadow-conflict" {
				var projections int
				if err := db.QueryRow("SELECT count(*) FROM exec_checkpoint WHERE kind='projection'").Scan(&projections); err != nil || projections != 0 {
					t.Fatal("rollback retained copied cursor", projections, err)
				}
			}
		})
	}
}

func TestLegacyFillMirrorRequiresExactIdentityAndNormalizedFacts(t *testing.T) {
	for _, scenario := range []string{"valid-cumulative", "valid-fee-correction", "wrong-order", "wrong-steps", "wrong-fee", "raw-cumulative-total"} {
		t.Run(scenario, func(t *testing.T) {
			s, _, owner, path := testStore(t)
			ctx := context.Background()
			order := planOrder(t, s, "mirror", 1, Buy, EntryIntent, map[string]int64{"a": 10})
			if err := executorFor(s, owner, &fakeExecutionAdapter{}).Send(order.ID, 11); err != nil {
				t.Fatal(err)
			}
			for _, fill := range []FillReport{{EventID: "first", OrderID: order.ID, Steps: 1, Fee: intentPrice("0.1"), Price: intentPrice("100"), Cumulative: true, AtMS: 12}, {EventID: "second", OrderID: order.ID, Steps: 3, Fee: intentPrice("0.3"), Price: intentPrice("100"), Cumulative: true, AtMS: 13}} {
				if _, err := s.ApplyFill(ctx, fill); err != nil {
					t.Fatal(err)
				}
			}
			eventID, orderID, steps, fee := "second", order.ID, int64(2), "0.2"
			switch scenario {
			case "valid-fee-correction":
				eventID, steps, fee = "correction", 0, "0.1"
				if _, err := s.ApplyFill(ctx, FillReport{EventID: eventID, OrderID: order.ID, Steps: 3, Fee: intentPrice("0.4"), Price: intentPrice("100"), Cumulative: true, AtMS: 14}); err != nil {
					t.Fatal(err)
				}
			case "wrong-order":
				other := planOrder(t, s, "other-order", 2, Buy, EntryIntent, map[string]int64{"b": 1})
				orderID = other.ID
			case "wrong-steps":
				steps = 4
			case "wrong-fee":
				fee = "0.21"
			case "raw-cumulative-total":
				steps, fee = 3, "0.3"
			}
			if _, err := s.db.Exec("CREATE TABLE exec_fill(account TEXT,event_id TEXT,order_id TEXT,steps INTEGER,fee TEXT); DELETE FROM exec_schema; INSERT INTO exec_schema VALUES(1)"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec("INSERT INTO exec_fill VALUES(?,?,?,?,?)", s.accountID, eventID, orderID, steps, fee); err != nil {
				t.Fatal(err)
			}
			s.Close()
			reopened, err := OpenStore(path, s.key)
			if scenario == "valid-cumulative" || scenario == "valid-fee-correction" {
				if err != nil {
					t.Fatal("valid normalized cumulative mirror rejected", err)
				}
				reopened.Close()
				return
			}
			if err == nil {
				reopened.Close()
				t.Fatal("contradictory fill mirror dropped", scenario)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var count, version int
			if err := db.QueryRow("SELECT count(*) FROM exec_fill").Scan(&count); err != nil || count != 1 {
				t.Fatal("failed shadow discarded original", count, err)
			}
			if err := db.QueryRow("SELECT max(version) FROM exec_schema").Scan(&version); err != nil || version != 1 {
				t.Fatal("failed shadow advanced schema", version, err)
			}
		})
	}
}
