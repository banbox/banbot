package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/shopspring/decimal"
)

const migrationSchema = `CREATE TABLE IF NOT EXISTS exec_migration(account TEXT NOT NULL,id TEXT NOT NULL,source_hash TEXT NOT NULL,source_version TEXT NOT NULL,schema_version INTEGER NOT NULL,state TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(account,id));
CREATE TABLE IF NOT EXISTS exec_legacy_source(account TEXT NOT NULL,migration_id TEXT NOT NULL,strategy TEXT NOT NULL,lot TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(account,strategy,lot),FOREIGN KEY(account,migration_id) REFERENCES exec_migration(account,id));`

// Projection cursors and strategy snapshots share physical storage, while
// their typed APIs keep highwater/monotonicity and JSON snapshot rules separate.
const checkpointSchema = `CREATE TABLE IF NOT EXISTS exec_checkpoint(
account TEXT NOT NULL,kind TEXT NOT NULL,strategy TEXT NOT NULL,name TEXT NOT NULL,
checkpoint INTEGER,payload TEXT,PRIMARY KEY(account,kind,strategy,name),
CHECK((kind='projection' AND strategy='' AND checkpoint IS NOT NULL AND checkpoint>=0 AND payload IS NULL)
OR (kind='strategy' AND strategy<>'' AND checkpoint IS NULL AND payload IS NOT NULL AND json_valid(payload))));`

func tableExists(transaction any, name string) (bool, error) {
	var tx *sql.Tx
	switch scope := transaction.(type) {
	case *sql.Tx:
		tx = scope
	case *storeTxn:
		if scope.memory != nil {
			return scope.memory.migrationEnabled, nil
		}
		tx = scope.sql
	default:
		return false, errors.New("execution: invalid schema transaction")
	}
	var n int
	err := tx.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", name).Scan(&n)
	return n != 0, err
}

// migrateExecutionSchema verifies redundant v1 mirrors before removing them.
// The enclosing SQLite transaction keeps every original row on any failure.
func migrateExecutionSchema(tx *sql.Tx) error {
	exists, err := tableExists(tx, "exec_schema")
	if err != nil {
		return err
	}
	if exists {
		var version int
		if err := tx.QueryRow("SELECT max(version) FROM exec_schema").Scan(&version); err != nil {
			return err
		}
		if version != 1 && version != 2 && version != 3 && version != 4 {
			return errors.New("execution: unsupported ledger schema version")
		}
		if version == 1 {
			for _, check := range []struct{ name, query string }{
				{"exec_outbox", "SELECT count(*) FROM exec_outbox b LEFT JOIN exec_order o ON o.account=b.account AND o.id=b.order_id WHERE o.id IS NULL OR b.state<>o.state"},
				{"exec_fill", "SELECT count(*) FROM exec_fill f LEFT JOIN exec_event e ON e.account=f.account AND e.id=f.event_id LEFT JOIN exec_order o ON o.account=f.account AND o.id=f.order_id WHERE e.id IS NULL OR o.id IS NULL OR e.kind<>'ExchangeFill'"},
			} {
				present, err := tableExists(tx, check.name)
				if err != nil {
					return err
				}
				if !present {
					continue
				}
				var missing int
				if err := tx.QueryRow(check.query).Scan(&missing); err != nil {
					return err
				}
				if missing != 0 {
					return fmt.Errorf("execution: unverified %s mirror records", check.name)
				}
			}
			if err := verifyLegacyFillMirrors(tx); err != nil {
				return err
			}
			for _, name := range []string{"exec_fill", "exec_outbox"} {
				if _, err := tx.Exec("DROP TABLE IF EXISTS " + name); err != nil {
					return err
				}
			}
		}
		if version < 3 {
			if err := migrateOrderAttempts(tx); err != nil {
				return err
			}
		}
		if version < 4 {
			if err := migrateCheckpoints(tx); err != nil {
				return err
			}
			if _, err := tx.Exec("DELETE FROM exec_schema; INSERT INTO exec_schema VALUES(4)"); err != nil {
				return err
			}
		}
	}
	_, err = tx.Exec(executionSchema)
	return err
}

func verifyLegacyFillMirrors(tx *sql.Tx) error {
	present, err := tableExists(tx, "exec_fill")
	if err != nil || !present {
		return err
	}
	rows, err := tx.Query("SELECT f.account,f.event_id,f.order_id,f.steps,f.fee,e.payload FROM exec_fill f JOIN exec_event e ON e.account=f.account AND e.id=f.event_id")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var account, eventID, orderID, feeBody, body string
		var steps int64
		if err := rows.Scan(&account, &eventID, &orderID, &steps, &feeBody, &body); err != nil {
			return err
		}
		var report FillReport
		if err := json.Unmarshal([]byte(body), &report); err != nil {
			return err
		}
		fee, err := decimal.NewFromString(feeBody)
		if err != nil {
			return err
		}
		if report.EventID != eventID || report.OrderID != orderID || steps < 0 || report.Steps <= 0 {
			return errors.New("execution: legacy fill identity/quantity shadow mismatch")
		}
		if !report.Cumulative && (report.Steps != steps || !report.Fee.Equal(fee)) {
			return errors.New("execution: legacy incremental fill report shadow mismatch")
		}
		// The old mirror holds normalized deltas. Cumulative raw report totals
		// cannot be compared to those deltas; the immutable account posting
		// retains the exact normalization, including zero-step fee corrections.
		postings, err := tx.Query("SELECT quantity,fee FROM exec_ledger WHERE account=? AND event_id=? AND kind IN ('RealAccountFill','RealAccountFeeCorrection')", account, eventID)
		if err != nil {
			return err
		}
		count := 0
		for postings.Next() {
			var quantity int64
			var retainedFee string
			if err := postings.Scan(&quantity, &retainedFee); err != nil {
				postings.Close()
				return err
			}
			value, err := decimal.NewFromString(retainedFee)
			if err != nil {
				postings.Close()
				return err
			}
			if quantity == -1<<63 || absSteps(quantity) != steps || !value.Equal(fee) || report.Cumulative && report.Steps < steps {
				postings.Close()
				return errors.New("execution: legacy normalized fill posting shadow mismatch")
			}
			count++
		}
		err = postings.Err()
		postings.Close()
		if err != nil {
			return err
		}
		if count != 1 {
			return errors.New("execution: legacy fill lacks one exact normalized account posting")
		}
	}
	return rows.Err()
}

// Copy and shadow-verify both typed namespaces before dropping either source.
// A constraint failure or conflicting destination preserves every old row and
// schema version through the caller's transaction rollback.
func migrateCheckpoints(tx *sql.Tx) error {
	if _, err := tx.Exec(checkpointSchema); err != nil {
		return err
	}
	for _, migration := range []struct{ table, copy, verify string }{
		{"exec_projection", "INSERT INTO exec_checkpoint(account,kind,strategy,name,checkpoint) SELECT account,'projection','',name,checkpoint FROM exec_projection WHERE true ON CONFLICT(account,kind,strategy,name) DO NOTHING",
			"SELECT count(*) FROM exec_projection old LEFT JOIN exec_checkpoint new ON new.account=old.account AND new.kind='projection' AND new.strategy='' AND new.name=old.name WHERE new.name IS NULL OR new.checkpoint IS NOT old.checkpoint OR new.payload IS NOT NULL"},
		{"exec_strategy_checkpoint", "INSERT INTO exec_checkpoint(account,kind,strategy,name,payload) SELECT account,'strategy',strategy,name,payload FROM exec_strategy_checkpoint WHERE true ON CONFLICT(account,kind,strategy,name) DO NOTHING",
			"SELECT count(*) FROM exec_strategy_checkpoint old LEFT JOIN exec_checkpoint new ON new.account=old.account AND new.kind='strategy' AND new.strategy=old.strategy AND new.name=old.name WHERE new.name IS NULL OR new.payload IS NOT old.payload OR new.checkpoint IS NOT NULL"},
	} {
		present, err := tableExists(tx, migration.table)
		if err != nil {
			return err
		}
		if !present {
			continue
		}
		if _, err := tx.Exec(migration.copy); err != nil {
			return fmt.Errorf("execution: migrate %s: %w", migration.table, err)
		}
		var mismatches int
		if err := tx.QueryRow(migration.verify).Scan(&mismatches); err != nil {
			return err
		}
		if mismatches != 0 {
			return fmt.Errorf("execution: checkpoint migration shadow mismatch in %s", migration.table)
		}
	}
	for _, table := range []string{"exec_projection", "exec_strategy_checkpoint"} {
		if _, err := tx.Exec("DROP TABLE IF EXISTS " + table); err != nil {
			return err
		}
	}
	return nil
}

// migrateOrderAttempts shadow-verifies every retained legacy attempt as a
// typed event before removing the table. Its original kind, generation,
// submission time and last result survive; no missing history is invented.
func migrateOrderAttempts(tx *sql.Tx) error {
	present, err := tableExists(tx, "exec_attempt")
	if err != nil || !present {
		return err
	}
	rows, err := tx.Query("SELECT account,order_id,number,kind,generation,at_ms,result FROM exec_attempt ORDER BY account,order_id,number")
	if err != nil {
		return err
	}
	type legacyAttempt struct {
		account string
		attempt OrderAttempt
	}
	var retained []legacyAttempt
	for rows.Next() {
		var record legacyAttempt
		var generation string
		if err := rows.Scan(&record.account, &record.attempt.OrderID, &record.attempt.Number, &record.attempt.Kind, &generation, &record.attempt.AtMS, &record.attempt.Result); err != nil {
			rows.Close()
			return err
		}
		record.attempt.Generation, err = strconv.ParseUint(generation, 10, 64)
		if err != nil {
			rows.Close()
			return err
		}
		record.attempt.Phase = AttemptImported
		retained = append(retained, record)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	boundary := &storeTxn{sql: tx, ctx: context.Background()}
	for _, record := range retained {
		store := &Store{accountID: record.account}
		order, err := store.readOrder(boundary.ctx, boundary, record.attempt.OrderID)
		if err != nil {
			return err
		}
		if record.attempt.Number > order.Attempt {
			return errors.New("execution: attempt exceeds order highwater")
		}
		if err := store.recordOrderAttempt(boundary, record.attempt); err != nil {
			return err
		}
	}
	// Re-read serialized typed facts inside the same transaction before DROP.
	for _, record := range retained {
		store := &Store{accountID: record.account}
		copied, err := store.readAttempt(boundary, record.attempt.OrderID, record.attempt.Number, AttemptImported)
		if err != nil {
			return err
		}
		if copied != record.attempt {
			return errors.New("execution: order attempt migration shadow mismatch")
		}
	}
	_, err = tx.Exec("DROP TABLE exec_attempt")
	return err
}

func ensureMigrationSchema(tx *storeTxn) error {
	if tx.memory != nil {
		previous := tx.memory.migrationEnabled
		tx.undo = append(tx.undo, func() { tx.memory.migrationEnabled = previous })
		tx.memory.migrationEnabled = true
		return nil
	}
	_, err := tx.sql.ExecContext(tx.ctx, migrationSchema)
	return err
}
