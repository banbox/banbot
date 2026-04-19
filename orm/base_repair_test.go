package orm

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type repairDBStub struct {
	execErr error
	execSQL []string
}

func (s *repairDBStub) Exec(_ context.Context, sql string, _ ...interface{}) (pgconn.CommandTag, error) {
	s.execSQL = append(s.execSQL, sql)
	return pgconn.CommandTag{}, s.execErr
}

func (s *repairDBStub) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	panic("unexpected Query call")
}

func (s *repairDBStub) QueryRow(context.Context, string, ...interface{}) pgx.Row {
	panic("unexpected QueryRow call")
}

func (s *repairDBStub) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	panic("unexpected CopyFrom call")
}

func TestParseQuestDBMissingPartitionRepair(t *testing.T) {
	err := errors.New("pq: ERROR: Partition `2026-04-15` does not exist in table 'ins_kline_q' directory. Run [ALTER TABLE ins_kline_q FORCE DROP PARTITION LIST '2026-04-15'] to repair the table or the database from the backup.; where='from'")
	repair := parseQuestDBMissingPartitionRepair(err)
	if repair == nil {
		t.Fatal("expected repair suggestion")
	}
	if repair.Table != "ins_kline_q" {
		t.Fatalf("table mismatch: %s", repair.Table)
	}
	if repair.Partition != "2026-04-15" {
		t.Fatalf("partition mismatch: %s", repair.Partition)
	}
	if repair.SQL() != "ALTER TABLE ins_kline_q FORCE DROP PARTITION LIST '2026-04-15'" {
		t.Fatalf("sql mismatch: %s", repair.SQL())
	}
}

func TestParseQuestDBMissingPartitionRepairRejectsMismatch(t *testing.T) {
	err := errors.New("pq: ERROR: Partition '2026-04-15' does not exist in table 'ins_kline_q' directory. Run [ALTER TABLE sranges_q FORCE DROP PARTITION LIST '2026-04-15'] to repair the table")
	if repair := parseQuestDBMissingPartitionRepair(err); repair != nil {
		t.Fatalf("unexpected repair parsed: %+v", repair)
	}
}

func TestTryRepairQuestDBMissingPartitionExecutesRepair(t *testing.T) {
	oldQuestDB := IsQuestDB
	IsQuestDB = true
	defer func() {
		IsQuestDB = oldQuestDB
	}()

	db := &repairDBStub{}
	err := errors.New("pq: ERROR: Partition '2026-04-15' does not exist in table 'ins_kline_q' directory. Run [ALTER TABLE ins_kline_q FORCE DROP PARTITION LIST '2026-04-15'] to repair the table")
	repaired, repairErr := tryRepairQuestDBMissingPartition(context.Background(), db, err, "unit-test")
	if repairErr != nil {
		t.Fatalf("unexpected repair error: %v", repairErr)
	}
	if !repaired {
		t.Fatal("expected repair to run")
	}
	if len(db.execSQL) != 1 {
		t.Fatalf("expected one repair statement, got %d", len(db.execSQL))
	}
	if db.execSQL[0] != "ALTER TABLE ins_kline_q FORCE DROP PARTITION LIST '2026-04-15'" {
		t.Fatalf("repair sql mismatch: %s", db.execSQL[0])
	}
}

func TestTryRepairQuestDBMissingPartitionIgnoresOtherErrors(t *testing.T) {
	oldQuestDB := IsQuestDB
	IsQuestDB = true
	defer func() {
		IsQuestDB = oldQuestDB
	}()

	db := &repairDBStub{}
	repaired, repairErr := tryRepairQuestDBMissingPartition(context.Background(), db, errors.New("random questdb error"), "unit-test")
	if repairErr != nil {
		t.Fatalf("unexpected repair error: %v", repairErr)
	}
	if repaired {
		t.Fatal("unexpected repair for unrelated error")
	}
	if len(db.execSQL) != 0 {
		t.Fatalf("unexpected repair sql: %v", db.execSQL)
	}
}
