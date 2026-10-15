package orm

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestQuestDuplicateBarTimestampColumnIsRetryable(t *testing.T) {
	if !isQuestDuplicateColumnErr(errors.New("column 'bar_ts' already exists")) {
		t.Fatal("bar_ts ADD interrupted before version marker cannot be retried")
	}
	if isQuestDuplicateColumnErr(errors.New("column 'bar_ts' not found")) || isQuestDuplicateColumnErr(errors.New("table already exists")) {
		t.Fatal("unrelated schema failure misclassified as duplicate column")
	}
}

type metadataMigrationDB struct {
	barColumn, exists, failMarker bool
	addCount                      int
	versions                      []int64
	ddlErr                        error
}

func (db *metadataMigrationDB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.HasPrefix(sql, "CREATE TABLE IF NOT EXISTS kline_un_q") && !db.exists {
		db.exists = true
		db.barColumn = strings.Contains(sql, "bar_ts")
	}
	if strings.HasPrefix(sql, "ALTER TABLE kline_un_q ADD COLUMN bar_ts") {
		db.addCount++
		if db.ddlErr != nil {
			return pgconn.CommandTag{}, db.ddlErr
		}
		if db.barColumn {
			return pgconn.CommandTag{}, errors.New("column 'bar_ts' already exists")
		}
		db.barColumn = true
	}
	if strings.HasPrefix(sql, "insert into schema_migrations") {
		if db.failMarker {
			db.failMarker = false
			return pgconn.CommandTag{}, errors.New("interrupted before migration marker")
		}
		db.versions = append(db.versions, args[0].(int64))
	}
	return pgconn.CommandTag{}, nil
}

func TestQuestMetadataMigrationFreshAndUpgrade(t *testing.T) {
	for _, start := range []int64{0, 4} {
		db := &metadataMigrationDB{exists: start == 4}
		got, err := applyQuestDBMigrations(context.Background(), db, ddlQdbMigrations, start)
		if err != nil || got != 5 || !db.barColumn || db.addCount != 1 {
			t.Fatalf("migration from %d: version=%d bar_ts=%v adds=%d err=%v", start, got, db.barColumn, db.addCount, err)
		}
	}
}

func TestQuestMetadataMigrationResumesAfterAddBeforeVersionMarker(t *testing.T) {
	db := &metadataMigrationDB{exists: true, failMarker: true}
	got, err := applyQuestDBMigrations(context.Background(), db, ddlQdbMigrations, 4)
	if err == nil || got != 4 || !db.barColumn || len(db.versions) != 0 {
		t.Fatalf("interrupted ADD: version=%d column=%v versions=%v err=%v", got, db.barColumn, db.versions, err)
	}
	got, err = applyQuestDBMigrations(context.Background(), db, ddlQdbMigrations, 4)
	if err != nil || got != 5 || db.addCount != 2 || len(db.versions) != 1 {
		t.Fatalf("retry ADD: version=%d adds=%d versions=%v err=%v", got, db.addCount, db.versions, err)
	}
}

func TestQuestMetadataMigrationDoesNotRecordFailedDDL(t *testing.T) {
	want := errors.New("column 'bar_ts' cannot be added: permission denied")
	db := &metadataMigrationDB{exists: true, ddlErr: want}
	got, err := applyQuestDBMigrations(context.Background(), db, ddlQdbMigrations, 4)
	if !errors.Is(err, want) || got != 4 || db.barColumn || len(db.versions) != 0 {
		t.Fatalf("failed ADD accepted: version=%d column=%v versions=%v err=%v", got, db.barColumn, db.versions, err)
	}
}

func TestQuestMetadataEnsureCreatesCurrentSchemaAfterTableLoss(t *testing.T) {
	db := &metadataMigrationDB{}
	got, err := applyQuestDBMigrations(context.Background(), db, ddlQdbMigrations, 5)
	if err != nil || got != 5 || db.exists {
		t.Fatalf("already applied migration replayed: version=%d exists=%v err=%v", got, db.exists, err)
	}
	if err := ensureQuestDBCreateTables(context.Background(), db, ddlQdbMigrations); err != nil {
		t.Fatal(err)
	}
	if !db.exists || !db.barColumn || db.addCount != 0 {
		t.Fatalf("missing table restored without current schema: %+v", db)
	}
}

func TestQuestMetadataMigrationContinuesAfterDuplicateAndRetainsLaterFailure(t *testing.T) {
	ddl := "-- version 6\nALTER TABLE kline_un_q ADD COLUMN bar_ts TIMESTAMP;\nUPDATE metadata_probe SET ready = true;"
	for _, failLater := range []bool{false, true} {
		laterCalled, markerWritten := false, false
		want := errors.New("later migration step failed")
		db := &rewriteExecStub{exec: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
			if strings.HasPrefix(sql, "ALTER TABLE") {
				return pgconn.CommandTag{}, errors.New("column 'bar_ts' already exists")
			}
			if strings.HasPrefix(sql, "UPDATE metadata_probe") {
				laterCalled = true
				if failLater {
					return pgconn.CommandTag{}, want
				}
			}
			if strings.HasPrefix(sql, "insert into schema_migrations") {
				markerWritten = true
			}
			return pgconn.CommandTag{}, nil
		}}
		got, err := applyQuestDBMigrations(context.Background(), db, ddl, 5)
		if !laterCalled || markerWritten == failLater || (failLater && (!errors.Is(err, want) || got != 5)) || (!failLater && (err != nil || got != 6)) {
			t.Fatalf("later step skipped/marked despite failure: fail=%v called=%v marker=%v version=%d err=%v", failLater, laterCalled, markerWritten, got, err)
		}
	}
}

func TestQuestMetadataMigrationRejectsNonAddDuplicateError(t *testing.T) {
	want := errors.New("duplicate column")
	db := &rewriteExecStub{exec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, want
	}}
	got, err := applyQuestDBMigrations(context.Background(), db, "-- version 6\nSELECT 1;", 5)
	if got != 5 || !errors.Is(err, want) {
		t.Fatalf("non-ADD duplicate error ignored: version=%d err=%v", got, err)
	}
}
