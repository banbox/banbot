package orm

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestKLineSeriesStoreRejectsReservedColumns(t *testing.T) {
	info := NewKLineSeriesInfo("custom_metric", "1h", []SeriesField{{Name: "open", Type: "float"}})
	if err := validateKLineSeriesInfo(info); err == nil || !strings.Contains(err.Short(), "conflicts with an existing table column") {
		t.Fatalf("expected reserved column error, got %v", err)
	}
}

func TestKLineSeriesStoreIgnoresQuestDBDuplicateColumn(t *testing.T) {
	oldQuest := IsQuestDB
	t.Cleanup(func() { IsQuestDB = oldQuest })

	duplicate := errors.New("duplicate column [name=open_interest]")
	IsQuestDB = true
	if !shouldIgnoreKLineSeriesAddColumnError(duplicate) {
		t.Fatal("expected QuestDB duplicate column error to be ignored")
	}
	if shouldIgnoreKLineSeriesAddColumnError(errors.New("permission denied")) {
		t.Fatal("expected unrelated QuestDB error to be returned")
	}
	IsQuestDB = false
	if shouldIgnoreKLineSeriesAddColumnError(duplicate) {
		t.Fatal("expected PostgreSQL duplicate column error to be returned")
	}
}

func TestKLineSeriesStoreBuildsBackendSQL(t *testing.T) {
	oldQuest := IsQuestDB
	t.Cleanup(func() { IsQuestDB = oldQuest })

	field := SeriesField{Name: "metric_value", Type: "float"}
	IsQuestDB = true
	if got := KLineSeriesTimeColumn(); got != "ts" {
		t.Fatalf("expected QuestDB time column ts, got %q", got)
	}
	addSQL := buildKLineSeriesAddColumnSQL("kline_1h", field)
	if !strings.Contains(addSQL, `ALTER TABLE "kline_1h" ADD COLUMN IF NOT EXISTS "metric_value" DOUBLE`) {
		t.Fatalf("unexpected QuestDB add column SQL: %s", addSQL)
	}
	updateSQL := buildKLineSeriesUpdateSQL(SeriesBinding{Table: "kline_1h", TimeColumn: "ts", SIDColumn: "sid", Fields: []SeriesField{field}})
	if updateSQL != `UPDATE "kline_1h" SET "metric_value" = $1 WHERE "sid" = $2 AND "ts" = $3` {
		t.Fatalf("unexpected QuestDB update SQL: %s", updateSQL)
	}

	IsQuestDB = false
	if got := KLineSeriesTimeColumn(); got != "time" {
		t.Fatalf("expected TimescaleDB time column time, got %q", got)
	}
	addSQL = buildKLineSeriesAddColumnSQL("kline_1h", field)
	if !strings.Contains(addSQL, `ALTER TABLE "kline_1h" ADD COLUMN IF NOT EXISTS "metric_value" DOUBLE PRECISION`) {
		t.Fatalf("unexpected TimescaleDB add column SQL: %s", addSQL)
	}
	updateSQL = buildKLineSeriesUpdateSQL(SeriesBinding{Table: "kline_1h", TimeColumn: "time", SIDColumn: "sid", Fields: []SeriesField{field}})
	if updateSQL != `UPDATE "kline_1h" SET "metric_value" = $1 WHERE "sid" = $2 AND "time" = $3` {
		t.Fatalf("unexpected TimescaleDB update SQL: %s", updateSQL)
	}
}

func TestNormalizeKLineSeriesRowsFillsSidAndSorts(t *testing.T) {
	rows, err := normalizeKLineSeriesRows(7, []SeriesField{{Name: "metric_value", Type: "float"}}, []*DataRecord{
		{TimeMS: 200, Values: map[string]any{"metric_value": 2.0}},
		{TimeMS: 100, Values: map[string]any{"metric_value": 1.0}},
	})
	if err != nil {
		t.Fatalf("normalizeKLineSeriesRows failed: %v", err)
	}
	if len(rows) != 2 || rows[0].Sid != 7 || rows[0].TimeMS != 100 || rows[1].TimeMS != 200 {
		t.Fatalf("unexpected normalized rows: %+v", rows)
	}
}

func TestScanKLineSeriesRecordOmitsNullFields(t *testing.T) {
	rec, err := scanKLineSeriesRecord(rowScannerFunc(func(dest ...any) error {
		*(dest[0].(*int32)) = 7
		*(dest[1].(*int64)) = 100
		*(dest[2].(*sql.NullFloat64)) = sql.NullFloat64{}
		*(dest[3].(*sql.NullFloat64)) = sql.NullFloat64{Float64: 2.5, Valid: true}
		return nil
	}), []SeriesField{
		{Name: "metric_a", Type: "float"},
		{Name: "metric_b", Type: "float"},
	})
	if err != nil {
		t.Fatalf("scanKLineSeriesRecord failed: %v", err)
	}
	if _, ok := rec.Values["metric_a"]; ok {
		t.Fatalf("expected null field to be omitted, got %+v", rec.Values)
	}
	if got := rec.Values["metric_b"]; got != 2.5 {
		t.Fatalf("expected valid field to be scanned, got %+v", rec.Values)
	}
}

type rowScannerFunc func(dest ...any) error

func (f rowScannerFunc) Scan(dest ...any) error {
	return f(dest...)
}
