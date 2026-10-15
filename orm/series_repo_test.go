package orm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/internal/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type contextWithoutValues struct {
	context.Context
}

func (contextWithoutValues) Value(any) any {
	panic("series table locks must not read context values")
}

func TestQuestSeriesReadLockUsesCancellationWithoutContextValues(t *testing.T) {
	withFreshCompactState(t)
	oldQuest := IsQuestDB
	oldRootFn := compactProcessLockRootFn
	IsQuestDB = true
	root := t.TempDir()
	compactProcessLockRootFn = func() string { return root }
	t.Cleanup(func() {
		IsQuestDB = oldQuest
		compactProcessLockRootFn = oldRootFn
	})

	releaseExclusive, acquired, err := tryAcquireCompactProcessExclusiveLock(root, "series_context_test")
	if err != nil || !acquired {
		t.Fatalf("acquire exclusive process lock: acquired=%v err=%v", acquired, err)
	}
	defer func() {
		if err := releaseExclusive(); err != nil {
			t.Errorf("release exclusive process lock: %v", err)
		}
	}()

	base, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = acquireQuestTableReadLock(contextWithoutValues{Context: base}, "series_context_test")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("read lock error = %v, want context.Canceled", err)
	}
}

func TestSeriesRepoLockedInsertDoesNotReacquireTableReadLock(t *testing.T) {
	withFreshCompactState(t)
	oldQuest := IsQuestDB
	oldRootFn := compactProcessLockRootFn
	IsQuestDB = true
	root := t.TempDir()
	compactProcessLockRootFn = func() string { return root }
	t.Cleanup(func() {
		IsQuestDB = oldQuest
		compactProcessLockRootFn = oldRootFn
	})

	info, sid := newSeriesRepoTestInfo("series_repo_reentry")
	row := &DataRecord{
		Sid: sid, TimeMS: 1_700_000_000_000, EndMS: 1_700_000_086_400_000,
		Values: map[string]any{"value": 1.0},
	}
	tableLock := cptState.getTableLock(info.Binding.Table)
	tableLock.RLock()
	released := false
	t.Cleanup(func() {
		if !released {
			tableLock.RUnlock()
		}
	})

	writerStarted := make(chan struct{})
	writerAcquired := make(chan struct{})
	go func() {
		close(writerStarted)
		tableLock.Lock()
		close(writerAcquired)
		tableLock.Unlock()
	}()
	<-writerStarted
	time.Sleep(10 * time.Millisecond)

	var execs, queryRows int
	db := &visibilityDBStub{
		exec: func(_ string, _ ...interface{}) (pgconn.CommandTag, error) {
			execs++
			return pgconn.CommandTag{}, nil
		},
		queryRow: func(_ string, _ ...interface{}) pgx.Row {
			queryRows++
			if queryRows == 1 {
				return scriptedRow{values: []any{int64(1), int64(1)}}
			}
			return visibilityRowStub{scan: func(dest ...interface{}) error {
				*dest[0].(*int32) = row.Sid
				*dest[1].(*int64) = row.TimeMS
				*dest[2].(*int64) = row.EndMS
				*dest[3].(*sql.NullFloat64) = sql.NullFloat64{Float64: 1, Valid: true}
				return nil
			}}
		},
	}
	done := make(chan bool, 1)
	go func() {
		err := (&dbSeriesRepo{}).insertSeriesBatchLocked(context.Background(), New(db), info, []*DataRecord{row})
		done <- err == nil
	}()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("locked series insert failed")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("locked series insert blocked behind its own queued writer")
	}
	if execs != 1 {
		t.Fatalf("expected one insert statement, got %d", execs)
	}

	tableLock.RUnlock()
	released = true
	select {
	case <-writerAcquired:
	case <-time.After(time.Second):
		t.Fatal("queued table writer did not acquire after read lock release")
	}
}

func TestQuestSeriesReadLockWaitsForQueuedTableWriter(t *testing.T) {
	withFreshCompactState(t)
	oldQuest := IsQuestDB
	oldRootFn := compactProcessLockRootFn
	IsQuestDB = true
	root := t.TempDir()
	compactProcessLockRootFn = func() string { return root }
	t.Cleanup(func() {
		IsQuestDB = oldQuest
		compactProcessLockRootFn = oldRootFn
	})

	const table = "series_repo_concurrent_lock"
	tableLock := cptState.getTableLock(table)
	tableLock.RLock()
	released := false
	t.Cleanup(func() {
		if !released {
			tableLock.RUnlock()
		}
	})

	writerStarted := make(chan struct{})
	writerAcquired := make(chan struct{})
	go func() {
		close(writerStarted)
		tableLock.Lock()
		close(writerAcquired)
		tableLock.Unlock()
	}()
	<-writerStarted
	time.Sleep(10 * time.Millisecond)

	type readLockResult struct {
		unlock func()
		err    error
	}
	done := make(chan readLockResult, 1)
	go func() {
		unlock, err := acquireQuestTableReadLock(context.Background(), table)
		done <- readLockResult{unlock: unlock, err: err}
	}()
	select {
	case <-done:
		t.Fatal("series read lock acquired while table writer was queued")
	case <-time.After(50 * time.Millisecond):
	}

	tableLock.RUnlock()
	released = true
	select {
	case <-writerAcquired:
	case <-time.After(time.Second):
		t.Fatal("queued table writer did not acquire after read lock release")
	}
	result := <-done
	if result.err != nil {
		t.Fatalf("series read lock failed after writer release: %v", result.err)
	}
	result.unlock()
}

func TestWaitForQuestSeriesCoverageDeletedClassifiesNoRowsAndHardErrors(t *testing.T) {
	oldPoll := questReadAfterWritePollInterval
	questReadAfterWritePollInterval = time.Millisecond
	t.Cleanup(func() { questReadAfterWritePollInterval = oldPoll })

	info := NewSeriesInfo("macro", "1m", []SeriesField{{Name: "value", Type: "float"}})
	const (
		sid   = int32(7)
		start = int64(100)
		stop  = int64(200)
	)

	t.Run("no rows means not yet visible", func(t *testing.T) {
		calls := 0
		db := &visibilityDBStub{query: func(_ string, _ ...interface{}) (pgx.Rows, error) {
			calls++
			switch calls {
			case 1:
				return nil, pgx.ErrNoRows
			case 2:
				return &staleCoveredRows{idx: -1, startMS: start, stopMS: stop, knownEmpty: true}, nil
			default:
				return newInterfaceRows(nil), nil
			}
		}}
		if err := waitForQuestSeriesCoverageDeletedWithQueries(context.Background(), New(db), info, sid, start, stop); err != nil {
			t.Fatalf("no-row visibility wait: %v", err)
		}
		if calls != 3 {
			t.Fatalf("no-row visibility checks = %d, want 3", calls)
		}
	})

	t.Run("marker hard error is not retried", func(t *testing.T) {
		wantErr := errors.New("sranges marker query failed")
		calls := 0
		db := &visibilityDBStub{query: func(_ string, _ ...interface{}) (pgx.Rows, error) {
			calls++
			return nil, wantErr
		}}
		err := waitForQuestSeriesCoverageDeletedWithQueries(context.Background(), New(db), info, sid, start, stop)
		if !errors.Is(err, wantErr) {
			t.Fatalf("marker visibility error = %v, want %v", err, wantErr)
		}
		if calls != 1 {
			t.Fatalf("marker hard error was retried %d times", calls)
		}
	})

	t.Run("coverage hard error is not retried", func(t *testing.T) {
		wantErr := errors.New("sranges coverage query failed")
		calls := 0
		db := &visibilityDBStub{query: func(_ string, _ ...interface{}) (pgx.Rows, error) {
			calls++
			if calls == 1 {
				return &staleCoveredRows{idx: -1, startMS: start, stopMS: stop, knownEmpty: true}, nil
			}
			return nil, wantErr
		}}
		err := waitForQuestSeriesCoverageDeletedWithQueries(context.Background(), New(db), info, sid, start, stop)
		if !errors.Is(err, wantErr) {
			t.Fatalf("coverage visibility error = %v, want %v", err, wantErr)
		}
		if calls != 2 {
			t.Fatalf("coverage hard error was retried %d times", calls)
		}
	})
}

func TestWaitForQuestSeriesVisibleMatchesExactPendingRow(t *testing.T) {
	installQuestWaitScript(t, 1)
	info := NewSeriesInfo("macro", "1m", []SeriesField{
		{Name: "value", Type: "float"},
		{Name: "optional_note", Type: "string"},
	})
	pending := &DataRecord{
		Sid:    7,
		TimeMS: 100,
		EndMS:  200,
		Values: map[string]any{"value": 1.25, "optional_note": nil},
	}
	var gotSQL string
	var gotArgs []any
	db := &visibilityDBStub{queryRow: func(sqlText string, args ...interface{}) pgx.Row {
		if strings.Contains(sqlText, "wal_tables()") {
			return scriptedRow{values: []any{int64(1), int64(1)}}
		}
		gotSQL = sqlText
		gotArgs = args
		return visibilityRowStub{scan: func(dest ...interface{}) error {
			*dest[0].(*int32) = pending.Sid
			*dest[1].(*int64) = pending.TimeMS
			*dest[2].(*int64) = pending.EndMS
			*dest[3].(*sql.NullFloat64) = sql.NullFloat64{Float64: 1.25, Valid: true}
			*dest[4].(*sql.NullString) = sql.NullString{}
			return nil
		}}
	}}

	if err := waitForQuestSeriesVisible(context.Background(), New(db), info, pending.Sid, pending.TimeMS, pending); err != nil {
		t.Fatalf("exact pending row visibility: %v", err)
	}
	if strings.Contains(strings.ToLower(gotSQL), "max(") ||
		!strings.Contains(gotSQL, `WHERE "sid" = $1 AND "ts" = $2`) {
		t.Fatalf("visibility query is not an exact sid/timestamp lookup: %s", gotSQL)
	}
	if len(gotArgs) != 2 || gotArgs[0] != int32(7) {
		t.Fatalf("visibility query args = %#v, want sid and timestamp", gotArgs)
	}
	gotTime, ok := gotArgs[1].(time.Time)
	if !ok || gotTime.UnixMilli() != pending.TimeMS {
		t.Fatalf("visibility timestamp arg = %#v, want %dms", gotArgs[1], pending.TimeMS)
	}
}

func TestWaitForQuestSeriesVisibleChecksAllPendingRowsInOneRangeQuery(t *testing.T) {
	installQuestWaitScript(t, 2)
	info := NewSeriesInfo("macro", "1m", []SeriesField{
		{Name: "value", Type: "float"},
		{Name: "optional_note", Type: "string"},
	})
	pending := []*DataRecord{
		{Sid: 7, TimeMS: 100, EndMS: 200, Values: map[string]any{"value": 1.25, "optional_note": nil}},
		{Sid: 7, TimeMS: 200, EndMS: 300, Values: map[string]any{"value": nil, "optional_note": "ready"}},
	}
	queryCalls := 0
	var gotSQL string
	var gotArgs []any
	db := &visibilityDBStub{
		queryRow: func(string, ...interface{}) pgx.Row {
			return scriptedRow{values: []any{int64(1), int64(1)}}
		},
		query: func(sqlText string, args ...interface{}) (pgx.Rows, error) {
			queryCalls++
			gotSQL = sqlText
			gotArgs = args
			rows := [][]any{
				{int32(7), int64(100), int64(200), sql.NullFloat64{Float64: 1.25, Valid: true}, sql.NullString{}},
			}
			if queryCalls == 2 {
				rows = append(rows, []any{int32(7), int64(200), int64(300), sql.NullFloat64{}, sql.NullString{String: "ready", Valid: true}})
			} else {
				rows = append(rows, []any{int32(7), int64(200), int64(300), sql.NullFloat64{Float64: 9, Valid: true}, sql.NullString{String: "ready", Valid: true}})
			}
			return newInterfaceRows(rows), nil
		},
	}

	if err := waitForQuestSeriesVisible(context.Background(), New(db), info, 7, 200, pending...); err != nil {
		t.Fatalf("all pending rows visibility: %v", err)
	}
	if queryCalls != 2 {
		t.Fatalf("range visibility queries = %d, want one retry after mismatch", queryCalls)
	}
	if strings.Contains(strings.ToLower(gotSQL), "max(") ||
		!strings.Contains(gotSQL, `WHERE "sid" = $1 AND "ts" >= $2 AND "ts" <= $3`) {
		t.Fatalf("visibility query is not a bounded exact-row check: %s", gotSQL)
	}
	if len(gotArgs) != 3 {
		t.Fatalf("range visibility args = %#v, want sid and bounds", gotArgs)
	}
	start, startOK := gotArgs[1].(time.Time)
	stop, stopOK := gotArgs[2].(time.Time)
	if !startOK || !stopOK || start.UnixMilli() != 100 || stop.UnixMilli() != 200 {
		t.Fatalf("range visibility bounds = %#v, want [100,200]ms", gotArgs[1:])
	}
}

func TestWaitForQuestSeriesVisiblePropagatesHardQueryErrors(t *testing.T) {
	info := NewSeriesInfo("macro", "1m", []SeriesField{{Name: "value", Type: "float"}})
	pending := &DataRecord{Sid: 7, TimeMS: 100, EndMS: 200, Values: map[string]any{"value": 1.25}}
	wantErr := errors.New("series visibility query failed")

	t.Run("wal", func(t *testing.T) {
		installQuestWaitScript(t, 1)
		db := &visibilityDBStub{queryRow: func(string, ...interface{}) pgx.Row {
			return scriptedRow{err: wantErr}
		}}
		if err := waitForQuestSeriesVisible(context.Background(), New(db), info, 7, 100, pending); !errors.Is(err, wantErr) {
			t.Fatalf("WAL visibility error = %v, want %v", err, wantErr)
		}
	})

	t.Run("exact row scan", func(t *testing.T) {
		installQuestWaitScript(t, 1)
		calls := 0
		db := &visibilityDBStub{queryRow: func(sqlText string, _ ...interface{}) pgx.Row {
			calls++
			if strings.Contains(sqlText, "wal_tables()") {
				return scriptedRow{values: []any{int64(1), int64(1)}}
			}
			return visibilityRowStub{scan: func(...interface{}) error { return wantErr }}
		}}
		if err := waitForQuestSeriesVisible(context.Background(), New(db), info, 7, 100, pending); !errors.Is(err, wantErr) {
			t.Fatalf("exact row visibility error = %v, want %v", err, wantErr)
		}
		if calls != 2 {
			t.Fatalf("exact row query calls = %d, want 2", calls)
		}
	})

	t.Run("range query", func(t *testing.T) {
		installQuestWaitScript(t, 1)
		queryCalls := 0
		db := &visibilityDBStub{
			queryRow: func(string, ...interface{}) pgx.Row {
				return scriptedRow{values: []any{int64(1), int64(1)}}
			},
			query: func(string, ...interface{}) (pgx.Rows, error) {
				queryCalls++
				return nil, wantErr
			},
		}
		rows := []*DataRecord{
			pending,
			{Sid: 7, TimeMS: 200, EndMS: 300, Values: map[string]any{"value": 2.5}},
		}
		if err := waitForQuestSeriesVisible(context.Background(), New(db), info, 7, 200, rows...); !errors.Is(err, wantErr) {
			t.Fatalf("range visibility error = %v, want %v", err, wantErr)
		}
		if queryCalls != 1 {
			t.Fatalf("range query calls = %d, want 1", queryCalls)
		}
	})
}

func TestSeriesRepoTimescaleRoundTrip(t *testing.T) {
	testutil.RequireIntegration(t)
	initSeriesRepoTestApp(t, mustFindSeriesRepoConfig(t, "config.local.yml"))
	runSeriesRepoRoundTrip(t, "timescale")
}

func TestSeriesRepoTimescaleRollbackKeepsRowsAndCoverageAtomic(t *testing.T) {
	testutil.RequireIntegration(t)
	initSeriesRepoTestApp(t, mustFindSeriesRepoConfig(t, "config.local.yml"))
	if IsQuestDB {
		t.Skip("postgres/timescale backend is not active")
	}

	info, sid := newSeriesRepoTestInfo("series_repo_pg_rollback")
	ctx := context.Background()
	repo := &dbSeriesRepo{}
	if err := repo.EnsureSeriesTable(ctx, info); err != nil {
		t.Fatal(err)
	}
	defer cleanupSeriesRepoTestTable(t, info)

	q, conn, err := Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	tx, write, txErr := q.begin(ctx)
	if txErr != nil {
		t.Fatal(txErr)
	}
	row := &DataRecord{Sid: sid, TimeMS: 100, EndMS: 300, Values: map[string]any{"value": 1.0}}
	if err := repo.insertSeriesBatch(ctx, write, info, []*DataRecord{row}); err != nil {
		t.Fatal(err)
	}
	if err := write.updateSeriesCoverage(ctx, info, sid, row.TimeMS, row.EndMS, []*DataRecord{row}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	var rowCount, rangeCount int
	if err := q.db.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", quoteIdent(info.Binding.Table))).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}
	if err := q.db.QueryRow(ctx, `SELECT count(*) FROM sranges WHERE sid = $1 AND tbl = $2 AND timeframe = $3`,
		sid, info.Binding.Table, info.TimeFrame).Scan(&rangeCount); err != nil {
		t.Fatal(err)
	}
	if rowCount != 0 || rangeCount != 0 {
		t.Fatalf("outer rollback leaked state: rows=%d ranges=%d", rowCount, rangeCount)
	}
}

func TestSeriesRepoTimescaleWriteRollsBackWhenCoverageFails(t *testing.T) {
	testutil.RequireIntegration(t)
	initSeriesRepoTestApp(t, mustFindSeriesRepoConfig(t, "config.local.yml"))
	if IsQuestDB {
		t.Skip("postgres/timescale backend is not active")
	}

	info, sid := newSeriesRepoTestInfo("series_repo_pg_coverage_fail")
	ctx := context.Background()
	repo := &dbSeriesRepo{}
	if err := repo.EnsureSeriesTable(ctx, info); err != nil {
		t.Fatal(err)
	}
	defer cleanupSeriesRepoTestTable(t, info)

	row := &DataRecord{Sid: sid + 1, TimeMS: 100, EndMS: 200, Values: map[string]any{"value": 1.0}}
	if err := repo.WriteSeriesBatch(ctx, info, sid, []*DataRecord{row}); err == nil {
		t.Fatal("expected coverage identity mismatch")
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	var count int
	if err := q.db.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", quoteIdent(info.Binding.Table))).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("failed coverage update left %d physical rows", count)
	}
}

func TestSeriesRepoTimescaleShorterUpsertClearsStaleCoverage(t *testing.T) {
	testutil.RequireIntegration(t)
	initSeriesRepoTestApp(t, mustFindSeriesRepoConfig(t, "config.local.yml"))
	if IsQuestDB {
		t.Skip("postgres/timescale backend is not active")
	}

	info, sid := newSeriesRepoTestInfo("series_repo_pg_shrink")
	ctx := context.Background()
	repo := DefaultSeriesRepo()
	store := NewSeriesStore(repo)
	defer cleanupSeriesRepoTestTable(t, info)
	target := &ExSymbol{ID: sid}
	row := &DataRecord{Sid: sid, TimeMS: 100, EndMS: 300, Values: map[string]any{"value": 1.0}}
	if err := store.WriteBatch(ctx, info, target, []*DataRecord{row}); err != nil {
		t.Fatal(err)
	}
	shorter := *row
	shorter.EndMS = 200
	if err := store.WriteBatch(ctx, info, target, []*DataRecord{&shorter}); err != nil {
		t.Fatal(err)
	}
	start, stop, err := repo.GetSeriesRange(ctx, info, sid)
	if err != nil {
		t.Fatal(err)
	}
	if start != 100 || stop != 200 {
		t.Fatalf("shorter upsert retained stale coverage: [%d,%d)", start, stop)
	}
}

func newSeriesRepoTestInfo(prefix string) (*SeriesInfo, int32) {
	suffix := time.Now().UnixNano()
	return &SeriesInfo{
		Name:      "macro_test",
		TimeFrame: "1d",
		Binding: SeriesBinding{
			Table:      fmt.Sprintf("%s_%d", prefix, suffix),
			TimeColumn: "ts",
			EndColumn:  "end_ms",
			SIDColumn:  "sid",
			Fields:     []SeriesField{{Name: "value", Type: "float", Role: "value"}},
		},
	}, int32(suffix%1_000_000 + 3_000_000)
}

func TestSeriesRepoQuestDBRoundTrip(t *testing.T) {
	testutil.RequireIntegration(t)
	initSeriesRepoTestApp(t, mustFindSeriesRepoConfig(t, "config.yml"))
	runSeriesRepoRoundTrip(t, "quest")
}

func mustFindSeriesRepoConfig(t *testing.T, name string) string {
	t.Helper()
	candidates := []string{
		filepath.Join("..", "..", "data", name),
		filepath.Join("..", "data", name),
		filepath.Join("..", "biz", name),
		name,
	}
	for _, candidate := range candidates {
		abs, err := filepath.Abs(candidate)
		if err != nil {
			continue
		}
		if _, err := os.Stat(abs); err == nil {
			return abs
		}
	}
	t.Fatalf("series repository test config %q not found in candidates: %v", name, candidates)
	return ""
}

func runSeriesRepoRoundTrip(t *testing.T, backend string) {
	t.Helper()
	repo := DefaultSeriesRepo()
	tableName := fmt.Sprintf("series_repo_%s_%d", backend, time.Now().UnixNano())
	info := &SeriesInfo{
		Name:      "macro_test",
		TimeFrame: "1d",
		Binding: SeriesBinding{
			Table:      tableName,
			TimeColumn: "ts",
			EndColumn:  "end_ms",
			SIDColumn:  "sid",
			Fields: []SeriesField{
				{Name: "value", Type: "float", Role: "value"},
				{Name: "label", Type: "string", Role: "custom"},
				{Name: "payload", Type: "json", Role: "custom"},
			},
		},
	}
	ctx := context.Background()
	if err := repo.EnsureSeriesTable(ctx, info); err != nil {
		t.Fatalf("EnsureSeriesTable failed: %v", err)
	}
	sid := int32(time.Now().UnixNano() % 1_000_000)
	startMS := int64(1_700_000_000_000)
	rows := []*DataRecord{
		{
			Sid:    sid,
			TimeMS: startMS,
			EndMS:  startMS + 86_400_000,
			Closed: true,
			Values: map[string]any{
				"value":   12.5,
				"label":   "fred",
				"payload": map[string]any{"source": backend},
			},
		},
		{
			Sid:    sid,
			TimeMS: startMS + 86_400_000,
			EndMS:  startMS + 172_800_000,
			Closed: true,
			Values: map[string]any{
				"value":   13.5,
				"label":   "wind",
				"payload": `{"source":"alt"}`,
			},
		},
	}
	store := NewSeriesStore(repo)
	target := &ExSymbol{ID: sid}
	if err := store.WriteBatch(ctx, info, target, rows); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
	got, err := repo.QuerySeriesRange(ctx, info, sid, rows[0].TimeMS, rows[len(rows)-1].EndMS, 10)
	if err != nil {
		t.Fatalf("QuerySeriesRange failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
	if got[0].Sid != sid || got[0].Values["label"] != "fred" {
		t.Fatalf("unexpected first row: %+v", got[0])
	}
	if got[1].Values["label"] != "wind" {
		t.Fatalf("unexpected second row: %+v", got[1])
	}
	updated := *rows[1]
	updated.Values = map[string]any{"value": 14.5, "label": "revised", "payload": `{"source":"update"}`}
	if err := store.WriteBatch(ctx, info, target, []*DataRecord{&updated}); err != nil {
		t.Fatalf("update WriteBatch failed: %v", err)
	}
	got, err = repo.QuerySeriesRange(ctx, info, sid, rows[0].TimeMS, rows[len(rows)-1].EndMS, 10)
	if err != nil || len(got) != 2 || got[1].Values["label"] != "revised" {
		t.Fatalf("updated series rows=%+v err=%v", got, err)
	}
	if start, stop, err := repo.GetSeriesRange(ctx, info, sid); err != nil {
		t.Fatalf("GetSeriesRange failed: %v", err)
	} else if start != rows[0].TimeMS || stop != rows[len(rows)-1].EndMS {
		t.Fatalf("unexpected series range: start=%d stop=%d", start, stop)
	}
	if err := repo.DeleteSeriesRange(ctx, info, sid, rows[0].TimeMS, rows[0].EndMS); err != nil {
		t.Fatalf("DeleteSeriesRange failed: %v", err)
	}
	got, err = repo.QuerySeriesRange(ctx, info, sid, rows[0].TimeMS, rows[len(rows)-1].EndMS, 10)
	if err != nil {
		t.Fatalf("QuerySeriesRange after delete failed: %v", err)
	}
	if len(got) != 1 || got[0].TimeMS != rows[1].TimeMS {
		t.Fatalf("expected only second row after delete, got %+v", got)
	}
	q, conn, connErr := Conn(ctx)
	if connErr != nil {
		t.Fatal(connErr)
	}
	spans, spanErr := q.ListSRanges(ctx, sid, info.Binding.Table, info.TimeFrame,
		rows[0].TimeMS, rows[len(rows)-1].EndMS)
	conn.Release()
	if spanErr != nil || !hasSeriesGap(spans, rows[0].TimeMS, rows[0].EndMS) {
		t.Fatalf("deleted gap spans=%+v err=%v", spans, spanErr)
	}
	if backend == "quest" {
		assertQuestSeriesPhysicalRows(t, info, sid, 1)
	}
	cleanupSeriesRepoTestTable(t, info)
}

func hasSeriesGap(spans []*SRange, startMS, endMS int64) bool {
	for _, span := range spans {
		if !span.HasData && span.StartMs == startMS && span.StopMs == endMS {
			return true
		}
	}
	return false
}

func TestSeriesRepoQuestDBDeleteHidesMiddleHole(t *testing.T) {
	testutil.RequireIntegration(t)
	initSeriesRepoTestApp(t, mustFindSeriesRepoConfig(t, "config.yml"))
	repo := DefaultSeriesRepo()
	tableName := fmt.Sprintf("series_repo_quest_hole_%d", time.Now().UnixNano())
	info := &SeriesInfo{
		Name:      "macro_test",
		TimeFrame: "1d",
		Binding: SeriesBinding{
			Table:      tableName,
			TimeColumn: "ts",
			EndColumn:  "end_ms",
			SIDColumn:  "sid",
			Fields: []SeriesField{
				{Name: "value", Type: "float", Role: "value"},
			},
		},
	}
	ctx := context.Background()
	if err := repo.EnsureSeriesTable(ctx, info); err != nil {
		t.Fatalf("EnsureSeriesTable failed: %v", err)
	}
	defer cleanupSeriesRepoTestTable(t, info)

	sid := int32(time.Now().UnixNano() % 1_000_000)
	startMS := int64(1_700_000_000_000)
	dayMS := int64(86_400_000)
	rows := make([]*DataRecord, 0, 3)
	for i := 0; i < 3; i++ {
		ts := startMS + int64(i)*dayMS
		rows = append(rows, &DataRecord{
			Sid:    sid,
			TimeMS: ts,
			EndMS:  ts + dayMS,
			Closed: true,
			Values: map[string]any{"value": float64(i)},
		})
	}
	if err := repo.InsertSeriesBatch(ctx, info, rows); err != nil {
		t.Fatalf("InsertSeriesBatch failed: %v", err)
	}
	if err := repo.UpdateSeriesRange(ctx, info, sid, rows[0].TimeMS, rows[len(rows)-1].EndMS); err != nil {
		t.Fatalf("UpdateSeriesRange failed: %v", err)
	}
	if err := repo.DeleteSeriesRange(ctx, info, sid, rows[1].TimeMS, rows[1].EndMS); err != nil {
		t.Fatalf("DeleteSeriesRange failed: %v", err)
	}
	got, err := repo.QuerySeriesRange(ctx, info, sid, rows[0].TimeMS, rows[len(rows)-1].EndMS, 10)
	if err != nil {
		t.Fatalf("QuerySeriesRange after middle delete failed: %v", err)
	}
	if len(got) != 2 || got[0].TimeMS != rows[0].TimeMS || got[1].TimeMS != rows[2].TimeMS {
		t.Fatalf("expected first and third rows after middle delete, got %+v", got)
	}
	assertQuestSeriesPhysicalRows(t, info, sid, 3)
}

func initSeriesRepoTestApp(t *testing.T, cfgPath string) {
	t.Helper()
	dataDir := filepath.Dir(cfgPath)
	t.Setenv("BanDataDir", dataDir)
	config.Loaded = false
	config.DataDir = ""
	config.Args = nil
	swapDefaultSymbolState(NewSymbolState())
	var args config.CmdArgs
	args.NoDefault = true
	args.Configs = []string{cfgPath}
	if filepath.Base(cfgPath) == "config.local.yml" {
		args.Configs = []string{filepath.Join(filepath.Dir(cfgPath), "config.yml"), cfgPath}
	}
	if err := config.LoadConfig(&args); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if err := exg.Setup(); err != nil {
		t.Fatalf("exg.Setup failed: %v", err)
	}
	if err := Setup(); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
}

func cleanupSeriesRepoTestTable(t *testing.T, info *SeriesInfo) {
	t.Helper()
	ctx := context.Background()
	q, conn, err := Conn(ctx)
	if err != nil {
		t.Fatalf("Conn failed during cleanup: %v", err)
	}
	defer conn.Release()
	if _, err_ := q.db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(info.Binding.Table))); err_ != nil {
		t.Fatalf("drop table failed: %v", err_)
	}
	if !IsQuestDB {
		if _, err_ := q.db.Exec(ctx, `DELETE FROM sranges WHERE tbl = $1 AND timeframe = $2`, info.Binding.Table, info.TimeFrame); err_ != nil {
			t.Fatalf("cleanup sranges failed: %v", err_)
		}
	}
}

func assertQuestSeriesPhysicalRows(t *testing.T, info *SeriesInfo, sid int32, want int64) {
	t.Helper()
	ctx := context.Background()
	q, conn, err := Conn(ctx)
	if err != nil {
		t.Fatalf("Conn failed during physical row assertion: %v", err)
	}
	defer conn.Release()
	binding := normalizedSeriesBinding(info.Binding)
	var got int64
	sqlText := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s = $1", quoteIdent(binding.Table), quoteIdent(binding.SIDColumn))
	if err := q.db.QueryRow(ctx, sqlText, sid).Scan(&got); err != nil {
		t.Fatalf("count physical rows failed: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d physical rows, got %d", want, got)
	}
}

func TestSeriesRepoRejectsInvalidInfo(t *testing.T) {
	repo := DefaultSeriesRepo()
	err := repo.EnsureSeriesTable(context.Background(), &SeriesInfo{
		Name:      "bad",
		TimeFrame: "1d",
		Binding: SeriesBinding{
			Table:      "bad_series",
			TimeColumn: "ts",
			EndColumn:  "end_ms",
			SIDColumn:  "sid",
			Fields: []SeriesField{
				{Name: "payload", Type: "unknown"},
			},
		},
	})
	if err == nil {
		t.Fatalf("expected invalid field type to fail")
	}
}

func TestValidateSeriesInfoDefaultsSIDColumn(t *testing.T) {
	info := &SeriesInfo{
		Name:      "macro_default_sid",
		TimeFrame: "1d",
		Binding: SeriesBinding{
			Table:      "macro_default_sid",
			TimeColumn: "ts",
			EndColumn:  "end_ms",
			Fields: []SeriesField{
				{Name: "value", Type: "float"},
			},
		},
	}
	if err := ValidateSeriesInfo(info); err != nil {
		t.Fatalf("expected empty SIDColumn to default to sid, got error: %v", err)
	}
	if got := normalizedSeriesBinding(info.Binding).SIDColumn; got != "sid" {
		t.Fatalf("expected default sid column, got %q", got)
	}
}

func TestBuildSeriesConflictAssignmentsDeduplicatesStructuralColumns(t *testing.T) {
	assigns := buildSeriesConflictAssignments(SeriesBinding{
		SIDColumn:  "instrument_id",
		TimeColumn: "event_time",
		EndColumn:  "end_ms",
		Fields: []SeriesField{
			{Name: "end_ms"},
			{Name: "spread"},
			{Name: "spread"},
			{Name: "instrument_id"},
			{Name: "event_time"},
			{Name: "label"},
		},
	})
	want := []string{
		`"end_ms" = EXCLUDED."end_ms"`,
		`"spread" = EXCLUDED."spread"`,
		`"label" = EXCLUDED."label"`,
	}
	if !reflect.DeepEqual(assigns, want) {
		t.Fatalf("conflict assignments = %v, want %v", assigns, want)
	}
}

func TestBuildSeriesRewriteSQLPreservesSnapshotColumnOrder(t *testing.T) {
	columns := []questTableColumn{
		{Name: "sid", Type: "INT", UpsertKey: true},
		{Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true},
		{Name: "end_ms", Type: "LONG"},
		{Name: "metric", Type: "DOUBLE"},
		{Name: "optional_note", Type: "STRING"},
	}

	got, err := buildQuestRewriteSQLChecked("series_rewrite", "series", `"sid" = 7`, "month", "ts", columns)
	if err != nil {
		t.Fatalf("build series rewrite SQL failed: %v", err)
	}
	last := -1
	for _, column := range columns {
		index := strings.Index(got, quoteIdent(column.Name))
		if index <= last {
			t.Fatalf("rewrite SQL lost snapshot column order at %q: %s", column.Name, got)
		}
		last = index
	}
	if strings.Contains(got, "SELECT *") {
		t.Fatalf("rewrite SQL must project the captured schema explicitly: %s", got)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	if pool != nil {
		pool.Close()
		pool = nil
	}
	os.Exit(code)
}
