package orm

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type scriptedCompactDB struct {
	mu         sync.Mutex
	rows       []pgx.Row
	querySQL   []string
	execSQL    []string
	execErrAt  map[int]error
	queryFn    func(string, ...any) (pgx.Rows, error)
	queryRowFn func(string, ...any) pgx.Row
}

func (s *scriptedCompactDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := len(s.execSQL)
	s.execSQL = append(s.execSQL, sql)
	return pgconn.CommandTag{}, s.execErrAt[idx]
}

func (s *scriptedCompactDB) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.querySQL = append(s.querySQL, sql)
	if s.queryRowFn != nil {
		return s.queryRowFn(sql)
	}
	if len(s.rows) == 0 {
		return scriptedRow{err: errors.New("unexpected QueryRow")}
	}
	row := s.rows[0]
	s.rows = s.rows[1:]
	return row
}

func (s *scriptedCompactDB) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.querySQL = append(s.querySQL, sql)
	if s.queryFn != nil {
		return s.queryFn(sql, args...)
	}
	return nil, errors.New("unexpected Query")
}

func (s *scriptedCompactDB) queryCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.querySQL)
}

func (s *scriptedCompactDB) execCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.execSQL)
}

type scriptedRow struct {
	values []any
	err    error
}

func (r scriptedRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return errors.New("scan destination count mismatch")
	}
	for i, value := range r.values {
		switch out := dest[i].(type) {
		case *int64:
			*out = value.(int64)
		case **int64:
			if value == nil {
				*out = nil
				continue
			}
			item := value.(int64)
			*out = &item
		case *bool:
			*out = value.(bool)
		default:
			return errors.New("unsupported scan destination")
		}
	}
	return nil
}

func metricsRow(rowCount, pendingRows, walTxn, tableTxn int64, suspended bool) pgx.Row {
	return scriptedRow{values: []any{rowCount, pendingRows, walTxn, tableTxn, suspended}}
}

func countRow(count int64) pgx.Row {
	return scriptedRow{values: []any{count}}
}

func withFreshCompactState(t *testing.T) {
	t.Helper()
	old := cptState
	cptState = &compactState{tables: make(map[string]*tableCompactState)}
	t.Cleanup(func() { cptState = old })
}

func installCompactWaitScript(t *testing.T, checks int) {
	t.Helper()
	old := compactWaitForCondition
	compactWaitForCondition = func(_ context.Context, _ time.Duration, _ time.Duration, check func() (bool, error)) (bool, error) {
		for i := 0; i < checks; i++ {
			ok, err := check()
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}
	t.Cleanup(func() { compactWaitForCondition = old })
}

type compactDBStub struct {
	counts   []int64
	queryErr error
	queryIdx int
}

func (s *compactDBStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	panic("unexpected Exec call")
}

func (s *compactDBStub) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("unexpected Query")
}

func (s *compactDBStub) QueryRow(context.Context, string, ...any) pgx.Row {
	if s.queryErr != nil {
		return compactRowStub{err: s.queryErr}
	}
	if len(s.counts) == 0 {
		return compactRowStub{count: 0}
	}
	idx := s.queryIdx
	if idx >= len(s.counts) {
		idx = len(s.counts) - 1
	}
	s.queryIdx++
	return compactRowStub{count: s.counts[idx]}
}

type compactRowStub struct {
	count int64
	err   error
}

func (r compactRowStub) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 1 {
		return errors.New("expected one scan destination")
	}
	ptr, ok := dest[0].(*int64)
	if !ok {
		return errors.New("expected *int64 scan destination")
	}
	*ptr = r.count
	return nil
}

func TestWaitCompactVisibleCountWaitsForWalVisibility(t *testing.T) {
	installCompactWaitScript(t, 3)

	db := &compactDBStub{counts: []int64{0, 0, 5}}
	got, err := waitCompactVisibleCount(context.Background(), db, "ins_kline_q_new", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 5 {
		t.Fatalf("count mismatch: got %d want 5", got)
	}
	if db.queryIdx < 3 {
		t.Fatalf("expected polling, query count=%d", db.queryIdx)
	}
}

func TestWaitCompactVisibleCountAllowsLegitimateEmptySnapshot(t *testing.T) {
	db := &compactDBStub{counts: []int64{0}}
	got, err := waitCompactVisibleCount(context.Background(), db, "sranges_q_new", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 0 {
		t.Fatalf("count mismatch: got %d want 0", got)
	}
}

func TestWaitCompactVisibleCountFailsOnPermanentMismatch(t *testing.T) {
	installCompactWaitScript(t, 1)

	db := &compactDBStub{counts: []int64{1, 1, 1}}
	_, err := waitCompactVisibleCount(context.Background(), db, "sranges_q_new", 2)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestCompactTempTableNameIsUniqueAndNotLegacyNewName(t *testing.T) {
	const table = "sranges_q"
	first := compactTempTableName(table)
	second := compactTempTableName(table)

	if first == table+"_new" || second == table+"_new" {
		t.Fatalf("compact temp table must not use legacy fixed _new name: %q %q", first, second)
	}
	if first == second {
		t.Fatalf("compact temp table names must be unique: %q", first)
	}
	if !strings.HasPrefix(first, table+"_compact_") || !strings.HasPrefix(second, table+"_compact_") {
		t.Fatalf("compact temp table name prefix mismatch: %q %q", first, second)
	}
}

func TestCompactIntervalsSeparateHotAndColdTables(t *testing.T) {
	hot := compactTables["sranges_q"]
	cold := compactTables["exsymbol_q"]
	if hot.CheckInterval >= cold.CheckInterval {
		t.Fatalf("hot table should be checked more often: hot=%s cold=%s", hot.CheckInterval, cold.CheckInterval)
	}
	if hot.FullScanInterval <= hot.CheckInterval {
		t.Fatalf("hot full scan must be less frequent than its fast check: check=%s full=%s", hot.CheckInterval, hot.FullScanInterval)
	}
	if cold.FullScanInterval <= cold.CheckInterval {
		t.Fatalf("cold full scan must be less frequent than its fast check: check=%s full=%s", cold.CheckInterval, cold.FullScanInterval)
	}
}

func TestMaintainCompactTableUsesRealMetricsButSkipsExactScan(t *testing.T) {
	withFreshCompactState(t)
	now := time.Now()
	cptState.tables["sranges_q"] = &tableCompactState{
		lastScannedRows: 1000,
		hasScannedRows:  true,
		lastFullScanAt:  now,
	}
	db := &scriptedCompactDB{rows: []pgx.Row{metricsRow(1010, 0, 7, 7, false)}}
	claim := compactCheckClaim{checkAt: now.Add(time.Minute)}
	checked, _, compacted, retry, err := maintainCompactTable(context.Background(), db, "sranges_q", compactTables["sranges_q"], claim)
	if err != nil {
		t.Fatalf("maintain compact table: %v", err)
	}
	if checked || compacted || retry {
		t.Fatalf("unexpected result: checked=%v compacted=%v retry=%v", checked, compacted, retry)
	}
	if got := db.queryCount(); got != 1 {
		t.Fatalf("expected only the in-memory tables() query, got %d queries", got)
	}
	if !strings.Contains(db.querySQL[0], "FROM tables()") {
		t.Fatalf("expected real QuestDB metadata query, got %q", db.querySQL[0])
	}
}

func TestMaintainCompactTableForcesFullScanWithoutLocalChanges(t *testing.T) {
	withFreshCompactState(t)
	now := time.Now()
	meta := compactTables["sranges_q"]
	cptState.tables["sranges_q"] = &tableCompactState{
		lastScannedRows: 1000,
		hasScannedRows:  true,
		lastFullScanAt:  now.Add(-meta.FullScanInterval),
	}
	db := &scriptedCompactDB{rows: []pgx.Row{
		metricsRow(1000, 0, 9, 9, false),
		countRow(1000),
		countRow(500),
	}}
	claim := compactCheckClaim{checkAt: now}
	checked, scanned, compacted, retry, err := maintainCompactTable(context.Background(), db, "sranges_q", meta, claim)
	if err != nil {
		t.Fatalf("maintain compact table: %v", err)
	}
	if !checked || compacted || retry || scanned != 1000 {
		t.Fatalf("unexpected result: checked=%v scanned=%d compacted=%v retry=%v", checked, scanned, compacted, retry)
	}
	if got := db.queryCount(); got != 3 {
		t.Fatalf("expected metrics plus exact total/valid queries, got %d", got)
	}
}

func TestCompactTableWriteLockBlocksOrdinaryAccess(t *testing.T) {
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

	lock := cptState.getTableLock("ins_kline_q")
	lock.Lock()
	started := make(chan struct{})
	acquired := make(chan struct{})
	go func() {
		close(started)
		unlock := LockCompactTableRead("ins_kline_q")
		close(acquired)
		unlock()
	}()
	<-started
	select {
	case <-acquired:
		t.Fatal("ordinary access acquired the table while compact write lock was held")
	case <-time.After(20 * time.Millisecond):
	}
	lock.Unlock()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("ordinary access did not resume after compact lock release")
	}
}

func TestCompactTableAccessBlocksExclusiveProcessLease(t *testing.T) {
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

	unlockAccess := LockCompactTableRead("ins_kline_q")
	_, acquired, err := tryAcquireCompactProcessExclusiveLock(root, "ins_kline_q")
	if err != nil {
		unlockAccess()
		t.Fatalf("try exclusive lease: %v", err)
	}
	if acquired {
		unlockAccess()
		t.Fatal("compact entered while ordinary table access held its shared lease")
	}
	unlockAccess()
}

func TestCompactProcessLockSerializesWorkers(t *testing.T) {
	root := t.TempDir()
	releaseFirst, acquired, err := tryAcquireCompactProcessExclusiveLock(root, "sranges_q")
	if err != nil || !acquired {
		t.Fatalf("acquire first compact process lock: acquired=%v err=%v", acquired, err)
	}
	_, acquired, err = tryAcquireCompactProcessExclusiveLock(root, "sranges_q")
	if err != nil {
		t.Fatalf("acquire competing compact process lock: %v", err)
	}
	if acquired {
		t.Fatal("second worker acquired the same table process lock")
	}
	if err := releaseFirst(); err != nil {
		t.Fatalf("release first compact process lock: %v", err)
	}
	releaseNext, acquired, err := tryAcquireCompactProcessExclusiveLock(root, "sranges_q")
	if err != nil || !acquired {
		t.Fatalf("reacquire compact process lock: acquired=%v err=%v", acquired, err)
	}
	if err := releaseNext(); err != nil {
		t.Fatalf("release reacquired compact process lock: %v", err)
	}
}

func TestCompactProcessLockAllowsConcurrentSharedLeases(t *testing.T) {
	root := t.TempDir()
	releaseFirst, err := acquireCompactProcessSharedLock(context.Background(), root, "sranges_q")
	if err != nil {
		t.Fatalf("acquire first shared lease: %v", err)
	}
	defer releaseFirst()
	releaseSecond, err := acquireCompactProcessSharedLock(context.Background(), root, "sranges_q")
	if err != nil {
		t.Fatalf("acquire second shared lease: %v", err)
	}
	if err := releaseSecond(); err != nil {
		t.Fatalf("release second shared lease: %v", err)
	}
}

func TestCompactProcessSharedLeaseBlocksExclusiveLease(t *testing.T) {
	root := t.TempDir()
	releaseShared, err := acquireCompactProcessSharedLock(context.Background(), root, "sranges_q")
	if err != nil {
		t.Fatalf("acquire shared lease: %v", err)
	}
	_, acquired, err := tryAcquireCompactProcessExclusiveLock(root, "sranges_q")
	if err != nil {
		t.Fatalf("try exclusive lease: %v", err)
	}
	if acquired {
		t.Fatal("exclusive compact lease acquired while an ordinary access lease was held")
	}
	if err := releaseShared(); err != nil {
		t.Fatalf("release shared lease: %v", err)
	}
}

func TestCompactProcessLockRootUsesDatabaseIdentity(t *testing.T) {
	oldDataDir, oldDatabase := config.DataDir, config.Database
	t.Cleanup(func() {
		config.DataDir = oldDataDir
		config.Database = oldDatabase
	})

	config.Database = &config.DatabaseConfig{Url: "postgresql://user:password@quest.example:8812/banbot"}
	config.DataDir = "/var/lib/banbot/one"
	remoteOne := compactProcessLockRoot()
	config.DataDir = "/var/lib/banbot/two"
	remoteTwo := compactProcessLockRoot()
	if remoteOne != remoteTwo {
		t.Fatalf("same remote database split compact lock roots: %q != %q", remoteOne, remoteTwo)
	}

	config.Database.Url = "postgresql://other:credentials@quest.example:8812/other"
	if got := compactProcessLockRoot(); got == remoteOne {
		t.Fatal("different remote database reused compact lock root")
	}

	config.Database.Url = "postgresql://user:password@127.0.0.1:8812/banbot"
	config.DataDir = "/var/lib/banbot/one"
	localOne := compactProcessLockRoot()
	config.DataDir = "/var/lib/banbot/two"
	localTwo := compactProcessLockRoot()
	if localOne == localTwo {
		t.Fatal("different loopback QuestDB data directories reused compact lock root")
	}
}

func TestCompactTableAccessWaitsForExclusiveProcessLease(t *testing.T) {
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

	releaseExclusive, acquired, err := tryAcquireCompactProcessExclusiveLock(root, "ins_kline_q")
	if err != nil || !acquired {
		t.Fatalf("acquire exclusive lease: acquired=%v err=%v", acquired, err)
	}
	accessAcquired := make(chan struct{})
	accessReleased := make(chan struct{})
	go func() {
		unlock := LockCompactTableRead("ins_kline_q")
		close(accessAcquired)
		unlock()
		close(accessReleased)
	}()
	select {
	case <-accessAcquired:
		t.Fatal("ordinary access passed an exclusive compact lease")
	case <-time.After(20 * time.Millisecond):
	}
	if err := releaseExclusive(); err != nil {
		t.Fatalf("release exclusive lease: %v", err)
	}
	select {
	case <-accessReleased:
	case <-time.After(time.Second):
		t.Fatal("ordinary access did not resume after exclusive lease release")
	}
}

func TestCompactMigrationDDLWaitsForExclusiveProcessLease(t *testing.T) {
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

	releaseExclusive, acquired, err := tryAcquireCompactProcessExclusiveLock(root, "sranges_q")
	if err != nil || !acquired {
		t.Fatalf("acquire exclusive lease: acquired=%v err=%v", acquired, err)
	}
	db := &scriptedCompactDB{}
	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		unlockTables, err := lockAllCompactTablesRead(context.Background())
		if err == nil {
			err = ensureQuestDBCreateTables(context.Background(), db,
				"CREATE TABLE IF NOT EXISTS sranges_q (ts TIMESTAMP);")
			unlockTables()
		}
		done <- err
	}()
	<-started
	select {
	case err := <-done:
		_ = releaseExclusive()
		t.Fatalf("migration DDL completed while compact held the exclusive table lease: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := releaseExclusive(); err != nil {
		t.Fatalf("release exclusive lease: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("migration DDL after lease release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("migration DDL did not resume after exclusive lease release")
	}
	if db.execCount() != 1 {
		t.Fatalf("expected one migration DDL statement, got %d", db.execCount())
	}
}

func TestWaitForCompactWalAppliedPollsTablesMetrics(t *testing.T) {
	installCompactWaitScript(t, 2)
	db := &scriptedCompactDB{rows: []pgx.Row{
		metricsRow(1000, 3, 10, 9, false),
		metricsRow(1000, 0, 10, 10, false),
	}}
	txn, err := waitForCompactWalApplied(context.Background(), db, "sranges_q")
	if err != nil {
		t.Fatalf("wait for WAL: %v", err)
	}
	if txn != 10 || db.queryCount() != 2 {
		t.Fatalf("unexpected WAL result: txn=%d queries=%d", txn, db.queryCount())
	}
}

func TestWaitForCompactWalAppliedFallsBackWhenMetricsAreUnknown(t *testing.T) {
	db := &scriptedCompactDB{rows: []pgx.Row{
		scriptedRow{values: []any{int64(1000), int64(0), nil, nil, false}},
		scriptedRow{values: []any{int64(12), int64(12), int64(0), false}},
	}}
	txn, err := waitForCompactWalApplied(context.Background(), db, "sranges_q")
	if err != nil {
		t.Fatalf("wait for fallback WAL state: %v", err)
	}
	if txn != 12 || db.queryCount() != 2 {
		t.Fatalf("unexpected fallback WAL result: txn=%d queries=%d", txn, db.queryCount())
	}
	if !strings.Contains(db.querySQL[1], "FROM wal_tables()") {
		t.Fatalf("expected wal_tables fallback query, got %q", db.querySQL[1])
	}
}

func TestExecCompactAbortsWhenSourceChanges(t *testing.T) {
	db := &scriptedCompactDB{rows: []pgx.Row{
		metricsRow(1000, 0, 11, 11, false),
		countRow(5),
	}, queryFn: compactRewriteQuery}
	err := execCompactLocked(context.Background(), db, "ins_kline_q", compactTables["ins_kline_q"], 1000, 5, 10)
	if err == nil || !strings.Contains(err.Error(), "source table changed") {
		t.Fatalf("expected source-change error, got %v", err)
	}
	if len(db.execSQL) != 0 {
		t.Fatalf("source changed before CTAS; no rewrite SQL should run, got %v", db.execSQL)
	}
}

func TestExecCompactRestoresSourceWhenActivationFails(t *testing.T) {
	db := &scriptedCompactDB{
		rows: []pgx.Row{
			metricsRow(1000, 0, 10, 10, false),
			metricsRow(1000, 0, 10, 10, false),
			countRow(5),
		},
		queryFn: func(sql string, args ...any) (pgx.Rows, error) {
			return compactRewriteQueryWithRowCount(sql, 5, args...)
		},
		execErrAt: map[int]error{2: errors.New("activate failed")},
	}
	err := execCompactLocked(context.Background(), db, "ins_kline_q", compactTables["ins_kline_q"], 1000, 5, 10)
	if err == nil || !strings.Contains(err.Error(), "source restored") {
		t.Fatalf("expected restored-source error, got %v", err)
	}
	if len(db.execSQL) != 5 {
		t.Fatalf("expected create, backup rename, failed activation, restore, temp cleanup; SQL=%v", db.execSQL)
	}
	if !strings.HasPrefix(db.execSQL[1], `RENAME TABLE "ins_kline_q" TO "ins_kline_q_backup_`) {
		t.Fatalf("source was not renamed to backup first: %q", db.execSQL[1])
	}
	if !strings.Contains(db.execSQL[3], ` TO "ins_kline_q"`) {
		t.Fatalf("source backup was not restored: %q", db.execSQL[3])
	}
	if !strings.HasPrefix(db.execSQL[4], `DROP TABLE IF EXISTS "ins_kline_q_compact_`) {
		t.Fatalf("temporary table was not cleaned up: %q", db.execSQL[4])
	}
}

func compactRewriteQuery(sql string, _ ...any) (pgx.Rows, error) {
	return compactRewriteQueryWithRowCount(sql, 1)
}

func compactRewriteQueryWithRowCount(sql string, rowCount int, _ ...any) (pgx.Rows, error) {
	if strings.Contains(sql, "table_columns") {
		return newInterfaceRows([][]any{
			{"sid", "INT", false, true},
			{"timeframe", "SYMBOL", false, true},
			{"ts", "TIMESTAMP", true, true},
			{"start_ms", "LONG", false, false},
			{"stop_ms", "LONG", false, false},
			{"is_deleted", "BOOLEAN", false, false},
			{"quality", "DOUBLE", false, false},
		}), nil
	}
	rows := make([][]any, rowCount)
	for i := range rows {
		rows[i] = []any{int32(7), "1m", time.UnixMilli(int64(123 + i)).UTC(), int64(100), int64(200), nil, float64(i) + 1.25}
	}
	return newInterfaceRows(rows), nil
}

func TestBuildQuestCompactRewriteSQLPreservesDynamicColumnsAndNullMarker(t *testing.T) {
	meta := compactTables["ins_kline_q"]
	columns := []questTableColumn{
		{Name: "sid", Type: "INT", UpsertKey: true},
		{Name: "timeframe", Type: "SYMBOL", UpsertKey: true},
		{Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true},
		{Name: "is_deleted", Type: "BOOLEAN"},
		{Name: "quality", Type: "DOUBLE"},
	}
	got, err := buildQuestCompactRewriteSQL("ins_kline_q_compact", "ins_kline_q", meta, columns)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`SELECT "sid", "timeframe", "ts", "is_deleted", "quality"`,
		`FROM "ins_kline_q"`,
		`LATEST BY "sid", "timeframe"`,
		`coalesce("is_deleted", false) = false`,
		`TIMESTAMP("ts") PARTITION BY DAY WAL`,
		`DEDUP UPSERT KEYS("sid", "timeframe", "ts")`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("compact rewrite SQL %q missing %q", got, want)
		}
	}
	if strings.Contains(got, "cast(false as boolean)") || strings.Contains(got, "SelectCols") {
		t.Fatalf("compact rewrite SQL rewrites or hard-codes the deletion marker: %q", got)
	}
}

func TestBuildQuestCompactRewriteSQLPlacesPhysicalCastInsideSelect(t *testing.T) {
	meta := &TableCompactMeta{LatestByKeys: "sid, instrument", PartitionBy: "DAY"}
	columns := []questTableColumn{
		{Name: "sid", Type: "INT", UpsertKey: true},
		{Name: "instrument", Type: "SYMBOL", Indexed: true, IndexBlockCapacity: 256, SymbolCached: true, SymbolCapacity: 128, UpsertKey: true, columnPropertiesKnown: true},
		{Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true},
		{Name: "is_deleted", Type: "BOOLEAN"},
		{Name: "metric", Type: "DOUBLE"},
	}

	got, err := buildQuestCompactRewriteSQL("series_compact", "series", meta, columns)
	if err != nil {
		t.Fatal(err)
	}
	wantCast := `CAST("instrument" AS SYMBOL CAPACITY 128 CACHE INDEX CAPACITY 256) AS "instrument"`
	castAt := strings.Index(got, wantCast)
	fromAt := strings.Index(got, `FROM "series"`)
	if castAt < 0 || fromAt < 0 || castAt > fromAt {
		t.Fatalf("physical cast is not in the SELECT projection: %q", got)
	}
	if strings.Contains(got[fromAt:], "CAST(") || strings.Contains(got, "),\nCAST(") {
		t.Fatalf("physical cast was appended after the CTAS projection: %q", got)
	}
	if !strings.Contains(got, `) TIMESTAMP("ts") PARTITION BY DAY WAL`) {
		t.Fatalf("CTAS schema clause is not after the closing projection: %q", got)
	}
}

func TestCaptureQuestCompactRewriteSnapshotPreservesSchemaTypesAndNulls(t *testing.T) {
	columns := []questTableColumn{
		{Name: "sid", Type: "INT", UpsertKey: true},
		{Name: "timeframe", Type: "SYMBOL", UpsertKey: true},
		{Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true},
		{Name: "start_ms", Type: "LONG"},
		{Name: "stop_ms", Type: "LONG"},
		{Name: "is_deleted", Type: "BOOLEAN"},
		{Name: "quality", Type: "DOUBLE"},
	}
	rows := [][]any{{int32(7), "1m", time.UnixMilli(123).UTC(), int64(100), int64(200), nil, float64(1.25)}}
	var fingerprintSQL string
	db := &scriptedCompactDB{
		queryFn: func(sql string, args ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "table_columns") {
				return compactRewriteQuery(sql, args...)
			}
			fingerprintSQL = sql
			return compactRewriteQuery(sql, args...)
		},
		queryRowFn: func(sql string, _ ...any) pgx.Row {
			fingerprintSQL = sql
			return scriptedRow{err: errors.New("unexpected QueryRow")}
		},
	}
	snapshot, err := captureQuestCompactRewriteSnapshot(context.Background(), db, "ins_kline_q", compactTables["ins_kline_q"])
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.RowCount != 1 || len(snapshot.Columns) != len(columns) {
		t.Fatalf("unexpected compact snapshot: %+v", snapshot)
	}
	if snapshot.Fingerprint != questTestFingerprint(columns, rows) {
		t.Fatal("fingerprint did not preserve all compact row values")
	}
	if snapshot.Fingerprint == questTestFingerprint(columns, [][]any{{int32(7), "1m", time.UnixMilli(123).UTC(), int64(100), int64(200), false, float64(1.25)}}) {
		t.Fatal("SQL NULL and false compared equal")
	}
	if !strings.Contains(fingerprintSQL, `"quality"`) ||
		!strings.Contains(fingerprintSQL, `FROM "ins_kline_q" LATEST BY "sid", "timeframe" WHERE`) ||
		!strings.Contains(fingerprintSQL, `ORDER BY "sid", "timeframe", "ts", "start_ms", "stop_ms", "is_deleted", "quality"`) ||
		strings.Contains(fingerprintSQL, "LIMIT") || strings.Contains(fingerprintSQL, "WHERE FROM") {
		t.Fatalf("snapshot query does not preserve full row/null/extended-field semantics: %q", fingerprintSQL)
	}
}

type memoryQuestRewriteIntentStore struct {
	intent  *questRewriteSwapIntent
	saves   int
	removes int
}

func (s *memoryQuestRewriteIntentStore) Load(table string) (*questRewriteSwapIntent, error) {
	if s.intent == nil || s.intent.Source != table {
		return nil, nil
	}
	return s.intent, nil
}

func (s *memoryQuestRewriteIntentStore) Save(intent *questRewriteSwapIntent) error {
	s.intent = intent
	s.saves++
	return nil
}

func (s *memoryQuestRewriteIntentStore) Remove(table string) error {
	if s.intent != nil && s.intent.Source == table {
		s.intent = nil
	}
	s.removes++
	return nil
}

func installMemoryQuestRewriteIntentStore(t *testing.T, store *memoryQuestRewriteIntentStore) {
	t.Helper()
	old := questRewriteIntentStoreFn
	questRewriteIntentStoreFn = func() questRewriteIntentStore { return store }
	t.Cleanup(func() { questRewriteIntentStoreFn = old })
}

func TestFileQuestRewriteIntentStoreRoundTrip(t *testing.T) {
	store := &fileQuestRewriteIntentStore{root: t.TempDir()}
	intent := &questRewriteSwapIntent{
		Version: questRewriteSwapIntentVersion, Kind: "compact", Source: "source", Temp: "source_compact_1", Backup: "source_backup_1",
		CompactMeta:     &TableCompactMeta{LatestByKeys: "sid", PartitionBy: "DAY"},
		CompactSnapshot: &questCompactRewriteSnapshot{RowCount: 3, Fingerprint: sha256.Sum256([]byte("snapshot"))},
	}
	if err := store.Save(intent); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load("source")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded, intent) {
		t.Fatalf("loaded intent = %+v, want %+v", loaded, intent)
	}
	if err := store.Remove("source"); err != nil {
		t.Fatal(err)
	}
	loaded, err = store.Load("source")
	if err != nil || loaded != nil {
		t.Fatalf("removed intent still visible: intent=%+v err=%v", loaded, err)
	}
}

type swapRecoveryDB struct {
	tables     map[string]bool
	rows       map[string][][]any
	execSQL    []string
	beforeExec func(string)
}

func (db *swapRecoveryDB) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	if strings.Contains(sql, "FROM tables()") {
		if len(args) != 1 {
			return scriptedRow{err: errors.New("missing table name")}
		}
		var count int64
		if db.tables[args[0].(string)] {
			count = 1
		}
		return countRow(count)
	}
	return scriptedRow{err: fmt.Errorf("unexpected QueryRow: %s", sql)}
}

func (db *swapRecoveryDB) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if strings.Contains(sql, "table_columns") {
		return newInterfaceRows([][]any{
			{"sid", "INT", false, true},
			{"ts", "TIMESTAMP", true, true},
			{"is_deleted", "BOOLEAN", false, false},
		}), nil
	}
	for table, exists := range db.tables {
		if exists && strings.Contains(sql, `FROM "`+table+`"`) {
			return newInterfaceRows(db.rows[table]), nil
		}
	}
	return nil, fmt.Errorf("query references no existing table: %s", sql)
}

func (db *swapRecoveryDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	if db.beforeExec != nil {
		db.beforeExec(sql)
	}
	db.execSQL = append(db.execSQL, sql)
	parts := strings.Fields(sql)
	switch {
	case len(parts) == 5 && parts[0] == "RENAME":
		from, to := strings.Trim(parts[2], `"`), strings.Trim(parts[4], `"`)
		if !db.tables[from] || db.tables[to] {
			return pgconn.CommandTag{}, fmt.Errorf("invalid rename %s to %s", from, to)
		}
		db.tables[from] = false
		db.tables[to] = true
		db.rows[to] = db.rows[from]
		delete(db.rows, from)
	case len(parts) >= 3 && parts[0] == "DROP":
		name := strings.Trim(parts[len(parts)-1], `"`)
		db.tables[name] = false
		delete(db.rows, name)
	default:
		return pgconn.CommandTag{}, fmt.Errorf("unexpected Exec: %s", sql)
	}
	return pgconn.CommandTag{}, nil
}

func TestReplaceVerifiedCompactTablePersistsIntentBeforeRename(t *testing.T) {
	installQuestWaitScript(t, 1)
	store := &memoryQuestRewriteIntentStore{}
	installMemoryQuestRewriteIntentStore(t, store)
	columns := []questTableColumn{{Name: "sid", Type: "INT", UpsertKey: true}, {Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true}, {Name: "is_deleted", Type: "BOOLEAN"}}
	rows := [][]any{{int32(7), time.UnixMilli(123).UTC(), nil}}
	expected := &questCompactRewriteSnapshot{Columns: columns, RowCount: 1, Fingerprint: questTestFingerprint(columns, rows)}
	db := &swapRecoveryDB{
		tables: map[string]bool{"source": true, "tmp": true},
		rows:   map[string][][]any{"source": rows, "tmp": rows},
	}
	db.beforeExec = func(sql string) {
		if strings.HasPrefix(sql, "RENAME TABLE") && (store.intent == nil || store.saves != 1) {
			t.Fatalf("destructive rename ran before durable intent: sql=%s marker=%+v saves=%d", sql, store.intent, store.saves)
		}
	}

	if err := replaceVerifiedCompactTable(context.Background(), db, "source", "tmp", "backup", &TableCompactMeta{LatestByKeys: "sid", PartitionBy: "DAY"}, expected); err != nil {
		t.Fatal(err)
	}
	if store.intent != nil || store.removes != 1 {
		t.Fatalf("completed replacement retained marker: marker=%+v removes=%d", store.intent, store.removes)
	}
}

func TestReconcileQuestRewriteSwapCrashBoundaries(t *testing.T) {
	installQuestWaitScript(t, 1)
	columns := []questTableColumn{
		{Name: "sid", Type: "INT", UpsertKey: true},
		{Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true},
		{Name: "is_deleted", Type: "BOOLEAN"},
	}
	rows := [][]any{{int32(7), time.UnixMilli(123).UTC(), nil}}
	expected := &questCompactRewriteSnapshot{Columns: columns, RowCount: 1, Fingerprint: questTestFingerprint(columns, rows)}

	tests := []struct {
		name       string
		tables     []string
		wantExec   []string
		wantSource bool
	}{
		{name: "after intent fsync", tables: []string{"source", "tmp"}, wantExec: []string{`DROP TABLE IF EXISTS "tmp"`}, wantSource: true},
		{name: "after source rename", tables: []string{"tmp", "backup"}, wantExec: []string{`RENAME TABLE "tmp" TO "source"`, `DROP TABLE IF EXISTS "backup"`}, wantSource: true},
		{name: "after temp rename", tables: []string{"source", "backup"}, wantExec: []string{`DROP TABLE IF EXISTS "backup"`}, wantSource: true},
		{name: "after backup drop", tables: []string{"source"}, wantSource: true},
		{name: "only backup remains", tables: []string{"backup"}, wantExec: []string{`RENAME TABLE "backup" TO "source"`}, wantSource: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			intent := &questRewriteSwapIntent{
				Version: questRewriteSwapIntentVersion, Kind: "compact", Source: "source", Temp: "tmp", Backup: "backup",
				CompactMeta: &TableCompactMeta{LatestByKeys: "sid", PartitionBy: "DAY"}, CompactSnapshot: expected,
			}
			store := &memoryQuestRewriteIntentStore{intent: intent}
			installMemoryQuestRewriteIntentStore(t, store)
			db := &swapRecoveryDB{tables: make(map[string]bool), rows: make(map[string][][]any)}
			for _, table := range test.tables {
				db.tables[table] = true
				db.rows[table] = rows
			}

			if err := reconcileQuestRewriteSwap(context.Background(), db, "source"); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(db.execSQL, test.wantExec) {
				t.Fatalf("recovery SQL = %v, want %v", db.execSQL, test.wantExec)
			}
			if db.tables["source"] != test.wantSource || store.intent != nil || store.removes != 1 {
				t.Fatalf("recovery result: tables=%v marker=%+v removes=%d", db.tables, store.intent, store.removes)
			}
		})
	}
}

func TestReconcileQuestRewriteSwapKeepsOnlyUnverifiedBackup(t *testing.T) {
	installQuestWaitScript(t, 1)
	columns := []questTableColumn{{Name: "sid", Type: "INT", UpsertKey: true}, {Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true}, {Name: "is_deleted", Type: "BOOLEAN"}}
	goodRows := [][]any{{int32(7), time.UnixMilli(123).UTC(), nil}}
	intent := &questRewriteSwapIntent{
		Version: questRewriteSwapIntentVersion, Kind: "compact", Source: "source", Temp: "tmp", Backup: "backup",
		CompactMeta:     &TableCompactMeta{LatestByKeys: "sid", PartitionBy: "DAY"},
		CompactSnapshot: &questCompactRewriteSnapshot{Columns: columns, RowCount: 1, Fingerprint: questTestFingerprint(columns, goodRows)},
	}
	store := &memoryQuestRewriteIntentStore{intent: intent}
	installMemoryQuestRewriteIntentStore(t, store)
	db := &swapRecoveryDB{
		tables: map[string]bool{"backup": true},
		rows:   map[string][][]any{"backup": {{int32(7), time.UnixMilli(123).UTC(), true}}},
	}

	if err := reconcileQuestRewriteSwap(context.Background(), db, "source"); err == nil {
		t.Fatal("expected unverified lone backup to fail closed")
	}
	if len(db.execSQL) != 0 || !db.tables["backup"] || store.intent == nil {
		t.Fatalf("unverified only copy was changed: sql=%v tables=%v marker=%+v", db.execSQL, db.tables, store.intent)
	}
}

func TestCompactMaintenanceReconcilesBeforeReadingMissingSource(t *testing.T) {
	installQuestWaitScript(t, 1)
	withFreshCompactState(t)
	oldRootFn := compactProcessLockRootFn
	root := t.TempDir()
	compactProcessLockRootFn = func() string { return root }
	t.Cleanup(func() { compactProcessLockRootFn = oldRootFn })

	columns := []questTableColumn{{Name: "sid", Type: "INT", UpsertKey: true}, {Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true}, {Name: "is_deleted", Type: "BOOLEAN"}}
	rows := [][]any{{int32(7), time.UnixMilli(123).UTC(), nil}}
	store := &memoryQuestRewriteIntentStore{intent: &questRewriteSwapIntent{
		Version: questRewriteSwapIntentVersion, Kind: "compact", Source: "source", Temp: "tmp", Backup: "backup",
		CompactMeta:     &TableCompactMeta{LatestByKeys: "sid", PartitionBy: "DAY"},
		CompactSnapshot: &questCompactRewriteSnapshot{Columns: columns, RowCount: 1, Fingerprint: questTestFingerprint(columns, rows)},
	}}
	installMemoryQuestRewriteIntentStore(t, store)
	db := &swapRecoveryDB{
		tables: map[string]bool{"tmp": true, "backup": true},
		rows:   map[string][][]any{"tmp": rows, "backup": rows},
	}

	if err := reconcileCompactRewriteBeforeMaintenance(context.Background(), db, "source"); err != nil {
		t.Fatal(err)
	}
	if !db.tables["source"] || db.tables["backup"] || store.intent != nil {
		t.Fatalf("maintenance preflight did not finish interrupted swap: tables=%v marker=%+v", db.tables, store.intent)
	}
}

func TestReplaceVerifiedCompactTableRestoresBeforeDroppingInvalidActivation(t *testing.T) {
	installQuestWaitScript(t, 1)

	var db *scriptedCompactDB
	db = &scriptedCompactDB{
		queryFn: func(sql string, _ ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "table_columns") {
				qualityType := "DOUBLE"
				if len(db.execSQL) >= 2 {
					qualityType = "STRING"
				}
				return newInterfaceRows([][]any{
					{"sid", "INT", false, true},
					{"ts", "TIMESTAMP", true, true},
					{"is_deleted", "BOOLEAN", false, false},
					{"quality", qualityType, false, false},
				}), nil
			}
			quality := any(float64(1.25))
			if len(db.execSQL) >= 2 {
				quality = "bad-type"
			}
			return newInterfaceRows([][]any{{int32(7), time.UnixMilli(123).UTC(), nil, quality}}), nil
		},
		queryRowFn: func(sql string, _ ...any) pgx.Row {
			return countRow(1)
		},
	}
	meta := &TableCompactMeta{LatestByKeys: "sid", PartitionBy: "DAY"}
	expected := &questCompactRewriteSnapshot{
		Columns:  []questTableColumn{{Name: "sid", Type: "INT", UpsertKey: true}, {Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true}, {Name: "is_deleted", Type: "BOOLEAN"}, {Name: "quality", Type: "DOUBLE"}},
		RowCount: 1,
		Fingerprint: questTestFingerprint(
			[]questTableColumn{{Name: "sid", Type: "INT", UpsertKey: true}, {Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true}, {Name: "is_deleted", Type: "BOOLEAN"}, {Name: "quality", Type: "DOUBLE"}},
			[][]any{{int32(7), time.UnixMilli(123).UTC(), nil, float64(1.25)}},
		),
	}
	if err := replaceVerifiedCompactTable(context.Background(), db, "source", "tmp", "backup", meta, expected); err == nil || !strings.Contains(err.Error(), "source restored") {
		t.Fatalf("replacement error = %v, want source restoration", err)
	}
	if len(db.execSQL) != 5 || strings.Contains(db.execSQL[4], `DROP TABLE "backup"`) {
		t.Fatalf("invalid activated table was dropped or restore was skipped: %v", db.execSQL)
	}
	if db.execSQL[2] != `RENAME TABLE "source" TO "tmp"` || db.execSQL[3] != `RENAME TABLE "backup" TO "source"` {
		t.Fatalf("restore order = %v", db.execSQL)
	}
	if db.execSQL[4] != `DROP TABLE IF EXISTS "tmp"` {
		t.Fatalf("temporary table cleanup = %q", db.execSQL[4])
	}
}

func TestReplaceVerifiedCompactTableRejectsCorruptionAfterAnyRow(t *testing.T) {
	installQuestWaitScript(t, 1)
	meta := &TableCompactMeta{LatestByKeys: "sid, timeframe", PartitionBy: "DAY"}
	columns := []questTableColumn{
		{Name: "sid", Type: "INT", UpsertKey: true},
		{Name: "timeframe", Type: "SYMBOL", UpsertKey: true},
		{Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true},
		{Name: "is_deleted", Type: "BOOLEAN"},
		{Name: "quality", Type: "DOUBLE"},
	}
	for _, test := range []struct {
		name      string
		rowCount  int
		corruptAt int
	}{
		{name: "second row", rowCount: 4, corruptAt: 1},
		{name: "sixty-fifth row", rowCount: 65, corruptAt: 64},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows := make([][]any, test.rowCount)
			for i := range rows {
				quality := any(float64(i) + 0.5)
				if i%4 == 0 {
					quality = nil
				}
				rows[i] = []any{int32(i%2 + 1), "1m", time.UnixMilli(int64(100 + i)).UTC(), nil, quality}
			}
			if test.rowCount > 2 {
				rows[2] = append([]any(nil), rows[1]...)
			}
			actualRows := make([][]any, len(rows))
			for i, row := range rows {
				actualRows[i] = append([]any(nil), row...)
			}
			actualRows[test.corruptAt][4] = float64(123456)
			var valuesCalls int
			var db *scriptedCompactDB
			db = &scriptedCompactDB{
				queryFn: func(sql string, _ ...any) (pgx.Rows, error) {
					if strings.Contains(sql, "table_columns") {
						return newInterfaceRows([][]any{{"sid", "INT", false, true}, {"timeframe", "SYMBOL", false, true}, {"ts", "TIMESTAMP", true, true}, {"is_deleted", "BOOLEAN", false, false}, {"quality", "DOUBLE", false, false}}), nil
					}
					streamRows := rows
					if len(db.execSQL) >= 2 {
						streamRows = actualRows
					}
					stream := newInterfaceRows(streamRows)
					stream.valuesCalls = &valuesCalls
					return stream, nil
				},
				queryRowFn: func(string, ...any) pgx.Row {
					return countRow(int64(len(rows)))
				},
			}
			expected := &questCompactRewriteSnapshot{
				Columns:     columns,
				RowCount:    int64(len(rows)),
				Fingerprint: questTestFingerprint(columns, rows),
			}

			err := replaceVerifiedCompactTable(context.Background(), db, "source", "tmp", "backup", meta, expected)
			if err == nil || !strings.Contains(err.Error(), "source restored") {
				t.Fatalf("corrupt row replacement error = %v, want source restoration", err)
			}
			if valuesCalls != len(rows)*2 {
				t.Fatalf("source and activated fingerprints consumed %d rows, want %d", valuesCalls, len(rows)*2)
			}
			if len(db.execSQL) != 5 || strings.Contains(strings.Join(db.execSQL, ";"), `DROP TABLE "backup"`) {
				t.Fatalf("corrupt row was allowed to drop the backup: %v", db.execSQL)
			}
			if db.execSQL[2] != `RENAME TABLE "source" TO "tmp"` || db.execSQL[3] != `RENAME TABLE "backup" TO "source"` {
				t.Fatalf("corrupt row did not preserve recovery order: %v", db.execSQL)
			}
			if db.execSQL[4] != `DROP TABLE IF EXISTS "tmp"` {
				t.Fatalf("temporary table cleanup = %q", db.execSQL[4])
			}
		})
	}
}

func TestCaptureQuestCompactRewriteFingerprintPropagatesRowsError(t *testing.T) {
	wantErr := errors.New("compact stream read failed")
	db := &scriptedCompactDB{
		queryFn: func(sql string, _ ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "table_columns") {
				return newInterfaceRows([][]any{{"sid", "INT", false, true}, {"is_deleted", "BOOLEAN", false, false}}), nil
			}
			return &interfaceRows{rows: [][]any{{int32(1), nil}}, idx: -1, valuesErrAt: 0, valuesErr: wantErr}, nil
		},
		queryRowFn: func(string, ...any) pgx.Row { return countRow(1) },
	}
	_, err := captureQuestCompactRewriteSnapshot(context.Background(), db, "source", &TableCompactMeta{LatestByKeys: "sid"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("compact fingerprint rows error = %v, want %v", err, wantErr)
	}
}
