package orm

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type scriptedCompactDB struct {
	mu        sync.Mutex
	rows      []pgx.Row
	querySQL  []string
	execSQL   []string
	execErrAt map[int]error
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
	if len(s.rows) == 0 {
		return scriptedRow{err: errors.New("unexpected QueryRow")}
	}
	row := s.rows[0]
	s.rows = s.rows[1:]
	return row
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

type compactDBStub struct {
	counts   []int64
	queryErr error
	queryIdx int
}

func (s *compactDBStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	panic("unexpected Exec call")
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
	oldTimeout := compactVerifyTimeout
	oldPoll := compactVerifyPollInterval
	compactVerifyTimeout = 200 * time.Millisecond
	compactVerifyPollInterval = time.Millisecond
	defer func() {
		compactVerifyTimeout = oldTimeout
		compactVerifyPollInterval = oldPoll
	}()

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
	oldTimeout := compactVerifyTimeout
	oldPoll := compactVerifyPollInterval
	compactVerifyTimeout = 5 * time.Millisecond
	compactVerifyPollInterval = time.Millisecond
	defer func() {
		compactVerifyTimeout = oldTimeout
		compactVerifyPollInterval = oldPoll
	}()

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
	oldTimeout := compactVerifyTimeout
	oldPoll := compactVerifyPollInterval
	compactVerifyTimeout = 200 * time.Millisecond
	compactVerifyPollInterval = time.Millisecond
	t.Cleanup(func() {
		compactVerifyTimeout = oldTimeout
		compactVerifyPollInterval = oldPoll
	})
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
		countRow(5),
		metricsRow(1000, 0, 11, 11, false),
	}}
	err := execCompactLocked(context.Background(), db, "ins_kline_q", compactTables["ins_kline_q"], 1000, 5, 10)
	if err == nil || !strings.Contains(err.Error(), "source table changed") {
		t.Fatalf("expected source-change error, got %v", err)
	}
	if len(db.execSQL) != 2 || !strings.HasPrefix(db.execSQL[1], "DROP TABLE IF EXISTS ins_kline_q_compact_") {
		t.Fatalf("expected temp cleanup without source rename, SQL=%v", db.execSQL)
	}
}

func TestExecCompactRestoresSourceWhenActivationFails(t *testing.T) {
	db := &scriptedCompactDB{
		rows: []pgx.Row{
			countRow(5),
			metricsRow(1000, 0, 10, 10, false),
		},
		execErrAt: map[int]error{2: errors.New("activate failed")},
	}
	err := execCompactLocked(context.Background(), db, "ins_kline_q", compactTables["ins_kline_q"], 1000, 5, 10)
	if err == nil || !strings.Contains(err.Error(), "source restored") {
		t.Fatalf("expected restored-source error, got %v", err)
	}
	if len(db.execSQL) != 4 {
		t.Fatalf("expected create, backup rename, failed activation, restore; SQL=%v", db.execSQL)
	}
	if !strings.HasPrefix(db.execSQL[1], "RENAME TABLE ins_kline_q TO ins_kline_q_backup_") {
		t.Fatalf("source was not renamed to backup first: %q", db.execSQL[1])
	}
	if !strings.Contains(db.execSQL[3], " TO ins_kline_q") {
		t.Fatalf("source backup was not restored: %q", db.execSQL[3])
	}
}
