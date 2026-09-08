package orm

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/banbox/banexg/log"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
)

// TableCompactMeta describes one append-only metadata table maintained by the
// background compact worker.
type TableCompactMeta struct {
	LatestByKeys     string
	SelectCols       string
	PartitionBy      string
	DedupKeys        string
	CheckInterval    time.Duration
	FullScanInterval time.Duration
	StartupDelay     time.Duration
	MinPendingRows   int64
}

var compactTables = map[string]*TableCompactMeta{
	"sranges_q": {
		LatestByKeys:     "sid, tbl, timeframe, start_ms",
		SelectCols:       "sid, ts, tbl, timeframe, start_ms, stop_ms, has_data",
		PartitionBy:      "MONTH",
		DedupKeys:        "sid, tbl, timeframe, start_ms, ts",
		CheckInterval:    30 * time.Minute,
		FullScanInterval: 2 * time.Hour,
		StartupDelay:     30 * time.Second,
		MinPendingRows:   256,
	},
	"kline_un_q": {
		LatestByKeys:     "sid, timeframe",
		SelectCols:       "sid, timeframe, ts, stop_ms, expire_ms, open, high, low, close, volume, quote, buy_volume, trade_num",
		PartitionBy:      "MONTH",
		DedupKeys:        "sid, timeframe, ts",
		CheckInterval:    time.Hour,
		FullScanInterval: 4 * time.Hour,
		StartupDelay:     time.Minute,
		MinPendingRows:   128,
	},
	"ins_kline_q": {
		LatestByKeys:     "sid, timeframe",
		SelectCols:       "sid, timeframe, ts, start_ms, stop_ms",
		PartitionBy:      "DAY",
		DedupKeys:        "sid, timeframe, ts",
		CheckInterval:    2 * time.Hour,
		FullScanInterval: 6 * time.Hour,
		StartupDelay:     90 * time.Second,
		MinPendingRows:   128,
	},
	"calendars_q": {
		LatestByKeys:     "market, start_ms",
		SelectCols:       "ts, market, start_ms, stop_ms",
		PartitionBy:      "YEAR",
		DedupKeys:        "market, start_ms, ts",
		CheckInterval:    6 * time.Hour,
		FullScanInterval: 24 * time.Hour,
		StartupDelay:     2 * time.Minute,
		MinPendingRows:   64,
	},
	"adj_factors_q": {
		LatestByKeys:     "sid, sub_id, start_ms",
		SelectCols:       "ts, sid, sub_id, start_ms, factor",
		PartitionBy:      "MONTH",
		DedupKeys:        "sid, sub_id, start_ms, ts",
		CheckInterval:    12 * time.Hour,
		FullScanInterval: 24 * time.Hour,
		StartupDelay:     150 * time.Second,
		MinPendingRows:   128,
	},
	"exsymbol_q": {
		LatestByKeys:     "sid",
		SelectCols:       "sid, ts, exchange, exg_real, market, symbol, combined, list_ms, delist_ms, agg_rules",
		PartitionBy:      "YEAR",
		DedupKeys:        "sid, ts",
		CheckInterval:    12 * time.Hour,
		FullScanInterval: 24 * time.Hour,
		StartupDelay:     3 * time.Minute,
		MinPendingRows:   32,
	},
}

var compactTableOrder = []string{
	"sranges_q",
	"kline_un_q",
	"ins_kline_q",
	"calendars_q",
	"adj_factors_q",
	"exsymbol_q",
}

const (
	compactRatioThresh = 0.35
	compactMinRows     = 500
)

var (
	compactVerifyTimeout      = 30 * time.Second
	compactVerifyPollInterval = 100 * time.Millisecond
	compactWorkerTick         = 15 * time.Second
	compactWaitForCondition   = waitForQuestCondition
)

type tableCompactState struct {
	accessLock      sync.RWMutex
	pendingRows     int64
	lastScannedRows int64
	hasScannedRows  bool
	baselinePending bool
	inFlight        bool
	nextCheckAt     time.Time
	lastCheckAt     time.Time
	lastFullScanAt  time.Time
	lastCompactAt   time.Time
}

type compactState struct {
	mu     sync.Mutex
	tables map[string]*tableCompactState
}

var cptState = &compactState{tables: make(map[string]*tableCompactState)}

var (
	compactWorkerMu     sync.Mutex
	compactWorkerCancel context.CancelFunc
	compactWorkerDone   chan struct{}
)

func (s *compactState) getTableState(table string) *tableCompactState {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.tables[table]
	if state == nil {
		state = &tableCompactState{baselinePending: true}
		s.tables[table] = state
	}
	return state
}

func (s *compactState) getTableLock(table string) *sync.RWMutex {
	return &s.getTableState(table).accessLock
}

// LockCompactTableRead makes ordinary reads and writes wait while a table is
// being replaced. Callers should hold the returned guard for the full logical
// operation, not just one SQL statement.
func LockCompactTableRead(table string) func() {
	unlock, err := lockCompactTableRead(context.Background(), table)
	if err != nil {
		panic(fmt.Errorf("acquire shared compact lease for %s: %w", table, err))
	}
	return unlock
}

func lockCompactTableRead(ctx context.Context, table string) (func(), error) {
	if !IsQuestDB {
		return func() {}, nil
	}
	if _, ok := compactTables[table]; !ok {
		return func() {}, nil
	}
	releaseProcessLock, err := acquireCompactProcessSharedLock(ctx, compactProcessLockRootFn(), table)
	if err != nil {
		return nil, err
	}
	lock := cptState.getTableLock(table)
	lock.RLock()
	return func() {
		lock.RUnlock()
		if err := releaseProcessLock(); err != nil {
			log.Error("release shared compact process lock failed", zap.String("table", table), zap.Error(err))
		}
	}, nil
}

// lockCompactTableReadExclusiveProcess serializes a metadata writer with
// table replacement and other processes. The table RW lock remains shared
// locally because this operation appends a new WAL version rather than
// replacing the table.
func lockCompactTableReadExclusiveProcess(ctx context.Context, table string) (func() error, error) {
	return lockCompactTableReadExclusiveProcessAtRoot(ctx, table, compactProcessLockRootFn())
}

func lockCompactTableReadExclusiveProcessAtRoot(ctx context.Context, table, root string) (func() error, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return func() error { return nil }, nil
	}
	if _, ok := compactTables[table]; !ok {
		return func() error { return nil }, nil
	}
	releaseProcessLock, err := acquireCompactProcessExclusiveLock(ctx, root, table)
	if err != nil {
		return nil, err
	}
	lock := cptState.getTableLock(table)
	lock.RLock()
	return func() error {
		lock.RUnlock()
		return releaseProcessLock()
	}, nil
}

func lockAllCompactTablesRead(ctx context.Context) (func(), error) {
	unlocks := make([]func(), 0, len(compactTableOrder))
	for _, table := range compactTableOrder {
		unlock, err := lockCompactTableRead(ctx, table)
		if err != nil {
			for i := len(unlocks) - 1; i >= 0; i-- {
				unlocks[i]()
			}
			return nil, err
		}
		unlocks = append(unlocks, unlock)
	}
	return func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}, nil
}

// MarkTableForCompact records append-only versions created since the last
// expensive check. It never queries QuestDB and is safe on hot write paths.
func MarkTableForCompact(table string, rows int) {
	if !IsQuestDB {
		return
	}
	if _, ok := compactTables[table]; !ok {
		return
	}
	if rows <= 0 {
		return
	}
	cptState.mu.Lock()
	state := cptState.tables[table]
	if state == nil {
		state = &tableCompactState{baselinePending: true}
		cptState.tables[table] = state
	}
	state.pendingRows += int64(rows)
	cptState.mu.Unlock()
}

func startCompactWorker() {
	if !IsQuestDB || pool == nil {
		return
	}
	stopCompactWorker()

	now := time.Now()
	cptState.mu.Lock()
	for _, table := range compactTableOrder {
		meta := compactTables[table]
		state := cptState.tables[table]
		if state == nil {
			state = &tableCompactState{}
			cptState.tables[table] = state
		}
		state.baselinePending = true
		state.inFlight = false
		state.nextCheckAt = now.Add(meta.StartupDelay)
	}
	cptState.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	compactWorkerMu.Lock()
	compactWorkerCancel = cancel
	compactWorkerDone = done
	compactWorkerMu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(compactWorkerTick)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				runCompactMaintenance(ctx, pool, now)
			}
		}
	}()
}

func stopCompactWorker() {
	compactWorkerMu.Lock()
	cancel := compactWorkerCancel
	done := compactWorkerDone
	compactWorkerCancel = nil
	compactWorkerDone = nil
	compactWorkerMu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	if done != nil {
		<-done
	}
}

type compactCheckClaim struct {
	checkAt     time.Time
	baseline    bool
	pendingRows int64
}

func claimCompactCheck(table string, meta *TableCompactMeta, now time.Time) (compactCheckClaim, bool) {
	cptState.mu.Lock()
	defer cptState.mu.Unlock()
	state := cptState.tables[table]
	if state == nil {
		state = &tableCompactState{baselinePending: true}
		cptState.tables[table] = state
	}
	if state.inFlight || now.Before(state.nextCheckAt) {
		return compactCheckClaim{}, false
	}
	state.inFlight = true
	state.lastCheckAt = now
	state.nextCheckAt = now.Add(meta.CheckInterval)
	return compactCheckClaim{checkAt: now, baseline: state.baselinePending, pendingRows: state.pendingRows}, true
}

func finishCompactCheck(table string, claim compactCheckClaim, checked bool, scannedRows int64, compacted bool, retrySoon bool) {
	now := time.Now()
	cptState.mu.Lock()
	defer cptState.mu.Unlock()
	state := cptState.tables[table]
	if state == nil {
		return
	}
	state.inFlight = false
	if retrySoon {
		state.nextCheckAt = now.Add(max(compactWorkerTick, time.Second))
	}
	if !checked {
		return
	}
	state.baselinePending = false
	state.lastScannedRows = scannedRows
	state.hasScannedRows = true
	state.lastFullScanAt = now
	state.pendingRows = max(int64(0), state.pendingRows-claim.pendingRows)
	if compacted {
		state.lastCompactAt = now
	}
}

func runCompactMaintenance(ctx context.Context, db compactDB, now time.Time) {
	for _, table := range compactTableOrder {
		if ctx.Err() != nil {
			return
		}
		meta := compactTables[table]
		claim, ok := claimCompactCheck(table, meta, now)
		if !ok {
			continue
		}
		checked, scannedRows, compacted, retrySoon, err := maintainCompactTable(ctx, db, table, meta, claim)
		finishCompactCheck(table, claim, checked, scannedRows, compacted, retrySoon)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Warn("compact_check failed", zap.String("table", table), zap.Error(err))
		}
	}
}

type compactDB interface {
	questRewriteDB
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type compactTableMetrics struct {
	rowCount    *int64
	pendingRows *int64
	walTxn      *int64
	tableTxn    *int64
	suspended   bool
}

func queryCompactTableMetrics(ctx context.Context, db compactDB, table string) (compactTableMetrics, error) {
	var metrics compactTableMetrics
	err := db.QueryRow(ctx, `SELECT table_row_count, wal_pending_row_count, wal_txn, table_txn, table_suspended
FROM tables() WHERE table_name = $1`, table).Scan(
		&metrics.rowCount,
		&metrics.pendingRows,
		&metrics.walTxn,
		&metrics.tableTxn,
		&metrics.suspended,
	)
	return metrics, err
}

func (m compactTableMetrics) walApplied() bool {
	if m.suspended {
		return false
	}
	if m.pendingRows != nil && *m.pendingRows > 0 {
		return false
	}
	return m.walTxn == nil || m.tableTxn == nil || *m.walTxn <= *m.tableTxn
}

func maintainCompactTable(ctx context.Context, db compactDB, table string, meta *TableCompactMeta, claim compactCheckClaim) (bool, int64, bool, bool, error) {
	if err := reconcileCompactRewriteBeforeMaintenance(ctx, db, table); err != nil {
		return false, 0, false, true, err
	}
	metrics, err := queryCompactTableMetrics(ctx, db, table)
	if err != nil {
		return false, 0, false, true, err
	}
	if !metrics.walApplied() {
		return false, 0, false, true, nil
	}

	cptState.mu.Lock()
	state := cptState.tables[table]
	lastScannedRows := int64(0)
	hasScannedRows := false
	lastFullScanAt := time.Time{}
	if state != nil {
		lastScannedRows = state.lastScannedRows
		hasScannedRows = state.hasScannedRows
		lastFullScanAt = state.lastFullScanAt
	}
	cptState.mu.Unlock()

	fullScanDue := claim.baseline || lastFullScanAt.IsZero() || !claim.checkAt.Before(lastFullScanAt.Add(meta.FullScanInterval))
	if !fullScanDue && claim.pendingRows < meta.MinPendingRows && metrics.rowCount != nil && *metrics.rowCount < compactMinRows {
		return false, lastScannedRows, false, false, nil
	}
	rowDeltaSmall := metrics.rowCount != nil && hasScannedRows && absInt64(*metrics.rowCount-lastScannedRows) < meta.MinPendingRows
	if !fullScanDue && claim.pendingRows < meta.MinPendingRows && rowDeltaSmall {
		return false, lastScannedRows, false, false, nil
	}

	totalRows, validRows, needed, err := queryCompactStats(ctx, db, table, meta)
	if err != nil {
		return false, 0, false, true, err
	}
	if !needed {
		return true, totalRows, false, false, nil
	}

	releaseProcessLock, acquired, err := tryAcquireCompactProcessExclusiveLock(compactProcessLockRootFn(), table)
	if err != nil {
		return false, 0, false, true, err
	}
	if !acquired {
		return false, 0, false, true, nil
	}
	defer func() {
		if err := releaseProcessLock(); err != nil {
			log.Warn("release compact process lock failed", zap.String("table", table), zap.Error(err))
		}
	}()

	lock := cptState.getTableLock(table)
	lock.Lock()
	defer lock.Unlock()

	sourceTxn, err := waitForCompactWalApplied(ctx, db, table)
	if err != nil {
		return false, 0, false, true, err
	}
	totalRows, validRows, needed, err = queryCompactStats(ctx, db, table, meta)
	if err != nil {
		return false, 0, false, true, err
	}
	if !needed {
		return true, totalRows, false, false, nil
	}
	stableTxn, err := waitForCompactWalApplied(ctx, db, table)
	if err != nil {
		return false, 0, false, true, err
	}
	if stableTxn != sourceTxn {
		return false, 0, false, true, fmt.Errorf("source table changed while compact snapshot was prepared: table=%s before_txn=%d after_txn=%d", table, sourceTxn, stableTxn)
	}
	if err := execCompactLocked(ctx, db, table, meta, totalRows, validRows, stableTxn); err != nil {
		return false, 0, false, true, err
	}
	return true, validRows, true, false, nil
}

func reconcileCompactRewriteBeforeMaintenance(ctx context.Context, db compactDB, table string) error {
	intent, err := questRewriteIntentStoreFn().Load(table)
	if err != nil || intent == nil {
		return err
	}
	releaseProcessLock, err := acquireCompactProcessExclusiveLock(ctx, compactProcessLockRootFn(), table)
	if err != nil {
		return err
	}
	defer func() {
		if err := releaseProcessLock(); err != nil {
			log.Warn("release rewrite recovery process lock failed", zap.String("table", table), zap.Error(err))
		}
	}()
	lock := cptState.getTableLock(table)
	lock.Lock()
	defer lock.Unlock()
	return reconcileQuestRewriteSwap(ctx, db, table)
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}

func queryCompactStats(ctx context.Context, db compactDB, table string, meta *TableCompactMeta) (int64, int64, bool, error) {
	totalRows, err := queryCompactRowCount(ctx, db, table)
	if err != nil {
		return 0, 0, false, err
	}
	if totalRows < compactMinRows {
		return totalRows, totalRows, false, nil
	}
	validSQL := fmt.Sprintf(`SELECT count(*) FROM (
  SELECT 1 FROM %s LATEST BY %s WHERE coalesce(%s, false) = false
)`, quoteIdent(table), meta.LatestByKeys, quoteIdent("is_deleted"))
	var validRows int64
	if err := db.QueryRow(ctx, validSQL).Scan(&validRows); err != nil {
		return 0, 0, false, err
	}
	ratio := float64(validRows) / float64(totalRows)
	return totalRows, validRows, ratio < compactRatioThresh, nil
}

func queryCompactWalFallback(ctx context.Context, db compactDB, table string) (int64, bool, bool, error) {
	var sequencerTxn, writerTxn int64
	var lagTxnCount int64
	var suspended bool
	err := db.QueryRow(ctx, `SELECT sequencerTxn, writerTxn, writerLagTxnCount, suspended
FROM wal_tables() WHERE name = $1`, table).Scan(&sequencerTxn, &writerTxn, &lagTxnCount, &suspended)
	if err != nil {
		return 0, false, false, err
	}
	return sequencerTxn, writerTxn >= sequencerTxn && lagTxnCount == 0, suspended, nil
}

func compactWalApplied(ctx context.Context, db compactDB, table string) (int64, bool, bool, error) {
	metrics, err := queryCompactTableMetrics(ctx, db, table)
	if err != nil {
		return 0, false, false, err
	}
	if metrics.walTxn == nil || metrics.tableTxn == nil {
		return queryCompactWalFallback(ctx, db, table)
	}
	return *metrics.walTxn, metrics.walApplied(), metrics.suspended, nil
}

func waitForCompactWalApplied(ctx context.Context, db compactDB, table string) (int64, error) {
	if err := reconcileQuestRewriteSwap(ctx, db, table); err != nil {
		return 0, fmt.Errorf("reconcile interrupted rewrite before WAL check: %w", err)
	}
	var walTxn int64
	ok, err := compactWaitForCondition(ctx, compactVerifyTimeout, compactVerifyPollInterval, func() (bool, error) {
		observedTxn, applied, suspended, err := compactWalApplied(ctx, db, table)
		if err != nil {
			return false, err
		}
		walTxn = observedTxn
		if applied {
			return true, nil
		}
		if suspended {
			return false, fmt.Errorf("source WAL table is suspended: %s", table)
		}
		return false, nil
	})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("source WAL not fully applied before compact timeout: table=%s timeout=%s", table, compactVerifyTimeout)
	}
	return walTxn, nil
}

func queryCompactRowCount(ctx context.Context, db compactDB, table string) (int64, error) {
	var count int64
	err := db.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&count)
	return count, err
}

func waitCompactVisibleCount(ctx context.Context, db compactDB, table string, expected int64) (int64, error) {
	var lastCount int64
	ok, err := compactWaitForCondition(ctx, compactVerifyTimeout, compactVerifyPollInterval, func() (bool, error) {
		count, err := queryCompactRowCount(ctx, db, table)
		if err != nil {
			return false, err
		}
		lastCount = count
		if count == expected {
			return true, nil
		}
		if count > expected {
			return false, fmt.Errorf("new table row count exceeded expected snapshot: got=%d expected=%d", count, expected)
		}
		return false, nil
	})
	if err != nil {
		return lastCount, err
	}
	if !ok {
		return lastCount, fmt.Errorf("new WAL table rows not fully visible before timeout: got=%d expected=%d timeout=%s", lastCount, expected, compactVerifyTimeout)
	}
	return lastCount, nil
}

func compactTempTableName(table string) string {
	return fmt.Sprintf("%s_compact_%d_%06d", table, time.Now().UnixNano(), rand.Intn(1000000))
}

func compactBackupTableName(table string) string {
	return fmt.Sprintf("%s_backup_%d_%06d", table, time.Now().UnixNano(), rand.Intn(1000000))
}

func execCompactLocked(ctx context.Context, db compactDB, table string, meta *TableCompactMeta, beforeTotal, expectedRows, sourceTxn int64) error {
	start := time.Now()
	tmpTable := compactTempTableName(table)
	backupTable := compactBackupTableName(table)
	stableTxn, err := waitForCompactWalApplied(ctx, db, table)
	if err != nil {
		return err
	}
	if stableTxn != sourceTxn {
		return fmt.Errorf("source table changed during compact: table=%s before_txn=%d after_txn=%d", table, sourceTxn, stableTxn)
	}
	expected, err := captureQuestCompactRewriteSnapshot(ctx, db, table, meta)
	if err != nil {
		return fmt.Errorf("capture compact source snapshot: %w", err)
	}
	if expected.RowCount != expectedRows {
		return fmt.Errorf("compact source snapshot changed before CTAS: table=%s got=%d want=%d", table, expected.RowCount, expectedRows)
	}
	stableTxn, err = waitForCompactWalApplied(ctx, db, table)
	if err != nil {
		return err
	}
	if stableTxn != sourceTxn {
		return fmt.Errorf("source table changed during compact: table=%s before_txn=%d after_txn=%d", table, sourceTxn, stableTxn)
	}
	createSQL, err := buildQuestCompactRewriteSQL(tmpTable, table, meta, expected.Columns)
	if err != nil {
		return fmt.Errorf("build compact table: %w", err)
	}

	if _, err := db.Exec(ctx, createSQL); err != nil {
		cause := fmt.Errorf("create compact table: %w", err)
		return cleanupQuestRewriteFailure(ctx, db, tmpTable, cause)
	}
	newCount, err := waitCompactVisibleCount(ctx, db, tmpTable, expected.RowCount)
	if err != nil {
		return cleanupQuestRewriteFailure(ctx, db, tmpTable, fmt.Errorf("verify compact table: %w", err))
	}
	if err := verifyQuestCompactRewriteSnapshot(ctx, db, tmpTable, meta, expected); err != nil {
		return cleanupQuestRewriteFailure(ctx, db, tmpTable, fmt.Errorf("verify compact snapshot: %w", err))
	}
	if err := replaceVerifiedCompactTable(ctx, db, table, tmpTable, backupTable, meta, expected); err != nil {
		return err
	}
	log.Info("compact_done",
		zap.String("table", table),
		zap.Int64("before", beforeTotal),
		zap.Int64("after", newCount),
		zap.String("ratio", fmt.Sprintf("%.3f", float64(newCount)/float64(max(beforeTotal, 1)))),
		zap.Duration("elapsed", time.Since(start)))
	return nil
}

func replaceVerifiedCompactTable(ctx context.Context, db compactDB, table, tmpTable, backupTable string, meta *TableCompactMeta, expected *questCompactRewriteSnapshot) error {
	if err := reconcileQuestRewriteSwap(ctx, db, table); err != nil {
		return fmt.Errorf("reconcile interrupted compact rewrite: %w", err)
	}
	if err := verifyQuestCompactRewriteSnapshot(ctx, db, table, meta, expected); err != nil {
		return fmt.Errorf("verify source compact snapshot before rename: %w", err)
	}
	intent := &questRewriteSwapIntent{
		Kind: "compact", Source: table, Temp: tmpTable, Backup: backupTable,
		CompactMeta: meta, CompactSnapshot: expected,
	}
	if err := saveQuestRewriteSwapIntent(intent); err != nil {
		return cleanupQuestRewriteFailure(ctx, db, tmpTable, err)
	}
	if _, err := db.Exec(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", quoteIdent(table), quoteIdent(backupTable))); err != nil {
		cause := cleanupQuestRewriteFailure(ctx, db, tmpTable, fmt.Errorf("rename source to backup: %w", err))
		if clearErr := clearQuestRewriteSwapIntent(table); clearErr != nil {
			cause = fmt.Errorf("%w; clear swap intent: %v", cause, clearErr)
		}
		return cause
	}
	if _, err := db.Exec(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", quoteIdent(tmpTable), quoteIdent(table))); err != nil {
		recoveryCtx, cancelRecovery := questRewriteRecoveryContext(ctx)
		_, restoreErr := db.Exec(recoveryCtx, fmt.Sprintf("RENAME TABLE %s TO %s", quoteIdent(backupTable), quoteIdent(table)))
		cancelRecovery()
		if restoreErr != nil {
			return fmt.Errorf("activate compact table: %w; restore source failed: %w; backup=%s temp=%s", err, restoreErr, backupTable, tmpTable)
		}
		if cleanupErr := dropQuestRewriteTable(ctx, db, tmpTable); cleanupErr != nil {
			return fmt.Errorf("activate compact table: %w; source restored; temporary cleanup failed: %v; temp=%s", err, cleanupErr, tmpTable)
		}
		if clearErr := clearQuestRewriteSwapIntent(table); clearErr != nil {
			return fmt.Errorf("activate compact table: %w; source restored; clear swap intent: %v", err, clearErr)
		}
		return fmt.Errorf("activate compact table: %w; source restored; temp=%s", err, tmpTable)
	}
	if err := verifyQuestCompactRewriteSnapshot(ctx, db, table, meta, expected); err != nil {
		recoveryCtx, cancelRecovery := questRewriteRecoveryContext(ctx)
		_, moveErr := db.Exec(recoveryCtx, fmt.Sprintf("RENAME TABLE %s TO %s", quoteIdent(table), quoteIdent(tmpTable)))
		_, restoreErr := db.Exec(recoveryCtx, fmt.Sprintf("RENAME TABLE %s TO %s", quoteIdent(backupTable), quoteIdent(table)))
		cancelRecovery()
		if moveErr != nil || restoreErr != nil {
			return fmt.Errorf("verify activated compact table: %w; restore failed: move=%v restore=%v; backup=%s temp=%s", err, moveErr, restoreErr, backupTable, tmpTable)
		}
		if cleanupErr := dropQuestRewriteTable(ctx, db, tmpTable); cleanupErr != nil {
			return fmt.Errorf("verify activated compact table: %w; source restored; temporary cleanup failed: %v; temp=%s", err, cleanupErr, tmpTable)
		}
		if clearErr := clearQuestRewriteSwapIntent(table); clearErr != nil {
			return fmt.Errorf("verify activated compact table: %w; source restored; clear swap intent: %v", err, clearErr)
		}
		return fmt.Errorf("verify activated compact table: %w; source restored; temp=%s", err, tmpTable)
	}
	cleanupCtx, cancelCleanup := questRewriteRecoveryContext(ctx)
	_, cleanupErr := db.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE %s", quoteIdent(backupTable)))
	cancelCleanup()
	if cleanupErr != nil {
		return fmt.Errorf("drop compact backup %s: %w", backupTable, cleanupErr)
	}
	if err := clearQuestRewriteSwapIntent(table); err != nil {
		return fmt.Errorf("clear completed compact swap intent: %w", err)
	}
	return nil
}
