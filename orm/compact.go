package orm

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/banbox/banexg/log"
	"github.com/jackc/pgx/v5"
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
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
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
  SELECT %s FROM %s LATEST BY %s WHERE coalesce(is_deleted, false) = false
)`, meta.SelectCols, table, meta.LatestByKeys)
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
	deadline := time.Now().Add(compactVerifyTimeout)
	for {
		walTxn, applied, suspended, err := compactWalApplied(ctx, db, table)
		if err != nil {
			return 0, err
		}
		if applied {
			return walTxn, nil
		}
		if suspended {
			return 0, fmt.Errorf("source WAL table is suspended: %s", table)
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("source WAL not fully applied before compact timeout: table=%s timeout=%s", table, compactVerifyTimeout)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(compactVerifyPollInterval):
		}
	}
}

func queryCompactRowCount(ctx context.Context, db compactDB, table string) (int64, error) {
	var count int64
	err := db.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&count)
	return count, err
}

func waitCompactVisibleCount(ctx context.Context, db compactDB, table string, expected int64) (int64, error) {
	deadline := time.Now().Add(compactVerifyTimeout)
	var lastCount int64
	for {
		count, err := queryCompactRowCount(ctx, db, table)
		if err != nil {
			return 0, err
		}
		lastCount = count
		if count == expected {
			return count, nil
		}
		if count > expected {
			return count, fmt.Errorf("new table row count exceeded expected snapshot: got=%d expected=%d", count, expected)
		}
		if time.Now().After(deadline) {
			return count, fmt.Errorf("new WAL table rows not fully visible before timeout: got=%d expected=%d timeout=%s", count, expected, compactVerifyTimeout)
		}
		select {
		case <-ctx.Done():
			return lastCount, ctx.Err()
		case <-time.After(compactVerifyPollInterval):
		}
	}
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
	createSQL := fmt.Sprintf(`CREATE TABLE %s AS (
  SELECT %s,
         cast(false as boolean) as is_deleted
  FROM %s
  LATEST BY %s
  WHERE coalesce(is_deleted, false) = false
) TIMESTAMP(ts) PARTITION BY %s WAL
DEDUP UPSERT KEYS(%s)`, tmpTable, meta.SelectCols, table, meta.LatestByKeys, meta.PartitionBy, meta.DedupKeys)

	if _, err := db.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("create compact table: %w", err)
	}
	newCount, err := waitCompactVisibleCount(ctx, db, tmpTable, expectedRows)
	if err != nil {
		_, _ = db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable))
		return fmt.Errorf("verify compact table: %w", err)
	}
	stableTxn, err := waitForCompactWalApplied(ctx, db, table)
	if err != nil {
		_, _ = db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable))
		return err
	}
	if stableTxn != sourceTxn {
		_, _ = db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable))
		return fmt.Errorf("source table changed during compact: table=%s before_txn=%d after_txn=%d", table, sourceTxn, stableTxn)
	}
	if _, err := db.Exec(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", table, backupTable)); err != nil {
		_, _ = db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable))
		return fmt.Errorf("rename source to backup: %w", err)
	}
	if _, err := db.Exec(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", tmpTable, table)); err != nil {
		_, restoreErr := db.Exec(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", backupTable, table))
		if restoreErr != nil {
			return fmt.Errorf("activate compact table: %w; restore source failed: %v; backup=%s temp=%s", err, restoreErr, backupTable, tmpTable)
		}
		return fmt.Errorf("activate compact table: %w; source restored; temp=%s", err, tmpTable)
	}
	if _, err := db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", backupTable)); err != nil {
		log.Warn("compact backup cleanup failed", zap.String("table", table), zap.String("backup", backupTable), zap.Error(err))
	}
	log.Info("compact_done",
		zap.String("table", table),
		zap.Int64("before", beforeTotal),
		zap.Int64("after", newCount),
		zap.String("ratio", fmt.Sprintf("%.3f", float64(newCount)/float64(max(beforeTotal, 1)))),
		zap.Duration("elapsed", time.Since(start)))
	return nil
}
