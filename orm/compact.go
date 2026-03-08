package orm

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

// TableCompactMeta holds per-table configuration for the auto-compact mechanism.
// Each QuestDB logical-deletion table registers its schema metadata here so that
// MaybeCompact can build the correct CREATE TABLE ... AS (SELECT ... LATEST BY ...)
// statement without hardcoding SQL per table.
type TableCompactMeta struct {
	LatestByKeys string        // e.g. "sid, tbl, timeframe, start_ms"
	SelectCols   string        // columns to copy (excluding is_deleted/deleted_at)
	PartitionBy  string        // e.g. "MONTH"
	DedupKeys    string        // e.g. "sid, tbl, timeframe, start_ms, ts"
	Cooldown     time.Duration // per-table cooldown between checks
}

var compactTables = map[string]*TableCompactMeta{
	"exsymbol_q": {
		LatestByKeys: "sid",
		SelectCols:   "sid, ts, exchange, exg_real, market, symbol, combined, list_ms, delist_ms",
		PartitionBy:  "YEAR",
		DedupKeys:    "sid, ts",
		Cooldown:     12 * time.Hour,
	},
	"calendars_q": {
		LatestByKeys: "market, start_ms",
		SelectCols:   "ts, market, start_ms, stop_ms",
		PartitionBy:  "YEAR",
		DedupKeys:    "market, start_ms, ts",
		Cooldown:     8 * time.Hour,
	},
	"adj_factors_q": {
		LatestByKeys: "sid, sub_id, start_ms",
		SelectCols:   "ts, sid, sub_id, start_ms, factor",
		PartitionBy:  "MONTH",
		DedupKeys:    "sid, sub_id, start_ms, ts",
		Cooldown:     12 * time.Hour,
	},
	"sranges_q": {
		LatestByKeys: "sid, tbl, timeframe, start_ms",
		SelectCols:   "sid, ts, tbl, timeframe, start_ms, stop_ms, has_data",
		PartitionBy:  "MONTH",
		DedupKeys:    "sid, tbl, timeframe, start_ms, ts",
		Cooldown:     2 * time.Hour,
	},
	"ins_kline_q": {
		LatestByKeys: "sid, timeframe",
		SelectCols:   "sid, timeframe, ts, start_ms, stop_ms",
		PartitionBy:  "DAY",
		DedupKeys:    "sid, timeframe, ts",
		Cooldown:     4 * time.Hour,
	},
	"kline_un_q": {
		LatestByKeys: "sid, timeframe",
		SelectCols:   "sid, timeframe, ts, stop_ms, expire_ms, open, high, low, close, volume, quote, buy_volume, trade_num",
		PartitionBy:  "MONTH",
		DedupKeys:    "sid, timeframe, ts",
		Cooldown:     4 * time.Hour,
	},
}

const (
	compactCheckProb   = 0.02
	compactRatioThresh = 0.35
	compactMinRows     = 500
)

type compactState struct {
	mu          sync.Mutex
	lastCheckAt map[string]time.Time
	tableLocks  map[string]*sync.RWMutex
}

var cptState = &compactState{
	lastCheckAt: make(map[string]time.Time),
	tableLocks:  make(map[string]*sync.RWMutex),
}

func (s *compactState) getTableLock(table string) *sync.RWMutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l, ok := s.tableLocks[table]; ok {
		return l
	}
	l := &sync.RWMutex{}
	s.tableLocks[table] = l
	return l
}

// MaybeCompact should be called after logical-delete writes.
// It probabilistically triggers a compact check, guarded by per-table cooldown.
func MaybeCompact(tableName string) {
	meta, ok := compactTables[tableName]
	if !ok {
		return
	}
	cptState.mu.Lock()
	last := cptState.lastCheckAt[tableName]
	if time.Since(last) < meta.Cooldown {
		cptState.mu.Unlock()
		return
	}
	if rand.Float64() > compactCheckProb {
		cptState.mu.Unlock()
		return
	}
	cptState.lastCheckAt[tableName] = time.Now()
	cptState.mu.Unlock()

	go doCompactCheck(tableName, meta)
}

func doCompactCheck(tableName string, meta *TableCompactMeta) {
	ctx := context.Background()

	var totalRows int64
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&totalRows); err != nil {
		log.Warn("compact_check: count total fail", zap.String("table", tableName), zap.Error(err))
		return
	}
	if totalRows < compactMinRows {
		return
	}

	validSQL := fmt.Sprintf(`SELECT count(*) FROM (
  SELECT %s FROM %s LATEST BY %s WHERE coalesce(is_deleted, false) = false
)`, meta.SelectCols, tableName, meta.LatestByKeys)
	var validRows int64
	if err := pool.QueryRow(ctx, validSQL).Scan(&validRows); err != nil {
		log.Warn("compact_check: count valid fail", zap.String("table", tableName), zap.Error(err))
		return
	}

	ratio := float64(validRows) / float64(totalRows)
	if ratio >= compactRatioThresh {
		log.Info("compact_check", zap.String("table", tableName),
			zap.Int64("total", totalRows), zap.Int64("valid", validRows),
			zap.String("ratio", fmt.Sprintf("%.3f", ratio)), zap.String("action", "skip"))
		return
	}

	log.Info("compact_check", zap.String("table", tableName),
		zap.Int64("total", totalRows), zap.Int64("valid", validRows),
		zap.String("ratio", fmt.Sprintf("%.3f", ratio)), zap.String("action", "compact"))

	execCompact(ctx, tableName, meta, totalRows)
}

func execCompact(ctx context.Context, tableName string, meta *TableCompactMeta, beforeTotal int64) {
	start := time.Now()
	tmpTable := tableName + "_new"

	createSQL := fmt.Sprintf(`CREATE TABLE %s AS (
  SELECT %s,
         cast(false as boolean) as is_deleted,
         cast(null as timestamp) as deleted_at
  FROM %s
  LATEST BY %s
  WHERE coalesce(is_deleted, false) = false
) TIMESTAMP(ts) PARTITION BY %s WAL
DEDUP UPSERT KEYS(%s)`,
		tmpTable, meta.SelectCols, tableName, meta.LatestByKeys, meta.PartitionBy, meta.DedupKeys)

	if _, err := pool.Exec(ctx, createSQL); err != nil {
		log.Error("compact_error", zap.String("table", tableName),
			zap.String("step", "create_new"), zap.Error(err))
		return
	}

	var newCount int64
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", tmpTable)).Scan(&newCount); err != nil {
		log.Error("compact_error", zap.String("table", tableName),
			zap.String("step", "verify_count"), zap.Error(err))
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable))
		return
	}
	if newCount == 0 && beforeTotal > compactMinRows {
		log.Error("compact_error", zap.String("table", tableName),
			zap.String("step", "verify_empty"),
			zap.String("msg", "new table has 0 rows but old table had data, aborting"))
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable))
		return
	}

	tblLock := cptState.getTableLock(tableName)
	tblLock.Lock()
	defer tblLock.Unlock()

	if _, err := pool.Exec(ctx, fmt.Sprintf("DROP TABLE %s", tableName)); err != nil {
		log.Error("compact_error", zap.String("table", tableName),
			zap.String("step", "drop_old"), zap.Error(err))
		return
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", tmpTable, tableName)); err != nil {
		log.Error("compact_error", zap.String("table", tableName),
			zap.String("step", "rename"),
			zap.Error(err),
			zap.String("recovery", "old table dropped, new data in "+tmpTable))
		return
	}

	elapsed := time.Since(start)
	log.Info("compact_done", zap.String("table", tableName),
		zap.Int64("before", beforeTotal), zap.Int64("after", newCount),
		zap.String("ratio", fmt.Sprintf("%.3f", float64(newCount)/float64(max(beforeTotal, 1)))),
		zap.Duration("elapsed", elapsed))
}
