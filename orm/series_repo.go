package orm

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

type SeriesRepo interface {
	EnsureSeriesTable(ctx context.Context, info *SeriesInfo) *errs.Error
	InsertSeriesBatch(ctx context.Context, info *SeriesInfo, rows []*DataRecord) *errs.Error
	QuerySeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64, limit int) ([]*DataRecord, *errs.Error)
	DeleteSeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error
	UpdateSeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error
	UpdateSeriesCoverage(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64, rows []*DataRecord) *errs.Error
	GetSeriesRange(ctx context.Context, info *SeriesInfo, sid int32) (int64, int64, *errs.Error)
}

type SeriesRangeRepo interface {
	MissingSeriesRanges(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) ([]MSRange, *errs.Error)
}

type atomicSeriesWriter interface {
	WriteSeriesBatch(ctx context.Context, info *SeriesInfo, sid int32, rows []*DataRecord) *errs.Error
}

var defaultSeriesRepo SeriesRepo = &dbSeriesRepo{}

const seriesQuestRewriteDeleteRatio = 0.5

func DefaultSeriesRepo() SeriesRepo {
	return defaultSeriesRepo
}

type dbSeriesRepo struct{}

type rowScanner interface {
	Scan(dest ...any) error
}

// acquireQuestTableReadLock coordinates all logical access to a QuestDB series
// table with table replacement. The process lease is acquired first so the
// in-process lock cannot be held while waiting for another process.
func acquireQuestTableReadLock(ctx context.Context, table string) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return func() {}, nil
	}
	releaseProcess, err := acquireCompactProcessSharedLock(ctx, compactProcessLockRootFn(), table)
	if err != nil {
		return nil, err
	}
	tableLock := cptState.getTableLock(table)
	tableLock.RLock()
	return func() {
		tableLock.RUnlock()
		if err := releaseProcess(); err != nil {
			log.Error("release series table process read lock failed", zap.String("table", table), zap.Error(err))
		}
	}, nil
}

func acquireQuestTableWriteLock(ctx context.Context, table string) (func(), bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return func() {}, true, nil
	}
	releaseProcess, acquired, err := tryAcquireCompactProcessExclusiveLock(compactProcessLockRootFn(), table)
	if err != nil || !acquired {
		return nil, acquired, err
	}
	tableLock := cptState.getTableLock(table)
	tableLock.Lock()
	return func() {
		tableLock.Unlock()
		if err := releaseProcess(); err != nil {
			log.Error("release series table process write lock failed", zap.String("table", table), zap.Error(err))
		}
	}, true, nil
}

func (r *dbSeriesRepo) EnsureSeriesTable(ctx context.Context, info *SeriesInfo) *errs.Error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSeriesInfo(info); err != nil {
		return err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	binding := normalizedSeriesBinding(info.Binding)
	unlock, lockErr := acquireQuestTableReadLock(ctx, binding.Table)
	if lockErr != nil {
		return NewDbErr(core.ErrDbExecFail, lockErr)
	}
	defer unlock()
	return r.ensureSeriesTableLocked(ctx, q, info)
}

func (r *dbSeriesRepo) ensureSeriesTableLocked(ctx context.Context, q *Queries, info *SeriesInfo) *errs.Error {
	sqlText := buildSeriesTableDDL(info)
	if _, err_ := q.db.Exec(ctx, sqlText); err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	return nil
}

func (r *dbSeriesRepo) InsertSeriesBatch(ctx context.Context, info *SeriesInfo, rows []*DataRecord) *errs.Error {
	if len(rows) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSeriesInfo(info); err != nil {
		return err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	binding := normalizedSeriesBinding(info.Binding)
	unlock, lockErr := acquireQuestTableReadLock(ctx, binding.Table)
	if lockErr != nil {
		return NewDbErr(core.ErrDbExecFail, lockErr)
	}
	defer unlock()
	if err := r.ensureSeriesTableLocked(ctx, q, info); err != nil {
		return err
	}
	return r.insertSeriesBatchLocked(ctx, q, info, rows)
}

func (r *dbSeriesRepo) insertSeriesBatch(ctx context.Context, q *Queries, info *SeriesInfo, rows []*DataRecord) *errs.Error {
	binding := normalizedSeriesBinding(info.Binding)
	unlock, lockErr := acquireQuestTableReadLock(ctx, binding.Table)
	if lockErr != nil {
		return NewDbErr(core.ErrDbExecFail, lockErr)
	}
	defer unlock()
	return r.insertSeriesBatchLocked(ctx, q, info, rows)
}

func (r *dbSeriesRepo) insertSeriesBatchLocked(ctx context.Context, q *Queries, info *SeriesInfo, rows []*DataRecord) *errs.Error {
	binding := normalizedSeriesBinding(info.Binding)
	cols := []string{
		quoteIdent(binding.SIDColumn),
		quoteIdent(binding.TimeColumn),
		quoteIdent(binding.EndColumn),
	}
	var questVisible map[int32]map[int64]*DataRecord
	if IsQuestDB {
		questVisible = make(map[int32]map[int64]*DataRecord)
	}
	for _, field := range binding.Fields {
		cols = append(cols, quoteIdent(field.Name))
	}

	const batchRows = 200
	for start := 0; start < len(rows); start += batchRows {
		stop := min(len(rows), start+batchRows)
		args := make([]any, 0, (stop-start)*(3+len(binding.Fields)))
		var sb strings.Builder
		fmt.Fprintf(&sb, "INSERT INTO %s (%s) VALUES ", quoteIdent(binding.Table), strings.Join(cols, ", "))
		for rowIdx := start; rowIdx < stop; rowIdx++ {
			row := rows[rowIdx]
			if row == nil {
				return errs.NewMsg(core.ErrBadConfig, "series row is nil")
			}
			if row.Sid <= 0 {
				return errs.NewMsg(core.ErrBadConfig, "series row sid is required")
			}
			if row.EndMS <= row.TimeMS {
				return errs.NewMsg(core.ErrBadConfig, "series row end_ms must be greater than time_ms")
			}
			if IsQuestDB {
				rowsByTime := questVisible[row.Sid]
				if rowsByTime == nil {
					rowsByTime = make(map[int64]*DataRecord)
					questVisible[row.Sid] = rowsByTime
				}
				rowsByTime[row.TimeMS] = row
			}
			if rowIdx > start {
				sb.WriteByte(',')
			}
			sb.WriteByte('(')
			for colIdx := 0; colIdx < len(cols); colIdx++ {
				if colIdx > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(fmt.Sprintf("$%d", len(args)+colIdx+1))
			}
			sb.WriteByte(')')

			args = append(args, row.Sid)
			if IsQuestDB {
				args = append(args, time.UnixMilli(row.TimeMS).UTC())
			} else {
				args = append(args, row.TimeMS)
			}
			args = append(args, row.EndMS)
			for _, field := range binding.Fields {
				val := row.Values[field.Name]
				normVal, err_ := normalizeSeriesFieldValue(field.Type, val)
				if err_ != nil {
					return errs.NewMsg(core.ErrBadConfig, "series row field %q invalid: %v", field.Name, err_)
				}
				args = append(args, normVal)
			}
		}
		if !IsQuestDB {
			assigns := buildSeriesConflictAssignments(binding)
			fmt.Fprintf(&sb, " ON CONFLICT (%s, %s) DO UPDATE SET %s",
				quoteIdent(binding.SIDColumn), quoteIdent(binding.TimeColumn), strings.Join(assigns, ", "))
		}
		if _, err_ := q.db.Exec(ctx, sb.String(), args...); err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
	}
	if IsQuestDB {
		for sid, rowsByTime := range questVisible {
			pending := make([]*DataRecord, 0, len(rowsByTime))
			var wantTimeMS int64
			first := true
			for timeMS, row := range rowsByTime {
				pending = append(pending, row)
				if first || timeMS > wantTimeMS {
					wantTimeMS = timeMS
					first = false
				}
			}
			if err := waitForQuestSeriesVisible(ctx, q, info, sid, wantTimeMS, pending...); err != nil {
				return err
			}
		}
	}
	return nil
}

func buildSeriesConflictAssignments(binding SeriesBinding) []string {
	binding = normalizedSeriesBinding(binding)
	keyColumns := map[string]struct{}{
		binding.SIDColumn:  {},
		binding.TimeColumn: {},
	}
	seen := make(map[string]struct{}, len(binding.Fields)+1)
	assigns := make([]string, 0, len(binding.Fields)+1)
	add := func(name string) {
		if name == "" {
			return
		}
		if _, ok := keyColumns[name]; ok {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		quoted := quoteIdent(name)
		assigns = append(assigns, fmt.Sprintf("%s = EXCLUDED.%s", quoted, quoted))
	}
	add(binding.EndColumn)
	for _, field := range binding.Fields {
		add(field.Name)
	}
	return assigns
}

func (r *dbSeriesRepo) WriteSeriesBatch(ctx context.Context, info *SeriesInfo, sid int32, rows []*DataRecord) *errs.Error {
	if len(rows) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSeriesInfo(info); err != nil {
		return err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	binding := normalizedSeriesBinding(info.Binding)
	unlock, lockErr := acquireQuestTableReadLock(ctx, binding.Table)
	if lockErr != nil {
		return NewDbErr(core.ErrDbExecFail, lockErr)
	}
	defer unlock()
	if err := r.ensureSeriesTableLocked(ctx, q, info); err != nil {
		return err
	}
	write := q
	var tx pgx.Tx
	if !IsQuestDB {
		var txErr error
		tx, write, txErr = q.begin(ctx)
		if txErr != nil {
			return NewDbErr(core.ErrDbExecFail, txErr)
		}
		defer func() { _ = tx.Rollback(ctx) }()
	}
	oldEnds, oldErr := seriesRowEnds(ctx, write, info, sid, rows)
	if oldErr != nil {
		return oldErr
	}
	if err := r.insertSeriesBatchLocked(ctx, write, info, rows); err != nil {
		return err
	}
	for _, row := range rows {
		if oldEnd := oldEnds[row.TimeMS]; oldEnd > row.EndMS {
			if err := write.UpdateSRanges(ctx, sid, binding.Table, info.TimeFrame, row.EndMS, oldEnd, false); err != nil {
				return NewDbErr(core.ErrDbExecFail, err)
			}
		}
	}
	if err := write.updateSeriesCoverage(ctx, info, sid, rows[0].TimeMS, rows[len(rows)-1].EndMS, rows); err != nil {
		return err
	}
	if tx != nil {
		if err := tx.Commit(ctx); err != nil {
			return NewDbErr(core.ErrDbExecFail, err)
		}
	}
	return nil
}

func seriesRowEnds(ctx context.Context, q *Queries, info *SeriesInfo, sid int32, rows []*DataRecord) (map[int64]int64, *errs.Error) {
	binding := normalizedSeriesBinding(info.Binding)
	result := make(map[int64]int64)
	wanted := make(map[int64]bool, len(rows))
	for _, row := range rows {
		wanted[row.TimeMS] = true
	}
	startVal, endVal := any(rows[0].TimeMS), any(rows[len(rows)-1].TimeMS)
	timeProjection := quoteIdent(binding.TimeColumn)
	if IsQuestDB {
		startVal = time.UnixMilli(rows[0].TimeMS).UTC()
		endVal = time.UnixMilli(rows[len(rows)-1].TimeMS).UTC()
		timeProjection = fmt.Sprintf("cast(%s as long)/1000", quoteIdent(binding.TimeColumn))
	}
	query := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = $1 AND %s >= $2 AND %s <= $3",
		timeProjection, quoteIdent(binding.EndColumn), quoteIdent(binding.Table), quoteIdent(binding.SIDColumn),
		quoteIdent(binding.TimeColumn), quoteIdent(binding.TimeColumn))
	dbRows, err := q.db.Query(ctx, query, sid, startVal, endVal)
	if err != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err)
	}
	defer dbRows.Close()
	for dbRows.Next() {
		var timeMS, endMS int64
		if err := dbRows.Scan(&timeMS, &endMS); err != nil {
			return nil, NewDbErr(core.ErrDbReadFail, err)
		}
		if wanted[timeMS] && endMS > result[timeMS] {
			result[timeMS] = endMS
		}
	}
	if err := dbRows.Err(); err != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err)
	}
	return result, nil
}

func (r *dbSeriesRepo) QuerySeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64, limit int) ([]*DataRecord, *errs.Error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSeriesInfo(info); err != nil {
		return nil, err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	binding := normalizedSeriesBinding(info.Binding)
	unlock, lockErr := acquireQuestTableReadLock(ctx, binding.Table)
	if lockErr != nil {
		return nil, NewDbErr(core.ErrDbReadFail, lockErr)
	}
	defer unlock()
	var covered []MSRange
	timeExpr := quoteIdent(binding.TimeColumn)
	startArg, endArg := any(startMS), any(endMS)
	if IsQuestDB {
		timeExpr = fmt.Sprintf("cast(%s as long)/1000", quoteIdent(binding.TimeColumn))
		startArg = startMS * 1000
		endArg = endMS * 1000
		var err_ error
		covered, err_ = q.getCoveredRanges(ctx, sid, binding.Table, info.TimeFrame, startMS, endMS)
		if err_ != nil {
			return nil, NewDbErr(core.ErrDbReadFail, err_)
		}
		if len(covered) == 0 {
			return nil, nil
		}
	}
	selectCols := []string{
		quoteIdent(binding.SIDColumn),
		timeExpr,
		quoteIdent(binding.EndColumn),
	}
	for _, field := range binding.Fields {
		colExpr := quoteIdent(field.Name)
		if !IsQuestDB && field.Type == "json" {
			colExpr = fmt.Sprintf("%s::text", colExpr)
		}
		selectCols = append(selectCols, colExpr)
	}
	timeFilter := fmt.Sprintf("%s >= $2 AND %s < $3", quoteIdent(binding.TimeColumn), quoteIdent(binding.TimeColumn))
	if IsQuestDB {
		timeFilter = fmt.Sprintf("cast(%s as long) >= $2 AND cast(%s as long) < $3",
			quoteIdent(binding.TimeColumn), quoteIdent(binding.TimeColumn))
	}
	sqlText := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1 AND %s ORDER BY %s",
		strings.Join(selectCols, ", "),
		quoteIdent(binding.Table),
		quoteIdent(binding.SIDColumn),
		timeFilter,
		quoteIdent(binding.TimeColumn),
	)
	args := []any{sid, startArg, endArg}
	if limit > 0 && !IsQuestDB {
		sqlText += " LIMIT $4"
		args = append(args, limit)
	}
	rows, err_ := q.db.Query(ctx, sqlText, args...)
	if err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	defer rows.Close()

	var out []*DataRecord
	for rows.Next() {
		rec, err_ := scanSeriesRecord(rows, binding.Fields)
		if err_ != nil {
			return nil, NewDbErr(core.ErrDbReadFail, err_)
		}
		if IsQuestDB && !seriesRangeCovered(rec.TimeMS, covered) {
			continue
		}
		out = append(out, rec)
		if IsQuestDB && limit > 0 && len(out) >= limit {
			break
		}
	}
	if err_ := rows.Err(); err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	return out, nil
}

func (r *dbSeriesRepo) DeleteSeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	if startMS >= endMS {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSeriesInfo(info); err != nil {
		return err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	binding := normalizedSeriesBinding(info.Binding)
	if !IsQuestDB {
		tx, write, err_ := q.begin(ctx)
		if err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		sqlText := fmt.Sprintf("DELETE FROM %s WHERE %s = $1 AND %s >= $2 AND %s < $3",
			quoteIdent(binding.Table),
			quoteIdent(binding.SIDColumn),
			quoteIdent(binding.TimeColumn),
			quoteIdent(binding.TimeColumn),
		)
		if _, err_ := write.db.Exec(ctx, sqlText, sid, startMS, endMS); err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
		if err_ := write.UpdateSRanges(ctx, sid, binding.Table, info.TimeFrame, startMS, endMS, false); err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
		if err_ := tx.Commit(ctx); err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
		return nil
	}
	unlock, acquired, lockErr := acquireQuestTableWriteLock(ctx, binding.Table)
	if lockErr != nil {
		return NewDbErr(core.ErrDbExecFail, lockErr)
	}
	if !acquired {
		return errs.NewMsg(core.ErrRunTime, "series table %s is in use by another process", binding.Table)
	}
	defer unlock()

	if err_ := q.UpdateSRanges(ctx, sid, binding.Table, info.TimeFrame, startMS, endMS, false); err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	if err_ := waitForQuestSeriesCoverageDeletedWithQueries(ctx, q, info, sid, startMS, endMS); err_ != nil {
		return err_
	}
	if err_ := maybeRewriteQuestSeriesTableLocked(ctx, q, info, sid, startMS, endMS); err_ != nil {
		return err_
	}
	return nil
}

func (r *dbSeriesRepo) MissingSeriesRanges(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) ([]MSRange, *errs.Error) {
	if startMS >= endMS {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSeriesInfo(info); err != nil {
		return nil, err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	binding := normalizedSeriesBinding(info.Binding)
	unlock, lockErr := acquireQuestTableReadLock(ctx, binding.Table)
	if lockErr != nil {
		return nil, NewDbErr(core.ErrDbReadFail, lockErr)
	}
	defer unlock()
	spans, err_ := q.ListSRanges(ctx, sid, binding.Table, info.TimeFrame, startMS, endMS)
	if err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	return subtractMSRanges(MSRange{Start: startMS, Stop: endMS}, answeredSeriesRanges(spans)), nil
}

func answeredSeriesRanges(spans []*SRange) []MSRange {
	// Missing means unanswered, not data-free: known-empty spans are answers too.
	answered := make([]MSRange, 0, len(spans))
	for _, span := range spans {
		if span != nil && span.StopMs > span.StartMs {
			answered = append(answered, MSRange{Start: span.StartMs, Stop: span.StopMs})
		}
	}
	return answered
}

func seriesRangeCovered(timeMS int64, covered []MSRange) bool {
	for _, r := range covered {
		if timeMS < r.Start {
			return false
		}
		if timeMS >= r.Start && timeMS < r.Stop {
			return true
		}
	}
	return false
}

func maybeRewriteQuestSeriesTableLocked(ctx context.Context, q *Queries, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	binding := normalizedSeriesBinding(info.Binding)

	totalRows, err := countQuestSeriesRows(ctx, q, binding, sid, 0, 0)
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if totalRows == 0 {
		return nil
	}
	deleteRows, err := countQuestSeriesRows(ctx, q, binding, sid, startMS, endMS)
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if float64(deleteRows)/float64(totalRows) < seriesQuestRewriteDeleteRatio {
		return nil
	}
	covered, err := q.getCoveredRanges(ctx, sid, binding.Table, info.TimeFrame, 0, math.MaxInt64)
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	covered = mergeMSRanges(covered)
	return rewriteQuestSeriesTableLocked(ctx, q, info, sid, covered)
}

func countQuestSeriesRows(ctx context.Context, q *Queries, binding SeriesBinding, sid int32, startMS, endMS int64) (int64, error) {
	sqlText := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s = $1",
		quoteIdent(binding.Table),
		quoteIdent(binding.SIDColumn),
	)
	args := []any{sid}
	if startMS < endMS {
		sqlText += fmt.Sprintf(" AND cast(%s as long) >= $2 AND cast(%s as long) < $3",
			quoteIdent(binding.TimeColumn), quoteIdent(binding.TimeColumn))
		args = append(args, startMS*1000, endMS*1000)
	}
	var count int64
	err := q.db.QueryRow(ctx, sqlText, args...).Scan(&count)
	return count, err
}

func questRewriteSnapshotRowCount(counts map[int32]int64) int64 {
	var total int64
	for _, count := range counts {
		total += count
	}
	return total
}

func verifyQuestRewriteSourceStable(ctx context.Context, q *Queries, table string, sourceTxn int64) error {
	stableTxn, err := waitForCompactWalApplied(ctx, q.db, table)
	if err != nil {
		return err
	}
	if stableTxn != sourceTxn {
		return fmt.Errorf("source table changed during rewrite: table=%s before_txn=%d after_txn=%d", table, sourceTxn, stableTxn)
	}
	return nil
}

func rewriteQuestSeriesTableLocked(ctx context.Context, q *Queries, info *SeriesInfo, sid int32, covered []MSRange) *errs.Error {
	binding := normalizedSeriesBinding(info.Binding)
	tmpTable := fmt.Sprintf("%s_rewrite_%d_%06d", binding.Table, time.Now().UnixNano(), rand.Intn(1000000))
	backupTable := fmt.Sprintf("%s_backup_%d_%06d", binding.Table, time.Now().UnixNano(), rand.Intn(1000000))
	predicate := questSeriesRewritePredicate(binding, sid, covered)
	sourceTxn, err := waitForCompactWalApplied(ctx, q.db, binding.Table)
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	expected, err := captureQuestRewriteTableSnapshot(ctx, q, binding.Table, binding.SIDColumn, predicate)
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if err := verifyQuestRewriteSourceStable(ctx, q, binding.Table, sourceTxn); err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	createSQL, buildErr := buildQuestRewriteSQLChecked(tmpTable, binding.Table, predicate,
		seriesQuestPartitionBy(info.TimeFrame), binding.TimeColumn, expected.Columns)
	if buildErr != nil {
		return NewDbErr(core.ErrDbReadFail, buildErr)
	}
	if _, err := q.db.Exec(ctx, createSQL); err != nil {
		return NewDbErr(core.ErrDbExecFail, cleanupQuestRewriteFailure(ctx, q.db, tmpTable, fmt.Errorf("create series rewrite table: %w", err)))
	}
	if _, err := waitCompactVisibleCount(ctx, q.db, tmpTable, questRewriteSnapshotRowCount(expected.Counts)); err != nil {
		return NewDbErr(core.ErrDbReadFail, cleanupQuestRewriteFailure(ctx, q.db, tmpTable, err))
	}
	if err := verifyQuestRewriteTableSnapshot(ctx, q, tmpTable, binding.SIDColumn, "", expected); err != nil {
		cause := fmt.Errorf("verify series rewrite table: %s", err.Short())
		return NewDbErr(core.ErrDbReadFail, cleanupQuestRewriteFailure(ctx, q.db, tmpTable, cause))
	}
	return replaceVerifiedQuestTable(ctx, q, binding.Table, tmpTable, backupTable, binding.SIDColumn, predicate, expected)
}

func questSeriesRewritePredicate(binding SeriesBinding, sid int32, covered []MSRange) string {
	sidCol := quoteIdent(binding.SIDColumn)
	timeCol := quoteIdent(binding.TimeColumn)
	var keepTarget []string
	for _, r := range covered {
		if r.Stop <= r.Start {
			continue
		}
		keepTarget = append(keepTarget, fmt.Sprintf("(%s = %d AND cast(%s as long) >= %d AND cast(%s as long) < %d)",
			sidCol, sid, timeCol, r.Start*1000, timeCol, r.Stop*1000))
	}
	if len(keepTarget) == 0 {
		return fmt.Sprintf("%s <> %d", sidCol, sid)
	}
	return fmt.Sprintf("(%s <> %d OR %s)", sidCol, sid, strings.Join(keepTarget, " OR "))
}

func (r *dbSeriesRepo) UpdateSeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSeriesInfo(info); err != nil {
		return err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	binding := normalizedSeriesBinding(info.Binding)
	unlock, lockErr := acquireQuestTableReadLock(ctx, binding.Table)
	if lockErr != nil {
		return NewDbErr(core.ErrDbExecFail, lockErr)
	}
	defer unlock()
	if err_ := q.UpdateSRanges(ctx, sid, binding.Table, info.TimeFrame, startMS, endMS, true); err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	return nil
}

func (r *dbSeriesRepo) UpdateSeriesCoverage(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64, rows []*DataRecord) *errs.Error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSeriesInfo(info); err != nil {
		return err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	binding := normalizedSeriesBinding(info.Binding)
	unlock, lockErr := acquireQuestTableReadLock(ctx, binding.Table)
	if lockErr != nil {
		return NewDbErr(core.ErrDbExecFail, lockErr)
	}
	defer unlock()
	return q.updateSeriesCoverage(ctx, info, sid, startMS, endMS, rows)
}

func (q *Queries) updateSeriesCoverage(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64, rows []*DataRecord) *errs.Error {
	binding := normalizedSeriesBinding(info.Binding)
	holes, err := seriesCoverageHoles(sid, startMS, endMS, rows)
	if err != nil {
		return err
	}
	if err := q.UpdateSRangesWithHoles(ctx, sid, binding.Table, info.TimeFrame, startMS, endMS, holes); err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
}

func (r *dbSeriesRepo) GetSeriesRange(ctx context.Context, info *SeriesInfo, sid int32) (int64, int64, *errs.Error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSeriesInfo(info); err != nil {
		return 0, 0, err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Release()
	binding := normalizedSeriesBinding(info.Binding)
	if !IsQuestDB {
		start, stop := getKlineRangePg(ctx, sid, binding.Table, info.TimeFrame)
		return start, stop, nil
	}
	unlock, lockErr := acquireQuestTableReadLock(ctx, binding.Table)
	if lockErr != nil {
		return 0, 0, NewDbErr(core.ErrDbReadFail, lockErr)
	}
	defer unlock()
	covered, err_ := q.getCoveredRanges(ctx, sid, binding.Table, info.TimeFrame, 0, math.MaxInt64)
	if err_ != nil {
		return 0, 0, NewDbErr(core.ErrDbReadFail, err_)
	}
	if len(covered) == 0 {
		return 0, 0, nil
	}
	return covered[0].Start, covered[len(covered)-1].Stop, nil
}

func seriesQuestPartitionBy(tf string) string {
	switch tf {
	case "1m":
		return "WEEK"
	case "1h", "1d":
		return "YEAR"
	default:
		return "MONTH"
	}
}

type questSeriesVisibleRow struct {
	sid    int32
	endMS  int64
	values map[string]any
}

func waitForQuestSeriesVisible(ctx context.Context, q *Queries, info *SeriesInfo, sid int32, wantTimeMS int64, pendingRows ...*DataRecord) *errs.Error {
	binding := normalizedSeriesBinding(info.Binding)
	applied, err := waitForQuestCondition(ctx, 5*time.Second, questReadAfterWritePollInterval, func() (bool, error) {
		var sequencerTxn, writerTxn int64
		err := q.db.QueryRow(ctx, `SELECT sequencerTxn, writerTxn FROM wal_tables() WHERE name = $1`,
			binding.Table).Scan(&sequencerTxn, &writerTxn)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return writerTxn >= sequencerTxn, err
	})
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if !applied {
		return errs.NewMsg(core.ErrDbReadFail, "questdb series WAL not applied in time: table=%s", binding.Table)
	}
	if len(pendingRows) == 0 {
		return waitForQuestSeriesKeyVisible(ctx, q, binding, sid, wantTimeMS)
	}
	expected := make(map[int64]questSeriesVisibleRow, len(pendingRows))
	minTimeMS, maxTimeMS := int64(0), int64(0)
	for _, pending := range pendingRows {
		values := make(map[string]any, len(binding.Fields))
		for _, field := range binding.Fields {
			normVal, valueErr := normalizeSeriesFieldValue(field.Type, pending.Values[field.Name])
			if valueErr != nil {
				return errs.NewMsg(core.ErrBadConfig, "series row field %q invalid: %v", field.Name, valueErr)
			}
			values[field.Name] = normVal
		}
		expected[pending.TimeMS] = questSeriesVisibleRow{
			sid:    pending.Sid,
			endMS:  pending.EndMS,
			values: values,
		}
		if len(expected) == 1 || pending.TimeMS < minTimeMS {
			minTimeMS = pending.TimeMS
		}
		if len(expected) == 1 || pending.TimeMS > maxTimeMS {
			maxTimeMS = pending.TimeMS
		}
	}
	selectCols := []string{
		quoteIdent(binding.SIDColumn),
		fmt.Sprintf("cast(%s as long)/1000", quoteIdent(binding.TimeColumn)),
		quoteIdent(binding.EndColumn),
	}
	for _, field := range binding.Fields {
		selectCols = append(selectCols, quoteIdent(field.Name))
	}
	var sqlText string
	var args []any
	if len(expected) == 1 {
		sqlText = fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1 AND %s = $2",
			strings.Join(selectCols, ", "),
			quoteIdent(binding.Table),
			quoteIdent(binding.SIDColumn),
			quoteIdent(binding.TimeColumn),
		)
		args = []any{sid, time.UnixMilli(minTimeMS).UTC()}
	} else {
		sqlText = fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1 AND %s >= $2 AND %s <= $3",
			strings.Join(selectCols, ", "),
			quoteIdent(binding.Table),
			quoteIdent(binding.SIDColumn),
			quoteIdent(binding.TimeColumn),
			quoteIdent(binding.TimeColumn),
		)
		args = []any{sid, time.UnixMilli(minTimeMS).UTC(), time.UnixMilli(maxTimeMS).UTC()}
	}
	ok, err := waitForQuestCondition(ctx, 5*time.Second, questReadAfterWritePollInterval, func() (bool, error) {
		if len(expected) == 1 {
			var got *DataRecord
			for timeMS, want := range expected {
				rec, scanErr := scanSeriesRecord(q.db.QueryRow(ctx, sqlText, args...), binding.Fields)
				if errors.Is(scanErr, pgx.ErrNoRows) {
					return false, nil
				}
				if scanErr != nil {
					return false, scanErr
				}
				got = rec
				return rec.Sid == want.sid && rec.TimeMS == timeMS && rec.EndMS == want.endMS &&
					reflect.DeepEqual(rec.Values, want.values), nil
			}
			return got != nil, nil
		}
		dbRows, queryErr := q.db.Query(ctx, sqlText, args...)
		if errors.Is(queryErr, pgx.ErrNoRows) {
			return false, nil
		}
		if queryErr != nil {
			return false, queryErr
		}
		defer dbRows.Close()
		seen := make(map[int64]struct{}, len(expected))
		for dbRows.Next() {
			rec, scanErr := scanSeriesRecord(dbRows, binding.Fields)
			if errors.Is(scanErr, pgx.ErrNoRows) {
				return false, nil
			}
			if scanErr != nil {
				return false, scanErr
			}
			want, ok := expected[rec.TimeMS]
			if !ok || rec.Sid != want.sid {
				continue
			}
			if rec.EndMS != want.endMS || !reflect.DeepEqual(rec.Values, want.values) {
				return false, nil
			}
			seen[rec.TimeMS] = struct{}{}
		}
		if err := dbRows.Err(); err != nil {
			return false, err
		}
		return len(seen) == len(expected), nil
	})
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if ok {
		return nil
	}
	return errs.NewMsg(core.ErrDbReadFail, "questdb series rows not visible in time: table=%s sid=%d time_ms=%d",
		binding.Table, sid, wantTimeMS)
}

func waitForQuestSeriesKeyVisible(ctx context.Context, q *Queries, binding SeriesBinding, sid int32, wantTimeMS int64) *errs.Error {
	sqlText := fmt.Sprintf("SELECT cast(%s as long)/1000 FROM %s WHERE %s = $1 AND %s = $2",
		quoteIdent(binding.TimeColumn),
		quoteIdent(binding.Table),
		quoteIdent(binding.SIDColumn),
		quoteIdent(binding.TimeColumn),
	)
	ok, err := waitForQuestCondition(ctx, 5*time.Second, questReadAfterWritePollInterval, func() (bool, error) {
		var visibleTime *int64
		if err := q.db.QueryRow(ctx, sqlText, sid, time.UnixMilli(wantTimeMS).UTC()).Scan(&visibleTime); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
			return false, err
		}
		return visibleTime != nil && *visibleTime == wantTimeMS, nil
	})
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if ok {
		return nil
	}
	return errs.NewMsg(core.ErrDbReadFail, "questdb series rows not visible in time: table=%s sid=%d time_ms=%d",
		binding.Table, sid, wantTimeMS)
}

func waitForQuestSeriesCoverageDeleted(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	if ctx == nil {
		ctx = context.Background()
	}
	q, conn, connErr := Conn(ctx)
	if connErr != nil {
		return connErr
	}
	defer conn.Release()
	return waitForQuestSeriesCoverageDeletedWithQueries(ctx, q, info, sid, startMS, endMS)
}

func waitForQuestSeriesCoverageDeletedWithQueries(ctx context.Context, q *Queries, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	binding := normalizedSeriesBinding(info.Binding)
	ok, err := waitForQuestCondition(ctx, 5*time.Second, questReadAfterWritePollInterval, func() (bool, error) {
		deleted, queryErr := questSeriesDeleteMarkerVisible(ctx, q, binding.Table, info.TimeFrame, sid, startMS, endMS)
		if queryErr != nil {
			if errors.Is(queryErr, pgx.ErrNoRows) {
				return false, nil
			}
			return false, queryErr
		}
		if !deleted {
			return false, nil
		}
		covered, queryErr := questSeriesCoveredRangesFromDB(ctx, q, binding.Table, info.TimeFrame, sid, startMS, endMS)
		if queryErr != nil {
			if errors.Is(queryErr, pgx.ErrNoRows) {
				return false, nil
			}
			return false, queryErr
		}
		return len(covered) == 0, nil
	})
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if ok {
		return nil
	}
	return errs.NewMsg(core.ErrDbReadFail, "questdb series delete coverage not visible in time: table=%s sid=%d range=[%d,%d)",
		binding.Table, sid, startMS, endMS)
}

func questSeriesCoveredRangesFromDB(ctx context.Context, q *Queries, table, timeframe string, sid int32, startMS, endMS int64) ([]MSRange, error) {
	unlock := LockCompactTableRead("sranges_q")
	defer unlock()
	spans, err := q.loadSRangesSpansFromDB(ctx, sid, table, timeframe, startMS, endMS)
	if err != nil {
		return nil, err
	}
	out := make([]MSRange, 0, len(spans))
	for _, s := range spans {
		if s.HasData && s.StopMs > s.StartMs {
			out = append(out, MSRange{Start: s.StartMs, Stop: s.StopMs})
		}
	}
	return mergeMSRanges(out), nil
}

func questSeriesDeleteMarkerVisible(ctx context.Context, q *Queries, table, timeframe string, sid int32, startMS, endMS int64) (bool, error) {
	unlock := LockCompactTableRead("sranges_q")
	defer unlock()
	rows, err := q.db.Query(ctx, `SELECT start_ms, stop_ms, has_data
FROM (
  SELECT start_ms, stop_ms, has_data, is_deleted
  FROM sranges_q
  LATEST BY sid, tbl, timeframe, start_ms
  WHERE sid = $1 AND tbl = $2 AND timeframe = $3 AND stop_ms > $4 AND start_ms < $5
)
WHERE coalesce(is_deleted, false) = false`,
		sid, table, timeframe, startMS, endMS)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	holes := make([]MSRange, 0)
	for rows.Next() {
		var spanStart, spanStop int64
		var hasData bool
		if err := rows.Scan(&spanStart, &spanStop, &hasData); err != nil {
			return false, err
		}
		if !hasData && spanStop > spanStart {
			holes = append(holes, MSRange{Start: spanStart, Stop: spanStop})
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	missing := subtractMSRanges(MSRange{Start: startMS, Stop: endMS}, holes)
	return len(missing) == 0, nil
}

func buildSeriesTableDDL(info *SeriesInfo) string {
	binding := normalizedSeriesBinding(info.Binding)
	colDefs := []string{
		fmt.Sprintf("%s INT NOT NULL", quoteIdent(binding.SIDColumn)),
		fmt.Sprintf("%s %s NOT NULL", quoteIdent(binding.TimeColumn), seriesTimeSQLType()),
		fmt.Sprintf("%s %s NOT NULL", quoteIdent(binding.EndColumn), seriesSQLType("int")),
	}
	for _, field := range binding.Fields {
		colDefs = append(colDefs, fmt.Sprintf("%s %s", quoteIdent(field.Name), seriesSQLType(field.Type)))
	}
	if IsQuestDB {
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) timestamp(%s) PARTITION BY %s WAL DEDUP UPSERT KEYS(%s, %s)",
			quoteIdent(binding.Table),
			strings.Join(colDefs, ", "),
			quoteIdent(binding.TimeColumn),
			seriesQuestPartitionBy(info.TimeFrame),
			quoteIdent(binding.SIDColumn),
			quoteIdent(binding.TimeColumn),
		)
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY (%s, %s))",
		quoteIdent(binding.Table),
		strings.Join(colDefs, ", "),
		quoteIdent(binding.SIDColumn),
		quoteIdent(binding.TimeColumn),
	)
}

func scanSeriesRecord(rows rowScanner, fields []SeriesField) (*DataRecord, error) {
	rec := &DataRecord{Values: make(map[string]any, len(fields))}
	targets := make([]any, 0, 3+len(fields))
	targets = append(targets, &rec.Sid, &rec.TimeMS, &rec.EndMS)
	fieldTargets := make([]any, len(fields))
	for i, field := range fields {
		switch field.Type {
		case "float":
			var val sql.NullFloat64
			fieldTargets[i] = &val
		case "int":
			var val sql.NullInt64
			fieldTargets[i] = &val
		case "string", "json":
			var val sql.NullString
			fieldTargets[i] = &val
		case "bool":
			var val sql.NullBool
			fieldTargets[i] = &val
		default:
			return nil, fmt.Errorf("unsupported series field type %q", field.Type)
		}
		targets = append(targets, fieldTargets[i])
	}
	if err := rows.Scan(targets...); err != nil {
		return nil, err
	}
	rec.Closed = true
	for i, field := range fields {
		rec.Values[field.Name] = nil
		switch field.Type {
		case "float":
			if val := fieldTargets[i].(*sql.NullFloat64); val.Valid {
				rec.Values[field.Name] = val.Float64
			}
		case "int":
			if val := fieldTargets[i].(*sql.NullInt64); val.Valid {
				rec.Values[field.Name] = val.Int64
			}
		case "string", "json":
			if val := fieldTargets[i].(*sql.NullString); val.Valid {
				rec.Values[field.Name] = val.String
			}
		case "bool":
			if val := fieldTargets[i].(*sql.NullBool); val.Valid {
				rec.Values[field.Name] = val.Bool
			}
		}
	}
	return rec, nil
}

func validateSeriesInfo(info *SeriesInfo) *errs.Error {
	if info == nil {
		return errs.NewMsg(core.ErrBadConfig, "series info is required")
	}
	if info.TimeFrame == "" {
		return errs.NewMsg(core.ErrBadConfig, "series timeframe is required")
	}
	binding := normalizedSeriesBinding(info.Binding)
	if binding.Table == "" || binding.TimeColumn == "" || binding.EndColumn == "" {
		return errs.NewMsg(core.ErrBadConfig, "series binding must define table/time/end columns")
	}
	seen := map[string]bool{
		binding.TimeColumn: true,
		binding.EndColumn:  true,
		binding.SIDColumn:  true,
	}
	for _, field := range binding.Fields {
		if field.Name == "" {
			return errs.NewMsg(core.ErrBadConfig, "series field name is required")
		}
		if seen[field.Name] {
			return errs.NewMsg(core.ErrBadConfig, "duplicate series column %q", field.Name)
		}
		seen[field.Name] = true
		switch field.Type {
		case "float", "int", "string", "bool", "json":
		default:
			return errs.NewMsg(core.ErrBadConfig, "unsupported series field type %q", field.Type)
		}
	}
	return nil
}

func normalizedSeriesBinding(binding SeriesBinding) SeriesBinding {
	if binding.SIDColumn == "" {
		binding.SIDColumn = "sid"
	}
	return binding
}

func seriesTimeSQLType() string {
	if IsQuestDB {
		return "TIMESTAMP"
	}
	return "BIGINT"
}

func seriesSQLType(fieldType string) string {
	if IsQuestDB {
		switch fieldType {
		case "float":
			return "DOUBLE"
		case "int":
			return "LONG"
		case "string", "json":
			return "STRING"
		case "bool":
			return "BOOLEAN"
		}
	}
	switch fieldType {
	case "float":
		return "DOUBLE PRECISION"
	case "int":
		return "BIGINT"
	case "string":
		return "TEXT"
	case "bool":
		return "BOOLEAN"
	case "json":
		return "JSONB"
	default:
		return "TEXT"
	}
}

func normalizeSeriesFieldValue(fieldType string, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch fieldType {
	case "float":
		return utils.ToFloat64(value)
	case "int":
		return utils.ToInt64(value)
	case "string":
		return fmt.Sprint(value), nil
	case "bool":
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			return strconv.ParseBool(v)
		default:
			return nil, fmt.Errorf("unsupported bool type %T", value)
		}
	case "json":
		switch v := value.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		default:
			data, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}
			return string(data), nil
		}
	default:
		return nil, fmt.Errorf("unsupported field type %q", fieldType)
	}
}

func quoteIdent(name string) string {
	return pgx.Identifier{name}.Sanitize()
}
