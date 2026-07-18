package orm

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/jackc/pgx/v5"
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

var defaultSeriesRepo SeriesRepo = &dbSeriesRepo{}

const seriesQuestRewriteDeleteRatio = 0.5

func DefaultSeriesRepo() SeriesRepo {
	return defaultSeriesRepo
}

type dbSeriesRepo struct{}

type seriesTableLockContextKey struct{}
type seriesTableWriteLockContextKey struct{}

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
	if err := r.EnsureSeriesTable(ctx, info); err != nil {
		return err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	binding := normalizedSeriesBinding(info.Binding)
	if IsQuestDB && !skipSeriesTableReadLock(ctx) {
		tblLock := cptState.getTableLock(binding.Table)
		tblLock.RLock()
		defer tblLock.RUnlock()
	}
	cols := []string{
		quoteIdent(binding.SIDColumn),
		quoteIdent(binding.TimeColumn),
		quoteIdent(binding.EndColumn),
	}
	questVisible := make(map[int32]int64)
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
			if row.TimeMS > questVisible[row.Sid] {
				questVisible[row.Sid] = row.TimeMS
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
				val, ok := row.Values[field.Name]
				if !ok {
					return errs.NewMsg(core.ErrBadConfig, "series row missing field %q", field.Name)
				}
				normVal, err_ := normalizeSeriesFieldValue(field.Type, val)
				if err_ != nil {
					return errs.NewMsg(core.ErrBadConfig, "series row field %q invalid: %v", field.Name, err_)
				}
				args = append(args, normVal)
			}
		}
		if !IsQuestDB {
			var assigns []string
			assigns = append(assigns, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(binding.EndColumn), quoteIdent(binding.EndColumn)))
			for _, field := range binding.Fields {
				assigns = append(assigns, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(field.Name), quoteIdent(field.Name)))
			}
			fmt.Fprintf(&sb, " ON CONFLICT (%s, %s) DO UPDATE SET %s",
				quoteIdent(binding.SIDColumn), quoteIdent(binding.TimeColumn), strings.Join(assigns, ", "))
		}
		if _, err_ := q.db.Exec(ctx, sb.String(), args...); err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
	}
	if IsQuestDB {
		for sid, wantTimeMS := range questVisible {
			if err := waitForQuestSeriesVisible(ctx, q, info, sid, wantTimeMS); err != nil {
				return err
			}
		}
	}
	return nil
}

func withSeriesTableReadLockSkipped(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, seriesTableLockContextKey{}, true)
}

func skipSeriesTableReadLock(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	val, _ := ctx.Value(seriesTableLockContextKey{}).(bool)
	return val
}

func withSeriesTableWriteLockHeld(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, seriesTableWriteLockContextKey{}, true)
}

func seriesTableWriteLockHeld(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	val, _ := ctx.Value(seriesTableWriteLockContextKey{}).(bool)
	return val
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
		sqlText := fmt.Sprintf("DELETE FROM %s WHERE %s = $1 AND %s >= $2 AND %s < $3",
			quoteIdent(binding.Table),
			quoteIdent(binding.SIDColumn),
			quoteIdent(binding.TimeColumn),
			quoteIdent(binding.TimeColumn),
		)
		if _, err_ := q.db.Exec(ctx, sqlText, sid, startMS, endMS); err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
		if err_ := q.UpdateSRanges(ctx, sid, binding.Table, info.TimeFrame, startMS, endMS, false); err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
		return nil
	}
	tblLock := cptState.getTableLock(binding.Table)
	tblLock.Lock()
	defer tblLock.Unlock()
	ctx = withSeriesTableWriteLockHeld(ctx)

	if err_ := q.UpdateSRanges(ctx, sid, binding.Table, info.TimeFrame, startMS, endMS, false); err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	if err_ := waitForQuestSeriesCoverageDeleted(ctx, info, sid, startMS, endMS); err_ != nil {
		return err_
	}
	if err_ := maybeRewriteQuestSeriesTable(ctx, q, info, sid, startMS, endMS); err_ != nil {
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
	covered, err_ := q.getCoveredRanges(ctx, sid, binding.Table, info.TimeFrame, startMS, endMS)
	if err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	return subtractMSRanges(MSRange{Start: startMS, Stop: endMS}, covered), nil
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

func maybeRewriteQuestSeriesTable(ctx context.Context, q *Queries, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	binding := normalizedSeriesBinding(info.Binding)
	if !seriesTableWriteLockHeld(ctx) {
		tblLock := cptState.getTableLock(binding.Table)
		tblLock.Lock()
		defer tblLock.Unlock()
	}

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
	expectedRows, err := countQuestSeriesCoveredRows(ctx, q, binding, sid, covered)
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	return rewriteQuestSeriesTableLocked(ctx, q, info, sid, covered, expectedRows)
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

func countQuestSeriesCoveredRows(ctx context.Context, q *Queries, binding SeriesBinding, sid int32, covered []MSRange) (int64, error) {
	var count int64
	predicate := questSeriesRewritePredicate(binding, sid, covered)
	sqlText := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s", quoteIdent(binding.Table), predicate)
	err := q.db.QueryRow(ctx, sqlText).Scan(&count)
	return count, err
}

func rewriteQuestSeriesTableLocked(ctx context.Context, q *Queries, info *SeriesInfo, sid int32, covered []MSRange, expectedRows int64) *errs.Error {
	binding := normalizedSeriesBinding(info.Binding)
	tmpTable := fmt.Sprintf("%s_rewrite_%d_%06d", binding.Table, time.Now().UnixNano(), rand.Intn(1000000))
	backupTable := fmt.Sprintf("%s_backup_%d_%06d", binding.Table, time.Now().UnixNano(), rand.Intn(1000000))
	selectCols := seriesSelectColumns(binding)
	predicate := questSeriesRewritePredicate(binding, sid, covered)
	createSQL := fmt.Sprintf(`CREATE TABLE %s AS (
  SELECT %s
  FROM %s
  WHERE %s
) TIMESTAMP(%s) PARTITION BY %s WAL DEDUP UPSERT KEYS(%s, %s)`,
		quoteIdent(tmpTable),
		strings.Join(selectCols, ", "),
		quoteIdent(binding.Table),
		predicate,
		quoteIdent(binding.TimeColumn),
		seriesQuestPartitionBy(info.TimeFrame),
		quoteIdent(binding.SIDColumn),
		quoteIdent(binding.TimeColumn),
	)
	if _, err := q.db.Exec(ctx, createSQL); err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	if _, err := waitCompactVisibleCount(ctx, q.db, tmpTable, expectedRows); err != nil {
		_, _ = q.db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(tmpTable)))
		return NewDbErr(core.ErrDbReadFail, err)
	}

	if _, err := q.db.Exec(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", quoteIdent(binding.Table), quoteIdent(backupTable))); err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	if _, err := q.db.Exec(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", quoteIdent(tmpTable), quoteIdent(binding.Table))); err != nil {
		_, restoreErr := q.db.Exec(ctx, fmt.Sprintf("RENAME TABLE %s TO %s", quoteIdent(backupTable), quoteIdent(binding.Table)))
		if restoreErr != nil {
			return NewDbErr(core.ErrDbExecFail, fmt.Errorf("rename compacted series table: %w; restore backup failed: %v; recovery table=%s backup_table=%s", err, restoreErr, tmpTable, backupTable))
		}
		return NewDbErr(core.ErrDbExecFail, fmt.Errorf("rename compacted series table: %w; recovery table=%s backup_table=%s", err, tmpTable, backupTable))
	}
	_, _ = q.db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(backupTable)))
	return nil
}

func seriesSelectColumns(binding SeriesBinding) []string {
	cols := []string{
		quoteIdent(binding.SIDColumn),
		quoteIdent(binding.TimeColumn),
		quoteIdent(binding.EndColumn),
	}
	for _, field := range binding.Fields {
		cols = append(cols, quoteIdent(field.Name))
	}
	return cols
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
	if IsQuestDB && !skipSeriesTableReadLock(ctx) {
		tblLock := cptState.getTableLock(binding.Table)
		tblLock.RLock()
		defer tblLock.RUnlock()
	}
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
	if IsQuestDB && !skipSeriesTableReadLock(ctx) {
		binding := normalizedSeriesBinding(info.Binding)
		tblLock := cptState.getTableLock(binding.Table)
		tblLock.RLock()
		defer tblLock.RUnlock()
	}
	return UpdateSeriesCoverage(ctx, info, sid, startMS, endMS, rows)
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

func waitForQuestSeriesVisible(ctx context.Context, q *Queries, info *SeriesInfo, sid int32, wantTimeMS int64) *errs.Error {
	binding := normalizedSeriesBinding(info.Binding)
	sqlText := fmt.Sprintf("SELECT max(cast(%s as long)/1000) FROM %s WHERE %s = $1",
		quoteIdent(binding.TimeColumn),
		quoteIdent(binding.Table),
		quoteIdent(binding.SIDColumn),
	)
	ok, err := waitForQuestCondition(ctx, 5*time.Second, questReadAfterWritePollInterval, func() (bool, error) {
		var maxTime *int64
		row := q.db.QueryRow(ctx, sqlText, sid)
		if err := row.Scan(&maxTime); err != nil {
			return false, nil
		}
		return maxTime != nil && *maxTime >= wantTimeMS, nil
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
	binding := normalizedSeriesBinding(info.Binding)
	ok, err := waitForQuestCondition(ctx, 5*time.Second, questReadAfterWritePollInterval, func() (bool, error) {
		q, conn, connErr := Conn(ctx)
		if connErr != nil {
			return false, connErr
		}
		defer conn.Release()
		deleted, queryErr := questSeriesDeleteMarkerVisible(ctx, q, binding.Table, info.TimeFrame, sid, startMS, endMS)
		if queryErr != nil {
			return false, queryErr
		}
		if !deleted {
			return false, nil
		}
		covered, queryErr := questSeriesCoveredRangesFromDB(ctx, q, binding.Table, info.TimeFrame, sid, startMS, endMS)
		if queryErr != nil {
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
		colDefs = append(colDefs, fmt.Sprintf("%s %s NOT NULL", quoteIdent(field.Name), seriesSQLType(field.Type)))
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

func scanSeriesRecord(rows pgx.Rows, fields []SeriesField) (*DataRecord, error) {
	rec := &DataRecord{Values: make(map[string]any, len(fields))}
	targets := make([]any, 0, 3+len(fields))
	targets = append(targets, &rec.Sid, &rec.TimeMS, &rec.EndMS)
	fieldTargets := make([]any, len(fields))
	for i, field := range fields {
		switch field.Type {
		case "float":
			var val float64
			fieldTargets[i] = &val
		case "int":
			var val int64
			fieldTargets[i] = &val
		case "string", "json":
			var val string
			fieldTargets[i] = &val
		case "bool":
			var val bool
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
		switch field.Type {
		case "float":
			rec.Values[field.Name] = *(fieldTargets[i].(*float64))
		case "int":
			rec.Values[field.Name] = *(fieldTargets[i].(*int64))
		case "string", "json":
			rec.Values[field.Name] = *(fieldTargets[i].(*string))
		case "bool":
			rec.Values[field.Name] = *(fieldTargets[i].(*bool))
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
