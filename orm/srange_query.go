package orm

import (
	"context"
	"fmt"
	"strings"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
)

type FindSRangesArgs struct {
	Sid       int32
	Table     string
	TimeFrame string
	Start     int64
	Stop      int64
	HasData   *bool
	Limit     int
	Offset    int
}

type ListSeriesRangeSummariesArgs struct {
	Source    string
	Table     string
	TimeFrame string
	Sid       int32
	HasData   *bool
	Limit     int
	Offset    int
}

func (q *Queries) ListSeriesRangeSummaries(args ListSeriesRangeSummariesArgs) ([]*SeriesRangeSummary, int64, *errs.Error) {
	unlock := LockCompactTableRead("sranges_q")
	defer unlock()
	ctx := context.Background()
	where, params := buildSeriesRangeSummaryWhere(args)
	fromSQL := fmt.Sprintf(`FROM sranges
  WHERE %s`, where)
	if IsQuestDB {
		fromSQL = fmt.Sprintf(`FROM (
    SELECT sid, tbl, timeframe, start_ms, stop_ms, has_data, is_deleted
    FROM sranges_q LATEST BY sid, tbl, timeframe, start_ms
    WHERE %s
  )
  WHERE coalesce(is_deleted, false) = false`, where)
	}
	countSQL := fmt.Sprintf(`SELECT count(*) FROM (
  SELECT sid, tbl, timeframe, has_data, min(start_ms) AS start_ms, max(stop_ms) AS stop_ms, count(*) AS segments
  %s
  GROUP BY sid, tbl, timeframe, has_data
)`, fromSQL)
	var total int64
	if err := q.db.QueryRow(ctx, countSQL, params...).Scan(&total); err != nil {
		return nil, 0, NewDbErr(core.ErrDbReadFail, err)
	}
	limit := args.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	params = append(params, args.Offset, limit)
	dataSQL := fmt.Sprintf(`SELECT sid, tbl, CASE WHEN tbl LIKE 'kline_%%' THEN 'kline' ELSE tbl END AS source, timeframe, start_ms, stop_ms, has_data, segments
FROM (
  SELECT sid, tbl, timeframe, has_data, min(start_ms) AS start_ms, max(stop_ms) AS stop_ms, count(*) AS segments
  %s
  GROUP BY sid, tbl, timeframe, has_data
)
ORDER BY stop_ms DESC
OFFSET $%d LIMIT $%d`, fromSQL, len(params)-1, len(params))
	rows, err := q.db.Query(ctx, dataSQL, params...)
	if err != nil {
		return nil, 0, NewDbErr(core.ErrDbReadFail, err)
	}
	defer rows.Close()
	var out []*SeriesRangeSummary
	for rows.Next() {
		var r SeriesRangeSummary
		if err := rows.Scan(&r.Sid, &r.Table, &r.Source, &r.Timeframe, &r.StartMs, &r.StopMs, &r.HasData, &r.Segments); err != nil {
			return nil, 0, NewDbErr(core.ErrDbReadFail, err)
		}
		r.Symbol = GetSymbolByID(r.Sid)
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, NewDbErr(core.ErrDbReadFail, err)
	}
	return out, total, nil
}

func buildSeriesRangeSummaryWhere(args ListSeriesRangeSummariesArgs) (string, []any) {
	var parts []string
	var params []any
	add := func(sql string, val any) {
		params = append(params, val)
		parts = append(parts, fmt.Sprintf(sql, len(params)))
	}
	if args.Sid > 0 {
		add("sid = $%d", args.Sid)
	}
	if args.Table != "" {
		add("tbl = $%d", args.Table)
	}
	if args.TimeFrame != "" {
		add("timeframe = $%d", args.TimeFrame)
	}
	if args.Source != "" {
		if args.Source == "kline" {
			parts = append(parts, "tbl LIKE 'kline_%'")
		} else {
			add("tbl = $%d", args.Source)
		}
	}
	if args.HasData != nil {
		add("has_data = $%d", *args.HasData)
	}
	if len(parts) == 0 {
		return "1 = 1", nil
	}
	return strings.Join(parts, " AND "), params
}

func (q *Queries) FindSRanges(args FindSRangesArgs) ([]*SRange, int64, *errs.Error) {
	if args.Sid == 0 {
		return nil, 0, errs.NewMsg(core.ErrDbReadFail, "sid is required")
	}
	unlock := LockCompactTableRead("sranges_q")
	defer unlock()
	ctx := context.Background()

	var whereParts []string
	whereParts = append(whereParts, fmt.Sprintf("sid = %d", args.Sid))
	if args.Table != "" {
		whereParts = append(whereParts, fmt.Sprintf("tbl = '%s'", args.Table))
	}
	if args.TimeFrame != "" {
		whereParts = append(whereParts, fmt.Sprintf("timeframe = '%s'", args.TimeFrame))
	}
	if args.Start > 0 {
		whereParts = append(whereParts, fmt.Sprintf("start_ms >= %d", args.Start))
	}
	if args.Stop > 0 {
		whereParts = append(whereParts, fmt.Sprintf("stop_ms <= %d", args.Stop))
	}
	if args.HasData != nil {
		whereParts = append(whereParts, fmt.Sprintf("has_data = %v", *args.HasData))
	}
	whereClause := strings.Join(whereParts, " AND ")

	countSQL := fmt.Sprintf(`SELECT count(*) FROM (
  SELECT sid, is_deleted FROM sranges_q LATEST BY sid, tbl, timeframe, start_ms WHERE %s
) WHERE coalesce(is_deleted, false) = false
`, whereClause)
	var total int64
	if err := q.db.QueryRow(ctx, countSQL).Scan(&total); err != nil {
		return nil, 0, NewDbErr(core.ErrDbReadFail, err)
	}

	limit := 100
	if args.Limit > 0 {
		limit = args.Limit
	}
	dataSQL := fmt.Sprintf(`SELECT sid, tbl, timeframe, start_ms, stop_ms, has_data
FROM (
  SELECT sid, tbl, timeframe, start_ms, stop_ms, has_data, is_deleted
  FROM sranges_q
  LATEST BY sid, tbl, timeframe, start_ms
  WHERE %s
)
WHERE coalesce(is_deleted, false) = false
ORDER BY start_ms DESC`, whereClause)
	if args.Offset > 0 {
		dataSQL += fmt.Sprintf(" OFFSET %d", args.Offset)
	}
	dataSQL += fmt.Sprintf(" LIMIT %d", limit)

	rows, err := q.db.Query(ctx, dataSQL)
	if err != nil {
		return nil, 0, NewDbErr(core.ErrDbReadFail, err)
	}
	defer rows.Close()

	var out []*SRange
	for rows.Next() {
		var r SRange
		if err := rows.Scan(&r.Sid, &r.Table, &r.Timeframe, &r.StartMs, &r.StopMs, &r.HasData); err != nil {
			return nil, 0, NewDbErr(core.ErrDbReadFail, err)
		}
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, NewDbErr(core.ErrDbReadFail, err)
	}
	return out, total, nil
}

func (q *Queries) ListSRangesBySid(ctx context.Context, sid int32) ([]*SRange, error) {
	unlock := LockCompactTableRead("sranges_q")
	defer unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := q.db.Query(ctx, `SELECT sid, tbl, timeframe, start_ms, stop_ms, has_data
FROM (
  SELECT sid, tbl, timeframe, start_ms, stop_ms, has_data, is_deleted
  FROM sranges_q
  LATEST BY sid, tbl, timeframe, start_ms
  WHERE sid = $1
)
WHERE coalesce(is_deleted, false) = false
ORDER BY tbl, timeframe, start_ms`, sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*SRange
	for rows.Next() {
		var r SRange
		if err := rows.Scan(&r.Sid, &r.Table, &r.Timeframe, &r.StartMs, &r.StopMs, &r.HasData); err != nil {
			return nil, err
		}
		out = append(out, &r)
	}
	return out, rows.Err()
}
