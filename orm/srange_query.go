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

func (q *Queries) FindSRanges(args FindSRangesArgs) ([]*SRange, int64, *errs.Error) {
	if args.Sid == 0 {
		return nil, 0, errs.NewMsg(core.ErrDbReadFail, "sid is required")
	}
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
	whereParts = append(whereParts, "coalesce(is_deleted, false) = false")
	whereClause := strings.Join(whereParts, " AND ")

	countSQL := fmt.Sprintf(`SELECT count(*) FROM (
  SELECT sid FROM sranges_q LATEST BY sid, tbl, timeframe, start_ms WHERE %s
)`, whereClause)
	var total int64
	if err := q.db.QueryRow(ctx, countSQL).Scan(&total); err != nil {
		return nil, 0, NewDbErr(core.ErrDbReadFail, err)
	}

	limit := 100
	if args.Limit > 0 {
		limit = args.Limit
	}
	dataSQL := fmt.Sprintf(`SELECT sid, tbl, timeframe, start_ms, stop_ms, has_data
FROM sranges_q
LATEST BY sid, tbl, timeframe, start_ms
WHERE %s
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
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := q.db.Query(ctx, `SELECT sid, tbl, timeframe, start_ms, stop_ms, has_data
FROM sranges_q
LATEST BY sid, tbl, timeframe, start_ms
WHERE sid = $1 AND coalesce(is_deleted, false) = false
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
