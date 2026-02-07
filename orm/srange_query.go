package orm

import (
	"context"
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

func (q *PubQueries) FindSRanges(args FindSRangesArgs) ([]*SRange, int64, *errs.Error) {
	if args.Sid == 0 {
		return nil, 0, errs.NewMsg(core.ErrDbReadFail, "sid is required")
	}
	ctx := context.Background()
	db, err2 := BanPubConn(false)
	if err2 != nil {
		return nil, 0, err2
	}
	defer db.Close()
	sqlParams := make([]any, 0, 8)
	var whereClause strings.Builder
	add := func(expr string, val any) {
		if whereClause.Len() == 0 {
			whereClause.WriteString("where ")
		} else {
			whereClause.WriteString("and ")
		}
		whereClause.WriteString(expr)
		whereClause.WriteByte(' ')
		sqlParams = append(sqlParams, val)
	}

	add("sid=?", args.Sid)
	if args.Table != "" {
		add("tbl=?", args.Table)
	}
	if args.TimeFrame != "" {
		add("timeframe=?", args.TimeFrame)
	}
	if args.Start > 0 {
		add("start_ms >= ?", args.Start)
	}
	if args.Stop > 0 {
		add("stop_ms <= ?", args.Stop)
	}
	if args.HasData != nil {
		val := 0
		if *args.HasData {
			val = 1
		}
		add("has_data = ?", val)
	}

	countSQL := "select count(*) from sranges " + whereClause.String()
	var total int64
	if err := db.QueryRowContext(ctx, countSQL, sqlParams...).Scan(&total); err != nil {
		return nil, 0, NewDbErr(core.ErrDbReadFail, err)
	}

	limit := 100
	if args.Limit > 0 {
		limit = args.Limit
	}
	var b strings.Builder
	b.WriteString("select id,sid,tbl,timeframe,start_ms,stop_ms,has_data from sranges ")
	b.WriteString(whereClause.String())
	b.WriteString("order by start_ms desc ")
	if args.Offset > 0 {
		b.WriteString("offset ? ")
		sqlParams = append(sqlParams, args.Offset)
	}
	b.WriteString("limit ?")
	sqlParams = append(sqlParams, limit)

	rows, err := db.QueryContext(ctx, b.String(), sqlParams...)
	if err != nil {
		return nil, 0, NewDbErr(core.ErrDbReadFail, err)
	}
	defer rows.Close()

	var out []*SRange
	for rows.Next() {
		var r SRange
		var hasData int64
		if err := rows.Scan(&r.ID, &r.Sid, &r.Table, &r.Timeframe, &r.StartMs, &r.StopMs, &hasData); err != nil {
			return nil, 0, NewDbErr(core.ErrDbReadFail, err)
		}
		r.HasData = hasData != 0
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, NewDbErr(core.ErrDbReadFail, err)
	}
	return out, total, nil
}

func (q *PubQueries) ListSRangesBySid(ctx context.Context, sid int32) ([]*SRange, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	db, err2 := BanPubConn(false)
	if err2 != nil {
		return nil, err2
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `
select id,sid,tbl,timeframe,start_ms,stop_ms,has_data
from sranges
where sid=?
order by tbl,timeframe,start_ms`, sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*SRange
	for rows.Next() {
		var r SRange
		var hasData int64
		if err := rows.Scan(&r.ID, &r.Sid, &r.Table, &r.Timeframe, &r.StartMs, &r.StopMs, &hasData); err != nil {
			return nil, err
		}
		r.HasData = hasData != 0
		out = append(out, &r)
	}
	return out, rows.Err()
}
