package orm

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

type MSRange struct {
	Start int64
	Stop  int64
}

type srangeSpan struct {
	ID      int64
	StartMs int64
	StopMs  int64
	HasData bool
}

func mergeMSRanges(ranges []MSRange) []MSRange {
	if len(ranges) == 0 {
		return nil
	}
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].Start != ranges[j].Start {
			return ranges[i].Start < ranges[j].Start
		}
		return ranges[i].Stop < ranges[j].Stop
	})
	out := make([]MSRange, 0, len(ranges))
	cur := ranges[0]
	for _, r := range ranges[1:] {
		if r.Stop <= r.Start {
			continue
		}
		if r.Start <= cur.Stop {
			if r.Stop > cur.Stop {
				cur.Stop = r.Stop
			}
			continue
		}
		out = append(out, cur)
		cur = r
	}
	out = append(out, cur)
	return out
}

func subtractMSRanges(target MSRange, covered []MSRange) []MSRange {
	if target.Stop <= target.Start {
		return nil
	}
	if len(covered) == 0 {
		return []MSRange{target}
	}
	covered = mergeMSRanges(covered)
	out := make([]MSRange, 0, 4)
	cur := target.Start
	for _, c := range covered {
		if c.Stop <= cur {
			continue
		}
		if c.Start >= target.Stop {
			break
		}
		if c.Start > cur {
			out = append(out, MSRange{Start: cur, Stop: min(c.Start, target.Stop)})
		}
		cur = max(cur, c.Stop)
		if cur >= target.Stop {
			break
		}
	}
	if cur < target.Stop {
		out = append(out, MSRange{Start: cur, Stop: target.Stop})
	}
	return out
}

func (q *PubQueries) ListSRanges(ctx context.Context, sid int32, table, timeframe string, startMs, stopMs int64) ([]*SRange, error) {
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
where sid=? and tbl=? and timeframe=? and stop_ms >= ? and start_ms <= ?
order by start_ms`,
		sid, table, timeframe, startMs, stopMs,
	)
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

func (q *PubQueries) getCoveredRanges(ctx context.Context, sid int32, table, timeframe string, startMs, stopMs int64) ([]MSRange, error) {
	rows, err := q.ListSRanges(ctx, sid, table, timeframe, startMs, stopMs)
	if err != nil {
		return nil, err
	}
	covered := make([]MSRange, 0, len(rows))
	for _, r := range rows {
		if !r.HasData {
			continue
		}
		if r.StopMs > r.StartMs {
			covered = append(covered, MSRange{Start: r.StartMs, Stop: r.StopMs})
		}
	}
	return mergeMSRanges(covered), nil
}

func (q *PubQueries) UpdateSRanges(ctx context.Context, sid int32, table, timeframe string, startMs, stopMs int64, hasData bool) error {
	if startMs >= stopMs {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	db, err2 := BanPubConn(true)
	if err2 != nil {
		return err2
	}
	defer db.Close()

	// 使用 IMMEDIATE 模式，在事务开始时就获取写锁，避免后续锁升级冲突
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		return err
	}
	commit := false
	defer func() {
		if !commit {
			_ = tx.Rollback()
		}
	}()

	rows, err := tx.QueryContext(ctx, `
select id,start_ms,stop_ms,has_data
from sranges
where sid=? and tbl=? and timeframe=? and stop_ms >= ? and start_ms <= ?
order by start_ms, stop_ms`,
		sid, table, timeframe, startMs, stopMs,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	spans := make([]srangeSpan, 0, 16)
	points := make([]int64, 0, 64)
	points = append(points, startMs, stopMs)
	for rows.Next() {
		var s srangeSpan
		var hd int64
		if err := rows.Scan(&s.ID, &s.StartMs, &s.StopMs, &hd); err != nil {
			return err
		}
		s.HasData = hd != 0
		if s.StopMs <= s.StartMs {
			continue
		}
		spans = append(spans, s)
		points = append(points, s.StartMs, s.StopMs)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Sort+dedupe boundary points.
	sort.Slice(points, func(i, j int) bool { return points[i] < points[j] })
	uniq := points[:0]
	var lastVal int64
	var hasLast bool
	for _, p := range points {
		if !hasLast || p != lastVal {
			uniq = append(uniq, p)
			lastVal = p
			hasLast = true
		}
	}
	points = uniq

	type seg struct {
		StartMs int64
		StopMs  int64
		HasData bool
	}
	segs := make([]seg, 0, len(points))
	inNew := func(a, b int64) bool {
		return a < stopMs && b > startMs
	}
	coveredBy := func(a, b int64, want bool) bool {
		for _, s := range spans {
			if s.HasData != want {
				continue
			}
			if s.StartMs < b && s.StopMs > a {
				return true
			}
		}
		return false
	}

	for i := 0; i+1 < len(points); i++ {
		a, b := points[i], points[i+1]
		if b <= a {
			continue
		}
		var state *bool
		if inNew(a, b) {
			state = &hasData
		} else if coveredBy(a, b, true) {
			t := true
			state = &t
		} else if coveredBy(a, b, false) {
			f := false
			state = &f
		}
		if state == nil {
			continue
		}
		if len(segs) > 0 && segs[len(segs)-1].HasData == *state && segs[len(segs)-1].StopMs == a {
			segs[len(segs)-1].StopMs = b
			continue
		}
		segs = append(segs, seg{StartMs: a, StopMs: b, HasData: *state})
	}

	// Rewrite: delete overlapping ids then insert merged segments.
	if len(spans) > 0 {
		ids := make([]string, 0, len(spans))
		for _, s := range spans {
			ids = append(ids, fmt.Sprintf("%d", s.ID))
		}
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`delete from sranges where id in (%s)`, strings.Join(ids, ",")))
		if err != nil {
			return err
		}
	}
	if len(segs) == 0 {
		if err := tx.Commit(); err != nil {
			return err
		}
		commit = true
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, `insert into sranges (sid,tbl,timeframe,start_ms,stop_ms,has_data) values (?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, s := range segs {
		hd := 0
		if s.HasData {
			hd = 1
		}
		if _, err := stmt.ExecContext(ctx, sid, table, timeframe, s.StartMs, s.StopMs, hd); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	commit = true
	return nil
}
