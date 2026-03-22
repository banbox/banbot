package orm

import (
	"context"
	"sort"
)

// pg_srange.go implements the sranges coverage-range management for TimescaleDB/PostgreSQL.
// Unlike QuestDB (append-only + soft delete), PostgreSQL supports real transactions,
// so all operations use DELETE + INSERT inside a transaction for atomic updates.
// No in-process cache is needed because PostgreSQL writes are immediately visible.

// listSRangesPg queries the PostgreSQL sranges table for segments overlapping [startMs, stopMs).
func (q *Queries) listSRangesPg(ctx context.Context, sid int32, table, timeframe string, startMs, stopMs int64) ([]*SRange, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := q.db.Query(ctx, `SELECT sid, tbl, timeframe, start_ms, stop_ms, has_data
FROM sranges
WHERE sid = $1 AND tbl = $2 AND timeframe = $3 AND stop_ms >= $4 AND start_ms <= $5
ORDER BY start_ms`,
		sid, table, timeframe, startMs, stopMs,
	)
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

// getCoveredRangesPg returns merged has_data=true MSRanges for TimescaleDB.
func (q *Queries) getCoveredRangesPg(ctx context.Context, sid int32, table, timeframe string, startMs, stopMs int64) ([]MSRange, error) {
	rows, err := q.listSRangesPg(ctx, sid, table, timeframe, startMs, stopMs)
	if err != nil {
		return nil, err
	}
	covered := make([]MSRange, 0, len(rows))
	for _, r := range rows {
		if r.HasData && r.StopMs > r.StartMs {
			covered = append(covered, MSRange{Start: r.StartMs, Stop: r.StopMs})
		}
	}
	return mergeMSRanges(covered), nil
}

// mergeSpansIntoPg is a helper that reads existing spans in [startMs, stopMs), computes
// the desired new span set, then runs DELETE + INSERT in a transaction.
// `wantSegs` describes the complete desired state of [startMs, stopMs).
// Spans that partially overlap the window boundaries are preserved outside the window.
func (q *Queries) mergeSpansIntoPg(ctx context.Context, sid int32, table, timeframe string, startMs, stopMs int64, wantSegs []srangeSpan) error {
	if ctx == nil {
		ctx = context.Background()
	}
	tx, txq, err2 := q.NewTx(ctx)
	if err2 != nil {
		return err2
	}
	defer func() { _ = tx.Close(ctx, false) }()

	// Read existing rows that overlap the window so we can recover any overhanging parts.
	rows, err := txq.db.Query(ctx, `SELECT start_ms, stop_ms, has_data FROM sranges
WHERE sid = $1 AND tbl = $2 AND timeframe = $3 AND stop_ms >= $4 AND start_ms <= $5`,
		sid, table, timeframe, startMs, stopMs)
	if err != nil {
		return err
	}
	var overhangSegs []srangeSpan
	for rows.Next() {
		var s srangeSpan
		if scanErr := rows.Scan(&s.StartMs, &s.StopMs, &s.HasData); scanErr != nil {
			rows.Close()
			return scanErr
		}
		if s.StartMs < startMs {
			overhangSegs = append(overhangSegs, srangeSpan{StartMs: s.StartMs, StopMs: startMs, HasData: s.HasData})
		}
		if s.StopMs > stopMs {
			overhangSegs = append(overhangSegs, srangeSpan{StartMs: stopMs, StopMs: s.StopMs, HasData: s.HasData})
		}
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}

	// Delete all existing rows overlapping the window.
	_, err = txq.db.Exec(ctx, `DELETE FROM sranges
WHERE sid = $1 AND tbl = $2 AND timeframe = $3 AND stop_ms >= $4 AND start_ms <= $5`,
		sid, table, timeframe, startMs, stopMs)
	if err != nil {
		return err
	}

	// Insert window segments plus any recovered overhang segments.
	allSegs := make([]srangeSpan, 0, len(wantSegs)+len(overhangSegs))
	allSegs = append(allSegs, wantSegs...)
	allSegs = append(allSegs, overhangSegs...)
	for _, s := range allSegs {
		_, err = txq.db.Exec(ctx, `INSERT INTO sranges (sid, tbl, timeframe, start_ms, stop_ms, has_data)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (sid, tbl, timeframe, start_ms) DO UPDATE
  SET stop_ms = EXCLUDED.stop_ms, has_data = EXCLUDED.has_data`,
			sid, table, timeframe, s.StartMs, s.StopMs, s.HasData)
		if err != nil {
			return err
		}
	}
	if closeErr := tx.Close(ctx, true); closeErr != nil {
		return closeErr
	}
	return nil
}

// UpdateSRangesPg is the TimescaleDB equivalent of UpdateSRanges.
// It merges [startMs, stopMs) with has_data=hasData into the existing sranges,
// preserving adjacent segments outside the window.
func (q *Queries) UpdateSRangesPg(ctx context.Context, sid int32, table, timeframe string, startMs, stopMs int64, hasData bool) error {
	if startMs >= stopMs {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	rows, err := q.listSRangesPg(ctx, sid, table, timeframe, startMs, stopMs)
	if err != nil {
		return err
	}

	spans := make([]srangeSpan, 0, len(rows))
	points := make([]int64, 0, len(rows)*2+2)
	points = append(points, startMs, stopMs)
	for _, r := range rows {
		if r.StopMs <= r.StartMs {
			continue
		}
		spans = append(spans, srangeSpan{StartMs: r.StartMs, StopMs: r.StopMs, HasData: r.HasData})
		points = append(points, r.StartMs, r.StopMs)
	}

	sort.Slice(points, func(i, j int) bool { return points[i] < points[j] })
	uniq := deduplicateInt64(points)

	type seg struct {
		StartMs int64
		StopMs  int64
		HasData bool
	}
	segs := make([]seg, 0, len(uniq))
	// Build per-hasData sorted arrays for O(log N) coverage lookup instead of O(N) per interval.
	// spans are sorted by StartMs (ORDER BY start_ms from listSRangesPg).
	var trueStarts, trueStops, falseStarts, falseStops []int64
	for _, s := range spans {
		if s.HasData {
			trueStarts = append(trueStarts, s.StartMs)
			trueStops = append(trueStops, s.StopMs)
		} else {
			falseStarts = append(falseStarts, s.StartMs)
			falseStops = append(falseStops, s.StopMs)
		}
	}
	for i := 0; i+1 < len(uniq); i++ {
		a, b := uniq[i], uniq[i+1]
		if b <= a {
			continue
		}
		var state *bool
		if a < stopMs && b > startMs {
			state = &hasData
		} else if coveredAt(a, trueStarts, trueStops) {
			t := true
			state = &t
		} else if coveredAt(a, falseStarts, falseStops) {
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

	wantSegs := make([]srangeSpan, len(segs))
	for i, s := range segs {
		wantSegs[i] = srangeSpan{StartMs: s.StartMs, StopMs: s.StopMs, HasData: s.HasData}
	}
	if err := q.mergeSpansIntoPg(ctx, sid, table, timeframe, startMs, stopMs, wantSegs); err != nil {
		return err
	}
	return nil
}

// UpdateSRangesWithHolesPg is the TimescaleDB equivalent of UpdateSRangesWithHoles.
// It atomically marks [startMs, stopMs) as has_data=true while carving out holes as has_data=false.
func (q *Queries) UpdateSRangesWithHolesPg(ctx context.Context, sid int32, table, timeframe string, startMs, stopMs int64, holes []MSRange) error {
	if startMs >= stopMs {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	type seg struct {
		StartMs int64
		StopMs  int64
		HasData bool
	}
	wantRaw := make([]seg, 0, len(holes)*2+1)
	cur := startMs
	for _, h := range holes {
		if h.Stop <= h.Start || h.Stop <= startMs || h.Start >= stopMs {
			continue
		}
		hs := max(h.Start, startMs)
		he := min(h.Stop, stopMs)
		if hs > cur {
			wantRaw = append(wantRaw, seg{cur, hs, true})
		}
		wantRaw = append(wantRaw, seg{hs, he, false})
		cur = he
	}
	if cur < stopMs {
		wantRaw = append(wantRaw, seg{cur, stopMs, true})
	}
	// Merge adjacent same-state segs.
	merged := wantRaw[:0]
	for _, s := range wantRaw {
		if len(merged) > 0 && merged[len(merged)-1].HasData == s.HasData && merged[len(merged)-1].StopMs == s.StartMs {
			merged[len(merged)-1].StopMs = s.StopMs
		} else {
			merged = append(merged, s)
		}
	}

	wantSegs := make([]srangeSpan, len(merged))
	for i, s := range merged {
		wantSegs[i] = srangeSpan{StartMs: s.StartMs, StopMs: s.StopMs, HasData: s.HasData}
	}
	if err := q.mergeSpansIntoPg(ctx, sid, table, timeframe, startMs, stopMs, wantSegs); err != nil {
		return err
	}
	return nil
}

// DelKInfoPg deletes all sranges entries for (sid, tbl, timeframe) in TimescaleDB.
func delKInfoPg(ctx context.Context, sid int32, tbl, timeFrame string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := pool.Exec(ctx, `DELETE FROM sranges WHERE sid = $1 AND tbl = $2 AND timeframe = $3`,
		sid, tbl, timeFrame)
	return err
}

// GetKlineRangePg returns (minStartMs, maxStopMs) for has_data=true segments in TimescaleDB.
func getKlineRangePg(ctx context.Context, sid int32, tbl, timeFrame string) (int64, int64) {
	if ctx == nil {
		ctx = context.Background()
	}
	row := pool.QueryRow(ctx, `SELECT min(start_ms), max(stop_ms)
FROM sranges
WHERE sid = $1 AND tbl = $2 AND timeframe = $3 AND has_data = true`,
		sid, tbl, timeFrame)
	var start, stop *int64
	_ = row.Scan(&start, &stop)
	if start == nil || stop == nil {
		return 0, 0
	}
	return *start, *stop
}

// GetKlineRangesPg returns (minStart, maxStop) per sid for a list of sids in TimescaleDB.
func getKlineRangesPg(ctx context.Context, sidList []int32, tbl, timeFrame string) map[int32][2]int64 {
	res := make(map[int32][2]int64)
	if len(sidList) == 0 || ctx == nil {
		return res
	}
	sidIn := buildIntList(sidList)
	sql := `SELECT sid, min(start_ms), max(stop_ms)
FROM sranges
WHERE tbl = $1 AND timeframe = $2 AND has_data = true AND sid IN (` + sidIn + `)
GROUP BY sid`
	rows, err := pool.Query(ctx, sql, tbl, timeFrame)
	if err != nil {
		return res
	}
	defer rows.Close()
	for rows.Next() {
		var sid int32
		var start, stop int64
		if err := rows.Scan(&sid, &start, &stop); err != nil {
			continue
		}
		res[sid] = [2]int64{start, stop}
	}
	return res
}

// deduplicateInt64 removes duplicates from a sorted slice.
func deduplicateInt64(sorted []int64) []int64 {
	if len(sorted) == 0 {
		return sorted
	}
	out := sorted[:1]
	for _, v := range sorted[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

// buildIntList converts a slice of int32 to a comma-separated string for IN clauses.
func buildIntList(ids []int32) string {
	if len(ids) == 0 {
		return "NULL"
	}
	b := make([]byte, 0, len(ids)*4)
	for i, id := range ids {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendInt(b, int64(id))
	}
	return string(b)
}

func appendInt(b []byte, v int64) []byte {
	return append(b, []byte(itoa(v))...)
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	buf := make([]byte, 20)
	pos := len(buf)
	for v > 0 {
		pos--
		buf[pos] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
