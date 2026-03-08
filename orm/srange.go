package orm

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// srangesCacheKey uniquely identifies a (sid, tbl, timeframe) combination.
type srangesCacheKey struct {
	sid       int32
	tbl       string
	timeframe string
}

// srangesCacheEntry holds the in-process cached segment list for one key.
type srangesCacheEntry struct {
	spans []srangeSpan // always sorted by StartMs, non-overlapping, non-zero
}

var (
	srangesCache     = make(map[srangesCacheKey]*srangesCacheEntry)
	srangesCacheLock sync.RWMutex
)

// srangesCacheGet returns the cached covered (has_data=true) MSRanges for a key.
// Returns nil if no cache entry exists.
func srangesCacheGet(sid int32, tbl, tf string) []MSRange {
	srangesCacheLock.RLock()
	entry, ok := srangesCache[srangesCacheKey{sid: sid, tbl: tbl, timeframe: tf}]
	srangesCacheLock.RUnlock()
	if !ok {
		return nil
	}
	out := make([]MSRange, 0, len(entry.spans))
	for _, s := range entry.spans {
		if s.HasData {
			out = append(out, MSRange{Start: s.StartMs, Stop: s.StopMs})
		}
	}
	return out
}

// srangesCacheUpdate replaces the cached segment list for a key.
func srangesCacheUpdate(sid int32, tbl, tf string, segs []srangeSpan) {
	key := srangesCacheKey{sid: sid, tbl: tbl, timeframe: tf}
	srangesCacheLock.Lock()
	srangesCache[key] = &srangesCacheEntry{spans: segs}
	srangesCacheLock.Unlock()
}

// srangesCacheDel removes the cache entry for a key (used when sranges are deleted).
func srangesCacheDel(sid int32, tbl, tf string) {
	key := srangesCacheKey{sid: sid, tbl: tbl, timeframe: tf}
	srangesCacheLock.Lock()
	delete(srangesCache, key)
	srangesCacheLock.Unlock()
}

type MSRange struct {
	Start int64
	Stop  int64
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

func (q *Queries) ListSRanges(ctx context.Context, sid int32, table, timeframe string, startMs, stopMs int64) ([]*SRange, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := q.db.Query(ctx, `SELECT sid, tbl, timeframe, start_ms, stop_ms, has_data
FROM sranges_q
LATEST BY sid, tbl, timeframe, start_ms
WHERE sid = $1 AND tbl = $2 AND timeframe = $3 AND stop_ms >= $4 AND start_ms <= $5
  AND coalesce(is_deleted, false) = false
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

func (q *Queries) getCoveredRanges(ctx context.Context, sid int32, table, timeframe string, startMs, stopMs int64) ([]MSRange, error) {
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
	merged := mergeMSRanges(covered)

	// WAL delay guard: if DB returned nothing for this (sid, tbl, tf) window but
	// the in-process cache has data (written in the same process run), use the
	// cache to avoid spurious re-downloads caused by QuestDB WAL commit lag.
	if len(merged) == 0 {
		cached := srangesCacheGet(sid, table, timeframe)
		if len(cached) > 0 {
			// Filter to the requested window
			var inWindow []MSRange
			for _, r := range cached {
				if r.Stop <= startMs || r.Start >= stopMs {
					continue
				}
				inWindow = append(inWindow, MSRange{
					Start: max(r.Start, startMs),
					Stop:  min(r.Stop, stopMs),
				})
			}
			if len(inWindow) > 0 {
				return mergeMSRanges(inWindow), nil
			}
		}
	}
	return merged, nil
}

type srangeSpan struct {
	StartMs int64
	StopMs  int64
	HasData bool
}

// UpdateSRangesWithHoles atomically marks [startMs, stopMs) as has_data=true while
// simultaneously carving out the given holes as has_data=false. This is done in a
// single DB read + write cycle so that the QuestDB WAL commit lag that exists between
// two consecutive UpdateSRanges calls cannot cause the has_data=true regions to be lost.
//
// holes must be non-overlapping sub-ranges of [startMs, stopMs) and are expected to
// be already filtered (non-trading time removed, tiny-hole filtered, etc.).
func (q *Queries) UpdateSRangesWithHoles(ctx context.Context, sid int32, table, timeframe string, startMs, stopMs int64, holes []MSRange) error {
	if startMs >= stopMs {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Build the complete desired seg list in memory first:
	// everything inside [startMs, stopMs) is has_data=true unless it falls in a hole.
	type seg struct {
		StartMs int64
		StopMs  int64
		HasData bool
	}
	wantSegs := make([]seg, 0, len(holes)*2+1)
	cur := startMs
	for _, h := range holes {
		if h.Start > h.Stop || h.Stop <= startMs || h.Start >= stopMs {
			continue
		}
		hs := max(h.Start, startMs)
		he := min(h.Stop, stopMs)
		if hs > cur {
			wantSegs = append(wantSegs, seg{cur, hs, true})
		}
		wantSegs = append(wantSegs, seg{hs, he, false})
		cur = he
	}
	if cur < stopMs {
		wantSegs = append(wantSegs, seg{cur, stopMs, true})
	}
	// Merge adjacent segs with same HasData (shouldn't happen in practice, but be safe).
	merged := wantSegs[:0]
	for _, s := range wantSegs {
		if len(merged) > 0 && merged[len(merged)-1].HasData == s.HasData && merged[len(merged)-1].StopMs == s.StartMs {
			merged[len(merged)-1].StopMs = s.StopMs
		} else {
			merged = append(merged, s)
		}
	}
	wantSegs = merged

	// Read existing spans that overlap [startMs, stopMs) from DB.
	rows, err := q.db.Query(ctx, `SELECT start_ms, stop_ms, has_data
FROM sranges_q
LATEST BY sid, tbl, timeframe, start_ms
WHERE sid = $1 AND tbl = $2 AND timeframe = $3 AND stop_ms >= $4 AND start_ms <= $5
  AND coalesce(is_deleted, false) = false
ORDER BY start_ms`,
		sid, table, timeframe, startMs, stopMs,
	)
	if err != nil {
		return err
	}
	spans := make([]srangeSpan, 0, 16)
	for rows.Next() {
		var s srangeSpan
		if err := rows.Scan(&s.StartMs, &s.StopMs, &s.HasData); err != nil {
			rows.Close()
			return err
		}
		if s.StopMs > s.StartMs {
			spans = append(spans, s)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	now := time.Now().UTC()
	microOff := 0

	// Logically delete all existing spans that overlap the window.
	if len(spans) > 0 {
		for _, s := range spans {
			ts := now.Add(time.Duration(microOff) * time.Microsecond)
			microOff++
			_, err = q.db.Exec(ctx, `INSERT INTO sranges_q (sid, ts, tbl, timeframe, start_ms, stop_ms, has_data, is_deleted, deleted_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, true, $2)`,
				sid, ts, table, timeframe, s.StartMs, s.StopMs, s.HasData)
			if err != nil {
				return fmt.Errorf("logical delete srange: %w", err)
			}
		}
		MaybeCompact("sranges_q")
	}

	if len(wantSegs) == 0 {
		srangesCacheDel(sid, table, timeframe)
		return nil
	}

	// Insert the new complete set of segs in one batch.
	for _, s := range wantSegs {
		ts := now.Add(time.Duration(microOff) * time.Microsecond)
		microOff++
		_, err = q.db.Exec(ctx, `INSERT INTO sranges_q (sid, ts, tbl, timeframe, start_ms, stop_ms, has_data, is_deleted)
VALUES ($1, $2, $3, $4, $5, $6, $7, false)`,
			sid, ts, table, timeframe, s.StartMs, s.StopMs, s.HasData)
		if err != nil {
			return fmt.Errorf("insert srange seg: %w", err)
		}
	}

	// Update in-process cache.
	srangesCacheLock.RLock()
	key := srangesCacheKey{sid: sid, tbl: table, timeframe: timeframe}
	entry, ok := srangesCache[key]
	var existingSpans []srangeSpan
	if ok {
		existingSpans = make([]srangeSpan, len(entry.spans))
		copy(existingSpans, entry.spans)
	}
	srangesCacheLock.RUnlock()

	newSpans := make([]srangeSpan, 0, len(existingSpans)+len(wantSegs))
	for _, s := range existingSpans {
		if s.StopMs <= startMs || s.StartMs >= stopMs {
			newSpans = append(newSpans, s)
		}
	}
	for _, s := range wantSegs {
		newSpans = append(newSpans, srangeSpan{StartMs: s.StartMs, StopMs: s.StopMs, HasData: s.HasData})
	}
	sort.Slice(newSpans, func(i, j int) bool { return newSpans[i].StartMs < newSpans[j].StartMs })
	srangesCacheUpdate(sid, table, timeframe, newSpans)
	return nil
}

func (q *Queries) UpdateSRanges(ctx context.Context, sid int32, table, timeframe string, startMs, stopMs int64, hasData bool) error {
	if startMs >= stopMs {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	rows, err := q.db.Query(ctx, `SELECT start_ms, stop_ms, has_data
FROM sranges_q
LATEST BY sid, tbl, timeframe, start_ms
WHERE sid = $1 AND tbl = $2 AND timeframe = $3 AND stop_ms >= $4 AND start_ms <= $5
  AND coalesce(is_deleted, false) = false
ORDER BY start_ms, stop_ms`,
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
		if err := rows.Scan(&s.StartMs, &s.StopMs, &s.HasData); err != nil {
			return err
		}
		if s.StopMs <= s.StartMs {
			continue
		}
		spans = append(spans, s)
		points = append(points, s.StartMs, s.StopMs)
	}
	if err := rows.Err(); err != nil {
		return err
	}

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

	now := time.Now().UTC()
	microOff := 0

	if len(spans) > 0 {
		for _, s := range spans {
			ts := now.Add(time.Duration(microOff) * time.Microsecond)
			microOff++
			_, err = q.db.Exec(ctx, `INSERT INTO sranges_q (sid, ts, tbl, timeframe, start_ms, stop_ms, has_data, is_deleted, deleted_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, true, $2)`,
				sid, ts, table, timeframe, s.StartMs, s.StopMs, s.HasData)
			if err != nil {
				return fmt.Errorf("logical delete srange: %w", err)
			}
		}
		MaybeCompact("sranges_q")
	}

	if len(segs) == 0 {
		// All spans deleted, nothing new – clear the cache for this key.
		srangesCacheDel(sid, table, timeframe)
		return nil
	}

	for _, s := range segs {
		ts := now.Add(time.Duration(microOff) * time.Microsecond)
		microOff++
		_, err = q.db.Exec(ctx, `INSERT INTO sranges_q (sid, ts, tbl, timeframe, start_ms, stop_ms, has_data, is_deleted)
VALUES ($1, $2, $3, $4, $5, $6, $7, false)`,
			sid, ts, table, timeframe, s.StartMs, s.StopMs, s.HasData)
		if err != nil {
			return fmt.Errorf("insert srange seg: %w", err)
		}
	}

	// Synchronously update in-process cache so subsequent reads in the same
	// process are not affected by QuestDB WAL commit lag (<100 ms).
	// We first load the full existing cache for this key (outside the window),
	// then merge in the new segs to produce the complete snapshot.
	cacheCopy := func() []srangeSpan {
		srangesCacheLock.RLock()
		key := srangesCacheKey{sid: sid, tbl: table, timeframe: timeframe}
		entry, ok := srangesCache[key]
		srangesCacheLock.RUnlock()
		if !ok {
			return nil
		}
		out := make([]srangeSpan, len(entry.spans))
		copy(out, entry.spans)
		return out
	}

	// Build updated full span list: keep existing spans outside [startMs, stopMs),
	// replace interior with the newly computed segs.
	existing := cacheCopy()
	newSpans := make([]srangeSpan, 0, len(existing)+len(segs))
	for _, s := range existing {
		if s.StopMs <= startMs || s.StartMs >= stopMs {
			newSpans = append(newSpans, s)
		}
	}
	for _, s := range segs {
		newSpans = append(newSpans, srangeSpan{StartMs: s.StartMs, StopMs: s.StopMs, HasData: s.HasData})
	}
	// Sort by StartMs
	sort.Slice(newSpans, func(i, j int) bool { return newSpans[i].StartMs < newSpans[j].StartMs })
	srangesCacheUpdate(sid, table, timeframe, newSpans)
	return nil
}
