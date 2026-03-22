package orm

import (
	"context"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

type srangeSnapshot struct {
	StartMs int64
	StopMs  int64
	HasData bool
}

func ensureQDBPool(t *testing.T) {
	t.Helper()
	if pool == nil {
		t.Skip("QuestDB pool not available, skipping integration test")
	}
}

func cleanSRanges(t *testing.T, sid int32, tbl, tf string) {
	t.Helper()
	ctx := context.Background()
	rows, err := pool.Query(ctx, `SELECT start_ms, stop_ms, has_data
FROM sranges_q
LATEST BY sid, tbl, timeframe, start_ms
WHERE sid = $1 AND tbl = $2 AND timeframe = $3 AND coalesce(is_deleted, false) = false`, sid, tbl, tf)
	if err != nil {
		return
	}
	defer rows.Close()
	type item struct {
		startMs int64
		stopMs  int64
		hasData bool
	}
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.startMs, &it.stopMs, &it.hasData); err != nil {
			return
		}
		items = append(items, it)
	}
	now := time.Now().UTC()
	for i, it := range items {
		ts := now.Add(time.Duration(i) * time.Microsecond)
		_, _ = pool.Exec(ctx, `INSERT INTO sranges_q (sid, ts, tbl, timeframe, start_ms, stop_ms, has_data, is_deleted)
VALUES ($1, $2, $3, $4, $5, $6, $7, true)`, sid, ts, tbl, tf, it.startMs, it.stopMs, it.hasData)
	}
	time.Sleep(200 * time.Millisecond)
}

func mustUpdateRange(t *testing.T, sid int32, tbl, tf string, startMs, stopMs int64, hasData bool) {
	t.Helper()
	sess, conn, err2 := Conn(nil)
	if err2 != nil {
		t.Fatalf("Conn fail: %v", err2)
	}
	defer conn.Release()
	if err := sess.UpdateSRanges(context.Background(), sid, tbl, tf, startMs, stopMs, hasData); err != nil {
		t.Fatalf("UpdateSRanges(%d,%d,%v) fail: %v", startMs, stopMs, hasData, err)
	}
	time.Sleep(150 * time.Millisecond)
}

func listSnapshots(t *testing.T, sid int32, tbl, tf string) []srangeSnapshot {
	t.Helper()
	sess, conn, err2 := Conn(nil)
	if err2 != nil {
		t.Fatalf("Conn fail: %v", err2)
	}
	defer conn.Release()
	rows, err := sess.ListSRanges(context.Background(), sid, tbl, tf, 0, 1<<62)
	if err != nil {
		t.Fatalf("ListSRanges fail: %v", err)
	}
	out := make([]srangeSnapshot, 0, len(rows))
	for _, r := range rows {
		out = append(out, srangeSnapshot{StartMs: r.StartMs, StopMs: r.StopMs, HasData: r.HasData})
	}
	return out
}

func assertNoOverlapAndNoSameStateTouch(t *testing.T, rows []srangeSnapshot) {
	t.Helper()
	for i := 1; i < len(rows); i++ {
		prev, cur := rows[i-1], rows[i]
		if cur.StartMs < prev.StopMs {
			t.Fatalf("overlap found: prev=%+v cur=%+v", prev, cur)
		}
		if cur.StartMs == prev.StopMs && cur.HasData == prev.HasData {
			t.Fatalf("touching ranges with same has_data should be merged: prev=%+v cur=%+v", prev, cur)
		}
	}
}

func TestUpdateSRangesMergeOutOfOrderTouching(t *testing.T) {
	ensureQDBPool(t)
	const (
		sid = int32(90001)
		tbl = "kline_1m"
		tf  = "1m"
	)
	cleanSRanges(t, sid, tbl, tf)

	mustUpdateRange(t, sid, tbl, tf, 600, 1000, true)
	mustUpdateRange(t, sid, tbl, tf, 100, 600, true)

	got := listSnapshots(t, sid, tbl, tf)
	want := []srangeSnapshot{{StartMs: 100, StopMs: 1000, HasData: true}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sranges mismatch\nwant=%+v\ngot =%+v", want, got)
	}
	assertNoOverlapAndNoSameStateTouch(t, got)
}

func TestUpdateSRangesSplitByDifferentHasData(t *testing.T) {
	ensureQDBPool(t)
	const (
		sid = int32(90002)
		tbl = "kline_1m"
		tf  = "1m"
	)
	cleanSRanges(t, sid, tbl, tf)

	mustUpdateRange(t, sid, tbl, tf, 100, 300, true)
	mustUpdateRange(t, sid, tbl, tf, 180, 220, false)

	got := listSnapshots(t, sid, tbl, tf)
	want := []srangeSnapshot{
		{StartMs: 100, StopMs: 180, HasData: true},
		{StartMs: 180, StopMs: 220, HasData: false},
		{StartMs: 220, StopMs: 300, HasData: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sranges mismatch\nwant=%+v\ngot =%+v", want, got)
	}
	assertNoOverlapAndNoSameStateTouch(t, got)
}

func TestUpdateSRangesMixedUpdatesInvariant(t *testing.T) {
	ensureQDBPool(t)
	const (
		sid = int32(90003)
		tbl = "kline_1m"
		tf  = "1m"
	)
	cleanSRanges(t, sid, tbl, tf)

	mustUpdateRange(t, sid, tbl, tf, 100, 200, true)
	mustUpdateRange(t, sid, tbl, tf, 140, 160, false)
	mustUpdateRange(t, sid, tbl, tf, 160, 260, true)
	mustUpdateRange(t, sid, tbl, tf, 80, 120, false)
	mustUpdateRange(t, sid, tbl, tf, 50, 90, true)

	got := listSnapshots(t, sid, tbl, tf)
	want := []srangeSnapshot{
		{StartMs: 50, StopMs: 90, HasData: true},
		{StartMs: 90, StopMs: 120, HasData: false},
		{StartMs: 120, StopMs: 140, HasData: true},
		{StartMs: 140, StopMs: 160, HasData: false},
		{StartMs: 160, StopMs: 260, HasData: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sranges mismatch\nwant=%+v\ngot =%+v", want, got)
	}
	assertNoOverlapAndNoSameStateTouch(t, got)
}

func TestGetCoveredRangesOnlyHasData(t *testing.T) {
	ensureQDBPool(t)
	const (
		sid = int32(90004)
		tbl = "kline_1m"
		tf  = "1m"
	)
	cleanSRanges(t, sid, tbl, tf)

	mustUpdateRange(t, sid, tbl, tf, 100, 150, false)
	mustUpdateRange(t, sid, tbl, tf, 150, 200, true)
	mustUpdateRange(t, sid, tbl, tf, 200, 240, true)
	mustUpdateRange(t, sid, tbl, tf, 240, 300, false)

	covSess, covConn, err2 := Conn(nil)
	if err2 != nil {
		t.Fatalf("Conn fail: %v", err2)
	}
	defer covConn.Release()
	covered, err := covSess.getCoveredRanges(context.Background(), sid, tbl, tf, 100, 300)
	if err != nil {
		t.Fatalf("getCoveredRanges fail: %v", err)
	}
	want := []MSRange{{Start: 150, Stop: 240}}
	if !reflect.DeepEqual(covered, want) {
		t.Fatalf("covered mismatch\nwant=%+v\ngot =%+v", want, covered)
	}
}

func TestUpdateSRangesRandomInvariant(t *testing.T) {
	ensureQDBPool(t)
	const (
		sid = int32(90005)
		tbl = "kline_1m"
		tf  = "1m"
	)
	cleanSRanges(t, sid, tbl, tf)

	rng := rand.New(rand.NewSource(20260207))
	for i := 0; i < 200; i++ {
		start := int64(rng.Intn(2000)) * 1_000
		stop := start + int64(rng.Intn(90)+10)*1_000
		hasData := rng.Intn(2) == 1
		mustUpdateRange(t, sid, tbl, tf, start, stop, hasData)

		rows := listSnapshots(t, sid, tbl, tf)
		assertNoOverlapAndNoSameStateTouch(t, rows)
		for _, r := range rows {
			if r.StartMs >= r.StopMs {
				t.Fatalf("invalid range found: %+v", r)
			}
		}

		wantCovered := make([]MSRange, 0, len(rows))
		for _, r := range rows {
			if r.HasData {
				wantCovered = append(wantCovered, MSRange{Start: r.StartMs, Stop: r.StopMs})
			}
		}
		covSess, covConn, err2 := Conn(nil)
		if err2 != nil {
			t.Fatalf("Conn fail: %v", err2)
		}
		defer covConn.Release()
		gotCovered, err := covSess.getCoveredRanges(context.Background(), sid, tbl, tf, 0, 1<<62)
		if err != nil {
			t.Fatalf("getCoveredRanges fail: %v", err)
		}
		if !reflect.DeepEqual(gotCovered, wantCovered) {
			t.Fatalf("covered mismatch at step=%d\nwant=%+v\ngot =%+v", i, wantCovered, gotCovered)
		}
	}
}

// mustUpdateWithHoles calls UpdateSRangesWithHoles and waits for WAL to commit.
func mustUpdateWithHoles(t *testing.T, sid int32, tbl, tf string, startMs, stopMs int64, holes []MSRange) {
	t.Helper()
	sess, conn, err2 := Conn(nil)
	if err2 != nil {
		t.Fatalf("Conn fail: %v", err2)
	}
	defer conn.Release()
	if err := sess.UpdateSRangesWithHoles(context.Background(), sid, tbl, tf, startMs, stopMs, holes); err != nil {
		t.Fatalf("UpdateSRangesWithHoles fail: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
}

// TestUpdateSRangesWithHolesNoHoles verifies that the entire window is marked has_data=true
// when no holes are provided.
func TestUpdateSRangesWithHolesNoHoles(t *testing.T) {
	ensureQDBPool(t)
	const (
		sid = int32(90010)
		tbl = "kline_1h"
		tf  = "1h"
	)
	cleanSRanges(t, sid, tbl, tf)

	mustUpdateWithHoles(t, sid, tbl, tf, 1000, 5000, nil)

	got := listSnapshots(t, sid, tbl, tf)
	want := []srangeSnapshot{{StartMs: 1000, StopMs: 5000, HasData: true}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("want=%+v got=%+v", want, got)
	}
}

// TestUpdateSRangesWithHolesSingleHole is the exact scenario that was broken:
// a window with one hole produces [true, false, true] in a single atomic write,
// so that LATEST BY always returns the correct has_data=true for non-hole regions.
func TestUpdateSRangesWithHolesSingleHole(t *testing.T) {
	ensureQDBPool(t)
	const (
		sid = int32(90011)
		tbl = "kline_1h"
		tf  = "1h"
	)
	cleanSRanges(t, sid, tbl, tf)

	// Window [1000, 9000), hole in the middle [4000, 5000).
	mustUpdateWithHoles(t, sid, tbl, tf, 1000, 9000, []MSRange{{Start: 4000, Stop: 5000}})

	got := listSnapshots(t, sid, tbl, tf)
	want := []srangeSnapshot{
		{StartMs: 1000, StopMs: 4000, HasData: true},
		{StartMs: 4000, StopMs: 5000, HasData: false},
		{StartMs: 5000, StopMs: 9000, HasData: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("want=%+v\ngot =%+v", want, got)
	}
	assertNoOverlapAndNoSameStateTouch(t, got)
}

// TestUpdateSRangesWithHolesHoleCoversAll ensures that when the single hole equals the
// entire window the result is one has_data=false segment (no spurious true segments).
func TestUpdateSRangesWithHolesHoleCoversAll(t *testing.T) {
	ensureQDBPool(t)
	const (
		sid = int32(90012)
		tbl = "kline_1h"
		tf  = "1h"
	)
	cleanSRanges(t, sid, tbl, tf)

	mustUpdateWithHoles(t, sid, tbl, tf, 1000, 9000, []MSRange{{Start: 1000, Stop: 9000}})

	got := listSnapshots(t, sid, tbl, tf)
	want := []srangeSnapshot{{StartMs: 1000, StopMs: 9000, HasData: false}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("want=%+v got=%+v", want, got)
	}
}

// TestUpdateSRangesWithHolesOverwritesPrevious verifies that a second call correctly
// overwrites the previous state (the bug scenario: previous false spans being read and
// the new true spans surviving in LATEST BY).
func TestUpdateSRangesWithHolesOverwritesPrevious(t *testing.T) {
	ensureQDBPool(t)
	const (
		sid = int32(90013)
		tbl = "kline_1h"
		tf  = "1h"
	)
	cleanSRanges(t, sid, tbl, tf)

	// First call: entire window is a hole (no data downloaded yet).
	mustUpdateWithHoles(t, sid, tbl, tf, 1000, 9000, []MSRange{{Start: 1000, Stop: 9000}})
	got := listSnapshots(t, sid, tbl, tf)
	if len(got) != 1 || got[0].HasData {
		t.Fatalf("after first call want single false span, got=%+v", got)
	}

	// Second call: data is now downloaded, no holes remain.
	mustUpdateWithHoles(t, sid, tbl, tf, 1000, 9000, nil)
	got = listSnapshots(t, sid, tbl, tf)
	want := []srangeSnapshot{{StartMs: 1000, StopMs: 9000, HasData: true}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("after second call want=%+v got=%+v", want, got)
	}
}
