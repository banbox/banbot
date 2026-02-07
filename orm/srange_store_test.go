package orm

import (
	"context"
	"math/rand"
	"path/filepath"
	"reflect"
	"testing"
)

type srangeSnapshot struct {
	StartMs int64
	StopMs  int64
	HasData bool
}

func useIsolatedBanPubDB(t *testing.T) {
	t.Helper()
	SetDbPath(DbPub, filepath.Join(t.TempDir(), "banpub.db"))
	db, err := BanPubConn(true)
	if err != nil {
		t.Fatalf("BanPubConn write fail: %v", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), `delete from sranges`); err != nil {
		t.Fatalf("clear sranges fail: %v", err)
	}
}

func mustUpdateRange(t *testing.T, sid int32, tbl, tf string, startMs, stopMs int64, hasData bool) {
	t.Helper()
	if err := PubQ().UpdateSRanges(context.Background(), sid, tbl, tf, startMs, stopMs, hasData); err != nil {
		t.Fatalf("UpdateSRanges(%d,%d,%v) fail: %v", startMs, stopMs, hasData, err)
	}
}

func listSnapshots(t *testing.T, sid int32, tbl, tf string) []srangeSnapshot {
	t.Helper()
	rows, err := PubQ().ListSRanges(context.Background(), sid, tbl, tf, 0, 1<<62)
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
	useIsolatedBanPubDB(t)
	const (
		sid = int32(1001)
		tbl = "kline_1m"
		tf  = "1m"
	)

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
	useIsolatedBanPubDB(t)
	const (
		sid = int32(1002)
		tbl = "kline_1m"
		tf  = "1m"
	)

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
	useIsolatedBanPubDB(t)
	const (
		sid = int32(1003)
		tbl = "kline_1m"
		tf  = "1m"
	)

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
	useIsolatedBanPubDB(t)
	const (
		sid = int32(1004)
		tbl = "kline_1m"
		tf  = "1m"
	)

	mustUpdateRange(t, sid, tbl, tf, 100, 150, false)
	mustUpdateRange(t, sid, tbl, tf, 150, 200, true)
	mustUpdateRange(t, sid, tbl, tf, 200, 240, true)
	mustUpdateRange(t, sid, tbl, tf, 240, 300, false)

	covered, err := PubQ().getCoveredRanges(context.Background(), sid, tbl, tf, 100, 300)
	if err != nil {
		t.Fatalf("getCoveredRanges fail: %v", err)
	}
	want := []MSRange{{Start: 150, Stop: 240}}
	if !reflect.DeepEqual(covered, want) {
		t.Fatalf("covered mismatch\nwant=%+v\ngot =%+v", want, covered)
	}
}

func TestUpdateSRangesRandomInvariant(t *testing.T) {
	useIsolatedBanPubDB(t)
	const (
		sid = int32(1005)
		tbl = "kline_1m"
		tf  = "1m"
	)

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
		gotCovered, err := PubQ().getCoveredRanges(context.Background(), sid, tbl, tf, 0, 1<<62)
		if err != nil {
			t.Fatalf("getCoveredRanges fail: %v", err)
		}
		if !reflect.DeepEqual(gotCovered, wantCovered) {
			t.Fatalf("covered mismatch at step=%d\nwant=%+v\ngot =%+v", i, wantCovered, gotCovered)
		}
	}
}
