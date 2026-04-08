package orm

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestUpdateSRangesPgMergesOverhangsWithWindow(t *testing.T) {
	initSeriesRepoTestApp(t, mustFindSeriesRepoConfig(t, "config.local.yml"))
	if IsQuestDB {
		t.Skip("postgres/timescale backend is not active")
	}

	const (
		tbl = "kline_1h"
		tf  = "1h"
	)
	sid := int32(time.Now().UnixNano()%1_000_000 + 1_000_000)
	ctx := context.Background()
	q, conn, err := Conn(ctx)
	if err != nil {
		t.Fatalf("Conn fail: %v", err)
	}
	defer conn.Release()

	if _, err_ := q.db.Exec(ctx, `DELETE FROM sranges WHERE sid = $1 AND tbl = $2 AND timeframe = $3`, sid, tbl, tf); err_ != nil {
		t.Fatalf("cleanup fail: %v", err_)
	}

	mustUpdate := func(startMs, stopMs int64) {
		t.Helper()
		if err_ := q.UpdateSRanges(ctx, sid, tbl, tf, startMs, stopMs, true); err_ != nil {
			t.Fatalf("UpdateSRanges(%d,%d) fail: %v", startMs, stopMs, err_)
		}
	}
	listRanges := func() []srangeSnapshot {
		t.Helper()
		rows, err_ := q.ListSRanges(ctx, sid, tbl, tf, 0, 1<<62)
		if err_ != nil {
			t.Fatalf("ListSRanges fail: %v", err_)
		}
		out := make([]srangeSnapshot, 0, len(rows))
		for _, row := range rows {
			out = append(out, srangeSnapshot{StartMs: row.StartMs, StopMs: row.StopMs, HasData: row.HasData})
		}
		return out
	}

	mustUpdate(100, 200)
	mustUpdate(150, 200)
	if got, want := listRanges(), []srangeSnapshot{{StartMs: 100, StopMs: 200, HasData: true}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected nested-update ranges\nwant=%+v\ngot =%+v", want, got)
	}
	if start, stop := getKlineRangePg(ctx, sid, tbl, tf); start != 100 || stop != 200 {
		t.Fatalf("unexpected first kline range: start=%d stop=%d", start, stop)
	}

	mustUpdate(200, 320)
	if got, want := listRanges(), []srangeSnapshot{{StartMs: 100, StopMs: 320, HasData: true}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected adjacent-extension ranges\nwant=%+v\ngot =%+v", want, got)
	}
	if start, stop := getKlineRangePg(ctx, sid, tbl, tf); start != 100 || stop != 320 {
		t.Fatalf("unexpected extended kline range: start=%d stop=%d", start, stop)
	}
}
