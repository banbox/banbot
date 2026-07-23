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

func TestRefreshAggPgRemovesIncompleteStaleBucket(t *testing.T) {
	initSeriesRepoTestApp(t, mustFindSeriesRepoConfig(t, "config.local.yml"))
	if IsQuestDB {
		t.Skip("postgres/timescale backend is not active")
	}

	const base = int64(1_700_000_100_000)
	sid := int32(time.Now().UnixNano()%1_000_000 + 2_000_000)
	ctx := context.Background()
	q, conn, err := Conn(ctx)
	if err != nil {
		t.Fatalf("Conn fail: %v", err)
	}
	defer conn.Release()
	defer q.db.Exec(ctx, `DELETE FROM kline_1m WHERE sid = $1; DELETE FROM kline_5m WHERE sid = $1`, sid)

	lockAlignOff.Lock()
	alignOffs[sid] = map[int64]int64{300_000: 0}
	lockAlignOff.Unlock()
	defer func() {
		lockAlignOff.Lock()
		delete(alignOffs, sid)
		lockAlignOff.Unlock()
	}()

	if _, execErr := q.db.Exec(ctx, `
INSERT INTO kline_5m (sid, time, open, high, low, close, volume, quote, buy_volume, trade_num)
VALUES ($1, $2, 1, 1, 1, 1, 1, 1, 1, 1)`, sid, base); execErr != nil {
		t.Fatal(execErr)
	}
	for index := int64(0); index < 4; index++ {
		if _, execErr := q.db.Exec(ctx, `
INSERT INTO kline_1m (sid, time, open, high, low, close, volume, quote, buy_volume, trade_num)
VALUES ($1, $2, 1, 1, 1, 1, 1, 1, 1, 1)`, sid, base+index*60_000); execErr != nil {
			t.Fatal(execErr)
		}
	}
	agg := NewKlineAgg("5m", "kline_5m", "1m", "", "", "", "", "")
	if _, _, err = q.refreshAggPg(agg, sid, base, base+300_000, ""); err != nil {
		t.Fatal(err)
	}
	var count int
	if scanErr := q.db.QueryRow(ctx, `SELECT count(*) FROM kline_5m WHERE sid = $1 AND time = $2`, sid, base).Scan(&count); scanErr != nil {
		t.Fatal(scanErr)
	}
	if count != 0 {
		t.Fatal("stale aggregate survived an incomplete source bucket")
	}
	if _, execErr := q.db.Exec(ctx, `
INSERT INTO kline_1m (sid, time, open, high, low, close, volume, quote, buy_volume, trade_num)
VALUES ($1, $2, 1, 1, 1, 1, 1, 1, 1, 1)`, sid, base+240_000); execErr != nil {
		t.Fatal(execErr)
	}
	if _, _, err = q.refreshAggPg(agg, sid, base, base+300_000, ""); err != nil {
		t.Fatal(err)
	}
	if scanErr := q.db.QueryRow(ctx, `SELECT count(*) FROM kline_5m WHERE sid = $1 AND time = $2`, sid, base).Scan(&count); scanErr != nil {
		t.Fatal(scanErr)
	}
	if count != 1 {
		t.Fatal("complete source bucket was not rebuilt")
	}
}
