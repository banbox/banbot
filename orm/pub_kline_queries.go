package orm

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
)

func (q *Queries) PurgeKlineUn() *errs.Error {
	if !IsQuestDB {
		return purgeKlineUnPg()
	}
	ctx := context.Background()
	_, err := q.db.Exec(ctx, `DROP TABLE IF EXISTS kline_un_q`)
	if err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	_, err = q.db.Exec(ctx, `CREATE TABLE IF NOT EXISTS kline_un_q (
  sid        INT,
  timeframe  SYMBOL,
  ts         TIMESTAMP,
  stop_ms    LONG,
  expire_ms  LONG,
  open       DOUBLE,
  high       DOUBLE,
  low        DOUBLE,
  close      DOUBLE,
  volume     DOUBLE,
  quote      DOUBLE,
  buy_volume DOUBLE,
  trade_num  LONG,
  is_deleted BOOLEAN,
  deleted_at TIMESTAMP
) TIMESTAMP(ts) PARTITION BY MONTH WAL
DEDUP UPSERT KEYS(sid, timeframe, ts)`)
	if err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
}

func (q *Queries) DelKInfo(sid int32, timeFrame string) *errs.Error {
	ctx := context.Background()
	tbl := "kline_" + timeFrame
	if !IsQuestDB {
		if err := delKInfoPg(ctx, sid, tbl, timeFrame); err != nil {
			return NewDbErr(core.ErrDbExecFail, err)
		}
		return nil
	}
	rows, err := q.db.Query(ctx, `SELECT sid, tbl, timeframe, start_ms, stop_ms, has_data
FROM sranges_q
LATEST BY sid, tbl, timeframe, start_ms
WHERE sid = $1 AND tbl = $2 AND timeframe = $3 AND coalesce(is_deleted, false) = false`, sid, tbl, timeFrame)
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	defer rows.Close()

	type sKey struct {
		startMs int64
		stopMs  int64
		hasData bool
	}
	var items []sKey
	for rows.Next() {
		var dummy1 int32
		var dummy2, dummy3 string
		var k sKey
		if err := rows.Scan(&dummy1, &dummy2, &dummy3, &k.startMs, &k.stopMs, &k.hasData); err != nil {
			return NewDbErr(core.ErrDbReadFail, err)
		}
		items = append(items, k)
	}
	if err := rows.Err(); err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}

	if len(items) > 0 {
		now := time.Now().UTC()
		microOff := 0
		spans := make([]srangeSpan, len(items))
		for i, k := range items {
			spans[i] = srangeSpan{StartMs: k.startMs, StopMs: k.stopMs, HasData: k.hasData}
		}
		if err := batchInsertSrangesDeleted(ctx, q, sid, tbl, timeFrame, spans, now, &microOff); err != nil {
			return NewDbErr(core.ErrDbExecFail, err)
		}
	}
	// Invalidate in-process cache so subsequent reads reflect the deletion.
	srangesCacheDel(sid, tbl, timeFrame)
	MaybeCompact("sranges_q")
	return nil
}

func (q *Queries) GetKlineRange(sid int32, timeFrame string) (int64, int64) {
	ctx := context.Background()
	tbl := "kline_" + timeFrame
	if !IsQuestDB {
		return getKlineRangePg(ctx, sid, tbl, timeFrame)
	}
	row := q.db.QueryRow(ctx, `SELECT min(start_ms), max(stop_ms)
FROM (
  SELECT start_ms, stop_ms
  FROM sranges_q
  LATEST BY sid, tbl, timeframe, start_ms
  WHERE sid = $1 AND tbl = $2 AND timeframe = $3 AND has_data = true AND coalesce(is_deleted, false) = false
)`, sid, tbl, timeFrame)
	var start, stop *int64
	_ = row.Scan(&start, &stop)
	if start == nil || stop == nil {
		return 0, 0
	}
	return *start, *stop
}

func (q *Queries) GetKlineRanges(sidList []int32, timeFrame string) map[int32][2]int64 {
	if len(sidList) == 0 {
		return map[int32][2]int64{}
	}
	ctx := context.Background()
	tbl := "kline_" + timeFrame
	if !IsQuestDB {
		return getKlineRangesPg(ctx, sidList, tbl, timeFrame)
	}

	var texts = make([]string, len(sidList))
	for i, sid := range sidList {
		texts[i] = fmt.Sprintf("%v", sid)
	}
	sidText := strings.Join(texts, ", ")
	sqlText := fmt.Sprintf(`SELECT sid, min(start_ms), max(stop_ms)
FROM (
  SELECT sid, start_ms, stop_ms
  FROM sranges_q
  LATEST BY sid, tbl, timeframe, start_ms
  WHERE tbl = $1 AND timeframe = $2 AND has_data = true AND coalesce(is_deleted, false) = false
    AND sid IN (%s)
)
GROUP BY sid`, sidText)
	rows, err := q.db.Query(ctx, sqlText, tbl, timeFrame)
	if err != nil {
		return map[int32][2]int64{}
	}
	res := make(map[int32][2]int64)
	defer rows.Close()
	for rows.Next() {
		var start, stop int64
		var sid int32
		if err := rows.Scan(&sid, &start, &stop); err != nil {
			continue
		}
		res[sid] = [2]int64{start, stop}
	}
	return res
}

func (q *Queries) DelFactors(sid int32, startMS, endMS int64) *errs.Error {
	ctx := context.Background()
	if !IsQuestDB {
		return delFactorsPg(ctx, sid, startMS, endMS)
	}
	sqlText := `SELECT sid, sub_id, start_ms, factor
FROM adj_factors_q
LATEST BY sid, sub_id, start_ms
WHERE (sid = $1 OR sub_id = $1) AND coalesce(is_deleted, false) = false`
	args := []any{sid}
	if startMS > 0 {
		sqlText += fmt.Sprintf(" AND start_ms >= %d", startMS)
	}
	if endMS > 0 {
		sqlText += fmt.Sprintf(" AND start_ms < %d", endMS)
	}
	rows, err := q.db.Query(ctx, sqlText, args...)
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	defer rows.Close()

	type factorKey struct {
		sid     int32
		subId   int32
		startMs int64
		factor  float64
	}
	var items []factorKey
	for rows.Next() {
		var k factorKey
		if err := rows.Scan(&k.sid, &k.subId, &k.startMs, &k.factor); err != nil {
			return NewDbErr(core.ErrDbReadFail, err)
		}
		items = append(items, k)
	}
	if err := rows.Err(); err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}

	now := time.Now().UTC()
	for i, k := range items {
		ts := now.Add(time.Duration(i) * time.Microsecond)
		_, err := q.db.Exec(ctx, `INSERT INTO adj_factors_q (ts, sid, sub_id, start_ms, factor, is_deleted, deleted_at)
VALUES ($1, $2, $3, $4, $5, true, $1)`, ts, k.sid, k.subId, k.startMs, k.factor)
		if err != nil {
			return NewDbErr(core.ErrDbExecFail, err)
		}
	}
	MaybeCompact("adj_factors_q")
	return nil
}

func (q *Queries) DelKLineUn(sid int32, timeFrame string) *errs.Error {
	ctx := context.Background()
	if !IsQuestDB {
		return delKLineUnPg(ctx, sid, timeFrame)
	}
	ts := time.Now().UTC()
	_, err := q.db.Exec(ctx, `INSERT INTO kline_un_q (sid, timeframe, ts, stop_ms, expire_ms,
open, high, low, close, volume, quote, buy_volume, trade_num, is_deleted, deleted_at)
VALUES ($1, $2, $3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, true, $3)`, sid, timeFrame, ts)
	if err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	MaybeCompact("kline_un_q")
	return nil
}
