package orm

// pg_queries.go implements TimescaleDB-specific query operations.
// All functions here are only called when IsQuestDB == false.

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

// ─────────────────────────────────────────────
// K-line insertion (CopyFrom for TimescaleDB)
// ─────────────────────────────────────────────

// insertKLinesPg uses pgx CopyFrom for high-throughput batch writes to TimescaleDB.
// Time column is `time int8` (milliseconds); columns unified with QuestDB (no `info`).
func insertKLinesPg(q *Queries, timeFrame string, sid int32, arr []*banexg.Kline) (int64, *errs.Error) {
	tblName := "kline_" + timeFrame
	cols := []string{"sid", "time", "open", "high", "low", "close", "volume", "quote", "buy_volume", "trade_num"}
	rows := make([]*KlineSid, 0, len(arr))
	for _, k := range arr {
		rows = append(rows, &KlineSid{Kline: *k, Sid: sid})
	}
	newSrc := func() *iterForAddKLinesPg { return &iterForAddKLinesPg{rows: rows} }
	n, err := q.db.CopyFrom(context.Background(), pgx.Identifier{tblName}, cols, newSrc())
	if err != nil {
		// On conflict (duplicate), fall back to DELETE + retry with a fresh iterator.
		tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
		startMS := arr[0].Time
		endMS := arr[len(arr)-1].Time + tfMSecs
		if delErr := delKLinesPg(q, timeFrame, sid, startMS, endMS); delErr != nil {
			return 0, delErr
		}
		n, err = q.db.CopyFrom(context.Background(), pgx.Identifier{tblName}, cols, newSrc())
		if err != nil {
			return 0, NewDbErr(core.ErrDbExecFail, err)
		}
	}
	return n, nil
}

// iterForAddKLinesPg implements pgx.CopyFromSource for TimescaleDB (time = ms int64).
// Uses an index pointer so the struct can be reset by creating a new instance.
type iterForAddKLinesPg struct {
	rows []*KlineSid
	idx  int
}

func (r *iterForAddKLinesPg) Next() bool {
	r.idx++
	return r.idx <= len(r.rows)
}

func (r *iterForAddKLinesPg) Values() ([]interface{}, error) {
	row := r.rows[r.idx-1]
	return []interface{}{
		row.Sid,
		row.Time, // int64 milliseconds
		row.Open,
		row.High,
		row.Low,
		row.Close,
		row.Volume,
		row.Quote,
		row.BuyVolume,
		row.TradeNum,
	}, nil
}

func (r *iterForAddKLinesPg) Err() error { return nil }

// delKLinesPg deletes kline rows for a given sid in [startMs, endMs) in TimescaleDB.
func delKLinesPg(q *Queries, timeFrame string, sid int32, startMs, endMs int64) *errs.Error {
	tblName := "kline_" + timeFrame
	sql := fmt.Sprintf("DELETE FROM %s WHERE sid = $1 AND time >= $2 AND time < $3", tblName)
	_, err := q.db.Exec(context.Background(), sql, sid, startMs, endMs)
	if err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
}

// ─────────────────────────────────────────────
// K-line query helpers (time column = `time int8`)
// ─────────────────────────────────────────────

// buildPgTimeFilter builds a WHERE clause fragment for time range filtering in TimescaleDB.
func buildPgTimeFilter(startMs, endMs int64) string {
	return fmt.Sprintf("time >= %d AND time < %d", startMs, endMs)
}

// queryOHLCVPg queries K-lines from TimescaleDB (time column = int8 ms).
func (q *Queries) queryOHLCVPg(sid int32, timeframe string, startMs, endMs int64, limit int, revRead bool) ([]*banexg.Kline, error) {
	tblName, subTF, rate := resolveTablePg(timeframe)
	rawLimit := limit
	if limit > 0 && subTF != "" && rate > 1 {
		limit = rate * (limit + 1)
	}

	var sqlText string
	if revRead {
		sqlText = fmt.Sprintf(`SELECT time,open,high,low,close,volume,quote,buy_volume,trade_num FROM %s
WHERE sid=%d AND time < %d
ORDER BY time DESC`, tblName, sid, endMs)
	} else {
		sqlText = fmt.Sprintf(`SELECT time,open,high,low,close,volume,quote,buy_volume,trade_num FROM %s
WHERE sid=%d AND %s
ORDER BY time`, tblName, sid, buildPgTimeFilter(startMs, endMs))
	}
	if limit > 0 {
		sqlText += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := q.db.Query(context.Background(), sqlText)
	items, err := mapToKlines(rows, err)
	if core.BackTestMode && config.Args != nil && strings.EqualFold(config.Args.LogLevel, "debug") {
		var firstMS, lastMS int64
		if len(items) > 0 {
			firstMS = items[0].Time
			lastMS = items[len(items)-1].Time
		}
		log.Debug("timescale query ohlcv",
			zap.Int32("sid", sid),
			zap.String("timeframe", timeframe),
			zap.String("table", tblName),
			zap.String("sub_tf", subTF),
			zap.Int("agg_rate", rate),
			zap.Int64("start_ms", startMs),
			zap.Int64("end_ms", endMs),
			zap.Int("raw_limit", rawLimit),
			zap.Int("sql_limit", limit),
			zap.Bool("rev_read", revRead),
			zap.Int("rows", len(items)),
			zap.Int64("first_bar_ms", firstMS),
			zap.Int64("last_bar_ms", lastMS))
	}
	return items, err
}

// resolveTablePg returns the actual table name, sub-timeframe (if aggregating), and rate for TimescaleDB.
func resolveTablePg(timeframe string) (string, string, int) {
	agg, ok := aggMap[timeframe]
	if ok {
		return agg.Table, "", 0
	}
	subTF, table, rate := getSubTf(timeframe)
	return table, subTF, rate
}

// getKLineTimesPg queries K-line timestamps in TimescaleDB (time = int8 ms).
func (q *Queries) getKLineTimesPg(sid int32, timeframe string, startMs, endMs int64) ([]int64, *errs.Error) {
	tblName := "kline_" + timeframe
	sqlText := fmt.Sprintf(`SELECT time FROM %s WHERE sid=%d AND %s ORDER BY time`,
		tblName, sid, buildPgTimeFilter(startMs, endMs))
	rows, err_ := q.db.Query(context.Background(), sqlText)
	res, err_ := mapToItems(rows, err_, func() (*int64, []any) {
		var t int64
		return &t, []any{&t}
	})
	if err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	out := make([]int64, len(res))
	for i, v := range res {
		out[i] = *v
	}
	return out, nil
}

// getKLineTimeRangePg returns (minTime, maxTime) from a kline table in TimescaleDB.
func (q *Queries) getKLineTimeRangePg(sid int32, timeframe string) (int64, int64, *errs.Error) {
	tblName := "kline_" + timeframe
	sqlText := fmt.Sprintf("SELECT min(time), max(time) FROM %s WHERE sid=%d", tblName, sid)
	row := q.db.QueryRow(context.Background(), sqlText)
	var minT, maxT *int64
	if err := row.Scan(&minT, &maxT); err != nil {
		return 0, 0, NewDbErr(core.ErrDbReadFail, err)
	}
	if minT == nil || maxT == nil {
		return 0, 0, nil
	}
	return *minT, *maxT, nil
}

// getKLineNumPg counts rows in a kline table for TimescaleDB.
func (q *Queries) getKLineNumPg(sid int32, timeFrame string, start, end int64) int {
	sqlText := fmt.Sprintf("SELECT COUNT(0) FROM kline_%s WHERE sid=%d AND %s",
		timeFrame, sid, buildPgTimeFilter(start, end))
	row := q.db.QueryRow(context.Background(), sqlText)
	var n int
	_ = row.Scan(&n)
	return n
}

// ─────────────────────────────────────────────
// refreshAgg for TimescaleDB (inline SQL aggregation)
// ─────────────────────────────────────────────

// refreshAggPg uses an in-DB INSERT … SELECT … GROUP BY for efficient aggregation in TimescaleDB.
func (q *Queries) refreshAggPg(item *KlineAgg, sid int32, aggStart, endMS int64, aggFrom, infoBy string) *errs.Error {
	if aggFrom == "" {
		aggFrom = item.AggFrom
	}
	if aggFrom == "" {
		return nil
	}
	fromTbl := "kline_" + aggFrom
	toTbl := item.Table
	tfMSecs := item.MSecs

	// Determine aggregation expression for buy_volume based on infoBy.
	var buyVolAgg string
	if infoBy == "last" {
		// PostgreSQL doesn't support LAST_VALUE in GROUP BY aggregates directly;
		// fall back to application-side aggregation for infoBy="last" case.
		return q.refreshAggPgAppSide(item, sid, aggStart, endMS, aggFrom, infoBy)
	}
	buyVolAgg = "SUM(buy_volume)"

	// Align windows (accounting for exchange-specific offset).
	offMS := GetAlignOff(sid, tfMSecs)
	alignedStart := utils2.AlignTfMSecs(aggStart-offMS, tfMSecs) + offMS
	alignedEnd := utils2.AlignTfMSecs(endMS-offMS, tfMSecs) + offMS
	if alignedStart >= alignedEnd {
		return nil
	}

	// Calculate bar group key in SQL: floor((time - offMS) / tfMSecs) * tfMSecs + offMS.
	// For UTC-aligned exchanges offMS == 0 which simplifies to (time / tfMSecs) * tfMSecs.
	groupExpr := buildAggGroupExpr(tfMSecs, offMS)

	insertSQL := fmt.Sprintf(`INSERT INTO %s (sid, time, open, high, low, close, volume, quote, buy_volume, trade_num)
SELECT $1,
       %s AS bar_time,
       (array_agg(open ORDER BY time))[1],
       MAX(high),
       MIN(low),
       (array_agg(close ORDER BY time DESC))[1],
       SUM(volume),
       SUM(quote),
       %s,
       SUM(trade_num)
FROM %s
WHERE sid = $1 AND time >= $2 AND time < $3
GROUP BY bar_time
HAVING COUNT(*) > 0
ON CONFLICT (sid, time) DO UPDATE SET
  open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
  volume = EXCLUDED.volume, quote = EXCLUDED.quote, buy_volume = EXCLUDED.buy_volume, trade_num = EXCLUDED.trade_num`,
		toTbl, groupExpr, buyVolAgg, fromTbl)

	_, err := q.db.Exec(context.Background(), insertSQL, sid, alignedStart, alignedEnd)
	if err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
}

// refreshAggPgAppSide performs application-side aggregation for TimescaleDB (used for infoBy="last").
func (q *Queries) refreshAggPgAppSide(item *KlineAgg, sid int32, aggStart, endMS int64, aggFrom, infoBy string) *errs.Error {
	tfMSecs := item.MSecs
	fromTbl := "kline_" + aggFrom
	ctx := context.Background()
	rows, err_ := q.db.Query(ctx, fmt.Sprintf(`
SELECT time,open,high,low,close,volume,quote,buy_volume,trade_num
FROM %s
WHERE sid=$1 AND time >= $2 AND time < $3
ORDER BY time`, fromTbl), sid, aggStart, endMS)
	src, err_ := mapToKlines(rows, err_)
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	if len(src) == 0 {
		return nil
	}
	fromTfMSecs := int64(utils2.TFToSecs(aggFrom) * 1000)
	offMS := GetAlignOff(sid, tfMSecs)
	aggBars, lastFinish := utils.BuildOHLCV(src, tfMSecs, 0, nil, fromTfMSecs, offMS, infoBy)
	if !lastFinish && len(aggBars) > 0 {
		aggBars = aggBars[:len(aggBars)-1]
	}
	if len(aggBars) == 0 {
		return nil
	}
	cut := aggBars[:0]
	for _, b := range aggBars {
		if b.Time < aggStart {
			continue
		}
		if b.Time+tfMSecs > endMS {
			break
		}
		cut = append(cut, b)
	}
	aggBars = cut
	if len(aggBars) == 0 {
		return nil
	}
	_, err := insertKLinesPg(q, item.TimeFrame, sid, aggBars)
	return err
}

// ─────────────────────────────────────────────
// exsymbol operations for TimescaleDB
// ─────────────────────────────────────────────

func (q *Queries) listSymbolsPg(ctx context.Context, exchange string) ([]*ExSymbol, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var sqlText string
	var args []any
	if exchange != "" {
		sqlText = `SELECT id, exchange, exg_real, market, symbol, combined, list_ms, delist_ms
FROM exsymbol WHERE exchange = $1 ORDER BY id`
		args = []any{exchange}
	} else {
		sqlText = `SELECT id, exchange, exg_real, market, symbol, combined, list_ms, delist_ms
FROM exsymbol ORDER BY id`
	}
	rows, err := q.db.Query(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ExSymbol
	for rows.Next() {
		var i ExSymbol
		if err := rows.Scan(&i.ID, &i.Exchange, &i.ExgReal, &i.Market, &i.Symbol, &i.Combined, &i.ListMs, &i.DelistMs); err != nil {
			return nil, err
		}
		out = append(out, &i)
	}
	return out, rows.Err()
}

func (q *Queries) listExchangesPg(ctx context.Context) ([]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := q.db.Query(ctx, `SELECT DISTINCT exchange FROM exsymbol ORDER BY exchange`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (q *Queries) addSymbolsPg(ctx context.Context, arg []AddSymbolsParams) (int64, error) {
	if len(arg) == 0 || ctx == nil {
		return 0, nil
	}
	// Sync maxSid from DB before assigning new IDs.
	if latest := queryMaxSidFromPg(ctx); latest > maxSid {
		maxSid = latest
	}
	for i, s := range arg {
		maxSid++
		sid := maxSid
		_, err := q.db.Exec(ctx, `INSERT INTO exsymbol (id, exchange, exg_real, market, symbol, combined, list_ms, delist_ms)
VALUES ($1, $2, $3, $4, $5, false, 0, 0)
ON CONFLICT (exchange, market, symbol) DO NOTHING`,
			sid, s.Exchange, s.ExgReal, s.Market, s.Symbol)
		if err != nil {
			return int64(i), err
		}
	}
	return int64(len(arg)), nil
}

func queryMaxSidFromPg(ctx context.Context) int32 {
	var maxVal *int32
	row := pool.QueryRow(ctx, `SELECT max(id) FROM exsymbol`)
	if err := row.Scan(&maxVal); err != nil || maxVal == nil {
		return 0
	}
	return *maxVal
}

func (q *Queries) setListMSPg(ctx context.Context, arg SetListMSParams) error {
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := q.db.Exec(ctx, `UPDATE exsymbol SET list_ms = $1, delist_ms = $2 WHERE id = $3`,
		arg.ListMs, arg.DelistMs, arg.ID)
	return err
}

// ─────────────────────────────────────────────
// calendars operations for TimescaleDB
// ─────────────────────────────────────────────

func (q *Queries) addCalendarsPg(ctx context.Context, arg []AddCalendarsParams) (int64, error) {
	if len(arg) == 0 || ctx == nil {
		return 0, nil
	}
	for i, c := range arg {
		_, err := q.db.Exec(ctx, `INSERT INTO calendars (market, start_ms, stop_ms)
VALUES ($1, $2, $3)
ON CONFLICT (market, start_ms) DO UPDATE SET stop_ms = EXCLUDED.stop_ms`,
			c.Name, c.StartMs, c.StopMs)
		if err != nil {
			return int64(i), err
		}
	}
	return int64(len(arg)), nil
}

func (q *Queries) getCalendarsPg(ctx context.Context, market string) ([]*Calendar, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := q.db.Query(ctx, `SELECT market, start_ms, stop_ms FROM calendars WHERE market = $1 ORDER BY start_ms`, market)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Calendar
	for rows.Next() {
		var c Calendar
		if err := rows.Scan(&c.Market, &c.StartMs, &c.StopMs); err != nil {
			return nil, err
		}
		out = append(out, &c)
	}
	return out, rows.Err()
}

func (q *Queries) delCalendarsPg(ctx context.Context, market string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := q.db.Exec(ctx, `DELETE FROM calendars WHERE market = $1`, market)
	return err
}

// ─────────────────────────────────────────────
// adj_factors operations for TimescaleDB
// ─────────────────────────────────────────────

func (q *Queries) addAdjFactorsPg(ctx context.Context, arg []AddAdjFactorsParams) (int64, error) {
	if len(arg) == 0 || ctx == nil {
		return 0, nil
	}
	for i, f := range arg {
		_, err := q.db.Exec(ctx, `INSERT INTO adj_factors (sid, sub_id, start_ms, factor)
VALUES ($1, $2, $3, $4)
ON CONFLICT (sid, sub_id, start_ms) DO UPDATE SET factor = EXCLUDED.factor`,
			f.Sid, f.SubID, f.StartMs, f.Factor)
		if err != nil {
			return int64(i), err
		}
	}
	return int64(len(arg)), nil
}

func (q *Queries) getAdjFactorsPg(ctx context.Context, sid int32) ([]*AdjFactor, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := q.db.Query(ctx, `SELECT sid, sub_id, start_ms, factor
FROM adj_factors WHERE sid = $1 ORDER BY start_ms`, sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*AdjFactor
	for rows.Next() {
		var a AdjFactor
		if err := rows.Scan(&a.Sid, &a.SubID, &a.StartMs, &a.Factor); err != nil {
			return nil, err
		}
		out = append(out, &a)
	}
	return out, rows.Err()
}

func (q *Queries) delAdjFactorsPg(ctx context.Context, sid int32) error {
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := q.db.Exec(ctx, `DELETE FROM adj_factors WHERE sid = $1 OR sub_id = $1`, sid)
	return err
}

func delFactorsPg(ctx context.Context, sid int32, startMS, endMS int64) *errs.Error {
	if ctx == nil {
		ctx = context.Background()
	}
	sqlText := `DELETE FROM adj_factors WHERE (sid = $1 OR sub_id = $1)`
	args := []any{sid}
	argIdx := 2
	if startMS > 0 {
		sqlText += fmt.Sprintf(" AND start_ms >= $%d", argIdx)
		args = append(args, startMS)
		argIdx++
	}
	if endMS > 0 {
		sqlText += fmt.Sprintf(" AND start_ms < $%d", argIdx)
		args = append(args, endMS)
	}
	_, err := pool.Exec(ctx, sqlText, args...)
	if err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
}

// ─────────────────────────────────────────────
// ins_kline operations for TimescaleDB
// ─────────────────────────────────────────────

func (q *Queries) addInsKlinePg(ctx context.Context, arg AddInsKlineParams) (time.Time, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	key := insKlineLockKey(arg.Sid, arg.Timeframe)
	insKlineLocksmu.Lock()
	if _, locked := insKlineLocks[key]; locked {
		insKlineLocksmu.Unlock()
		return time.Time{}, nil
	}
	ts := time.Now().UTC()
	insKlineLocks[key] = ts
	insKlineLocksmu.Unlock()

	_, err := q.db.Exec(ctx, `INSERT INTO ins_kline (sid, timeframe, start_ms, stop_ms)
VALUES ($1, $2, $3, $4)
ON CONFLICT (sid, timeframe) DO UPDATE SET start_ms = EXCLUDED.start_ms, stop_ms = EXCLUDED.stop_ms`,
		arg.Sid, arg.Timeframe, arg.StartMs, arg.StopMs)
	if err != nil {
		insKlineLocksmu.Lock()
		delete(insKlineLocks, key)
		insKlineLocksmu.Unlock()
		return time.Time{}, err
	}
	return ts, nil
}

func (q *Queries) getAllInsKlinesPg(ctx context.Context) ([]*InsKline, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := q.db.Query(ctx, `SELECT sid, timeframe, start_ms, stop_ms FROM ins_kline`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*InsKline
	for rows.Next() {
		var i InsKline
		if err := rows.Scan(&i.Sid, &i.Timeframe, &i.StartMs, &i.StopMs); err != nil {
			return nil, err
		}
		out = append(out, &i)
	}
	return out, rows.Err()
}

func (q *Queries) getInsKlinePg(ctx context.Context, sid int32, timeframe string) (*InsKline, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	row := q.db.QueryRow(ctx, `SELECT sid, timeframe, start_ms, stop_ms FROM ins_kline WHERE sid = $1 AND timeframe = $2`, sid, timeframe)
	var i InsKline
	if err := row.Scan(&i.Sid, &i.Timeframe, &i.StartMs, &i.StopMs); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &i, nil
}

func (q *Queries) delInsKlinePg(ctx context.Context, sid int32, timeframe string) error {
	key := insKlineLockKey(sid, timeframe)
	insKlineLocksmu.Lock()
	delete(insKlineLocks, key)
	insKlineLocksmu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	_, err := q.db.Exec(ctx, `DELETE FROM ins_kline WHERE sid = $1 AND timeframe = $2`, sid, timeframe)
	return err
}

// ─────────────────────────────────────────────
// kline_un (unfinished bar) operations for TimescaleDB
// ─────────────────────────────────────────────

func queryUnfinishPg(sid int32, timeFrame string, barStartMS int64) (*banexg.Kline, int64, *int64, error) {
	ctx := context.Background()
	row := pool.QueryRow(ctx, `SELECT start_ms, open, high, low, close, volume, quote, buy_volume, trade_num, stop_ms, expire_ms
FROM kline_un
WHERE sid = $1 AND timeframe = $2 AND start_ms >= $3`,
		sid, timeFrame, barStartMS,
	)
	var (
		startMs                    int64
		open, high, low            float64
		closeP, vol, quote, buyVol float64
		tradeNum                   int64
		stopMs                     int64
		expireMsVal                int64
	)
	if err := row.Scan(&startMs, &open, &high, &low, &closeP, &vol, &quote, &buyVol, &tradeNum, &stopMs, &expireMsVal); err != nil {
		return nil, 0, nil, err
	}
	bar := &banexg.Kline{
		Time: startMs, Open: open, High: high, Low: low, Close: closeP,
		Volume: vol, Quote: quote, BuyVolume: buyVol, TradeNum: tradeNum,
	}
	return bar, stopMs, &expireMsVal, nil
}

func setUnfinishPg(sid int32, tf string, endMS int64, bar *banexg.Kline) *errs.Error {
	expireMS := utils2.AlignTfMSecs(btime.UTCStamp(), 60000) + 60000
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO kline_un (sid, timeframe, start_ms, stop_ms, expire_ms,
open, high, low, close, volume, quote, buy_volume, trade_num)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (sid, timeframe) DO UPDATE SET
  start_ms = EXCLUDED.start_ms, stop_ms = EXCLUDED.stop_ms, expire_ms = EXCLUDED.expire_ms,
  open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
  volume = EXCLUDED.volume, quote = EXCLUDED.quote, buy_volume = EXCLUDED.buy_volume, trade_num = EXCLUDED.trade_num`,
		sid, tf, bar.Time, endMS, expireMS,
		bar.Open, bar.High, bar.Low, bar.Close, bar.Volume, bar.Quote, bar.BuyVolume, bar.TradeNum,
	)
	if err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
}

func delKLineUnPg(ctx context.Context, sid int32, timeFrame string) *errs.Error {
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := pool.Exec(ctx, `DELETE FROM kline_un WHERE sid = $1 AND timeframe = $2`, sid, timeFrame)
	if err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
}

func purgeKlineUnPg() *errs.Error {
	ctx := context.Background()
	_, err := pool.Exec(ctx, `TRUNCATE TABLE kline_un`)
	if err != nil {
		return NewDbErr(core.ErrDbExecFail, err)
	}
	return nil
}

// ─────────────────────────────────────────────
// DelKLines for TimescaleDB (standard DELETE, no table rewrite needed)
// ─────────────────────────────────────────────

// delKLinesPgBySid deletes rows for the given sid set from kline_<timeFrame>.
// delSids contains the sids whose data must be removed; other sids are kept.
func (q *Queries) delKLinesPgBySid(timeFrame string, delSids map[int32]bool) *errs.Error {
	tblName := "kline_" + timeFrame
	ctx := context.Background()

	if len(delSids) == 0 {
		return nil
	}

	delList := make([]string, 0, len(delSids))
	for sid := range delSids {
		delList = append(delList, itoa(int64(sid)))
	}
	sidIn := strings.Join(delList, ",")
	_, err_ := pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE sid IN (%s)", tblName, sidIn))
	if err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	return nil
}

// ─────────────────────────────────────────────
// CalcKLineRanges for TimescaleDB (time column = int8 ms)
// ─────────────────────────────────────────────

// buildAggGroupExpr returns a SQL expression that computes the bar open time
// from the `time` column (int8 ms), accounting for exchange-specific UTC offset.
// For UTC-aligned exchanges (offMS=0): (time / tfMSecs) * tfMSecs
// For offset exchanges: ((time - offMS) / tfMSecs) * tfMSecs + offMS
func buildAggGroupExpr(tfMSecs, offMS int64) string {
	if offMS == 0 {
		return fmt.Sprintf("(time / %d) * %d", tfMSecs, tfMSecs)
	}
	return fmt.Sprintf("((time - %d) / %d) * %d + %d", offMS, tfMSecs, tfMSecs, offMS)
}

func (q *Queries) calcKLineRangesPg(timeFrame string, sids map[int32]bool) (map[int32][2]int64, *errs.Error) {
	tblName := "kline_" + timeFrame
	where := ""
	if len(sids) > 0 {
		var b strings.Builder
		b.WriteString(" WHERE sid IN (")
		first := true
		for sid := range sids {
			if !first {
				b.WriteRune(',')
			}
			first = false
			b.WriteString(itoa(int64(sid)))
		}
		b.WriteRune(')')
		where = b.String()
	}
	sqlText := fmt.Sprintf("SELECT sid, min(time), max(time) FROM %s%s GROUP BY sid", tblName, where)
	ctx := context.Background()
	rows, err_ := q.db.Query(ctx, sqlText)
	if err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	defer rows.Close()
	res := make(map[int32][2]int64)
	tfMSecs := int64(utils2.TFToSecs(timeFrame) * 1000)
	for rows.Next() {
		var sid int32
		var realStart, realEnd int64
		err_ = rows.Scan(&sid, &realStart, &realEnd)
		res[sid] = [2]int64{realStart, realEnd + tfMSecs}
		if err_ != nil {
			return res, NewDbErr(core.ErrDbReadFail, err_)
		}
	}
	if err_ = rows.Err(); err_ != nil {
		return res, NewDbErr(core.ErrDbReadFail, err_)
	}
	return res, nil
}

// queryOHLCVBatchPg handles multi-sid batch K-line queries for TimescaleDB.
func (q *Queries) queryOHLCVBatchPg(exsMap map[int32]*ExSymbol, timeframe string, startMs, finishEndMS, tfMSecs int64, handle func(int32, []*banexg.Kline)) *errs.Error {
	sidTA := make([]string, 0, len(exsMap))
	for _, exs := range exsMap {
		sidTA = append(sidTA, itoa(int64(exs.ID)))
	}
	tblName, subTF, _ := resolveTablePg(timeframe)
	sidText := strings.Join(sidTA, ",")
	sqlText := fmt.Sprintf(`SELECT time,open,high,low,close,volume,quote,buy_volume,trade_num,sid FROM %s
WHERE %s AND sid IN (%s)
ORDER BY sid, time`, tblName, buildPgTimeFilter(startMs, finishEndMS), sidText)

	rows, err_ := q.db.Query(context.Background(), sqlText)
	arrs, err_ := mapToItems(rows, err_, func() (*KlineSid, []any) {
		var i KlineSid
		return &i, []any{&i.Time, &i.Open, &i.High, &i.Low, &i.Close, &i.Volume, &i.Quote, &i.BuyVolume, &i.TradeNum, &i.Sid}
	})
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}

	initCap := max(len(arrs)/max(len(exsMap), 1), 16)
	var klineArr []*banexg.Kline
	curSid := int32(0)
	fromTfMSecs := int64(0)
	if subTF != "" {
		fromTfMSecs = int64(utils2.TFToSecs(subTF) * 1000)
	}
	noFired := make(map[int32]bool)
	for _, exs := range exsMap {
		noFired[exs.ID] = true
	}
	callBack := func() {
		if fromTfMSecs > 0 {
			var lastDone bool
			offMS := GetAlignOff(curSid, tfMSecs)
			infoBy := exsMap[curSid].InfoBy()
			klineArr, lastDone = utils.BuildOHLCV(klineArr, tfMSecs, 0, nil, fromTfMSecs, offMS, infoBy)
			if !lastDone && len(klineArr) > 0 {
				klineArr = klineArr[:len(klineArr)-1]
			}
		}
		if len(klineArr) > 0 {
			delete(noFired, curSid)
			handle(curSid, klineArr)
		}
	}
	for _, k := range arrs {
		if k.Sid != curSid {
			if curSid > 0 && len(klineArr) > 0 {
				callBack()
			}
			curSid = k.Sid
			klineArr = make([]*banexg.Kline, 0, initCap)
		}
		klineArr = append(klineArr, &banexg.Kline{Time: k.Time, Open: k.Open, High: k.High, Low: k.Low,
			Close: k.Close, Volume: k.Volume, Quote: k.Quote, BuyVolume: k.BuyVolume, TradeNum: k.TradeNum})
	}
	if curSid > 0 && len(klineArr) > 0 {
		callBack()
	}
	for sid := range noFired {
		handle(sid, nil)
	}
	return nil
}
