package orm

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// insKlineLocks is an in-process lock map for active kline insert jobs.
// Key format: "<sid>/<timeframe>"  Value: ts assigned during AddInsKline.
// This avoids false "locked" detection caused by QuestDB WAL async commit lag:
// DelInsKline writes is_deleted=true to QuestDB but the WAL row may not be
// visible to LATEST BY queries immediately, making a subsequent GetInsKline
// see the old lock row and incorrectly block the next insert.
var (
	insKlineLocks   = make(map[string]time.Time)
	insKlineLocksmu sync.Mutex
)

func insKlineLockKey(sid int32, timeframe string) string {
	return fmt.Sprintf("%d/%s", sid, timeframe)
}

type AddAdjFactorsParams struct {
	Sid     int32   `json:"sid"`
	SubID   int32   `json:"sub_id"`
	StartMs int64   `json:"start_ms"`
	Factor  float64 `json:"factor"`
}

type AddCalendarsParams struct {
	Name    string `json:"name"`
	StartMs int64  `json:"start_ms"`
	StopMs  int64  `json:"stop_ms"`
}

type AddInsKlineParams struct {
	Sid       int32  `json:"sid"`
	Timeframe string `json:"timeframe"`
	StartMs   int64  `json:"start_ms"`
	StopMs    int64  `json:"stop_ms"`
}

func (q *Queries) AddCalendars(ctx context.Context, arg []AddCalendarsParams) (int64, error) {
	if len(arg) == 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return q.addCalendarsPg(ctx, arg)
	}
	now := time.Now().UTC()
	const cols = 4
	args := make([]any, 0, len(arg)*cols)
	for i, c := range arg {
		args = append(args, now.Add(time.Duration(i)*time.Microsecond), c.Name, c.StartMs, c.StopMs)
	}
	sql := "INSERT INTO calendars_q (ts,market,start_ms,stop_ms,is_deleted) VALUES " + buildBatchValues(len(arg), cols, ",false")
	if _, err := q.db.Exec(ctx, sql, args...); err != nil {
		return 0, err
	}
	return int64(len(arg)), nil
}

func (q *Queries) AddAdjFactors(ctx context.Context, arg []AddAdjFactorsParams) (int64, error) {
	if len(arg) == 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return q.addAdjFactorsPg(ctx, arg)
	}
	now := time.Now().UTC()
	const cols = 5
	args := make([]any, 0, len(arg)*cols)
	for i, f := range arg {
		args = append(args, now.Add(time.Duration(i)*time.Microsecond), f.Sid, f.SubID, f.StartMs, f.Factor)
	}
	sql := "INSERT INTO adj_factors_q (ts,sid,sub_id,start_ms,factor,is_deleted) VALUES " + buildBatchValues(len(arg), cols, ",false")
	if _, err := q.db.Exec(ctx, sql, args...); err != nil {
		return 0, err
	}
	return int64(len(arg)), nil
}

func (q *Queries) GetAdjFactors(ctx context.Context, sid int32) ([]*AdjFactor, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return q.getAdjFactorsPg(ctx, sid)
	}
	rows, err := q.db.Query(ctx, `SELECT sid, sub_id, start_ms, factor
FROM adj_factors_q
LATEST BY sid, sub_id, start_ms
WHERE sid = $1 AND coalesce(is_deleted, false) = false
ORDER BY start_ms`, sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*AdjFactor
	for rows.Next() {
		var i AdjFactor
		if err := rows.Scan(&i.Sid, &i.SubID, &i.StartMs, &i.Factor); err != nil {
			return nil, err
		}
		out = append(out, &i)
	}
	return out, rows.Err()
}

func (q *Queries) DelAdjFactors(ctx context.Context, sid int32) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return q.delAdjFactorsPg(ctx, sid)
	}
	factors, err := q.GetAdjFactors(ctx, sid)
	if err != nil {
		return err
	}
	if len(factors) == 0 {
		return nil
	}
	now := time.Now().UTC()
	if err = batchInsertAdjFactorsDeleted(ctx, q, factors, now); err != nil {
		return err
	}
	MaybeCompact("adj_factors_q")
	return nil
}

// batchInsertAdjFactorsDeleted marks a list of adj_factor rows as deleted in one multi-row INSERT.
func batchInsertAdjFactorsDeleted(ctx context.Context, q *Queries, factors []*AdjFactor, now time.Time) error {
	const cols = 5 // ts, sid, sub_id, start_ms, factor
	args := make([]any, 0, len(factors)*cols)
	for i, f := range factors {
		args = append(args, now.Add(time.Duration(i)*time.Microsecond), f.Sid, f.SubID, f.StartMs, f.Factor)
	}
	sql := "INSERT INTO adj_factors_q (ts,sid,sub_id,start_ms,factor,is_deleted) VALUES " + buildBatchValues(len(factors), cols, ",true")
	_, err := q.db.Exec(ctx, sql, args...)
	return err
}

func (q *Queries) GetInsKline(ctx context.Context, sid int32, timeframe string) (*InsKline, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return q.getInsKlinePg(ctx, sid, timeframe)
	}
	row := q.db.QueryRow(ctx, `SELECT sid, timeframe, ts, start_ms, stop_ms
FROM ins_kline_q
LATEST BY sid, timeframe
WHERE sid = $1 AND timeframe = $2 AND coalesce(is_deleted, false) = false`, sid, timeframe)
	var i InsKline
	if err := row.Scan(&i.Sid, &i.Timeframe, &i.Ts, &i.StartMs, &i.StopMs); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &i, nil
}

func (q *Queries) GetAllInsKlines(ctx context.Context) ([]*InsKline, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return q.getAllInsKlinesPg(ctx)
	}
	rows, err := q.db.Query(ctx, `SELECT sid, timeframe, ts, start_ms, stop_ms
FROM ins_kline_q
LATEST BY sid, timeframe
WHERE coalesce(is_deleted, false) = false`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*InsKline
	for rows.Next() {
		var i InsKline
		if err := rows.Scan(&i.Sid, &i.Timeframe, &i.Ts, &i.StartMs, &i.StopMs); err != nil {
			return nil, err
		}
		out = append(out, &i)
	}
	return out, rows.Err()
}

func (q *Queries) DelInsKline(ctx context.Context, sid int32, timeframe string, ts time.Time) error {
	key := insKlineLockKey(sid, timeframe)
	insKlineLocksmu.Lock()
	delete(insKlineLocks, key)
	insKlineLocksmu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return q.delInsKlinePg(ctx, sid, timeframe)
	}
	_, err := q.db.Exec(ctx, `INSERT INTO ins_kline_q (sid, timeframe, ts, start_ms, stop_ms, is_deleted)
VALUES ($1, $2, $3, 0, 0, true)`, sid, timeframe, ts)
	if err == nil {
		MaybeCompact("ins_kline_q")
	}
	return err
}

func (q *Queries) AddInsKline(ctx context.Context, arg AddInsKlineParams) (time.Time, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return q.addInsKlinePg(ctx, arg)
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

	_, err := q.db.Exec(ctx, `INSERT INTO ins_kline_q (sid, timeframe, ts, start_ms, stop_ms, is_deleted)
VALUES ($1, $2, $3, $4, $5, false)`, arg.Sid, arg.Timeframe, ts, arg.StartMs, arg.StopMs)
	if err != nil {
		insKlineLocksmu.Lock()
		delete(insKlineLocks, key)
		insKlineLocksmu.Unlock()
		return time.Time{}, err
	}
	return ts, nil
}

type AddSymbolsParams struct {
	Exchange string `json:"exchange"`
	ExgReal  string `json:"exg_real"`
	Market   string `json:"market"`
	Symbol   string `json:"symbol"`
}

type SetListMSParams struct {
	ID       int32 `json:"id"`
	ListMs   int64 `json:"list_ms"`
	DelistMs int64 `json:"delist_ms"`
}

func (q *Queries) ListExchanges(ctx context.Context) ([]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return q.listExchangesPg(ctx)
	}
	rows, err := q.db.Query(ctx, `SELECT DISTINCT exchange
FROM exsymbol_q
LATEST BY sid
WHERE coalesce(is_deleted, false) = false
ORDER BY exchange`)
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

func (q *Queries) ListSymbols(ctx context.Context, exchange string) ([]*ExSymbol, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return q.listSymbolsPg(ctx, exchange)
	}
	rows, err := q.db.Query(ctx, `SELECT sid, exchange, exg_real, market, symbol, combined, list_ms, delist_ms
FROM exsymbol_q
LATEST BY sid
WHERE exchange = $1 AND coalesce(is_deleted, false) = false
ORDER BY sid`, exchange)
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

func (q *Queries) AddSymbols(ctx context.Context, arg []AddSymbolsParams) (int64, error) {
	if len(arg) == 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return q.addSymbolsPg(ctx, arg)
	}
	if latest := queryMaxSidFromQDB(ctx); latest > maxSid {
		maxSid = latest
	}
	now := time.Now().UTC()
	for i, s := range arg {
		maxSid++
		sid := maxSid
		ts := now.Add(time.Duration(i) * time.Microsecond)
		_, err := q.db.Exec(ctx, `INSERT INTO exsymbol_q (sid, ts, exchange, exg_real, market, symbol, combined, list_ms, delist_ms, is_deleted)
VALUES ($1, $2, $3, $4, $5, $6, false, 0, 0, false)`, sid, ts, s.Exchange, s.ExgReal, s.Market, s.Symbol)
		if err != nil {
			return int64(i), err
		}
	}
	return int64(len(arg)), nil
}

func queryMaxSidFromQDB(ctx context.Context) int32 {
	var maxVal *int32
	row := pool.QueryRow(ctx, `SELECT max(sid) FROM exsymbol_q`)
	if err := row.Scan(&maxVal); err != nil || maxVal == nil {
		return 0
	}
	return *maxVal
}

func (q *Queries) SetListMS(ctx context.Context, arg SetListMSParams) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return q.setListMSPg(ctx, arg)
	}
	row := q.db.QueryRow(ctx, `SELECT sid, exchange, exg_real, market, symbol, combined, list_ms, delist_ms
FROM exsymbol_q
LATEST BY sid
WHERE sid = $1 AND coalesce(is_deleted, false) = false`, arg.ID)
	var i ExSymbol
	if err := row.Scan(&i.ID, &i.Exchange, &i.ExgReal, &i.Market, &i.Symbol, &i.Combined, &i.ListMs, &i.DelistMs); err != nil {
		return fmt.Errorf("SetListMS: sid %d not found: %w", arg.ID, err)
	}
	ts := time.Now().UTC()
	_, err := q.db.Exec(ctx, `INSERT INTO exsymbol_q (sid, ts, exchange, exg_real, market, symbol, combined, list_ms, delist_ms, is_deleted)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false)`,
		i.ID, ts, i.Exchange, i.ExgReal, i.Market, i.Symbol, i.Combined, arg.ListMs, arg.DelistMs)
	if err == nil {
		MaybeCompact("exsymbol_q")
	}
	return err
}
