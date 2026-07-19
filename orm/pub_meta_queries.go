package orm

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/banbox/banexg/log"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
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

func releaseKlineInsertOwnership(sid int32, timeframe string, ts time.Time) error {
	err := releaseKlineInsertFileLock(klineInsertLockRoot(), sid, timeframe, ts)
	key := insKlineLockKey(sid, timeframe)
	insKlineLocksmu.Lock()
	delete(insKlineLocks, key)
	insKlineLocksmu.Unlock()
	return err
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
	unlock := LockCompactTableRead("calendars_q")
	defer unlock()
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
	unlock := LockCompactTableRead("adj_factors_q")
	defer unlock()
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
	unlock := LockCompactTableRead("adj_factors_q")
	defer unlock()
	return q.getAdjFactorsQuest(ctx, sid)
}

func (q *Queries) getAdjFactorsQuest(ctx context.Context, sid int32) ([]*AdjFactor, error) {
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
	unlock := LockCompactTableRead("adj_factors_q")
	defer unlock()
	factors, err := q.getAdjFactorsQuest(ctx, sid)
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
	MarkTableForCompact("adj_factors_q", len(factors))
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
	unlock := LockCompactTableRead("ins_kline_q")
	defer unlock()
	load := func() (*InsKline, error) {
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
	item, err := load()
	if err != nil {
		repaired, repairErr := tryRepairQuestDBMissingPartition(ctx, q.db, err, "GetInsKline")
		if repairErr != nil {
			return nil, repairErr
		}
		if repaired {
			return load()
		}
	}
	return item, err
}

func (q *Queries) GetAllInsKlines(ctx context.Context) ([]*InsKline, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return q.getAllInsKlinesPg(ctx)
	}
	unlock := LockCompactTableRead("ins_kline_q")
	defer unlock()
	load := func() ([]*InsKline, error) {
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
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	}
	items, err := load()
	if err != nil {
		repaired, repairErr := tryRepairQuestDBMissingPartition(ctx, q.db, err, "GetAllInsKlines")
		if repairErr != nil {
			return nil, repairErr
		}
		if repaired {
			return load()
		}
	}
	return items, err
}

func (q *Queries) DelInsKline(ctx context.Context, sid int32, timeframe string, ts time.Time) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		err := q.delInsKlinePg(ctx, sid, timeframe)
		releaseErr := releaseKlineInsertOwnership(sid, timeframe, ts)
		if err != nil {
			return err
		}
		return releaseErr
	}
	unlock := LockCompactTableRead("ins_kline_q")
	defer unlock()
	write := func() error {
		_, err := q.db.Exec(ctx, `INSERT INTO ins_kline_q (sid, timeframe, ts, start_ms, stop_ms, is_deleted)
	VALUES ($1, $2, $3, 0, 0, true)`, sid, timeframe, ts)
		return err
	}
	err := write()
	if err != nil {
		repaired, repairErr := tryRepairQuestDBMissingPartition(ctx, q.db, err, "DelInsKline")
		if repairErr != nil {
			return repairErr
		}
		if repaired {
			err = write()
		}
	}
	// Keep the table access guard through ownership release so a compact cannot
	// replace the lease table in the middle of this logical operation.
	releaseErr := releaseKlineInsertOwnership(sid, timeframe, ts)
	if err != nil {
		return err
	}
	MarkTableForCompact("ins_kline_q", 1)
	return releaseErr
}

func (q *Queries) AddInsKline(ctx context.Context, arg AddInsKlineParams) (time.Time, error) {
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
	if IsQuestDB {
		ts = normalizeQuestTimestamp(ts)
	}
	insKlineLocks[key] = ts
	insKlineLocksmu.Unlock()
	claimed, err := acquireKlineInsertFileLock(klineInsertLockRoot(), arg.Sid, arg.Timeframe, ts)
	if err != nil || !claimed {
		insKlineLocksmu.Lock()
		delete(insKlineLocks, key)
		insKlineLocksmu.Unlock()
		return time.Time{}, err
	}
	if !IsQuestDB {
		claimed, err = q.tryAddInsKlinePg(ctx, arg)
		if err != nil || !claimed {
			_ = releaseKlineInsertOwnership(arg.Sid, arg.Timeframe, ts)
			return time.Time{}, err
		}
		return ts, nil
	}
	unlock := LockCompactTableRead("ins_kline_q")
	defer unlock()

	write := func() error {
		_, err := q.db.Exec(ctx, `INSERT INTO ins_kline_q (sid, timeframe, ts, start_ms, stop_ms, is_deleted)
VALUES ($1, $2, $3, $4, $5, false)`, arg.Sid, arg.Timeframe, ts, arg.StartMs, arg.StopMs)
		return err
	}
	err = write()
	if err != nil {
		repaired, repairErr := tryRepairQuestDBMissingPartition(ctx, q.db, err, "AddInsKline")
		if repairErr != nil {
			err = repairErr
		} else if repaired {
			err = write()
		}
	}
	if err != nil {
		_ = releaseKlineInsertOwnership(arg.Sid, arg.Timeframe, ts)
		return time.Time{}, err
	}
	return ts, nil
}

type AddSymbolsParams struct {
	Exchange string `json:"exchange"`
	ExgReal  string `json:"exg_real"`
	Market   string `json:"market"`
	Symbol   string `json:"symbol"`
	Combined bool   `json:"combined"`
	ListMs   int64  `json:"list_ms"`
	DelistMs int64  `json:"delist_ms"`
	AggRules string `json:"agg_rules"`
}

type SetListMSParams struct {
	ID       int32 `json:"id"`
	ListMs   int64 `json:"list_ms"`
	DelistMs int64 `json:"delist_ms"`
}

type SetAggRulesParams struct {
	ID       int32  `json:"id"`
	AggRules string `json:"agg_rules"`
}

func (q *Queries) ListExchanges(ctx context.Context) ([]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return q.listExchangesPg(ctx)
	}
	unlock := LockCompactTableRead("exsymbol_q")
	defer unlock()
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
	unlock := LockCompactTableRead("exsymbol_q")
	defer unlock()
	rows, err := q.db.Query(ctx, `SELECT sid, exchange, exg_real, market, symbol, combined, list_ms, delist_ms, coalesce(agg_rules, '')
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
		if err := rows.Scan(&i.ID, &i.Exchange, &i.ExgReal, &i.Market, &i.Symbol, &i.Combined, &i.ListMs, &i.DelistMs, &i.AggRules); err != nil {
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
	unlock := LockCompactTableRead("exsymbol_q")
	defer unlock()
	if latest := queryMaxSidFromQDB(ctx, q.db); latest > maxSid {
		maxSid = latest
	}
	now := time.Now().UTC()
	lastSID := maxSid
	var lastSymbol AddSymbolsParams
	for i, s := range arg {
		maxSid++
		sid := maxSid
		lastSID = sid
		lastSymbol = s
		ts := now.Add(time.Duration(i) * time.Microsecond)
		_, err := q.db.Exec(ctx, `INSERT INTO exsymbol_q (sid, ts, exchange, exg_real, market, symbol, combined, list_ms, delist_ms, agg_rules, is_deleted)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, false)`, sid, ts, s.Exchange, s.ExgReal, s.Market, s.Symbol, s.Combined, s.ListMs, s.DelistMs, s.AggRules)
		if err != nil {
			return int64(i), err
		}
		cacheExSymbol(makeExSymbolFromAdd(sid, s))
	}
	// QuestDB WAL commits are async. The caller already has canonical in-process
	// state from cacheExSymbol, so visibility lag is informational rather than fatal.
	visible, err := questExsymbolVisible(ctx, q, lastSID)
	if err != nil {
		return int64(len(arg)), err
	}
	if !visible {
		log.Warn("questdb exsymbol row still not visible after timeout; continue with cached symbol state",
			zap.Int32("sid", lastSID),
			zap.String("exchange", lastSymbol.Exchange),
			zap.String("market", lastSymbol.Market),
			zap.String("symbol", lastSymbol.Symbol))
	}
	return int64(len(arg)), nil
}

func queryMaxSidFromQDB(ctx context.Context, db DBTX) int32 {
	var maxVal *int32
	row := db.QueryRow(ctx, `SELECT max(sid) FROM exsymbol_q`)
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
	unlock := LockCompactTableRead("exsymbol_q")
	defer unlock()
	item, err := waitForQuestExsymbolVisible(ctx, q, arg.ID)
	if err != nil || item == nil {
		return fmt.Errorf("SetListMS: sid %d not found: %w", arg.ID, err)
	}
	ts := time.Now().UTC()
	_, err = q.db.Exec(ctx, `INSERT INTO exsymbol_q (sid, ts, exchange, exg_real, market, symbol, combined, list_ms, delist_ms, agg_rules, is_deleted)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, false)`,
		item.ID, ts, item.Exchange, item.ExgReal, item.Market, item.Symbol, item.Combined, arg.ListMs, arg.DelistMs, item.AggRules)
	if err == nil {
		if err = waitForQuestExsymbolTimestampVisible(ctx, q, item.ID, ts); err != nil {
			return err
		}
		MarkTableForCompact("exsymbol_q", 1)
	}
	return err
}

func (q *Queries) SetAggRules(ctx context.Context, arg SetAggRulesParams) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !IsQuestDB {
		return q.setAggRulesPg(ctx, arg)
	}
	unlock := LockCompactTableRead("exsymbol_q")
	defer unlock()
	item, err := waitForQuestExsymbolVisible(ctx, q, arg.ID)
	if err != nil || item == nil {
		return fmt.Errorf("SetAggRules: sid %d not found: %w", arg.ID, err)
	}
	ts := time.Now().UTC()
	_, err = q.db.Exec(ctx, `INSERT INTO exsymbol_q (sid, ts, exchange, exg_real, market, symbol, combined, list_ms, delist_ms, agg_rules, is_deleted)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, false)`,
		item.ID, ts, item.Exchange, item.ExgReal, item.Market, item.Symbol, item.Combined, item.ListMs, item.DelistMs, arg.AggRules)
	if err == nil {
		item.AggRules = arg.AggRules
		cacheExSymbol(item)
		if err = waitForQuestExsymbolTimestampVisible(ctx, q, item.ID, ts); err != nil {
			return err
		}
		MarkTableForCompact("exsymbol_q", 1)
	}
	return err
}
