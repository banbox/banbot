package orm

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type PubQueries struct{}

var defaultPubQueries = &PubQueries{}

func PubQ() *PubQueries {
	return defaultPubQueries
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
	Timeout   int64  `json:"timeout"` // busy_timeout in ms, default 5000
}

func (q *PubQueries) AddCalendars(ctx context.Context, arg []AddCalendarsParams) (int64, error) {
	if len(arg) == 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db, err2 := BanPubConn(true)
	if err2 != nil {
		return 0, err2
	}
	defer db.Close()
	// 使用 IMMEDIATE 模式，在事务开始时就获取写锁，避免后续锁升级冲突
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		return 0, err
	}
	commit := false
	defer func() {
		if !commit {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.PrepareContext(ctx, `insert into calendars (name,start_ms,stop_ms) values (?,?,?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	for _, c := range arg {
		if _, err := stmt.ExecContext(ctx, c.Name, c.StartMs, c.StopMs); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	commit = true
	return int64(len(arg)), nil
}

func (q *PubQueries) AddAdjFactors(ctx context.Context, arg []AddAdjFactorsParams) (int64, error) {
	if len(arg) == 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db, err2 := BanPubConn(true)
	if err2 != nil {
		return 0, err2
	}
	defer db.Close()
	// 使用 IMMEDIATE 模式，在事务开始时就获取写锁，避免后续锁升级冲突
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		return 0, err
	}
	commit := false
	defer func() {
		if !commit {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.PrepareContext(ctx, `insert into adj_factors (sid,sub_id,start_ms,factor) values (?,?,?,?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	for _, f := range arg {
		if _, err := stmt.ExecContext(ctx, f.Sid, f.SubID, f.StartMs, f.Factor); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	commit = true
	return int64(len(arg)), nil
}

func (q *PubQueries) GetAdjFactors(ctx context.Context, sid int32) ([]*AdjFactor, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	db, err2 := BanPubConn(false)
	if err2 != nil {
		return nil, err2
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `select id,sid,sub_id,start_ms,factor from adj_factors where sid=? order by start_ms`, sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*AdjFactor
	for rows.Next() {
		var i AdjFactor
		if err := rows.Scan(&i.ID, &i.Sid, &i.SubID, &i.StartMs, &i.Factor); err != nil {
			return nil, err
		}
		out = append(out, &i)
	}
	return out, rows.Err()
}

func (q *PubQueries) DelAdjFactors(ctx context.Context, sid int32) error {
	if ctx == nil {
		ctx = context.Background()
	}
	db, err2 := BanPubConn(true)
	if err2 != nil {
		return err2
	}
	defer db.Close()
	_, err := db.ExecContext(ctx, `delete from adj_factors where sid=?`, sid)
	return err
}

func (q *PubQueries) GetInsKline(ctx context.Context, sid int32, timeframe string) (*InsKline, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	db, err2 := BanPubConn(false)
	if err2 != nil {
		return nil, err2
	}
	defer db.Close()
	row := db.QueryRowContext(ctx, `select id,sid,timeframe,start_ms,stop_ms from ins_kline where sid=? and timeframe=? limit 1`, sid, timeframe)
	var i InsKline
	if err := row.Scan(&i.ID, &i.Sid, &i.Timeframe, &i.StartMs, &i.StopMs); err != nil {
		return nil, err
	}
	return &i, nil
}

func (q *PubQueries) GetAllInsKlines(ctx context.Context) ([]*InsKline, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	db, err2 := BanPubConn(false)
	if err2 != nil {
		return nil, err2
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `select id,sid,timeframe,start_ms,stop_ms from ins_kline`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*InsKline
	for rows.Next() {
		var i InsKline
		if err := rows.Scan(&i.ID, &i.Sid, &i.Timeframe, &i.StartMs, &i.StopMs); err != nil {
			return nil, err
		}
		out = append(out, &i)
	}
	return out, rows.Err()
}

func (q *PubQueries) DelInsKline(ctx context.Context, id int64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	db, err2 := BanPubConn(true)
	if err2 != nil {
		return err2
	}
	defer db.Close()
	_, err := db.ExecContext(ctx, `delete from ins_kline where id=?`, id)
	return err
}

func (q *PubQueries) AddInsKline(ctx context.Context, arg AddInsKlineParams) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	db, err2 := BanPubConn(true)
	if err2 != nil {
		return 0, err2
	}
	defer db.Close()
	timeout := arg.Timeout
	if timeout <= 0 {
		timeout = 5000
	}
	_, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d", timeout))
	if err != nil {
		return 0, err
	}
	defer db.ExecContext(ctx, "PRAGMA busy_timeout = 10000")
	nowMs := time.Now().UTC().UnixMilli()
	res, err := db.ExecContext(ctx, `
insert or ignore into ins_kline (sid,timeframe,start_ms,stop_ms,created_ms)
values (?, ?, ?, ?, ?)`,
		arg.Sid, arg.Timeframe, arg.StartMs, arg.StopMs, nowMs)
	if err != nil {
		return 0, err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if aff == 0 {
		return 0, nil
	}
	return res.LastInsertId()
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

func (q *PubQueries) ListExchanges(ctx context.Context) ([]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	db, err2 := BanPubConn(false)
	if err2 != nil {
		return nil, err2
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `select distinct exchange from exsymbol order by exchange`)
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

func (q *PubQueries) ListSymbols(ctx context.Context, exchange string) ([]*ExSymbol, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	db, err2 := BanPubConn(false)
	if err2 != nil {
		return nil, err2
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `
select id,exchange,exg_real,market,symbol,combined,list_ms,delist_ms
from exsymbol
where exchange = ?
order by id`, exchange)
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

func (q *PubQueries) AddSymbols(ctx context.Context, arg []AddSymbolsParams) (int64, error) {
	if len(arg) == 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db, err2 := BanPubConn(true)
	if err2 != nil {
		return 0, err2
	}
	defer db.Close()
	// 使用 IMMEDIATE 模式，在事务开始时就获取写锁，避免后续锁升级冲突
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		return 0, err
	}
	commit := false
	defer func() {
		if !commit {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.PrepareContext(ctx, `insert into exsymbol (exchange,exg_real,market,symbol,combined,list_ms,delist_ms) values (?,?,?,?,?,?,?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	for _, s := range arg {
		if _, err := stmt.ExecContext(ctx, s.Exchange, s.ExgReal, s.Market, s.Symbol, 0, int64(0), int64(0)); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	commit = true
	return int64(len(arg)), nil
}

func (q *PubQueries) SetListMS(ctx context.Context, arg SetListMSParams) error {
	if ctx == nil {
		ctx = context.Background()
	}
	db, err2 := BanPubConn(true)
	if err2 != nil {
		return err2
	}
	defer db.Close()
	_, err := db.ExecContext(ctx, `update exsymbol set list_ms = ?, delist_ms = ? where id = ?`, arg.ListMs, arg.DelistMs, arg.ID)
	return err
}
