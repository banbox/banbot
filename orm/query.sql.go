// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.24.0
// source: query.sql

package orm

import (
	"context"
)

type AddKHolesParams struct {
	Sid       int64
	Timeframe string
	Start     int64
	Stop      int64
}

const addKInfo = `-- name: AddKInfo :one
insert into kinfo
("sid", "timeframe", "start", "stop")
values ($1, $2, $3, $4)
    returning sid, timeframe, start, stop
`

type AddKInfoParams struct {
	Sid       int64
	Timeframe string
	Start     int64
	Stop      int64
}

func (q *Queries) AddKInfo(ctx context.Context, arg AddKInfoParams) (*KInfo, error) {
	row := q.db.QueryRow(ctx, addKInfo,
		arg.Sid,
		arg.Timeframe,
		arg.Start,
		arg.Stop,
	)
	var i KInfo
	err := row.Scan(
		&i.Sid,
		&i.Timeframe,
		&i.Start,
		&i.Stop,
	)
	return &i, err
}

type AddSymbolsParams struct {
	Exchange string
	Market   string
	Symbol   string
}

const addTask = `-- name: AddTask :one
insert into bottask
("mode", "name", "create_at", "start_at", "stop_at", "info")
values ($1, $2, $3, $4, $5, $6)
returning id, mode, name, create_at, start_at, stop_at, info
`

type AddTaskParams struct {
	Mode     string
	Name     string
	CreateAt int64
	StartAt  int64
	StopAt   int64
	Info     string
}

func (q *Queries) AddTask(ctx context.Context, arg AddTaskParams) (*BotTask, error) {
	row := q.db.QueryRow(ctx, addTask,
		arg.Mode,
		arg.Name,
		arg.CreateAt,
		arg.StartAt,
		arg.StopAt,
		arg.Info,
	)
	var i BotTask
	err := row.Scan(
		&i.ID,
		&i.Mode,
		&i.Name,
		&i.CreateAt,
		&i.StartAt,
		&i.StopAt,
		&i.Info,
	)
	return &i, err
}

const findTask = `-- name: FindTask :one
select id, mode, name, create_at, start_at, stop_at, info from bottask
where mode = $1 and name = $2
order by create_at desc
limit 1
`

type FindTaskParams struct {
	Mode string
	Name string
}

func (q *Queries) FindTask(ctx context.Context, arg FindTaskParams) (*BotTask, error) {
	row := q.db.QueryRow(ctx, findTask, arg.Mode, arg.Name)
	var i BotTask
	err := row.Scan(
		&i.ID,
		&i.Mode,
		&i.Name,
		&i.CreateAt,
		&i.StartAt,
		&i.StopAt,
		&i.Info,
	)
	return &i, err
}

const getExOrders = `-- name: GetExOrders :many
select id, task_id, inout_id, symbol, enter, order_type, order_id, side, create_at, price, average, amount, filled, status, fee, fee_type, update_at from exorder
where inout_id=$1
`

func (q *Queries) GetExOrders(ctx context.Context, inoutID int32) ([]*ExOrder, error) {
	rows, err := q.db.Query(ctx, getExOrders, inoutID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []*ExOrder{}
	for rows.Next() {
		var i ExOrder
		if err := rows.Scan(
			&i.ID,
			&i.TaskID,
			&i.InoutID,
			&i.Symbol,
			&i.Enter,
			&i.OrderType,
			&i.OrderID,
			&i.Side,
			&i.CreateAt,
			&i.Price,
			&i.Average,
			&i.Amount,
			&i.Filled,
			&i.Status,
			&i.Fee,
			&i.FeeType,
			&i.UpdateAt,
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getIOrder = `-- name: GetIOrder :one
select id, task_id, symbol, sid, timeframe, short, status, enter_tag, init_price, quote_cost, exit_tag, leverage, enter_at, exit_at, strategy, stg_ver, profit_rate, profit, info from iorder
where id = $1
`

func (q *Queries) GetIOrder(ctx context.Context, id int64) (*IOrder, error) {
	row := q.db.QueryRow(ctx, getIOrder, id)
	var i IOrder
	err := row.Scan(
		&i.ID,
		&i.TaskID,
		&i.Symbol,
		&i.Sid,
		&i.Timeframe,
		&i.Short,
		&i.Status,
		&i.EnterTag,
		&i.InitPrice,
		&i.QuoteCost,
		&i.ExitTag,
		&i.Leverage,
		&i.EnterAt,
		&i.ExitAt,
		&i.Strategy,
		&i.StgVer,
		&i.ProfitRate,
		&i.Profit,
		&i.Info,
	)
	return &i, err
}

const getKHoles = `-- name: GetKHoles :many
select id, sid, timeframe, start, stop from khole
where sid = $1 and timeframe = $2
`

type GetKHolesParams struct {
	Sid       int64
	Timeframe string
}

func (q *Queries) GetKHoles(ctx context.Context, arg GetKHolesParams) ([]*KHole, error) {
	rows, err := q.db.Query(ctx, getKHoles, arg.Sid, arg.Timeframe)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []*KHole{}
	for rows.Next() {
		var i KHole
		if err := rows.Scan(
			&i.ID,
			&i.Sid,
			&i.Timeframe,
			&i.Start,
			&i.Stop,
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getTask = `-- name: GetTask :one
select id, mode, name, create_at, start_at, stop_at, info from bottask
where id = $1
`

func (q *Queries) GetTask(ctx context.Context, id int64) (*BotTask, error) {
	row := q.db.QueryRow(ctx, getTask, id)
	var i BotTask
	err := row.Scan(
		&i.ID,
		&i.Mode,
		&i.Name,
		&i.CreateAt,
		&i.StartAt,
		&i.StopAt,
		&i.Info,
	)
	return &i, err
}

const listKInfos = `-- name: ListKInfos :many
select sid, timeframe, start, stop from kinfo
`

func (q *Queries) ListKInfos(ctx context.Context) ([]*KInfo, error) {
	rows, err := q.db.Query(ctx, listKInfos)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []*KInfo{}
	for rows.Next() {
		var i KInfo
		if err := rows.Scan(
			&i.Sid,
			&i.Timeframe,
			&i.Start,
			&i.Stop,
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const listSymbols = `-- name: ListSymbols :many
select id, exchange, market, symbol, list_ms, delist_ms from exsymbol
where exchange = $1
order by id
`

func (q *Queries) ListSymbols(ctx context.Context, exchange string) ([]*ExSymbol, error) {
	rows, err := q.db.Query(ctx, listSymbols, exchange)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []*ExSymbol{}
	for rows.Next() {
		var i ExSymbol
		if err := rows.Scan(
			&i.ID,
			&i.Exchange,
			&i.Market,
			&i.Symbol,
			&i.ListMs,
			&i.DelistMs,
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const listTaskPairs = `-- name: ListTaskPairs :many
select symbol from iorder
where task_id = $1
and enter_at >= $2
and enter_at <= $3
`

type ListTaskPairsParams struct {
	TaskID    int32
	EnterAt   int64
	EnterAt_2 int64
}

func (q *Queries) ListTaskPairs(ctx context.Context, arg ListTaskPairsParams) ([]string, error) {
	rows, err := q.db.Query(ctx, listTaskPairs, arg.TaskID, arg.EnterAt, arg.EnterAt_2)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []string{}
	for rows.Next() {
		var symbol string
		if err := rows.Scan(&symbol); err != nil {
			return nil, err
		}
		items = append(items, symbol)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const listTasks = `-- name: ListTasks :many
select id, mode, name, create_at, start_at, stop_at, info from bottask
order by id
`

func (q *Queries) ListTasks(ctx context.Context) ([]*BotTask, error) {
	rows, err := q.db.Query(ctx, listTasks)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []*BotTask{}
	for rows.Next() {
		var i BotTask
		if err := rows.Scan(
			&i.ID,
			&i.Mode,
			&i.Name,
			&i.CreateAt,
			&i.StartAt,
			&i.StopAt,
			&i.Info,
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const setKHole = `-- name: SetKHole :exec
update khole set start = $2, stop = $3
where id = $1
`

type SetKHoleParams struct {
	ID    int64
	Start int64
	Stop  int64
}

func (q *Queries) SetKHole(ctx context.Context, arg SetKHoleParams) error {
	_, err := q.db.Exec(ctx, setKHole, arg.ID, arg.Start, arg.Stop)
	return err
}

const setKInfo = `-- name: SetKInfo :exec
update kinfo set start = $3, stop = $4
where sid = $1 and timeframe = $2
`

type SetKInfoParams struct {
	Sid       int64
	Timeframe string
	Start     int64
	Stop      int64
}

func (q *Queries) SetKInfo(ctx context.Context, arg SetKInfoParams) error {
	_, err := q.db.Exec(ctx, setKInfo,
		arg.Sid,
		arg.Timeframe,
		arg.Start,
		arg.Stop,
	)
	return err
}

const setListMS = `-- name: SetListMS :exec
update exsymbol set list_ms = $2, delist_ms = $3
where id = $1
`

type SetListMSParams struct {
	ID       int64
	ListMs   int64
	DelistMs int64
}

func (q *Queries) SetListMS(ctx context.Context, arg SetListMSParams) error {
	_, err := q.db.Exec(ctx, setListMS, arg.ID, arg.ListMs, arg.DelistMs)
	return err
}