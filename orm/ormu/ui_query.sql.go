// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.29.0
// source: ui_query.sql

package ormu

import (
	"context"
	"strings"
)

const addTask = `-- name: AddTask :one
insert into task
(mode, args, config, path, strats, periods, pairs, create_at, start_at, stop_at, status, progress, order_num, profit_rate, win_rate, max_drawdown, sharpe, info, note)
values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    returning id, mode, args, config, path, strats, periods, pairs, create_at, start_at, stop_at, status, progress, order_num, profit_rate, win_rate, max_drawdown, sharpe, info, note
`

type AddTaskParams struct {
	Mode        string  `json:"mode"`
	Args        string  `json:"args"`
	Config      string  `json:"config"`
	Path        string  `json:"path"`
	Strats      string  `json:"strats"`
	Periods     string  `json:"periods"`
	Pairs       string  `json:"pairs"`
	CreateAt    int64   `json:"createAt"`
	StartAt     int64   `json:"startAt"`
	StopAt      int64   `json:"stopAt"`
	Status      int64   `json:"status"`
	Progress    float64 `json:"progress"`
	OrderNum    int64   `json:"orderNum"`
	ProfitRate  float64 `json:"profitRate"`
	WinRate     float64 `json:"winRate"`
	MaxDrawdown float64 `json:"maxDrawdown"`
	Sharpe      float64 `json:"sharpe"`
	Info        string  `json:"info"`
	Note        string  `json:"note"`
}

func (q *Queries) AddTask(ctx context.Context, arg AddTaskParams) (*Task, error) {
	row := q.db.QueryRowContext(ctx, addTask,
		arg.Mode,
		arg.Args,
		arg.Config,
		arg.Path,
		arg.Strats,
		arg.Periods,
		arg.Pairs,
		arg.CreateAt,
		arg.StartAt,
		arg.StopAt,
		arg.Status,
		arg.Progress,
		arg.OrderNum,
		arg.ProfitRate,
		arg.WinRate,
		arg.MaxDrawdown,
		arg.Sharpe,
		arg.Info,
		arg.Note,
	)
	var i Task
	err := row.Scan(
		&i.ID,
		&i.Mode,
		&i.Args,
		&i.Config,
		&i.Path,
		&i.Strats,
		&i.Periods,
		&i.Pairs,
		&i.CreateAt,
		&i.StartAt,
		&i.StopAt,
		&i.Status,
		&i.Progress,
		&i.OrderNum,
		&i.ProfitRate,
		&i.WinRate,
		&i.MaxDrawdown,
		&i.Sharpe,
		&i.Info,
		&i.Note,
	)
	return &i, err
}

const delTasks = `-- name: DelTasks :exec
delete from task where id in (/*SLICE:ids*/?)
`

func (q *Queries) DelTasks(ctx context.Context, ids []int64) error {
	query := delTasks
	var queryParams []interface{}
	if len(ids) > 0 {
		for _, v := range ids {
			queryParams = append(queryParams, v)
		}
		query = strings.Replace(query, "/*SLICE:ids*/?", strings.Repeat(",?", len(ids))[1:], 1)
	} else {
		query = strings.Replace(query, "/*SLICE:ids*/?", "NULL", 1)
	}
	_, err := q.db.ExecContext(ctx, query, queryParams...)
	return err
}

const getTask = `-- name: GetTask :one
select id, mode, args, config, path, strats, periods, pairs, create_at, start_at, stop_at, status, progress, order_num, profit_rate, win_rate, max_drawdown, sharpe, info, note from task
where id = ?
`

func (q *Queries) GetTask(ctx context.Context, id int64) (*Task, error) {
	row := q.db.QueryRowContext(ctx, getTask, id)
	var i Task
	err := row.Scan(
		&i.ID,
		&i.Mode,
		&i.Args,
		&i.Config,
		&i.Path,
		&i.Strats,
		&i.Periods,
		&i.Pairs,
		&i.CreateAt,
		&i.StartAt,
		&i.StopAt,
		&i.Status,
		&i.Progress,
		&i.OrderNum,
		&i.ProfitRate,
		&i.WinRate,
		&i.MaxDrawdown,
		&i.Sharpe,
		&i.Info,
		&i.Note,
	)
	return &i, err
}

const getTaskOptions = `-- name: GetTaskOptions :many
select strats, periods, start_at, stop_at from task
`

type GetTaskOptionsRow struct {
	Strats  string `json:"strats"`
	Periods string `json:"periods"`
	StartAt int64  `json:"startAt"`
	StopAt  int64  `json:"stopAt"`
}

func (q *Queries) GetTaskOptions(ctx context.Context) ([]*GetTaskOptionsRow, error) {
	rows, err := q.db.QueryContext(ctx, getTaskOptions)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*GetTaskOptionsRow
	for rows.Next() {
		var i GetTaskOptionsRow
		if err := rows.Scan(
			&i.Strats,
			&i.Periods,
			&i.StartAt,
			&i.StopAt,
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const setTaskNote = `-- name: SetTaskNote :exec
update task set note=? where id = ?
`

type SetTaskNoteParams struct {
	Note string `json:"note"`
	ID   int64  `json:"id"`
}

func (q *Queries) SetTaskNote(ctx context.Context, arg SetTaskNoteParams) error {
	_, err := q.db.ExecContext(ctx, setTaskNote, arg.Note, arg.ID)
	return err
}

const setTaskPath = `-- name: SetTaskPath :exec
update task set path=? where id = ?
`

type SetTaskPathParams struct {
	Path string `json:"path"`
	ID   int64  `json:"id"`
}

func (q *Queries) SetTaskPath(ctx context.Context, arg SetTaskPathParams) error {
	_, err := q.db.ExecContext(ctx, setTaskPath, arg.Path, arg.ID)
	return err
}

const updateTask = `-- name: UpdateTask :exec
update task set status=?,progress=?,order_num=?,profit_rate=?,win_rate=?,max_drawdown=?,sharpe=?,info=?  where id = ?
`

type UpdateTaskParams struct {
	Status      int64   `json:"status"`
	Progress    float64 `json:"progress"`
	OrderNum    int64   `json:"orderNum"`
	ProfitRate  float64 `json:"profitRate"`
	WinRate     float64 `json:"winRate"`
	MaxDrawdown float64 `json:"maxDrawdown"`
	Sharpe      float64 `json:"sharpe"`
	Info        string  `json:"info"`
	ID          int64   `json:"id"`
}

func (q *Queries) UpdateTask(ctx context.Context, arg UpdateTaskParams) error {
	_, err := q.db.ExecContext(ctx, updateTask,
		arg.Status,
		arg.Progress,
		arg.OrderNum,
		arg.ProfitRate,
		arg.WinRate,
		arg.MaxDrawdown,
		arg.Sharpe,
		arg.Info,
		arg.ID,
	)
	return err
}
