// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0

package ormu

import (
	"context"
)

type Querier interface {
	AddTask(ctx context.Context, arg AddTaskParams) (*Task, error)
	DelTasks(ctx context.Context, ids []int64) error
	GetTask(ctx context.Context, id int64) (*Task, error)
	GetTaskOptions(ctx context.Context) ([]*GetTaskOptionsRow, error)
	SetTaskPath(ctx context.Context, arg SetTaskPathParams) error
	UpdateTask(ctx context.Context, arg UpdateTaskParams) error
}

var _ Querier = (*Queries)(nil)