package ormo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

func InitTask(showLog bool, outDir string) *errs.Error {
	if len(accTasks) > 0 {
		return nil
	}
	if !core.LiveMode {
		accTasks[config.DefAcc] = &BotTask{ID: -1, Mode: core.RunMode, CreateAt: btime.UTCStamp(),
			StartAt: config.TimeRange.StartMS, StopAt: config.TimeRange.EndMS}
		taskIdAccMap[-1] = config.DefAcc
		if showLog {
			log.Info("init task ok", zap.Int64("id", -1))
		}
		return nil
	}
	orm.SetDbPath(orm.DbTrades, filepath.Join(outDir, fmt.Sprintf("orders_%s.db", config.Name)))
	q, conn, err := Conn(orm.DbTrades, true)
	if err != nil {
		return err
	}
	defer conn.Close()
	tasks, err_ := q.ListTasks(context.Background())
	if err_ != nil {
		return errs.New(core.ErrDbReadFail, err_)
	}
	idList := make([]string, 0, len(config.Accounts))
	for _, task := range tasks {
		parts := strings.Split(task.Name, "/")
		account := parts[len(parts)-1]
		task.StopAt = btime.UTCStamp()
		accTasks[account] = task
		taskIdAccMap[task.ID] = account
		idList = append(idList, fmt.Sprintf("%s:%v", account, task.ID))
	}
	for account := range config.Accounts {
		if task, ok := accTasks[account]; ok {
			task.StopAt = 0
			continue
		}
		task, err := q.GetAccTask(account)
		if err != nil {
			return err
		}
		task.StopAt = 0
		accTasks[account] = task
		taskIdAccMap[task.ID] = account
		idList = append(idList, fmt.Sprintf("%s:%v", account, task.ID))
	}
	if showLog {
		log.Info("init task ok", zap.String("id", strings.Join(idList, ", ")))
	}
	return nil
}

// InitTasksWithState installs in-memory task identities into an explicit
// runtime order state. Negative IDs are intentionally local-only: they keep
// simulated task identity independent from the process-wide legacy registry
// while remaining distinct for every account in one runtime.
func InitTasksWithState(state *OrderState, accounts []string, mode string, startAt, stopAt int64, showLog bool) *errs.Error {
	if state == nil {
		return errs.NewMsg(errs.CodeParamRequired, "order state is required")
	}
	if len(accounts) == 0 {
		accounts = []string{"default"}
	}
	accounts = append([]string(nil), accounts...)
	sort.Strings(accounts)
	nextID := int64(-1)
	ids := make([]string, 0, len(accounts))
	for _, account := range accounts {
		if account == "" || state.GetTask(account) != nil {
			continue
		}
		for state.GetTaskAcc(nextID) != "" {
			nextID--
		}
		task := &BotTask{ID: nextID, Mode: mode, CreateAt: state.TimeMS(), StartAt: startAt, StopAt: stopAt}
		state.SetTask(account, task)
		ids = append(ids, fmt.Sprintf("%s:%v", account, task.ID))
		nextID--
	}
	if showLog && len(ids) > 0 {
		log.Info("init runtime tasks ok", zap.String("ids", strings.Join(ids, ", ")))
	}
	return nil
}

// InitTaskWithState is the single-account compatibility wrapper around
// InitTasksWithState.
func InitTaskWithState(state *OrderState, account, mode string, startAt, stopAt int64, showLog bool) *errs.Error {
	if account == "" {
		account = "default"
	}
	return InitTasksWithState(state, []string{account}, mode, startAt, stopAt, showLog)
}

func InitLiveTasksWithState(state *OrderState, accounts []string, name string, real bool) *errs.Error {
	sess, conn, err := state.Conn(true)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx := context.Background()
	for _, account := range accounts {
		taskName := name
		if real {
			taskName += "/" + account
		}
		task, queryErr := sess.FindTask(ctx, FindTaskParams{Mode: core.RunModeLive, Name: taskName})
		if errors.Is(queryErr, sql.ErrNoRows) {
			nowMS := state.TimeMS()
			task, queryErr = sess.AddTask(ctx, AddTaskParams{
				Mode: core.RunModeLive, Name: taskName, CreateAt: nowMS, StartAt: nowMS,
			})
		}
		if queryErr != nil {
			return errs.New(core.ErrDbExecFail, queryErr)
		}
		state.SetTask(account, task)
	}
	return nil
}

func (q *Queries) GetAccTask(account string) (*BotTask, *errs.Error) {
	ctx := context.Background()
	var err_ error
	var task *BotTask
	taskName := config.Name
	if core.EnvReal {
		taskName += "/" + account
	}
	task, err_ = q.FindTask(ctx, FindTaskParams{
		Mode: core.RunMode,
		Name: taskName,
	})
	isLiveMode := core.LiveMode
	if err_ != nil || !isLiveMode {
		startAt := btime.UTCStamp()
		if !isLiveMode {
			startAt = config.TimeRange.StartMS
		}
		task, err_ = q.AddTask(ctx, AddTaskParams{
			Mode:     core.RunMode,
			Name:     taskName,
			CreateAt: btime.UTCStamp(),
			StartAt:  startAt,
			StopAt:   0,
		})
		if err_ != nil {
			return nil, errs.New(core.ErrDbExecFail, err_)
		}
	}
	return task, nil
}
