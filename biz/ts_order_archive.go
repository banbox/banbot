package biz

import (
	"time"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm/ormo"
	"go.uber.org/zap"
)

// Execution-backed time-series managers keep orders only as in-memory
// projections of the account ledger. The web UI, Telegram and strategy history
// queries read closed orders from the trades database, so each closed
// projection is copied there once as a read-only historical row. The ledger
// stays the source of truth: a failed or missing copy never affects trading.

// EnsureTimeSeriesOrderTask creates or loads the trades-database task that
// owns archived orders of a live runtime. Without a task nothing is archived.
func EnsureTimeSeriesOrderTask(deps RuntimeDeps) {
	if deps.Core == nil || !deps.Core.LiveMode || deps.Orders == nil {
		return
	}
	if deps.Orders.GetTaskID(deps.DefaultAccount) >= 0 {
		return
	}
	name := "banbot"
	if deps.Config != nil && deps.Config.View() != nil && deps.Config.View().Name != "" {
		name = deps.Config.View().Name
	}
	if err := ormo.InitLiveTasksWithState(deps.Orders, []string{deps.DefaultAccount}, name, deps.Core.EnvReal); err != nil {
		deps.Logger().Warn("order history disabled: trades database task unavailable", zap.String("account", deps.DefaultAccount), zap.Error(err))
	}
}

func timeSeriesArchiveKey(account execution.AccountKey, strategy execution.StrategyID, lot execution.VirtualLotID) string {
	return account.VenueSessionIdentity + "|" + account.Account + "|" + account.SettlementDomain + "|" + string(strategy) + "|" + string(lot)
}

// orderArchiver writes closed projections and backs off after a failure so a
// broken trades database is not retried on every projection poll.
type orderArchiver struct {
	retryAfter time.Time
}

// archive is offered closed projections only. It reports whether nothing
// remains to be done for od: it was written, already present, or is not
// history (never filled). It returns false when the
// order should be offered again later.
func (a *orderArchiver) archive(deps RuntimeDeps, od *ormo.InOutOrder, key string) bool {
	if od == nil || od.IOrder == nil || od.Enter == nil {
		return true
	}
	if od.Enter.Filled == 0 {
		return true
	}
	if od.Status != ormo.InOutStatusFullExit || od.Exit == nil || deps.Orders == nil {
		return false
	}
	// Projections record the close on the exit leg; history is keyed by ExitAt.
	exitAt := od.ExitAt
	if exitAt <= 0 {
		exitAt = od.Exit.UpdateAt
	}
	if exitAt <= 0 {
		return false
	}
	taskID := deps.Orders.GetTaskID(deps.DefaultAccount)
	if taskID < 0 || time.Now().Before(a.retryAfter) {
		return false
	}
	view := od.Clone()
	view.TaskID, view.ExitAt = taskID, exitAt
	// The projection carries realized profit but not its rate.
	if notional := view.Enter.Average * view.Enter.Filled; notional > 0 && view.ProfitRate == 0 {
		view.ProfitRate = view.Profit / notional
	}
	if _, err := deps.Orders.ArchiveOrder(view, key); err != nil {
		a.retryAfter = time.Now().Add(30 * time.Second)
		deps.Logger().Warn("archive closed order failed; will retry", zap.String("key", key), zap.Error(err))
		return false
	}
	return true
}
