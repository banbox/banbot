package biz

import (
	"math"
	"sort"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

// CloseAccOrdersWithState closes orders through one runtime's manager map.
// It is the explicit counterpart of CloseAccOrders and never consults the
// process-wide manager registry.
func CloseAccOrdersWithState(state *TradingState, account string, orders []*ormo.InOutOrder, req *strat.ExitReq) (int, int, *errs.Error) {
	if state == nil {
		return 0, len(orders), errs.NewMsg(core.ErrRunTime, "runtime trading state is required")
	}
	manager := state.OrderManager(account)
	if manager == nil {
		return 0, len(orders), errs.NewMsg(core.ErrRunTime, "order manager is not initialized: %s", account)
	}
	closed, failed := 0, 0
	for _, order := range orders {
		if order == nil {
			continue
		}
		request := &strat.ExitReq{}
		if req != nil {
			request = req.Clone()
		}
		request.StratName = order.Strategy
		request.OrderID = order.ID
		if _, err := manager.ExitOrder(order, request); err != nil {
			failed++
			log.Error("close runtime order fail", zap.String("account", account), zap.Int64("order_id", order.ID), zap.Error(err))
			continue
		}
		closed++
	}
	if failed > 0 {
		return closed, failed, errs.NewMsg(core.ErrRunTime, "failed to close %d runtime orders", failed)
	}
	return closed, failed, nil
}

// VerifyTriggerOdsWithRuntimeDeps verifies only the trigger orders owned by
// one explicit runtime. The lock is per TradingState, so independent runtimes
// do not serialize on a package-level mutex.
func VerifyTriggerOdsWithRuntimeDeps(deps RuntimeDeps) {
	if deps.Trading == nil {
		return
	}
	deps.Trading.triggerMu.Lock()
	defer deps.Trading.triggerMu.Unlock()
	for account, cfg := range deps.AccountConfigs() {
		if cfg == nil || cfg.NoTrade {
			continue
		}
		if manager := deps.Trading.LiveManager(account); manager != nil {
			manager.verifyAccountTriggerOds()
		}
	}
}

// MakeCheckFatalStopWithRuntime builds the low-frequency loss guard for one
// explicit runtime. The order/task/account registries and clock all come from
// the supplied state; the legacy fatal-stop facade is never read.
func MakeCheckFatalStopWithRuntime(deps RuntimeDeps, fatal map[int]float64, fatalHours int, nowMS func() int64) func() {
	if fatalHours <= 0 {
		fatalHours = 8
	}
	nowMS = runtimeFatalStopClock(deps, nowMS)
	intervals := make([]int, 0, len(fatal))
	for interval := range fatal {
		if interval > 0 {
			intervals = append(intervals, interval)
		}
	}
	sort.Ints(intervals)
	return func() {
		if deps.Core == nil || deps.Orders == nil || deps.Trading == nil || nowMS == nil || len(intervals) == 0 {
			return
		}
		accounts := deps.AccountConfigs()
		for account, accountCfg := range accounts {
			if accountCfg == nil || accountCfg.NoTrade {
				continue
			}
			checkRuntimeFatalStop(deps, account, fatal, fatalHours, intervals, nowMS)
		}
	}
}

func runtimeFatalStopClock(deps RuntimeDeps, nowMS func() int64) func() int64 {
	if nowMS != nil {
		return nowMS
	}
	if deps.Clock != nil {
		return deps.Clock.TimeMS
	}
	return nil
}

func checkRuntimeFatalStop(deps RuntimeDeps, account string, fatal map[int]float64, fatalHours int,
	intervals []int, nowMS func() int64) {
	stopUntil, _ := deps.Core.NoEnterUntilFor(account)
	now := nowMS()
	if stopUntil >= now {
		return
	}
	taskID := deps.Orders.GetTaskID(account)
	if taskID < 0 {
		return
	}
	sess, conn, err := deps.Orders.Conn(false)
	if err != nil {
		log.Error("get runtime db session fail", zap.Error(err))
		return
	}
	defer conn.Close()
	orders, err := sess.GetOrders(ormo.GetOrdersArgs{TaskID: taskID, Status: 2, CloseAfter: now - int64(intervals[len(intervals)-1])*60000})
	if err != nil {
		log.Error("get runtime closed orders fail", zap.Error(err))
		return
	}
	wallet := deps.Trading.Wallet(account)
	for _, interval := range intervals {
		lossRate := calcRuntimeFatalLoss(wallet, orders, interval, now, deps.Core.StartAt)
		if lossRate < fatal[interval] {
			continue
		}
		deps.Core.SetNoEnterUntil(account, now+int64(fatalHours)*int64(60*60*1000))
		log.Error("runtime fatal stop activated", zap.String("account", account), zap.Int("minutes", interval), zap.Float64("loss_rate", lossRate))
		return
	}
}

func calcRuntimeFatalLoss(wallet *BanWallets, orders []*ormo.InOutOrder, interval int, nowMS, startMS int64) float64 {
	minMS := nowMS - int64(interval)*60000
	if startMS > 0 && minMS > startMS {
		minMS = startMS
	}
	profit := 0.0
	for i := len(orders) - 1; i >= 0; i-- {
		if orders[i].RealEnterMS() < minMS {
			break
		}
		profit += orders[i].Profit
	}
	if profit >= 0 || wallet == nil {
		return 0
	}
	loss := math.Abs(profit)
	legal := wallet.TotalLegal(nil, false)
	return loss / (loss + legal)
}
