package biz

import (
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
