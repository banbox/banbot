package live

import (
	"fmt"
	"sort"
	"strings"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"go.uber.org/zap"
)

type TradeCloseRequest struct {
	Accounts, Pairs, Strategies []string
	Exchange, Confirmed         bool
}

type TradeCloseResult struct{ Closed, Failed int }

// CloseOrdersWithRuntimeDeps uses only the supplied runtime. It never falls
// back to process-global configuration, exchange, or order-manager state.
func CloseOrdersWithRuntimeDeps(deps biz.RuntimeDeps, request TradeCloseRequest) (*TradeCloseResult, *errs.Error) {
	if deps.Core == nil || deps.Clock == nil || deps.ConfigView() == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "runtime core, clock, and config are required")
	}
	if deps.Exchange == nil {
		return nil, errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required")
	}
	accounts := deps.AccountConfigs()
	if len(accounts) == 0 {
		return nil, errs.NewMsg(core.ErrBadConfig, "runtime accounts are required")
	}
	selected, err := closeSelectedAccounts(accounts, closeFilter(request.Accounts))
	if err != nil {
		return nil, err
	}
	if !request.Exchange && (deps.Orders == nil || deps.Trading == nil) {
		return nil, errs.NewMsg(core.ErrBadConfig, "runtime orders and trading state are required")
	}
	pairs, strategies := closeFilter(request.Pairs), closeFilter(request.Strategies)
	service, result := biz.NewRemoteCommandServiceWithRuntimeDeps(deps), &TradeCloseResult{}
	for _, account := range selected {
		account := account
		commandResult, commandErr := service.Run(biz.RemoteCommand{
			Source: biz.RemoteSourceCLI, Actor: "cli", Account: account, Action: biz.RemoteActionCloseOrder,
			All: true, Confirmed: request.Confirmed, ExitTag: core.ExitTagCli,
			ExecClose: func() (int, int, *errs.Error) {
				if err := cancelPendingOrdersWithRuntimeDeps(deps, account, pairs); err != nil {
					return 0, 0, err
				}
				if request.Exchange {
					closed, err := closePositionsWithRuntimeDeps(deps, account, pairs)
					return closed, 0, err
				}
				return closeLocalOrdersWithRuntimeDeps(deps, account, pairs, strategies)
			},
		})
		if commandResult != nil {
			result.Closed, result.Failed = result.Closed+commandResult.CloseNum, result.Failed+commandResult.FailNum
		}
		if commandErr != nil {
			return result, commandErr
		}
	}
	return result, nil
}

func closeFilter(values []string) map[string]bool {
	result := make(map[string]bool)
	for _, value := range values {
		for _, item := range strings.Split(value, ",") {
			if item = strings.TrimSpace(item); item != "" {
				result[item] = true
			}
		}
	}
	return result
}

func closeSelectedAccounts(accounts map[string]*config.AccountConfig, filter map[string]bool) ([]string, *errs.Error) {
	for account := range filter {
		if cfg := accounts[account]; cfg == nil {
			return nil, errs.NewMsg(errs.CodeParamInvalid, "account invalid: %s", account)
		}
	}
	result := make([]string, 0, len(accounts))
	for account, cfg := range accounts {
		if cfg != nil && !cfg.NoTrade && (len(filter) == 0 || filter[account]) {
			result = append(result, account)
		}
	}
	sort.Strings(result)
	return result, nil
}

func cancelPendingOrdersWithRuntimeDeps(deps biz.RuntimeDeps, account string, pairs map[string]bool) *errs.Error {
	params := map[string]interface{}{banexg.ParamAccount: account, banexg.ParamSettleCoins: deps.ConfigView().StakeCurrency}
	orders, err := deps.Exchange.FetchOpenOrders("", 0, 1000, params)
	if err != nil {
		return err
	}
	algoParams := map[string]interface{}{banexg.ParamAccount: account, banexg.ParamAlgoOrder: true, banexg.ParamSettleCoins: deps.ConfigView().StakeCurrency}
	if algo, algoErr := deps.Exchange.FetchOpenOrders("", 0, 1000, algoParams); algoErr != nil {
		deps.Logger().Error("fetch open algo orders fail", zap.Error(algoErr))
	} else {
		orders = append(orders, algo...)
	}
	for _, order := range orders {
		if order == nil || (len(pairs) > 0 && !pairs[order.Symbol]) || (order.Status != "open" && order.Status != "partially_filled") {
			continue
		}
		_, cancelErr := deps.Exchange.CancelOrder(order.ID, order.Symbol, map[string]interface{}{banexg.ParamAccount: account})
		if cancelErr != nil && cancelErr.Code != errs.CodeOrderNotFound && cancelErr.Code != errs.CodeOrderNotCancelable {
			deps.Logger().Error("cancel order fail", zap.String("account", account), zap.String("order_id", order.ID), zap.Error(cancelErr))
		}
	}
	return nil
}

func closePositionsWithRuntimeDeps(deps biz.RuntimeDeps, account string, pairs map[string]bool) (int, *errs.Error) {
	positions, err := deps.Exchange.FetchAccountPositions(nil, map[string]interface{}{banexg.ParamAccount: account, banexg.ParamSettleCoins: deps.ConfigView().StakeCurrency})
	if err != nil {
		return 0, err
	}
	closed := 0
	for _, position := range positions {
		if position == nil || (len(pairs) > 0 && !pairs[position.Symbol]) {
			continue
		}
		side := banexg.OdSideSell
		params := map[string]interface{}{banexg.ParamAccount: account, banexg.ParamClientOrderId: fmt.Sprintf("bancli_%d_%d", deps.Clock.TimeMS(), closed)}
		if deps.Core.IsContract {
			params[banexg.ParamPositionSide] = "LONG"
			if position.Side == banexg.PosSideShort {
				params[banexg.ParamPositionSide], side = "SHORT", banexg.OdSideBuy
			}
		}
		if _, err := deps.Exchange.CreateOrder(position.Symbol, banexg.OdTypeMarket, side, position.Contracts, 0, params); err != nil {
			return closed, err
		}
		closed++
	}
	return closed, nil
}

func closeLocalOrdersWithRuntimeDeps(deps biz.RuntimeDeps, account string, pairs, strategies map[string]bool) (int, int, *errs.Error) {
	biz.InitLiveOrderMgrWithRuntimeDeps(deps, nil)
	manager := deps.Trading.LiveManager(account)
	if manager == nil {
		return 0, 0, errs.NewMsg(core.ErrRunTime, "live order manager is not initialized: %s", account)
	}
	if _, _, _, err := manager.SyncExgOrders(); err != nil {
		return 0, 0, err
	}
	openOrders, lock := deps.Orders.GetOpenODs(account)
	lock.Lock()
	matched := make([]*ormo.InOutOrder, 0, len(openOrders))
	for _, order := range openOrders {
		if order != nil && (len(pairs) == 0 || pairs[order.Symbol]) && (len(strategies) == 0 || strategies[order.Strategy]) {
			matched = append(matched, order)
		}
	}
	lock.Unlock()
	return biz.CloseAccOrdersWithState(deps.Trading, account, matched, &strat.ExitReq{Tag: core.ExitTagCli, Force: true})
}
