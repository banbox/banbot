package live

import (
	"sort"
	"strings"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
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
	if !closeExecutionManaged(deps) {
		return nil, errs.NewMsg(core.ErrBadConfig, "closing orders requires a bound execution account (embedded or Hub); the legacy live order manager was removed")
	}
	if request.Exchange {
		return nil, errs.NewMsg(core.ErrBadConfig, "execution close requires attributed strategy orders; raw venue position closing is not supported")
	}
	accounts := deps.AccountConfigs()
	if len(accounts) == 0 {
		return nil, errs.NewMsg(core.ErrBadConfig, "runtime accounts are required")
	}
	selected, err := closeSelectedAccounts(accounts, closeFilter(request.Accounts))
	if err != nil {
		return nil, err
	}
	if !closeAccountsMatchOwner(selected, closeOwnerAccount(deps)) {
		return nil, errs.NewMsg(core.ErrBadConfig, "close account does not match the bound execution owner")
	}
	if deps.Orders == nil || deps.Trading == nil {
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

func closeLocalOrdersWithRuntimeDeps(deps biz.RuntimeDeps, account string, pairs, strategies map[string]bool) (int, int, *errs.Error) {
	// Closing goes only through the execution-backed manager's attributed
	// orders (biz.IOrderMgr.ExitOrder); never through raw venue calls.
	switch deps.Trading.OrderManager(account).(type) {
	case *biz.SharedOrderMgr, *biz.HubOrderMgr:
	default:
		return 0, 0, errs.NewMsg(core.ErrRunTime, "execution order manager is not initialized: %s", account)
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

// closeExecutionManaged reports whether orders are owned by an execution
// account: embedded (SharedExecution) or reached through a Hub bridge
// (embedded HubOrderMgr, or a remote Hub robot).
func closeExecutionManaged(deps biz.RuntimeDeps) bool {
	return deps.SharedExecution != nil || deps.HubOrderBridge != nil
}

func closeOwnerAccount(deps biz.RuntimeDeps) string {
	if deps.SharedExecution != nil {
		return deps.SharedExecution.AccountKey().Account
	}
	return deps.HubOrderBridge.Account.Account
}

// closeAccountsMatchOwner reports whether every selected account is the one
// bound to the execution owner. The check is against the owner only: the
// runtime default account is unrelated to which account the owner may close.
func closeAccountsMatchOwner(selected []string, owner string) bool {
	for _, account := range selected {
		if account != owner {
			return false
		}
	}
	return true
}
