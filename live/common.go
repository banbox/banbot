package live

import (
	"sync"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/goods"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

// refreshPairJobsWithRuntime rotates pairs inside one explicitly owned live
// runtime. Legacy callers must not enter this path.
func refreshPairJobsWithRuntime(dp data.IProvider, symbols *orm.SymbolState, deps *biz.RuntimeDeps,
	clock *btime.ClockState, exchange banexg.BanExchange, showLog, isFirst bool) *errs.Error {
	if deps == nil {
		return errs.NewMsg(core.ErrBadConfig, "live pair refresh requires explicit runtime dependencies")
	}
	if deps.Config == nil || deps.Config.View() == nil {
		return errs.NewMsg(core.ErrBadConfig, "runtime config is required")
	}
	cfg := deps.Config.View()
	pairMgr := cfg.PairMgr
	if pairMgr == nil {
		pairMgr = &config.PairMgrConfig{}
	}
	state := deps.Core
	if state == nil {
		return errs.NewMsg(core.ErrBadConfig, "runtime core state is required")
	}
	if symbols == nil {
		symbols = deps.Symbols
	}
	if symbols == nil {
		return errs.NewMsg(core.ErrBadConfig, "runtime symbol state is required")
	}
	if deps.Symbols == nil {
		deps.Symbols = symbols
	}
	if clock == nil {
		clock = deps.Clock
	}
	if clock == nil {
		return errs.NewMsg(core.ErrBadConfig, "runtime clock is required")
	}
	if err := validateCryptoTraderRuntimeDeps(deps, symbols); err != nil {
		return err
	}
	if dp == nil {
		return errs.NewMsg(core.ErrRunTime, "live provider is required")
	}
	if exchange == nil {
		return errs.NewMsg(core.ErrBadConfig, "runtime exchange is required")
	}
	curTime := clock.TimeMS()
	envReal := state.EnvReal
	var err *errs.Error
	if isFirst {
		if pairMgr.Cron != "" {
			location := deps.Config.Location()
			schedule, err := utils.NewCronSchedulerWithLocation(pairMgr.Cron, location)
			if err != nil {
				return errs.New(errs.CodeRunTime, err)
			}
			curTime = utils.CronAlign(schedule, btime.ToTime(curTime)).UnixMilli()
		} else if !envReal && !state.LiveMode && pairMgr.UseLatest && cfg.TimeRange != nil {
			curTime = min(curTime, cfg.TimeRange.EndMS)
		}
	}

	var pairs []string
	pairs, err = goods.RefreshPairListWithRuntimeDeps(&goods.RuntimeDeps{
		Core: deps.Core, Clock: deps.Clock, Config: cfg, DataDir: deps.Config.DataDir, Storage: deps.Storage,
		Symbols: symbols, Exchange: exchange, ShowLog: showLog,
	}, curTime)
	if err != nil {
		return err
	}
	allPairs := make([]string, 0, len(pairs))
	allPairs = append(allPairs, pairs...)
	for _, policy := range cfg.RunPolicy {
		allPairs = append(allPairs, policy.Pairs...)
	}
	allPairs, _ = utils.UniqueItems(allPairs)
	pairTfScores, err := strat.CalcPairTfScoresWithState(deps.Strategies, symbols, exchange, allPairs)
	if err != nil {
		return err
	}
	state.SetPairs(pairs, policyPairs(cfg.RunPolicy))
	warms, exitOrders, loadErr := strat.LoadStratJobsWithState(deps.Strategies, state, symbols, pairs, pairTfScores, deps.Orders)
	if loadErr != nil {
		return loadErr
	}
	for acc, orders := range exitOrders {
		if len(orders) == 0 {
			continue
		}
		mgr := biz.GetOdMgrWithState(deps.Trading, acc)
		if mgr == nil {
			return errs.NewMsg(core.ErrRunTime, "order manager is required for pair rotation: %s", acc)
		}
		if err := mgr.ExitAndFill(orders, &strat.ExitReq{Tag: core.ExitTagPairDel}); err != nil {
			return err
		}
		strat.FinalizePairRotation(deps.Strategies, deps.Core)
		deps.Logger().Info("exit old orders as pair rotation", zap.Int("num", len(orders)))
	}
	if showLog {
		strat.PrintStratGroupsWithState(deps.Strategies, state)
	}
	if isFirst {
		biz.InitOdSubsWithRuntimeDeps(deps)
	}
	symbols.ResetSubSymbol()
	if err := dp.SubWarmPairs(warms, true); err != nil {
		return err
	}
	return nil
}

func policyPairs(policies []*config.RunPolicyConfig) []string {
	if len(policies) == 0 {
		return nil
	}
	pairs := make([]string, 0)
	for _, policy := range policies {
		if policy != nil {
			pairs = append(pairs, policy.Pairs...)
		}
	}
	pairs, _ = utils.UniqueItems(pairs)
	return pairs
}

var backtestToCompareWithRuntime = opt.BacktestToCompareWithRuntime

func cronBacktestInLiveWithRuntime(scheduler com.Scheduler, deps biz.RuntimeDeps) {
	if scheduler == nil || deps.Config == nil {
		return
	}
	cfg := deps.Config.View()
	if cfg == nil || cfg.BTInLive == nil || cfg.BTInLive.Cron == "" {
		return
	}
	_, err := scheduler.AddFunc(cfg.BTInLive.Cron, func() {
		backtestToCompareWithRuntime(deps)
	})
	if err != nil {
		deps.Logger().Error("add runtime CronBacktestInLive fail", zap.Error(err))
	}
}

// StartLoopBalancePositionsWithRuntime ties the polling worker to an
// explicit runtime.
type balanceRuntimeDeps struct {
	exchange banexg.BanExchange
	core     *core.State
	config   *config.Config
	accounts map[string]*config.AccountConfig
	orders   *ormo.OrderState
	trading  *biz.TradingState
	interval time.Duration
}

// snapshotBalanceAccounts copies the low-frequency account configuration at
// the worker boundary. The worker may outlive a config refresh, so retaining
// either the source map or its AccountConfig pointers would reintroduce a
// concurrent map/pointer mutation race.
func snapshotBalanceAccounts(accounts map[string]*config.AccountConfig) map[string]*config.AccountConfig {
	if accounts == nil {
		return nil
	}
	snapshot := config.NewSnapshotWithDirs(&config.Config{Accounts: accounts}, "", "")
	if snapshot == nil || snapshot.View() == nil {
		return nil
	}
	return snapshot.View().Accounts
}

func bindBalanceRuntimeDeps(deps biz.RuntimeDeps) *balanceRuntimeDeps {
	bound := &balanceRuntimeDeps{
		exchange: deps.Exchange,
		core:     deps.Core,
		orders:   deps.Orders,
		trading:  deps.Trading,
		interval: time.Minute,
	}
	if bound.orders == nil {
		bound.orders = ormo.NewOrderState()
	}
	if bound.trading == nil {
		bound.trading = biz.NewTradingState()
	}
	if deps.Config != nil {
		bound.config = deps.Config.View()
	}
	if bound.config != nil {
		if bound.config.AccountPullSecs > 0 {
			bound.interval = time.Duration(bound.config.AccountPullSecs) * time.Second
		}
	}
	bound.accounts = snapshotBalanceAccounts(deps.AccountConfigs())
	return bound
}

func StartLoopBalancePositionsWithRuntime(lifecycle RuntimeLifecycle, runtimeDeps ...biz.RuntimeDeps) {
	if lifecycle == nil {
		log.Error("runtime balance worker requires a lifecycle")
		return
	}
	if len(runtimeDeps) == 0 {
		log.Error("runtime balance worker requires explicit dependencies")
		return
	}
	deps := bindBalanceRuntimeDeps(runtimeDeps[0])
	ctx := lifecycle.Context()
	if ctx != nil {
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
	for account, cfg := range deps.accounts {
		if cfg == nil || cfg.NoTrade {
			continue
		}
		updateAccBalanceWithRuntime(deps, account)
	}
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	stop := make(chan struct{})
	ticker := time.NewTicker(deps.interval)
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				updateBalancePosWithRuntime(deps)
			case <-done:
				return
			case <-stop:
				return
			}
		}
	}()
	var stopOnce sync.Once
	lifecycle.OnClose(func() {
		ticker.Stop()
		stopOnce.Do(func() { close(stop) })
	})
	lifecycle.OnCloseWait(wait.Wait)
}

func updateBalancePosWithRuntime(deps *balanceRuntimeDeps) {
	if deps == nil {
		return
	}
	logger := deps.core.Log()
	for account, cfg := range deps.accounts {
		if cfg == nil || cfg.NoTrade {
			continue
		}
		if deps.orders == nil || deps.trading == nil {
			continue
		}
		odList, lock := deps.orders.GetOpenODs(account)
		lock.Lock()
		odNum := len(odList)
		lock.Unlock()
		if odNum == 0 {
			continue
		}
		if deps.core != nil && (deps.core.Market == banexg.MarketLinear || deps.core.Market == banexg.MarketInverse) {
			// 定期同步仓位检查不匹配订单，现货不支持
			odMgr := biz.GetLiveOdMgrWithState(deps.trading, account)
			if odMgr == nil {
				continue
			}
			_, err := odMgr.SyncLocalOrders()
			if err != nil {
				logger.Error("SyncLocalOrders fail", zap.String("acc", account), zap.Error(err))
			}
		}
		updateAccBalanceWithRuntime(deps, account)
	}
}

func updateAccBalanceWithRuntime(deps *balanceRuntimeDeps, account string) {
	if deps == nil {
		log.Error("UpdateBalance requires a runtime exchange", zap.String("acc", account))
		return
	}
	logger := deps.core.Log()
	if deps.exchange == nil {
		logger.Error("UpdateBalance requires a runtime exchange", zap.String("acc", account))
		return
	}
	if deps.trading == nil {
		logger.Error("UpdateBalance requires runtime trading state", zap.String("acc", account))
		return
	}
	wallet := deps.trading.Wallet(account)
	rsp, err := deps.exchange.FetchBalance(map[string]interface{}{
		banexg.ParamAccount: account,
	})
	if err != nil {
		logger.Error("UpdateBalance fail", zap.String("acc", account), zap.Error(err))
	} else {
		biz.UpdateWalletByBalancesWithRuntime(wallet, rsp)
	}
}

func sendOrderMsg(od *ormo.InOutOrder, isEnter bool) {
	sendOrderMsgWithOrderState(od, isEnter, nil)
}

func sendOrderMsgWithOrderState(od *ormo.InOutOrder, isEnter bool, orderState *ormo.OrderState) {
	sendOrderMsgWithRuntime(od, isEnter, orderState, nil)
}

func sendOrderMsgWithRuntime(od *ormo.InOutOrder, isEnter bool, orderState *ormo.OrderState, notifications *rpc.Session) {
	msgType := rpc.MsgTypeExit
	subOd := od.Exit
	action := "Close Long"
	if od.Short {
		action = "Close Short"
	}
	if isEnter {
		msgType = rpc.MsgTypeEntry
		subOd = od.Enter
		action = "Open Long"
		if od.Short {
			action = "Open Short"
		}
	}
	if subOd == nil {
		return
	}
	filled, price := subOd.Filled, subOd.Average
	var account string
	if orderState != nil {
		account = orderState.GetTaskAcc(od.TaskID)
	} else {
		account = ormo.GetTaskAcc(od.TaskID)
	}
	if account == "" {
		log.Info("skip send rpc msg, unknown account", zap.Int64("task_id", od.TaskID), zap.String("key", od.Key()),
			zap.Int64("status", subOd.Status), zap.Float64("filled", filled))
		return
	}
	if subOd.Status != ormo.OdStatusClosed || filled == 0 {
		log.Info("skip send rpc msg", zap.String("acc", account), zap.String("key", od.Key()),
			zap.Int64("status", subOd.Status), zap.Float64("filled", filled))
		return
	}
	message := map[string]interface{}{
		"type":          msgType,
		"account":       account,
		"action":        action,
		"enter_tag":     od.EnterTag,
		"exit_tag":      od.ExitTag,
		"side":          subOd.Side,
		"short":         od.Short,
		"leverage":      od.Leverage,
		"amount":        filled,
		"price":         price,
		"value":         filled * price,
		"cost":          filled * price / od.Leverage,
		"strategy":      od.Strategy,
		"pair":          od.Symbol,
		"timeframe":     od.Timeframe,
		"profit":        od.Profit,
		"profit_rate":   od.ProfitRate,
		"max_pft_rate":  od.MaxPftRate,
		"max_draw_down": od.MaxDrawDown,
	}
	if notifications != nil {
		notifications.SendMsg(message)
	} else {
		rpc.SendMsg(message)
	}
}
