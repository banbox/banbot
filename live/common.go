package live

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
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

func legacyScheduler(scheduler com.Scheduler) com.Scheduler {
	if scheduler == nil {
		return com.Cron()
	}
	return scheduler
}

func CronRefreshPairs(dp data.IProvider, afterRefresh ...func() error) {
	CronRefreshPairsWithSymbolState(dp, nil, afterRefresh...)
}

// CronRefreshPairsWithSymbolState schedules pair refreshes against the
// supplied runtime symbol state. A nil state keeps the legacy facade.
func CronRefreshPairsWithSymbolState(dp data.IProvider, symbols *orm.SymbolState, afterRefresh ...func() error) {
	cronRefreshPairs(legacyScheduler(nil), dp, symbols, afterRefresh...)
}

// refreshPairJobsWithRuntime performs the parts of pair rotation that can be
// composed from explicit dependencies. Strategy/order registries are still
// compatibility globals, so callers must keep the legacy session gate around
// this operation (the official live entry already does so).
func refreshPairJobsWithRuntime(dp data.IProvider, symbols *orm.SymbolState, deps *biz.RuntimeDeps,
	clock *btime.ClockState, exchange banexg.BanExchange, showLog, isFirst bool) *errs.Error {
	var cfg *config.Config
	if deps != nil {
		if deps.Config == nil || deps.Config.View() == nil {
			return errs.NewMsg(core.ErrBadConfig, "runtime config is required")
		}
		cfg = deps.Config.View()
	} else {
		cfg = &config.Data
	}
	pairMgr := cfg.PairMgr
	if pairMgr == nil {
		pairMgr = &config.PairMgrConfig{}
	}
	var state *core.State
	if deps != nil {
		state = deps.Core
	}
	if deps != nil {
		if symbols == nil {
			symbols = deps.Symbols
		}
		if deps.Symbols == nil {
			deps.Symbols = symbols
		}
		if clock == nil {
			clock = deps.Clock
		}
		if err := validateCryptoTraderRuntimeDeps(deps, symbols); err != nil {
			return err
		}
	}
	if dp == nil {
		return errs.NewMsg(core.ErrRunTime, "live provider is required")
	}
	if deps != nil && exchange == nil {
		return errs.NewMsg(core.ErrBadConfig, "runtime exchange is required")
	}
	if exchange == nil {
		exchange = exg.Default
	}
	var curTime int64
	var envReal bool
	if clock != nil {
		curTime = clock.TimeMS()
	} else {
		curTime = btime.TimeMS()
	}
	if state != nil {
		envReal = state.EnvReal
	} else {
		envReal = core.EnvReal
	}
	var err *errs.Error
	if isFirst {
		if pairMgr.Cron != "" {
			location := (*time.Location)(nil)
			if deps != nil && deps.Config != nil {
				location = deps.Config.Location()
			}
			schedule, err := utils.NewCronSchedulerWithLocation(pairMgr.Cron, location)
			if err != nil {
				return errs.New(errs.CodeRunTime, err)
			}
			curTime = utils.CronAlign(schedule, btime.ToTime(curTime)).UnixMilli()
		} else if !envReal && !stateIsLive(state) && pairMgr.UseLatest && cfg.TimeRange != nil {
			curTime = min(curTime, cfg.TimeRange.EndMS)
		}
	}

	var pairs []string
	if deps != nil {
		dataDir := ""
		if deps.Config != nil {
			dataDir = deps.Config.DataDir
		}
		pairs, err = goods.RefreshPairListWithRuntimeDeps(&goods.RuntimeDeps{
			Core: deps.Core, Clock: deps.Clock, Config: cfg, DataDir: dataDir, Storage: deps.Storage,
			Symbols: symbols, Exchange: exchange, ShowLog: showLog,
		}, curTime)
	} else {
		// The legacy implementation still updates package-level pair state. Keep
		// that compatibility mutation scoped to the legacy operation only.
		oldPairs, oldPairsMap, oldShowLog := core.Pairs, core.PairsMap, goods.ShowLog
		defer func() {
			core.Pairs, core.PairsMap, goods.ShowLog = oldPairs, oldPairsMap, oldShowLog
		}()
		goods.ShowLog = showLog
		pairs, err = goods.RefreshPairListWithSymbolState(symbols, exchange, curTime)
	}
	if err != nil {
		return err
	}
	allPairs := make([]string, 0, len(pairs))
	allPairs = append(allPairs, pairs...)
	for _, policy := range cfg.RunPolicy {
		allPairs = append(allPairs, policy.Pairs...)
	}
	allPairs, _ = utils.UniqueItems(allPairs)
	var scoreState *strat.State
	if deps != nil {
		scoreState = deps.Strategies
	}
	pairTfScores, err := strat.CalcPairTfScoresWithState(scoreState, symbols, exchange, allPairs)
	if err != nil {
		return err
	}
	if state != nil {
		state.SetPairs(pairs, policyPairs(cfg.RunPolicy))
	}
	var warms strat.Warms
	var exitOrders map[string][]*ormo.InOutOrder
	if deps != nil {
		var loadErr *errs.Error
		warms, exitOrders, loadErr = strat.LoadStratJobsWithState(deps.Strategies, state, symbols, pairs, pairTfScores, deps.Orders)
		if loadErr != nil {
			return loadErr
		}
	} else {
		warms, exitOrders, err = strat.LoadStratJobsWithRuntimeState(state, symbols, pairs, pairTfScores)
	}
	if err != nil {
		return err
	}
	for acc, orders := range exitOrders {
		if len(orders) == 0 {
			continue
		}
		var mgr biz.IOrderMgr
		if deps != nil {
			mgr = biz.GetOdMgrWithState(deps.Trading, acc)
		} else {
			mgr = biz.GetOdMgr(acc)
		}
		if mgr == nil {
			return errs.NewMsg(core.ErrRunTime, "order manager is required for pair rotation: %s", acc)
		}
		if err := mgr.ExitAndFill(orders, &strat.ExitReq{Tag: core.ExitTagPairDel}); err != nil {
			return err
		}
		if deps != nil {
			strat.FinalizePairRotation(deps.Strategies, deps.Core)
		} else {
			strat.FinalizePairRotation(nil)
		}
		log.Info("exit old orders as pair rotation", zap.Int("num", len(orders)))
	}
	if showLog {
		if deps != nil {
			strat.PrintStratGroupsWithState(deps.Strategies, state)
		} else {
			strat.PrintStratGroups()
		}
	}
	if isFirst {
		biz.InitOdSubsWithRuntimeDeps(deps)
	}
	if symbols == nil {
		orm.ResetSubSymbol()
	} else {
		symbols.ResetSubSymbol()
	}
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

func stateIsLive(state *core.State) bool {
	if state != nil {
		return state.LiveMode
	}
	return core.LiveMode
}

func syncRuntimeOrderMatchState(state *core.State) {
	if state == nil {
		return
	}
	core.LockOdMatch.RLock()
	state.LockOdMatch.Lock()
	state.OrderMatchTfs = make(map[string]bool, len(core.OrderMatchTfs))
	for tf, enabled := range core.OrderMatchTfs {
		state.OrderMatchTfs[tf] = enabled
	}
	state.LockOdMatch.Unlock()
	core.LockOdMatch.RUnlock()
}

func cronRefreshPairs(scheduler com.Scheduler, dp data.IProvider, symbols *orm.SymbolState, afterRefresh ...func() error) {
	if scheduler == nil {
		return
	}
	if config.PairMgr.Cron != "" {
		lastRefreshMS := btime.TimeMS()
		_, err_ := scheduler.AddFunc(config.PairMgr.Cron, func() {
			curMS := btime.TimeMS()
			if curMS-lastRefreshMS < config.MinPairCronGapMS {
				return
			}
			lastRefreshMS = curMS
			err := opt.RefreshPairJobsWithSymbolState(dp, symbols, true, false, nil)
			if err != nil {
				log.Error("RefreshPairJobs fail", zap.Error(err))
				return
			}
			if len(afterRefresh) > 0 && afterRefresh[0] != nil {
				if err := afterRefresh[0](); err != nil {
					log.Error("RefreshPairJobs post-refresh fail", zap.Error(err))
				}
			}
		})
		if err_ != nil {
			log.Error("add RefreshPairList fail", zap.Error(err_))
		}
	}
}

func FetchHourKlines(dp *data.LiveProvider) {
	FetchHourKlinesWithSymbolState(dp, nil)
}

// FetchHourKlinesWithSymbolState keeps the periodic higher-timeframe fetch
// on the same symbol subscription state as the live provider.
func FetchHourKlinesWithSymbolState(dp *data.LiveProvider, symbols *orm.SymbolState) {
	fetchHourKlines(legacyScheduler(nil), dp, symbols)
}

func fetchHourKlines(scheduler com.Scheduler, dp *data.LiveProvider, symbols *orm.SymbolState) {
	if scheduler == nil {
		return
	}
	endMap := make(map[int32]int64)
	_, err := scheduler.AddFunc("0 0 * * * *", func() {
		exsList := make(map[int32]*orm.ExSymbol)
		if symbols == nil {
			exsList = orm.GetHourOnlySymbols()
		} else {
			exsList = symbols.GetHourOnlySymbols()
		}
		if len(exsList) == 0 {
			return
		}
		log.Info("FetchHourKlines", zap.Int("num", len(exsList)))
		for sid := range exsList {
			if _, ok := endMap[sid]; !ok {
				endMap[sid] = 0
			}
		}
		for sid := range endMap {
			if _, ok := exsList[sid]; !ok {
				delete(endMap, sid)
			}
		}
		data.DownEmitHourKlinesWithSymbolState(dp, symbols, endMap)
	})
	if err != nil {
		log.Error("add FetchHourKlines fail", zap.Error(err))
	}
}

func CronLoadMarkets() {
	cronLoadMarkets(legacyScheduler(nil))
}

func cronLoadMarkets(scheduler com.Scheduler) {
	if scheduler == nil {
		return
	}
	// 2小时更新一次市场行情
	_, err := scheduler.AddFunc("30 3 */2 * * *", func() {
		_, _ = orm.LoadMarkets(exg.Default, true)
	})
	if err != nil {
		log.Error("add CronLoadMarkets fail", zap.Error(err))
	}
}

func CronFatalLossCheck() {
	cronFatalLossCheck(legacyScheduler(nil))
}

func cronFatalLossCheck(scheduler com.Scheduler) {
	if scheduler == nil {
		return
	}
	checkIntvs := utils.KeysOfMap(config.FatalStop)
	if len(checkIntvs) == 0 {
		return
	}
	minIntv := slices.Min(checkIntvs)
	if minIntv < 1 {
		log.Error("fatal_stop invalid, min is 1, skip", zap.Int("current", minIntv))
		return
	}
	cronStr := fmt.Sprintf("35 */%v * * * *", min(5, minIntv))
	maxIntv := slices.Max(checkIntvs)
	_, err := scheduler.AddFunc(cronStr, biz.MakeCheckFatalStop(maxIntv))
	if err != nil {
		log.Error("add CronFatalLossCheck fail", zap.Error(err))
	}
}

func CronKlineDelays(dp *data.LiveProvider) {
	cronKlineDelays(legacyScheduler(nil), dp, com.LegacyPairCopiedState(), btime.TimeMS)
}

// cronKlineDelays is the runtime-owned implementation. The state and clock
// are explicit so a live Runtime never observes another Runtime's progress.
func cronKlineDelays(scheduler com.Scheduler, dp *data.LiveProvider, copied *com.PairCopiedState, clock func() int64) {
	if scheduler == nil || dp == nil || copied == nil {
		return
	}
	if clock == nil {
		clock = btime.TimeMS
	}
	lastNotifyDelay := int64(0)
	logDelay := func(msgText string) {
		curMS := clock()
		log.Warn(msgText)
		if curMS-lastNotifyDelay > 600000 {
			// Delay reminders are sent every 10 minutes
			// 10 分钟发送一次延迟提醒
			lastNotifyDelay = curMS
			rpc.SendMsg(map[string]interface{}{
				"type":   rpc.MsgTypeException,
				"status": msgText,
			})
		}
	}
	stuckCount := 0
	_, err_ := scheduler.AddFunc("30 * * * * *", func() {
		jobs := dp.GetJobs("ohlcv")
		if len(jobs) == 0 {
			return
		}
		curMS := clock()
		delaySecs := int((curMS - copied.LastCopiedMs()) / 1000)
		if delaySecs > 120 {
			// It should be received every minute, alert if haven't been received for more than 2 minutes
			// 应该每分钟都能收到，超过2分钟未收到爬虫推送报警
			logDelay("Listen to the spider kline timeout!")
			stuckCount += 1
			if stuckCount > config.CloseOnStuck {
				// 超时未收到K线，全部平仓
				for account, cfg := range config.Accounts {
					if cfg.NoTrade {
						continue
					}
					openOds, lock := ormo.GetOpenODs(account)
					lock.Lock()
					var odList = utils.ValsOfMap(openOds)
					lock.Unlock()
					if len(odList) > 0 {
						closeNum, failNum, err := biz.CloseAccOrders(account, odList, &strat.ExitReq{
							Tag:   core.ExitTagDataStuck,
							Force: true,
						})
						if err != nil {
							log.Error("close orders on stuck fail", zap.String("acc", account),
								zap.Int("success", closeNum), zap.Int("fail", failNum), zap.Error(err))
						} else {
							log.Warn(fmt.Sprintf("close orders on stuck: %s, %d closed, %d failed",
								account, closeNum, failNum))
						}
					}
				}
				// 防止频繁检查
				stuckCount = 0
			}
			return
		}
		stuckCount = 0
		var fails = make(map[string][]string)
		pairWaits := copied.GetPairCopieds()
		for pair, wait := range pairWaits {
			if wait[0]+wait[1]*2 > curMS {
				continue
			}
			timeoutMin := strconv.Itoa(int((curMS-wait[0])/60000)) + "mins"
			arr, _ := fails[timeoutMin]
			fails[timeoutMin] = append(arr, pair)
		}
		if len(fails) > 0 {
			failText := core.GroupByPairQuotes(fails, false)
			logDelay("Listen to the spider kline timeout:" + failText)
		}
	})
	if err_ != nil {
		log.Error("add Monitor Klines fail", zap.Error(err_))
	}
}

func CronKlineSummary() {
	cronKlineSummary(legacyScheduler(nil))
}

func cronKlineSummary(scheduler com.Scheduler) {
	if scheduler == nil {
		return
	}
	_, err_ := scheduler.AddFunc("30 1-59/10 * * * *", func() {
		core.TfPairHitsLock.Lock()
		var pairGroups = make(map[string][]string)
		for tf, tfMap := range core.TfPairHits {
			hitMap := make(map[int][]string)
			for pair, num := range tfMap {
				arr, _ := hitMap[num]
				hitMap[num] = append(arr, pair)
			}
			for num, arr := range hitMap {
				arrLen := len(arr)
				pairGroups[fmt.Sprintf("%s_%v: %v", tf, num, arrLen)] = arr
			}
			core.TfPairHits[tf] = make(map[string]int)
		}
		core.TfPairHitsLock.Unlock()
		if len(pairGroups) > 0 {
			staText := core.GroupByPairQuotes(pairGroups, true)
			log.Info(fmt.Sprintf("receive bars in 10 mins:\n%s", staText))
		}
	})
	if err_ != nil {
		log.Error("add Receive Klines Summary fail", zap.Error(err_))
	}
}

func CronDumpStratOutputs() {
	cronDumpStratOutputs(legacyScheduler(nil))
}

func cronDumpStratOutputs(scheduler com.Scheduler) {
	if scheduler == nil {
		return
	}
	_, err_ := scheduler.AddFunc("31 * * * * *", func() {
		groups := make(map[string][]string)
		for _, items := range strat.PairStrats {
			for _, stgy := range items {
				if len(stgy.Outputs) == 0 {
					continue
				}
				rows, _ := groups[stgy.Name]
				groups[stgy.Name] = append(rows, stgy.Outputs...)
				stgy.Outputs = nil
			}
		}
		for name, lines := range groups {
			name = strings.ReplaceAll(name, ":", "_")
			fname := fmt.Sprintf("%s_%s.log", config.Name, name)
			outPath := filepath.Join(config.GetLogsDir(), fname)
			file, err := os.OpenFile(outPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Error("create strategy output file fail", zap.String("name", name), zap.Error(err))
				continue
			}
			_, err = file.WriteString(strings.Join(lines, "\n"))
			if err != nil {
				log.Error("write strategy output fail", zap.String("name", name), zap.Error(err))
			}
			_, _ = file.WriteString("\n")
			err = file.Close()
			if err != nil {
				log.Error("close strategy output fail", zap.String("name", name), zap.Error(err))
			}
		}
	})
	if err_ != nil {
		log.Error("add CronDumpStratOutputs fail", zap.Error(err_))
	}
}

func CronCheckTriggerOds() {
	cronCheckTriggerOds(legacyScheduler(nil))
}

func cronCheckTriggerOds(scheduler com.Scheduler) {
	if scheduler == nil {
		return
	}
	// Check every minute 15 seconds to see if the limit order submission is triggered
	// 在每分钟的15s检查是否触发限价单提交
	_, err_ := scheduler.AddFunc("15,45 * * * * *", biz.VerifyTriggerOds)
	if err_ != nil {
		log.Error("add VerifyTriggerOds fail", zap.Error(err_))
	}
}

func CronBacktestInLive() {
	cronBacktestInLive(legacyScheduler(nil))
}

var backtestToCompareWithRuntime = opt.BacktestToCompareWithRuntime

func cronBacktestInLive(scheduler com.Scheduler) {
	if scheduler == nil {
		return
	}
	if config.BTInLive != nil && config.BTInLive.Cron != "" {
		_, err := scheduler.AddFunc(config.BTInLive.Cron, opt.BacktestToCompare)
		if err != nil {
			log.Error("add CronBacktestInLive fail", zap.Error(err))
		}
	}
}

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
		log.Error("add runtime CronBacktestInLive fail", zap.Error(err))
	}
}

func StartLoopBalancePositions() {
	for account, cfg := range config.Accounts {
		if cfg.NoTrade {
			continue
		}
		updateAccBalance(account)
	}
	go func() {
		ticker := time.NewTicker(time.Duration(config.AccountPullSecs) * time.Second)
		core.ExitCalls = append(core.ExitCalls, ticker.Stop)
		for {
			select {
			case <-ticker.C:
				updateBalancePos()
			}
		}
	}()
}

// StartLoopBalancePositionsWithRuntime ties the polling worker to an
// explicit runtime. The zero-argument wrapper above retains legacy globals.
type balanceRuntimeDeps struct {
	exchange banexg.BanExchange
	core     *core.State
	config   *config.Config
	accounts map[string]*config.AccountConfig
	orders   *ormo.OrderState
	trading  *biz.TradingState
	interval time.Duration
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
		bound.accounts = bound.config.Accounts
		if bound.config.AccountPullSecs > 0 {
			bound.interval = time.Duration(bound.config.AccountPullSecs) * time.Second
		}
	}
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
				log.Error("SyncLocalOrders fail", zap.String("acc", account), zap.Error(err))
			}
		}
		updateAccBalanceWithRuntime(deps, account)
	}
}

func updateBalancePos() {
	for account, cfg := range config.Accounts {
		if cfg == nil || cfg.NoTrade {
			continue
		}
		odList, lock := ormo.GetOpenODs(account)
		lock.Lock()
		odNum := len(odList)
		lock.Unlock()
		if odNum == 0 {
			continue
		}
		if core.Market == banexg.MarketLinear || core.Market == banexg.MarketInverse {
			odMgr := biz.GetLiveOdMgr(account)
			_, err := odMgr.SyncLocalOrders()
			if err != nil {
				log.Error("SyncLocalOrders fail", zap.String("acc", account), zap.Error(err))
			}
		}
		updateAccBalance(account)
	}
}

func updateAccBalance(account string) {
	updateAccBalanceWithRuntime(&balanceRuntimeDeps{exchange: exg.Default}, account)
}

func updateAccBalanceWithRuntime(deps *balanceRuntimeDeps, account string) {
	if deps == nil || deps.exchange == nil {
		log.Error("UpdateBalance requires a runtime exchange", zap.String("acc", account))
		return
	}
	if deps.trading == nil {
		log.Error("UpdateBalance requires runtime trading state", zap.String("acc", account))
		return
	}
	wallet := deps.trading.Wallet(account)
	rsp, err := deps.exchange.FetchBalance(map[string]interface{}{
		banexg.ParamAccount: account,
	})
	if err != nil {
		log.Error("UpdateBalance fail", zap.String("acc", account), zap.Error(err))
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
