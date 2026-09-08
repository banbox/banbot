package opt

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	banutils "github.com/banbox/banexg/utils"
	"github.com/banbox/cron/v3"
	"go.uber.org/zap"
)

const (
	ShowNum = 600
)

type BackTestLite struct {
	biz.Trader
	*BTResult
	dp           *data.HistProvider
	symbols      *orm.SymbolState
	isOpt        bool // whether is hyper optimization
	runErr       *errs.Error
	stoppedEarly bool
}

type BackTest struct {
	*BackTestLite
	lastDumpMs           int64 // The last time the backtest status was saved 上一次保存回测状态的时间
	PBar                 *utils.StagedPrg
	dataPrep             bool
	dataPrepErr          *errs.Error
	nextRefresh          int64 // The time of the next refresh of the trading pair 下一次刷新交易对的时间
	schedule             cron.Schedule
	seriesRuntime        *data.SeriesRuntime
	loopMainFn           func() *errs.Error
	historicalCloseMS    []int64
	historicalCloseIndex int
}

/*
NewBackTestLite 创建一个临时内部回测，仅用于寻找回测未平仓订单来接力
Create a temporary internal backtest, solely for the purpose of finding backtest open orders to relay.
*/
func NewBackTestLite(isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pBar *utils.StagedPrg) *BackTestLite {
	return newBackTestLite(biz.NewTrader(strat.NewBatchState()), nil, isOpt, onBar, getEnd, pBar, nil)
}

// NewBackTestLiteWithBatchState lets a composition root make the batch queue
// part of the same runtime as the backtest. The session must remain active for
// the returned runner's lifetime because setup still uses legacy globals.
func NewBackTestLiteWithBatchState(session LegacySession, batchState *strat.BatchState, isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pBar *utils.StagedPrg) *BackTestLite {
	session.require()
	return newBackTestLite(biz.NewTrader(batchState), nil, isOpt, onBar, getEnd, pBar, nil)
}

// NewBackTestLiteWithBatchAndSymbolState binds both mutable runtime state
// objects used by a backtest. A nil symbol state preserves the legacy facade;
// the session must remain active for the returned runner's lifetime.
func NewBackTestLiteWithBatchAndSymbolState(session LegacySession, batchState *strat.BatchState, symbols *orm.SymbolState, isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pBar *utils.StagedPrg) *BackTestLite {
	session.require()
	if batchState == nil {
		batchState = strat.NewBatchState()
	}
	return newBackTestLite(biz.NewTrader(batchState), symbols, isOpt, onBar, getEnd, pBar, nil)
}

// NewBackTestLiteWithRuntimeDeps binds the typed runtime state used by the
// runner hot path. Strategy jobs, wallets, and orders remain legacy globals;
// the session must remain active for the returned runner's lifetime.
func NewBackTestLiteWithRuntimeDeps(session LegacySession, deps biz.RuntimeDeps, symbols *orm.SymbolState, isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pBar *utils.StagedPrg) *BackTestLite {
	session.require()
	trader := biz.NewTraderWithRuntimeDeps(deps)
	return newBackTestLite(trader, symbols, isOpt, onBar, getEnd, pBar,
		legacyDataRuntimeDeps(trader.RuntimeDependencies(), symbols))
}

// NewBackTestLiteWithRuntimeDataDeps is the explicit composition-root entry
// point. The older constructor remains compatible by snapshotting the legacy
// config/exchange facade at its boundary; the session must remain active for
// the returned runner's lifetime.
func NewBackTestLiteWithRuntimeDataDeps(session LegacySession, deps biz.RuntimeDeps, symbols *orm.SymbolState, isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pBar *utils.StagedPrg, dataDeps *data.RuntimeDeps) *BackTestLite {
	session.require()
	trader := biz.NewTraderWithRuntimeDeps(deps)
	return newBackTestLite(trader, symbols, isOpt, onBar, getEnd, pBar,
		dataDeps)
}

func legacyDataRuntimeDeps(deps *biz.RuntimeDeps, symbols *orm.SymbolState) *data.RuntimeDeps {
	if deps != nil {
		return bindDataRuntimeDeps(deps, symbols, &data.RuntimeDeps{
			Config:   deps.Config,
			Exchange: deps.Exchange,
			Symbols:  deps.Symbols,
		})
	}
	return bindDataRuntimeDeps(deps, symbols, &data.RuntimeDeps{
		Config:   config.NewSnapshot(&config.Data),
		Exchange: exg.Default,
	})
}

func bindDataRuntimeDeps(traderDeps *biz.RuntimeDeps, symbols *orm.SymbolState, supplied *data.RuntimeDeps) *data.RuntimeDeps {
	bound := &data.RuntimeDeps{}
	if supplied != nil {
		*bound = *supplied
	}
	if traderDeps != nil {
		if bound.Core == nil {
			bound.Core = traderDeps.Core
		}
		if bound.Clock == nil {
			bound.Clock = traderDeps.Clock
		}
		if bound.Market == nil {
			bound.Market = traderDeps.Market
		}
	}
	if bound.Symbols == nil {
		bound.Symbols = symbols
	}
	if bound.ExchangeName == "" && bound.Core != nil {
		bound.ExchangeName = bound.Core.ExgName
	}
	if bound.MarketType == "" && bound.Core != nil {
		bound.MarketType = bound.Core.Market
	}
	if bound.Exchange != nil && (bound.ExchangeName == "" || bound.MarketType == "") {
		if info := bound.Exchange.Info(); info != nil {
			if bound.ExchangeName == "" {
				bound.ExchangeName = info.ID
			}
			if bound.MarketType == "" {
				bound.MarketType = info.MarketType
			}
		}
	}
	return bound
}

func newBackTestLite(trader biz.Trader, symbols *orm.SymbolState, isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pBar *utils.StagedPrg, dataDeps *data.RuntimeDeps) *BackTestLite {
	if deps := trader.RuntimeDependencies(); deps != nil {
		dataDeps = bindDataRuntimeDeps(deps, symbols, dataDeps)
		symbols = dataDeps.Symbols
	}
	b := &BackTestLite{
		Trader:   trader,
		BTResult: NewBTResult(),
		symbols:  symbols,
		isOpt:    isOpt,
	}
	b.BTResult.runtimeDeps = b.RuntimeDependencies()
	var wallets *biz.BanWallets
	if deps := b.RuntimeDependencies(); deps != nil {
		wallets = biz.InitFakeWalletsWithRuntimeDeps(*deps)
	} else {
		biz.InitFakeWallets()
		wallets = biz.GetWallets(config.DefAcc)
	}
	b.TotalInvest = wallets.TotalLegal(nil, false)
	if onBar == nil {
		onBar = func(evt *orm.DataSeries) {
			b.FeedDataSeries(evt)
		}
	}
	if b.RuntimeDependencies() == nil {
		b.dp = data.NewHistProviderWithSymbolState(symbols, onBar, b.OnEnvEnd, getEnd, !isOpt, pBar)
	} else {
		b.dp = data.NewHistProviderWithRuntimeDeps(dataDeps, onBar, b.OnEnvEnd, getEnd, !isOpt, pBar)
	}
	b.dp.SetAllowDownload(allowBacktestKlineDownload(isOpt))
	if state := b.runtimeCore(); state != nil {
		state.OnExit(b.dp.Terminate)
	}
	pairHooks := strat.PairUpdateHooks{
		SubWarmPairs: b.dp.SubWarmPairs,
		SymbolState:  symbols,
		Core:         b.runtimeCore(),
		StrategyState: func() *strat.State {
			if deps := b.RuntimeDependencies(); deps != nil {
				return deps.Strategies
			}
			return nil
		}(),
		LookupSymbol: func(pair string) (*orm.ExSymbol, *errs.Error) {
			if symbols == nil {
				return orm.GetExSymbolCur(pair)
			}
			return symbols.GetExSymbolCur(pair)
		},
		ExitOrders: func(acc string, orders []*ormo.InOutOrder, req *strat.ExitReq) *errs.Error {
			if deps := b.RuntimeDependencies(); deps != nil {
				return biz.GetOdMgrWithState(deps.Trading, acc).ExitAndFill(orders, req)
			}
			return biz.GetOdMgr(acc).ExitAndFill(orders, req)
		},
	}
	if deps := b.RuntimeDependencies(); deps != nil && deps.Strategies != nil {
		deps.Strategies.SetPairUpdateHooks(pairHooks)
	} else {
		strat.SetPairUpdateHooks(pairHooks)
	}
	stopBacktest := core.StopAll
	if state := b.runtimeCore(); state != nil {
		stopBacktest = state.Stop
	}
	if isOpt {
		stopBacktest = b.dp.Terminate
	}
	var prices *com.PriceState
	var clock *btime.ClockState
	if deps := b.RuntimeDependencies(); deps != nil {
		if deps.Market != nil {
			prices = deps.Market.Prices
		}
		clock = deps.Clock
	}
	if deps := b.RuntimeDependencies(); deps != nil {
		biz.InitLocalOrderMgrWithRuntimeDeps(*deps, b.orderCB, !isOpt, stopBacktest)
	} else {
		biz.InitLocalOrderMgrWithPriceState(b.orderCB, !isOpt, prices, clock, stopBacktest)
	}
	return b
}

func allowBacktestKlineDownload(isOpt bool) bool {
	return !isOpt && !config.Data.BTNoKlineDownload
}

func (b *BackTestLite) FeedDataSeries(evt *orm.DataSeries) bool {
	if b.RuntimeDependencies() != nil {
		b.SetTimeMS(seriesEndMS(evt))
	}
	if orm.NormalizeSeriesSource(evt.Source) != orm.SeriesSourceKline || !evt.HasOHLCV() {
		if err := b.Trader.FeedDataSeries(evt); err != nil {
			log.Error("FeedDataSeries fail", zap.Int32("sid", evt.Sid), zap.Error(err))
			b.setRunError(err)
			return false
		}
		return true
	}
	b.BarNum += 1
	curTime := b.TimeMS()
	batchState := b.batchStateForRun()
	lastBatchMS := batchState.LastBatchMS()
	if curTime > lastBatchMS {
		// Enter the next timeframe and trigger the batch entry callback
		// 进入下一个时间帧，触发批量入场回调
		b.SetTimeMS(lastBatchMS)
		waitNum := biz.TryFireBatchesWithRuntimeDeps(b.RuntimeDependencies(), batchState, curTime, evt.IsWarmUp)
		if waitNum > 0 {
			log.Warn(fmt.Sprintf("batch job exec fail, wait: %v", waitNum))
		}
		batchState.SetLastBatchMS(curTime)
		b.SetTimeMS(curTime)
	}
	if curTime > b.lastTime {
		b.lastTime = curTime
		b.TimeNum += 1
		if !evt.IsWarmUp {
			b.setCheckWallets(true)
		}
	}
	if errRun := b.Trader.FeedDataSeries(evt); errRun != nil {
		if errRun.Code == core.ErrLiquidation {
			b.onLiquidation(evt.Symbol())
		} else {
			log.Error("FeedDataSeries fail", zap.String("p", evt.Symbol()), zap.Error(errRun))
			b.setRunError(errRun)
		}
		return false
	}
	if !b.botRunning() {
		b.stoppedEarly = true
		b.dp.Terminate()
		return false
	}
	return true
}

// SetTimeMS keeps the legacy feeder facade aligned with the explicit clock
// while the remaining historical provider code still reads btime.CurTimeMS.
// Entry runners hold the legacy gate, so this compatibility write is safe and
// adds no lookup or synchronization to the typed state path.
func (b *BackTestLite) SetTimeMS(timeMS int64) {
	b.Trader.SetTimeMS(timeMS)
	if b.RuntimeDependencies() != nil {
		btime.CurTimeMS = timeMS
	}
}

func seriesEndMS(evt *orm.DataSeries) int64 {
	if evt == nil {
		return 0
	}
	if evt.EndMS > evt.TimeMS {
		return evt.EndMS
	}
	if evt.TimeFrame != "" {
		return evt.TimeMS + int64(banutils.TFToSecs(evt.TimeFrame))*1000
	}
	return evt.TimeMS
}

func (b *BackTestLite) runtimeCore() *core.State {
	if b == nil {
		return nil
	}
	if deps := b.RuntimeDependencies(); deps != nil {
		return deps.Core
	}
	return nil
}

func (b *BackTestLite) setCheckWallets(check bool) {
	if state := b.runtimeCore(); state != nil {
		state.CheckWallets = check
		return
	}
	core.CheckWallets = check
}

func (b *BackTestLite) checkWallets() bool {
	if state := b.runtimeCore(); state != nil {
		return state.CheckWallets
	}
	return core.CheckWallets
}

func (b *BackTestLite) botRunning() bool {
	if state := b.runtimeCore(); state != nil {
		return state.BotRunning
	}
	return core.BotRunning
}

func (b *BackTestLite) batchStateForRun() *strat.BatchState {
	return b.Trader.BatchState()
}

func (b *BackTestLite) setRunError(err *errs.Error) {
	if err == nil || b.runErr != nil {
		return
	}
	b.runErr = err
	if b.dp != nil {
		b.dp.Terminate()
	}
}

func (b *BackTestLite) resolveLoopError(err *errs.Error) *errs.Error {
	if err != nil {
		return err
	}
	return b.runErr
}

func (b *BackTestLite) onLiquidation(symbol string) {
	date := btime.ToDateStr(b.TimeMS(), "")
	chargeOnBomb := config.ChargeOnBomb
	if deps := b.RuntimeDependencies(); deps != nil && deps.Config != nil {
		if cfg := deps.Config.View(); cfg != nil {
			chargeOnBomb = cfg.ChargeOnBomb
		}
	}
	if chargeOnBomb {
		wallets := biz.GetWallets(config.DefAcc)
		if deps := b.RuntimeDependencies(); deps != nil {
			wallets = biz.InitFakeWalletsWithRuntimeDeps(*deps, symbol)
		}
		oldVal := wallets.TotalLegal(nil, false)
		if b.RuntimeDependencies() == nil {
			biz.InitFakeWallets(symbol)
		}
		newVal := wallets.TotalLegal(nil, false)
		b.TotalInvest += newVal - oldVal
		log.Warn(fmt.Sprintf("wallet %s BOMB at %s, reset wallet and continue..", symbol, date))
	} else {
		log.Warn(fmt.Sprintf("wallet %s BOMB at %s, exit", symbol, date))
		if !b.isOpt {
			if state := b.runtimeCore(); state != nil {
				state.Stop()
			} else if core.StopAll != nil {
				core.StopAll()
			}
		}
		b.dp.Terminate()
	}
}

func (b *BackTestLite) orderCB(order *ormo.InOutOrder, isEnter bool) {
	account := config.DefAcc
	var deps *biz.RuntimeDeps
	if b != nil {
		deps = b.RuntimeDependencies()
	}
	var wallets *biz.BanWallets
	if deps != nil {
		if deps.DefaultAccount != "" {
			account = deps.DefaultAccount
		}
		if deps.Trading != nil {
			wallets = deps.Trading.Wallet(account)
		}
	}
	if isEnter {
		openNum := ormo.OpenNum(account, ormo.InOutStatusPartEnter)
		if deps != nil && deps.Orders != nil {
			openNum = deps.Orders.OpenNum(account, ormo.InOutStatusPartEnter)
		}
		if openNum > b.MaxOpenOrders {
			b.MaxOpenOrders = openNum
		}
	} else {
		if wallets == nil {
			wallets = biz.GetWallets(account)
		}
		// 更新单笔开单金额
		wallets.TryUpdateStakePctAmt()
		if config.DrawBalanceOver > 0 {
			quoteLegal := wallets.AvaLegal(config.StakeCurrency)
			if quoteLegal > config.DrawBalanceOver {
				wallets.WithdrawLegal(quoteLegal-config.DrawBalanceOver, config.StakeCurrency)
			}
		}
	}
}

func NewBackTest(isOpt bool, outDir string) (*BackTest, *errs.Error) {
	return newBackTest(biz.NewTrader(strat.NewBatchState()), nil, isOpt, outDir, nil)
}

// NewBackTestWithBatchState binds the backtest to a composition root's batch
// queue. A nil state creates a private state; the session must remain active
// for the returned runner's lifetime.
func NewBackTestWithBatchState(session LegacySession, batchState *strat.BatchState, isOpt bool, outDir string) (*BackTest, *errs.Error) {
	session.require()
	return newBackTest(biz.NewTrader(batchState), nil, isOpt, outDir, nil)
}

// NewBackTestWithBatchAndSymbolState binds a backtest to the composition
// root's batch queue and symbol indexes. A nil symbol state preserves legacy
// package-level symbol APIs for existing callers; the session must remain
// active for the returned runner's lifetime.
func NewBackTestWithBatchAndSymbolState(session LegacySession, batchState *strat.BatchState, symbols *orm.SymbolState, isOpt bool, outDir string) (*BackTest, *errs.Error) {
	session.require()
	return newBackTest(biz.NewTrader(batchState), symbols, isOpt, outDir, nil)
}

// NewBackTestWithRuntimeDeps constructs a backtest over explicit runtime state
// while retaining the legacy strategy, order, and wallet globals.
func NewBackTestWithRuntimeDeps(session LegacySession, deps biz.RuntimeDeps, symbols *orm.SymbolState, isOpt bool, outDir string) (*BackTest, *errs.Error) {
	session.require()
	trader := biz.NewTraderWithRuntimeDeps(deps)
	return newBackTest(trader, symbols, isOpt, outDir,
		legacyDataRuntimeDeps(trader.RuntimeDependencies(), symbols))
}

// NewBackTestWithRuntimeDataDeps is the explicit composition-root entry point
// for a backtest. dataDeps carries runtime-owned data dependencies, but
// strategy, order, wallet, and other legacy facades still require the session
// to remain active for the returned runner's lifetime.
func NewBackTestWithRuntimeDataDeps(session LegacySession, deps biz.RuntimeDeps, symbols *orm.SymbolState, isOpt bool, outDir string, dataDeps *data.RuntimeDeps) (*BackTest, *errs.Error) {
	session.require()
	trader := biz.NewTraderWithRuntimeDeps(deps)
	return newBackTest(trader, symbols, isOpt, outDir, dataDeps)
}

func newBackTest(trader biz.Trader, symbols *orm.SymbolState, isOpt bool, outDir string, dataDeps *data.RuntimeDeps) (*BackTest, *errs.Error) {
	stages := []string{"init", "listMs", "loadPairs", "tfScores", "loadJobs", "warmJobs", "downKline", "runBT"}
	stgWeis := []float64{1, 1, 2, 2, 1, 2, 10, 10}
	b := &BackTest{
		PBar: utils.NewStagedPrg(stages, stgWeis),
	}
	getEnd := func() int64 {
		if b.nextRefresh > 0 {
			return b.nextRefresh
		}
		return config.TimeRange.EndMS
	}
	b.BackTestLite = newBackTestLite(trader, symbols, isOpt, b.FeedDataSeries, getEnd, b.PBar, dataDeps)
	if outDir == "" && !isOpt {
		hash, err := config.Data.HashCode()
		if err != nil {
			return nil, err
		}
		outDir = fmt.Sprintf("%s/backtest/%s", config.GetDataDir(), hash)
	}
	b.OutDir = config.ParsePath(outDir)
	config.LoadPerfs(config.GetDataDir())
	return b, nil
}

func (b *BackTest) Init() *errs.Error {
	b.SetTimeMS(config.TimeRange.StartMS)
	b.historicalCloseMS = historicalCloseBoundaries(config.HistoricalCoverage, config.TimeRange)
	b.historicalCloseIndex = 0
	b.MinReal = math.MaxFloat64
	log.Info("backtest config summary",
		zap.Bool("questdb", orm.IsQuestDB),
		zap.String("data_dir", config.GetDataDirSafe()),
		zap.String("timerange", config.Data.TimeRangeRaw),
		zap.String("time_start", config.Data.TimeStart),
		zap.String("time_end", config.Data.TimeEnd),
		zap.Int("pair_count", len(config.Pairs)),
		zap.Int("run_policy_count", len(config.RunPolicy)),
		zap.Int("pair_filter_count", len(config.PairFilters)),
		zap.Int("run_tf_count", len(config.RunTimeframes)),
		zap.Float64("stake_amount", config.StakeAmount),
		zap.Float64("bt_net_cost", config.BTNetCost),
		zap.Int("order_bar_max", config.OrderBarMax),
		zap.String("out_dir", b.OutDir))
	if b.OutDir != "" {
		err_ := os.MkdirAll(b.OutDir, 0755)
		if err_ != nil {
			return errs.New(core.ErrIOWriteFail, err_)
		}
	}
	var err *errs.Error
	if deps := b.RuntimeDependencies(); deps != nil {
		startAt, endAt := int64(0), int64(0)
		if deps.Config != nil {
			if cfg := deps.Config.View(); cfg != nil && cfg.TimeRange != nil {
				startAt, endAt = cfg.TimeRange.StartMS, cfg.TimeRange.EndMS
			}
		}
		if deps.Core != nil {
			if startAt == 0 {
				startAt = deps.Core.StartAt
			}
			err = ormo.InitTaskWithState(deps.Orders, deps.DefaultAccount, deps.Core.RunMode, startAt, endAt, !b.isOpt)
		} else {
			err = ormo.InitTaskWithState(deps.Orders, deps.DefaultAccount, core.RunModeBackTest, startAt, endAt, !b.isOpt)
		}
	} else {
		err = ormo.InitTask(!b.isOpt, b.OutDir)
	}
	if err != nil {
		return err
	}
	err = b.initTaskOut()
	if err != nil {
		return err
	}
	b.PBar.SetProgress("init", 1)
	err = orm.InitListDatesWithState(b.symbols)
	if err != nil {
		return err
	}
	b.PBar.SetProgress("listMs", 1)
	// 交易对初始化
	err = refreshPairJobsWithRuntimeDeps(b.dp, b.symbols, b.RuntimeDependencies(), !b.isOpt, true, b.PBar)
	if err != nil {
		return err
	}
	_, err = b.syncThirdPartySeriesRange()
	if err != nil {
		return err
	}
	return nil
}

func (b *BackTest) syncThirdPartySeriesRange() (*data.SeriesPlan, *errs.Error) {
	plan, err := backtestBootstrapPlan(collectBacktestJobsForBacktest(b), config.TimeRange)
	if err != nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "%v", err)
	}
	if err := b.thirdPartyRuntime().Ensure(b.backtestSeriesContext(), plan); err != nil {
		return plan, errs.New(core.ErrRunTime, err)
	}
	if b.BackTestLite != nil && b.dp != nil {
		if err := b.dp.SetSeriesSubs(plan.Subs); err != nil {
			return plan, err
		}
	}
	return plan, nil
}

func (b *BackTest) ensureThirdPartySeriesRange() (*data.SeriesPlan, *errs.Error) {
	return b.syncThirdPartySeriesRange()
}

func (b *BackTest) thirdPartyRuntime() *data.SeriesRuntime {
	if b.seriesRuntime == nil {
		b.seriesRuntime = data.NewSeriesRuntime(nil)
	}
	return b.seriesRuntime
}

func (b *BackTest) backtestSeriesContext() context.Context {
	if b != nil && b.BackTestLite != nil {
		if state := b.runtimeCore(); state != nil && state.Context() != nil {
			return state.Context()
		}
	}
	if core.Ctx != nil {
		return core.Ctx
	}
	return context.Background()
}

func collectBacktestJobs() []*strat.StratJob {
	return collectBacktestJobsFromMap(strat.GetJobs(config.DefAcc))
}

func collectBacktestJobsForBacktest(b *BackTest) []*strat.StratJob {
	if b != nil && b.BackTestLite != nil && b.RuntimeDependencies() != nil && b.RuntimeDependencies().Strategies != nil {
		deps := b.RuntimeDependencies()
		return collectBacktestJobsFromMap(deps.Strategies.Jobs(deps.DefaultAccount))
	}
	return collectBacktestJobs()
}

func collectBacktestJobsFromMap(jobsByEnv map[string]map[string]*strat.StratJob) []*strat.StratJob {
	seen := make(map[*strat.StratJob]bool)
	var jobs []*strat.StratJob
	envKeys := make([]string, 0, len(jobsByEnv))
	for envKey := range jobsByEnv {
		envKeys = append(envKeys, envKey)
	}
	sort.Strings(envKeys)
	for _, envKey := range envKeys {
		stgMap := jobsByEnv[envKey]
		stgNames := make([]string, 0, len(stgMap))
		for stgName := range stgMap {
			stgNames = append(stgNames, stgName)
		}
		sort.Strings(stgNames)
		for _, stgName := range stgNames {
			job := stgMap[stgName]
			if job == nil || seen[job] {
				continue
			}
			seen[job] = true
			jobs = append(jobs, job)
		}
	}
	return jobs
}

func backtestBootstrapPlan(jobs []*strat.StratJob, tr *config.TimeTuple) (*data.SeriesPlan, error) {
	if tr == nil {
		return nil, fmt.Errorf("bootstrap collect phase=collect: time range is required")
	}
	if tr.StartMS >= tr.EndMS {
		return nil, fmt.Errorf("bootstrap collect phase=collect: invalid time range")
	}
	return data.NewSeriesPlan(jobs, tr.StartMS, tr.EndMS)
}

func (b *BackTest) FeedDataSeries(evt *orm.DataSeries) {
	if orm.NormalizeSeriesSource(evt.Source) != orm.SeriesSourceKline || !evt.HasOHLCV() {
		_ = b.BackTestLite.FeedDataSeries(evt)
		return
	}
	if b.shouldCloseHistoricalBoundary(evt.TimeMS) {
		if err := b.closeHistoricalBoundaries(evt.TimeMS); err != nil {
			b.setRunError(err)
			return
		}
	}
	curTime := b.TimeMS()
	ok := b.BackTestLite.FeedDataSeries(evt)
	if !evt.IsWarmUp && b.checkWallets() {
		b.setCheckWallets(false)
		account := config.DefAcc
		if deps := b.RuntimeDependencies(); deps != nil && deps.DefaultAccount != "" {
			account = deps.DefaultAccount
		}
		odNum := ormo.OpenNum(account, ormo.InOutStatusPartEnter)
		if deps := b.RuntimeDependencies(); deps != nil && deps.Orders != nil {
			odNum = deps.Orders.OpenNum(account, ormo.InOutStatusPartEnter)
		}
		b.logState(evt.TimeMS, curTime, odNum)
	}
	if ok && b.nextRefresh > 0 && evt.TimeMS >= b.nextRefresh {
		// 刷新交易对
		refreshMs := evt.TimeMS // 这里bar.Time 可能远大于b.nextRefresh，所以应当用bar.Time
		b.nextRefresh = b.schedule.Next(time.UnixMilli(evt.TimeMS)).UnixMilli()
		b.SetTimeMS(refreshMs)
		err := refreshPairJobsWithRuntimeDeps(b.dp, b.symbols, b.RuntimeDependencies(), !b.isOpt, false, nil)
		b.SetTimeMS(curTime)
		dateStr := btime.ToDateStr(refreshMs, "")
		if err != nil {
			log.Error("RefreshPairJobs", zap.String("date", dateStr), zap.Error(err))
			if b.dataPrep {
				b.dataPrepErr = err
			}
			b.setRunError(err)
			return
		} else {
			if _, err := b.syncThirdPartySeriesRange(); err != nil {
				log.Error("ensure third-party series after pair refresh", zap.String("date", dateStr), zap.Error(err))
				if b.dataPrep {
					b.dataPrepErr = err
				}
				b.setRunError(err)
				return
			}
			log.Info("refreshed pairs at", zap.String("date", dateStr))
		}
		b.dp.SetDirty()
	}
}

func historicalCloseBoundaries(coverage *config.HistoricalCoverageConfig, runRange *config.TimeTuple) []int64 {
	if coverage == nil || runRange == nil {
		return nil
	}
	result := make([]int64, 0, 2)
	if endMS := coverage.BaselineEndMS; endMS > runRange.StartMS && runRange.EndMS > endMS &&
		(len(result) == 0 || result[len(result)-1] != endMS) {
		result = append(result, endMS)
	}
	if endMS := coverage.HistoricalResultEndMS; endMS > runRange.StartMS && runRange.EndMS > endMS &&
		(len(result) == 0 || result[len(result)-1] != endMS) {
		result = append(result, endMS)
	}
	return result
}

func (b *BackTest) shouldCloseHistoricalBoundary(eventMS int64) bool {
	return b.historicalCloseIndex < len(b.historicalCloseMS) &&
		eventMS >= b.historicalCloseMS[b.historicalCloseIndex]
}

func (b *BackTest) closeHistoricalBoundaries(eventMS int64) *errs.Error {
	for b.shouldCloseHistoricalBoundary(eventMS) {
		account := config.DefAcc
		if deps := b.RuntimeDependencies(); deps != nil {
			if deps.DefaultAccount != "" {
				account = deps.DefaultAccount
			}
			if err := biz.CloseBacktestOrdersAtWithState(deps.Trading, account, b.historicalCloseMS[b.historicalCloseIndex]); err != nil {
				return err
			}
		} else if err := biz.CloseBacktestOrdersAt(account, b.historicalCloseMS[b.historicalCloseIndex]); err != nil {
			return err
		}
		b.historicalCloseIndex++
	}
	return nil
}

func (b *BackTest) Run() *errs.Error {
	err := b.initRefreshCron()
	if err != nil {
		log.Error("init pair cron fail", zap.Error(err))
		return err
	}
	err = b.Init()
	if err != nil {
		log.Error("backtest init fail", zap.Error(err))
		return err
	}
	if !b.isOpt {
		b.cronDumpBtStatus()
		b.schedulerForRun().Start()
	}
	btStart := btime.UTCTime()
	loopMainFn := b.loopMainFn
	if loopMainFn == nil {
		loopMainFn = b.dp.LoopMain
	}
	err = b.resolveLoopError(loopMainFn())
	if !b.isOpt {
		b.schedulerForRun().Stop()
	}
	if err != nil {
		log.Error("backtest loop fail", zap.Error(err))
		return err
	}
	// Some feeders finish without emitting a bar at or after a historical
	// cutoff (for example when every series ends at the old baseline). Ensure
	// those positions are closed before the final cleanup uses the new end.
	if err := b.closeHistoricalBoundaries(math.MaxInt64); err != nil {
		log.Error("close historical boundaries fail", zap.Error(err))
		return err
	}
	btCost := btime.UTCTime() - btStart
	account := config.DefAcc
	var wallets *biz.BanWallets
	var odMgr biz.IOrderMgr
	if deps := b.RuntimeDependencies(); deps != nil {
		if deps.DefaultAccount != "" {
			account = deps.DefaultAccount
		}
		odMgr = biz.GetOdMgrWithState(deps.Trading, account)
		if deps.Trading != nil {
			wallets = deps.Trading.Wallet(account)
		}
	} else {
		odMgr = biz.GetOdMgr(account)
		wallets = biz.GetWallets(account)
	}
	if odMgr == nil {
		return errs.NewMsg(core.ErrRunTime, "backtest order manager is not initialized")
	}
	err = odMgr.CleanUp()
	if deps := b.RuntimeDependencies(); deps != nil && deps.Strategies != nil {
		strat.ExitStratJobsWithState(deps.Strategies)
	} else {
		strat.ExitStratJobs()
	}
	if err != nil {
		log.Error("backtest clean orders fail", zap.Error(err))
		return err
	}
	if b.dataPrep {
		return nil
	}
	if wallets == nil {
		wallets = biz.GetWallets(account)
	}
	b.logPlot(wallets, b.TimeMS(), -1, -1)
	normalizeBacktestResultRange(b.BTResult, b.stoppedEarly)
	b.Collect()
	if AfterBacktest != nil {
		AfterBacktest(b)
	}
	if !b.isOpt {
		log.Info(fmt.Sprintf("Complete! cost: %.1fs, avg: %.1f bar/s", btCost, float64(b.BarNum)/btCost))
		failOpens := strat.DumpAccFailOpens()
		if failOpens != "" {
			log.Info("fail open tag nums:\n" + failOpens)
		}
		b.printBtResult(true)
	}
	return nil
}

// normalizeBacktestResultRange keeps the reported window tied to the
// immutable backtest request. A run with no non-warmup events can otherwise
// report its first appended-tail event as the start of the whole backtest.
func normalizeBacktestResultRange(result *BTResult, stoppedEarly bool) {
	if stoppedEarly {
		return
	}
	if result == nil || config.TimeRange == nil || config.TimeRange.StartMS <= 0 ||
		config.TimeRange.EndMS <= config.TimeRange.StartMS {
		return
	}
	result.StartMS = config.TimeRange.StartMS
	result.EndMS = config.TimeRange.EndMS
}

func (b *BackTest) resolveLoopError(err *errs.Error) *errs.Error {
	if err != nil {
		return err
	}
	if b.dataPrepErr != nil {
		return b.dataPrepErr
	}
	if b.BackTestLite != nil {
		return b.BackTestLite.resolveLoopError(nil)
	}
	return nil
}

func (b *BackTest) initTaskOut() *errs.Error {
	if b.OutDir != "" {
		logFile := b.OutDir + "/out.log"
		config.Args.Logfile = logFile
		if utils.Exists(logFile) {
			err_ := os.Remove(logFile)
			if err_ != nil {
				log.Warn("delete old log fail", zap.Error(err_))
			}
		}
		config.Args.SetLog(!b.isOpt)
	}
	_, ok := config.Accounts[config.DefAcc]
	if !ok {
		panic("default Account invalid!")
	}
	if config.StakePct > 0 {
		log.Warn("stake_amt may result in inconsistent order amounts with each backtest!")
	}
	return nil
}

func (b *BackTest) cronDumpBtStatus() {
	if config.StrictBacktest() {
		return
	}
	b.lastDumpMs = btime.UTCStamp()
	_, err_ := b.schedulerForRun().AddFunc("30 * * * * *", func() {
		curTime := btime.UTCStamp()
		if curTime-b.lastDumpMs < 300000 {
			// 5分钟保存一次回测状态
			return
		}
		b.lastDumpMs = curTime
		log.Info("dump backTest status to files...")
		b.Collect()
		b.printBtResult(false)
	})
	if err_ != nil {
		log.Error("add Dump BackTest Status fail", zap.Error(err_))
	}
}

func (b *BackTest) schedulerForRun() com.Scheduler {
	if b != nil {
		if deps := b.RuntimeDependencies(); deps != nil && deps.Scheduler != nil {
			return deps.Scheduler
		}
	}
	return com.Cron()
}

func (b *BackTest) initRefreshCron() *errs.Error {
	if config.PairMgr.Cron != "" {
		var err_ error
		b.schedule, err_ = utils.NewCronScheduler(config.PairMgr.Cron)
		if err_ != nil {
			return errs.New(core.ErrBadConfig, err_)
		}
		baseMS := config.TimeRange.StartMS
		for {
			baseTime := time.UnixMilli(baseMS)
			b.nextRefresh = b.schedule.Next(baseTime).UnixMilli()
			if b.nextRefresh-baseMS > config.MinPairCronGapMS {
				break
			}
			baseMS = b.nextRefresh
		}
	}
	return nil
}

func RefreshPairJobs(dp data.IProvider, showLog, isFirst bool, pBar *utils.StagedPrg) *errs.Error {
	return RefreshPairJobsWithSymbolState(dp, nil, showLog, isFirst, pBar)
}

// RefreshPairJobsWithSymbolState keeps the pair refresh's symbol subscription
// set aligned with the provider's runtime-owned symbol state.
func RefreshPairJobsWithSymbolState(dp data.IProvider, symbols *orm.SymbolState, showLog, isFirst bool, pBar *utils.StagedPrg) *errs.Error {
	return refreshPairJobsWithRuntimeDeps(dp, symbols, nil, showLog, isFirst, pBar)
}

// refreshPairJobsWithRuntimeDeps uses typed clock/core state when supplied.
// Job registries, order managers, and relay snapshots remain legacy globals.
func refreshPairJobsWithRuntimeDeps(dp data.IProvider, symbols *orm.SymbolState, deps *biz.RuntimeDeps, showLog, isFirst bool, pBar *utils.StagedPrg) *errs.Error {
	curTime := btime.TimeMS()
	envReal := core.EnvReal
	if deps != nil {
		curTime = deps.Clock.TimeMS()
		envReal = deps.Core.EnvReal
	}
	if isFirst {
		if config.PairMgr.Cron != "" {
			schedule, err_ := utils.NewCronScheduler(config.PairMgr.Cron)
			if err_ != nil {
				return errs.New(errs.CodeRunTime, err_)
			}
			curTime = utils.CronAlign(schedule, btime.ToTime(curTime)).UnixMilli()
		} else if !envReal && config.PairMgr.UseLatest {
			// 回测时配置use_latest=true，且cron为空，则使用最新时间刷新交易品种
			curTime = min(btime.UTCStamp(), config.TimeRange.EndMS)
		}
	}
	pairs, pairTfScores, err := biz.RefreshPairsWithSymbolState(symbols, showLog, curTime, pBar)
	if err != nil {
		return err
	}
	if deps != nil {
		syncRuntimePairs(deps.Core, pairs)
	}
	// store the currently running jobs and mark them as prohibited from running
	// 获取旧的已运行一段时间的任务（在刷新任务前运行），标记为禁止运行
	forbidJobs := strat.GetJobKeys()
	if deps != nil && deps.Strategies != nil {
		forbidJobs = deps.Strategies.JobKeysAll()
	}
	// 刷新交易任务
	warms, err := biz.RefreshJobsWithRuntimeDeps(deps, symbols, pairs, pairTfScores, showLog, pBar)
	if err != nil {
		return err
	}
	if isFirst {
		// 监听订单状态变化，触发策略的OnOrderChange
		biz.InitOdSubsWithRuntimeDeps(deps)
	}
	// relay the simulate open position orders for new symbols at this time
	// 接力入场新品种的截止此时模拟持仓订单
	backMode := core.RunMode
	err = relayUnFinishOrders(pairTfScores, forbidJobs, isFirst, symbols)
	core.SetRunMode(backMode)
	core.SetRunEnv(core.RunEnv)
	if err != nil {
		return err
	}
	if symbols == nil {
		orm.ResetSubSymbol()
	} else {
		symbols.ResetSubSymbol()
	}
	// warm up for new symbols
	return dp.SubWarmPairs(warms, true)
}

func syncRuntimePairs(state *core.State, pairs []string) {
	if state == nil {
		return
	}
	additionalAllowed := make([]string, 0)
	for _, policy := range config.RunPolicy {
		additionalAllowed = append(additionalAllowed, policy.Pairs...)
	}
	state.SetPairs(pairs, additionalAllowed)
}

// syncRuntimeOrderMatchState is the temporary boundary between the legacy
// strategy registry and the typed Trader. Strategy loading still builds its
// timeframe map in core's compatibility facade; copy the completed snapshot
// once per refresh so the hot path never consults that facade.
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

/*
获取模拟回测的未完成订单，接力入场；
应在RefreshJobs之后再调用，否则入场订单可能被视为旧的平仓掉
*/
func withRelayBacktestState(run func() *errs.Error) *errs.Error {
	backUp := biz.BackupVars()
	timeRange := config.TimeRange.Clone()
	backTime := btime.CurTimeMS
	backPols := config.RunPolicy
	backRunMode := core.RunMode
	backRunEnv := core.RunEnv
	bakAccs := config.MergeAccounts()
	defer func() {
		config.RunPolicy = backPols
		config.ClearRefineMap()
		config.TimeRange = timeRange
		btime.CurTimeMS = backTime
		biz.RestoreVars(backUp)
		core.SetRunMode(backRunMode)
		core.SetRunEnv(backRunEnv)
		config.Accounts = bakAccs
	}()
	core.SetRunMode(core.RunModeBackTest)
	core.SetRunEnv(backRunEnv)
	return run()
}

func relayUnFinishOrders(pairTfScores map[string]map[string]float64, forbidJobs map[string]map[string]bool, isFirst bool, symbols *orm.SymbolState) *errs.Error {
	if !config.RelaySimUnFinish {
		return nil
	}
	simEndMs := btime.CurTimeMS
	if core.LiveMode {
		simEndMs = btime.TimeMS()
	}
	// pair_tf_stratID
	relayOpens := make(map[string]*ormo.InOutOrder)
	relayDones := make(map[string]*ormo.InOutOrder)
	err := withRelayBacktestState(func() *errs.Error {
		// Divide into multiple groups based on the subscription period according to the strategy
		// 按策略订阅周期划分为多个组
		groups := strat.RelayPolicyGroupsWithSymbolState(symbols)
		for _, gp := range groups {
			// Reset global variables, backtest for time range, and search for open orders
			// 重置全局变量，回测过去一段时间，查找未平仓订单
			biz.ResetVars()
			strat.ForbidJobs = forbidJobs
			btime.CurTimeMS = gp.StartMS
			config.TimeRange = &config.TimeTuple{
				StartMS: gp.StartMS,
				EndMS:   simEndMs,
			}
			err := ormo.InitTask(false, "")
			if err != nil {
				return err
			}
			lite := newBackTestLite(biz.NewTrader(strat.NewBatchState()), symbols, true, nil, nil, nil, nil)
			// set policy to run
			// 重新加载策略任务
			err = config.SetRunPolicy(false, gp.Policies...)
			if err != nil {
				return err
			}
			warms, _, err := strat.LoadStratJobsWithSymbolState(symbols, core.Pairs, pairTfScores)
			if err != nil {
				return err
			}
			if len(warms) == 0 {
				// 没有需要预回测的任务
				continue
			}
			err = lite.dp.SubWarmPairs(warms, true)
			if err != nil {
				return err
			}
			err = lite.resolveLoopError(lite.dp.LoopMain())
			if err != nil {
				return err
			}
			// Record the last unfinished orders
			// 记录最后的未完成订单
			odMap, lock := ormo.GetOpenODs(config.DefAcc)
			lock.Lock()
			for _, od := range odMap {
				if od.Status >= ormo.InOutStatusPartEnter && od.ExitTag == "" {
					relayOpens[od.KeyAlign()] = od
				} else if od.Status >= ormo.InOutStatusFullExit {
					relayDones[od.KeyAlign()] = od
				}
			}
			lock.Unlock()
			for _, od := range ormo.HistODs {
				relayDones[od.KeyAlign()] = od
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return syncSimOrders(isFirst, relayOpens, relayDones)
}

func syncSimOrders(isFirst bool, relayOpens, relayDones map[string]*ormo.InOutOrder) *errs.Error {
	if isFirst {
		// 如果是初次执行，检查打开的订单是否已在测试期间平仓，是则自动平仓
		// 主要针对实盘隔一段时间后重启有未平仓订单场景，需检查订单是否应在机器人停止期间平仓
		var err *errs.Error
		closeNums := make(map[string]int)
		for acc := range utils.MapKeys(config.Accounts, config.StrictBacktest()) {
			cfg := config.Accounts[acc]
			if cfg.NoTrade {
				continue
			}
			odMgr := biz.GetOdMgr(acc)
			odMap, lock := ormo.GetOpenODs(acc)
			var exitOds []*ormo.InOutOrder
			lock.Lock()
			for _, od := range odMap {
				if _, ok := relayDones[od.KeyAlign()]; ok {
					exitOds = append(exitOds, od)
				}
			}
			lock.Unlock()
			if len(exitOds) > 0 {
				err = odMgr.ExitAndFill(exitOds, &strat.ExitReq{Tag: core.ExitTagExitDelay})
				if err != nil {
					log.Error("close delayed order fail", zap.Int("num", len(exitOds)), zap.Error(err))
				} else {
					closeNums[acc] = len(exitOds)
				}
			}
		}
		if len(closeNums) > 0 {
			log.Info("closed delayed order", zap.Any("nums", closeNums))
		}
	}
	if len(relayOpens) == 0 {
		return nil
	}
	var err *errs.Error
	for acc := range utils.MapKeys(config.Accounts, config.StrictBacktest()) {
		cfg := config.Accounts[acc]
		if cfg.NoTrade {
			continue
		}
		odMgr := biz.GetOdMgr(acc)
		jobs := strat.GetJobs(acc)
		allowOds := make([]*ormo.InOutOrder, 0, len(relayOpens))
		odMap, lock := ormo.GetOpenODs(acc)
		curKeyMap := make(map[string]*ormo.InOutOrder)
		lock.Lock()
		for _, od := range odMap {
			curKeyMap[od.KeyAlign()] = od
		}
		lock.Unlock()
		for keyAlign := range utils.MapKeys(relayOpens, config.StrictBacktest()) {
			od := relayOpens[keyAlign]
			if _, ok := curKeyMap[keyAlign]; ok {
				// 此订单已存在，跳过
				continue
			}
			stgMap, ok := jobs[fmt.Sprintf("%s_%s", od.Symbol, od.Timeframe)]
			if ok {
				if job, ok := stgMap[od.Strategy]; ok {
					job.OrderNum += 1
					allowOds = append(allowOds, od)
					continue
				}
			}
			// 此账户未订阅此策略任务，忽略即可
		}
		if len(allowOds) > 0 {
			err = odMgr.RelayOrders(allowOds)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
