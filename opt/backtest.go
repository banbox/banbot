package opt

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	banutils "github.com/banbox/banexg/utils"
	"github.com/banbox/cron/v3"
	"go.uber.org/zap"
)

const (
	ShowNum                       = 600
	runtimeMinPairCronGapMS int64 = 30 * 60 * 1000
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
	afterBacktest        func(*BackTest)
	dataPrep             bool
	dataPrepErr          *errs.Error
	nextRefresh          int64 // The time of the next refresh of the trading pair 下一次刷新交易对的时间
	schedule             cron.Schedule
	seriesRuntime        *data.SeriesRuntime
	loopMainFn           func() *errs.Error
	historicalCloseMS    []int64
	historicalCloseIndex int
	outputOwned          bool
	schedulerMu          sync.Mutex
	runScheduler         com.Scheduler
	runSchedulerOwned    bool
}

func (b *BackTest) SetAfterBacktest(callback func(*BackTest)) {
	if b != nil {
		b.afterBacktest = callback
	}
}

func (b *BackTest) runAfterBacktestCallback() {
	if b == nil {
		return
	}
	if b.afterBacktest != nil {
		b.afterBacktest(b)
	}
}

func (b *BackTestLite) runConfig() *config.Config {
	if b != nil && b.RuntimeDependencies() != nil {
		return b.RuntimeDependencies().ConfigView()
	}
	return nil
}

func (b *BackTestLite) runSnapshot() *config.Snapshot {
	if b != nil && b.RuntimeDependencies() != nil {
		return b.RuntimeDependencies().Config
	}
	return nil
}

func (b *BackTestLite) runTimeRange() *config.TimeTuple {
	if cfg := b.runConfig(); cfg != nil {
		return cfg.TimeRange
	}
	return nil
}

func (b *BackTestLite) strictBacktest() bool {
	if b != nil && b.RuntimeDependencies() != nil {
		return b.RuntimeDependencies().StrictBacktest()
	}
	return false
}

func (b *BackTestLite) defaultAccount() string {
	if b != nil {
		if deps := b.RuntimeDependencies(); deps != nil {
			if deps.DefaultAccount != "" {
				return deps.DefaultAccount
			}
			if deps.Config != nil {
				return deps.Config.DefaultAccount()
			}
			if accounts := deps.AccountConfigs(); len(accounts) == 1 {
				for account := range accounts {
					return account
				}
			}
			return "default"
		}
	}
	return ""
}

func (b *BackTestLite) accountConfigs() map[string]*config.AccountConfig {
	if b != nil {
		if deps := b.RuntimeDependencies(); deps != nil {
			return deps.AccountConfigs()
		}
	}
	return nil
}

func runtimeConfigForDeps(deps *biz.RuntimeDeps) *config.Config {
	if deps == nil {
		return nil
	}
	return deps.ConfigView()
}

func runtimeTimeRangeForDeps(deps *biz.RuntimeDeps) *config.TimeTuple {
	if deps != nil {
		if cfg := deps.ConfigView(); cfg != nil {
			return cfg.TimeRange
		}
		return nil
	}
	return nil
}

func runtimeCronSchedule(exp string, deps *biz.RuntimeDeps) (cron.Schedule, error) {
	location := time.UTC
	if deps != nil && deps.Config != nil {
		location = deps.Config.Location()
	}
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	return parser.Parse(exp, location)
}

/*
NewBackTestLite 创建一个临时内部回测，仅用于寻找回测未平仓订单来接力
Create a temporary internal backtest, solely for the purpose of finding backtest open orders to relay.
*/

// NewBackTestLiteWithRuntimeDeps constructs an isolated backtest runner. All
// runtime-owned state, including the symbol catalog and data-provider
// projection, comes from deps.
func NewBackTestLiteWithRuntimeDeps(deps biz.RuntimeDeps, isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pBar *utils.StagedPrg) (*BackTestLite, *errs.Error) {
	if err := biz.BindRuntimeDeps(deps); err != nil {
		return nil, err
	}
	trader, err := biz.NewTraderWithRuntimeDeps(deps)
	if err != nil {
		return nil, err
	}
	bound := trader.RuntimeDependencies()
	dataDeps := bound.DataDeps()
	if dataDeps.IdentityErr != nil {
		return nil, errs.New(core.ErrRunTime, dataDeps.IdentityErr)
	}
	lite := newBackTestLite(trader, bound.Symbols, isOpt, onBar, getEnd, pBar, dataDeps)
	if lite.runErr != nil {
		return nil, lite.runErr
	}
	return lite, nil
}

func newBackTestLite(trader biz.Trader, symbols *orm.SymbolState, isOpt bool, onBar data.FnDataSeries, getEnd data.FnGetInt64, pBar *utils.StagedPrg, dataDeps *data.RuntimeDeps) *BackTestLite {
	b := &BackTestLite{
		Trader:   trader,
		BTResult: NewBTResult(),
		symbols:  symbols,
		isOpt:    isOpt,
	}
	deps := b.RuntimeDependencies()
	if deps == nil {
		b.runErr = errs.NewMsg(core.ErrRunTime, "explicit runtime dependencies are required for backtest")
		return b
	}
	symbols = deps.Symbols
	dataDeps = deps.DataDeps()
	b.symbols = symbols
	b.BTResult.runtimeDeps = deps
	b.BTResult.reportDeps = reportDepsFromRuntime(deps)
	if runtimeDepsErr := validateBacktestRuntimeDeps(deps, symbols); runtimeDepsErr != nil {
		b.runErr = runtimeDepsErr
		return b
	}
	if dataDeps.IdentityErr != nil {
		b.runErr = errs.New(core.ErrRunTime, dataDeps.IdentityErr)
		return b
	}
	wallets := biz.InitFakeWalletsWithRuntimeDeps(*deps)
	b.TotalInvest = wallets.TotalLegal(nil, false)
	if onBar == nil {
		onBar = func(evt *orm.DataSeries) {
			b.FeedDataSeries(evt)
		}
	}
	provider, providerErr := data.NewHistProviderWithRuntimeDeps(dataDeps, onBar, b.OnEnvEnd, getEnd, !isOpt, pBar)
	if providerErr != nil {
		b.runErr = providerErr
		return b
	}
	b.dp = provider
	b.dp.SetAllowDownload(backtestKlineDownloadAllowed(isOpt, deps))
	deps.Core.OnExit(b.dp.Terminate)
	pairHooks := strat.PairUpdateHooks{
		SubWarmPairs:  b.dp.SubWarmPairs,
		SymbolState:   symbols,
		Core:          deps.Core,
		StrategyState: deps.Strategies,
		LookupSymbol: func(pair string) (*orm.ExSymbol, *errs.Error) {
			if symbols == nil {
				return nil, errs.NewMsg(core.ErrRunTime, "runtime symbol state is required to resolve %s", pair)
			}
			return symbols.GetExSymbolCur(pair)
		},
		ExitOrders: func(acc string, orders []*ormo.InOutOrder, req *strat.ExitReq) *errs.Error {
			return biz.GetOdMgrWithState(deps.Trading, acc).ExitAndFill(orders, req)
		},
	}
	deps.Strategies.SetPairUpdateHooks(pairHooks)
	stopBacktest := deps.Core.Stop
	if isOpt {
		stopBacktest = b.dp.Terminate
	}
	biz.InitLocalOrderMgrWithRuntimeDeps(*deps, b.orderCB, !isOpt, stopBacktest)
	return b
}

func backtestKlineDownloadAllowed(isOpt bool, deps *biz.RuntimeDeps) bool {
	if isOpt || deps == nil {
		return false
	}
	cfg := deps.ConfigView()
	return cfg == nil || !cfg.BTNoKlineDownload
}

func (b *BackTestLite) FeedDataSeries(evt *orm.DataSeries) bool {
	if b == nil || evt == nil {
		return false
	}
	if deps := b.RuntimeDependencies(); deps != nil && deps.Core != nil {
		select {
		case <-deps.Core.Done():
			return false
		default:
		}
	}
	b.SetTimeMS(seriesEndMS(evt))
	if orm.NormalizeSeriesSource(evt.Source) != orm.SeriesSourceKline || !evt.HasOHLCV() {
		if err := b.Trader.FeedDataSeries(evt); err != nil {
			b.Logger().Error("FeedDataSeries fail", zap.Int32("sid", evt.Sid), zap.Error(err))
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
			b.Logger().Warn(fmt.Sprintf("batch job exec fail, wait: %v", waitNum))
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
			b.Logger().Error("FeedDataSeries fail", zap.String("p", evt.Symbol()), zap.Error(errRun))
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
// without changing the process clock.
func (b *BackTestLite) SetTimeMS(timeMS int64) {
	b.Trader.SetTimeMS(timeMS)
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
		state.SetCheckWallets(check)
	}
}

func (b *BackTestLite) checkWallets() bool {
	if state := b.runtimeCore(); state != nil {
		return state.ShouldCheckWallets()
	}
	return false
}

func (b *BackTestLite) botRunning() bool {
	if state := b.runtimeCore(); state != nil {
		return state.IsBotRunning()
	}
	return false
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
	chargeOnBomb := false
	if cfg := b.runConfig(); cfg != nil {
		chargeOnBomb = cfg.ChargeOnBomb
	}
	if chargeOnBomb {
		deps := b.RuntimeDependencies()
		if deps == nil {
			b.setRunError(errs.NewMsg(core.ErrRunTime, "explicit runtime dependencies are required for liquidation"))
			return
		}
		wallets := biz.InitFakeWalletsWithRuntimeDeps(*deps, symbol)
		oldVal := wallets.TotalLegal(nil, false)
		newVal := wallets.TotalLegal(nil, false)
		b.TotalInvest += newVal - oldVal
		b.Logger().Warn(fmt.Sprintf("wallet %s BOMB at %s, reset wallet and continue..", symbol, date))
	} else {
		b.Logger().Warn(fmt.Sprintf("wallet %s BOMB at %s, exit", symbol, date))
		if !b.isOpt {
			if state := b.runtimeCore(); state != nil {
				state.Stop()
			}
		}
		b.dp.Terminate()
	}
}

func (b *BackTestLite) orderCB(order *ormo.InOutOrder, isEnter bool) {
	account := b.defaultAccount()
	var deps *biz.RuntimeDeps
	if b != nil {
		deps = b.RuntimeDependencies()
	}
	if deps == nil {
		b.setRunError(errs.NewMsg(core.ErrRunTime, "explicit runtime dependencies are required for order callback"))
		return
	}
	if deps.DefaultAccount != "" {
		account = deps.DefaultAccount
	}
	wallets := deps.Trading.Wallet(account)
	if isEnter {
		openNum := deps.Orders.OpenNum(account, ormo.InOutStatusPartEnter)
		if openNum > b.MaxOpenOrders {
			b.MaxOpenOrders = openNum
		}
	} else {
		if wallets == nil {
			b.setRunError(errs.NewMsg(core.ErrRunTime, "runtime wallet is required for order callback"))
			return
		}
		// 更新单笔开单金额
		wallets.TryUpdateStakePctAmt()
		cfg := b.runConfig()
		if cfg != nil && cfg.DrawBalanceOver > 0 {
			quoteLegal := wallets.AvaLegal(cfg.StakeCurrency)
			if quoteLegal > cfg.DrawBalanceOver {
				wallets.WithdrawLegal(quoteLegal-cfg.DrawBalanceOver, cfg.StakeCurrency)
			}
		}
	}
}

// NewBackTestWithRuntimeDeps constructs a backtest over one explicit runtime.
// Its symbol state and data-provider dependencies are projected from deps.
func NewBackTestWithRuntimeDeps(deps biz.RuntimeDeps, isOpt bool, outDir string) (*BackTest, *errs.Error) {
	if err := biz.BindRuntimeDeps(deps); err != nil {
		return nil, err
	}
	trader, err := biz.NewTraderWithRuntimeDeps(deps)
	if err != nil {
		return nil, err
	}
	bound := trader.RuntimeDependencies()
	dataDeps := bound.DataDeps()
	if dataDeps.IdentityErr != nil {
		return nil, errs.New(core.ErrRunTime, dataDeps.IdentityErr)
	}
	return newBackTest(trader, bound.Symbols, isOpt, outDir, dataDeps)
}

func newBackTest(trader biz.Trader, symbols *orm.SymbolState, isOpt bool, outDir string, dataDeps *data.RuntimeDeps) (*BackTest, *errs.Error) {
	deps := trader.RuntimeDependencies()
	if deps == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "explicit runtime dependencies are required for backtest")
	}
	if err := validateBacktestRuntimeDeps(deps, deps.Symbols); err != nil {
		return nil, err
	}
	stages := []string{"init", "listMs", "loadPairs", "tfScores", "loadJobs", "warmJobs", "downKline", "runBT"}
	stgWeis := []float64{1, 1, 2, 2, 1, 2, 10, 10}
	b := &BackTest{
		PBar: utils.NewStagedPrg(stages, stgWeis),
	}
	getEnd := func() int64 {
		if b.nextRefresh > 0 {
			return b.nextRefresh
		}
		runRange := b.runTimeRange()
		if runRange == nil {
			return 0
		}
		return runRange.EndMS
	}
	b.BackTestLite = newBackTestLite(trader, symbols, isOpt, b.FeedDataSeries, getEnd, b.PBar, dataDeps)
	if b.BackTestLite.runErr != nil {
		return nil, b.BackTestLite.runErr
	}
	if outDir == "" && !isOpt {
		cfg := b.runConfig()
		if cfg == nil {
			return nil, errs.NewMsg(core.ErrBadConfig, "backtest runtime config is required")
		}
		hash, err := cfg.HashCode()
		if err != nil {
			return nil, err
		}
		snapshot := b.runSnapshot()
		if snapshot == nil {
			return nil, errs.NewMsg(core.ErrBadConfig, "backtest runtime configuration is required")
		}
		baseDir := filepath.Join(snapshot.DataDir, "backtest", hash)
		allocatedDir, outputErr := config.AllocateOutputDir(baseDir)
		if outputErr != nil {
			return nil, errs.New(core.ErrIOWriteFail, outputErr)
		}
		outDir = allocatedDir
		b.outputOwned = true
	}
	snapshot := b.runSnapshot()
	if snapshot == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "backtest runtime configuration is required")
	}
	b.OutDir = snapshot.ParsePath(outDir)
	if cfg := b.runConfig(); cfg != nil {
		config.LoadPerfsWithCoreState(snapshot.DataDir, b.runtimeCore(), cfg.StratPerf)
	}
	return b, nil
}

func (b *BackTest) Init() *errs.Error {
	if b != nil && b.BackTestLite != nil && b.BackTestLite.runErr != nil {
		// Runtime dependency binding errors are recorded by the lite runner so
		// its constructor remains source-compatible. Surface them before any
		// listing, provider, or adapter operation can run.
		return b.BackTestLite.runErr
	}
	cfg := b.runConfig()
	runRange := b.runTimeRange()
	if cfg == nil || runRange == nil {
		return errs.NewMsg(core.ErrBadConfig, "backtest runtime config and time range are required")
	}
	if err := validateBacktestRuntimeDeps(b.RuntimeDependencies(), b.symbols); err != nil {
		return err
	}
	b.SetTimeMS(runRange.StartMS)
	b.historicalCloseMS = historicalCloseBoundaries(cfg.HistoricalCoverage, runRange)
	b.historicalCloseIndex = 0
	b.MinReal = math.MaxFloat64
	snapshot := b.runSnapshot()
	if snapshot == nil {
		return errs.NewMsg(core.ErrBadConfig, "backtest runtime configuration is required")
	}
	dataDir := snapshot.DataDir
	b.Logger().Info("backtest config summary",
		zap.Bool("questdb", b.runtimeQuestDB()),
		zap.String("data_dir", dataDir),
		zap.String("timerange", cfg.TimeRangeRaw),
		zap.String("time_start", cfg.TimeStart),
		zap.String("time_end", cfg.TimeEnd),
		zap.Int("pair_count", len(cfg.Pairs)),
		zap.Int("run_policy_count", len(cfg.RunPolicy)),
		zap.Int("pair_filter_count", len(cfg.PairFilters)),
		zap.Int("run_tf_count", len(cfg.RunTimeframes)),
		zap.Float64("stake_amount", cfg.StakeAmount),
		zap.Float64("bt_net_cost", cfg.BTNetCost),
		zap.Int("order_bar_max", cfg.OrderBarMax),
		zap.String("out_dir", b.OutDir))
	if b.OutDir != "" {
		err_ := os.MkdirAll(b.OutDir, 0755)
		if err_ != nil {
			return errs.New(core.ErrIOWriteFail, err_)
		}
	}
	deps := b.RuntimeDependencies()
	if deps == nil || deps.Core == nil {
		return errs.NewMsg(core.ErrRunTime, "runtime core is required for backtest initialization")
	}
	startAt, endAt := int64(0), int64(0)
	if deps.Config != nil {
		if cfg := deps.Config.View(); cfg != nil && cfg.TimeRange != nil {
			startAt, endAt = cfg.TimeRange.StartMS, cfg.TimeRange.EndMS
		}
	}
	if startAt == 0 {
		startAt = deps.Core.StartAt
	}
	err := ormo.InitTaskWithState(deps.Orders, deps.DefaultAccount, deps.Core.RunMode, startAt, endAt, !b.isOpt)
	if err != nil {
		return err
	}
	err = b.initTaskOut()
	if err != nil {
		return err
	}
	b.PBar.SetProgress("init", 1)
	if b.symbols == nil || deps.Exchange == nil {
		return errs.NewMsg(core.ErrRunTime, "runtime symbols and exchange are required for listing dates")
	}
	err = orm.InitListDatesWithExchange(b.symbols, deps.Exchange)
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
	jobs, collectErr := collectBacktestJobsForBacktest(b)
	if collectErr != nil {
		return nil, collectErr
	}
	seriesRuntime := b.thirdPartyRuntime()
	var plan *data.SeriesPlan
	var err error
	plan, err = backtestBootstrapPlanWithCatalog(seriesRuntime.Catalog, jobs, b.runTimeRange())
	if err != nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "%v", err)
	}
	if err := seriesRuntime.Ensure(b.backtestSeriesContext(), plan); err != nil {
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
		var catalog *data.DataSourceCatalog
		if b.dp != nil {
			catalog = b.dp.DataSourceCatalog()
		}
		b.seriesRuntime = data.NewSeriesRuntimeWithCatalog(catalog, nil)
		if deps := b.RuntimeDependencies(); deps != nil {
			b.seriesRuntime.Repo = orm.NewSeriesRepo(deps.Storage)
		}
	}
	return b.seriesRuntime
}

func (b *BackTest) runtimeQuestDB() bool {
	if b != nil && b.RuntimeDependencies() != nil {
		deps := b.RuntimeDependencies()
		return deps.Storage != nil && deps.Storage.IsQuestDB()
	}
	return false
}

func (b *BackTest) backtestSeriesContext() context.Context {
	if b != nil && b.BackTestLite != nil && b.RuntimeDependencies() != nil {
		if deps := b.RuntimeDependencies(); deps.Core != nil && deps.Core.Context() != nil {
			return deps.Core.Context()
		}
	}
	return context.Background()
}

func collectBacktestJobsForBacktest(b *BackTest) ([]*strat.StratJob, *errs.Error) {
	if b == nil || b.BackTestLite == nil || b.RuntimeDependencies() == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "explicit runtime dependencies are required for backtest series")
	}
	deps := b.RuntimeDependencies()
	if deps.Strategies == nil || strat.IsLegacyState(deps.Strategies) {
		return nil, errs.NewMsg(core.ErrRunTime, "runtime strategy state is required for backtest series")
	}
	return collectBacktestJobsFromMap(deps.Strategies.JobMapsView(deps.DefaultAccount)), nil
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

func backtestBootstrapPlanWithCatalog(catalog *data.DataSourceCatalog, jobs []*strat.StratJob, tr *config.TimeTuple) (*data.SeriesPlan, error) {
	if tr == nil {
		return nil, fmt.Errorf("bootstrap collect phase=collect: time range is required")
	}
	if tr.StartMS >= tr.EndMS {
		return nil, fmt.Errorf("bootstrap collect phase=collect: invalid time range")
	}
	if catalog == nil || catalog == data.LegacyDataSourceCatalog() {
		return nil, fmt.Errorf("bootstrap collect phase=collect: explicit data source catalog is required")
	}
	return data.NewSeriesPlanWithCatalog(catalog, jobs, tr.StartMS, tr.EndMS)
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
		account := b.defaultAccount()
		odNum := 0
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
			b.Logger().Error("RefreshPairJobs", zap.String("date", dateStr), zap.Error(err))
			if b.dataPrep {
				b.dataPrepErr = err
			}
			b.setRunError(err)
			return
		} else {
			if _, err := b.syncThirdPartySeriesRange(); err != nil {
				b.Logger().Error("ensure third-party series after pair refresh", zap.String("date", dateStr), zap.Error(err))
				if b.dataPrep {
					b.dataPrepErr = err
				}
				b.setRunError(err)
				return
			}
			b.Logger().Info("refreshed pairs at", zap.String("date", dateStr))
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
		account := b.defaultAccount()
		deps := b.RuntimeDependencies()
		if deps == nil {
			return errs.NewMsg(core.ErrRunTime, "explicit runtime dependencies are required for historical cleanup")
		}
		if err := biz.CloseBacktestOrdersAtWithState(deps.Trading, account, b.historicalCloseMS[b.historicalCloseIndex]); err != nil {
			return err
		}
		b.historicalCloseIndex++
	}
	return nil
}

func (b *BackTest) Run() *errs.Error {
	completed := false
	defer func() {
		if !completed && b != nil && b.outputOwned && b.OutDir != "" {
			_ = os.RemoveAll(b.OutDir)
		}
	}()
	err := b.initRefreshCron()
	if err != nil {
		b.Logger().Error("init pair cron fail", zap.Error(err))
		return err
	}
	err = b.Init()
	if err != nil {
		b.Logger().Error("backtest init fail", zap.Error(err))
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
		b.stopRunScheduler()
	}
	if err != nil {
		b.Logger().Error("backtest loop fail", zap.Error(err))
		return err
	}
	// Some feeders finish without emitting a bar at or after a historical
	// cutoff (for example when every series ends at the old baseline). Ensure
	// those positions are closed before the final cleanup uses the new end.
	if err := b.closeHistoricalBoundaries(math.MaxInt64); err != nil {
		b.Logger().Error("close historical boundaries fail", zap.Error(err))
		return err
	}
	btCost := btime.UTCTime() - btStart
	account := b.defaultAccount()
	deps := b.RuntimeDependencies()
	if deps == nil {
		return errs.NewMsg(core.ErrRunTime, "explicit runtime dependencies are required for backtest")
	}
	if deps.DefaultAccount != "" {
		account = deps.DefaultAccount
	}
	odMgr := biz.GetOdMgrWithState(deps.Trading, account)
	wallets := deps.Trading.Wallet(account)
	if odMgr == nil {
		return errs.NewMsg(core.ErrRunTime, "backtest order manager is not initialized")
	}
	err = odMgr.CleanUp()
	if deps.Strategies != nil {
		strat.ExitStratJobsWithState(deps.Strategies)
	}
	if err != nil {
		b.Logger().Error("backtest clean orders fail", zap.Error(err))
		return err
	}
	if b.dataPrep {
		completed = true
		return nil
	}
	if wallets == nil {
		return errs.NewMsg(core.ErrRunTime, "runtime wallet is required for backtest")
	}
	b.logPlot(wallets, b.TimeMS(), -1, -1)
	b.normalizeBacktestResultRange()
	if err := b.Collect(); err != nil {
		b.Logger().Error("backtest report collect fail", zap.Error(err))
		return err
	}
	b.runAfterBacktestCallback()
	if !b.isOpt {
		b.Logger().Info(fmt.Sprintf("Complete! cost: %.1fs, avg: %.1f bar/s", btCost, float64(b.BarNum)/btCost))
		var failOpens string
		if deps.Strategies != nil {
			failOpens = strat.DumpAccFailOpensWithState(deps.Strategies)
		}
		if failOpens != "" {
			b.Logger().Info("fail open tag nums:\n" + failOpens)
		}
		b.printBtResult(true)
	}
	completed = true
	return nil
}

func (b *BackTest) normalizeBacktestResultRange() {
	if b == nil {
		return
	}
	normalizeBacktestResultRangeForTimeRange(b.BTResult, b.stoppedEarly, b.runTimeRange())
}

func normalizeBacktestResultRangeForTimeRange(result *BTResult, stoppedEarly bool, runRange *config.TimeTuple) {
	if stoppedEarly {
		return
	}
	if result == nil || runRange == nil || runRange.StartMS <= 0 ||
		runRange.EndMS <= runRange.StartMS {
		return
	}
	result.StartMS = runRange.StartMS
	result.EndMS = runRange.EndMS
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
	account := b.defaultAccount()
	_, ok := b.accountConfigs()[account]
	if !ok {
		return errs.NewMsg(core.ErrBadConfig, "default account %q is invalid", account)
	}
	cfg := b.runConfig()
	if cfg != nil && cfg.StakePct > 0 {
		b.Logger().Warn("stake_amt may result in inconsistent order amounts with each backtest!")
	}
	return nil
}

func (b *BackTest) cronDumpBtStatus() {
	if b.strictBacktest() {
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
		b.Logger().Info("dump backTest status to files...")
		if err := b.Collect(); err != nil {
			b.Logger().Error("dump backTest status collect fail", zap.Error(err))
			return
		}
		b.printBtResult(false)
	})
	if err_ != nil {
		b.Logger().Error("add Dump BackTest Status fail", zap.Error(err_))
	}
}

func (b *BackTest) schedulerForRun() com.Scheduler {
	if b == nil {
		return com.Cron()
	}
	b.schedulerMu.Lock()
	defer b.schedulerMu.Unlock()
	if b.runScheduler != nil {
		return b.runScheduler
	}
	if deps := b.RuntimeDependencies(); deps != nil {
		if deps.Scheduler != nil {
			b.runScheduler = deps.Scheduler
			// A scheduler supplied through RuntimeDeps belongs to its caller
			// (normally Runtime). BackTest may add jobs and start it, but must
			// leave shutdown to the scheduler owner so another runner sharing
			// the Runtime cannot be stopped early.
			b.runSchedulerOwned = false
			return b.runScheduler
		}
		location := time.UTC
		lang := ""
		if deps.Config != nil {
			location = deps.Config.Location()
			if cfg := deps.Config.View(); cfg != nil {
				lang = cfg.NTPLangCode
			}
		}
		// Keep the fallback private to this BackTest. Run, status-dump, and
		// cleanup must all address the same scheduler instance.
		b.runScheduler = com.NewSchedulerWithConfig(location, lang)
		b.runSchedulerOwned = true
		return b.runScheduler
	}
	b.runScheduler = com.NewSchedulerWithConfig(time.UTC, "")
	b.runSchedulerOwned = true
	return b.runScheduler
}

func (b *BackTest) ownsRunScheduler() bool {
	if b == nil {
		return false
	}
	b.schedulerMu.Lock()
	defer b.schedulerMu.Unlock()
	return b.runSchedulerOwned
}

func (b *BackTest) stopRunScheduler() {
	if b == nil || !b.ownsRunScheduler() {
		return
	}
	if scheduler := b.schedulerForRun(); scheduler != nil {
		stop := scheduler.Stop()
		if stop != nil && stop.Done() != nil {
			<-stop.Done()
		}
	}
}

func (b *BackTest) initRefreshCron() *errs.Error {
	cfg := b.runConfig()
	runRange := b.runTimeRange()
	if cfg == nil || cfg.PairMgr == nil || cfg.PairMgr.Cron == "" {
		return nil
	}
	if runRange == nil {
		return errs.NewMsg(core.ErrBadConfig, "backtest time range is required for pair refresh")
	}
	if cfg.PairMgr.Cron != "" {
		var err_ error
		b.schedule, err_ = runtimeCronSchedule(cfg.PairMgr.Cron, b.RuntimeDependencies())
		if err_ != nil {
			return errs.New(core.ErrBadConfig, err_)
		}
		baseMS := runRange.StartMS
		for {
			baseTime := time.UnixMilli(baseMS)
			b.nextRefresh = b.schedule.Next(baseTime).UnixMilli()
			if b.nextRefresh-baseMS > runtimeMinPairCronGapMS {
				break
			}
			baseMS = b.nextRefresh
		}
	}
	return nil
}

// refreshPairJobsWithRuntimeDeps refreshes jobs and relay state for one owner.
func refreshPairJobsWithRuntimeDeps(dp data.IProvider, symbols *orm.SymbolState, deps *biz.RuntimeDeps, showLog, isFirst bool, pBar *utils.StagedPrg) *errs.Error {
	if deps == nil {
		return errs.NewMsg(core.ErrRunTime, "explicit runtime dependencies are required for pair refresh")
	}
	if symbols == nil {
		symbols = deps.Symbols
	}
	if err := validateBacktestRuntimeDeps(deps, symbols); err != nil {
		return err
	}
	if dp == nil {
		return errs.NewMsg(core.ErrRunTime, "backtest data provider is required for pair refresh")
	}
	cfg := runtimeConfigForDeps(deps)
	runRange := runtimeTimeRangeForDeps(deps)
	curTime := deps.Clock.TimeMS()
	envReal := deps.Core.EnvReal
	if isFirst {
		if cfg != nil && cfg.PairMgr != nil && cfg.PairMgr.Cron != "" {
			schedule, err_ := runtimeCronSchedule(cfg.PairMgr.Cron, deps)
			if err_ != nil {
				return errs.New(errs.CodeRunTime, err_)
			}
			curTime = utils.CronAlign(schedule, btime.ToTime(curTime)).UnixMilli()
		} else if !envReal && cfg != nil && cfg.PairMgr != nil && cfg.PairMgr.UseLatest && runRange != nil {
			// 回测时配置use_latest=true，且cron为空，则使用最新时间刷新交易品种
			curTime = min(curTime, runRange.EndMS)
		}
	}
	var pairs []string
	var pairTfScores map[string]map[string]float64
	var err *errs.Error
	pairs, pairTfScores, err = biz.RefreshPairsWithRuntimeDeps(deps, showLog, curTime, pBar)
	if err != nil {
		return err
	}
	syncRuntimePairsWithConfig(deps.Core, pairs, cfg)
	// store the currently running jobs and mark them as prohibited from running
	// 获取旧的已运行一段时间的任务（在刷新任务前运行），标记为禁止运行
	forbidJobs := deps.Strategies.JobKeysAll()
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
	err = relayUnFinishOrdersWithDeps(pairTfScores, forbidJobs, isFirst, symbols, deps)
	if err != nil {
		return err
	}
	symbols.ResetSubSymbol()
	// warm up for new symbols
	return dp.SubWarmPairs(warms, true)
}

func validateBacktestRuntimeDeps(deps *biz.RuntimeDeps, symbols *orm.SymbolState) *errs.Error {
	if deps == nil {
		return errs.NewMsg(core.ErrRunTime, "explicit runtime dependencies are required for backtest")
	}
	if deps.Core == nil || deps.Clock == nil || deps.Market == nil || deps.Strategies == nil ||
		deps.Orders == nil || deps.Trading == nil || deps.Config == nil || deps.Accounts == nil ||
		deps.Symbols == nil || symbols == nil || deps.Exchange == nil {
		return errs.NewMsg(core.ErrRunTime, "backtest runtime dependencies are incomplete")
	}
	return nil
}

func syncRuntimePairsWithConfig(state *core.State, pairs []string, cfg *config.Config) {
	if state == nil {
		return
	}
	additionalAllowed := make([]string, 0)
	if cfg == nil {
		state.SetPairs(pairs, additionalAllowed)
		return
	}
	for _, policy := range cfg.RunPolicy {
		additionalAllowed = append(additionalAllowed, policy.Pairs...)
	}
	state.SetPairs(pairs, additionalAllowed)
}

/*
获取模拟回测的未完成订单，接力入场；
应在RefreshJobs之后再调用，否则入场订单可能被视为旧的平仓掉
*/

func relayUnFinishOrdersWithDeps(pairTfScores map[string]map[string]float64, forbidJobs map[string]map[string]bool,
	isFirst bool, symbols *orm.SymbolState, deps *biz.RuntimeDeps) *errs.Error {
	if deps == nil {
		return errs.NewMsg(core.ErrRunTime, "relay requires explicit runtime dependencies")
	}
	cfg := deps.ConfigView()
	if cfg == nil || !cfg.RelaySimUnFinish {
		return nil
	}
	if deps.Core == nil || deps.Clock == nil || deps.Strategies == nil || deps.Orders == nil || deps.Trading == nil {
		return errs.NewMsg(core.ErrRunTime, "runtime relay requires core, clock, strategy, order, and trading state")
	}
	simEndMS := deps.Clock.TimeMS()
	relayOpens := make(map[string]*ormo.InOutOrder)
	relayDones := make(map[string]*ormo.InOutOrder)
	for _, group := range strat.RelayPolicyGroupsWithState(deps.Strategies, symbols) {
		if group == nil || len(group.Policies) == 0 || group.StartMS >= simEndMS {
			continue
		}
		tempDeps, lite, cleanup, err := newRelayRuntime(*deps, symbols, group, simEndMS, forbidJobs, pairTfScores)
		if err != nil {
			return err
		}
		warms, _, loadErr := strat.LoadStratJobsWithState(tempDeps.Strategies, tempDeps.Core, symbols,
			tempDeps.Core.AdmissionPairs(), pairTfScores, tempDeps.Orders)
		if loadErr != nil {
			cleanup()
			return loadErr
		}
		if len(warms) > 0 {
			if warmErr := lite.dp.SubWarmPairs(warms, true); warmErr != nil {
				cleanup()
				return warmErr
			}
			if runErr := lite.resolveLoopError(lite.dp.LoopMain()); runErr != nil {
				cleanup()
				return runErr
			}
		}
		collectRelayOrders(tempDeps.Orders, tempDeps.DefaultAccount, relayOpens, relayDones)
		cleanup()
	}
	return syncSimOrdersWithDeps(deps, isFirst, relayOpens, relayDones)
}

func newRelayRuntime(parent biz.RuntimeDeps, symbols *orm.SymbolState, group *strat.PolicyGroup,
	simEndMS int64, forbidJobs map[string]map[string]bool, pairTfScores map[string]map[string]float64,
) (biz.RuntimeDeps, *BackTestLite, func(), *errs.Error) {
	state, err := core.NewState(parent.Core.Context())
	if err != nil {
		return biz.RuntimeDeps{}, nil, func() {}, errs.New(core.ErrRunTime, err)
	}
	state.Logger = parent.Core.Log()
	state.SetRunMode(core.RunModeBackTest)
	state.SetRunEnv(parent.Core.RunEnv)
	state.StartAt = group.StartMS
	state.ExgName, state.Market, state.ContractType = parent.Core.ExgName, parent.Core.Market, parent.Core.ContractType
	state.IsContract = parent.Core.IsContract
	state.NetDisable = parent.Core.NetDisable
	state.ParallelOnBar = parent.Core.ParallelOnBar
	state.NumTaCache, state.ConcurNum = parent.Core.NumTaCache, parent.Core.ConcurNum
	state.SetPairs(parent.Core.AdmissionPairs(), nil)
	clock := btime.NewClockState(true, parent.Config.Location())
	clock.SetTimeMS(group.StartMS)
	market := com.NewMarketStateWithExchange(state.ExgName, parent.Exchange)
	configSnapshot := parent.Config.Clone()
	runConfig := configSnapshot.View()
	runConfig.TimeRange = &config.TimeTuple{StartMS: group.StartMS, EndMS: simEndMS}
	runConfig.RunPolicy = make([]*config.RunPolicyConfig, len(group.Policies))
	for i, policy := range group.Policies {
		if policy != nil {
			runConfig.RunPolicy[i] = policy.Clone()
		}
	}
	strategyState := strat.NewStateWithRuntime(state, clock, runConfig, symbols, parent.Exchange)
	orderState := ormo.NewOrderState()
	tradingState := biz.NewTradingState()
	parent.AccountsMu.RLock()
	accounts := config.CloneAccountConfigsForRuntime(parent.AccountConfigs())
	parent.AccountsMu.RUnlock()
	accountsMu := &sync.RWMutex{}
	defaultAccount := parent.DefaultAccount
	if defaultAccount == "" {
		defaultAccount = configSnapshot.DefaultAccount()
	}
	tempDeps := biz.RuntimeDeps{
		Core: state, Clock: clock, Market: market, Batch: strat.NewBatchState(),
		Strategies: strategyState, Orders: orderState, Trading: tradingState,
		Config: configSnapshot, Accounts: accounts, AccountsMu: accountsMu, DefaultAccount: defaultAccount,
		Symbols: symbols, Storage: parent.Storage, Exchange: parent.Exchange,
		Scheduler: parent.Scheduler, Catalog: parent.Catalog, Callbacks: parent.Callbacks,
	}
	if bindErr := biz.BindRuntimeDeps(tempDeps); bindErr != nil {
		state.Close()
		return biz.RuntimeDeps{}, nil, func() {}, bindErr
	}
	trader, traderErr := biz.NewTraderWithRuntimeDeps(tempDeps)
	if traderErr != nil {
		state.Close()
		return biz.RuntimeDeps{}, nil, func() {}, traderErr
	}
	tempDeps = *trader.RuntimeDependencies()
	if err := ormo.InitTasksWithState(tempDeps.Orders, runtimeAccountNames(tempDeps.AccountConfigs()),
		core.RunModeBackTest, group.StartMS, simEndMS, false); err != nil {
		state.Close()
		return biz.RuntimeDeps{}, nil, func() {}, err
	}
	strategyState.SetForbidJobs(forbidJobs)

	lite := newBackTestLite(trader, symbols, true, nil, nil, nil, tempDeps.DataDeps())
	cleanup := func() {
		if lite != nil && lite.dp != nil {
			lite.dp.Terminate()
		}
		state.Close()
	}
	return tempDeps, lite, cleanup, nil
}

func runtimeAccountNames(accounts map[string]*config.AccountConfig) []string {
	result := make([]string, 0, len(accounts))
	for account := range accounts {
		result = append(result, account)
	}
	sort.Strings(result)
	return result
}

func collectRelayOrders(state *ormo.OrderState, account string, opens, dones map[string]*ormo.InOutOrder) {
	if state == nil {
		return
	}
	openOrders, lock := state.GetOpenODs(account)
	lock.Lock()
	for _, order := range openOrders {
		if order == nil {
			continue
		}
		if order.Status >= ormo.InOutStatusPartEnter && order.ExitTag == "" {
			opens[order.KeyAlign()] = order
		} else if order.Status >= ormo.InOutStatusFullExit {
			dones[order.KeyAlign()] = order
		}
	}
	lock.Unlock()
	for _, order := range state.HistoricalOrders() {
		if order != nil {
			dones[order.KeyAlign()] = order
		}
	}
}

func syncSimOrdersWithDeps(deps *biz.RuntimeDeps, isFirst bool, relayOpens, relayDones map[string]*ormo.InOutOrder) *errs.Error {
	if deps == nil || deps.Orders == nil || deps.Trading == nil || deps.Strategies == nil {
		return errs.NewMsg(core.ErrRunTime, "runtime relay state is incomplete")
	}
	accounts := deps.AccountConfigs()
	strict := deps.StrictBacktest()
	if isFirst {
		closeNums := make(map[string]int)
		for _, account := range runtimeAccountNames(accounts) {
			cfg := accounts[account]
			if cfg == nil || cfg.NoTrade {
				continue
			}
			manager := biz.GetOdMgrWithState(deps.Trading, account)
			if manager == nil {
				continue
			}
			openOrders, lock := deps.Orders.GetOpenODs(account)
			var exits []*ormo.InOutOrder
			lock.Lock()
			for _, order := range openOrders {
				if _, ok := relayDones[order.KeyAlign()]; ok {
					exits = append(exits, order)
				}
			}
			lock.Unlock()
			if len(exits) == 0 {
				continue
			}
			if err := manager.ExitAndFill(exits, &strat.ExitReq{Tag: core.ExitTagExitDelay}); err != nil {
				return err
			}
			closeNums[account] = len(exits)
		}
		if len(closeNums) > 0 {
			deps.Logger().Info("closed delayed order", zap.Any("nums", closeNums))
		}
	}
	if len(relayOpens) == 0 {
		return nil
	}
	for _, account := range runtimeAccountNames(accounts) {
		cfg := accounts[account]
		if cfg == nil || cfg.NoTrade {
			continue
		}
		manager := biz.GetOdMgrWithState(deps.Trading, account)
		if manager == nil {
			continue
		}
		jobs := deps.Strategies.JobMapsView(account)
		openOrders, lock := deps.Orders.GetOpenODs(account)
		current := make(map[string]*ormo.InOutOrder, len(openOrders))
		lock.Lock()
		for _, order := range openOrders {
			current[order.KeyAlign()] = order
		}
		lock.Unlock()
		allowed := make([]*ormo.InOutOrder, 0, len(relayOpens))
		for _, key := range sortedMapKeys(relayOpens, strict) {
			order := relayOpens[key]
			if _, exists := current[key]; exists {
				continue
			}
			if strategyJobs := jobs[fmt.Sprintf("%s_%s", order.Symbol, order.Timeframe)]; strategyJobs != nil {
				if job := strategyJobs[order.Strategy]; job != nil {
					job.AddOrderCount(1)
					allowed = append(allowed, order)
				}
			}
		}
		if len(allowed) > 0 {
			if err := manager.RelayOrders(allowed); err != nil {
				return err
			}
		}
	}
	return nil
}

func sortedMapKeys(items map[string]*ormo.InOutOrder, strict bool) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	if strict {
		sort.Strings(keys)
	}
	return keys
}
