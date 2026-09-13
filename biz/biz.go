package biz

import (
	"context"
	"embed"
	_ "embed"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/banbox/banbot/com"

	"github.com/sasha-s/go-deadlock"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/goods"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	ta "github.com/banbox/banta"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

//go:embed config.yml
var configData []byte

//go:embed config.local.yml
var configLocalData []byte

//go:embed zh-CN/*
var zhCNData embed.FS

//go:embed en-US/*
var enUSData embed.FS

func SetupComs(args *config.CmdArgs) *errs.Error {
	args.Init()
	if core.LiveMode {
		// 实时模式下启用死锁检测
		deadlock.Opts.Disable = false
	}
	errs.PrintErr = utils.PrintErr
	ctx, cancel := context.WithCancel(context.Background())
	core.Ctx = ctx
	core.StopAll = cancel
	err := InitDataDir()
	if err != nil {
		return err
	}
	err = config.LoadConfig(args)
	if err != nil {
		return err
	}
	var logCores []zapcore.Core
	if core.LiveMode {
		logCores = append(logCores, rpc.NewExcNotify())
		if args.Logfile == "" {
			args.Logfile = filepath.Join(config.GetLogsDir(), config.Name+".log")
		}
	}
	args.SetLog(true, logCores...)
	err = core.Setup()
	if err != nil {
		return err
	}
	if config.Mail != nil && config.Mail.Enable {
		c := config.Mail
		utils.SetMailSender(c.Host, c.Port, c.Username, c.Password)
	}
	config.LoadLangMessages()
	err = utils.Setup()
	if err != nil {
		return err
	}
	err = exg.Setup()
	if err != nil {
		return err
	}
	err = orm.SetupWithAutoCompact(args.AutoCompact)
	if err != nil {
		return err
	}
	err = goods.Setup()
	if err != nil {
		return err
	}
	return nil
}

func SetupComsExg(args *config.CmdArgs) *errs.Error {
	err := SetupComs(args)
	if err != nil {
		return err
	}
	return orm.InitExg(exg.Default)
}

var refreshPairsCache struct {
	timeMS       int64
	pairs        []string
	pairTfScores map[string]map[string]float64
	corePairs    []string
	pairsMap     map[string]bool
}

func RefreshPairs(showLog bool, timeMS int64, pBar *utils.StagedPrg) ([]string, map[string]map[string]float64, *errs.Error) {
	return RefreshPairsWithSymbolState(nil, showLog, timeMS, pBar)
}

// RefreshPairsWithSymbolState keeps symbol discovery and timeframe scoring on
// one runtime's symbol state. The legacy refresh cache is intentionally used
// only by the legacy facade because it also restores process-global core pairs.
func RefreshPairsWithSymbolState(symbols *orm.SymbolState, showLog bool, timeMS int64, pBar *utils.StagedPrg) ([]string, map[string]map[string]float64, *errs.Error) {
	if symbols == nil {
		return refreshPairsLegacy(showLog, timeMS, pBar)
	}
	// A symbol catalog alone does not identify the exchange, configuration,
	// strategy registry, or pair admission state that owns a refresh. Refuse
	// this ambiguous compatibility path instead of combining one Runtime's
	// symbols with process-global dependencies.
	return nil, nil, errs.NewMsg(core.ErrBadConfig,
		"explicit pair refresh requires complete runtime dependencies")
}

func refreshPairsLegacy(showLog bool, timeMS int64, pBar *utils.StagedPrg) ([]string, map[string]map[string]float64, *errs.Error) {
	c := &refreshPairsCache
	if core.BackTestMode && c.timeMS == timeMS && c.pairTfScores != nil {
		core.ReplaceLegacyPairState(c.corePairs, c.pairsMap)
		if pBar != nil {
			pBar.SetProgress("loadPairs", 1)
			pBar.SetProgress("tfScores", 1)
		}
		return c.pairs, c.pairTfScores, nil
	}
	goods.ShowLog = showLog
	pairs, err := goods.RefreshPairList(timeMS)
	if err != nil {
		return nil, nil, err
	}
	if pBar != nil {
		pBar.SetProgress("loadPairs", 1)
	}
	allPairs := make([]string, 0, len(pairs))
	allPairs = append(allPairs, pairs...)
	for _, r := range config.RunPolicy {
		if len(r.Pairs) > 0 {
			allPairs = append(allPairs, r.Pairs...)
		}
	}
	allPairs, _ = utils.UniqueItems(allPairs)
	pairTfScores, err := strat.CalcPairTfScores(exg.Default, allPairs)
	if err != nil {
		return nil, nil, err
	}
	if pBar != nil {
		pBar.SetProgress("tfScores", 1)
	}
	if core.BackTestMode {
		c.timeMS = timeMS
		c.pairs = pairs
		c.pairTfScores = pairTfScores
		c.corePairs, c.pairsMap = core.LegacyPairStateSnapshot()
	}
	return pairs, pairTfScores, nil
}

func RefreshJobs(pairs []string, pairTfScores map[string]map[string]float64, showLog bool, pBar *utils.StagedPrg) (map[string]map[string]int, *errs.Error) {
	return RefreshJobsWithSymbolState(nil, pairs, pairTfScores, showLog, pBar)
}

// RefreshJobsWithSymbolState loads strategy jobs against the supplied symbol state.
func RefreshJobsWithSymbolState(symbols *orm.SymbolState, pairs []string, pairTfScores map[string]map[string]float64, showLog bool, pBar *utils.StagedPrg) (map[string]map[string]int, *errs.Error) {
	warms, accOds, err := strat.LoadStratJobsWithSymbolState(symbols, pairs, pairTfScores)
	if err != nil {
		return nil, err
	}
	if len(accOds) > 0 {
		for acc := range executionMapKeys(accOds) {
			odList := accOds[acc]
			odMgr := GetOdMgr(acc)
			err = odMgr.ExitAndFill(odList, &strat.ExitReq{Tag: core.ExitTagPairDel})
			if err != nil {
				return nil, err
			}
			strat.FinalizePairRotation(nil)
			log.Info("exit old orders as pair rotation", zap.Int("num", len(odList)))
		}
	}
	if showLog {
		strat.PrintStratGroups()
	}
	if pBar != nil {
		pBar.SetProgress("loadJobs", 1)
	}
	return warms, nil
}

// RefreshJobsWithRuntimeDeps is the explicit-runtime counterpart to
// RefreshJobsWithSymbolState. Every mutable registry used by job loading and
// pair-rotation exits comes from deps; the legacy function above remains the
// serialized compatibility path.
func RefreshJobsWithRuntimeDeps(deps *RuntimeDeps, symbols *orm.SymbolState, pairs []string,
	pairTfScores map[string]map[string]float64, showLog bool, pBar *utils.StagedPrg) (map[string]map[string]int, *errs.Error) {
	if deps == nil {
		return RefreshJobsWithSymbolState(symbols, pairs, pairTfScores, showLog, pBar)
	}
	warms, accOds, err := strat.LoadStratJobsWithState(deps.Strategies, deps.Core, symbols, pairs, pairTfScores, deps.Orders)
	if err != nil {
		return nil, err
	}
	if len(accOds) > 0 {
		for acc := range executionMapKeys(accOds, deps) {
			odList := accOds[acc]
			odMgr := GetOdMgrWithState(deps.Trading, acc)
			if odMgr == nil {
				return nil, errs.NewMsg(core.ErrRunTime, "order manager is required for pair rotation: %s", acc)
			}
			if err = odMgr.ExitAndFill(odList, &strat.ExitReq{Tag: core.ExitTagPairDel}); err != nil {
				return nil, err
			}
			strat.FinalizePairRotation(deps.Strategies, deps.Core)
			log.Info("exit old orders as pair rotation", zap.Int("num", len(odList)))
		}
	}
	if showLog {
		strat.PrintStratGroupsWithState(deps.Strategies, deps.Core)
	}
	if pBar != nil {
		pBar.SetProgress("loadJobs", 1)
	}
	return warms, nil
}

// RefreshPairsWithRuntimeDeps is the explicit pair-selection path. It keeps
// pair admission and ban timestamps on the runtime core instead of the legacy
// package registries.
func RefreshPairsWithRuntimeDeps(deps *RuntimeDeps, showLog bool, timeMS int64,
	pBar *utils.StagedPrg) ([]string, map[string]map[string]float64, *errs.Error) {
	if deps == nil {
		return RefreshPairsWithSymbolState(nil, showLog, timeMS, pBar)
	}
	if deps.Exchange == nil || deps.Symbols == nil {
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "runtime exchange and symbol state are required")
	}
	dataDir := ""
	if deps.Config != nil {
		dataDir = deps.Config.DataDir
	}
	pairs, err := goods.RefreshPairListWithRuntimeDeps(&goods.RuntimeDeps{
		Core: deps.Core, Clock: deps.Clock, Config: deps.ConfigView(), DataDir: dataDir, Storage: deps.Storage,
		Symbols: deps.Symbols, Exchange: deps.Exchange, ShowLog: showLog,
	}, timeMS)
	if err != nil {
		return nil, nil, err
	}
	if pBar != nil {
		pBar.SetProgress("loadPairs", 1)
	}
	allPairs := append([]string(nil), pairs...)
	if cfg := deps.ConfigView(); cfg != nil {
		for _, policy := range cfg.RunPolicy {
			if policy != nil {
				allPairs = append(allPairs, policy.Pairs...)
			}
		}
	}
	allPairs, _ = utils.UniqueItems(allPairs)
	pairTfScores, err := strat.CalcPairTfScoresWithState(deps.Strategies, deps.Symbols, deps.Exchange, allPairs)
	if err != nil {
		return nil, nil, err
	}
	if pBar != nil {
		pBar.SetProgress("tfScores", 1)
	}
	return pairs, pairTfScores, nil
}

/*
InitOdSubs 为所有策略OnOrderChange注册订单事件监听。

只需在LoadStratJobs后调用一次，交易的Accounts不变就始终生效
*/
func InitOdSubs() {
	// 这里只调用成员函数，不读取变量，所以每个策略只存储一个实例即可
	var subStgys = map[string]*strat.TradeStrat{}
	for _, items := range strat.PairStrats {
		for stgName, stagy := range items {
			if stagy.OnOrderChange != nil || stagy.HedgeOff {
				subStgys[stgName] = stagy
			}
		}
	}
	if len(subStgys) == 0 {
		return
	}
	strat.LockJobsRead()
	for acc := range executionMapKeys(strat.AccJobs) {
		strat.AddOdSub(acc, func(acc string, od *ormo.InOutOrder, evt int) {
			stgy, ok := subStgys[od.Strategy]
			if !ok {
				// The current strategy does not monitor order status
				// 当前策略未监听订单状态
				return
			}
			strat.LockJobsRead()
			items, _ := strat.AccJobs[acc]
			if len(items) == 0 {
				strat.UnlockJobsRead()
				return
			}
			pairTF := strings.Join([]string{od.Symbol, od.Timeframe}, "_")
			its, _ := items[pairTF]
			if len(its) == 0 {
				strat.UnlockJobsRead()
				return
			}
			job, _ := its[od.Strategy]
			strat.UnlockJobsRead()
			if job != nil {
				if core.LiveMode && !job.IsWarmUpState() {
					if err := com.RefreshLatestPrice(job.Symbol.Symbol); err != nil {
						log.Warn("refresh latest price fail", zap.String("pair", job.Symbol.Symbol), zap.Error(err))
					}
				}
				if stgy.HedgeOff && evt == strat.OdChgEnterFill {
					// 策略为单向持仓，成交时尝试关闭另一侧订单
					closeSideOrders(job, !od.Short)
				}
				if stgy.OnOrderChange != nil {
					if evt == strat.OdChgExitFill {
						openOds, lock := ormo.GetOpenODs(acc)
						lock.Lock()
						job.UpdateOrders(executionOpenOrders(openOds))
						lock.Unlock()
					}
					stgy.OnOrderChange(job, od, evt)
				}
				requests := job.ExecutionSnapshot()
				if len(requests.Entrys) > 0 || len(requests.Exits) > 0 {
					_, _, err := GetOdMgr(acc).ProcessOrders(job)
					if err != nil {
						log.Error("process orders fail", zap.Error(err))
					}
				}
				// A close-on-remove job remains in AccJobs until its terminal
				// callback so this route stays valid across pair rotation.
				strat.FinalizePairRotation(nil)
			}
		})
	}
	strat.UnlockJobsRead()
}

// InitOdSubsWithRuntimeDeps registers order callbacks against one explicit
// runtime. The callback closure captures typed state once, so order events do
// not discover a runtime through package globals.
func InitOdSubsWithRuntimeDeps(deps *RuntimeDeps) {
	if deps == nil {
		InitOdSubs()
		return
	}
	state := deps.Strategies
	if state == nil {
		return
	}
	var prices *com.PriceState
	if deps.Market != nil {
		prices = deps.Market.Prices
	}
	stateMap := state.PairStrategies()
	subStgys := make(map[string]*strat.TradeStrat)
	for _, items := range stateMap {
		for stgName, stgy := range items {
			if stgy.OnOrderChange != nil || stgy.HedgeOff {
				subStgys[stgName] = stgy
			}
		}
	}
	if len(subStgys) == 0 {
		return
	}
	for _, acc := range state.Accounts() {
		account := acc
		state.AddOdSub(account, func(_ string, od *ormo.InOutOrder, evt int) {
			stgy := subStgys[od.Strategy]
			if stgy == nil {
				return
			}
			pairTF := strings.Join([]string{od.Symbol, od.Timeframe}, "_")
			job := state.LookupJob(account, pairTF, od.Strategy)
			if job == nil {
				return
			}
			if deps.Core != nil && deps.Core.LiveMode && !job.IsWarmUpState() && prices != nil && deps.Clock != nil && deps.Exchange != nil {
				if err := prices.RefreshLatestPriceAt(deps.Clock.TimeMS(), deps.Exchange, job.Symbol.Symbol); err != nil {
					log.Warn("refresh latest price fail", zap.String("pair", job.Symbol.Symbol), zap.Error(err))
				}
			}
			if stgy.HedgeOff && evt == strat.OdChgEnterFill {
				closeSideOrders(job, !od.Short)
			}
			if stgy.OnOrderChange != nil {
				if evt == strat.OdChgExitFill {
					if deps.Orders != nil {
						openOds, lock := deps.Orders.GetOpenODs(account)
						lock.Lock()
						job.UpdateOrders(executionOpenOrders(openOds, deps))
						lock.Unlock()
					} else {
						job.UpdateOrders(nil)
					}
				}
				stgy.OnOrderChange(job, od, evt)
			}
			requests := job.ExecutionSnapshot()
			if len(requests.Entrys) > 0 || len(requests.Exits) > 0 {
				mgr := GetOdMgrWithState(deps.Trading, account)
				if mgr != nil {
					if _, _, err := mgr.ProcessOrders(job); err != nil {
						log.Error("process orders fail", zap.Error(err))
					}
				}
			}
			// A close-on-remove job remains in AccJobs until its terminal
			// callback so this route stays valid across pair rotation.
			strat.FinalizePairRotation(state, deps.Core)
		})
	}
}

func closeSideOrders(s *strat.StratJob, isShort bool) {
	snapshot := s.ExecutionSnapshot()
	var closeList = snapshot.LongOrders
	if isShort {
		closeList = snapshot.ShortOrders
	}
	if len(closeList) > 0 {
		for _, o := range closeList {
			if o.Status >= ormo.InOutStatusPartEnter && o.Status <= ormo.InOutStatusPartExit {
				_ = s.CloseOrders(&strat.ExitReq{
					Tag:     core.ExitTagHedgeOff,
					OrderID: o.ID,
					Force:   true,
					Log:     true,
				})
			}
		}
	}
}

/*
AddBatchJob
Add batch entry tasks.
Even if the job has no entry tasks, this method should be called to postpone the entry time TFEnterMS
添加批量入场任务。
即使job没有入场任务，也应该调用此方法，用于推迟入场时间TFEnterMS
*/
func AddBatchJob(account, tf string, job *strat.StratJob, infoEnv *ta.BarEnv) {
	AddBatchJobWithState(strat.LegacyBatchState(), account, tf, job, infoEnv)
}

// AddBatchJobWithState adds a task to one trader's batch state.
func AddBatchJobWithState(state *strat.BatchState, account, tf string, job *strat.StratJob, infoEnv *ta.BarEnv) {
	addBatchJobWithRuntimeDeps(nil, state, account, tf, job, infoEnv)
}

// AddBatchJobWithRuntimeDeps schedules a batch task using the runtime clock.
// Explicit runners must use this path so task admission never observes the
// process-wide simulated timestamp.
func AddBatchJobWithRuntimeDeps(deps *RuntimeDeps, state *strat.BatchState, account, tf string, job *strat.StratJob, infoEnv *ta.BarEnv) {
	addBatchJobWithRuntimeDeps(deps, state, account, tf, job, infoEnv)
}

func addBatchJobWithRuntimeDeps(deps *RuntimeDeps, state *strat.BatchState, account, tf string, job *strat.StratJob, infoEnv *ta.BarEnv) {
	if state == nil || job == nil || job.Strat == nil || job.Symbol == nil {
		return
	}
	key := tf + "_" + account + "_" + job.Strat.Name
	var pair = job.Symbol.Symbol
	var pairKey = pair + "_main"
	if infoEnv != nil {
		pair = infoEnv.Symbol
		pairKey = pair + "_info"
	}
	var nowMS int64
	if deps == nil {
		nowMS = btime.TimeMS()
	} else if deps.Clock != nil {
		nowMS = deps.Clock.TimeMS()
	}
	state.AddTask(key, pairKey, &strat.JobEnv{Job: job, Env: infoEnv, Symbol: pair},
		int64(utils2.TFToSecs(tf)*1000), nowMS+core.DelayBatchMS)
}

func TryFireBatches(currMS int64, isWarmUp bool) int {
	return TryFireBatchesWithState(strat.LegacyBatchState(), currMS, isWarmUp)
}

// TryFireBatchesWithState removes ready tasks under the state lock and runs callbacks after unlocking.
func TryFireBatchesWithState(state *strat.BatchState, currMS int64, isWarmUp bool) int {
	return tryFireBatches(state, currMS, isWarmUp, nil)
}

// TryFireBatchesWithRuntimeDeps is the explicit-runtime batch execution path.
// It resolves both open orders and order managers from the supplied runtime;
// no package-level order registry is consulted on this hot path.
func TryFireBatchesWithRuntimeDeps(deps *RuntimeDeps, state *strat.BatchState, currMS int64, isWarmUp bool) int {
	if deps == nil {
		return TryFireBatchesWithState(state, currMS, isWarmUp)
	}
	if deps.Orders == nil || deps.Trading == nil {
		log.Error("runtime batch execution requires typed order and trading state")
		return 0
	}
	return tryFireBatches(state, currMS, isWarmUp, deps)
}

func tryFireBatches(state *strat.BatchState, currMS int64, isWarmUp bool,
	deps *RuntimeDeps) int {
	if state == nil {
		return 0
	}
	var orderState *ormo.OrderState
	var trading *TradingState
	var defaultAccount string
	if deps != nil {
		orderState = deps.Orders
		trading = deps.Trading
		defaultAccount = deps.DefaultAccount
	}
	readyItems, waitNum := state.TakeReady(currMS, strictBacktestFor(deps))

	var err *errs.Error
	for _, item := range readyItems {
		openOds, lock := getBatchOpenOrders(orderState, item.Account)
		lock.Lock()
		allOrders := executionOpenOrders(openOds, deps)
		lock.Unlock()
		for _, job := range item.MainJobs {
			bindBatchJobRuntime(job, deps)
			job.InitBar(allOrders)
		}
		if len(item.InfoJobs) > 0 {
			num1, num2 := 0, 0
			for _, j := range item.InfoJobs {
				bindBatchJobRuntime(j.Job, deps)
				inNum, outNum := strat.GetJobInOutNum(j.Job)
				num1 += inNum
				num2 += outNum
			}
			item.Strategy.OnBatchInfos(item.TimeFrame, item.InfoJobs)
			num3, num4 := 0, 0
			for _, j := range item.InfoJobs {
				inNum, outNum := strat.GetJobInOutNum(j.Job)
				num3 += inNum
				num4 += outNum
			}
			if num3 > num1 || num4 > num2 {
				log.Warn("Open/Close order in OnBatchInfos not support, please call `biz.GetOdMgr(s.Account).ProcessOrders(s)` manually")
			}
		}
		if len(item.MainJobs) > 0 {
			// Check all batch tasks at this time and decide which ones to enter or exit
			// 检查此时间所有批量任务，决定哪些入场或那些出场
			item.Strategy.OnBatchJobs(item.MainJobs)
			// Perform entry/exit tasks
			// 执行入场/出场任务
			if !isWarmUp {
				account := item.Account
				if defaultAccount != "" {
					account = defaultAccount
				}
				odMgr := getBatchOrderManager(trading, account)
				if odMgr == nil {
					log.Error("process orders fail: order manager is not initialized", zap.String("account", account))
					continue
				}
				for _, job := range item.MainJobs {
					bindBatchJobRuntime(job, deps)
					_, _, err = odMgr.ProcessOrders(job)
					if err != nil {
						log.Error("process orders fail", zap.Error(err))
					}
				}
			}
		}
	}
	return waitNum
}

func bindBatchJobRuntime(job *strat.StratJob, deps *RuntimeDeps) {
	if job == nil || deps == nil {
		return
	}
	var prices *com.PriceState
	if deps.Market != nil {
		prices = deps.Market.Prices
	}
	job.BindRuntimeMarket(prices, deps.Clock)
}

func getBatchOpenOrders(state *ormo.OrderState, account string) (map[int64]*ormo.InOutOrder, *deadlock.Mutex) {
	if state != nil {
		return state.GetOpenODs(account)
	}
	return ormo.GetOpenODs(account)
}

func getBatchOrderManager(state *TradingState, account string) IOrderMgr {
	if state != nil {
		return state.OrderManager(account)
	}
	return GetOdMgr(account)
}

func InitDataDir() *errs.Error {
	return InitDataDirAt(config.GetDataDir())
}

// InitDataDirAt creates the developer data layout at an explicit directory.
func InitDataDirAt(dataDir string) *errs.Error {
	if dataDir == "" {
		return errs.NewMsg(errs.CodeParamRequired, "-datadir or env `BanDataDir` is required")
	}
	err_ := utils.EnsureDir(dataDir, 0755)
	if err_ != nil {
		return errs.New(errs.CodeIOWriteFail, err_)
	}
	configPath := filepath.Join(dataDir, "config.yml")
	configLocalPath := filepath.Join(dataDir, "config.local.yml")
	if !utils.Exists(configPath) && !utils.Exists(configLocalPath) {
		// dont init config in dataDir if any of config.yml/config.local.yml exist
		err := utils.WriteFile(configPath, configData)
		if err != nil {
			return err
		}
		log.Info("init done", zap.String("p", configPath))
		err = utils.WriteFile(configLocalPath, configLocalData)
		log.Info("init done", zap.String("p", configLocalPath))
		if err != nil {
			return err
		}
	}

	// 初始化语言文件
	for _, lang := range []string{"zh-CN", "en-US"} {
		err := initLangFile(dataDir, lang)
		if err != nil {
			return err
		}
	}

	return nil
}

func initLangFile(dataDir, lang string) *errs.Error {
	langDir := filepath.Join(dataDir, lang)
	err_ := utils.EnsureDir(langDir, 0755)
	if err_ != nil {
		return errs.New(errs.CodeIOWriteFail, err_)
	}

	// 获取对应的嵌入文件系统
	var embedFS embed.FS
	if lang == "zh-CN" {
		embedFS = zhCNData
	} else if lang == "en-US" {
		embedFS = enUSData
	} else {
		return nil // 不支持的语言，跳过
	}

	// 读取嵌入文件系统中的所有文件
	entries, err_ := embedFS.ReadDir(lang)
	if err_ != nil {
		return errs.New(errs.CodeRunTime, err_)
	}

	// 遍历所有文件
	for _, entry := range entries {
		if entry.IsDir() {
			continue // 跳过目录
		}

		fileName := entry.Name()
		sourcePath := path.Join(lang, fileName)
		targetPath := filepath.Join(langDir, fileName)

		// 从嵌入文件系统读取文件内容
		sourceData, err_ := embedFS.ReadFile(sourcePath)
		if err_ != nil {
			log.Warn("failed to read embedded file", zap.String("file", sourcePath), zap.Error(err_))
			continue
		}

		// 对于 messages.json 文件，进行合并更新
		if fileName == "messages.json" {
			err := mergeMessagesFile(targetPath, sourceData)
			if err == nil {
				continue
			}
		}
		err := utils.WriteFile(targetPath, sourceData)
		if err != nil {
			return err
		}
	}

	return nil
}

// mergeMessagesFile 合并更新 messages.json 文件
func mergeMessagesFile(targetPath string, sourceData []byte) *errs.Error {
	// 读取目标文件
	targetData, err_ := os.ReadFile(targetPath)
	if err_ != nil {
		return errs.New(errs.CodeIOReadFail, err_)
	}

	var sourceMap, targetMap map[string]string
	if err := utils2.UnmarshalString(string(sourceData), &sourceMap, utils2.JsonNumDefault); err != nil {
		return errs.New(errs.CodeUnmarshalFail, err)
	}
	if err := utils2.UnmarshalString(string(targetData), &targetMap, utils2.JsonNumDefault); err != nil {
		return errs.New(errs.CodeUnmarshalFail, err)
	}

	updated := false
	for key, value := range sourceMap {
		if _, exists := targetMap[key]; !exists {
			targetMap[key] = value
			updated = true
		}
	}

	if updated {
		newData, err_ := utils2.MarshalString(targetMap)
		if err_ != nil {
			return errs.New(errs.CodeMarshalFail, err_)
		}
		err := utils.WriteFile(targetPath, []byte(newData))
		if err != nil {
			return err
		}
		log.Info("updated messages file", zap.String("file", targetPath))
	}

	return nil
}
