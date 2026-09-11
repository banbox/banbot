package strat

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/goods"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	ta "github.com/banbox/banta"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

/*
LoadStratJobs Loading strategies and trading pairs 加载策略和交易对

更新以下全局变量：
Update the following global variables:
core.TFSecs
core.StgPairTfs
core.BookPairs
strat.Versions
strat.Envs
strat.PairStrats
strat.AccJobs
strat.AccInfoJobs

	return：pair:timeframe:warmNum, acc:exit orders, error
*/
func LoadStratJobs(pairs []string, tfScores map[string]map[string]float64) (map[string]map[string]int, map[string][]*ormo.InOutOrder, *errs.Error) {
	return loadStratJobsWithExchange(nil, nil, nil, nil, pairs, tfScores)
}

// LoadStratJobsWithSymbolState loads strategy jobs using the supplied symbol state.
func LoadStratJobsWithSymbolState(symbols *orm.SymbolState, pairs []string, tfScores map[string]map[string]float64) (map[string]map[string]int, map[string][]*ormo.InOutOrder, *errs.Error) {
	hooks := SnapshotPairUpdateHooks()
	exchange, _ := resolveStratExchange(nil, nil, symbols, hooks)
	return loadStratJobsWithExchange(nil, nil, symbols, exchange, pairs, tfScores)
}

// LoadStratJobsWithState loads strategy registries into explicit runtime
// state. Typed runners use this path instead of installing package globals.
func LoadStratJobsWithState(strategyState *State, runtimeState *core.State, symbols *orm.SymbolState,
	pairs []string, tfScores map[string]map[string]float64, orderStates ...*ormo.OrderState) (map[string]map[string]int, map[string][]*ormo.InOutOrder, *errs.Error) {
	if strategyState != nil && strategyState != legacyState &&
		(len(orderStates) == 0 || orderStates[0] == nil) {
		return nil, nil, errs.NewMsg(core.ErrRunTime, "explicit strategy state requires order state")
	}
	var hooks PairUpdateHooks
	if strategyState != nil && strategyState != legacyState {
		hooks = strategyState.PairUpdateHooks()
		if runtimeState == nil {
			runtimeState = strategyState.Core
		}
		if symbols == nil {
			symbols = strategyState.Symbols
		}
		if hooks.Exchange == nil {
			hooks.Exchange = strategyState.Exchange
		}
	} else {
		hooks = SnapshotPairUpdateHooks()
	}
	exchange, _ := resolveStratExchange(nil, runtimeState, symbols, hooks)
	return loadStratJobsWithExchange(strategyState, runtimeState, symbols, exchange, pairs, tfScores, orderStates...)
}

// LoadStratJobsWithRuntimeState keeps pair admission on the supplied runtime
// state while retaining the legacy strategy registries and job lifecycle.
func LoadStratJobsWithRuntimeState(state *core.State, symbols *orm.SymbolState, pairs []string, tfScores map[string]map[string]float64) (map[string]map[string]int, map[string][]*ormo.InOutOrder, *errs.Error) {
	hooks := SnapshotPairUpdateHooks()
	exchange, _ := resolveStratExchange(nil, state, symbols, hooks)
	return loadStratJobsWithExchange(nil, state, symbols, exchange, pairs, tfScores)
}

func loadStratJobsWithExchange(strategyState *State, state *core.State, symbols *orm.SymbolState, exchange banexg.BanExchange,
	pairs []string, tfScores map[string]map[string]float64, orderStates ...*ormo.OrderState) (map[string]map[string]int, map[string][]*ormo.InOutOrder, *errs.Error) {
	if len(pairs) == 0 || len(tfScores) == 0 {
		return nil, nil, errs.NewMsg(errs.CodeParamRequired, "`pairs` and `tfScores` are required for LoadStratJobs")
	}
	if strategyState == nil {
		strategyState = LegacyState()
	}
	strategyState.ensureMaps()
	// Explicit runtimes publish their job registry through immutable snapshots.
	// Keep the mutable construction phase under the same write lock used by
	// snapshot readers and pair rotation. Readers that already hold the prior
	// snapshot can continue without blocking; readers that observe the dirty
	// bit wait until this complete registry update has finished and then clone
	// a consistent view. The legacy facade keeps its historical locking and
	// callback behavior for source compatibility.
	var pendingUnwatches map[string][]string
	var pendingShutdown []*StratJob
	if strategyState != legacyState {
		pendingUnwatches = make(map[string][]string)
		lockJobsWriteForState(strategyState)
		defer func() {
			unlockJobsWriteForState(strategyState)
			for _, job := range pendingShutdown {
				if job != nil && job.Strat != nil && job.Strat.OnShutDown != nil {
					job.Strat.OnShutDown(job)
				}
			}
			if callback := strategyState.WsSubUnWatchFunc(); callback != nil && len(pendingUnwatches) > 0 {
				callback(pendingUnwatches)
			}
		}()
	}
	strict := strictBacktestFor(strategyState, state)
	accounts := runtimeAccountsFor(strategyState)
	if strategyState != legacyState && runtimeConfigFor(strategyState) == nil {
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "explicit strategy state requires runtime config")
	}
	for account := range accounts {
		strategyState.Jobs(account)
		strategyState.InfoJobs(account)
	}
	if state != nil {
		state.EnsureRuntimeMaps()
	}
	// Explicit runtimes own these maps. The package globals are reset only for
	// the compatibility facade, so loading a second runtime cannot overwrite
	// the first runtime's admission and timeframe state.
	var tfSecs map[string]int
	var stgPairTfs map[string]map[string]string
	if state != nil {
		tfSecs = state.TFSecs
		stgPairTfs = state.StgPairTfs
		state.LockOdMatch.Lock()
		state.OrderMatchTfs = make(map[string]bool)
		state.LockOdMatch.Unlock()
	} else {
		core.TFSecs = make(map[string]int)
		core.StgPairTfs = make(map[string]map[string]string)
		core.LockOdMatch.Lock()
		core.OrderMatchTfs = make(map[string]bool)
		core.LockOdMatch.Unlock()
		tfSecs = core.TFSecs
		stgPairTfs = core.StgPairTfs
	}
	if strategyState == legacyState {
		config.ClearRefineMap()
	}
	Versions := strategyState.Versions
	if strategyState == legacyState {
		resetJobsWithState(strategyState, state, orderStates...)
	} else {
		resetJobsWithStateLocked(strategyState, state, orderStates...)
	}
	pairTfWarms := make(Warms)
	// 记录每个账户下，每个策略的任务数量，防止超过账户要求数量
	accLimits, maxJobNum := newAccStratLimitsForState(strategyState)
	policies := config.RunPolicy
	if strategyState != nil && strategyState != legacyState {
		cfg := runtimeConfigFor(strategyState)
		if cfg == nil {
			return nil, nil, errs.NewMsg(core.ErrBadConfig, "explicit strategy state requires runtime config")
		}
		policies = cfg.RunPolicy
	}
	for _, pol := range policies {
		stgy := newStrategyWithState(strategyState, pol)
		polID := pol.ID()
		if stgy == nil {
			return nil, nil, errs.NewMsg(core.ErrRunTime, "strategy %s load fail", polID)
		}
		Versions[stgy.Name] = stgy.Version
		stgyMaxNum := pol.MaxPair
		if stgyMaxNum == 0 {
			stgyMaxNum = maxJobNum
		}
		holdNum := 0
		failTfScores := make(map[string]map[string]float64)
		var curPairs, err = getPolicyPairsWithStrategyState(strategyState, state, symbols, exchange, pol, pairs)
		if err != nil {
			return nil, nil, err
		}
		exsList, err := callStratSymbolsWithExchange(strategyState, state, symbols, exchange, stgy, curPairs, tfScores)
		if err != nil {
			return nil, nil, err
		}
		dirt := pol.OdDirt()
		// 旧job允许开单的，先添加计数
		oldAllowOpen := stgy.OrderOnRotation == "open"
		oldAddPairs := make(map[string]bool)
		for acc := range utils.MapKeys(strategyState.AccJobs, strictBacktestFor(strategyState, state)) {
			accJobs := strategyState.AccJobs[acc]
			codes := make([]string, 0, len(exsList)/2)
			for _, jobs := range accJobs {
				if job, ok := jobs[polID]; ok {
					snapshot := job.ExecutionSnapshot()
					if snapshot.MaxOpenLong >= 0 || snapshot.MaxOpenShort >= 0 {
						holdNum += 1
						oldAddPairs[job.Symbol.Symbol] = true
						if !accLimits.tryAdd(acc, polID) {
							codes = append(codes, job.Symbol.Symbol)
						}
					}
				}
			}
			if len(codes) > 0 {
				log.Error("old job num exceed limit", zap.String("acc", acc), zap.String("strat", polID),
					zap.Strings("pairs", codes))
			}
		}
		for _, exs := range exsList {
			if holdNum >= stgyMaxNum || len(oldAddPairs) >= stgyMaxNum {
				break
			}
			var pairAdded bool
			if oldAllowOpen {
				_, pairAdded = oldAddPairs[exs.Symbol]
			}
			oldAddPairs[exs.Symbol] = true
			curStgy := stgy
			scores, _ := tfScores[exs.Symbol]
			tf := curStgy.pickTimeFrame(exs.Symbol, scores)
			if tf == "" {
				if !pairAdded {
					failTfScores[exs.Symbol] = scores
				}
				continue
			}
			jobType := jobForbidType(strategyState, exs.Symbol, tf, polID)
			if jobType > 0 {
				if jobType > 1 && !pairAdded {
					// 任务禁止，但增加占位
					holdNum += 1
					for acc := range utils.MapKeys(accounts, strict) {
						cfg := accounts[acc]
						if cfg.NoTrade {
							continue
						}
						accLimits.tryAdd(acc, polID)
					}
				}
				continue
			}
			items, ok := strategyState.PairStrats[exs.Symbol]
			if !ok {
				items = make(map[string]*TradeStrat)
				strategyState.PairStrats[exs.Symbol] = items
			}
			if _, ok = items[polID]; ok {
				// 当前pair+stratID已有任务，跳过
				newAdd := 0
				newAdd, err = markStratJobWithState(strategyState, tf, polID, exs, dirt, accLimits)
				if err != nil {
					return nil, nil, err
				}
				if newAdd > 0 {
					holdNum += 1
				}
				continue
			}
			// Check for proprietary parameters of the current target and reinitialize the strategy
			// 检查有当前标的专有参数，重新初始化策略
			if curPol, isDiff := pol.PairDup(exs.Symbol); isDiff {
				curStgy = newStrategyWithState(strategyState, curPol)
			}
			items[polID] = curStgy
			holdNum += 1
			// 初始化BarEnv
			env := initBarEnvWithState(strategyState, state, exs, tf)
			ensureStratJobWithRuntimeState(strategyState, state, curStgy, tf, exs, env, dirt, pairTfWarms.Update, accLimits, symbols)
		}
		printFailTfScores(polID, failTfScores)
	}
	var envKeys = make(map[string]bool)
	// 对AccJobs中，当前禁止开单的job，如果无入场订单，则删除job
	accExitOds := make(map[string][]*ormo.InOutOrder)
	exitJobs := make(map[*StratJob]bool)
	exitPairs := make(map[string]bool) // 不再监听的品种
	newPairs := make(map[string]bool)  // 继续监听的品种
	pairTfs := make(Warms)
	holdPosition := true
	if cfg := runtimeConfigFor(strategyState); cfg != nil && cfg.PairMgr != nil {
		holdPosition = cfg.PairMgr.PosOnRotation != "close"
	}
	for acc := range utils.MapKeys(strategyState.AccJobs, strict) {
		jobs := strategyState.AccJobs[acc]
		exitOds := make([]*ormo.InOutOrder, 0, 4)
		for envKey := range utils.MapKeys(jobs, strict) {
			envJobs := jobs[envKey]
			resJobs := make(map[string]*StratJob)
			for name := range utils.MapKeys(envJobs, strict) {
				job := envJobs[name]
				snapshot := job.ExecutionSnapshot()
				if snapshot.MaxOpenLong == -1 && snapshot.MaxOpenShort == -1 {
					// disable open order
					if snapshot.EnteredNum > 0 && holdPosition {
						// 有未平仓订单，继续跟踪
						resJobs[name] = job
					} else {
						// 立刻平仓
						if strategyState == legacyState {
							if job.Strat.OnShutDown != nil {
								job.Strat.OnShutDown(job)
							}
							unRegWsJobWithState(strategyState, job)
						} else {
							pendingShutdown = append(pendingShutdown, job)
							for msgType, pairs := range unRegWsJobLockedWithState(strategyState, job) {
								pendingUnwatches[msgType] = append(pendingUnwatches[msgType], pairs...)
							}
						}
						exitJobs[job] = true
						exitPairs[job.Symbol.Symbol] = true
						if jobHasOutstandingOrders(job) || len(snapshot.LongOrders) > 0 || len(snapshot.ShortOrders) > 0 {
							job.SetPairRemovalPending(true)
							resJobs[name] = job
							exitOds = append(exitOds, snapshot.LongOrders...)
							exitOds = append(exitOds, snapshot.ShortOrders...)
						}
					}
				} else {
					// 可以继续开单
					resJobs[name] = job
					newPairs[job.Symbol.Symbol] = true
				}
			}
			if len(resJobs) > 0 {
				jobs[envKey] = resJobs
				hasLiveJob := false
				arr := strings.Split(envKey, "_")
				pair, tf := arr[0], arr[1]
				if _, ok := tfSecs[tf]; !ok {
					tfSecs[tf] = utils2.TFToSecs(tf)
				}
				for name := range utils.MapKeys(resJobs, strict) {
					j := resJobs[name]
					if j.PairRemovalPending() {
						continue
					}
					hasLiveJob = true
					subMap, ok := stgPairTfs[j.Strat.Name]
					if !ok {
						subMap = make(map[string]string)
						stgPairTfs[j.Strat.Name] = subMap
					}
					subMap[pair] = tf
					if len(j.Strat.WsSubs) > 0 {
						var err *errs.Error
						if strategyState == legacyState {
							err = regWsJobWithState(strategyState, j)
						} else {
							err = regWsJobLockedWithState(strategyState, j)
						}
						if err != nil {
							return nil, nil, err
						}
					}
					matchTf := strategyState.refineTimeFrame(j.Strat.Name, tf)
					pairTfs.Update(pair, matchTf, 0)
				}
				if hasLiveJob {
					envKeys[envKey] = true
					pairTfs.Update(pair, tf, 0)
				}
			} else {
				delete(jobs, envKey)
			}
		}
		if len(exitOds) > 0 {
			accExitOds[acc] = exitOds
		}
	}
	for p := range exitPairs {
		if _, ok := newPairs[p]; ok {
			delete(exitPairs, p)
		}
	}
	if len(exitPairs) > 0 {
		keys := utils2.KeysOfMap(exitPairs)
		log.Info("exit pairs", zap.Int("num", len(keys)), zap.Strings("arr", keys))
	}
	setAdmissionSnapshot(state, newPairs)
	// 从AccInfoJobs中移除已取消的项
	lockInfoJobsWrite(strategyState)
	for acc := range utils.MapKeys(strategyState.AccInfoJobs, strict) {
		jobMap := strategyState.AccInfoJobs[acc]
		newJobMap := make(map[string]map[string]*StratJob)
		for subKey := range utils.MapKeys(jobMap, strict) {
			stgMap := jobMap[subKey]
			newStgMap := make(map[string]*StratJob)
			for name := range utils.MapKeys(stgMap, strict) {
				job := stgMap[name]
				if _, ok := exitJobs[job]; !ok {
					newStgMap[name] = job
					matchTf := strategyState.refineTimeFrame(job.Strat.Name, job.TimeFrame)
					pairTfs.Update(job.Symbol.Symbol, matchTf, 0)
				}
			}
			if len(newStgMap) > 0 {
				newJobMap[subKey] = newStgMap
				source, sid, tf, ok := ParseDataSubKey(subKey)
				if ok {
					if _, ok = tfSecs[tf]; !ok {
						tfSecs[tf] = utils2.TFToSecs(tf)
					}
					if source == "kline" {
						var exs *orm.ExSymbol
						if symbols == nil {
							exs = orm.GetSymbolByID(sid)
						} else {
							exs = symbols.GetSymbolByID(sid)
						}
						if exs != nil {
							pairTfs.Update(exs.Symbol, tf, 0)
							envKeys[strings.Join([]string{exs.Symbol, tf}, "_")] = true
							initBarEnvWithState(strategyState, state, exs, tf)
						}
					}
				}
			}
		}
		strategyState.AccInfoJobs[acc] = newJobMap
	}
	unlockInfoJobsWrite(strategyState)
	// Ensure that all pairs and TFs are recorded in the returned data to prevent them from being removed by the data subscriber
	// 确保所有pair、tf都在返回的中有记录，防止被数据订阅端移除
	for _, pairMap := range stgPairTfs {
		for pair, tf := range pairMap {
			pairTfs.Update(pair, tf, 0)
		}
	}
	// Remove useless items from PairStrats
	// 从PairStrats中删除无用的项
	for pair, stgMap := range strategyState.PairStrats {
		for name := range stgMap {
			if pairMap, ok := stgPairTfs[name]; ok {
				if _, ok = pairMap[pair]; ok {
					continue
				}
			}
			delete(stgMap, name)
		}
	}
	// Remove useless items from Envs
	// 从Envs中删除无用的项
	for _, envKey := range strategyState.EnvKeys() {
		if _, ok := envKeys[envKey]; !ok {
			strategyState.DeleteEnv(envKey)
			strategyState.tmpEnvLock.Lock()
			delete(strategyState.TmpEnvs, envKey)
			strategyState.tmpEnvLock.Unlock()
		}
	}
	// 从pairTfs中确认哪些要恢复
	for pair, tfMap := range pairTfs {
		rawTfMap, ok := pairTfWarms[pair]
		if !ok {
			continue
		}
		for tf := range tfMap {
			rawNum, ok2 := rawTfMap[tf]
			if ok2 {
				tfMap[tf] = rawNum
			}
		}
	}
	return pairTfs, accExitOds, nil
}

func ExitStratJobs() {
	ExitStratJobsWithState(nil)
}

// ExitStratJobsWithState shuts down strategy callbacks owned by one runtime.
// A nil state preserves the legacy package facade; explicit runners never need
// to consult the process-global strategy registries during cleanup.
func ExitStratJobsWithState(strategyState *State) {
	if strategyState == nil {
		strategyState = LegacyState()
	}
	for _, job := range strategyState.CollectJobs() {
		if job == nil || job.Strat == nil {
			continue
		}
		if job.Strat.OnShutDown != nil {
			job.Strat.OnShutDown(job)
		}
		unRegWsJobWithState(strategyState, job)
	}
	var strats []*TradeStrat
	if strategyState == legacyState {
		cacheMu.Lock()
		strats = make([]*TradeStrat, 0, len(cacheStrats))
		for _, stg := range cacheStrats {
			strats = append(strats, stg)
		}
		cacheMu.Unlock()
	} else {
		strategyState.cacheMu.Lock()
		strats = make([]*TradeStrat, 0, len(strategyState.cacheStrats))
		for _, stg := range strategyState.cacheStrats {
			strats = append(strats, stg)
		}
		strategyState.cacheMu.Unlock()
	}
	for _, stg := range strats {
		if stg.OnStratExit != nil {
			stg.OnStratExit()
		}
	}
}

func CallStratSymbols(stgy *TradeStrat, curPairs []string, tfScores map[string]map[string]float64) ([]*orm.ExSymbol, *errs.Error) {
	return callStratSymbolsWithExchange(nil, nil, nil, nil, stgy, curPairs, tfScores)
}

// CallStratSymbolsWithSymbolState resolves strategy symbols from the supplied state.
func CallStratSymbolsWithSymbolState(symbols *orm.SymbolState, stgy *TradeStrat, curPairs []string, tfScores map[string]map[string]float64) ([]*orm.ExSymbol, *errs.Error) {
	hooks := SnapshotPairUpdateHooks()
	exchange, _ := resolveStratExchange(nil, nil, symbols, hooks)
	return callStratSymbolsWithExchange(nil, nil, symbols, exchange, stgy, curPairs, tfScores)
}

// CallStratSymbolsWithRuntimeState keeps dynamic pair admission on the
// supplied runtime state while preserving legacy behavior for a nil state.
func CallStratSymbolsWithRuntimeState(state *core.State, symbols *orm.SymbolState, stgy *TradeStrat, curPairs []string, tfScores map[string]map[string]float64) ([]*orm.ExSymbol, *errs.Error) {
	hooks := SnapshotPairUpdateHooks()
	exchange, _ := resolveStratExchange(nil, state, symbols, hooks)
	return callStratSymbolsWithExchange(nil, state, symbols, exchange, stgy, curPairs, tfScores)
}

func callStratSymbolsWithExchange(strategyState *State, state *core.State, symbols *orm.SymbolState, exchange banexg.BanExchange,
	stgy *TradeStrat, curPairs []string, tfScores map[string]map[string]float64) ([]*orm.ExSymbol, *errs.Error) {
	var exsMap = make(map[string]*orm.ExSymbol)
	for _, pair := range curPairs {
		var exs *orm.ExSymbol
		var err *errs.Error
		if symbols == nil {
			exs, err = orm.GetExSymbolCur(pair)
		} else {
			exs, err = symbols.GetExSymbolCur(pair)
		}
		if err != nil {
			return nil, err
		}
		exsMap[pair] = exs
	}
	if stgy.OnSymbols == nil {
		return utils2.ValsOfMapBy(exsMap, curPairs), nil
	}
	modified := stgy.OnSymbols(curPairs)
	adds, removes := utils.GetAddsRemoves(modified, curPairs)
	if len(adds) > 0 || len(removes) > 0 {
		log.Info("strategy change symbols", zap.String("strat", stgy.Name),
			zap.Int("add", len(adds)), zap.Int("remove", len(removes)))
		if len(adds) > 0 {
			newPairs := make([]string, 0, len(adds))
			for _, pair := range adds {
				if _, ok := exsMap[pair]; !ok {
					var exs *orm.ExSymbol
					var err *errs.Error
					if symbols == nil {
						exs, err = orm.GetExSymbolCur(pair)
					} else {
						exs, err = symbols.GetExSymbolCur(pair)
					}
					if err != nil {
						return nil, err
					}
					exsMap[pair] = exs
					if _, ok = tfScores[pair]; !ok {
						newPairs = append(newPairs, pair)
						enableAdmissionPair(state, pair)
					}
				}
			}
			if len(newPairs) > 0 {
				explicit := strategyState != nil && strategyState != legacyState || state != nil || symbols != nil || exchange != nil
				pairTfScores, err := calcPairTfScoresForRuntime(strategyState, symbols, exchange, explicit, newPairs)
				if err != nil {
					if explicit {
						return nil, err
					}
					log.Error("CalcPairTfScores fail", zap.Error(err))
				} else {
					for pair, scores := range pairTfScores {
						tfScores[pair] = scores
					}
				}
			}
		}
		if len(removes) > 0 {
			for _, it := range removes {
				if _, ok := exsMap[it]; ok {
					delete(exsMap, it)
				}
			}
		}
	}
	return utils2.ValsOfMapBy(exsMap, modified), nil
}

func printFailTfScores(stratName string, pairTfScores map[string]map[string]float64) {
	if len(pairTfScores) == 0 {
		return
	}
	lines := make([]string, 0, len(pairTfScores))
	for pair, tfScores := range pairTfScores {
		if len(tfScores) == 0 {
			lines = append(lines, fmt.Sprintf("%v: ", pair))
			continue
		}
		scoreStrs := make([]string, 0, len(pairTfScores))
		for tf_, score := range tfScores {
			scoreStrs = append(scoreStrs, fmt.Sprintf("%v: %.3f", tf_, score))
		}
		lines = append(lines, fmt.Sprintf("%v: %v", pair, strings.Join(scoreStrs, ", ")))
	}
	log.Info(fmt.Sprintf("%v filter pairs by tfScore: \n%v", stratName, strings.Join(lines, "\n")))
}

func initBarEnv(exs *orm.ExSymbol, tf string) *ta.BarEnv {
	return initBarEnvWithState(nil, nil, exs, tf)
}

func initBarEnvWithState(strategyState *State, state *core.State, exs *orm.ExSymbol, tf string) *ta.BarEnv {
	if strategyState == nil {
		strategyState = LegacyState()
	}
	strategyState.ensureMaps()
	envKey := strings.Join([]string{exs.Symbol, tf}, "_")
	env, ok := strategyState.Env(envKey)
	if !ok {
		var err error
		exgName, market := exs.Exchange, exs.Market
		if exgName == "" {
			if state != nil {
				exgName = state.ExgName
			} else {
				exgName = core.ExgName
			}
		}
		if market == "" {
			if state != nil {
				market = state.Market
			} else {
				market = core.Market
			}
		}
		env, err = ta.NewBarEnv(exgName, market, exs.Symbol, tf)
		if err != nil {
			panic(err)
		}
		env.MaxCache = 1500
		if strategyState != legacyState && strategyState.Core != nil && strategyState.Core.NumTaCache > 0 {
			env.MaxCache = strategyState.Core.NumTaCache
		} else if state != nil && state.NumTaCache > 0 {
			env.MaxCache = state.NumTaCache
		} else if strategyState == legacyState {
			env.MaxCache = core.NumTaCache
		}
		env.Data.Store("sid", int64(exs.ID))
		strategyState.SetEnv(envKey, env)
	}
	return env
}

func markStratJob(tf, polID string, exs *orm.ExSymbol, dirt int, accLimits accStratLimits) (int, *errs.Error) {
	return markStratJobWithState(LegacyState(), tf, polID, exs, dirt, accLimits)
}

func markStratJobWithState(strategyState *State, tf, polID string, exs *orm.ExSymbol, dirt int, accLimits accStratLimits) (int, *errs.Error) {
	if strategyState == nil {
		strategyState = LegacyState()
	}
	strategyState.ensureMaps()
	envKey := strings.Join([]string{exs.Symbol, tf}, "_")
	newAdd := 0
	for acc := range utils.MapKeys(strategyState.AccJobs, strictBacktestFor(strategyState, nil)) {
		jobs := strategyState.AccJobs[acc]
		envJobs, ok := jobs[envKey]
		if !ok {
			// 对于多账户且品种数不一样时，忽略未配置的账户
			log.Info("AccJobs not found, skip", zap.String("acc", acc), zap.String("env", envKey))
			continue
		}
		job, ok := envJobs[polID]
		if !ok {
			log.Info("StratJob not found, skip", zap.String("acc", acc), zap.String("env", envKey),
				zap.String("strat", polID))
			continue
		}
		snapshot := job.ExecutionSnapshot()
		if snapshot.MaxOpenShort >= 0 || snapshot.MaxOpenLong >= 0 {
			// 已事先允许，跳过避免重复计数
			continue
		}
		if accLimits.tryAdd(acc, polID) {
			newAdd += 1
			maxLong, maxShort := job.Strat.EachMaxLong, job.Strat.EachMaxShort
			if dirt == core.OdDirtShort {
				maxLong = -1
			} else if dirt == core.OdDirtLong {
				maxShort = -1
			}
			job.SetOpenLimits(maxLong, maxShort)
		}
	}
	return newAdd, nil
}

func ensureStratJob(stgy *TradeStrat, tf string, exs *orm.ExSymbol, env *ta.BarEnv, dirt int,
	logWarm func(pair, tf string, num int), accLimits accStratLimits) {
	ensureStratJobWithRuntimeState(LegacyState(), nil, stgy, tf, exs, env, dirt, logWarm, accLimits, nil)
}

func ensureStratJobWithSymbolState(stgy *TradeStrat, tf string, exs *orm.ExSymbol, env *ta.BarEnv, dirt int,
	logWarm func(pair, tf string, num int), accLimits accStratLimits, symbols *orm.SymbolState) {
	ensureStratJobWithRuntimeState(LegacyState(), nil, stgy, tf, exs, env, dirt, logWarm, accLimits, symbols)
}

func ensureStratJobWithRuntimeState(strategyState *State, state *core.State, stgy *TradeStrat, tf string, exs *orm.ExSymbol, env *ta.BarEnv, dirt int,
	logWarm func(pair, tf string, num int), accLimits accStratLimits, symbols *orm.SymbolState) {
	if strategyState == nil {
		strategyState = LegacyState()
	}
	runtimeCore := runtimeCoreFor(strategyState, state)
	runtimeClock := (*btime.ClockState)(nil)
	if strategyState != legacyState {
		runtimeClock = strategyState.Clock
	}
	strategyState.ensureMaps()
	logWarm(exs.Symbol, tf, stgy.WarmupNum)
	if stgy.Policy.RefineTF == nil && stgy.RefineTF != nil {
		stgy.Policy.RefineTF = stgy.RefineTF
	}
	matchTf := strategyState.refineTimeFrame(stgy.Name, tf)
	if matchTf != tf {
		logWarm(exs.Symbol, matchTf, 0)
	}
	if runtimeCore != nil {
		runtimeCore.EnsureRuntimeMaps()
		runtimeCore.LockOdMatch.Lock()
		runtimeCore.OrderMatchTfs[matchTf] = true
		runtimeCore.LockOdMatch.Unlock()
	} else if strategyState == legacyState {
		core.LockOdMatch.Lock()
		core.OrderMatchTfs[matchTf] = true
		core.LockOdMatch.Unlock()
	}
	envKey := strings.Join([]string{exs.Symbol, tf}, "_")
	for account := range utils.MapKeys(strategyState.AccJobs, strictBacktestFor(strategyState, state)) {
		jobs := strategyState.AccJobs[account]
		envJobs, ok := jobs[envKey]
		if !ok {
			envJobs = make(map[string]*StratJob)
			jobs[envKey] = envJobs
		}
		allowOpen := accLimits.tryAdd(account, stgy.Name)
		job, ok := envJobs[stgy.Name]
		if !ok {
			if !allowOpen {
				continue
			}
			job = &StratJob{
				Strat:         stgy,
				Env:           env,
				DataHub:       NewDataHub(),
				Symbol:        exs,
				TimeFrame:     tf,
				Account:       account,
				TPMaxs:        make(map[int64]float64),
				CloseLong:     true,
				CloseShort:    true,
				ExgStopLoss:   true,
				ExgTakeProfit: true,
				symbols:       symbols,
				strategyState: strategyState,
				runtimeCore:   runtimeCore,
				runtimeClock:  runtimeClock,
			}
			if stgy.OnStartUp != nil {
				stgy.OnStartUp(job)
			}
			envJobs[stgy.Name] = job
		} else {
			job.symbols = symbols
			job.strategyState = strategyState
			job.runtimeCore = runtimeCore
			job.runtimeClock = runtimeClock
		}
		if allowOpen {
			maxLong, maxShort := stgy.EachMaxLong, stgy.EachMaxShort
			if dirt == core.OdDirtShort {
				maxLong = -1
			} else if dirt == core.OdDirtLong {
				maxShort = -1
			}
			job.SetOpenLimits(maxLong, maxShort)
		}
		// Load subscription information for other targets
		// 加载订阅其他标的信息
		if stgy.OnPairInfos != nil || stgy.OnDataSubs != nil {
			subs := CollectDataSubs(job)
			hasInfoSubs := false
			for _, sub := range subs {
				if sub != nil && sub.ExSymbol != nil {
					hasInfoSubs = true
					break
				}
			}
			hasSideInputHandler := stgy.OnData != nil || stgy.OnInfoBar != nil ||
				stgy.BatchInfo && stgy.OnBatchInfos != nil
			if hasInfoSubs && !hasSideInputHandler {
				panic(fmt.Sprintf("%s: side-input subscriptions require `OnData`, `OnInfoBar`, or `BatchInfo` + `OnBatchInfos`", stgy.Name))
			}
			lockInfoJobsWrite(strategyState)
			infoJobs := strategyState.InfoJobs(account)
			for _, s := range subs {
				if s == nil || s.ExSymbol == nil {
					continue
				}
				hasInfoSubs = true
				if orm.NormalizeSeriesSource(s.Source) == orm.SeriesSourceKline {
					pair := s.ExSymbol.Symbol
					initBarEnvWithState(strategyState, state, s.ExSymbol, s.TimeFrame)
					logWarm(pair, s.TimeFrame, s.WarmupNum)
				}
				jobKey := DataSubKey(s.Source, s.ExSymbol.ID, s.TimeFrame)
				items, ok := infoJobs[jobKey]
				if !ok {
					items = make(map[string]*StratJob)
					infoJobs[jobKey] = items
				}
				// 这里需要stratID+pair作为键，否则多个品种订阅同一个额外品种数据时，只记录了最后一个
				items[strings.Join([]string{stgy.Name, exs.Symbol}, "_")] = job
			}
			unlockInfoJobsWrite(strategyState)
		}
	}
}

/*
将jobs的MaxOpenLong,MacOpenShort都置为-1，禁止开单，并更新附加订单
*/
func resetJobs() {
	resetJobsWithState(LegacyState(), nil)
}

func resetJobsWithState(strategyState *State, state *core.State, orderStates ...*ormo.OrderState) {
	if strategyState == nil {
		strategyState = LegacyState()
	}
	strategyState.ensureMaps()
	lockJobsWriteForState(strategyState)
	defer unlockJobsWriteForState(strategyState)
	resetJobsWithStateLocked(strategyState, state, orderStates...)
}

// resetJobsWithStateLocked is the construction-phase variant used by an
// explicit registry reload that already owns jobsMu. Keeping the locking
// wrapper above preserves the public/legacy helper without recursive locking.
func resetJobsWithStateLocked(strategyState *State, state *core.State, orderStates ...*ormo.OrderState) {
	if strategyState == nil {
		strategyState = LegacyState()
	}
	strategyState.ensureMaps()
	orderState := ormo.LegacyState()
	if len(orderStates) > 0 && orderStates[0] != nil {
		orderState = orderStates[0]
	}
	accounts := runtimeAccountsFor(strategyState)
	strict := strictBacktestFor(strategyState, state)
	for account := range utils.MapKeys(accounts, strict) {
		cfg := accounts[account]
		if cfg.NoTrade {
			continue
		}
		openOds, lock := orderState.GetOpenODs(account)
		lock.Lock()
		odList := rotationOpenOrderViewWithStrict(openOds, strict)
		lock.Unlock()
		accJobs := strategyState.Jobs(account)
		for envKey := range utils.MapKeys(accJobs, strict) {
			jobs := accJobs[envKey]
			for name := range utils.MapKeys(jobs, strict) {
				job := jobs[name]
				job.InitBar(odList)
				snapshot := job.ExecutionSnapshot()
				if job.Strat.OrderOnRotation != "open" || snapshot.OrderNum == 0 {
					job.SetOpenLimits(-1, -1)
				} else {
					pair := job.Symbol.Symbol
					enableAdmissionPair(state, pair)
				}
			}
		}
	}
}

func jobHasOutstandingOrders(job *StratJob) bool {
	if job == nil {
		return false
	}
	snapshot := job.ExecutionSnapshot()
	if snapshot.EnteredNum > 0 {
		return true
	}
	seen := make(map[*ormo.InOutOrder]struct{}, len(snapshot.LongOrders)+len(snapshot.ShortOrders))
	for _, orders := range [][]*ormo.InOutOrder{snapshot.LongOrders, snapshot.ShortOrders} {
		for _, od := range orders {
			if od == nil {
				continue
			}
			if _, ok := seen[od]; ok {
				continue
			}
			seen[od] = struct{}{}
			if od.Status < ormo.InOutStatusFullExit {
				return true
			}
			if od.Enter != nil && od.Exit != nil &&
				od.Enter.Filled-od.Exit.Filled > core.AmtDust {
				return true
			}
		}
	}
	return false
}

// FinalizePairRotation removes disabled jobs after their requested exits have
// reached a terminal state. Until then they remain in AccJobs so order events
// can still be routed to the owning strategy instance.
func FinalizePairRotation(strategyState *State, coreStates ...*core.State) {
	if strategyState == nil {
		strategyState = LegacyState()
	}
	strategyState.ensureMaps()
	var coreState *core.State
	if len(coreStates) > 0 {
		coreState = coreStates[0]
	}
	if coreState == nil && strategyState != legacyState {
		coreState = strategyState.Core
	}
	var pairTfs map[string]map[string]string
	if coreState != nil {
		coreState.EnsureRuntimeMaps()
		pairTfs = coreState.StgPairTfs
	} else if strategyState == legacyState {
		pairTfs = core.StgPairTfs
	}
	finalizedPairs := make(map[string]struct{})
	lockJobsWriteForState(strategyState)
	defer unlockJobsWriteForState(strategyState)
	for account, jobs := range strategyState.AccJobs {
		for envKey, envJobs := range jobs {
			for name, job := range envJobs {
				if job == nil || !job.PairRemovalPending() || jobHasOutstandingOrders(job) {
					continue
				}
				delete(envJobs, name)
				if job.Symbol != nil {
					if items := strategyState.PairStrats[job.Symbol.Symbol]; items != nil && items[name] == job.Strat {
						delete(items, name)
						if len(items) == 0 {
							delete(strategyState.PairStrats, job.Symbol.Symbol)
						}
					}
					if pairMap := pairTfs[name]; pairMap != nil {
						delete(pairMap, job.Symbol.Symbol)
						if len(pairMap) == 0 {
							delete(pairTfs, name)
						}
					}
					finalizedPairs[job.Symbol.Symbol] = struct{}{}
				}
			}
			if len(envJobs) == 0 {
				delete(jobs, envKey)
			}
		}
		if len(jobs) == 0 {
			delete(strategyState.AccJobs, account)
		}
	}
	for pair := range finalizedPairs {
		used := false
		for _, pairs := range pairTfs {
			if _, ok := pairs[pair]; ok {
				used = true
				break
			}
		}
		if !used && (strategyState == legacyState || coreState != nil) {
			setAdmissionPair(coreState, pair, false)
		}
	}
}

func admissionPairs(state *core.State) []string {
	if state != nil {
		return state.AdmissionPairs()
	}
	return core.LegacyAdmissionPairs()
}

func enableAdmissionPair(state *core.State, pair string) {
	if state != nil {
		state.SetAdmissionPair(pair, true)
		return
	}
	core.SetLegacyAdmissionPair(pair, true)
}

func setAdmissionPair(state *core.State, pair string, enabled bool) {
	if state != nil {
		state.SetAdmissionPair(pair, enabled)
		return
	}
	core.SetLegacyAdmissionPair(pair, enabled)
}

func setAdmissionSnapshot(state *core.State, active map[string]bool) {
	if state != nil {
		state.SetAdmissionSnapshot(active)
		return
	}
	core.SetLegacyAdmissionSnapshot(active)
}

func rotationOpenOrderView(orders map[int64]*ormo.InOutOrder) []*ormo.InOutOrder {
	return rotationOpenOrderViewWithStrict(orders, config.StrictBacktest())
}

func rotationOpenOrderViewWithStrict(orders map[int64]*ormo.InOutOrder, strict bool) []*ormo.InOutOrder {
	result := utils2.ValsOfMap(orders)
	if strict {
		slices.SortFunc(result, func(a, b *ormo.InOutOrder) int {
			if order := cmp.Compare(a.RealEnterMS(), b.RealEnterMS()); order != 0 {
				return order
			}
			return cmp.Compare(a.ID, b.ID)
		})
	}
	return result
}

func regWsJob(j *StratJob) *errs.Error {
	return regWsJobWithState(LegacyState(), j)
}

func regWsJobWithState(strategyState *State, j *StratJob) *errs.Error {
	if strategyState == nil {
		strategyState = LegacyState()
	}
	strategyState.ensureMaps()
	lockJobsWriteForState(strategyState)
	defer unlockJobsWriteForState(strategyState)
	return regWsJobLockedWithState(strategyState, j)
}

// regWsJobLocked mutates the legacy websocket registry while lockJobs is held.
// Pair updates already hold that lock across the related job maps, so those
// callers use this helper to avoid recursive locking.
func regWsJobLocked(j *StratJob) *errs.Error {
	return regWsJobLockedWithState(LegacyState(), j)
}

func regWsJobLockedWithState(strategyState *State, j *StratJob) *errs.Error {
	if strategyState == nil {
		strategyState = LegacyState()
	}
	strategyState.ensureMaps()
	for msgType, subPairs := range j.Strat.WsSubs {
		if _, ok := core.WsSubMap[msgType]; !ok {
			return errs.NewMsg(errs.CodeRunTime, "WsSubs.%s for %s is invalid", msgType, j.Strat.Name)
		}
		if msgType == core.WsSubDepth && j.Strat.OnWsDepth == nil {
			continue
		}
		if msgType == core.WsSubTrade && j.Strat.OnWsTrades == nil {
			continue
		}
		if msgType == core.WsSubKLine && j.Strat.OnWsKline == nil {
			continue
		}
		pairMap, ok := strategyState.WsSubJobs[msgType]
		if !ok {
			pairMap = make(map[string]map[*StratJob]bool)
			strategyState.WsSubJobs[msgType] = pairMap
		}
		pairArr := strings.Split(subPairs, ",")
		for _, pairs := range pairArr {
			if pairs == "_cur_" || pairs == "" {
				pairs = j.Symbol.Symbol
			}
			arr := strings.Split(pairs, ",")
			for _, p := range arr {
				jobMap, ok := pairMap[p]
				if !ok {
					jobMap = make(map[*StratJob]bool)
					pairMap[p] = jobMap
				}
				jobMap[j] = true
			}
		}
	}
	return nil
}

func unRegWsJob(j *StratJob) {
	unRegWsJobWithState(LegacyState(), j)
}

func unRegWsJobWithState(strategyState *State, j *StratJob) {
	if strategyState == nil {
		strategyState = LegacyState()
	}
	strategyState.ensureMaps()
	lockJobsWriteForState(strategyState)
	unwatches := unRegWsJobLockedWithState(strategyState, j)
	callback := strategyState.WsSubUnWatchFunc()
	unlockJobsWriteForState(strategyState)
	if callback != nil && len(unwatches) > 0 {
		callback(unwatches)
	}
}

// unRegWsJobLocked mutates the legacy websocket registry while lockJobs is
// held. It returns the pairs that should be passed to the compatibility
// unwatch callback after the lock is released.
func unRegWsJobLocked(j *StratJob) {
	unRegWsJobLockedWithState(LegacyState(), j)
}

func unRegWsJobLockedWithState(strategyState *State, j *StratJob) map[string][]string {
	if strategyState == nil {
		strategyState = LegacyState()
	}
	strategyState.ensureMaps()
	unwatches := make(map[string][]string)
	for msgType, subPairs := range j.Strat.WsSubs {
		pairMap, ok := strategyState.WsSubJobs[msgType]
		if !ok {
			continue
		}
		pairArr := strings.Split(subPairs, ",")
		var removes []string
		for _, p := range pairArr {
			if p == "_cur_" || p == "" {
				p = j.Symbol.Symbol
			}
			if jobMap, ok := pairMap[p]; ok {
				delete(jobMap, j)
				if len(jobMap) == 0 {
					removes = append(removes, p)
				}
			}
		}
		if len(removes) > 0 {
			unwatches[msgType] = removes
		}
	}
	return unwatches
}

var polFilters = make(map[string][]goods.IFilter)

func strategyStorage(symbols *orm.SymbolState) *orm.Storage {
	if symbols == nil {
		return nil
	}
	return symbols.Storage()
}

func getPolicyPairs(pol *config.RunPolicyConfig, pairs []string) ([]string, *errs.Error) {
	return getPolicyPairsWithRuntimeState(nil, nil, nil, pol, pairs)
}

// getPolicyPairsWithStrategyState applies policy filters using the strategy
// state's immutable configuration and clock. Explicit runtimes must not read
// the process-wide filter cache or simulated time; the legacy wrapper below
// retains those defaults for callers that do not provide a State.
func getPolicyPairsWithStrategyState(strategyState *State, state *core.State, symbols *orm.SymbolState,
	exchange banexg.BanExchange, pol *config.RunPolicyConfig, pairs []string) ([]string, *errs.Error) {
	if strategyState == nil || strategyState == legacyState {
		return getPolicyPairsWithRuntimeState(state, symbols, exchange, pol, pairs)
	}
	if pol == nil {
		return nil, errs.NewMsg(errs.CodeParamRequired, "policy is required")
	}
	if state == nil {
		state = strategyState.Core
	}
	if symbols == nil {
		symbols = strategyState.Symbols
	}
	if exchange == nil {
		exchange = strategyState.Exchange
	}
	if len(pol.Pairs) > 0 {
		pairs = pol.Pairs
	}
	if len(pairs) == 0 || len(pol.Filters) == 0 {
		return pairs, nil
	}
	cfg := runtimeConfigFor(strategyState)
	if cfg == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "explicit strategy state requires runtime config")
	}
	strategyState.policyMu.Lock()
	filters := strategyState.policyFilters[pol.ID()]
	if filters == nil {
		created, err := goods.GetPairFiltersWithConfig(pol.Filters, false, cfg)
		if err != nil {
			strategyState.policyMu.Unlock()
			return nil, err
		}
		filters = created
		strategyState.policyFilters[pol.ID()] = filters
	}
	strategyState.policyMu.Unlock()
	curMS := runtimeTimeMSFor(strategyState)
	filterDeps := &goods.RuntimeDeps{
		Core: state, Clock: strategyState.Clock, Config: cfg,
		Symbols: symbols, Storage: strategyStorage(symbols), Exchange: exchange,
	}
	var err *errs.Error
	for _, flt := range filters {
		if flt == nil || flt.IsDisable() {
			continue
		}
		if runtimeFilter, ok := flt.(goods.RuntimeFilter); ok {
			pairs, err = runtimeFilter.FilterWithRuntimeDeps(filterDeps, pairs, curMS)
		} else if stateFilter, ok := flt.(goods.SymbolStateFilter); ok {
			if exchange == nil {
				return nil, errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required to filter strategy pairs")
			}
			pairs, err = stateFilter.FilterWithSymbolState(symbols, exchange, pairs, curMS)
		} else {
			pairs, err = flt.Filter(pairs, curMS)
		}
		if err != nil {
			return nil, err
		}
	}
	return pairs, nil
}

func getPolicyPairsWithSymbolState(symbols *orm.SymbolState, pol *config.RunPolicyConfig, pairs []string) ([]string, *errs.Error) {
	hooks := SnapshotPairUpdateHooks()
	exchange, _ := resolveStratExchange(nil, nil, symbols, hooks)
	return getPolicyPairsWithRuntimeState(nil, symbols, exchange, pol, pairs)
}

func getPolicyPairsWithRuntimeState(state *core.State, symbols *orm.SymbolState, exchange banexg.BanExchange,
	pol *config.RunPolicyConfig, pairs []string) ([]string, *errs.Error) {
	// According to pol.Pairs determine the tradable symbols
	// 根据pol.Pairs确定交易的标的
	if len(pol.Pairs) > 0 {
		pairs = pol.Pairs
	}
	if len(pairs) == 0 {
		return pairs, nil
	}
	if len(pol.Filters) > 0 {
		// Filter based on filters
		// 根据filters过滤筛选
		polID := pol.ID()
		filters, ok := polFilters[polID]
		var err *errs.Error
		if !ok {
			filters, err = goods.GetPairFilters(pol.Filters, false)
			if err != nil {
				return nil, err
			}
			polFilters[polID] = filters
		}
		curMS := btime.TimeMS()
		for _, flt := range filters {
			if stateFilter, ok := flt.(goods.SymbolStateFilter); ok && (state != nil || symbols != nil || exchange != nil) {
				if exchange == nil {
					return nil, errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required to filter strategy pairs")
				}
				pairs, err = stateFilter.FilterWithSymbolState(symbols, exchange, pairs, curMS)
			} else {
				pairs, err = flt.Filter(pairs, curMS)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return pairs, nil
}

func ListStrats(args []string) error {
	command := NewListStratsCommand()
	command.SetArgs(args)
	return command.Execute()
}

func NewListStratsCommand() *cobra.Command {
	var prefix string
	command := &cobra.Command{
		Use:     "list-strats",
		Aliases: []string{"list_strats"},
		Short:   "list registered strategies",
		Args:    cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return listStrats(prefix)
		},
	}
	command.Flags().StringVar(&prefix, "prefix", "", "strategy name prefix")
	return command
}

func listStrats(prefix string) error {
	arr := utils.KeysOfMap(StratMake)
	if prefix != "" {
		filtered := make([]string, 0, len(arr))
		for _, code := range arr {
			if strings.HasPrefix(code, prefix) {
				filtered = append(filtered, code)
			}
		}
		arr = filtered
	}
	sort.Strings(arr)
	fmt.Println(strings.Join(arr, "\n"))
	return nil
}
