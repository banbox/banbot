package strat

import (
	"fmt"
	"slices"
	"strings"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/sasha-s/go-deadlock"
)

type PairUpdateReq struct {
	Strat         *TradeStrat
	StrategyState *State
	Core          *core.State
	Exchange      banexg.BanExchange
	Add           []string
	Remove        []string
	CloseOnRemove bool
	ForceAdd      bool
	Reason        string
}

type PairUpdateResult struct {
	Added      []string
	Removed    []string
	Skipped    []string
	ExitOrders map[string][]*ormo.InOutOrder
	Warnings   []string
}

type PairUpdateHooks struct {
	SubWarmPairs  func(items map[string]map[string]int, delOther bool) *errs.Error
	ExitOrders    func(acc string, orders []*ormo.InOutOrder, req *ExitReq) *errs.Error
	LookupSymbol  func(pair string) (*orm.ExSymbol, *errs.Error)
	Core          *core.State
	StrategyState *State
	SymbolState   *orm.SymbolState
	Exchange      banexg.BanExchange
}

// pairRemoval keeps a disabled job routable while its outstanding orders are
// being closed. Order callbacks can be synchronous with ExitOrders, so deleting
// the job before that hook runs would orphan the strategy's lifecycle state.
type pairRemoval struct {
	account string
	envKey  string
	name    string
	pair    string
	job     *StratJob
}

type PairUpdateManager struct {
	mu    deadlock.Mutex
	hooks PairUpdateHooks
}

var pairUpdateMgr = &PairUpdateManager{
	hooks: PairUpdateHooks{LookupSymbol: orm.GetExSymbolCur},
}

func SetPairUpdateHooks(h PairUpdateHooks) {
	pairUpdateMgr.mu.Lock()
	if h.LookupSymbol == nil {
		if h.SymbolState != nil {
			h.LookupSymbol = h.SymbolState.GetExSymbolCur
		} else {
			h.LookupSymbol = orm.GetExSymbolCur
		}
	}
	pairUpdateMgr.hooks = h
	pairUpdateMgr.mu.Unlock()
}

func SnapshotPairUpdateHooks() PairUpdateHooks {
	pairUpdateMgr.mu.Lock()
	defer pairUpdateMgr.mu.Unlock()
	return pairUpdateMgr.hooks
}

func PairUpdateHooksReady() bool {
	pairUpdateMgr.mu.Lock()
	defer pairUpdateMgr.mu.Unlock()
	return pairUpdateMgr.hooks.SubWarmPairs != nil
}

func (s *TradeStrat) UpdatePairs(req PairUpdateReq) (*PairUpdateResult, *errs.Error) {
	if req.Strat == nil {
		req.Strat = s
	}
	if req.Strat != s {
		return nil, errs.NewMsg(errs.CodeParamRequired, "req.Strat mismatch")
	}
	return pairUpdateMgr.Apply(req)
}

func resolveStratExchange(exchange banexg.BanExchange, state *core.State, symbols *orm.SymbolState, hooks PairUpdateHooks) (banexg.BanExchange, bool) {
	if exchange == nil {
		exchange = hooks.Exchange
	}
	explicit := exchange != nil || state != nil || symbols != nil || hooks.Core != nil || hooks.SymbolState != nil
	return exchange, explicit
}

func calcPairTfScoresForRuntime(strategyState *State, symbols *orm.SymbolState, exchange banexg.BanExchange, explicit bool, pairs []string) (map[string]map[string]float64, *errs.Error) {
	if !explicit {
		return CalcPairTfScores(nil, pairs)
	}
	if exchange == nil {
		return nil, errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required to score strategy pairs")
	}
	if strategyState != nil && strategyState != legacyState {
		return CalcPairTfScoresWithState(strategyState, symbols, exchange, pairs)
	}
	return CalcPairTfScoresWithSymbolState(symbols, exchange, pairs)
}

func parsePairsForState(state *State, pairs ...string) ([]string, *errs.Error) {
	if state == nil || state == legacyState {
		return config.ParsePairs(pairs...)
	}
	cfg := state.Config
	exchangeName, market, quote := "", "", ""
	if cfg != nil {
		if cfg.Exchange != nil {
			exchangeName = cfg.Exchange.Name
		}
		market = cfg.MarketType
		if len(cfg.StakeCurrency) > 0 {
			quote = cfg.StakeCurrency[0]
		}
	}
	if config.ExchangeUsesOpaqueSymbols(exchangeName) {
		return slices.Clone(pairs), nil
	}
	result := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		if strings.Contains(pair, "/") {
			result = append(result, pair)
			continue
		}
		if quote == "" {
			return nil, errs.NewMsg(core.ErrBadConfig, "`stake_currency` is required")
		}
		switch market {
		case banexg.MarketSpot:
			result = append(result, fmt.Sprintf("%s/%s", pair, quote))
		case banexg.MarketLinear:
			result = append(result, fmt.Sprintf("%s/%s:%s", pair, quote, quote))
		case banexg.MarketInverse:
			result = append(result, fmt.Sprintf("%s/%s:%s", pair, quote, pair))
		default:
			return nil, errs.NewMsg(core.ErrBadConfig, "option market don't support short pair")
		}
	}
	return result, nil
}

func (m *PairUpdateManager) Apply(req PairUpdateReq) (*PairUpdateResult, *errs.Error) {
	var hooks PairUpdateHooks
	if req.StrategyState != nil && req.StrategyState != legacyState {
		hooks = req.StrategyState.PairUpdateHooks()
	} else {
		m.mu.Lock()
		hooks = m.hooks
		m.mu.Unlock()
	}
	if hooks.SubWarmPairs == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "PairUpdateHooks.SubWarmPairs not set")
	}
	if req.Strat == nil || req.Strat.Policy == nil {
		return nil, errs.NewMsg(errs.CodeParamRequired, "Strat and Strat.Policy are required")
	}
	admissionState := req.Core
	if admissionState == nil {
		admissionState = hooks.Core
	}
	strategyState := req.StrategyState
	if strategyState == nil {
		strategyState = hooks.StrategyState
	}
	if strategyState == nil {
		strategyState = LegacyState()
	}
	explicitStrategyState := strategyState != legacyState
	if hooks.SymbolState == nil && explicitStrategyState {
		hooks.SymbolState = strategyState.Symbols
	}
	if hooks.LookupSymbol == nil {
		if hooks.SymbolState != nil {
			hooks.LookupSymbol = hooks.SymbolState.GetExSymbolCur
		} else if !explicitStrategyState {
			hooks.LookupSymbol = orm.GetExSymbolCur
		}
	}
	if explicitStrategyState && hooks.SubWarmPairs == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "explicit strategy state requires PairUpdateHooks.SubWarmPairs")
	}
	if explicitStrategyState && hooks.LookupSymbol == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "explicit strategy state requires PairUpdateHooks.LookupSymbol or SymbolState")
	}
	if explicitStrategyState && hooks.Exchange == nil {
		hooks.Exchange = strategyState.Exchange
	}
	strategyState.ensureMaps()
	exchange, runtimeExplicit := resolveStratExchange(req.Exchange, admissionState, hooks.SymbolState, hooks)
	var stgPairTfs map[string]map[string]string
	if admissionState != nil {
		admissionState.EnsureRuntimeMaps()
		stgPairTfs = admissionState.StgPairTfs
	} else {
		stgPairTfs = core.StgPairTfs
	}
	res := &PairUpdateResult{ExitOrders: map[string][]*ormo.InOutOrder{}}
	adds, err := parsePairsForState(strategyState, req.Add...)
	if err != nil {
		return nil, err
	}
	removes, err := parsePairsForState(strategyState, req.Remove...)
	if err != nil {
		return nil, err
	}
	lockJobsWriteForState(strategyState)
	locked := true
	defer func() {
		if locked {
			unlockJobsWriteForState(strategyState)
		}
	}()
	curMap, ok := stgPairTfs[req.Strat.Name]
	if !ok {
		curMap = map[string]string{}
		stgPairTfs[req.Strat.Name] = curMap
	}
	allowedSet := map[string]bool{}
	if !req.ForceAdd {
		basePairs := admissionPairs(admissionState)
		candidates := make([]string, 0, len(basePairs)+len(adds))
		seen := map[string]bool{}
		for _, pair := range basePairs {
			if !seen[pair] {
				seen[pair] = true
				candidates = append(candidates, pair)
			}
		}
		for _, pair := range adds {
			if !seen[pair] {
				seen[pair] = true
				candidates = append(candidates, pair)
			}
		}
		pol := *req.Strat.Policy
		pol.Pairs = nil
		allowedPairs, err := getPolicyPairsWithStrategyState(strategyState, admissionState, hooks.SymbolState, exchange, &pol, candidates)
		if err != nil {
			return nil, err
		}
		for _, p := range allowedPairs {
			allowedSet[p] = true
		}
	}
	removals := make([]pairRemoval, 0, len(removes))
	pendingUnwatches := make(map[string][]string)
	shutdownJobs := make([]*StratJob, 0, len(removes))
	pendingScores := map[string]bool{}
	for _, pair := range adds {
		if _, exists := curMap[pair]; exists {
			res.Skipped = append(res.Skipped, pair)
			continue
		}
		_, exErr := hooks.LookupSymbol(pair)
		if exErr != nil {
			res.Skipped = append(res.Skipped, pair)
			res.Warnings = append(res.Warnings, exErr.Short())
			continue
		}
		pendingScores[pair] = true
	}
	pairTfScores := map[string]map[string]float64{}
	if len(pendingScores) > 0 {
		pairs := utils.KeysOfMap(pendingScores)
		var scores map[string]map[string]float64
		var err *errs.Error
		// A dynamically supplied strategy may be updated before it is inserted
		// into the runtime registry. Use its own policy projection for scoring
		// rather than consulting the process-wide config.
		if runtimeExplicit && strategyState != nil && strategyState.Config == nil && req.Strat.Policy != nil && len(req.Strat.Policy.RunTimeframes) > 0 {
			if exchange == nil {
				return nil, errs.NewMsg(core.ErrExgNotInit, "runtime exchange is required to score strategy pairs")
			}
			cfg := &config.Config{
				RunTimeframes: req.Strat.Policy.RunTimeframes,
			}
			scores, err = calcPairTfScoresWithConfig(strategyState, cfg, hooks.SymbolState, exchange, runtimeTimeMSFor(strategyState), pairs)
		} else {
			scores, err = calcPairTfScoresForRuntime(strategyState, hooks.SymbolState, exchange, runtimeExplicit, pairs)
		}
		if err != nil {
			return nil, err
		}
		pairTfScores = scores
	}
	var accLimits accStratLimits
	if !req.ForceAdd {
		accLimits, _ = newAccStratLimitsForState(strategyState)
		for acc := range utils.MapKeys(strategyState.AccJobs, strictBacktestFor(strategyState, admissionState)) {
			jobsMap := strategyState.AccJobs[acc]
			for _, stgMap := range jobsMap {
				if _, ok := stgMap[req.Strat.Name]; ok {
					accLimits.tryAdd(acc, req.Strat.Name)
				}
			}
		}
	}
	logWarm := func(pair, tf string, num int) {}
	dirt := req.Strat.Policy.OdDirt()
	for _, pair := range adds {
		if _, exists := curMap[pair]; exists {
			continue
		}
		if !req.ForceAdd && !allowedSet[pair] {
			res.Skipped = append(res.Skipped, pair)
			res.Warnings = append(res.Warnings, "pair not allowed by pairlist/filters")
			continue
		}
		scores := pairTfScores[pair]
		tf := req.Strat.pickTimeFrame(pair, scores)
		if tf == "" {
			res.Skipped = append(res.Skipped, pair)
			res.Warnings = append(res.Warnings, "no valid timeframe")
			continue
		}
		exs, _ := hooks.LookupSymbol(pair)
		items, ok := strategyState.PairStrats[pair]
		if !ok {
			items = map[string]*TradeStrat{}
			strategyState.PairStrats[pair] = items
		}
		items[req.Strat.Name] = req.Strat
		curMap[pair] = tf
		enableAdmissionPair(admissionState, pair)
		env := initBarEnvWithState(strategyState, admissionState, exs, tf)
		ensureStratJobWithRuntimeState(strategyState, admissionState, req.Strat, tf, exs, env, dirt, logWarm, accLimits, hooks.SymbolState)
		if len(req.Strat.WsSubs) > 0 {
			envKey := pair + "_" + tf
			for acc := range utils.MapKeys(strategyState.AccJobs, strictBacktestFor(strategyState, admissionState)) {
				jobsMap := strategyState.AccJobs[acc]
				if stgMap, ok := jobsMap[envKey]; ok {
					if job := stgMap[req.Strat.Name]; job != nil {
						if err := regWsJobLockedWithState(strategyState, job); err != nil {
							return nil, err
						}
					}
				}
			}
		}
		var tfSecs map[string]int
		if admissionState != nil {
			tfSecs = admissionState.TFSecs
		} else {
			tfSecs = core.TFSecs
		}
		if _, ok := tfSecs[tf]; !ok {
			tfSecs[tf] = utils2.TFToSecs(tf)
		}
		res.Added = append(res.Added, pair)
	}
	for _, pair := range removes {
		tf, ok := curMap[pair]
		if !ok {
			res.Skipped = append(res.Skipped, pair)
			continue
		}
		envKey := pair + "_" + tf
		for acc := range utils.MapKeys(strategyState.AccJobs, strictBacktestFor(strategyState, admissionState)) {
			accJobs := strategyState.AccJobs[acc]
			if stgMap, ok := accJobs[envKey]; ok {
				if job, ok := stgMap[req.Strat.Name]; ok {
					if req.CloseOnRemove {
						job.SetOpenLimits(-1, -1)
						// Keep a disabled job routable until every outstanding order
						// reaches a terminal state. Live order events can arrive after
						// this method returns.
						job.SetPairRemovalPending(true)
						shutdownJobs = append(shutdownJobs, job)
						for msgType, pairs := range unRegWsJobLockedWithState(strategyState, job) {
							pendingUnwatches[msgType] = append(pendingUnwatches[msgType], pairs...)
						}
						removals = append(removals, pairRemoval{
							account: acc,
							envKey:  envKey,
							name:    req.Strat.Name,
							pair:    pair,
							job:     job,
						})
						snapshot := job.ExecutionSnapshot()
						if snapshot.EnteredNum > 0 || len(snapshot.LongOrders) > 0 || len(snapshot.ShortOrders) > 0 {
							res.ExitOrders[acc] = append(res.ExitOrders[acc], snapshot.LongOrders...)
							res.ExitOrders[acc] = append(res.ExitOrders[acc], snapshot.ShortOrders...)
						}
					} else {
						job.SetOpenLimits(-1, -1)
					}
				}
			}
		}
		if !req.CloseOnRemove {
			res.Removed = append(res.Removed, pair)
		}
	}
	unlockJobsWriteForState(strategyState)
	locked = false
	for _, job := range shutdownJobs {
		if job != nil && job.Strat != nil && job.Strat.OnShutDown != nil {
			job.Strat.OnShutDown(job)
		}
	}
	if callback := strategyState.WsSubUnWatchFunc(); callback != nil && len(pendingUnwatches) > 0 {
		callback(pendingUnwatches)
	}
	if req.CloseOnRemove {
		if hooks.ExitOrders == nil && len(res.ExitOrders) > 0 {
			return nil, errs.NewMsg(core.ErrRunTime, "ExitOrders hook is required to close removed pair orders")
		}
		for acc := range utils.MapKeys(res.ExitOrders, strictBacktestFor(strategyState, admissionState)) {
			orders := res.ExitOrders[acc]
			if len(orders) == 0 {
				continue
			}
			if err := hooks.ExitOrders(acc, orders, &ExitReq{Tag: core.ExitTagPairDel}); err != nil {
				return nil, err
			}
		}
		lockJobsWriteForState(strategyState)
		locked = true
		for _, removal := range removals {
			accJobs := strategyState.AccJobs[removal.account]
			if stgMap := accJobs[removal.envKey]; stgMap != nil && stgMap[removal.name] == removal.job {
				if jobHasOutstandingOrders(removal.job) {
					// The job remains in AccJobs as the callback route for the
					// pending exit. FinalizePairRotation removes it after the
					// terminal order event.
					res.Removed = append(res.Removed, removal.pair)
					continue
				}
				delete(stgMap, removal.name)
				if len(stgMap) == 0 {
					delete(accJobs, removal.envKey)
				}
			}
			delete(curMap, removal.pair)
			if items := strategyState.PairStrats[removal.pair]; items != nil && items[removal.name] == req.Strat {
				delete(items, removal.name)
				if len(items) == 0 {
					delete(strategyState.PairStrats, removal.pair)
				}
			}
			used := false
			for _, stgMap := range stgPairTfs {
				if _, ok := stgMap[removal.pair]; ok {
					used = true
					break
				}
			}
			if !used {
				setAdmissionPair(admissionState, removal.pair, false)
			}
			res.Removed = append(res.Removed, removal.pair)
		}
	}
	allWarms := collectAllWarmsLockedWithState(strategyState)
	if locked {
		unlockJobsWriteForState(strategyState)
		locked = false
	}
	if err := hooks.SubWarmPairs(allWarms, true); err != nil {
		return nil, err
	}
	return res, nil
}

func collectAllWarmsLocked() Warms {
	return collectAllWarmsLockedWithState(LegacyState())
}

func collectAllWarmsLockedWithState(strategyState *State) Warms {
	if strategyState == nil {
		strategyState = LegacyState()
	}
	strategyState.ensureMaps()
	all := make(Warms)
	for acc := range utils.MapKeys(strategyState.AccJobs, strictBacktestFor(strategyState, nil)) {
		accJobs := strategyState.AccJobs[acc]
		for _, stgMap := range accJobs {
			for _, job := range stgMap {
				pair := job.Symbol.Symbol
				tf := job.TimeFrame
				all.Update(pair, tf, job.Strat.WarmupNum)
				matchTf := strategyState.refineTimeFrame(job.Strat.Name, tf)
				all.Update(pair, matchTf, 0)
				for _, sub := range CollectDataSubs(job) {
					if sub == nil || orm.NormalizeSeriesSource(sub.Source) != orm.SeriesSourceKline || sub.ExSymbol == nil {
						continue
					}
					all.Update(sub.ExSymbol.Symbol, sub.TimeFrame, sub.WarmupNum)
				}
			}
		}
		break
	}
	return all
}
