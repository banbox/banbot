package strat

import (
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/sasha-s/go-deadlock"
)

type PairUpdateReq struct {
	Strat         *TradeStrat
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
	SubWarmPairs func(items map[string]map[string]int, delOther bool) *errs.Error
	ExitOrders   func(acc string, orders []*ormo.InOutOrder, req *ExitReq) *errs.Error
	LookupSymbol func(pair string) (*orm.ExSymbol, *errs.Error)
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
		h.LookupSymbol = orm.GetExSymbolCur
	}
	pairUpdateMgr.hooks = h
	pairUpdateMgr.mu.Unlock()
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

func (m *PairUpdateManager) Apply(req PairUpdateReq) (*PairUpdateResult, *errs.Error) {
	m.mu.Lock()
	hooks := m.hooks
	m.mu.Unlock()
	if hooks.SubWarmPairs == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "PairUpdateHooks.SubWarmPairs not set")
	}
	if req.Strat == nil || req.Strat.Policy == nil {
		return nil, errs.NewMsg(errs.CodeParamRequired, "Strat and Strat.Policy are required")
	}
	if hooks.LookupSymbol == nil {
		hooks.LookupSymbol = orm.GetExSymbolCur
	}
	res := &PairUpdateResult{ExitOrders: map[string][]*ormo.InOutOrder{}}
	adds, err := config.ParsePairs(req.Add...)
	if err != nil {
		return nil, err
	}
	removes, err := config.ParsePairs(req.Remove...)
	if err != nil {
		return nil, err
	}
	lockJobs.Lock()
	defer lockJobs.Unlock()
	curMap, ok := core.StgPairTfs[req.Strat.Name]
	if !ok {
		curMap = map[string]string{}
		core.StgPairTfs[req.Strat.Name] = curMap
	}
	allowedSet := map[string]bool{}
	if !req.ForceAdd {
		allowedPairs, err := getPolicyPairs(req.Strat.Policy, core.Pairs)
		if err != nil {
			return nil, err
		}
		for _, p := range allowedPairs {
			allowedSet[p] = true
		}
	}
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
		scores, err := CalcPairTfScores(exg.Default, pairs)
		if err != nil {
			return nil, err
		}
		pairTfScores = scores
	}
	var accLimits accStratLimits
	if !req.ForceAdd {
		accLimits, _ = newAccStratLimits()
		for acc, jobsMap := range AccJobs {
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
		items, ok := PairStrats[pair]
		if !ok {
			items = map[string]*TradeStrat{}
			PairStrats[pair] = items
		}
		items[req.Strat.Name] = req.Strat
		curMap[pair] = tf
		if _, ok := core.PairsMap[pair]; !ok {
			core.PairsMap[pair] = true
			core.Pairs = append(core.Pairs, pair)
		}
		env := initBarEnv(exs, tf)
		ensureStratJob(req.Strat, tf, exs, env, dirt, logWarm, accLimits)
		if len(req.Strat.WsSubs) > 0 {
			envKey := pair + "_" + tf
			for _, jobsMap := range AccJobs {
				if stgMap, ok := jobsMap[envKey]; ok {
					if job := stgMap[req.Strat.Name]; job != nil {
						if err := regWsJob(job); err != nil {
							return nil, err
						}
					}
				}
			}
		}
		if _, ok := core.TFSecs[tf]; !ok {
			core.TFSecs[tf] = utils2.TFToSecs(tf)
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
		for acc, accJobs := range AccJobs {
			if stgMap, ok := accJobs[envKey]; ok {
				if job, ok := stgMap[req.Strat.Name]; ok {
					if req.CloseOnRemove {
						if job.Strat.OnShutDown != nil {
							job.Strat.OnShutDown(job)
						}
						unRegWsJob(job)
						if job.EnteredNum > 0 {
							res.ExitOrders[acc] = append(res.ExitOrders[acc], job.LongOrders...)
							res.ExitOrders[acc] = append(res.ExitOrders[acc], job.ShortOrders...)
						}
						delete(stgMap, req.Strat.Name)
					} else {
						job.MaxOpenLong = -1
						job.MaxOpenShort = -1
					}
					if len(stgMap) == 0 {
						delete(accJobs, envKey)
					}
				}
			}
		}
		if req.CloseOnRemove {
			delete(curMap, pair)
			if items, ok := PairStrats[pair]; ok {
				delete(items, req.Strat.Name)
				if len(items) == 0 {
					delete(PairStrats, pair)
				}
			}
			used := false
			for _, stgMap := range core.StgPairTfs {
				if _, ok := stgMap[pair]; ok {
					used = true
					break
				}
			}
			if !used {
				core.PairsMap[pair] = false
			}
		}
		res.Removed = append(res.Removed, pair)
	}
	if req.CloseOnRemove && hooks.ExitOrders != nil {
		for acc, orders := range res.ExitOrders {
			_ = hooks.ExitOrders(acc, orders, &ExitReq{Tag: core.ExitTagPairDel})
		}
	}
	allWarms := collectAllWarmsLocked()
	if err := hooks.SubWarmPairs(allWarms, true); err != nil {
		return nil, err
	}
	return res, nil
}

func collectAllWarmsLocked() Warms {
	all := make(Warms)
	for _, accJobs := range AccJobs {
		for _, stgMap := range accJobs {
			for _, job := range stgMap {
				pair := job.Symbol.Symbol
				tf := job.TimeFrame
				all.Update(pair, tf, job.Strat.WarmupNum)
				matchTf, _ := config.GetStratRefineTF(job.Strat.Name, tf)
				all.Update(pair, matchTf, 0)
				if job.Strat.OnPairInfos != nil {
					for _, sub := range job.Strat.OnPairInfos(job) {
						p := sub.Pair
						if p == "_cur_" {
							p = pair
						}
						all.Update(p, sub.TimeFrame, sub.WarmupNum)
					}
				}
			}
		}
		break
	}
	return all
}
