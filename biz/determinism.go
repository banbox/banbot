package biz

import (
	"cmp"
	"iter"
	"slices"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
)

func executionMapKeys[M ~map[K]V, K cmp.Ordered, V any](items M, deps ...*RuntimeDeps) iter.Seq[K] {
	return utils.MapKeys(items, strictBacktestFor(deps...))
}

func strictBacktestFor(deps ...*RuntimeDeps) bool {
	if len(deps) > 0 {
		return deps[0].StrictBacktest()
	}
	return config.StrictBacktest()
}

func executionAccountConfigs(deps *RuntimeDeps) map[string]*config.AccountConfig {
	if deps != nil {
		return deps.AccountConfigs()
	}
	return config.Accounts
}

func executionAccountNames(deps ...*RuntimeDeps) iter.Seq[string] {
	var runtimeDeps *RuntimeDeps
	if len(deps) > 0 {
		runtimeDeps = deps[0]
	}
	accounts := executionAccountConfigs(runtimeDeps)
	if len(accounts) == 1 {
		for account := range accounts {
			return func(yield func(string) bool) {
				yield(account)
			}
		}
	}
	if strictBacktestFor(runtimeDeps) {
		return utils.MapKeys(accounts, true)
	}
	return func(yield func(string) bool) {
		for account := range accounts {
			if !yield(account) {
				return
			}
		}
	}
}

func executionStratJobs(jobs map[string]*strat.StratJob, deps ...*RuntimeDeps) iter.Seq[*strat.StratJob] {
	return func(yield func(*strat.StratJob) bool) {
		if strictBacktestFor(deps...) {
			for key := range executionMapKeys(jobs, deps...) {
				if !yield(jobs[key]) {
					return
				}
			}
			return
		}
		for _, job := range jobs {
			if !yield(job) {
				return
			}
		}
	}
}

func executionJobEnvs(jobs map[string]*strat.JobEnv, deps ...*RuntimeDeps) iter.Seq[*strat.JobEnv] {
	return func(yield func(*strat.JobEnv) bool) {
		if strictBacktestFor(deps...) {
			for key := range executionMapKeys(jobs, deps...) {
				if !yield(jobs[key]) {
					return
				}
			}
			return
		}
		for _, job := range jobs {
			if !yield(job) {
				return
			}
		}
	}
}

func executionOpenOrders(orders map[int64]*ormo.InOutOrder, deps ...*RuntimeDeps) []*ormo.InOutOrder {
	result := utils.ValsOfMap(orders)
	// Maps have no supplied order to preserve, including during frozen replays.
	if strictBacktestFor(deps...) {
		slices.SortFunc(result, func(a, b *ormo.InOutOrder) int {
			if order := cmp.Compare(a.RealEnterMS(), b.RealEnterMS()); order != 0 {
				return order
			}
			return cmp.Compare(a.ID, b.ID)
		})
	}
	return result
}

func preserveFrozenReplayExecutionOrder(deps ...*RuntimeDeps) bool {
	if len(deps) > 0 && deps[0] != nil {
		cfg := deps[0].ConfigView()
		if cfg == nil {
			return false
		}
		return isFrozenRuntimePairs(cfg, deps[0].StrictBacktest())
	}
	pairs, _ := config.GetStaticPairs()
	return config.IsFrozenStaticPairs(pairs)
}

func executionOrderView(orders []*ormo.InOutOrder, deps ...*RuntimeDeps) []*ormo.InOutOrder {
	if !strictBacktestFor(deps...) {
		if len(deps) > 0 && deps[0] != nil {
			return orders
		}
		return legacyWalletOrderView(orders)
	}
	if preserveFrozenReplayExecutionOrder(deps...) {
		return orders
	}
	if len(deps) > 0 && deps[0] != nil {
		return orders
	}
	return legacyWalletOrderView(orders)
}

func isFrozenRuntimePairs(cfg *config.Config, strict bool) bool {
	return cfg != nil && len(cfg.Pairs) > 0 && len(cfg.PairFilters) == 0 &&
		(cfg.PairMgr == nil || !cfg.PairMgr.ForceFilters) && strict && cfg.BTNoKlineDownload
}
