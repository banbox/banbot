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

func executionMapKeys[M ~map[K]V, K cmp.Ordered, V any](items M) iter.Seq[K] {
	return utils.MapKeys(items, config.StrictBacktest())
}

func executionAccountNames() iter.Seq[string] {
	if len(config.Accounts) == 1 {
		for account := range config.Accounts {
			return func(yield func(string) bool) {
				yield(account)
			}
		}
	}
	if config.StrictBacktest() {
		return utils.MapKeys(config.Accounts, true)
	}
	return func(yield func(string) bool) {
		for account := range config.Accounts {
			if !yield(account) {
				return
			}
		}
	}
}

func executionStratJobs(jobs map[string]*strat.StratJob) iter.Seq[*strat.StratJob] {
	return func(yield func(*strat.StratJob) bool) {
		if config.StrictBacktest() {
			for key := range executionMapKeys(jobs) {
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

func executionJobEnvs(jobs map[string]*strat.JobEnv) iter.Seq[*strat.JobEnv] {
	return func(yield func(*strat.JobEnv) bool) {
		if config.StrictBacktest() {
			for key := range executionMapKeys(jobs) {
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

func executionOpenOrders(orders map[int64]*ormo.InOutOrder) []*ormo.InOutOrder {
	result := utils.ValsOfMap(orders)
	// Maps have no supplied order to preserve, including during frozen replays.
	if config.StrictBacktest() {
		slices.SortFunc(result, func(a, b *ormo.InOutOrder) int {
			if order := cmp.Compare(a.RealEnterMS(), b.RealEnterMS()); order != 0 {
				return order
			}
			return cmp.Compare(a.ID, b.ID)
		})
	}
	return result
}

func preserveFrozenReplayExecutionOrder() bool {
	pairs, _ := config.GetStaticPairs()
	return config.IsFrozenStaticPairs(pairs)
}

func executionOrderView(orders []*ormo.InOutOrder) []*ormo.InOutOrder {
	if !config.StrictBacktest() {
		return legacyWalletOrderView(orders)
	}
	if preserveFrozenReplayExecutionOrder() {
		return orders
	}
	return legacyWalletOrderView(orders)
}
