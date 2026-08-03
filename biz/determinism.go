package biz

import (
	"cmp"
	"iter"
	"maps"
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
	return executionMapKeys(config.Accounts)
}

func executionStratJobs(jobs map[string]*strat.StratJob) iter.Seq[*strat.StratJob] {
	return func(yield func(*strat.StratJob) bool) {
		for key := range executionMapKeys(jobs) {
			if !yield(jobs[key]) {
				return
			}
		}
	}
}

func executionJobEnvs(jobs map[string]*strat.JobEnv) iter.Seq[*strat.JobEnv] {
	return func(yield func(*strat.JobEnv) bool) {
		for key := range executionMapKeys(jobs) {
			if !yield(jobs[key]) {
				return
			}
		}
	}
}

func executionOpenOrders(orders map[int64]*ormo.InOutOrder) []*ormo.InOutOrder {
	result := slices.Collect(maps.Values(orders))
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
