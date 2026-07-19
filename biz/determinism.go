package biz

import (
	"cmp"
	"maps"
	"slices"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

func sortedAccountNames() []string {
	return slices.Sorted(maps.Keys(config.Accounts))
}

func sortedStratJobs(jobs map[string]*strat.StratJob) []*strat.StratJob {
	result := make([]*strat.StratJob, 0, len(jobs))
	for _, key := range slices.Sorted(maps.Keys(jobs)) {
		result = append(result, jobs[key])
	}
	return result
}

func sortedJobEnvs(jobs map[string]*strat.JobEnv) []*strat.JobEnv {
	result := make([]*strat.JobEnv, 0, len(jobs))
	for _, key := range slices.Sorted(maps.Keys(jobs)) {
		result = append(result, jobs[key])
	}
	return result
}

func sortedOpenOrders(orders map[int64]*ormo.InOutOrder) []*ormo.InOutOrder {
	result := slices.Collect(maps.Values(orders))
	slices.SortFunc(result, func(a, b *ormo.InOutOrder) int {
		if order := cmp.Compare(a.RealEnterMS(), b.RealEnterMS()); order != 0 {
			return order
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return result
}
