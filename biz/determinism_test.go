package biz

import (
	"slices"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	ta "github.com/banbox/banta"
)

func enableStrictBacktest(t *testing.T) {
	t.Helper()
	oldMode, oldData := core.BackTestMode, config.Data
	core.BackTestMode = true
	config.Data.BTStrict = true
	t.Cleanup(func() {
		core.BackTestMode = oldMode
		config.Data = oldData
	})
}

func orderIDs(orders []*ormo.InOutOrder) []int64 {
	ids := make([]int64, len(orders))
	for i, order := range orders {
		ids[i] = order.ID
	}
	return ids
}

func TestDeterministicExecutionViewsUseStableBusinessKeys(t *testing.T) {
	enableStrictBacktest(t)
	oldAccounts := config.Accounts
	config.Accounts = map[string]*config.AccountConfig{"z": {}, "a": {}, "m": {}}
	t.Cleanup(func() { config.Accounts = oldAccounts })
	if got := slices.Collect(executionAccountNames()); !slices.Equal(got, []string{"a", "m", "z"}) {
		t.Fatalf("accounts = %v", got)
	}

	jobs := map[string]*strat.StratJob{
		"z": {TimeFrame: "z"}, "a": {TimeFrame: "a"}, "m": {TimeFrame: "m"},
	}
	gotJobs := slices.Collect(executionStratJobs(jobs))
	if gotJobs[0].TimeFrame != "a" || gotJobs[1].TimeFrame != "m" || gotJobs[2].TimeFrame != "z" {
		t.Fatalf("jobs = %#v", gotJobs)
	}

	orders := map[int64]*ormo.InOutOrder{
		3: {IOrder: &ormo.IOrder{ID: 3, EnterAt: 20}},
		2: {IOrder: &ormo.IOrder{ID: 2, EnterAt: 10}},
		1: {IOrder: &ormo.IOrder{ID: 1, EnterAt: 10}},
	}
	if got := orderIDs(executionOpenOrders(orders)); !slices.Equal(got, []int64{1, 2, 3}) {
		t.Fatalf("orders = %v", got)
	}
}

func TestFrozenReplayDisablesCanonicalOrderExecution(t *testing.T) {
	oldMode, oldData := core.BackTestMode, config.Data
	oldPairs, oldFilters, oldMgr := config.Pairs, config.PairFilters, config.PairMgr
	core.BackTestMode = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true
	config.Pairs = []string{"BTC/USDT:USDT", "DOGE/USDT:USDT"}
	config.PairFilters = nil
	config.PairMgr = &config.PairMgrConfig{}
	t.Cleanup(func() {
		core.BackTestMode, config.Data = oldMode, oldData
		config.Pairs, config.PairFilters, config.PairMgr = oldPairs, oldFilters, oldMgr
	})

	if canonicalExecutionOrder() {
		t.Fatal("frozen strict replay enabled canonical execution ordering")
	}
	orders := []*ormo.InOutOrder{
		{IOrder: &ormo.IOrder{ID: 3}},
		{IOrder: &ormo.IOrder{ID: 1}},
		{IOrder: &ormo.IOrder{ID: 2}},
	}
	got := executionOrderView(orders)
	if !slices.Equal(orderIDs(got), []int64{3, 1, 2}) || &got[0] != &orders[0] {
		t.Fatalf("execution order = %v, want supplied order [3 1 2]", orderIDs(got))
	}
}

func TestTryFireBatchesUsesStableTaskOrderWhenDeterministic(t *testing.T) {
	enableStrictBacktest(t)
	oldTasks := strat.BatchTasks
	t.Cleanup(func() { strat.BatchTasks = oldTasks })

	for shift := 0; shift < 3; shift++ {
		var got []string
		strategy := &strat.TradeStrat{Name: "demo", OnBatchJobs: func(jobs []*strat.StratJob) {
			for _, job := range jobs {
				got = append(got, job.Symbol.Symbol)
			}
		}}
		keys := []string{"z_main", "a_main", "m_main"}
		symbols := map[string]string{"z_main": "z", "a_main": "a", "m_main": "m"}
		tasks := &strat.BatchMap{Map: make(map[string]*strat.JobEnv), TFMSecs: 3_600_000}
		for offset := range keys {
			key := keys[(shift+offset)%len(keys)]
			job := &strat.StratJob{Strat: strategy, Env: &ta.BarEnv{}, Symbol: &orm.ExSymbol{Symbol: symbols[key]}}
			tasks.Map[key] = &strat.JobEnv{Job: job, Symbol: symbols[key]}
		}
		strat.BatchTasks = map[string]*strat.BatchMap{"1h_default_demo": tasks}
		TryFireBatches(1, true)
		if !slices.Equal(got, []string{"a", "m", "z"}) {
			t.Fatalf("layout %d batch jobs = %v", shift, got)
		}
	}
}

func TestDataOnlySeriesUsesStableAccountAndJobOrderWhenDeterministic(t *testing.T) {
	enableStrictBacktest(t)
	oldAccounts, oldInfoJobs, oldEnvReal := config.Accounts, strat.AccInfoJobs, core.EnvReal
	config.Accounts = map[string]*config.AccountConfig{"z": {}, "a": {}}
	strat.AccInfoJobs = make(map[string]map[string]map[string]*strat.StratJob)
	core.EnvReal = true
	t.Cleanup(func() {
		config.Accounts, strat.AccInfoJobs, core.EnvReal = oldAccounts, oldInfoJobs, oldEnvReal
	})

	evt := &orm.DataSeries{Source: "macro", Sid: 1, TimeFrame: "1d", Values: map[string]interface{}{"value": 1.0}}
	subKey := strat.DataSubKey(evt.Source, evt.Sid, evt.TimeFrame)
	var got []string
	for _, account := range []string{"z", "a"} {
		jobs := make(map[string]*strat.StratJob)
		for _, key := range []string{"z", "a"} {
			label := account + key
			jobs[key] = &strat.StratJob{
				Strat: &strat.TradeStrat{OnData: func(job *strat.StratJob, _ strat.DataEvent) {
					got = append(got, job.More.(string))
				}},
				DataHub: strat.NewDataHub(), Symbol: &orm.ExSymbol{ID: 1, Symbol: "macro"}, More: label,
			}
		}
		strat.AccInfoJobs[account] = map[string]map[string]*strat.StratJob{subKey: jobs}
	}
	if err := (&Trader{}).feedDataOnlySeries(evt); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"aa", "az", "za", "zz"}) {
		t.Fatalf("data-only callbacks = %v", got)
	}
}
