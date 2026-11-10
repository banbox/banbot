package biz

import (
	"slices"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

// enableDefaultBacktest turns on a plain backtest: bt_strict / bt_legacy_* all OFF (the production trendlab / bansite
// configuration before issue 111).
func enableDefaultBacktest(t *testing.T) {
	t.Helper()
	oldMode, oldData := core.BackTestMode, config.Data
	core.BackTestMode = true
	config.Data.BTStrict = false
	config.Data.BTLegacyWallet = false
	t.Cleanup(func() {
		core.BackTestMode = oldMode
		config.Data = oldData
	})
}

// Issue 111: many symbols trigger on the same bar and the capital only admits a part of them. The admitted set
// must not depend on Go's randomized map iteration, even without bt_strict.
func TestDefaultBacktestCapitalCompetitionIsRepeatable(t *testing.T) {
	enableDefaultBacktest(t)
	const symbols = 24
	var want []int64
	for run := 0; run < 200; run++ {
		jobs := make(map[string]*strat.StratJob, symbols)
		open := make(map[int64]*ormo.InOutOrder, symbols)
		for i := 0; i < symbols; i++ {
			id := int64(i + 1)
			key := string(rune('A'+i)) + "/USDT:USDT_15m"
			jobs[key] = &strat.StratJob{TimeFrame: key}
			// the same enter time for every symbol: only the ID breaks the tie
			open[id] = &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: id, EnterAt: 1_700_000_000_000}}
		}
		var jobOrder []string
		for job := range executionStratJobs(jobs) {
			jobOrder = append(jobOrder, job.TimeFrame)
		}
		if !slices.IsSorted(jobOrder) {
			t.Fatalf("run %d: jobs are not visited in key order: %v", run, jobOrder)
		}
		// capital for 5 orders only: the first 5 in execution order are admitted
		var admitted []int64
		budget := 5.0
		for _, od := range executionOpenOrders(open) {
			if budget >= 1 {
				budget--
				admitted = append(admitted, od.ID)
			}
		}
		if want == nil {
			want = admitted
		}
		if !slices.Equal(admitted, want) || !slices.Equal(want, []int64{1, 2, 3, 4, 5}) {
			t.Fatalf("run %d: admitted %v, want %v", run, admitted, want)
		}
	}
}

func TestDefaultBacktestAccountsAndRuntimeDepsAreOrdered(t *testing.T) {
	enableDefaultBacktest(t)
	oldAccounts := config.Accounts
	config.Accounts = map[string]*config.AccountConfig{"z": {}, "a": {}, "m": {}, "b": {}}
	t.Cleanup(func() { config.Accounts = oldAccounts })
	for range 50 {
		if got := slices.Collect(executionAccountNames()); !slices.Equal(got, []string{"a", "b", "m", "z"}) {
			t.Fatalf("accounts = %v", got)
		}
	}
	// live mode keeps the cheap unordered walk
	core.BackTestMode = false
	if orderedBacktestFor() {
		t.Fatal("non-backtest must not pay for ordering")
	}
}
