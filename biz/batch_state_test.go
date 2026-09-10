package biz

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
	ta "github.com/banbox/banta"
)

const fireAllBatchMS = int64(^uint64(0) >> 1)

func batchTestJob(strategy *strat.TradeStrat, symbol string) *strat.StratJob {
	return &strat.StratJob{Strat: strategy, Env: &ta.BarEnv{}, Symbol: &orm.ExSymbol{Symbol: symbol}}
}

type batchRuntimeOrderMgr struct {
	IOrderMgr
	calls int
}

func (m *batchRuntimeOrderMgr) ProcessOrders(*strat.StratJob) ([]*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error) {
	m.calls++
	return nil, nil, nil
}

func TestBatchStatesAreIsolated(t *testing.T) {
	first, second := strat.NewBatchState(), strat.NewBatchState()
	var firstCalls, secondCalls int
	firstStrategy := &strat.TradeStrat{Name: "first", OnBatchJobs: func([]*strat.StratJob) { firstCalls++ }}
	secondStrategy := &strat.TradeStrat{Name: "second", OnBatchJobs: func([]*strat.StratJob) { secondCalls++ }}
	AddBatchJobWithState(first, "default", "1m", batchTestJob(firstStrategy, "BTC/USDT"), nil)
	AddBatchJobWithState(second, "default", "1m", batchTestJob(secondStrategy, "ETH/USDT"), nil)

	TryFireBatchesWithState(first, fireAllBatchMS, true)
	if firstCalls != 1 || secondCalls != 0 || first.PendingCount() != 0 || second.PendingCount() != 1 {
		t.Fatalf("states leaked: calls=%d/%d pending=%d/%d", firstCalls, secondCalls, first.PendingCount(), second.PendingCount())
	}
	first.SetLastBatchMS(100)
	second.SetLastBatchMS(200)
	if first.LastBatchMS() != 100 || second.LastBatchMS() != 200 {
		t.Fatalf("timestamps leaked: first=%d second=%d", first.LastBatchMS(), second.LastBatchMS())
	}
}

func TestRuntimeBatchAdmissionUsesOwnedClock(t *testing.T) {
	oldTime := btime.CurTimeMS
	btime.SetTimeMS(1)
	t.Cleanup(func() { btime.SetTimeMS(oldTime) })

	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(100_000)
	state := strat.NewBatchState()
	strategy := &strat.TradeStrat{Name: "runtime-clock"}
	AddBatchJobWithRuntimeDeps(&RuntimeDeps{Clock: clock}, state, "default", "1m",
		batchTestJob(strategy, "BTC/USDT"), nil)

	ready, _ := state.TakeReady(btime.CurTimeMS+core.DelayBatchMS+1, false)
	if len(ready) != 0 {
		t.Fatalf("runtime batch became ready at legacy time: %d", len(ready))
	}
	ready, _ = state.TakeReady(clock.TimeMS()+core.DelayBatchMS+1, false)
	if len(ready) != 1 {
		t.Fatalf("runtime batch ready count = %d, want 1", len(ready))
	}
}

func TestTraderZeroValueBatchStateInitializesOnceConcurrently(t *testing.T) {
	var trader Trader
	const callers = 64
	states := make(chan *strat.BatchState, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			states <- trader.BatchState()
		}()
	}
	close(start)
	wg.Wait()
	close(states)

	var first *strat.BatchState
	for state := range states {
		if state == nil {
			t.Fatal("zero-value trader returned nil batch state")
		}
		if first == nil {
			first = state
		} else if state != first {
			t.Fatalf("zero-value trader initialized multiple batch states: %p and %p", first, state)
		}
	}
}

func TestTypedBatchAPIsDoNotFallbackToLegacyState(t *testing.T) {
	tasks, lastMS := strat.BackupLegacyBatchState()
	strat.LegacyBatchState().Reset()
	t.Cleanup(func() { strat.RestoreLegacyBatchState(tasks, lastMS) })
	strategy := &strat.TradeStrat{Name: "typed-nil"}
	job := batchTestJob(strategy, "BTC/USDT")

	AddBatchJobWithState(nil, "default", "1m", job, nil)
	if got := strat.LegacyBatchState().PendingCount(); got != 0 {
		t.Fatalf("nil typed state touched legacy queue: pending=%d", got)
	}
	if got := TryFireBatchesWithState(nil, fireAllBatchMS, true); got != 0 {
		t.Fatalf("nil typed state fired legacy queue: wait=%d", got)
	}
}

func TestRuntimeBatchPathUsesTypedOrderManager(t *testing.T) {
	oldManagers := accOdMgrs
	accOdMgrs = make(map[string]IOrderMgr)
	t.Cleanup(func() { accOdMgrs = oldManagers })

	state := strat.NewBatchState()
	manager := &batchRuntimeOrderMgr{}
	trading := NewTradingState()
	trading.OrderManagers["typed-account"] = manager
	deps := &RuntimeDeps{
		Orders:         ormo.NewOrderState(),
		Trading:        trading,
		DefaultAccount: "typed-account",
	}
	strategy := &strat.TradeStrat{Name: "typed-runtime-batch", OnBatchJobs: func([]*strat.StratJob) {}}
	AddBatchJobWithState(state, "typed-account", "1m", batchTestJob(strategy, "BTC/USDT"), nil)

	TryFireBatchesWithRuntimeDeps(deps, state, fireAllBatchMS, false)

	if manager.calls != 1 {
		t.Fatalf("typed order manager calls = %d, want 1", manager.calls)
	}
}

func TestRuntimeBatchPathDoesNotFallbackWhenTypedStateIsIncomplete(t *testing.T) {
	oldManagers := accOdMgrs
	legacyManager := &batchRuntimeOrderMgr{}
	accOdMgrs = map[string]IOrderMgr{"legacy-account": legacyManager}
	t.Cleanup(func() { accOdMgrs = oldManagers })

	state := strat.NewBatchState()
	var callbackCalls int
	strategy := &strat.TradeStrat{
		Name:        "incomplete-runtime-batch",
		OnBatchJobs: func([]*strat.StratJob) { callbackCalls++ },
	}
	AddBatchJobWithState(state, "legacy-account", "1m", batchTestJob(strategy, "BTC/USDT"), nil)

	if got := TryFireBatchesWithRuntimeDeps(&RuntimeDeps{}, state, fireAllBatchMS, false); got != 0 {
		t.Fatalf("incomplete runtime returned wait count %d, want 0", got)
	}
	if callbackCalls != 0 || legacyManager.calls != 0 || state.PendingCount() != 1 {
		t.Fatalf("incomplete runtime touched another state: callbacks=%d manager=%d pending=%d",
			callbackCalls, legacyManager.calls, state.PendingCount())
	}
}

func TestBatchStateConcurrentAdd(t *testing.T) {
	state := strat.NewBatchState()
	var fired atomic.Int64
	strategy := &strat.TradeStrat{Name: "concurrent", OnBatchJobs: func(jobs []*strat.StratJob) {
		fired.Add(int64(len(jobs)))
	}}
	const jobCount = 100
	var wg sync.WaitGroup
	for i := 0; i < jobCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			AddBatchJobWithState(state, "default", "1m", batchTestJob(strategy, fmt.Sprintf("PAIR-%d", index)), nil)
		}(i)
	}
	wg.Wait()
	TryFireBatchesWithState(state, fireAllBatchMS, true)
	if got := fired.Load(); got != jobCount {
		t.Fatalf("fired jobs = %d, want %d", got, jobCount)
	}
}

func TestBatchCallbackCanReenterSameState(t *testing.T) {
	state := strat.NewBatchState()
	reentered := make(chan struct{})
	nextStrategy := &strat.TradeStrat{Name: "next", OnBatchJobs: func([]*strat.StratJob) {}}
	strategy := &strat.TradeStrat{Name: "reenter"}
	strategy.OnBatchJobs = func([]*strat.StratJob) {
		AddBatchJobWithState(state, "default", "1m", batchTestJob(nextStrategy, "ETH/USDT"), nil)
		close(reentered)
	}
	AddBatchJobWithState(state, "default", "1m", batchTestJob(strategy, "BTC/USDT"), nil)
	done := make(chan struct{})
	go func() {
		TryFireBatchesWithState(state, fireAllBatchMS, true)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("batch callback reentry deadlocked")
	}
	select {
	case <-reentered:
	default:
		t.Fatal("batch callback did not reenter")
	}
	if state.PendingCount() != 1 {
		t.Fatalf("pending batches = %d, want 1 reentered batch", state.PendingCount())
	}
}

func TestLegacyBatchFacadeStillWorks(t *testing.T) {
	tasks, lastMS := strat.BackupLegacyBatchState()
	strat.LegacyBatchState().Reset()
	t.Cleanup(func() { strat.RestoreLegacyBatchState(tasks, lastMS) })
	var calls int
	strategy := &strat.TradeStrat{Name: "legacy", OnBatchJobs: func([]*strat.StratJob) { calls++ }}
	AddBatchJob("default", "1m", batchTestJob(strategy, "BTC/USDT"), nil)
	TryFireBatches(fireAllBatchMS, true)
	if calls != 1 || strat.LegacyBatchState().PendingCount() != 0 {
		t.Fatalf("legacy facade calls=%d pending=%d", calls, strat.LegacyBatchState().PendingCount())
	}
}

func TestBackupRestoreVarsPreservesLegacyBatchState(t *testing.T) {
	original := BackupVars()
	t.Cleanup(func() { RestoreVars(original) })
	ResetVars()
	strategy := &strat.TradeStrat{Name: "backup", OnBatchJobs: func([]*strat.StratJob) {}}
	AddBatchJob("default", "1m", batchTestJob(strategy, "BTC/USDT"), nil)
	strat.LegacyBatchState().SetLastBatchMS(42)
	backup := BackupVars()
	ResetVars()
	if strat.LegacyBatchState().PendingCount() != 0 || strat.LegacyBatchState().LastBatchMS() != 0 {
		t.Fatal("ResetVars did not reset legacy batch state")
	}
	RestoreVars(backup)
	if strat.LegacyBatchState().PendingCount() != 1 || strat.LegacyBatchState().LastBatchMS() != 42 {
		t.Fatalf("restored legacy batch pending=%d last=%d", strat.LegacyBatchState().PendingCount(), strat.LegacyBatchState().LastBatchMS())
	}
}
