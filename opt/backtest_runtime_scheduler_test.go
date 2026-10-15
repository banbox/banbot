package opt

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/cron/v3"
)

type backtestSchedulerProbe struct {
	startCalls atomic.Int32
	stopCalls  atomic.Int32
}

func (s *backtestSchedulerProbe) AddFunc(string, func()) (cron.EntryID, error) {
	return 0, nil
}

func (s *backtestSchedulerProbe) Start() {
	s.startCalls.Add(1)
}

func (s *backtestSchedulerProbe) Stop() context.Context {
	s.stopCalls.Add(1)
	return context.Background()
}

func TestBackTestExplicitRuntimeUsesOneSchedulerWhenMissingDependency(t *testing.T) {
	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()

	trader := newBacktestTraderForTest(t, biz.RuntimeDeps{
		Core:   state,
		Config: config.NewSnapshot(&config.Config{}),
	})
	backtest := &BackTest{BackTestLite: &BackTestLite{Trader: trader}}

	first := backtest.schedulerForRun()
	second := backtest.schedulerForRun()
	if first == nil || second == nil {
		t.Fatal("explicit backtest did not create a scheduler")
	}
	if first != second {
		t.Fatalf("schedulerForRun returned different instances: %p and %p", first, second)
	}
	if backtest.runScheduler != first {
		t.Fatal("backtest did not retain its run scheduler")
	}
	if !backtest.ownsRunScheduler() {
		t.Fatal("backtest did not retain ownership of its private fallback scheduler")
	}
}

func TestBackTestDoesNotOwnSuppliedRuntimeScheduler(t *testing.T) {
	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()

	scheduler := &backtestSchedulerProbe{}
	trader := newBacktestTraderForTest(t, biz.RuntimeDeps{
		Core:      state,
		Config:    config.NewSnapshot(&config.Config{}),
		Scheduler: scheduler,
	})
	backtest := &BackTest{BackTestLite: &BackTestLite{Trader: trader}}
	if got := backtest.schedulerForRun(); got != scheduler {
		t.Fatalf("schedulerForRun returned %T, want supplied scheduler", got)
	}
	if backtest.ownsRunScheduler() {
		t.Fatal("backtest claimed ownership of supplied runtime scheduler")
	}
	// This is the ownership gate used by Run: an externally supplied scheduler
	// must remain available to the Runtime or another runner sharing it.
	backtest.stopRunScheduler()
	if got := scheduler.stopCalls.Load(); got != 0 {
		t.Fatalf("supplied runtime scheduler was stopped by backtest: %d", got)
	}
}
