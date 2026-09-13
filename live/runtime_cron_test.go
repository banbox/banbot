package live

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/cron/v3"
)

func TestRuntimeRunGateSkipsOverlap(t *testing.T) {
	var gate runtimeRunGate
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		gate.run(func() {
			close(started)
			<-release
		})
		close(firstDone)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first job did not start")
	}
	var calls atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gate.run(func() { calls.Add(1) })
		}()
	}
	wg.Wait()
	if got := calls.Load(); got != 0 {
		t.Fatalf("overlapping jobs ran %d times", got)
	}
	close(release)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first job did not finish")
	}
	gate.run(func() { calls.Add(1) })
	if got := calls.Load(); got != 1 {
		t.Fatalf("gate did not reopen, calls=%d", got)
	}
}

type runtimeFatalSchedulerProbe struct {
	addCalls atomic.Int32
}

func (s *runtimeFatalSchedulerProbe) AddFunc(string, func()) (cron.EntryID, error) {
	s.addCalls.Add(1)
	return 0, nil
}

func (s *runtimeFatalSchedulerProbe) Start() {}

func (s *runtimeFatalSchedulerProbe) Stop() context.Context { return context.Background() }

func TestCronFatalLossCheckRequiresRuntimeClock(t *testing.T) {
	scheduler := &runtimeFatalSchedulerProbe{}
	deps := biz.RuntimeDeps{
		Config: config.NewSnapshot(&config.Config{
			FatalStop: map[string]float64{"1": 0.5},
		}),
		Accounts: map[string]*config.AccountConfig{"account": {}},
	}
	cronFatalLossCheckWithRuntime(scheduler, deps, nil)
	if got := scheduler.addCalls.Load(); got != 0 {
		t.Fatalf("fatal-stop task registered without runtime clock: %d", got)
	}

	deps.Clock = btime.NewClockState(true, nil)
	cronFatalLossCheckWithRuntime(scheduler, deps, nil)
	if got := scheduler.addCalls.Load(); got != 1 {
		t.Fatalf("fatal-stop task registrations = %d, want 1", got)
	}
}

func TestCronKlineDelaysRequiresRuntimeClock(t *testing.T) {
	scheduler := &runtimeFatalSchedulerProbe{}
	cronKlineDelaysWithRuntime(scheduler, &data.LiveProvider{}, com.NewPairCopiedState(), nil, biz.RuntimeDeps{
		Core: &core.State{},
	})
	if got := scheduler.addCalls.Load(); got != 0 {
		t.Fatalf("kline-delay task registered without runtime clock: %d", got)
	}

	clock := btime.NewClockState(false, nil)
	cronKlineDelaysWithRuntime(scheduler, &data.LiveProvider{}, com.NewPairCopiedState(), nil, biz.RuntimeDeps{
		Core:  &core.State{},
		Clock: clock,
	})
	if got := scheduler.addCalls.Load(); got != 1 {
		t.Fatalf("kline-delay task registrations = %d, want 1", got)
	}
}
