package live

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/cron/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const delayMonitorSpec = "30 * * * * *"

// jobCapturingScheduler keeps the registered callbacks so a test can fire them.
type jobCapturingScheduler struct {
	mu    sync.Mutex
	specs []string
	jobs  map[string]func()
}

func (s *jobCapturingScheduler) AddFunc(spec string, fn func()) (cron.EntryID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jobs == nil {
		s.jobs = map[string]func(){}
	}
	s.specs = append(s.specs, spec)
	s.jobs[spec] = fn
	return 0, nil
}
func (s *jobCapturingScheduler) Start()                {}
func (s *jobCapturingScheduler) Stop() context.Context { return context.Background() }

type exitRecordingMgr struct {
	biz.IOrderMgr
	mu    sync.Mutex
	exits []*strat.ExitReq
}

func (m *exitRecordingMgr) ExitOrder(_ *ormo.InOutOrder, req *strat.ExitReq) (*ormo.InOutOrder, *errs.Error) {
	m.mu.Lock()
	m.exits = append(m.exits, req)
	m.mu.Unlock()
	return nil, nil
}

func (m *exitRecordingMgr) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.exits)
}

// providerWithOHLCVJobs returns a provider whose "ohlcv" job registry is
// non-empty (the monitor is a no-op without subscriptions). The registry is
// unexported and only filled over the spider websocket, so tests set it via
// reflection.
func providerWithOHLCVJobs(t *testing.T) *data.LiveProvider {
	t.Helper()
	watcher := &data.SeriesWatcher{}
	field := reflect.ValueOf(watcher).Elem().FieldByName("jobs")
	if !field.IsValid() {
		t.Fatal("SeriesWatcher.jobs field not found; update the test helper")
	}
	jobs := map[string]map[string]*data.PairTFCache{"ohlcv": {"BTC/USDT": &data.PairTFCache{TimeFrame: "1m"}}}
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(jobs))
	provider := &data.LiveProvider{SeriesWatcher: watcher}
	if len(provider.GetJobs("ohlcv")) == 0 {
		t.Fatal("fixture failed to install ohlcv jobs")
	}
	return provider
}

type delayMonitorFixture struct {
	deps   *biz.RuntimeDeps
	logs   *observer.ObservedLogs
	mgr    *exitRecordingMgr
	nowMS  int64
	sched  *jobCapturingScheduler
	run    func()
	hasJob bool
}

func newDelayMonitorFixture(t *testing.T, received *com.PairCopiedState, closeOnStuck int) *delayMonitorFixture {
	t.Helper()
	obsCore, logs := observer.New(zap.WarnLevel)
	dir := t.TempDir()
	f := &delayMonitorFixture{logs: logs, mgr: &exitRecordingMgr{}, nowMS: 1_700_000_000_000, sched: &jobCapturingScheduler{}}
	orders := ormo.NewOrderState()
	odMap, lock := orders.GetOpenODs("acc")
	lock.Lock()
	odMap[1] = &ormo.InOutOrder{Enter: &ormo.ExOrder{}, IOrder: &ormo.IOrder{ID: 1, Strategy: "s"}}
	lock.Unlock()
	trading := biz.NewTradingState()
	trading.SetOrderManager("acc", f.mgr)
	f.deps = &biz.RuntimeDeps{
		Core:     &core.State{Logger: zap.New(obsCore)},
		Clock:    btime.NewClockState(false, nil),
		Config:   config.NewSnapshotWithDirs(&config.Config{Name: "t", CloseOnStuck: closeOnStuck}, dir, dir),
		Orders:   orders,
		Trading:  trading,
		Accounts: map[string]*config.AccountConfig{"acc": {}},
	}
	cronKlineDelaysWithRuntime(f.sched, providerWithOHLCVJobs(t), received, func() int64 { return f.nowMS }, *f.deps)
	if job := f.sched.jobs[delayMonitorSpec]; job != nil {
		f.run, f.hasJob = job, true
	}
	return f
}

func (f *delayMonitorFixture) timeouts() int {
	n := 0
	for _, e := range f.logs.All() {
		if e.Message == "Listen to the spider kline timeout!" {
			n++
		}
	}
	return n
}

func TestKlineDelayMonitorFreshSeededStateNeverTimesOut(t *testing.T) {
	received := com.NewPairCopiedState()
	f := newDelayMonitorFixture(t, received, 1)
	if !f.hasJob {
		t.Fatal("delay monitor not registered")
	}
	received.Touch(f.nowMS) // what startExecutionRobotCrons does at startup
	for i := 0; i < 5; i++ {
		f.run()
	}
	if f.timeouts() != 0 || f.mgr.count() != 0 {
		t.Fatalf("fresh state: timeouts=%d exits=%d, want 0/0 (logs=%v)", f.timeouts(), f.mgr.count(), f.logs.All())
	}
	// An unwritten state (the old execution-path bug) reads as 0 => permanent timeout.
	untouched := newDelayMonitorFixture(t, com.NewPairCopiedState(), 1)
	untouched.run()
	if untouched.timeouts() != 1 {
		t.Fatalf("a never-written state must report the timeout, got %d", untouched.timeouts())
	}
}

func TestKlineDelayMonitorNilReceivedIsNotRegistered(t *testing.T) {
	f := newDelayMonitorFixture(t, nil, 1)
	if f.hasJob || len(f.sched.specs) != 0 {
		t.Fatalf("monitor registered without a Received state: %v", f.sched.specs)
	}
	// Same through the execution-robot entry point.
	sched := &jobCapturingScheduler{}
	deps := executionCronDeps()
	registerExecutionRobotCrons(sched, deps, &data.LiveProvider{}, nil, nil, deps.Clock.TimeMS, ExecutionRobotCronOptions{})
	for _, spec := range sched.specs {
		if spec == delayMonitorSpec {
			t.Fatalf("delay monitor registered with nil Received: %v", sched.specs)
		}
	}
	sched = &jobCapturingScheduler{}
	registerExecutionRobotCrons(sched, deps, &data.LiveProvider{}, nil, com.NewPairCopiedState(), deps.Clock.TimeMS, ExecutionRobotCronOptions{})
	if sched.jobs[delayMonitorSpec] == nil {
		t.Fatalf("delay monitor missing with a Received state: %v", sched.specs)
	}
}

func TestKlineDelayMonitorStaleStateClosesOrdersOnlyAfterCloseOnStuck(t *testing.T) {
	received := com.NewPairCopiedState()
	f := newDelayMonitorFixture(t, received, 2)
	received.Touch(f.nowMS - 121_000) // older than the 120 s threshold
	f.run()
	f.run()
	if f.timeouts() != 2 || f.mgr.count() != 0 {
		t.Fatalf("after 2 stuck runs: timeouts=%d exits=%d, want 2/0", f.timeouts(), f.mgr.count())
	}
	f.run() // stuck count 3 > close_on_stuck 2
	if f.timeouts() != 3 || f.mgr.count() != 1 {
		t.Fatalf("after 3 stuck runs: timeouts=%d exits=%d, want 3/1", f.timeouts(), f.mgr.count())
	}
	if req := f.mgr.exits[0]; req.Tag != core.ExitTagDataStuck || !req.Force {
		t.Fatalf("close request = %+v", req)
	}
	// Recovery resets the counter: a fresh touch stops timeouts and closes.
	received.Touch(f.nowMS)
	f.run()
	f.run()
	f.run()
	if f.timeouts() != 3 || f.mgr.count() != 1 {
		t.Fatalf("after recovery: timeouts=%d exits=%d, want 3/1", f.timeouts(), f.mgr.count())
	}
}

func TestKlineDelayMonitorAtThresholdDoesNotTimeOut(t *testing.T) {
	received := com.NewPairCopiedState()
	f := newDelayMonitorFixture(t, received, 1)
	received.Touch(f.nowMS - 120_000) // exactly 120 s: not "> 120"
	f.run()
	if f.timeouts() != 0 {
		t.Fatalf("120 s delay must not time out, logs=%v", f.logs.All())
	}
}
