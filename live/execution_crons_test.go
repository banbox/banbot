package live

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/cron/v3"
)

type recordingScheduler struct {
	mu     sync.Mutex
	specs  []string
	events []string
}

func (s *recordingScheduler) AddFunc(spec string, _ func()) (cron.EntryID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.specs = append(s.specs, spec)
	s.events = append(s.events, "add")
	return 0, nil
}

func (s *recordingScheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "start")
}

func (s *recordingScheduler) Stop() context.Context { return context.Background() }

type fakeCronExchange struct{ banexg.BanExchange }

func executionCronDeps() *biz.RuntimeDeps {
	return &biz.RuntimeDeps{
		Core:       &core.State{},
		Clock:      btime.NewClockState(false, nil),
		Config:     config.NewSnapshotWithDirs(&config.Config{Name: "t"}, execCronTestDir, execCronTestDir),
		Symbols:    orm.NewSymbolState(),
		Strategies: strat.NewState(),
		Exchange:   fakeCronExchange{},
	}
}

const execCronTestDir = "/tmp/execution-crons-test"

func TestExecutionRobotCronsRegisterExactlyApplicableJobsThenStart(t *testing.T) {
	scheduler := &recordingScheduler{}
	deps := executionCronDeps()
	runExecutionRobotCrons(scheduler, deps, &data.LiveProvider{}, nil, com.NewPairCopiedState(), deps.Clock.TimeMS, ExecutionRobotCronOptions{})
	want := []string{
		"0 0 * * * *",        // hourly kline repair
		"30 3 */2 * * *",     // market reload
		"30 * * * * *",       // kline delay monitor
		"30 1-59/10 * * * *", // receive summary
		"31 * * * * *",       // strategy outputs dump
	}
	if !reflect.DeepEqual(scheduler.specs, want) {
		t.Fatalf("registered specs = %v, want %v", scheduler.specs, want)
	}
	if got := scheduler.events; len(got) != len(want)+1 || got[len(got)-1] != "start" {
		t.Fatalf("scheduler must start exactly once after registration, events=%v", got)
	}
}

// Legacy-only jobs must never be registered on the execution-backed path, even
// when their configuration is present.
func TestExecutionRobotCronsSkipLegacyWalletJobs(t *testing.T) {
	scheduler := &recordingScheduler{}
	deps := executionCronDeps()
	deps.Config = config.NewSnapshotWithDirs(&config.Config{
		Name:      "t",
		PairMgr:   &config.PairMgrConfig{Cron: "0 */5 * * * *"},
		BTInLive:  &config.BtInLiveConfig{Cron: "0 0 * * * *"},
		FatalStop: map[string]float64{"1": 0.5},
	}, execCronTestDir, execCronTestDir)
	deps.Accounts = map[string]*config.AccountConfig{"a": {}}
	runExecutionRobotCrons(scheduler, deps, &data.LiveProvider{}, nil, com.NewPairCopiedState(), deps.Clock.TimeMS, ExecutionRobotCronOptions{})
	if len(scheduler.specs) != 5 {
		t.Fatalf("expected the 5 applicable jobs only, got %v", scheduler.specs)
	}
	for _, spec := range scheduler.specs {
		if spec == "0 */5 * * * *" {
			t.Fatalf("pair refresh job registered: %v", scheduler.specs)
		}
	}
}

func TestExecutionRobotCronsUseSuppliedHourOnlySet(t *testing.T) {
	scheduler := &recordingScheduler{}
	deps := executionCronDeps()
	called := false
	runExecutionRobotCrons(scheduler, deps, &data.LiveProvider{}, nil, com.NewPairCopiedState(), deps.Clock.TimeMS, ExecutionRobotCronOptions{
		HourOnlySymbols: func() map[int32]*orm.ExSymbol { called = true; return nil },
	})
	if called {
		t.Fatal("hour-only source must be read when the job fires, not at registration")
	}
	if len(scheduler.specs) != 5 {
		t.Fatalf("specs = %v", scheduler.specs)
	}
}

func TestStartExecutionRobotCronsRejectsMissingInputs(t *testing.T) {
	if err := StartExecutionRobotCrons(nil, &data.LiveProvider{}, ExecutionRobotCronOptions{}); err == nil {
		t.Fatal("nil trader must be rejected")
	}
	if err := StartExecutionRobotCrons(&CryptoTrader{}, &data.LiveProvider{}, ExecutionRobotCronOptions{}); err == nil {
		t.Fatal("trader without runtime dependencies must be rejected")
	}
	if err := StartExecutionRobotCrons(&CryptoTrader{}, nil, ExecutionRobotCronOptions{}); err == nil {
		t.Fatal("nil provider must be rejected")
	}
}
