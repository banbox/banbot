package live

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/cron/v3"
	log2 "log"
)

type balanceExchangeStub struct {
	banexg.BanExchange
	balanceCalls int
}

func (e *balanceExchangeStub) FetchBalance(map[string]interface{}) (*banexg.Balances, *errs.Error) {
	e.balanceCalls++
	return nil, errs.NewMsg(core.ErrRunTime, "balance test")
}

type captureScheduler struct {
	spec string
	call func()
}

func (s *captureScheduler) AddFunc(spec string, call func()) (cron.EntryID, error) {
	s.spec = spec
	s.call = call
	return 1, nil
}

func (s *captureScheduler) Start() {}

func (s *captureScheduler) Stop() context.Context { return context.Background() }

func TestCron(t *testing.T) {
	t.Skip("integration test (manual cron timing inspection)")
	logger := cron.VerbosePrintfLogger(log2.New(os.Stdout, "cron: ", log2.LstdFlags))
	loc, _ := time.LoadLocation("Asia/Shanghai")
	bntpClock := cron.NewNtpClock(loc, "zh-CN")
	c := cron.New(cron.WithSeconds(), cron.WithLogger(logger), cron.WithClock(bntpClock))
	//c := cron.New(cron.WithSeconds(), cron.WithLogger(logger))
	c.AddFunc("0 * * * * *", func() {
		realTime := btime.UTCStamp()      // correct utc timestamp
		sysTime := time.Now().UnixMilli() // local system timestamp
		log.Info(fmt.Sprintf("system: %d, real: %d", sysTime, realTime))
	})
	c.Start()
	time.Sleep(time.Minute * 3)
}

func TestRuntimeWorkersDoNotStartAfterCancellation(t *testing.T) {
	oldAccounts, oldPullSecs, oldEnvReal := config.Accounts, config.AccountPullSecs, core.EnvReal
	config.Accounts = nil
	core.EnvReal = true
	t.Cleanup(func() {
		config.Accounts, config.AccountPullSecs, core.EnvReal = oldAccounts, oldPullSecs, oldEnvReal
	})

	lifecycle := newTestRuntimeLifecycle()
	lifecycle.cancel()
	config.AccountPullSecs = 1
	StartLoopBalancePositionsWithRuntime(lifecycle)
	biz.StartLiveWalletSnapshots(lifecycle)
	if len(lifecycle.hooks) != 0 || len(lifecycle.waitHooks) != 0 {
		t.Fatalf("canceled runtime registered workers: hooks=%d waitHooks=%d", len(lifecycle.hooks), len(lifecycle.waitHooks))
	}

	config.AccountPullSecs = 0
	active := newTestRuntimeLifecycle()
	StartLoopBalancePositionsWithRuntime(active)
	active.closeAndWait()
}

func TestRuntimeBalanceWorkerUsesBoundDependencies(t *testing.T) {
	oldAccounts, oldPullSecs, oldDefault := config.Accounts, config.AccountPullSecs, exg.Default
	t.Cleanup(func() {
		config.Accounts, config.AccountPullSecs, exg.Default = oldAccounts, oldPullSecs, oldDefault
	})
	legacyExchange := &balanceExchangeStub{}
	runtimeExchange := &balanceExchangeStub{}
	exg.Default = legacyExchange
	config.Accounts = map[string]*config.AccountConfig{"legacy": {}}
	config.AccountPullSecs = 1

	state, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.Market = banexg.MarketLinear
	clock := btime.NewClockState(false, nil)
	market := com.NewMarketState("runtime-exchange")
	runtimeConfig := config.NewSnapshot(&config.Config{
		AccountPullSecs: 3600,
		Accounts:        map[string]*config.AccountConfig{"runtime": {}},
	})
	deps := biz.RuntimeDeps{
		Core:     state,
		Clock:    clock,
		Market:   market,
		Config:   runtimeConfig,
		Exchange: runtimeExchange,
	}
	bound := bindBalanceRuntimeDeps(deps)
	if bound.core != state ||
		bound.config != runtimeConfig.View() || bound.exchange != runtimeExchange ||
		bound.accounts["runtime"] == nil || bound.accounts["legacy"] != nil ||
		bound.interval != time.Hour {
		t.Fatalf("balance deps were not bound from runtime: %#v", bound)
	}

	lifecycle := newTestRuntimeLifecycle()
	StartLoopBalancePositionsWithRuntime(lifecycle, deps)
	if runtimeExchange.balanceCalls != 1 {
		t.Fatalf("runtime balance calls = %d, want 1", runtimeExchange.balanceCalls)
	}
	if legacyExchange.balanceCalls != 0 {
		t.Fatalf("legacy default balance calls = %d, want 0", legacyExchange.balanceCalls)
	}
	lifecycle.closeAndWait()
}

func TestRuntimeBacktestCronBindsRuntimeDependencies(t *testing.T) {
	oldBTInLive := config.BTInLive
	oldBacktest := backtestToCompareWithRuntime
	t.Cleanup(func() {
		config.BTInLive = oldBTInLive
		backtestToCompareWithRuntime = oldBacktest
	})
	config.BTInLive = &config.BtInLiveConfig{Cron: "global"}

	state, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.Market = banexg.MarketLinear
	clock := btime.NewClockState(false, nil)
	market := com.NewMarketState("runtime-exchange")
	runtimeConfig := config.NewSnapshotWithDirs(&config.Config{
		Name:     "runtime-config",
		BTInLive: &config.BtInLiveConfig{Cron: "runtime"},
		Accounts: map[string]*config.AccountConfig{"runtime": {}},
	}, t.TempDir(), "")
	boundExchange := &balanceExchangeStub{}
	deps := biz.RuntimeDeps{
		Core:     state,
		Clock:    clock,
		Market:   market,
		Config:   runtimeConfig,
		Exchange: boundExchange,
	}
	var got biz.RuntimeDeps
	backtestToCompareWithRuntime = func(runtimeDeps biz.RuntimeDeps) { got = runtimeDeps }
	scheduler := &captureScheduler{}
	cronBacktestInLiveWithRuntime(scheduler, deps)
	if scheduler.spec != "runtime" || scheduler.call == nil {
		t.Fatalf("runtime cron registration = spec %q, callback=%v", scheduler.spec, scheduler.call != nil)
	}
	scheduler.call()
	if got.Config != runtimeConfig || got.Core != state || got.Clock != clock ||
		got.Market != market || got.Exchange != boundExchange {
		t.Fatalf("runtime cron callback deps = %#v, want bound runtime deps", got)
	}
}
