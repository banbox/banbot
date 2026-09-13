package live

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/internal/testutil"
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
	spec  string
	call  func()
	specs []string
}

func (s *captureScheduler) AddFunc(spec string, call func()) (cron.EntryID, error) {
	s.spec = spec
	s.call = call
	s.specs = append(s.specs, spec)
	return 1, nil
}

func (s *captureScheduler) Start() {}

func (s *captureScheduler) Stop() context.Context { return context.Background() }

func (s *captureScheduler) hasSpec(spec string) bool {
	for _, item := range s.specs {
		if item == spec {
			return true
		}
	}
	return false
}

func TestCron(t *testing.T) {
	testutil.RequireIntegration(t)
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
		Accounts: config.CloneAccountConfigsForRuntime(runtimeConfig.View().Accounts),
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

func TestExplicitStartJobsIgnorePoisonedLegacyWorkerGlobals(t *testing.T) {
	oldAccounts, oldBTInLive, oldEnvReal, oldExchange := config.Accounts, config.BTInLive, core.EnvReal, exg.Default
	t.Cleanup(func() {
		config.Accounts, config.BTInLive, core.EnvReal, exg.Default = oldAccounts, oldBTInLive, oldEnvReal, oldExchange
	})
	legacyExchange := &balanceExchangeStub{}
	config.Accounts = map[string]*config.AccountConfig{"legacy": {}}
	config.BTInLive = &config.BtInLiveConfig{Cron: "legacy-poisoned-cron"}
	core.EnvReal = false
	exg.Default = legacyExchange

	state, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.EnvReal = true
	state.Market = banexg.MarketLinear
	scheduler := &captureScheduler{}
	deps := completeCryptoTraderDepsForTest(biz.RuntimeDeps{
		Core: state,
		Config: config.NewSnapshot(&config.Config{
			BTInLive:        &config.BtInLiveConfig{Cron: "runtime-cron"},
			AccountPullSecs: 3600,
			Accounts:        map[string]*config.AccountConfig{"runtime": {NoTrade: true}},
		}),
		Scheduler: scheduler,
	})
	lifecycle := newTestRuntimeLifecycle()
	trader, err := newRuntimeCryptoTraderForTest(lifecycle, deps, nil)
	if err != nil {
		t.Fatal(err)
	}
	trader.dp = &data.LiveProvider{}
	trader.startJobs()
	lifecycle.closeAndWait()

	if scheduler.hasSpec("legacy-poisoned-cron") {
		t.Fatalf("explicit startJobs registered poisoned legacy cron: %#v", scheduler.specs)
	}
	if !scheduler.hasSpec("runtime-cron") || !scheduler.hasSpec("15,45 * * * * *") {
		t.Fatalf("explicit startJobs missed runtime workers: %#v", scheduler.specs)
	}
	allowed := map[string]bool{
		"0 0 * * * *":        true,
		"30 3 */2 * * *":     true,
		"30 * * * * *":       true,
		"30 1-59/10 * * * *": true,
		"31 * * * * *":       true,
		"runtime-cron":       true,
		"15,45 * * * * *":    true,
	}
	for _, spec := range scheduler.specs {
		if !allowed[spec] {
			t.Fatalf("explicit startJobs registered non-runtime worker %q: %#v", spec, scheduler.specs)
		}
	}
	if legacyExchange.balanceCalls != 0 {
		t.Fatalf("explicit startJobs used the legacy exchange: %d calls", legacyExchange.balanceCalls)
	}
}

func TestBindBalanceRuntimeDepsSnapshotsAccounts(t *testing.T) {
	const account = "runtime"
	runtimeConfig := config.NewSnapshot(&config.Config{
		Accounts: map[string]*config.AccountConfig{account: {}},
	})
	bound := bindBalanceRuntimeDeps(biz.RuntimeDeps{
		Config:   runtimeConfig,
		Accounts: config.CloneAccountConfigsForRuntime(runtimeConfig.View().Accounts),
	})
	source := runtimeConfig.View().Accounts

	source[account].NoTrade = true
	delete(source, account)
	if cfg := bound.accounts[account]; cfg == nil || cfg.NoTrade {
		t.Fatalf("balance worker retained mutable account state: %#v", bound.accounts)
	}
}

func TestBalanceRuntimeDepsAccountSnapshotIsRaceFree(t *testing.T) {
	const accountCount = 256
	accounts := make(map[string]*config.AccountConfig, accountCount)
	for i := 0; i < accountCount; i++ {
		accounts[fmt.Sprintf("account-%d", i)] = &config.AccountConfig{NoTrade: true}
	}
	runtimeConfig := config.NewSnapshot(&config.Config{Accounts: accounts})
	bound := bindBalanceRuntimeDeps(biz.RuntimeDeps{Config: runtimeConfig})
	source := runtimeConfig.View().Accounts

	var calls sync.WaitGroup
	calls.Add(2)
	go func() {
		defer calls.Done()
		for i := 0; i < 1000; i++ {
			source["account-0"] = &config.AccountConfig{NoTrade: true}
		}
	}()
	go func() {
		defer calls.Done()
		for i := 0; i < 1000; i++ {
			updateBalancePosWithRuntime(bound)
		}
	}()
	calls.Wait()
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
