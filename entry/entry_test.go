package entry

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banexg/errs"
	"github.com/sasha-s/go-deadlock"
)

func TestExecuteBackTestPropagatesRunFailure(t *testing.T) {
	want := errs.New(core.ErrRunTime, errors.New("prediction loop failed"))

	outDir, got := executeBackTest("report", func() *errs.Error { return want })

	if got != want {
		t.Fatalf("executeBackTest() error = %v, want %v", got, want)
	}
	if outDir != "" {
		t.Fatalf("executeBackTest() output = %q on failure, want empty", outDir)
	}
}

func TestExecuteBackTestReturnsReportPath(t *testing.T) {
	outDir, err := executeBackTest("report", func() *errs.Error { return nil })

	if err != nil {
		t.Fatalf("executeBackTest() error = %v, want nil", err)
	}
	if outDir != "report" {
		t.Fatalf("executeBackTest() output = %q, want report", outDir)
	}
}

func TestNewEntryRuntimeUsesExplicitOptionsAndLifecycle(t *testing.T) {
	oldData, oldDataDir, oldPairs := config.Data, config.DataDir, config.Pairs
	oldRunEnv, oldExgName, oldMarket, oldContractType, oldCtx := core.RunEnv, core.ExgName, core.Market, core.ContractType, core.Ctx
	t.Cleanup(func() {
		config.Data, config.DataDir, config.Pairs = oldData, oldDataDir, oldPairs
		core.RunEnv, core.ExgName, core.Market, core.ContractType, core.Ctx = oldRunEnv, oldExgName, oldMarket, oldContractType, oldCtx
	})

	config.Data = config.Config{
		Env:      "config-env",
		Pairs:    []string{"configured-pair"},
		Exchange: &config.ExchangeConfig{Name: "config-exchange"},
	}
	config.DataDir = t.TempDir()
	config.Pairs = []string{"active-pair"}
	core.RunEnv = core.RunEnvTest
	core.ExgName = "explicit-exchange"
	core.Market = "explicit-market"
	core.ContractType = "explicit-contract"

	process := runtime.NewProcess()
	rt, err := newEntryRuntime(process, core.RunModeLive, 123)
	if err != nil {
		t.Fatal(err)
	}
	if rt.Core.RunMode != core.RunModeLive || rt.Core.RunEnv != core.RunEnvTest ||
		rt.Core.ExgName != "explicit-exchange" || rt.Core.Market != "explicit-market" ||
		rt.Core.ContractType != "explicit-contract" || rt.Core.Pairs[0] != "active-pair" {
		t.Fatalf("runtime options were not explicit: %#v", rt.Core)
	}
	if rt.Config.DataDir != config.DataDir || rt.Config.View().Pairs[0] != "configured-pair" {
		t.Fatalf("runtime config snapshot = %#v", rt.Config)
	}
	rt.Batch.SetLastBatchMS(123)
	rt.Close()
	select {
	case <-rt.Done():
	default:
		t.Fatal("entry runtime was not closed")
	}
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("closed entry runtime batch timestamp = %d, want 0", got)
	}
}

func TestEntryRuntimesShareSessionProcess(t *testing.T) {
	oldData, oldDataDir, oldExgName, oldMarket := config.Data, config.DataDir, core.ExgName, core.Market
	t.Cleanup(func() {
		config.Data, config.DataDir = oldData, oldDataDir
		core.ExgName, core.Market = oldExgName, oldMarket
	})
	config.DataDir = t.TempDir()
	core.ExgName, core.Market = "test", "spot"

	process := runtime.NewProcess()
	first, err := newEntryRuntime(process, core.RunModeBackTest, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newEntryRuntime(process, core.RunModeBackTest, 2)
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)

	firstSID := first.Symbols.NextSID()
	secondSID := second.Symbols.NextSID()
	if first.Process != process || second.Process != process || secondSID != firstSID+1 {
		t.Fatalf("entry runtimes did not share process allocator: process=%p/%p/%p sid=%d/%d", process, first.Process, second.Process, firstSID, secondSID)
	}
}

func TestNewEntryRuntimeRejectsSeedSIDConflict(t *testing.T) {
	oldData, oldDataDir, oldExgName, oldMarket := config.Data, config.DataDir, core.ExgName, core.Market
	t.Cleanup(func() {
		config.Data, config.DataDir = oldData, oldDataDir
		core.ExgName, core.Market = oldExgName, oldMarket
	})
	config.Data = config.Config{}
	config.DataDir = t.TempDir()
	core.ExgName, core.Market = "binance", "spot"

	restoreFirst, err := orm.InstallFrozenExSymbols([]*orm.ExSymbol{{
		ID: 7, Exchange: core.ExgName, Market: core.Market, Symbol: "BTC/USDT",
	}})
	if err != nil {
		t.Fatal(err)
	}
	process := runtime.NewProcess()
	first, runtimeErr := newEntryRuntime(process, core.RunModeBackTest, 1)
	if runtimeErr != nil {
		restoreFirst()
		t.Fatal(runtimeErr)
	}
	t.Cleanup(first.Close)
	restoreFirst()

	restoreSecond, err := orm.InstallFrozenExSymbols([]*orm.ExSymbol{{
		ID: 7, Exchange: core.ExgName, Market: core.Market, Symbol: "ETH/USDT",
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restoreSecond)

	second, runtimeErr := newEntryRuntime(process, core.RunModeBackTest, 2)
	if runtimeErr == nil {
		second.Close()
		t.Fatal("entry startup discarded a conflicting SID reservation")
	}
}

func TestLegacyRunnerSessionCreatesOneProcess(t *testing.T) {
	var calls int
	var sessionProcess *runtime.Process
	err := runLegacyRunnerSession(func(process *runtime.Process) *errs.Error {
		calls++
		sessionProcess = process
		first, firstErr := process.NewRuntime(runtime.Options{})
		if firstErr != nil {
			return errs.New(core.ErrRunTime, firstErr)
		}
		second, secondErr := process.NewRuntime(runtime.Options{})
		if secondErr != nil {
			first.Close()
			return errs.New(core.ErrRunTime, secondErr)
		}
		first.Close()
		second.Close()
		if first.Process != process || second.Process != process {
			return errs.NewMsg(core.ErrRunTime, "session runtimes did not retain the session process")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runLegacyRunnerSession() error = %v", err)
	}
	if calls != 1 || sessionProcess == nil {
		t.Fatalf("session callback calls/process = %d/%p, want 1/non-nil", calls, sessionProcess)
	}
}

func TestLegacyEntrySessionReleasesAfterError(t *testing.T) {
	want := errs.NewMsg(core.ErrRunTime, "legacy entry failed")
	if got := runLegacyEntrySession(func() *errs.Error { return want }); got != want {
		t.Fatalf("runLegacyEntrySession() error = %v, want %v", got, want)
	}

	done := make(chan struct{})
	go func() {
		runLegacyEntrySession(func() *errs.Error {
			close(done)
			return nil
		})
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("legacy gate remained locked after callback error")
	}
}

func TestLegacyEntrySessionReleasesAfterPanic(t *testing.T) {
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		runLegacyEntrySession(func() *errs.Error { panic("legacy entry panic") })
	}()
	if recovered == nil {
		t.Fatal("runLegacyEntrySession() did not propagate callback panic")
	}

	done := make(chan struct{})
	go func() {
		runLegacyEntrySession(func() *errs.Error {
			close(done)
			return nil
		})
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("legacy gate remained locked after callback panic")
	}
}

func TestExplicitRuntimeConstructionDoesNotWaitForLegacyEntryGate(t *testing.T) {
	unlock := runtime.LockLegacy()
	released := false
	t.Cleanup(func() {
		if !released {
			unlock()
		}
	})

	done := make(chan error, 1)
	go func() {
		rt, err := runtime.NewProcess().NewRuntime(runtime.Options{})
		if rt != nil {
			rt.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("explicit runtime construction failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("explicit runtime construction unexpectedly waited for legacy gate")
	}
	unlock()
	released = true
}

func TestBacktestAndLiveRunnersUseIsolatedRuntimeDeps(t *testing.T) {
	oldData, oldDataDir, oldExgName, oldMarket := config.Data, config.DataDir, core.ExgName, core.Market
	t.Cleanup(func() {
		config.Data, config.DataDir = oldData, oldDataDir
		core.ExgName, core.Market = oldExgName, oldMarket
	})
	config.DataDir = t.TempDir()
	core.ExgName, core.Market = "test", "spot"

	process := runtime.NewProcess()
	backtestRuntime, err := newEntryRuntime(process, core.RunModeBackTest, 100)
	if err != nil {
		t.Fatal(err)
	}
	liveRuntime, err := newEntryRuntime(process, core.RunModeLive, 200)
	if err != nil {
		backtestRuntime.Close()
		t.Fatal(err)
	}
	t.Cleanup(backtestRuntime.Close)
	t.Cleanup(liveRuntime.Close)

	opt.WithLegacySession(func(session opt.LegacySession) struct{} {
		backtest := opt.NewBackTestLiteWithRuntimeDeps(session, runtimeRunnerDeps(backtestRuntime), backtestRuntime.Symbols, true, nil, nil, nil)
		liveTrader := live.NewCryptoTraderWithRuntimeDeps(liveRuntime, runtimeRunnerDeps(liveRuntime), liveRuntime.Symbols, nil)
		backtestDeps := backtest.RuntimeDependencies()
		liveDeps := liveTrader.RuntimeDependencies()
		if backtestDeps.Core != backtestRuntime.Core || backtestDeps.Clock != backtestRuntime.Clock ||
			backtestDeps.Market != backtestRuntime.Market || backtestDeps.Batch != backtestRuntime.Batch ||
			backtestDeps.Symbols != backtestRuntime.Symbols {
			t.Fatal("backtest runner did not retain its runtime dependencies")
		}
		if liveDeps.Core != liveRuntime.Core || liveDeps.Clock != liveRuntime.Clock ||
			liveDeps.Market != liveRuntime.Market || liveDeps.Batch != liveRuntime.Batch ||
			liveDeps.Symbols != liveRuntime.Symbols {
			t.Fatal("live runner did not retain its runtime dependencies")
		}

		if !backtest.FeedDataSeries(&orm.DataSeries{
			TimeMS: 300, TimeFrame: "1m", IsWarmUp: true,
			ExSymbol: &orm.ExSymbol{Symbol: "BTC/USDT"},
			Values: map[string]any{
				"open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0, "volume": 1.0,
			},
		}) {
			t.Fatal("backtest runner rejected isolated runtime event")
		}
		if backtestRuntime.Clock.TimeMS() != 60_300 ||
			backtestRuntime.Market.Prices.GetLastBarPriceAt("BTC/USDT") != 11 ||
			backtestRuntime.Batch.LastBatchMS() != 60_300 {
			t.Fatal("backtest runner did not consume its typed clock, market, and batch state")
		}
		backtestRuntime.Clock.SetTimeMS(111)
		backtestRuntime.Core.BotRunning = false
		backtestRuntime.Core.CheckWallets = true
		backtestRuntime.Batch.SetLastBatchMS(333)
		if liveRuntime.Clock.TimeMS() == 111 || !liveRuntime.Core.BotRunning || liveRuntime.Core.CheckWallets ||
			liveRuntime.Market.Prices.GetLastBarPriceAt("BTC/USDT") != -1 || liveRuntime.Batch.LastBatchMS() != 0 {
			t.Fatal("backtest runtime state leaked into the live runner")
		}
		if backtest.TimeMS() != 111 || liveTrader.TimeMS() < 1_000_000_000_000 {
			t.Fatal("runners did not read their bound clocks")
		}
		return struct{}{}
	})
}

func TestDistinctLegacyEntryPathsAreSerialized(t *testing.T) {
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- runConfigCommand(&config.CmdArgs{}, &legacyCommandFlags{}, func(*config.CmdArgs) *errs.Error {
			close(firstEntered)
			<-releaseFirst
			return nil
		})
	}()
	<-firstEntered

	secondStarted := make(chan struct{})
	secondDone := make(chan *errs.Error, 1)
	go func() {
		close(secondStarted)
		secondDone <- RunSeriesDown(&config.CmdArgs{Tables: []string{"missing-legacy-gate-fixture"}})
	}()
	<-secondStarted
	select {
	case <-secondDone:
		t.Fatal("series entry path overlapped an active config command")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("config command entry path returned error: %v", err)
	}
	select {
	case err := <-secondDone:
		if err == nil {
			t.Fatal("series entry path unexpectedly succeeded with an unknown source")
		}
	case <-time.After(time.Second):
		t.Fatal("series entry path remained blocked after runner release")
	}
}

func TestRunSpiderWithUsesNonReentrantDataEntry(t *testing.T) {
	oldDataDir, oldLoaded := config.DataDir, config.Loaded
	oldCtx, oldStopAll := core.Ctx, core.StopAll
	oldRunMode, oldLiveMode, oldBackTestMode := core.RunMode, core.LiveMode, core.BackTestMode
	t.Cleanup(func() {
		config.DataDir, config.Loaded = oldDataDir, oldLoaded
		core.Ctx, core.StopAll = oldCtx, oldStopAll
		core.RunMode, core.LiveMode, core.BackTestMode = oldRunMode, oldLiveMode, oldBackTestMode
	})

	config.Loaded = false
	args := &config.CmdArgs{
		DataDir:    t.TempDir(),
		NoDefault:  true,
		ConfigData: "invalid: [",
		Logfile:    filepath.Join(t.TempDir(), "runner.log"),
	}
	unlock := runtime.LockLegacy()
	released := false
	t.Cleanup(func() {
		if !released {
			unlock()
		}
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = RunSpiderWith(args, nil)
	}()

	select {
	case <-done:
		t.Fatal("RunSpiderWith bypassed the held legacy gate")
	case <-time.After(50 * time.Millisecond):
	}
	unlock()
	released = true

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunSpiderWith remained blocked after the legacy gate was released")
	}
}

func TestPublicLegacyEntryAPIsShareProcessGate(t *testing.T) {
	oldDataDir, oldLoaded := config.DataDir, config.Loaded
	oldCtx, oldStopAll := core.Ctx, core.StopAll
	oldRunMode, oldLiveMode, oldBackTestMode := core.RunMode, core.LiveMode, core.BackTestMode
	oldDeadlockDisabled := deadlock.Opts.Disable
	t.Cleanup(func() {
		config.DataDir, config.Loaded = oldDataDir, oldLoaded
		core.Ctx, core.StopAll = oldCtx, oldStopAll
		core.RunMode, core.LiveMode, core.BackTestMode = oldRunMode, oldLiveMode, oldBackTestMode
		deadlock.Opts.Disable = oldDeadlockDisabled
	})
	config.Loaded = false

	tests := []struct {
		name string
		run  func(*config.CmdArgs) *errs.Error
	}{
		{name: "backtest", run: RunBackTest},
		{name: "trade", run: RunTrade},
		{name: "trade-with", run: func(args *config.CmdArgs) *errs.Error { return RunTradeWith(args, nil) }},
		{name: "down", run: RunDownData},
		{name: "repair-ranges", run: RunRepairKlineRanges},
		{name: "correct", run: RunKlineCorrect},
		{name: "adjust", run: RunKlineAdjFactors},
		{name: "verify", run: RunVerifyData},
		{name: "spider", run: RunSpider},
		{name: "spider-with", run: func(args *config.CmdArgs) *errs.Error { return RunSpiderWith(args, nil) }},
		{name: "load", run: LoadKLinesToDB},
		{name: "aggregate", run: AggKlineBigs},
		{name: "series", run: func(args *config.CmdArgs) *errs.Error {
			args.Tables = []string{"missing-legacy-gate-fixture"}
			return RunSeriesDown(args)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dataDir := t.TempDir()
			args := &config.CmdArgs{
				DataDir:    dataDir,
				NoDefault:  true,
				ConfigData: "invalid: [",
				Logfile:    filepath.Join(dataDir, "runner.log"),
			}
			unlock := runtime.LockLegacy()
			started := make(chan struct{})
			done := make(chan struct{})
			var got *errs.Error
			var panicValue any
			go func() {
				close(started)
				defer func() {
					panicValue = recover()
					close(done)
				}()
				got = test.run(args)
			}()
			<-started
			select {
			case <-done:
				unlock()
				t.Fatalf("%s completed while legacy gate was held: err=%v panic=%v", test.name, got, panicValue)
			case <-time.After(50 * time.Millisecond):
			}
			unlock()
			select {
			case <-done:
				if panicValue != nil {
					t.Fatalf("%s panicked after gate release: %v", test.name, panicValue)
				}
			case <-time.After(time.Second):
				t.Fatalf("%s did not finish after gate release", test.name)
			}
		})
	}
}
