package entry

import (
	"errors"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

// newEntryRuntime is a test-only adapter for exercising the legacy facade's
// runtime construction. Production entrypoints use explicitEntrySession and
// never need to rebuild a Runtime from package globals.
func newEntryRuntime(process *runtime.Process, mode string, startAt int64) (*runtime.Runtime, *errs.Error) {
	rt, err := process.NewRuntime(runtime.Options{
		Context:      core.Ctx,
		Config:       &config.Data,
		DataDir:      config.GetDataDirSafe(),
		StrategyDir:  config.GetStratDir(),
		Mode:         mode,
		Env:          core.RunEnv,
		StartAt:      startAt,
		Exchange:     exg.Default,
		Storage:      orm.CurrentStorage(),
		ExchangeName: core.ExgName,
		Market:       core.Market,
		ContractType: core.ContractType,
		Pairs:        config.Pairs,
	})
	if err != nil {
		return nil, errs.New(errs.CodeRunTime, err)
	}
	for _, item := range orm.GetExSymbols(core.ExgName, core.Market) {
		if cacheErr := rt.Symbols.CacheExSymbolChecked(item); cacheErr != nil {
			rt.Close()
			rt.Join()
			return nil, errs.New(errs.CodeRunTime, cacheErr)
		}
	}
	return rt, nil
}

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

func TestBacktestAndLiveRunnersUseIsolatedRuntimeDeps(t *testing.T) {
	oldData, oldDataDir, oldExgName, oldMarket, oldExchange := config.Data, config.DataDir, core.ExgName, core.Market, exg.Default
	t.Cleanup(func() {
		config.Data, config.DataDir = oldData, oldDataDir
		core.ExgName, core.Market = oldExgName, oldMarket
		exg.Default = oldExchange
	})
	config.DataDir = t.TempDir()
	core.ExgName, core.Market = "test", "spot"
	exg.Default = &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "test", MarketType: "spot"}}

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

	backtestRunnerDeps := backtestRuntime.BizDeps()
	backtest, backtestErr := opt.NewBackTestLiteWithRuntimeDeps(backtestRunnerDeps, true, nil, nil, nil)
	if backtestErr != nil {
		t.Fatal(backtestErr)
	}
	liveRunnerDeps := liveRuntime.BizDeps()
	liveTrader, traderErr := live.NewCryptoTraderWithRuntimeDeps(liveRunnerDeps, nil)
	if traderErr != nil {
		t.Fatal(traderErr)
	}
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
}
