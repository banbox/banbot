package opt

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
)

func TestExplicitBacktestUsesRuntimeOutputConfig(t *testing.T) {
	oldData, oldDataDir, oldTimeRange := config.Data, config.DataDir, config.TimeRange
	t.Cleanup(func() {
		config.Data, config.DataDir, config.TimeRange = oldData, oldDataDir, oldTimeRange
	})

	globalDir := t.TempDir()
	runtimeDir := t.TempDir()
	globalConfig := config.Config{
		TimeRange:     &config.TimeTuple{StartMS: 900, EndMS: 1_000},
		StakeCurrency: []string{"USDT"},
	}
	config.Data = globalConfig
	config.DataDir = globalDir
	config.TimeRange = globalConfig.TimeRange

	coreState, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coreState.Close)
	coreState.SetRunMode(core.RunModeBackTest)
	coreState.ExgName, coreState.Market = "test", "spot"
	runtimeConfig := config.NewSnapshotWithDirs(&config.Config{
		TimeRange:     &config.TimeTuple{StartMS: 100, EndMS: 200},
		StakeCurrency: []string{"USDT"},
		BTStrict:      true,
		Accounts:      map[string]*config.AccountConfig{"default": {}},
	}, runtimeDir, "")
	deps := biz.RuntimeDeps{
		Core: coreState, Clock: btime.NewClockState(true, nil), Config: runtimeConfig,
		Symbols: orm.NewSymbolStateWithIdentity("test", "spot"), Exchange: newBacktestRuntimeExchangeStub("test", "spot"),
	}

	trader := newBacktestTraderForTest(t, deps)
	backtest, runErr := newBackTest(trader, nil, false, "", nil)
	if runErr != nil {
		t.Fatal(runErr)
	}
	t.Cleanup(func() { backtest.dp.Terminate() })

	hash, hashErr := runtimeConfig.View().HashCode()
	if hashErr != nil {
		t.Fatal(hashErr)
	}
	want := filepath.Join(runtimeDir, "backtest", hash)
	if backtest.OutDir != want {
		t.Fatalf("explicit backtest output directory = %q, want runtime directory %q", backtest.OutDir, want)
	}
	if got := backtest.runTimeRange(); got == nil || got.StartMS != 100 || got.EndMS != 200 {
		t.Fatalf("explicit backtest time range = %+v, want [100, 200]", got)
	}
	if !backtest.strictBacktest() {
		t.Fatal("explicit backtest did not use runtime strict mode")
	}
	result := &BTResult{}
	backtest.stoppedEarly = false
	backtest.BTResult = result
	backtest.normalizeBacktestResultRange()
	if result.StartMS != 100 || result.EndMS != 200 {
		t.Fatalf("normalized runtime result range = [%d, %d], want [100, 200]", result.StartMS, result.EndMS)
	}
}

func TestExplicitBacktestUsesRuntimeDownloadPolicy(t *testing.T) {
	oldData := config.Data
	t.Cleanup(func() { config.Data = oldData })
	config.Data.BTNoKlineDownload = false

	coreState, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coreState.Close)
	coreState.SetRunMode(core.RunModeBackTest)
	runtimeConfig := config.NewSnapshotWithDirs(&config.Config{BTNoKlineDownload: true}, t.TempDir(), "")
	deps := &biz.RuntimeDeps{Core: coreState, Config: runtimeConfig}
	if backtestKlineDownloadAllowed(false, deps) {
		t.Fatal("explicit bt_no_kline_download was ignored")
	}
	if backtestKlineDownloadAllowed(false, nil) {
		t.Fatal("missing runtime dependencies enabled downloads")
	}
}

func TestExplicitPairRefreshRequiresRuntimeClock(t *testing.T) {
	deps := completeBacktestDepsForTest(biz.RuntimeDeps{Exchange: newBacktestRuntimeExchangeStub("test", "spot")})
	deps.Clock = nil
	_, err := NewBackTestLiteWithRuntimeDeps(deps, true, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "clock") {
		t.Fatalf("missing runtime clock error = %v", err)
	}
}

func TestExplicitPairRefreshRequiresOwnedOrderState(t *testing.T) {
	deps := completeBacktestDepsForTest(biz.RuntimeDeps{Exchange: &banexg.Exchange{}})
	deps.Orders = nil
	_, err := NewBackTestLiteWithRuntimeDeps(deps, true, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "order state") {
		t.Fatalf("missing runtime order state error = %v", err)
	}
}
