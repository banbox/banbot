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
	"github.com/banbox/banbot/strat"
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
	runtimeConfig := config.NewSnapshotWithDirs(&config.Config{
		TimeRange:     &config.TimeTuple{StartMS: 100, EndMS: 200},
		StakeCurrency: []string{"USDT"},
		BTStrict:      true,
		Accounts:      map[string]*config.AccountConfig{"default": {}},
	}, runtimeDir, "")
	deps := biz.RuntimeDeps{
		Core:   coreState,
		Clock:  btime.NewClockState(true, nil),
		Config: runtimeConfig,
	}

	WithLegacySession(func(session LegacySession) struct{} {
		trader := biz.NewTraderWithRuntimeDeps(deps)
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
		return struct{}{}
	})
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
	if allowBacktestKlineDownloadForRuntime(false, deps) {
		t.Fatal("explicit bt_no_kline_download was ignored")
	}
	if !allowBacktestKlineDownloadForRuntime(false, nil) {
		t.Fatal("legacy download policy unexpectedly changed")
	}
}

func TestExplicitPairRefreshRequiresRuntimeClock(t *testing.T) {
	err := refreshPairJobsWithRuntimeDeps(nil, nil, &biz.RuntimeDeps{}, false, false, nil)
	if err == nil || !strings.Contains(err.Error(), "clock") {
		t.Fatalf("missing runtime clock error = %v", err)
	}
}

func TestExplicitPairRefreshRequiresOwnedOrderState(t *testing.T) {
	coreState, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coreState.Close)
	coreState.SetRunMode(core.RunModeBackTest)
	exchange := &banexg.Exchange{}
	deps := &biz.RuntimeDeps{
		Core:       coreState,
		Clock:      btime.NewClockState(true, nil),
		Strategies: strat.NewStateWithRuntime(coreState, nil, &config.Config{}, nil, exchange),
		Trading:    biz.NewTradingState(),
		Symbols:    orm.NewSymbolState(),
		Exchange:   exchange,
		Config:     config.NewSnapshot(&config.Config{}),
	}
	err = refreshPairJobsWithRuntimeDeps(nil, deps.Symbols, deps, false, false, nil)
	if err == nil || !strings.Contains(err.Error(), "order state") {
		t.Fatalf("missing runtime order state error = %v", err)
	}
}
