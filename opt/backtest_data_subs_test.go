package opt

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

func backtestTestCatalog(t *testing.T, sources ...string) *data.DataSourceCatalog {
	t.Helper()
	catalog := data.NewDataSourceCatalog()
	for _, name := range sources {
		source, err := data.NewFuncDataSource(
			orm.NewSeriesInfo(name, "1d", []orm.SeriesField{{Name: "value", Type: "float", Role: "value"}}),
			func(context.Context, *strat.DataSub, int64, int64) ([]*orm.DataRecord, error) { return nil, nil }, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := catalog.RegisterDataSource(source); err != nil {
			t.Fatal(err)
		}
	}
	return catalog
}

func TestBackTestInitEnsuresThirdPartyBeforeLoop(t *testing.T) {
	job := &strat.StratJob{
		Symbol: &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT"},
		Strat: &strat.TradeStrat{
			Name: "stg",
			OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 2}}
			},
		},
	}
	deps := completeBacktestDepsForTest(biz.RuntimeDeps{Config: config.NewSnapshot(&config.Config{
		TimeRange: &config.TimeTuple{StartMS: 100_000, EndMS: 200_000},
		Accounts:  map[string]*config.AccountConfig{"default": {}},
	})})
	steps := make([]string, 0, 3)
	deps.Strategies.SetJobMap(deps.DefaultAccount, "BTC/USDT_1h", map[string]*strat.StratJob{"stg": job})
	bt := &BackTest{BackTestLite: &BackTestLite{Trader: newBacktestTraderForTest(t, deps)}}
	bt.seriesRuntime = data.NewSeriesRuntimeWithCatalog(backtestTestCatalog(t, "macro"), nil)
	bt.seriesRuntime.EnsureFunc = func(ctx context.Context, plan *data.ThirdPartySeriesBootstrap) *errs.Error {
		steps = append(steps, fmt.Sprintf("ensure:%d:%d", plan.StartMS, plan.EndMS))
		if len(plan.Subs) != 1 || plan.Subs[0].Source != "macro" {
			t.Fatalf("unexpected bootstrap plan subs: %+v", plan.Subs)
		}
		return nil
	}
	bt.loopMainFn = func() *errs.Error {
		steps = append(steps, "loop")
		return nil
	}
	if _, err := bt.ensureThirdPartySeriesRange(); err != nil {
		t.Fatalf("ensureThirdPartySeriesRange failed: %v", err)
	}
	if err := bt.loopMainFn(); err != nil {
		t.Fatalf("loopMainFn failed: %v", err)
	}
	want := []string{"ensure:-172700000:200000", "loop"}
	if !reflect.DeepEqual(steps, want) {
		t.Fatalf("unexpected backtest order\nwant: %+v\n got: %+v", want, steps)
	}
}

func TestBacktestKlineDownloadUsesRuntimeConfig(t *testing.T) {
	deps := completeBacktestDepsForTest(biz.RuntimeDeps{Config: config.NewSnapshot(&config.Config{
		Accounts: map[string]*config.AccountConfig{"default": {}},
	})})
	if !backtestKlineDownloadAllowed(false, &deps) {
		t.Fatal("normal backtests should download missing klines by default")
	}
	if backtestKlineDownloadAllowed(true, &deps) {
		t.Fatal("optimization backtests should not download klines")
	}
	deps.Config = config.NewSnapshot(&config.Config{BTNoKlineDownload: true, Accounts: map[string]*config.AccountConfig{"default": {}}})
	if backtestKlineDownloadAllowed(false, &deps) {
		t.Fatal("bt_no_kline_download should disable implicit kline downloads")
	}
}

func TestBackTestEnsureThirdPartyRangeUsesWarmupDepthAndRunWindow(t *testing.T) {
	deps := completeBacktestDepsForTest(biz.RuntimeDeps{Config: config.NewSnapshot(&config.Config{
		TimeRange: &config.TimeTuple{StartMS: 500_000, EndMS: 800_000}, Accounts: map[string]*config.AccountConfig{"default": {}},
	})})
	job := &strat.StratJob{Symbol: &orm.ExSymbol{ID: 11, Symbol: "BTC/USDT"}, Strat: &strat.TradeStrat{
		Name: "macro", OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 3}}
		},
	}}
	deps.Strategies.SetJobMap(deps.DefaultAccount, "BTC/USDT_1h", map[string]*strat.StratJob{"macro": job})

	var gotStart, gotEnd int64
	bt := &BackTest{BackTestLite: &BackTestLite{Trader: newBacktestTraderForTest(t, deps)}}
	bt.seriesRuntime = data.NewSeriesRuntimeWithCatalog(backtestTestCatalog(t, "macro"), nil)
	bt.seriesRuntime.EnsureFunc = func(ctx context.Context, plan *data.ThirdPartySeriesBootstrap) *errs.Error {
		gotStart, gotEnd = plan.StartMS, plan.EndMS
		return nil
	}

	if _, err := bt.ensureThirdPartySeriesRange(); err != nil {
		t.Fatalf("ensureThirdPartySeriesRange failed: %v", err)
	}
	if gotStart != -258700000 || gotEnd != 800_000 {
		t.Fatalf("unexpected ensure range start=%d end=%d", gotStart, gotEnd)
	}
}

func TestBackTestEnsureFailureStopsBeforeLoop(t *testing.T) {
	deps := completeBacktestDepsForTest(biz.RuntimeDeps{Config: config.NewSnapshot(&config.Config{
		TimeRange: &config.TimeTuple{StartMS: 1_000, EndMS: 2_000}, Accounts: map[string]*config.AccountConfig{"default": {}},
	})})
	job := &strat.StratJob{Symbol: &orm.ExSymbol{ID: 5, Symbol: "BTC/USDT"}, Strat: &strat.TradeStrat{
		Name: "macro", OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 1}}
		},
	}}
	deps.Strategies.SetJobMap(deps.DefaultAccount, "BTC/USDT_1h", map[string]*strat.StratJob{"macro": job})
	bt := &BackTest{BackTestLite: &BackTestLite{Trader: newBacktestTraderForTest(t, deps)}}
	bt.seriesRuntime = data.NewSeriesRuntimeWithCatalog(backtestTestCatalog(t, "macro"), nil)
	bt.seriesRuntime.EnsureFunc = func(ctx context.Context, plan *data.ThirdPartySeriesBootstrap) *errs.Error {
		return errs.NewMsg(core.ErrRunTime, "bootstrap ensure source=macro sid=5 tf=1d phase=ensure: fail")
	}
	loopCalls := 0
	bt.loopMainFn = func() *errs.Error {
		loopCalls++
		return nil
	}

	_, err := bt.ensureThirdPartySeriesRange()
	if err == nil || !strings.Contains(err.Short(), "phase=ensure") {
		t.Fatalf("expected ensure phase error, got %v", err)
	}
	if loopCalls != 0 {
		t.Fatalf("expected ensure failure to stop before loop, got loop=%d", loopCalls)
	}
}

func TestBackTestBootstrapRangeSkipsWhenNoThirdPartySubs(t *testing.T) {
	prevTimeRange := config.TimeRange
	config.TimeRange = &config.TimeTuple{StartMS: 1_000, EndMS: 2_000}
	defer func() { config.TimeRange = prevTimeRange }()

	plan, err := backtestBootstrapPlan(nil, config.TimeRange)
	if err != nil {
		t.Fatalf("backtestBootstrapPlan returned error: %v", err)
	}
	if plan.HasSubs() || plan.StartMS != config.TimeRange.StartMS || plan.EndMS != config.TimeRange.EndMS {
		t.Fatalf("expected no bootstrap subs with range preserved, got %+v", plan)
	}
}

func TestBacktestBootstrapPlanWithCatalogUsesExplicitCatalog(t *testing.T) {
	const sourceName = "opt_runtime_catalog_plan_test"
	catalog := data.NewDataSourceCatalog()
	source, err := data.NewFuncDataSource(
		orm.NewSeriesInfo(sourceName, "1d", []orm.SeriesField{{Name: "runtime_field", Type: "float", Role: "value"}}),
		func(context.Context, *strat.DataSub, int64, int64) ([]*orm.DataRecord, error) {
			return nil, nil
		}, nil,
	)
	if err != nil {
		t.Fatalf("new data source: %v", err)
	}
	if err := catalog.RegisterDataSource(source); err != nil {
		t.Fatalf("register data source: %v", err)
	}

	job := &strat.StratJob{
		Symbol: &orm.ExSymbol{ID: 101, Symbol: "BTC/USDT"},
		Strat: &strat.TradeStrat{OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			return []*strat.DataSub{{Source: sourceName, ExSymbol: s.Symbol, TimeFrame: "1d"}}
		}},
	}
	plan, err := backtestBootstrapPlanWithCatalog(catalog, []*strat.StratJob{job}, &config.TimeTuple{StartMS: 100, EndMS: 200})
	if err != nil {
		t.Fatalf("backtestBootstrapPlanWithCatalog returned error: %v", err)
	}
	if len(plan.Subs) != 1 || plan.Subs[0].Source != sourceName ||
		!reflect.DeepEqual(plan.Subs[0].Fields, []string{"runtime_field"}) {
		t.Fatalf("explicit catalog was not used, got %+v", plan.Subs)
	}
}

func TestBacktestBootstrapPlanWithCatalogRejectsMissingCatalog(t *testing.T) {
	for _, catalog := range []*data.DataSourceCatalog{nil, data.LegacyDataSourceCatalog()} {
		_, err := backtestBootstrapPlanWithCatalog(catalog, nil, &config.TimeTuple{StartMS: 100, EndMS: 200})
		if err == nil || !strings.Contains(err.Error(), "explicit data source catalog") {
			t.Fatalf("expected explicit catalog error for catalog=%p, got %v", catalog, err)
		}
	}
}

func TestExplicitBacktestSeriesSyncDoesNotUseLegacyCatalog(t *testing.T) {
	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.SetRunMode(core.RunModeBackTest)
	snapshot := config.NewSnapshot(&config.Config{TimeRange: &config.TimeTuple{StartMS: 100, EndMS: 200}})
	trader := newBacktestTraderForTest(t, biz.RuntimeDeps{
		Core: state, Config: snapshot, Strategies: strat.NewState(),
	})
	deps := trader.RuntimeDependencies()
	job := &strat.StratJob{
		Symbol: &orm.ExSymbol{ID: 102, Symbol: "BTC/USDT"},
		Strat: &strat.TradeStrat{OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
			return []*strat.DataSub{{Source: "opt_runtime_missing_catalog_test", ExSymbol: s.Symbol, TimeFrame: "1d"}}
		}},
	}
	deps.Strategies.SetJobMap(deps.DefaultAccount, "BTC/USDT_1h", map[string]*strat.StratJob{"test": job})
	backtest := &BackTest{BackTestLite: &BackTestLite{Trader: trader}}
	backtest.seriesRuntime = data.NewSeriesRuntime(nil)
	ensureCalls := 0
	backtest.seriesRuntime.EnsureFunc = func(context.Context, *data.ThirdPartySeriesBootstrap) *errs.Error {
		ensureCalls++
		return nil
	}

	_, gotErr := backtest.syncThirdPartySeriesRange()
	if gotErr == nil || !strings.Contains(gotErr.Error(), "explicit data source catalog") {
		t.Fatalf("expected explicit catalog error, got %v", gotErr)
	}
	if ensureCalls != 0 {
		t.Fatalf("ensure callback ran after catalog validation failure: %d", ensureCalls)
	}
}

func TestBackTestCollectRuntimeSubsExcludeKlineWarmPath(t *testing.T) {
	job := &strat.StratJob{
		Symbol: &orm.ExSymbol{ID: 13, Symbol: "BTC/USDT"},
		Strat: &strat.TradeStrat{
			Name: "mixed",
			OnPairInfos: func(s *strat.StratJob) []*strat.PairSub {
				return []*strat.PairSub{{Pair: "_cur_", TimeFrame: "1h", WarmupNum: 10}}
			},
			OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{
					{Source: orm.SeriesSourceKline, ExSymbol: s.Symbol, TimeFrame: "4h", WarmupNum: 2},
					{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 3},
				}
			},
		},
	}
	subs, err := data.CollectRuntimeDataSubs([]*strat.StratJob{job})
	if err != nil {
		t.Fatalf("CollectRuntimeDataSubs failed: %v", err)
	}
	if len(subs) != 1 {
		t.Fatalf("expected only non-kline runtime sub, got %+v", subs)
	}
	if subs[0].Source != "macro" || subs[0].TimeFrame != "1d" {
		t.Fatalf("unexpected runtime sub %+v", subs[0])
	}
}

func TestBackTestEnsureThirdPartyUsesLoadedJobs(t *testing.T) {
	deps := completeBacktestDepsForTest(biz.RuntimeDeps{Config: config.NewSnapshot(&config.Config{
		TimeRange: &config.TimeTuple{StartMS: 10_000, EndMS: 20_000}, Accounts: map[string]*config.AccountConfig{"default": {}},
	})})
	job1 := &strat.StratJob{Symbol: &orm.ExSymbol{ID: 21, Symbol: "BTC/USDT"}, Strat: &strat.TradeStrat{Name: "a", OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
		return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 1}}
	}}}
	job2 := &strat.StratJob{Symbol: &orm.ExSymbol{ID: 22, Symbol: "ETH/USDT"}, Strat: &strat.TradeStrat{Name: "b", OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
		return []*strat.DataSub{{Source: "flow", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 2}}
	}}}
	deps.Strategies.SetJobMap(deps.DefaultAccount, "BTC/USDT_1h", map[string]*strat.StratJob{"a": job1})
	deps.Strategies.SetJobMap(deps.DefaultAccount, "ETH/USDT_1h", map[string]*strat.StratJob{"b": job2})

	var calls int
	bt := &BackTest{BackTestLite: &BackTestLite{Trader: newBacktestTraderForTest(t, deps)}}
	bt.seriesRuntime = data.NewSeriesRuntimeWithCatalog(backtestTestCatalog(t, "macro", "flow"), nil)
	bt.seriesRuntime.EnsureFunc = func(ctx context.Context, plan *data.ThirdPartySeriesBootstrap) *errs.Error {
		calls++
		if len(plan.Subs) != 2 {
			t.Fatalf("expected 2 runtime data subs, got %+v", plan.Subs)
		}
		return nil
	}
	if _, err := bt.ensureThirdPartySeriesRange(); err != nil {
		t.Fatalf("ensureThirdPartySeriesRange failed: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected single deduped ensure bootstrap call, got %d", calls)
	}
}
