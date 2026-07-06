package opt

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

func TestBackTestInitEnsuresThirdPartyBeforeLoop(t *testing.T) {
	prevTimeRange := config.TimeRange
	prevEnvReal := core.EnvReal
	config.TimeRange = &config.TimeTuple{StartMS: 100_000, EndMS: 200_000}
	core.EnvReal = false
	defer func() {
		config.TimeRange = prevTimeRange
		core.EnvReal = prevEnvReal
	}()

	job := &strat.StratJob{
		Symbol: &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT"},
		Strat: &strat.TradeStrat{
			Name: "stg",
			OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 2}}
			},
		},
	}
	bt := &BackTest{}
	steps := make([]string, 0, 3)
	bt.seriesRuntime = data.NewSeriesRuntime(nil)
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
	jobs := map[string]map[string]*strat.StratJob{
		"BTC/USDT_1h": {"stg": job},
	}
	prevAccJobs := strat.AccJobs
	strat.AccJobs = map[string]map[string]map[string]*strat.StratJob{config.DefAcc: jobs}
	defer func() { strat.AccJobs = prevAccJobs }()

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

func TestBackTestEnsureThirdPartyRangeUsesWarmupDepthAndRunWindow(t *testing.T) {
	prevTimeRange := config.TimeRange
	prevEnvReal := core.EnvReal
	config.TimeRange = &config.TimeTuple{StartMS: 500_000, EndMS: 800_000}
	core.EnvReal = false
	defer func() {
		config.TimeRange = prevTimeRange
		core.EnvReal = prevEnvReal
	}()

	prevAccJobs := strat.AccJobs
	strat.AccJobs = map[string]map[string]map[string]*strat.StratJob{
		config.DefAcc: {
			"BTC/USDT_1h": {
				"macro": {
					Symbol: &orm.ExSymbol{ID: 11, Symbol: "BTC/USDT"},
					Strat: &strat.TradeStrat{
						Name: "macro",
						OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
							return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 3}}
						},
					},
				},
			},
		},
	}
	defer func() { strat.AccJobs = prevAccJobs }()

	var gotStart, gotEnd int64
	bt := &BackTest{}
	bt.seriesRuntime = data.NewSeriesRuntime(nil)
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
	bt := &BackTest{}
	bt.seriesRuntime = data.NewSeriesRuntime(nil)
	bt.seriesRuntime.EnsureFunc = func(ctx context.Context, plan *data.ThirdPartySeriesBootstrap) *errs.Error {
		return errs.NewMsg(core.ErrRunTime, "bootstrap ensure source=macro sid=5 tf=1d phase=ensure: fail")
	}
	loopCalls := 0
	bt.loopMainFn = func() *errs.Error {
		loopCalls++
		return nil
	}

	prevTimeRange := config.TimeRange
	prevEnvReal := core.EnvReal
	prevAccJobs := strat.AccJobs
	config.TimeRange = &config.TimeTuple{StartMS: 1_000, EndMS: 2_000}
	core.EnvReal = false
	strat.AccJobs = map[string]map[string]map[string]*strat.StratJob{
		config.DefAcc: {
			"BTC/USDT_1h": {
				"macro": {
					Symbol: &orm.ExSymbol{ID: 5, Symbol: "BTC/USDT"},
					Strat: &strat.TradeStrat{
						Name: "macro",
						OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
							return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 1}}
						},
					},
				},
			},
		},
	}
	defer func() {
		config.TimeRange = prevTimeRange
		core.EnvReal = prevEnvReal
		strat.AccJobs = prevAccJobs
	}()

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
	prevTimeRange := config.TimeRange
	prevEnvReal := core.EnvReal
	prevAccJobs := strat.AccJobs
	config.TimeRange = &config.TimeTuple{StartMS: 10_000, EndMS: 20_000}
	core.EnvReal = false
	job1 := &strat.StratJob{Symbol: &orm.ExSymbol{ID: 21, Symbol: "BTC/USDT"}, Strat: &strat.TradeStrat{Name: "a", OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
		return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 1}}
	}}}
	job2 := &strat.StratJob{Symbol: &orm.ExSymbol{ID: 22, Symbol: "ETH/USDT"}, Strat: &strat.TradeStrat{Name: "b", OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
		return []*strat.DataSub{{Source: "flow", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 2}}
	}}}
	strat.AccJobs = map[string]map[string]map[string]*strat.StratJob{config.DefAcc: {"BTC/USDT_1h": {"a": job1}, "ETH/USDT_1h": {"b": job2}}}
	defer func() {
		config.TimeRange = prevTimeRange
		core.EnvReal = prevEnvReal
		strat.AccJobs = prevAccJobs
	}()

	var calls int
	bt := &BackTest{}
	bt.seriesRuntime = data.NewSeriesRuntime(nil)
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
