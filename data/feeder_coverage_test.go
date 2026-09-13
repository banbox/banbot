package data

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	ta "github.com/banbox/banta"
)

func TestFeederHistoricalCoverageFiltersCallbacks(t *testing.T) {
	called := make([]int64, 0, 2)
	feeder := &Feeder{
		ExSymbol: &orm.ExSymbol{Symbol: "BNB/USDT:USDT"},
		CallBack: func(evt *orm.DataSeries) { called = append(called, evt.TimeMS) },
		coverage: &config.HistoricalCoverageConfig{
			BaselineEndMS: 500,
			Bars: map[string]map[string][]config.HistoricalCoverageRange{
				"BNB/USDT:USDT": {"5m": {{StartMS: 100, StopMS: 200}}},
			},
		},
	}
	oldTime := btime.CurTimeMS
	t.Cleanup(func() { btime.CurTimeMS = oldTime })
	feeder.fireCallBacks("5m", 10, []*orm.DataSeries{
		{TimeMS: 100}, {TimeMS: 200}, {TimeMS: 500},
	}, nil)
	if len(called) != 2 || called[0] != 100 || called[1] != 500 {
		t.Fatalf("unexpected callbacks: %v", called)
	}
	if btime.CurTimeMS != 510 {
		t.Fatalf("current time = %d, want 510", btime.CurTimeMS)
	}
}

func TestFeederBacktestWarmupBoundaryUsesBarEnd(t *testing.T) {
	const hourMS = int64(60 * 60 * 1000)
	startMS := int64(10 * hourMS)
	previousBackTest, previousLive, previousRange := core.BackTestMode, core.LiveMode, config.TimeRange
	oldTime := btime.CurTimeMS
	t.Cleanup(func() {
		core.BackTestMode, core.LiveMode, config.TimeRange = previousBackTest, previousLive, previousRange
		btime.CurTimeMS = oldTime
	})
	core.BackTestMode = true
	core.LiveMode = false
	config.TimeRange = &config.TimeTuple{StartMS: startMS}

	var got []*orm.DataSeries
	feeder := &Feeder{
		ExSymbol: &orm.ExSymbol{Symbol: "BTC/USDT:USDT"},
		CallBack: func(evt *orm.DataSeries) { got = append(got, evt) },
	}
	feeder.fireCallBacks("1h", hourMS, []*orm.DataSeries{
		{TimeMS: startMS - hourMS},
		{TimeMS: startMS},
	}, nil)

	if len(got) != 2 || !got[0].IsWarmUp || got[1].IsWarmUp {
		t.Fatalf("warmup flags at boundary and next bar = %+v, want [true false]", got)
	}
}

func TestHistoricalCoverageForFeeder(t *testing.T) {
	previous := config.HistoricalCoverage
	config.HistoricalCoverage = &config.HistoricalCoverageConfig{
		BaselineEndMS: 500,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			"BNB/USDT:USDT": {"5m": {{StartMS: 100, StopMS: 400}}},
		},
	}
	t.Cleanup(func() { config.HistoricalCoverage = previous })
	if got := historicalCoverageForFeeder("BNB/USDT:USDT", false); got != nil {
		t.Fatalf("live feeder received historical coverage: %#v", got)
	}
	known := historicalCoverageForFeeder("BNB/USDT:USDT", true)
	if known == nil || !known.Allows("5m", 200) {
		t.Fatalf("backtest feeder lost known coverage: %#v", known)
	}
	missing := historicalCoverageForFeeder("BTC/USDT:USDT", true)
	if missing == nil || missing.Allows("5m", 200) || missing.Allows("5m", 500) {
		t.Fatalf("missing symbol did not fail closed across the extension tail: %#v", missing)
	}
}

func TestFeederHistoricalCoverageFiltersStateAndCache(t *testing.T) {
	oldTime := btime.CurTimeMS
	t.Cleanup(func() { btime.CurTimeMS = oldTime })
	called := make([]int64, 0, 1)
	state := &PairTFCache{TimeFrame: "8h", TFSecs: 8 * 60 * 60}
	feeder := &Feeder{
		ExSymbol: &orm.ExSymbol{Symbol: "BTC/USDT:USDT"},
		CallBack: func(evt *orm.DataSeries) { called = append(called, evt.TimeMS) },
		tfBars:   make(map[string][]*orm.DataSeries),
		coverage: &config.HistoricalCoverageConfig{
			BaselineEndMS: 500,
			Bars: map[string]map[string][]config.HistoricalCoverageRange{
				"BTC/USDT:USDT": {"8h": {{StartMS: 200, StopMS: 300}}},
			},
		},
	}
	if rows := feeder.onStateOhlcvs(state, []*orm.DataSeries{{TimeMS: 100}}, true); len(rows) != 0 {
		t.Fatalf("excluded rows reached state: %v", rows)
	}
	if state.Latest != nil || len(feeder.tfBars["8h"]) != 0 || len(called) != 0 {
		t.Fatalf("excluded row polluted state/cache: state=%#v cache=%v callbacks=%v", state, feeder.tfBars, called)
	}
	if rows := feeder.onStateOhlcvs(state, []*orm.DataSeries{{TimeMS: 500}}, true); len(rows) != 1 {
		t.Fatalf("post-baseline row was filtered: %v", rows)
	}
	if state.Latest == nil || state.Latest.TimeMS != 500 || len(feeder.tfBars["8h"]) != 1 || len(called) != 1 {
		t.Fatalf("post-baseline row did not reach state/cache: state=%#v cache=%v callbacks=%v", state, feeder.tfBars, called)
	}
}

func TestFeederHistoricalCoverageRecomputesLastCompletedRow(t *testing.T) {
	oldTime := btime.CurTimeMS
	t.Cleanup(func() { btime.CurTimeMS = oldTime })
	called := make([]int64, 0, 1)
	state := &PairTFCache{TimeFrame: "8h", TFSecs: 8 * 60 * 60}
	feeder := &Feeder{
		ExSymbol: &orm.ExSymbol{Symbol: "BTC/USDT:USDT"},
		CallBack: func(evt *orm.DataSeries) { called = append(called, evt.TimeMS) },
		tfBars:   make(map[string][]*orm.DataSeries),
		coverage: &config.HistoricalCoverageConfig{
			BaselineEndMS: 500,
			Bars: map[string]map[string][]config.HistoricalCoverageRange{
				"BTC/USDT:USDT": {"8h": {{StartMS: 100, StopMS: 200}}},
			},
		},
	}
	rows := feeder.onStateOhlcvs(state, []*orm.DataSeries{{TimeMS: 100}, {TimeMS: 200}}, false)
	if len(rows) != 1 || rows[0].TimeMS != 100 || state.WaitBar != nil || len(called) != 1 || called[0] != 100 ||
		len(feeder.tfBars["8h"]) != 1 {
		t.Fatalf("completed covered row was delayed: rows=%v state=%#v cache=%v callbacks=%v",
			rows, state, feeder.tfBars, called)
	}
}

func TestFeederPhysicalCoverageOnlyAppliesToInternalPhysicalState(t *testing.T) {
	const hourMS = int64(60 * 60 * 1000)
	previousBackTest, previousLive := core.BackTestMode, core.LiveMode
	previousStrict, previousNoDownload := config.Data.BTStrict, config.Data.BTNoKlineDownload
	oldTime := btime.CurTimeMS
	t.Cleanup(func() {
		core.BackTestMode, core.LiveMode = previousBackTest, previousLive
		config.Data.BTStrict, config.Data.BTNoKlineDownload = previousStrict, previousNoDownload
		btime.CurTimeMS = oldTime
	})
	core.BackTestMode = true
	core.LiveMode = false
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true

	const symbol = "BTC/USDT:USDT"
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: 10 * hourMS,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {"8h": {{StartMS: 0, StopMS: 10 * hourMS}}},
		},
		PhysicalBars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {"1h": {{StartMS: 0, StopMS: 10 * hourMS}}},
		},
	}

	t.Run("internal physical state accepts authorized row", func(t *testing.T) {
		state := &PairTFCache{TimeFrame: "1h", TFSecs: 60 * 60, physicalOnly: true}
		var called []*orm.DataSeries
		feeder := &Feeder{
			ExSymbol: &orm.ExSymbol{Symbol: symbol},
			States:   []*PairTFCache{state},
			CallBack: func(evt *orm.DataSeries) { called = append(called, evt) },
			tfBars:   make(map[string][]*orm.DataSeries),
			coverage: coverage,
		}
		row := &orm.DataSeries{TimeMS: hourMS}

		if rows := feeder.onStateOhlcvs(state, []*orm.DataSeries{row}, true); len(rows) != 1 {
			t.Fatalf("physical row was filtered: %v", rows)
		}
		if state.Latest != row || len(feeder.tfBars["1h"]) != 1 || len(called) != 1 ||
			called[0].TimeMS != hourMS || called[0].TimeFrame != "1h" {
			t.Fatalf("physical row did not reach state/cache/callback: state=%#v cache=%v callbacks=%v",
				state, feeder.tfBars, called)
		}
		waiting := &orm.DataSeries{TimeMS: 2 * hourMS}
		if rows := feeder.onStateOhlcvs(state, []*orm.DataSeries{waiting}, false); len(rows) != 0 ||
			state.WaitBar != waiting || len(feeder.tfBars["1h"]) != 1 || len(called) != 1 {
			t.Fatalf("unfinished physical row fired early: rows=%v state=%#v cache=%v callbacks=%v",
				rows, state, feeder.tfBars, called)
		}
	})

	t.Run("explicit state still requires logical coverage", func(t *testing.T) {
		state := &PairTFCache{TimeFrame: "1h", TFSecs: 60 * 60}
		var called []*orm.DataSeries
		feeder := &Feeder{
			ExSymbol: &orm.ExSymbol{Symbol: symbol},
			States:   []*PairTFCache{state},
			CallBack: func(evt *orm.DataSeries) { called = append(called, evt) },
			tfBars:   make(map[string][]*orm.DataSeries),
			coverage: coverage,
		}

		if rows := feeder.onStateOhlcvs(state, []*orm.DataSeries{{TimeMS: hourMS}}, true); len(rows) != 0 {
			t.Fatalf("explicit row bypassed logical coverage: %v", rows)
		}
		if state.Latest != nil || len(feeder.tfBars["1h"]) != 0 || len(called) != 0 {
			t.Fatalf("rejected row polluted state/cache/callback: state=%#v cache=%v callbacks=%v",
				state, feeder.tfBars, called)
		}
	})
}

func TestFeederKeepsUnfinishedProvedListingBucketWaiting(t *testing.T) {
	const hour = int64(60 * 60 * 1000)
	previousMode, previousData := core.BackTestMode, config.Data
	t.Cleanup(func() { core.BackTestMode, config.Data = previousMode, previousData })
	core.BackTestMode = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true

	symbol := "WLD/USDT:USDT"
	state := &PairTFCache{TimeFrame: "8h", TFSecs: 8 * 60 * 60}
	called := make([]int64, 0, 1)
	feeder := &Feeder{
		ExSymbol: &orm.ExSymbol{ID: 7, Exchange: "binance", Symbol: symbol, ListMs: 4 * hour},
		CallBack: func(evt *orm.DataSeries) { called = append(called, evt.TimeMS) },
		tfBars:   make(map[string][]*orm.DataSeries),
		coverage: &config.HistoricalCoverageConfig{
			BaselineEndMS: 24 * hour,
			Bars: map[string]map[string][]config.HistoricalCoverageRange{
				symbol: {
					"8h": {{StartMS: 4 * hour, StopMS: 24 * hour}},
					"1h": {{StartMS: 4 * hour, StopMS: 24 * hour}},
					"1m": {{StartMS: 4 * hour, StopMS: 24 * hour}},
				},
			},
		},
	}
	row := &orm.DataSeries{TimeMS: 0}
	if rows := feeder.onStateOhlcvs(state, []*orm.DataSeries{row}, false); len(rows) != 0 ||
		state.WaitBar != row || len(called) != 0 {
		t.Fatalf("unfinished listing bucket fired early: rows=%v wait=%v callbacks=%v", rows, state.WaitBar, called)
	}
}

func TestSeriesFeederWarmupStateUsesLastAllowedRow(t *testing.T) {
	called := make([]int64, 0, 1)
	state := &PairTFCache{TimeFrame: "1h", TFSecs: 3600}
	feeder := &SeriesFeeder{Feeder: Feeder{
		ExSymbol: &orm.ExSymbol{Symbol: "BTC/USDT:USDT"},
		CallBack: func(evt *orm.DataSeries) { called = append(called, evt.TimeMS) },
		States:   []*PairTFCache{state},
		coverage: &config.HistoricalCoverageConfig{
			BaselineEndMS: 500,
			Bars: map[string]map[string][]config.HistoricalCoverageRange{
				"BTC/USDT:USDT": {"1h": {{StartMS: 100, StopMS: 200}}},
			},
		},
	}}
	endMS := feeder.warmTf("1h", []*orm.DataSeries{{TimeMS: 100}, {TimeMS: 200}})
	wantEnd := int64(100 + 3600*1000)
	if endMS != wantEnd || state.SubNextMS != wantEnd || len(called) != 1 || called[0] != 100 {
		t.Fatalf("end=%d state=%d callbacks=%v want=%d", endMS, state.SubNextMS, called, wantEnd)
	}
}

func TestExplicitSeriesFeederRequiresRuntimeState(t *testing.T) {
	exs := &orm.ExSymbol{ID: 1, Symbol: "BTC/USDT:USDT"}
	if feeder, err := NewSeriesFeederWithRuntimeDeps(nil, exs, nil, false); err == nil || feeder != nil {
		t.Fatalf("nil runtime dependencies returned feeder=%v err=%v", feeder, err)
	}
	if feeder, err := NewSeriesFeederWithRuntimeDeps(&RuntimeDeps{}, exs, nil, false); err == nil || feeder != nil {
		t.Fatalf("missing strategy state returned feeder=%v err=%v", feeder, err)
	}
	if feeder, err := NewDBSeriesFeederWithRuntimeDeps(nil, exs, nil, false); err == nil || feeder != nil {
		t.Fatalf("nil historical runtime dependencies returned feeder=%v err=%v", feeder, err)
	}
	if feeder, err := NewDBSeriesFeederWithRuntimeDeps(&RuntimeDeps{}, exs, nil, false); err == nil || feeder != nil {
		t.Fatalf("missing historical strategy state returned feeder=%v err=%v", feeder, err)
	}
}

func TestExplicitSeriesFeedersRejectForeignRuntimeIdentityBeforeIO(t *testing.T) {
	deps := &RuntimeDeps{
		Strategies:   strat.NewState(),
		Exchange:     &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "owner", MarketType: banexg.MarketSpot}},
		ExchangeName: "owner",
		MarketType:   banexg.MarketSpot,
	}
	exs := &orm.ExSymbol{Exchange: "foreign", Market: banexg.MarketSpot, Symbol: "BTC/USDT"}
	if feeder, err := NewSeriesFeederWithRuntimeDeps(deps, exs, nil, false); err == nil || feeder != nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("series feeder = %#v, error = %v, want identity mismatch", feeder, err)
	}
	if feeder, err := NewDBSeriesFeederWithRuntimeDeps(deps, exs, nil, false); err == nil || feeder != nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("DB feeder = %#v, error = %v, want identity mismatch", feeder, err)
	}
}

func TestSeriesFeederWarmupUsesOnlyExplicitRuntimeState(t *testing.T) {
	const (
		symbol = "BTC/USDT:USDT"
		tf     = "1h"
	)
	previousBacktest := core.BackTestMode
	previousTime := btime.CurTimeMS
	t.Cleanup(func() {
		core.BackTestMode = previousBacktest
		btime.CurTimeMS = previousTime
	})
	core.BackTestMode = false
	btime.CurTimeMS = 987654321

	runtimeStrategies := strat.NewState()
	runtimeEnv := &ta.BarEnv{BarNum: 12}
	runtimeStrategies.SetEnv(symbol+"_"+tf, runtimeEnv)
	otherRuntimeStrategies := strat.NewState()
	otherRuntimeEnv := &ta.BarEnv{BarNum: 34}
	otherRuntimeStrategies.SetEnv(symbol+"_"+tf, otherRuntimeEnv)
	legacyEnv := &ta.BarEnv{BarNum: 77}
	legacyKey := symbol + "_" + tf
	previousLegacyEnv, hadPreviousLegacyEnv := strat.Envs[legacyKey]
	strat.Envs[legacyKey] = legacyEnv
	t.Cleanup(func() {
		if hadPreviousLegacyEnv {
			strat.Envs[legacyKey] = previousLegacyEnv
		} else {
			delete(strat.Envs, legacyKey)
		}
	})

	clock := btime.NewClockState(true, nil)
	seenWarmup := false
	feeder := &SeriesFeeder{Feeder: Feeder{
		ExSymbol: &orm.ExSymbol{Symbol: symbol},
		deps: &RuntimeDeps{
			Core:       &core.State{BackTestMode: true},
			Clock:      clock,
			Strategies: runtimeStrategies,
		},
		CallBack: func(evt *orm.DataSeries) { seenWarmup = evt.IsWarmUp },
		tfBars:   make(map[string][]*orm.DataSeries),
	}}

	endMS, err := feeder.warmTfWithErr(tf, []*orm.DataSeries{{TimeMS: 100}})
	if err != nil {
		t.Fatalf("warm explicit runtime: %v", err)
	}
	if want := int64(100 + 60*60*1000); endMS != want || clock.TimeMS() != want {
		t.Fatalf("end=%d clock=%d want=%d", endMS, clock.TimeMS(), want)
	}
	if !seenWarmup {
		t.Fatal("explicit warmup callback was not marked warm")
	}
	if runtimeEnv.BarNum != 0 {
		t.Fatalf("runtime env was not reset: BarNum=%d", runtimeEnv.BarNum)
	}
	if otherRuntimeEnv.BarNum != 34 {
		t.Fatalf("other runtime env was mutated: BarNum=%d", otherRuntimeEnv.BarNum)
	}
	if legacyEnv.BarNum != 77 {
		t.Fatalf("legacy env was mutated: BarNum=%d", legacyEnv.BarNum)
	}
	if core.BackTestMode || btime.CurTimeMS != 987654321 {
		t.Fatalf("legacy globals changed: backtest=%v time=%d", core.BackTestMode, btime.CurTimeMS)
	}
}

func TestLegacyFeederSubTfsHandlesExchangeFailure(t *testing.T) {
	feeder := &Feeder{ExSymbol: &orm.ExSymbol{Exchange: "not-a-real-exchange", Market: banexg.MarketSpot, Symbol: "BTC/USDT"}}
	if added := feeder.SubTfs([]string{"1m"}, false); added != nil {
		t.Fatalf("added timeframes = %v, want nil", added)
	}
}

func TestFeederSubTfsHandlesEmptyState(t *testing.T) {
	feeder := &Feeder{deps: &RuntimeDeps{Exchange: &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "test", MarketType: banexg.MarketSpot}}}}
	if added := feeder.SubTfs(nil, true); len(added) != 0 {
		t.Fatalf("added timeframes = %v, want empty", added)
	}
	if len(feeder.States) != 0 || feeder.hour != nil {
		t.Fatalf("empty subscription retained state: states=%v hour=%v", feeder.States, feeder.hour)
	}
}
