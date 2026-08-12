package data

import (
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm"
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
	if missing == nil || missing.Allows("5m", 200) || !missing.Allows("5m", 500) {
		t.Fatalf("missing symbol did not fail closed before baseline end: %#v", missing)
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
		coverage: &config.HistoricalCoverageConfig{BaselineEndMS: 500},
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
