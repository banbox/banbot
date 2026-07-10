package strat

import (
	"reflect"
	"testing"

	"github.com/banbox/banbot/orm"
)

func TestCollectDataSubsNormalizesThirdPartySource(t *testing.T) {
	job := &StratJob{
		Strat: &TradeStrat{
			OnDataSubs: func(s *StratJob) []*DataSub {
				return []*DataSub{{Source: "", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 5}}
			},
		},
		Symbol: &orm.ExSymbol{ID: 7, Exchange: "macro", Market: "macro", Symbol: "CPI_US"},
	}

	subs := CollectDataSubs(job)
	if len(subs) != 1 {
		t.Fatalf("expected 1 data sub, got %d", len(subs))
	}
	if subs[0].Source != "kline" {
		t.Fatalf("expected empty source normalized to kline, got %+v", subs[0])
	}
	if !reflect.DeepEqual(subs[0].Fields, orm.DefaultKlineFields()) {
		t.Fatalf("expected default kline fields, got %v", subs[0].Fields)
	}
}

func TestCollectDataSubsPreservesRequestedFields(t *testing.T) {
	job := &StratJob{
		Symbol: &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT"},
		Strat: &TradeStrat{OnDataSubs: func(s *StratJob) []*DataSub {
			return []*DataSub{{Source: "kline", ExSymbol: s.Symbol, TimeFrame: "1h", Fields: []string{"close", "signal", "close"}}}
		}},
	}
	subs := CollectDataSubs(job)
	want := []string{"close", "signal"}
	if len(subs) != 1 || !reflect.DeepEqual(subs[0].Fields, want) {
		t.Fatalf("expected requested fields %v, got %+v", want, subs)
	}
}

func TestDataHubLatestAndWindow(t *testing.T) {
	hub := NewDataHub()
	hub.Set(&orm.DataSeries{Source: "kline", Sid: 1, TimeMS: 100, EndMS: 200, TimeFrame: "1m", Values: map[string]any{"close": 1.0}})
	hub.Set(&orm.DataSeries{Source: "kline", Sid: 1, TimeMS: 200, EndMS: 300, TimeFrame: "1m", Values: map[string]any{"close": 2.0}})
	hub.Set(&orm.DataSeries{Source: "macro", Sid: 7, TimeMS: 300, EndMS: 400, TimeFrame: "1d", IsWarmUp: true, Values: map[string]any{"value": 10.0}})
	hub.Set(&orm.DataSeries{Source: "macro", Sid: 7, TimeMS: 400, EndMS: 500, TimeFrame: "1d", Values: map[string]any{"value": 11.0}})
	hub.Set(&orm.DataSeries{Source: "macro", Sid: 7, TimeMS: 500, EndMS: 600, TimeFrame: "1d", Values: map[string]any{"value": 12.0}})

	latest := hub.Latest("kline", 1, "1m")
	if latest == nil || latest.TimeMS != 200 {
		t.Fatalf("unexpected kline latest event: %+v", latest)
	}
	window := hub.Window("kline", 1, "1m", 2)
	if len(window) != 2 || window[0].TimeMS != 100 || window[1].TimeMS != 200 {
		t.Fatalf("unexpected kline window: %+v", window)
	}

	macroLatest := hub.Latest("macro", 7, "1d")
	if macroLatest == nil || macroLatest.TimeMS != 500 || macroLatest.IsWarmUp {
		t.Fatalf("unexpected macro latest event: %+v", macroLatest)
	}
	macroWindow := hub.Window("macro", 7, "1d", 2)
	if len(macroWindow) != 2 || macroWindow[0].TimeMS != 400 || macroWindow[1].TimeMS != 500 {
		t.Fatalf("unexpected macro trailing window: %+v", macroWindow)
	}
	fullMacroWindow := hub.Window("macro", 7, "1d", 0)
	if len(fullMacroWindow) != 3 || !fullMacroWindow[0].IsWarmUp || fullMacroWindow[2].TimeMS != 500 {
		t.Fatalf("unexpected macro full window ordering: %+v", fullMacroWindow)
	}
}

func TestDataHubLatestAndWindowSeparatesThirdPartyAndLegacySeriesBySource(t *testing.T) {
	hub := NewDataHub()
	hub.Set(&orm.DataSeries{Source: "kline", Sid: 7, TimeMS: 100, EndMS: 200, TimeFrame: "1d", Values: map[string]any{"close": 1.0}})
	hub.Set(&orm.DataSeries{Source: "macro", Sid: 7, TimeMS: 300, EndMS: 400, TimeFrame: "1d", IsWarmUp: true, Values: map[string]any{"value": 10.0}})
	hub.Set(&orm.DataSeries{Source: "macro", Sid: 7, TimeMS: 400, EndMS: 500, TimeFrame: "1d", Values: map[string]any{"value": 11.0}})
	hub.Set(&orm.DataSeries{Source: "kline", Sid: 7, TimeMS: 500, EndMS: 600, TimeFrame: "1d", Values: map[string]any{"close": 2.0}})

	legacyLatest := hub.Latest("kline", 7, "1d")
	if legacyLatest == nil || legacyLatest.TimeMS != 500 {
		t.Fatalf("expected legacy latest at 500, got %+v", legacyLatest)
	}
	thirdPartyLatest := hub.Latest("macro", 7, "1d")
	if thirdPartyLatest == nil || thirdPartyLatest.TimeMS != 400 || thirdPartyLatest.IsWarmUp {
		t.Fatalf("expected third-party latest at 400 without warmup, got %+v", thirdPartyLatest)
	}
	legacyWindow := hub.Window("kline", 7, "1d", 0)
	if len(legacyWindow) != 2 || legacyWindow[0].TimeMS != 100 || legacyWindow[1].TimeMS != 500 {
		t.Fatalf("expected legacy window to remain source-scoped, got %+v", legacyWindow)
	}
	thirdPartyWindow := hub.Window("macro", 7, "1d", 0)
	if len(thirdPartyWindow) != 2 || !thirdPartyWindow[0].IsWarmUp || thirdPartyWindow[1].TimeMS != 400 {
		t.Fatalf("expected third-party window to remain source-scoped, got %+v", thirdPartyWindow)
	}
}

func TestCollectDataSubsBridgesLegacyPairInfos(t *testing.T) {
	job := &StratJob{
		Strat: &TradeStrat{
			OnPairInfos: func(s *StratJob) []*PairSub {
				return []*PairSub{{Pair: "_cur_", TimeFrame: "5m", WarmupNum: 20}}
			},
			OnDataSubs: func(s *StratJob) []*DataSub {
				return []*DataSub{{Source: "kline", ExSymbol: s.Symbol, TimeFrame: "1h", WarmupNum: 5}}
			},
		},
		Symbol: &orm.ExSymbol{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
	}

	subs := CollectDataSubs(job)
	if len(subs) != 2 {
		t.Fatalf("expected 2 data subs, got %d", len(subs))
	}
	if subs[0].Source != "kline" || subs[0].ExSymbol.Symbol != "BTC/USDT" || subs[0].TimeFrame != "5m" {
		t.Fatalf("unexpected legacy bridged sub: %+v", subs[0])
	}
	if subs[1].TimeFrame != "1h" {
		t.Fatalf("unexpected explicit data sub: %+v", subs[1])
	}
}
