package strat

import (
	"math"
	"reflect"
	"testing"

	"github.com/banbox/banbot/orm"
	ta "github.com/banbox/banta"
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
			return []*DataSub{{Source: "kline", ExSymbol: s.Symbol, TimeFrame: "1h", Fields: []string{"close"}, SeriesFields: []string{"signal", "signal"}}}
		}},
	}
	subs := CollectDataSubs(job)
	want := []string{"close", "signal"}
	if len(subs) != 1 || !reflect.DeepEqual(subs[0].Fields, want) {
		t.Fatalf("expected requested fields %v, got %+v", want, subs)
	}
	if !reflect.DeepEqual(subs[0].SeriesFields, []string{"signal"}) {
		t.Fatalf("expected normalized series fields, got %+v", subs[0].SeriesFields)
	}
}

func TestDataHubBuildsConfiguredAndDefaultSeries(t *testing.T) {
	hub := NewDataHub(2)
	exs := &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT"}
	hub.Configure([]*DataSub{{
		Source: "kline", ExSymbol: exs, TimeFrame: "1m", SeriesFields: []string{"close", "trade_num"},
	}})
	avg := math.NaN()
	for i := int64(1); i <= 3; i++ {
		updated := hub.Set(&orm.DataSeries{
			Source: "kline", Sid: 7, TimeMS: i * 100, EndMS: i*100 + 100, TimeFrame: "1m",
			Values: map[string]any{"close": float64(i), "trade_num": i, "status": "ok"},
		})
		avg = ta.SMA(updated.Series("close"), 2).Get(0)
	}
	fields := hub.Get("1m", "kline", 7)
	if fields == nil || fields.TimeMS != 300 || fields.DoneMS != 400 {
		t.Fatalf("unexpected field timestamps: %+v", fields)
	}
	if fields.Series("close").Len() != 2 || fields.Float64("close") != 3 {
		t.Fatalf("unexpected close series: %+v", fields.Series("close"))
	}
	if fields.Series("trade_num").Get(0) != 3 || fields.Int64("trade_num") != 3 {
		t.Fatalf("explicit integer field was not converted to series")
	}
	if avg != 2.5 {
		t.Fatalf("configured series should support banta indicators, got %v", avg)
	}
	if fields.Series("status") != nil || fields.String("status") != "ok" {
		t.Fatalf("non-series value not retained: %v", fields.Raw("status"))
	}

	hub.Set(&orm.DataSeries{
		Source: "macro", Sid: 7, TimeMS: 100, EndMS: 200, TimeFrame: "1m",
		Values: map[string]any{"ratio": float32(1.5), "count": int64(4)},
	})
	macro := hub.Get("1m", "macro", 7)
	if macro.Series("ratio") == nil || macro.Series("count") != nil || macro.Int64("count") != 4 {
		t.Fatalf("default series selection should include only float values: %+v", macro)
	}
	if hub.Get("1m", "kline", 7).Float64("close") != 3 {
		t.Fatal("sources sharing sid/timeframe must remain isolated")
	}
	hub.Set(&orm.DataSeries{Source: "kline", Sid: 7, TimeMS: 300, EndMS: 400, TimeFrame: "1m", Values: map[string]any{"close": 99.0}})
	if fields.Series("close").Len() != 2 || fields.Float64("close") != 3 || math.IsNaN(fields.Float64("close")) {
		t.Fatal("duplicate end timestamps must not append or replace series values")
	}

	missingHub := NewDataHub()
	missingHub.Configure([]*DataSub{{Source: "macro", ExSymbol: exs, TimeFrame: "1m", SeriesFields: []string{"a", "b"}}})
	missingHub.Set(&orm.DataSeries{Source: "macro", Sid: 7, TimeMS: 100, EndMS: 200, TimeFrame: "1m", Values: map[string]any{"a": 1.0, "b": 2.0}})
	missingHub.Set(&orm.DataSeries{Source: "macro", Sid: 7, TimeMS: 200, EndMS: 300, TimeFrame: "1m", Values: map[string]any{"a": 3.0}})
	missingFields := missingHub.Get("1m", "macro", 7)
	if missingFields.Series("a").Len() != missingFields.Series("b").Len() || !math.IsNaN(missingFields.Series("b").Get(0)) {
		t.Fatal("missing configured fields must append NaN to keep series aligned")
	}
}

func TestDataHubAllReadyOnlyChecksClosingTimeframes(t *testing.T) {
	const minute = int64(60_000)
	base := int64(1_700_000_000_000 / (15 * minute) * (15 * minute))
	exs := &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT"}
	macro := &orm.ExSymbol{ID: 8, Symbol: "CPI_US"}
	hub := NewDataHub()
	hub.Configure([]*DataSub{
		{Source: "kline", ExSymbol: exs, TimeFrame: "1m"},
		{Source: "kline", ExSymbol: exs, TimeFrame: "5m"},
		{Source: "kline", ExSymbol: exs, TimeFrame: "15m"},
		{Source: "macro", ExSymbol: macro, TimeFrame: "5m"},
	})
	hub.Set(&orm.DataSeries{Source: "kline", Sid: 7, TimeFrame: "15m", TimeMS: base - 15*minute, EndMS: base, Values: map[string]any{"close": 1.0}})
	hub.Set(&orm.DataSeries{Source: "kline", Sid: 7, TimeFrame: "5m", TimeMS: base, EndMS: base + 5*minute, Values: map[string]any{"close": 1.0}})
	if hub.AllReady() {
		t.Fatal("1m must block 16:05 readiness until its data arrives")
	}
	hub.Set(&orm.DataSeries{Source: "kline", Sid: 7, TimeFrame: "1m", TimeMS: base + 4*minute, EndMS: base + 5*minute, Values: map[string]any{"close": 1.0}})
	if hub.AllReady() {
		t.Fatal("all sources and sids closing at 16:05 must be ready")
	}
	hub.Set(&orm.DataSeries{Source: "macro", Sid: 8, TimeFrame: "5m", TimeMS: base, EndMS: base + 5*minute, Values: map[string]any{"value": 1.0}})
	if !hub.AllReady() {
		t.Fatal("1m and 5m should be ready at 16:05; 15m must not block")
	}

	end15 := base + 15*minute
	hub.Set(&orm.DataSeries{Source: "kline", Sid: 7, TimeFrame: "1m", TimeMS: end15 - minute, EndMS: end15, Values: map[string]any{"close": 2.0}})
	hub.Set(&orm.DataSeries{Source: "kline", Sid: 7, TimeFrame: "5m", TimeMS: end15 - 5*minute, EndMS: end15, Values: map[string]any{"close": 2.0}})
	hub.Set(&orm.DataSeries{Source: "macro", Sid: 8, TimeFrame: "5m", TimeMS: end15 - 5*minute, EndMS: end15, Values: map[string]any{"value": 2.0}})
	if hub.AllReady() {
		t.Fatal("15m must block readiness at its closing boundary")
	}
	hub.Set(&orm.DataSeries{Source: "kline", Sid: 7, TimeFrame: "15m", TimeMS: base, EndMS: end15, Values: map[string]any{"close": 2.0}})
	if !hub.AllReady() {
		t.Fatal("all closing timeframes should be ready at 16:15")
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
