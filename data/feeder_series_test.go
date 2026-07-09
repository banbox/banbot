package data

import (
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
)

func TestApplyAdjSeriesKeepsGenericSeries(t *testing.T) {
	rows := []*orm.DataSeries{{
		Source:    "funding",
		Sid:       7,
		TimeMS:    1000,
		EndMS:     2000,
		TimeFrame: "1s",
		Closed:    true,
		Values:    map[string]any{"rate": 0.01},
	}}
	adj := &orm.AdjInfo{ExSymbol: &orm.ExSymbol{ID: 7}, CumFactor: 2}

	got := applyAdjSeries(adj, rows)
	if len(got) != 1 || got[0].Values["rate"] != 0.01 || got[0].HasOHLCV() {
		t.Fatalf("generic series should pass through unchanged: %+v", got)
	}
}

func TestApplyAdjSeriesAdjustsDataSeriesWithoutKlineConversion(t *testing.T) {
	rows := []*orm.DataSeries{{
		Source:    "custom_kline",
		Sid:       7,
		TimeMS:    1000,
		EndMS:     2000,
		TimeFrame: "1s",
		Closed:    true,
		IsWarmUp:  true,
		Values: map[string]any{
			"open":       1.0,
			"high":       2.0,
			"low":        0.5,
			"close":      1.5,
			"volume":     10.0,
			"quote":      99.0,
			"buy_volume": 4.0,
			"trade_num":  int64(3),
			"extra":      "keep",
		},
	}}
	adj := &orm.AdjInfo{ExSymbol: &orm.ExSymbol{ID: 7}, CumFactor: 2}

	got := applyAdjSeries(adj, rows)
	if got[0] == rows[0] || got[0].Values["open"] != 2.0 || got[0].Values["volume"] != 20.0 || got[0].Values["buy_volume"] != 8.0 {
		t.Fatalf("expected adjusted cloned series: %+v", got[0])
	}
	if got[0].Source != "custom_kline" || got[0].Values["extra"] != "keep" || got[0].Values["quote"] != 99.0 || got[0].Adj != adj {
		t.Fatalf("series metadata should be preserved: %+v", got[0])
	}
	if rows[0].Values["open"] != 1.0 {
		t.Fatalf("source row mutated: %+v", rows[0])
	}
}

func TestApplyAdjSeriesListTrimsGenericSeries(t *testing.T) {
	rows := []*orm.DataSeries{
		{Source: "funding", TimeMS: 1000, Values: map[string]any{"rate": 1.0}},
		{Source: "funding", TimeMS: 2000, Values: map[string]any{"rate": 2.0}},
		{Source: "funding", TimeMS: 3000, Values: map[string]any{"rate": 3.0}},
	}

	got := applyAdjSeriesList(&orm.ExSymbol{ID: 7}, nil, rows, core.AdjFront, 2500, 1)
	if len(got) != 1 || got[0].TimeMS != 2000 || got[0].Values["rate"] != 2.0 {
		t.Fatalf("unexpected trimmed generic series: %+v", got)
	}
}

func TestApplyAdjSeriesListAdjustsDataSeries(t *testing.T) {
	rows := []*orm.DataSeries{
		{Source: "custom_kline", TimeMS: 1000, Values: map[string]any{
			"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10.0,
		}},
		{Source: "custom_kline", TimeMS: 2000, Values: map[string]any{
			"open": 2.0, "high": 3.0, "low": 1.5, "close": 2.5, "volume": 20.0,
		}},
	}
	adjs := []*orm.AdjInfo{
		{Factor: 2, StartMS: 0, StopMS: 1500},
		{Factor: 3, StartMS: 1500, StopMS: 10_000},
	}

	got := applyAdjSeriesList(&orm.ExSymbol{ID: 7}, adjs, rows, core.AdjFront, 0, 0)
	if got[0] == rows[0] || got[0].Values["open"] != 3.0 || got[1].Values["close"] != 2.5 {
		t.Fatalf("expected adjusted data series: %+v", got)
	}
	if got[0].Source != "custom_kline" || rows[0].Values["open"] != 1.0 {
		t.Fatalf("series should preserve metadata and leave source unchanged: got=%+v src=%+v", got[0], rows[0])
	}
}

func TestLatestSeriesOHLCVOnlyChecksLastRow(t *testing.T) {
	rows := []*orm.DataSeries{
		{Source: "funding", Values: map[string]any{"rate": 1.0}},
		{Source: orm.SeriesSourceKline, Values: map[string]any{
			"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10.0,
		}},
	}

	if !latestSeriesOHLCV(rows) {
		t.Fatalf("expected latest OHLCV row to be enough")
	}
}
