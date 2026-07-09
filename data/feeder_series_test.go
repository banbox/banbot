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
