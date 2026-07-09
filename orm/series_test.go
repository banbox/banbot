package orm

import (
	"testing"

	"github.com/banbox/banexg"
)

func TestKlineToSeriesRoundTrip(t *testing.T) {
	bar := &InfoKline{
		PairTFKline: testPairTFKline("BTC/USDT", "5m", 1_700_000_000_000),
		Sid:         42,
		IsWarmUp:    true,
	}
	bar.Open = 1
	bar.High = 2
	bar.Low = 0.5
	bar.Close = 1.5
	bar.Volume = 100
	bar.Quote = 150
	bar.BuyVolume = 60
	bar.TradeNum = 8

	evt := KlineToDataSeries(bar)
	if evt == nil {
		t.Fatalf("expected series event")
	}
	if evt.Sid != 42 || evt.Source != "kline" || evt.TimeFrame != "5m" || !evt.IsWarmUp {
		t.Fatalf("unexpected series header: %+v", evt)
	}

	exs := &ExSymbol{ID: 42, Symbol: "BTC/USDT"}
	got, err := AsKline(evt, exs, nil)
	if err != nil {
		t.Fatalf("AsKline returned error: %v", err)
	}
	if got.Sid != 42 || got.Symbol != "BTC/USDT" || got.TimeFrame != "5m" {
		t.Fatalf("unexpected kline header: %+v", got)
	}
	if got.Open != bar.Open || got.High != bar.High || got.Low != bar.Low || got.Close != bar.Close {
		t.Fatalf("unexpected OHLC values: %+v", got.Kline)
	}
}

func TestAsKlineRequiresOHLCV(t *testing.T) {
	evt := &DataSeries{
		Source:    "macro",
		Sid:       9,
		TimeMS:    100,
		EndMS:     200,
		TimeFrame: "1d",
		Closed:    true,
		Values: map[string]any{
			"close":  10.0,
			"volume": 11.0,
		},
	}
	_, err := AsKline(evt, &ExSymbol{ID: 9, Symbol: "CPI_US"}, nil)
	if err == nil {
		t.Fatalf("expected AsKline to reject non-kline-shaped series")
	}
}

func TestResampleSeriesRecordsUsesAggRules(t *testing.T) {
	if !RegisterAggRule("test_span", func(rows []*DataRecord, field SeriesField) (any, error) {
		first, err := aggFirst(rows, field)
		if err != nil {
			return nil, err
		}
		last, err := aggLast(rows, field)
		if err != nil {
			return nil, err
		}
		firstVal, err := seriesFloatAny(first)
		if err != nil {
			return nil, err
		}
		lastVal, err := seriesFloatAny(last)
		if err != nil {
			return nil, err
		}
		return lastVal - firstVal, nil
	}) {
		t.Fatal("expected custom agg rule registration to succeed")
	}

	info := NewSeriesInfo("metric", "1m", []SeriesField{
		{Name: "price", Type: "float"},
		{Name: "volume", Type: "float"},
		{Name: "count", Type: "int"},
		{Name: "state", Type: "string"},
		{Name: "spread", Type: "float"},
	})
	exs := &ExSymbol{ID: 3, AggRules: `{"price":"mid","volume":"sum","count":"sum","spread":"test_span"}`}
	rows := []*DataRecord{
		{Sid: 3, TimeMS: 1_700_000_040_000, EndMS: 1_700_000_100_000, Values: map[string]any{"price": 10.0, "volume": 3.0, "count": int64(2), "state": "open", "spread": 100.0}},
		{Sid: 3, TimeMS: 1_700_000_100_000, EndMS: 1_700_000_160_000, Closed: true, Values: map[string]any{"price": 14.0, "volume": 7.0, "count": int64(5), "state": "close", "spread": 130.0}},
	}

	got, err := ResampleSeriesRecords(info, exs, rows, 120_000, 0)
	if err != nil {
		t.Fatalf("ResampleSeriesRecords returned error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected one row, got %d", len(got))
	}
	values := got[0].Values
	if values["price"] != 12.0 || values["volume"] != 10.0 || values["count"] != int64(7) ||
		values["state"] != "close" || values["spread"] != 30.0 {
		t.Fatalf("unexpected values: %+v", values)
	}
}

func TestResampleDataSeriesUsesAggRulesForGenericSeries(t *testing.T) {
	exs := &ExSymbol{ID: 3, AggRules: `{"rate":"avg","volume":"sum"}`}
	rows := []*DataSeries{
		{Source: "funding", Sid: 3, TimeMS: 1_700_000_040_000, EndMS: 1_700_000_100_000, Closed: true, Values: map[string]any{"rate": 1.0, "volume": 2.0}},
		{Source: "funding", Sid: 3, TimeMS: 1_700_000_100_000, EndMS: 1_700_000_160_000, Closed: true, Values: map[string]any{"rate": 3.0, "volume": 5.0}},
	}

	got, done, err := ResampleDataSeries(exs, "2m", rows, nil, 120_000, 0, 60_000, 0, true)
	if err != nil {
		t.Fatalf("ResampleDataSeries returned error: %v", err)
	}
	if !done || len(got) != 1 {
		t.Fatalf("expected one finished row, done=%v len=%d", done, len(got))
	}
	if got[0].Source != "funding" || got[0].TimeFrame != "2m" || !got[0].IsWarmUp {
		t.Fatalf("unexpected series header: %+v", got[0])
	}
	if got[0].Values["rate"] != 2.0 || got[0].Values["volume"] != 7.0 {
		t.Fatalf("unexpected values: %+v", got[0].Values)
	}
}

func testPairTFKline(symbol, tf string, ts int64) *banexg.PairTFKline {
	return &banexg.PairTFKline{
		Kline:     banexg.Kline{Time: ts},
		Symbol:    symbol,
		TimeFrame: tf,
	}
}
