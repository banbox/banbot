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

func testPairTFKline(symbol, tf string, ts int64) *banexg.PairTFKline {
	return &banexg.PairTFKline{
		Kline:     banexg.Kline{Time: ts},
		Symbol:    symbol,
		TimeFrame: tf,
	}
}
