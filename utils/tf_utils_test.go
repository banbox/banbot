package utils

import (
	"testing"

	"github.com/banbox/banexg"
)

func TestBuildOHLCVSumsKlineExtraColumns(t *testing.T) {
	rows := []*banexg.Kline{
		{Time: 1_700_000_040_000, Open: 1, High: 3, Low: 1, Close: 2, Volume: 10, Quote: 100, BuyVolume: 4, TradeNum: 1},
		{Time: 1_700_000_100_000, Open: 2, High: 5, Low: 2, Close: 4, Volume: 20, Quote: 200, BuyVolume: 7, TradeNum: 2},
	}
	got, done := BuildOHLCV(rows, 120_000, 0, nil, 60_000, 0)
	if !done || len(got) != 1 {
		t.Fatalf("expected one finished bar, done=%v len=%d", done, len(got))
	}
	bar := got[0]
	if bar.Open != 1 || bar.High != 5 || bar.Low != 1 || bar.Close != 4 {
		t.Fatalf("unexpected ohlc: %+v", bar)
	}
	if bar.Volume != 30 || bar.Quote != 300 || bar.BuyVolume != 11 || bar.TradeNum != 3 {
		t.Fatalf("expected extra columns to sum, got %+v", bar)
	}
}
