package orm

import (
	"testing"

	"github.com/banbox/banexg"
)

func TestResampleDataSeriesPreservesOHLCVSemantics(t *testing.T) {
	exs := &ExSymbol{ID: 7, Symbol: "BTC/USDT"}
	rows := []*DataSeries{
		NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
			Time: 1_700_000_040_000, Open: 10, High: 13, Low: 9, Close: 12,
			Volume: 2, Quote: 21, BuyVolume: 1, TradeNum: 3,
		}, nil, false, true),
		NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
			Time: 1_700_000_100_000, Open: 12, High: 15, Low: 8, Close: 14,
			Volume: 5, Quote: 65, BuyVolume: 4, TradeNum: 6,
		}, nil, false, true),
	}

	got, done, err := ResampleDataSeries(exs, "2m", rows, nil, 120_000, 0, 60_000, 0, false)
	if err != nil {
		t.Fatalf("ResampleDataSeries returned error: %v", err)
	}
	if !done || len(got) != 1 {
		t.Fatalf("expected one finished row, done=%v len=%d", done, len(got))
	}
	view, err := got[0].OHLCV(exs)
	if err != nil {
		t.Fatalf("OHLCV projection returned error: %v", err)
	}
	if view.Open != 10 || view.High != 15 || view.Low != 8 || view.Close != 14 {
		t.Fatalf("unexpected prices: %+v", view)
	}
	if view.Volume != 7 || view.Quote != 86 || view.BuyVolume != 5 || view.TradeNum != 9 {
		t.Fatalf("unexpected accumulated fields: %+v", view)
	}
}

func TestResampleDataSeriesIgnoresZeroVolumePrices(t *testing.T) {
	exs := &ExSymbol{ID: 7, Symbol: "BTC/USDT"}
	rows := []*DataSeries{
		NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
			Time: 1_700_000_040_000, Open: 100, High: 200, Low: 50, Close: 150,
		}, nil, false, true),
		NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
			Time: 1_700_000_100_000, Open: 10, High: 13, Low: 9, Close: 12, Volume: 2,
		}, nil, false, true),
	}

	got, done, err := ResampleDataSeries(exs, "2m", rows, nil, 120_000, 0, 60_000, 0, false)
	if err != nil {
		t.Fatalf("ResampleDataSeries returned error: %v", err)
	}
	if !done || len(got) != 1 {
		t.Fatalf("expected one finished row, done=%v len=%d", done, len(got))
	}
	view, err := got[0].OHLCV(exs)
	if err != nil {
		t.Fatalf("OHLCV projection returned error: %v", err)
	}
	if view.Open != 10 || view.High != 13 || view.Low != 9 || view.Close != 12 || view.Volume != 2 {
		t.Fatalf("zero-volume prices polluted aggregate: %+v", view)
	}
}

func TestResampleDataSeriesDropsSparseZeroVolumeBucket(t *testing.T) {
	exs := &ExSymbol{ID: 7, Symbol: "BTC/USDT"}
	rows := []*DataSeries{
		NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
			Time: 1_700_000_040_000, Open: 10, High: 10, Low: 10, Close: 10,
		}, nil, false, true),
	}

	got, done, err := ResampleDataSeries(exs, "5m", rows, nil, 300_000, 0, 60_000, 0, false)
	if err != nil {
		t.Fatalf("ResampleDataSeries returned error: %v", err)
	}
	if done {
		t.Fatal("sparse bucket must not be marked finished")
	}
	if len(got) != 0 {
		t.Fatalf("expected sparse zero-volume bucket to be dropped, got %d rows", len(got))
	}
}

func TestResampleDataSeriesDoesNotRequireExSymbolForOHLCV(t *testing.T) {
	rows := []*DataSeries{
		{
			Source: SeriesSourceKline, Sid: 7, TimeMS: 1_700_000_040_000, EndMS: 1_700_000_100_000,
			Values: map[string]any{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 3.0},
		},
		{
			Source: SeriesSourceKline, Sid: 7, TimeMS: 1_700_000_100_000, EndMS: 1_700_000_160_000,
			Values: map[string]any{"open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0, "volume": 4.0},
		},
	}

	got, done, err := ResampleDataSeries(nil, "2m", rows, nil, 120_000, 0, 60_000, 0, false)
	if err != nil {
		t.Fatalf("ResampleDataSeries returned error: %v", err)
	}
	if !done || len(got) != 1 || got[0].Sid != 7 {
		t.Fatalf("unexpected aggregate: done=%v rows=%+v", done, got)
	}
}

func TestOHLCVSeriesValuesPreserveFieldsAndUseTargetSID(t *testing.T) {
	exs := &ExSymbol{ID: 11, Symbol: "ETH/USDT"}
	row := NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
		Time: 123_000, Open: 1, High: 2, Low: 0.5, Close: 1.5,
		Volume: 10, Quote: 15, BuyVolume: 6, TradeNum: 8,
	}, nil, false, true)

	got, err := ohlcvSeriesValues(row, 99)
	if err != nil {
		t.Fatalf("ohlcvSeriesValues returned error: %v", err)
	}
	if got.sid != 99 || got.timeMS != 123_000 {
		t.Fatalf("unexpected identity: %+v", got)
	}
	if got.open != 1 || got.high != 2 || got.low != 0.5 || got.close != 1.5 ||
		got.volume != 10 || got.quote != 15 || got.buyVolume != 6 || got.tradeNum != 8 {
		t.Fatalf("unexpected values: %+v", got)
	}
}

func TestOHLCVSeriesValuesRejectsInvalidRows(t *testing.T) {
	tests := []struct {
		name string
		row  *DataSeries
	}{
		{name: "nil row"},
		{
			name: "missing required field",
			row: &DataSeries{
				Sid: 1, TimeMS: 60_000, Values: map[string]any{
					"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ohlcvSeriesValues(test.row, 1); err == nil {
				t.Fatal("expected invalid row error")
			}
		})
	}
}

func TestNormalizeOHLCVSeriesValidatesWholeBatchBeforeWrite(t *testing.T) {
	exs := &ExSymbol{ID: 1, Symbol: "BTC/USDT"}
	valid := NewDataSeriesFromKline(exs, "1m", &banexg.Kline{Time: 60_000}, nil, false, true)
	invalid := &DataSeries{Sid: 1, TimeMS: 120_000, Values: map[string]any{"open": 1.0}}

	got, err := normalizeOHLCVSeries([]*DataSeries{nil, valid, invalid}, exs.ID)
	if err == nil {
		t.Fatal("expected invalid batch error")
	}
	if got != nil {
		t.Fatalf("invalid batch must not return partial values: %+v", got)
	}
}

func TestUpdateSeriesValidatesRowsBeforeRangeUpdate(t *testing.T) {
	exs := &ExSymbol{ID: 1, Symbol: "BTC/USDT"}
	invalid := &DataSeries{Sid: 1, TimeMS: 60_000, Values: map[string]any{"open": 1.0}}

	if err := (&Queries{}).UpdateSeries(exs, "1m", 60_000, 120_000, []*DataSeries{invalid}, false); err == nil {
		t.Fatal("expected invalid series error before database range update")
	}
}

func TestBindSeriesTargetPreservesAdjustmentAndUsesRequestedIdentity(t *testing.T) {
	underlying := &ExSymbol{ID: 12, Symbol: "AU2406"}
	target := &ExSymbol{ID: 99, Symbol: "AU888"}
	adj := &AdjInfo{ExSymbol: underlying, Factor: 1.2}
	row := NewDataSeriesFromKline(underlying, "1d", &banexg.Kline{Time: 1_700_000_000_000}, adj, false, true)

	got := bindSeriesTarget([]*DataSeries{row}, target)
	if len(got) != 1 {
		t.Fatalf("expected one row, got %d", len(got))
	}
	if got[0].Sid != target.ID || got[0].ExSymbol != target {
		t.Fatalf("unexpected target identity: %+v", got[0])
	}
	if got[0].Adj != adj || got[0].Adj.ExSymbol != underlying {
		t.Fatalf("adjustment source was not preserved: %+v", got[0].Adj)
	}
	if row.Sid != underlying.ID || row.ExSymbol != underlying {
		t.Fatalf("source row was mutated: %+v", row)
	}
}
