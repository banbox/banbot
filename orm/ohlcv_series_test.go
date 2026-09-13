package orm

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/internal/testutil"
	"github.com/banbox/banexg"
	"github.com/jackc/pgx/v5"
)

func TestExplicitQueryAlignmentUsesBoundExchange(t *testing.T) {
	const hourMS = int64(60 * 60 * 1000)
	const dayMS = 24 * hourMS
	const wantOffsetMS = 6 * hourMS
	exs := &ExSymbol{ID: 7, Exchange: "legacy", Market: banexg.MarketSpot, Symbol: "RUNTIME/USDT"}
	exchange := &localReadExchange{
		info: &banexg.ExgInfo{ID: "runtime", MarketType: banexg.MarketSpot},
		market: &banexg.Market{
			Symbol:     exs.Symbol,
			DayTimes:   [][2]int64{{9 * hourMS, 15 * hourMS}},
			NightTimes: [][2]int64{{21 * hourMS, 23 * hourMS}},
		},
	}
	state := NewSymbolStateWithIdentity(exs.Exchange, exs.Market)
	q := NewWithStorage(nil, NewStorage(nil, true, "storage:alignment"))
	q = q.WithSeriesSymbolState(state).WithExchange(exchange)

	if got := q.alignOff(exs, dayMS); got != wantOffsetMS {
		t.Fatalf("explicit alignment offset = %d, want %d", got, wantOffsetMS)
	}
	if got := q.alignOff(exs, hourMS); got != 0 {
		t.Fatalf("short timeframe alignment offset = %d, want 0", got)
	}
}

func TestSeriesForwardReadKeepsHistoricalEndAndClampsLiveUnfinishedBar(t *testing.T) {
	const (
		startMS = int64(1_767_225_600_000)
		endMS   = startMS + 4*60*60*1000
	)
	exs := &ExSymbol{ID: 7, Exchange: "runtime", Market: banexg.MarketSpot, Symbol: "BTC/USDT"}
	exchange := &questVisibilityExchangeStub{info: &banexg.ExgInfo{ID: exs.Exchange, MarketType: exs.Market}}
	readEnd := func(backtest, withUnFinish bool) string {
		var sqlText string
		q := NewWithStorage(&visibilityDBStub{query: func(sql string, _ ...interface{}) (pgx.Rows, error) {
			sqlText = sql
			return newInterfaceRows(nil), nil
		}}, NewStorage(nil, true, t.Name())).
			WithExchange(exchange).
			WithKlineRuntimeOptions(KlineRuntimeOptions{Backtest: backtest, NowMS: startMS, ClockValid: true})
		if _, _, err := q.GetSeriesFields(exs, "1h", nil, startMS, endMS, 0, withUnFinish); err != nil {
			t.Fatal(err)
		}
		return sqlText
	}
	if sqlText := readEnd(true, false); !strings.Contains(sqlText, "ts < cast(1767240000000000 as timestamp)") {
		t.Fatalf("historical forward read was clipped: %s", sqlText)
	}
	if sqlText := readEnd(false, true); !strings.Contains(sqlText, "ts < cast(1767225600000000 as timestamp)") {
		t.Fatalf("live unfinished-bar read was not clamped: %s", sqlText)
	}
}

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

func TestResampleDataSeriesPreservesCustomFields(t *testing.T) {
	exs := &ExSymbol{ID: 7, Symbol: "BTC/USDT"}
	first := NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
		Time: 1_700_000_040_000, Open: 10, High: 13, Low: 9, Close: 12, Volume: 2,
	}, nil, false, true)
	second := NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
		Time: 1_700_000_100_000, Open: 12, High: 15, Low: 8, Close: 14, Volume: 5,
	}, nil, false, true)
	first.Values["signal"] = "buy"
	second.Values["signal"] = "sell"

	got, _, err := ResampleDataSeries(exs, "2m", []*DataSeries{first, second}, nil, 120_000, 0, 60_000, 0, false)
	if err != nil {
		t.Fatalf("ResampleDataSeries returned error: %v", err)
	}
	if len(got) != 1 || got[0].Values["signal"] != "sell" {
		t.Fatalf("custom field was not preserved with last-row semantics: %+v", got)
	}
}

func TestResampleDataSeriesUsesAggRulesForKlineExtensionsAndPreservesBuiltins(t *testing.T) {
	const captureRule = "ohlcv_series_capture"
	var capturedType string
	var capturedRows int
	var sawExplicitNull bool
	if !RegisterAggRule(captureRule, func(rows []*DataRecord, field SeriesField) (any, error) {
		capturedType = field.Type
		capturedRows = len(rows)
		if len(rows) > 0 && rows[0] != nil && rows[0].Values != nil {
			value, ok := rows[0].Values[field.Name]
			sawExplicitNull = ok && value == nil
		}
		return aggLast(rows, field)
	}) {
		t.Fatal("expected custom agg rule registration to succeed")
	}

	exs := &ExSymbol{
		ID: 7, Symbol: "BTC/USDT",
		AggRules: `{"open":"last","high":"min","low":"max","close":"first","volume":"last","quote":"last","buy_volume":"last","trade_num":"last","signal":"first","count":"sum","nullable":"first","captured":"ohlcv_series_capture","average":"avg"}`,
	}
	base := int64(1_700_000_100_000)
	rows := []*DataSeries{
		NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
			Time: base, Open: 10, High: 13, Low: 9, Close: 12, Volume: 2, Quote: 21, BuyVolume: 1, TradeNum: 3,
		}, nil, false, true),
		NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
			Time: base + 60_000, Open: 12, High: 15, Low: 8, Close: 14, Volume: 5, Quote: 65, BuyVolume: 4, TradeNum: 6,
		}, nil, false, true),
		NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
			Time: base + 120_000, Open: 14, High: 17, Low: 7, Close: 16, Volume: 3, Quote: 30, BuyVolume: 2, TradeNum: 4,
		}, nil, false, true),
	}
	rows[0].Values["signal"] = "first"
	rows[1].Values["signal"] = "middle"
	rows[2].Values["signal"] = "last"
	rows[0].Values["count"] = int64(2)
	rows[1].Values["count"] = int64(5)
	rows[2].Values["count"] = int64(7)
	rows[0].Values["nullable"] = nil
	rows[1].Values["nullable"] = "later"
	rows[2].Values["nullable"] = "latest"
	rows[0].Values["captured"] = nil
	rows[1].Values["captured"] = int64(4)
	rows[2].Values["captured"] = int64(8)
	rows[0].Values["average"] = 1.0
	rows[1].Values["average"] = 3.0
	rows[2].Values["average"] = 9.0

	got, done, err := ResampleDataSeries(exs, "3m", rows, nil, 180_000, 0, 60_000, 0, false)
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
	if view.Open != 10 || view.High != 17 || view.Low != 7 || view.Close != 16 ||
		view.Volume != 10 || view.Quote != 116 || view.BuyVolume != 7 || view.TradeNum != 13 {
		t.Fatalf("AggRules must not replace built-in OHLCV rules: %+v", view)
	}
	if got[0].Values["signal"] != "first" {
		t.Fatalf("extension first rule was not applied: %#v", got[0].Values["signal"])
	}
	if value, ok := got[0].Values["count"].(int64); !ok || value != 14 {
		t.Fatalf("extension sum/type mismatch: value=%#v type=%T", got[0].Values["count"], got[0].Values["count"])
	}
	if value, ok := got[0].Values["nullable"]; !ok || value != nil {
		t.Fatalf("extension first rule must preserve explicit NULL: %#v present=%v", value, ok)
	}
	if value, ok := got[0].Values["captured"].(int64); !ok || value != 8 {
		t.Fatalf("registered extension rule result mismatch: value=%#v type=%T", got[0].Values["captured"], got[0].Values["captured"])
	}
	if capturedType != "int" || capturedRows != 3 || !sawExplicitNull {
		t.Fatalf("registered rule did not receive field type/NULL-preserving rows: type=%q rows=%d null=%v", capturedType, capturedRows, sawExplicitNull)
	}
	if value, ok := got[0].Values["average"].(float64); !ok || value != 13.0/3.0 {
		t.Fatalf("extension avg was not evaluated over the whole bucket: value=%#v type=%T", got[0].Values["average"], got[0].Values["average"])
	}
}

func TestResampleDataSeriesPreservesRawInputsAcrossBatches(t *testing.T) {
	exs := &ExSymbol{
		ID: 7, Symbol: "BTC/USDT",
		AggRules: `{"average":"avg"}`,
	}
	base := int64(1_700_000_100_000)
	rows := make([]*DataSeries, 0, 3)
	for i, value := range []float64{1, 3, 9} {
		row := NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
			Time: base + int64(i)*60_000, Open: 10, High: 10, Low: 10, Close: 10, Volume: 1,
		}, nil, false, true)
		row.Values["average"] = value
		rows = append(rows, row)
	}

	first, done, err := ResampleDataSeries(exs, "3m", rows[:2], nil, 180_000, 0, 60_000, 0, false)
	if err != nil {
		t.Fatalf("first batch returned error: %v", err)
	}
	if done || len(first) != 1 {
		t.Fatalf("first batch should leave one unfinished bucket, done=%v len=%d", done, len(first))
	}
	second, done, err := ResampleDataSeries(exs, "3m", rows[2:], first, 180_000, 0, 60_000, 0, false)
	if err != nil {
		t.Fatalf("second batch returned error: %v", err)
	}
	if !done || len(second) != 1 {
		t.Fatalf("second batch should finish one bucket, done=%v len=%d", done, len(second))
	}
	if got, ok := second[0].Values["average"].(float64); !ok || got != 13.0/3.0 {
		t.Fatalf("cross-batch average = %#v (%T), want %v", second[0].Values["average"], second[0].Values["average"], 13.0/3.0)
	}
}

func TestResampleGenericSeriesPreservesRawInputsAcrossBatches(t *testing.T) {
	const source = "macro"
	const averageRule = "generic_batch_average"
	if !RegisterAggRule(averageRule, func(rows []*DataRecord, field SeriesField) (any, error) {
		var sum float64
		var count int
		for _, row := range rows {
			if row == nil {
				continue
			}
			value, ok := row.Values[field.Name].(float64)
			if ok {
				sum += value
				count++
			}
		}
		if count == 0 {
			return nil, nil
		}
		return sum / float64(count), nil
	}) {
		t.Fatal("expected custom generic aggregation rule registration to succeed")
	}
	exs := &ExSymbol{ID: 8, Symbol: "CPI", AggRules: `{"value":"generic_batch_average"}`}
	base := int64(1_700_000_100_000)
	rows := make([]*DataSeries, 0, 3)
	for i, value := range []float64{1, 3, 9} {
		rows = append(rows, &DataSeries{
			Source: source, Sid: exs.ID, TimeMS: base + int64(i)*60_000, TimeFrame: "1m",
			Values: map[string]any{"value": value}, ExSymbol: exs,
		})
	}
	first, done, err := ResampleDataSeries(exs, "3m", rows[:2], nil, 180_000, 0, 60_000, 0, false)
	if err != nil || done || len(first) != 1 {
		t.Fatalf("first generic batch = len:%d done:%v err:%v", len(first), done, err)
	}
	second, done, err := ResampleDataSeries(exs, "3m", rows[2:], first, 180_000, 0, 60_000, 0, false)
	if err != nil || !done || len(second) != 1 {
		t.Fatalf("second generic batch = len:%d done:%v err:%v", len(second), done, err)
	}
	if got := second[0].Values["value"]; got != 13.0/3.0 {
		t.Fatalf("cross-batch generic average = %#v, want %v", got, 13.0/3.0)
	}
}

func TestResampleDataSeriesUsesAggRulesForKlineExtensions(t *testing.T) {
	const ruleName = "ohlcv_series_test_bucket_size"
	var fieldType string
	if !RegisterAggRule(ruleName, func(rows []*DataRecord, field SeriesField) (any, error) {
		fieldType = field.Type
		return int64(len(rows)), nil
	}) {
		t.Fatal("expected custom agg rule registration to succeed")
	}

	exs := &ExSymbol{ID: 7, Symbol: "BTC/USDT", AggRules: `{"bucket_size":"` + ruleName + `"}`}
	row := NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
		Time: 1_700_000_040_000, Open: 10, High: 13, Low: 9, Close: 12, Volume: 2,
	}, nil, false, true)
	row.Values["bucket_size"] = 30.0

	got, _, err := ResampleDataSeries(exs, "2m", []*DataSeries{row}, nil, 120_000, 0, 60_000, 0, false)
	if err != nil {
		t.Fatalf("ResampleDataSeries returned error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected one aggregate row, got %d", len(got))
	}
	if fieldType != "float" {
		t.Fatalf("registered rule received field type %q, want float", fieldType)
	}
	if got[0].Values["bucket_size"] != int64(1) {
		t.Fatalf("registered rule was not applied to a single-row bucket: %#v", got[0].Values["bucket_size"])
	}
}

func TestResampleDataSeriesPreservesNullCustomField(t *testing.T) {
	exs := &ExSymbol{ID: 7, Symbol: "BTC/USDT"}
	first := NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
		Time: 1_700_000_040_000, Open: 10, High: 13, Low: 9, Close: 12, Volume: 2,
	}, nil, false, true)
	second := NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
		Time: 1_700_000_100_000, Open: 12, High: 15, Low: 8, Close: 14, Volume: 5,
	}, nil, false, true)
	first.Values["optional_note"] = "present"
	second.Values["optional_note"] = nil

	got, _, err := ResampleDataSeries(exs, "2m", []*DataSeries{first, second}, nil, 120_000, 0, 60_000, 0, false)
	if err != nil {
		t.Fatalf("ResampleDataSeries returned error: %v", err)
	}
	value, ok := got[0].Values["optional_note"]
	if !ok || value != nil {
		t.Fatalf("aggregate must preserve explicit NULL extension key: %#v, present=%v", value, ok)
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
	if got.Sid != 99 || got.TimeMS != 123_000 {
		t.Fatalf("unexpected identity: %+v", got)
	}
	if got.Values["open"] != 1.0 || got.Values["high"] != 2.0 || got.Values["low"] != 0.5 || got.Values["close"] != 1.5 ||
		got.Values["volume"] != 10.0 || got.Values["quote"] != 15.0 || got.Values["buy_volume"] != 6.0 || got.Values["trade_num"] != int64(8) {
		t.Fatalf("unexpected values: %+v", got.Values)
	}
}

func TestOHLCVSeriesValuesPreserveExtraFieldsForWrite(t *testing.T) {
	row := NewDataSeriesFromKline(&ExSymbol{ID: 11}, "1m", &banexg.Kline{Time: 123_000}, nil, false, true)
	row.Values["signal_b"] = "sell"
	row.Values["signal_a"] = 1.25
	row.Values["signal_nil"] = nil
	got, err := normalizeOHLCVSeries([]*DataSeries{row}, 11)
	if err != nil {
		t.Fatalf("normalizeOHLCVSeries returned error: %v", err)
	}
	wantFields := []string{"signal_a", "signal_b", "signal_nil"}
	if len(got) != 1 || !reflect.DeepEqual(klineExtraFields(got), wantFields) {
		t.Fatalf("unexpected extra fields: %+v", got)
	}
	if got[0].Values["signal_a"] != 1.25 || got[0].Values["signal_b"] != "sell" {
		t.Fatalf("extra values were dropped: %+v", got[0].Values)
	}
	if value, ok := got[0].Values["signal_nil"]; !ok || value != nil {
		t.Fatalf("explicit nil extra value was dropped: %#v, present=%v", value, ok)
	}
	wantCols := []string{"sid", "ts", "open", "high", "low", "close", "volume", "quote", "buy_volume", "trade_num", "signal_a", "signal_b", "signal_nil"}
	if cols := klineInsertColumns("ts", wantFields); !reflect.DeepEqual(cols, wantCols) {
		t.Fatalf("unexpected insert columns: %v", cols)
	}
}

func TestKlineSelectProjectionUsesDefaultsOrRequestedFields(t *testing.T) {
	wantDefault := `"open","high","low","close","volume","quote","buy_volume","trade_num"`
	if got := klineSelectProjection(nil, false); got != wantDefault {
		t.Fatalf("unexpected default projection: %s", got)
	}
	wantRequested := `"close","signal","sid"`
	if got := klineSelectProjection([]string{"close", "signal", "close"}, true); got != wantRequested {
		t.Fatalf("unexpected requested projection: %s", got)
	}
}

func TestBuildQuestKlineAggregateQueryIncludesDynamicColumns(t *testing.T) {
	columns := []questTableColumn{
		{Name: "sid", Type: "INT", UpsertKey: true},
		{Name: "ts", Type: "TIMESTAMP", Designated: true, UpsertKey: true},
		{Name: "open", Type: "DOUBLE"},
		{Name: "high", Type: "DOUBLE"},
		{Name: "low", Type: "DOUBLE"},
		{Name: "close", Type: "DOUBLE"},
		{Name: "volume", Type: "DOUBLE"},
		{Name: "quote", Type: "DOUBLE"},
		{Name: "buy_volume", Type: "DOUBLE"},
		{Name: "trade_num", Type: "LONG"},
		{Name: "optional_note", Type: "STRING"},
	}
	query, fields, err := buildQuestKlineAggregateQuery("kline_1m", columns)
	if err != nil {
		t.Fatalf("buildQuestKlineAggregateQuery returned error: %v", err)
	}
	if !reflect.DeepEqual(fields, []string{"open", "high", "low", "close", "volume", "quote", "buy_volume", "trade_num", "optional_note"}) {
		t.Fatalf("unexpected aggregate fields: %v", fields)
	}
	for _, want := range []string{`cast("ts" as long)/1000`, `"open"`, `"optional_note"`, `FROM "kline_1m"`} {
		if !strings.Contains(query, want) {
			t.Fatalf("aggregate query %q missing %q", query, want)
		}
	}
	if _, err := buildQuestKlineAddColumnSQL("kline_2m", questTableColumn{Name: "optional_note", Type: "STRING"}); err != nil {
		t.Fatalf("dynamic extension column must have valid DDL: %v", err)
	}
}

func TestMapToSeriesFieldsPreservesNullKeysAndValueTypes(t *testing.T) {
	fields := []string{"open", "quote", "signal"}
	rows := newInterfaceRows([][]any{{
		int64(123_000), float64(1.25), nil, "buy",
	}})

	got, err := mapToSeriesFields(&ExSymbol{ID: 7}, "1m", fields, rows, nil)
	if err != nil {
		t.Fatalf("mapToSeriesFields returned error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected one row, got %d", len(got))
	}
	valueMap := got[0].Values
	if value, ok := valueMap["quote"]; !ok || value != nil {
		t.Fatalf("SQL NULL must remain an explicit nil value: %#v, present=%v", value, ok)
	}
	if value, ok := valueMap["signal"]; !ok || value != "buy" {
		t.Fatalf("non-NULL extension field was not preserved: %#v, present=%v", value, ok)
	}
	if _, ok := valueMap["open"].(float64); !ok {
		t.Fatalf("OHLCV value type changed: %T", valueMap["open"])
	}
}

func TestHandleSeriesBatchFieldsPreservesNullKeysAndValueTypes(t *testing.T) {
	fields := []string{"open", "quote", "signal"}
	exsMap := map[int32]*ExSymbol{7: {ID: 7}, 8: {ID: 8}}
	rows := newInterfaceRows([][]any{
		{int64(123_000), float64(1.25), nil, "buy", int32(7)},
		{int64(183_000), nil, float32(2.5), int64(42), int32(8)},
	})
	got := make(map[int32][]*DataSeries)

	if err := handleSeriesBatchFields(exsMap, "1m", fields, 60_000, "", rows, nil,
		func(sid int32, series []*DataSeries) { got[sid] = series }); err != nil {
		t.Fatalf("handleSeriesBatchFields returned error: %v", err)
	}
	if len(got[7]) != 1 || len(got[8]) != 1 {
		t.Fatalf("expected one row per SID, got: %+v", got)
	}
	if value, ok := got[7][0].Values["quote"]; !ok || value != nil {
		t.Fatalf("batch SQL NULL must remain an explicit nil value: %#v, present=%v", value, ok)
	}
	if value, ok := got[7][0].Values["signal"]; !ok || value != "buy" {
		t.Fatalf("batch extension field was not preserved: %#v, present=%v", value, ok)
	}
	if value, ok := got[8][0].Values["open"]; !ok || value != nil {
		t.Fatalf("batch NULL OHLCV field must remain an explicit nil value: %#v, present=%v", value, ok)
	}
	if value, ok := got[8][0].Values["quote"]; !ok {
		t.Fatalf("batch non-NULL field key was dropped")
	} else if _, ok := value.(float32); !ok {
		t.Fatalf("batch extension value type changed: %T", value)
	}
	if value, ok := got[8][0].Values["signal"]; !ok {
		t.Fatalf("batch extension field key was dropped")
	} else if _, ok := value.(int64); !ok {
		t.Fatalf("batch extension type changed: %T", value)
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

func TestInsertOHLCVSeriesAutoPostgresRollbackKeepsRecoveryJob(t *testing.T) {
	testutil.RequireIntegration(t)
	initSeriesRepoTestApp(t, mustFindSeriesRepoConfig(t, "config.local.yml"))
	if IsQuestDB {
		t.Skip("postgres/timescale backend is not active")
	}

	ctx := context.Background()
	q, conn, err := Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	sid := int32(time.Now().UnixNano()%1_000_000 + 5_000_000)
	startMS := int64(1_700_000_040_000)
	exs := &ExSymbol{ID: sid, Symbol: "ROLLBACK/USDT"}
	row := NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
		Time: startMS, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 3,
	}, nil, false, true)
	defer func() {
		_, _ = q.db.Exec(ctx, `DELETE FROM kline_1m WHERE sid = $1`, sid)
		_, _ = q.db.Exec(ctx, `DELETE FROM sranges WHERE sid = $1 AND tbl = 'kline_1m' AND timeframe = '1m'`, sid)
		_ = q.delInsKlinePg(ctx, sid, "1m")
	}()

	tx, write, txErr := q.begin(ctx)
	if txErr != nil {
		t.Fatal(txErr)
	}
	if _, err := write.InsertOHLCVSeriesAuto("1m", exs, []*DataSeries{row}, false); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	var rowCount, rangeCount int
	if err := q.db.QueryRow(ctx, `SELECT count(*) FROM kline_1m WHERE sid = $1`, sid).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}
	if err := q.db.QueryRow(ctx, `SELECT count(*) FROM sranges WHERE sid = $1 AND tbl = 'kline_1m' AND timeframe = '1m'`, sid).Scan(&rangeCount); err != nil {
		t.Fatal(err)
	}
	job, jobErr := q.getInsKlinePg(ctx, sid, "1m")
	if jobErr != nil {
		t.Fatal(jobErr)
	}
	if rowCount != 0 || rangeCount != 0 || job == nil {
		t.Fatalf("rollback state rows=%d ranges=%d recovery_job=%+v", rowCount, rangeCount, job)
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

func TestSeriesAdjustmentRoutingUsesMarketBoundaryMetadata(t *testing.T) {
	const sid = int32(910001)
	const startMS = int64(1_700_000_000_000)
	underlying := &ExSymbol{ID: 12, Symbol: "AU2406"}
	cached := []*AdjInfo{{ExSymbol: underlying, StartMS: startMS, StopMS: startMS + 60_000}}
	amLock.Lock()
	previous, hadPrevious := adjMap[sid]
	adjMap[sid] = cached
	amLock.Unlock()
	t.Cleanup(func() {
		amLock.Lock()
		if hadPrevious {
			adjMap[sid] = previous
		} else {
			delete(adjMap, sid)
		}
		amLock.Unlock()
	})

	oldQuestDB := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuestDB })
	q := New(&visibilityDBStub{query: func(_ string, _ ...interface{}) (pgx.Rows, error) {
		return newInterfaceRows(nil), nil
	}})

	tests := []struct {
		name         string
		exchange     string
		market       *banexg.Market
		wantAdjusted bool
	}{
		{
			name:     "non China suffix is not enough",
			exchange: "binance",
			market:   &banexg.Market{Symbol: "AU888", Type: banexg.MarketLinear, Combined: false},
		},
		{
			name:         "China special market uses combined metadata",
			exchange:     "china",
			market:       &banexg.Market{Symbol: "AU888", Type: banexg.MarketLinear, Combined: true},
			wantAdjusted: true,
		},
		{
			name:         "non China combined market uses combined metadata",
			exchange:     "binance",
			market:       &banexg.Market{Symbol: "MAIN", Type: banexg.MarketLinear, Combined: true},
			wantAdjusted: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exs := &ExSymbol{
				ID: sid, Exchange: test.exchange, Market: test.market.Type,
				Symbol: test.market.Symbol, Combined: test.market.Combined,
			}
			adjs, _, err := q.getSeriesFieldsRawMode(exs, "1m", nil, startMS, startMS+60_000, 0, false, false)
			if err != nil {
				t.Fatalf("getSeriesFieldsRawMode returned error: %v", err)
			}
			gotAdjusted := len(adjs) > 0
			if gotAdjusted != test.wantAdjusted {
				t.Fatalf("adjustment routing = %v, want %v for market=%+v", gotAdjusted, test.wantAdjusted, test.market)
			}
		})
	}
}
