package data

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
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

func TestEnrichKlineFieldRowsPropagatesReaderFailures(t *testing.T) {
	exs := &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT"}
	row := &orm.DataSeries{
		TimeMS: 100, EndMS: 60_100,
		Values: map[string]any{"close": 3.0},
	}
	cases := []struct {
		name string
		err  *errs.Error
	}{
		{name: "connection", err: errs.NewMsg(core.ErrDbConnFail, "connection failed")},
		{name: "query", err: errs.NewMsg(core.ErrDbReadFail, "query failed")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			got, err := enrichKlineFieldRows(exs, "1m", []string{"close", "open_interest"}, []*orm.DataSeries{row},
				func(*orm.ExSymbol, string, []string, int64, int64) ([]*orm.DataSeries, *errs.Error) {
					called = true
					return nil, tc.err
				})
			if !called || err != tc.err || got != nil {
				t.Fatalf("reader failure = (called=%v, rows=%v, err=%v), want no rows and %v", called, got, err, tc.err)
			}
		})
	}
}

func TestEnrichKlineFieldRowsRejectsMissingTimestampRow(t *testing.T) {
	exs := &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT"}
	row := &orm.DataSeries{TimeMS: 100, EndMS: 60_100, Values: map[string]any{"close": 3.0}}
	got, err := enrichKlineFieldRows(exs, "1m", []string{"close", "open_interest"}, []*orm.DataSeries{row},
		func(*orm.ExSymbol, string, []string, int64, int64) ([]*orm.DataSeries, *errs.Error) {
			return []*orm.DataSeries{{TimeMS: 200, Values: map[string]any{"open_interest": 9.0}}}, nil
		})
	if got != nil || err == nil || err.Code != core.ErrDbReadFail || !strings.Contains(err.Short(), "time_ms=100") {
		t.Fatalf("missing timestamp result = (rows=%v, err=%v), want db read failure", got, err)
	}
}

func TestEnrichKlineFieldRowsPreservesExplicitNull(t *testing.T) {
	exs := &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT"}
	row := &orm.DataSeries{
		TimeMS: 100, EndMS: 60_100,
		Values: map[string]any{"close": 3.0, "open_interest": nil},
	}
	called := false
	got, err := enrichKlineFieldRows(exs, "1m", []string{"close", "open_interest"}, []*orm.DataSeries{row},
		func(*orm.ExSymbol, string, []string, int64, int64) ([]*orm.DataSeries, *errs.Error) {
			called = true
			return nil, nil
		})
	value, ok := got[0].Values["open_interest"]
	if err != nil || called || len(got) != 1 || got[0] != row || !ok || value != nil {
		t.Fatalf("explicit NULL enrichment = (rows=%v, err=%v, called=%v), want unchanged row with NULL", got, err, called)
	}
}

func TestMergeKlineFieldRowsPreservesExplicitNull(t *testing.T) {
	row := &orm.DataSeries{TimeMS: 100, Values: map[string]any{"open_interest": nil}}
	stored := &orm.DataSeries{TimeMS: 100, Values: map[string]any{"open_interest": 9.0}}
	got := mergeKlineFieldRows([]*orm.DataSeries{row}, map[int64]*orm.DataSeries{100: stored})
	value, ok := got[0].Values["open_interest"]
	if len(got) != 1 || got[0] == row || !ok || value != nil {
		t.Fatalf("merge overwrote explicit NULL: %v", got)
	}
}

func TestEnrichKlineFieldRowsMergesCompleteStoredRow(t *testing.T) {
	exs := &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT"}
	row := &orm.DataSeries{
		TimeMS: 100, EndMS: 60_100,
		Values: map[string]any{"close": 3.0},
	}
	got, err := enrichKlineFieldRows(exs, "1m", []string{"close", "open_interest"}, []*orm.DataSeries{row},
		func(gotExs *orm.ExSymbol, gotTF string, fields []string, startMS, endMS int64) ([]*orm.DataSeries, *errs.Error) {
			if gotExs != exs || gotTF != "1m" || startMS != 100 || endMS != 60_100 || len(fields) != 2 {
				t.Fatalf("reader args = (%v, %s, %v, %d, %d)", gotExs, gotTF, fields, startMS, endMS)
			}
			return []*orm.DataSeries{{TimeMS: 100, Values: map[string]any{"close": 2.0, "open_interest": 9.0}}}, nil
		})
	if err != nil || len(got) != 1 || got[0] == row || got[0].Values["close"] != 3.0 || got[0].Values["open_interest"] != 9.0 {
		t.Fatalf("complete stored merge = (rows=%v, err=%v), want cloned row with source close and stored extra", got, err)
	}
	if _, ok := row.Values["open_interest"]; ok {
		t.Fatalf("input row was mutated: %v", row.Values)
	}
}

func TestSeriesFeederBlocksCallbackOnKlineEnrichmentError(t *testing.T) {
	exs := &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT"}
	tf := "1m"
	key := strat.DataSubKey(orm.SeriesSourceKline, exs.ID, tf)
	oldJobs := strat.AccInfoJobs
	strat.AccInfoJobs = map[string]map[string]map[string]*strat.StratJob{
		"test": {
			key: {
				"job": {
					Strat: &strat.TradeStrat{OnDataSubs: func(*strat.StratJob) []*strat.DataSub {
						return []*strat.DataSub{{Source: orm.SeriesSourceKline, ExSymbol: exs, TimeFrame: tf, Fields: []string{"open_interest"}}}
					}},
				},
			},
		},
	}
	t.Cleanup(func() { strat.AccInfoJobs = oldJobs })

	called := 0
	feeder := &SeriesFeeder{Feeder: Feeder{
		ExSymbol: exs,
		States:   []*PairTFCache{{TimeFrame: tf, TFSecs: 60}},
		tfBars:   make(map[string][]*orm.DataSeries),
		CallBack: func(*orm.DataSeries) { called++ },
		readKlineFields: func(*orm.ExSymbol, string, []string, int64, int64) ([]*orm.DataSeries, *errs.Error) {
			return nil, errs.NewMsg(core.ErrDbReadFail, "extension query failed")
		},
	}}
	_, err := feeder.onNewData(60_000, []*orm.DataSeries{{
		TimeMS: 100, EndMS: 60_100, Values: map[string]any{"close": 3.0},
	}})
	if err == nil || err.Code != core.ErrDbReadFail || called != 0 {
		t.Fatalf("feeder enrichment result = (err=%v, callbacks=%d), want read failure and no callback", err, called)
	}
}

func TestBuildAggSeriesWithSymbolStateDoesNotUseLegacySIDCatalog(t *testing.T) {
	restore, err := orm.InstallFrozenExSymbols([]*orm.ExSymbol{{
		ID: 7, Exchange: "legacy", Market: "spot", Symbol: "LEGACY/USDT",
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()

	state := orm.NewSymbolStateWithIdentity("runtime", "spot")
	explicit := &orm.ExSymbol{ID: 7, Exchange: "runtime", Market: "spot", Symbol: "RUNTIME/USDT"}
	state.CacheExSymbol(explicit)
	rows := []*orm.DataSeries{{
		Sid: 7, TimeMS: 1_700_000_040_000, EndMS: 1_700_000_100_000,
		Source: orm.SeriesSourceKline, TimeFrame: "1m", Closed: true,
		Values: map[string]any{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 1.0},
	}}

	got, _, aggErr := buildAggSeriesWithSymbolState(state, nil, "1m", rows, 60_000, 0, nil, 60_000, 0, false)
	if aggErr != nil {
		t.Fatal(aggErr)
	}
	if len(got) != 1 || got[0].ExSymbol == nil || got[0].ExSymbol.ID != explicit.ID ||
		got[0].ExSymbol.Symbol != explicit.Symbol {
		t.Fatalf("aggregate symbol = %+v, want explicit runtime symbol %+v", got, explicit)
	}
}
