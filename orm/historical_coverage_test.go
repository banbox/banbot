package orm

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

func TestDerivedHistoricalCoverageConsumesOnlyAuditedPhysicalPrefix(t *testing.T) {
	const hour = int64(60 * 60 * 1000)
	const bucketStart = int64(1_699_977_600_000)
	symbol := "BTC/USDT:USDT"
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: bucketStart + 8*hour,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {
				"4h": {{StartMS: bucketStart, StopMS: bucketStart + 8*hour}},
				"1h": {{StartMS: bucketStart + hour, StopMS: bucketStart + 8*hour}},
			},
		},
	}
	startMS, stopMS, constrained, err := historicalPhysicalCoverageBounds(
		coverage, symbol, "4h", bucketStart, bucketStart+8*hour)
	if err != nil || !constrained || startMS != bucketStart+hour || stopMS != bucketStart+8*hour {
		t.Fatalf("physical bounds=%d:%d constrained=%v err=%v", startMS, stopMS, constrained, err)
	}
	exs := &ExSymbol{ID: 7, Symbol: symbol}
	raw := make([]*DataSeries, 0, 8)
	for index := int64(0); index < 8; index++ {
		raw = append(raw, NewDataSeriesFromKline(exs, "1h", &banexg.Kline{
			Time: bucketStart + index*hour, Open: float64(index + 16), High: float64(index + 16),
			Low: float64(index + 16), Close: float64(index + 16), Volume: 1,
		}, nil, false, true))
	}
	raw = slices.DeleteFunc(raw, func(row *DataSeries) bool {
		return row.TimeMS < startMS || row.TimeMS >= stopMS
	})
	rows, done, resampleErr := ResampleDataSeries(exs, "4h", raw, nil, 4*hour, 0, hour, 0, false)
	if resampleErr != nil || !done || len(rows) != 2 {
		t.Fatalf("resample rows=%v done=%v err=%v", seriesTimes(rows), done, resampleErr)
	}
	first, valueErr := rows[0].OHLCV(exs)
	if valueErr != nil || rows[0].TimeMS != bucketStart || first.Open != 17 || first.Volume != 3 {
		t.Fatalf("partial first bucket time=%d values=%+v err=%v", rows[0].TimeMS, first, valueErr)
	}
}

func TestDerivedHistoricalCoverageRejectsSegmentedPhysicalAuthority(t *testing.T) {
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: 1000,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			"BTC/USDT:USDT": {
				"4h": {{StartMS: 100, StopMS: 1000}},
				"1h": {{StartMS: 100, StopMS: 400}, {StartMS: 500, StopMS: 1000}},
			},
		},
	}
	_, _, constrained, err := historicalPhysicalCoverageBounds(
		coverage, "BTC/USDT:USDT", "4h", 100, 1000)
	if !constrained || err == nil || !strings.Contains(err.Error(), "one contiguous physical range") {
		t.Fatalf("constrained=%v err=%v", constrained, err)
	}
}

func TestHistoricalCoverageReverseReadBackfillsAllowedRows(t *testing.T) {
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: 1000,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			"BTC/USDT:USDT": {"1h": {{StartMS: 100, StopMS: 500}, {StartMS: 700, StopMS: 1000}}},
		},
	}
	physical := []*DataSeries{{TimeMS: 100}, {TimeMS: 200}, {TimeMS: 300}, {TimeMS: 400},
		{TimeMS: 500}, {TimeMS: 600}, {TimeMS: 700}, {TimeMS: 800}, {TimeMS: 900}}
	reads := 0
	_, rows, err := readHistoricalCoverageSeries(coverage, "BTC/USDT:USDT", "1h", 0, 1000, 5, false,
		func(_ int64, endMS int64, limit int, _ bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
			reads++
			eligible := make([]*DataSeries, 0)
			for _, row := range physical {
				if row.TimeMS < endMS {
					eligible = append(eligible, row)
				}
			}
			if len(eligible) > limit {
				eligible = eligible[len(eligible)-limit:]
			}
			return nil, eligible, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{300, 400, 700, 800, 900}
	if len(rows) != len(want) || reads != 2 {
		t.Fatalf("rows=%v reads=%d", seriesTimes(rows), reads)
	}
	for index := range want {
		if rows[index].TimeMS != want[index] {
			t.Fatalf("rows=%v want=%v", seriesTimes(rows), want)
		}
	}
}

func TestHistoricalCoverageForwardReadBackfillsAllowedRows(t *testing.T) {
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: 1000,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			"BTC/USDT:USDT": {"1h": {{StartMS: 100, StopMS: 500}, {StartMS: 700, StopMS: 1000}}},
		},
	}
	physical := []*DataSeries{{TimeMS: 100}, {TimeMS: 200}, {TimeMS: 300}, {TimeMS: 400},
		{TimeMS: 500}, {TimeMS: 600}, {TimeMS: 700}, {TimeMS: 800}, {TimeMS: 900}}
	reads := 0
	_, rows, err := readHistoricalCoverageSeries(coverage, "BTC/USDT:USDT", "1h", 100, 1000, 5, false,
		func(startMS, endMS int64, limit int, _ bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
			reads++
			eligible := make([]*DataSeries, 0)
			for _, row := range physical {
				if row.TimeMS >= startMS && row.TimeMS < endMS {
					eligible = append(eligible, row)
				}
			}
			if len(eligible) > limit {
				eligible = eligible[:limit]
			}
			return nil, eligible, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{100, 200, 300, 400, 700}
	if got := seriesTimes(rows); !slices.Equal(got, want) || reads != 2 {
		t.Fatalf("rows=%v reads=%d want=%v/2", got, reads, want)
	}
}

func TestAutoFetchSeriesStrictReadsLocalWithOriginalArgs(t *testing.T) {
	previousMode, previousData := core.BackTestMode, config.Data
	core.BackTestMode = true
	config.Data = config.Config{BTNoKlineDownload: true}
	t.Cleanup(func() { core.BackTestMode, config.Data = previousMode, previousData })

	downloads, reads := 0, 0
	_, rows, err := autoFetchSeries("1h", 0, 12345, 7, true, nil,
		func(string, int64, int64) *errs.Error {
			downloads++
			return nil
		}, func(startMS, endMS int64, limit int, withUnFinish bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
			reads++
			if startMS != 0 || endMS != 12345 || limit != 7 || !withUnFinish {
				t.Fatalf("read args=(%d,%d,%d,%v)", startMS, endMS, limit, withUnFinish)
			}
			return nil, []*DataSeries{{TimeMS: 42}}, nil
		})
	if err != nil || downloads != 0 || reads != 1 || len(rows) != 1 || rows[0].TimeMS != 42 {
		t.Fatalf("err=%v downloads=%d reads=%d rows=%v", err, downloads, reads, seriesTimes(rows))
	}
}

func TestAutoFetchSeriesDownloadPreservesLegacyReadWindow(t *testing.T) {
	previousMode := core.BackTestMode
	core.BackTestMode = false
	t.Cleanup(func() { core.BackTestMode = previousMode })

	downloads, reads := 0, 0
	_, _, err := autoFetchSeries("1h", 1_699_999_200_001, 1_700_006_400_001, 7, true, nil,
		func(timeframe string, startMS, endMS int64) *errs.Error {
			downloads++
			if timeframe != "1h" || startMS != 1_700_002_800_000 || endMS != 1_700_010_000_000 {
				t.Fatalf("download args=(%s,%d,%d)", timeframe, startMS, endMS)
			}
			return nil
		}, func(startMS, endMS int64, limit int, withUnFinish bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
			reads++
			if startMS != 1_700_002_800_000 || endMS != 1_700_010_000_000 || limit != 7 || !withUnFinish {
				t.Fatalf("read args=(%d,%d,%d,%v)", startMS, endMS, limit, withUnFinish)
			}
			return nil, nil, nil
		})
	if err != nil || downloads != 1 || reads != 1 {
		t.Fatalf("err=%v downloads=%d reads=%d", err, downloads, reads)
	}
}

func TestAutoFetchSeriesBacktestWithoutStrictGateDownloads(t *testing.T) {
	previousMode, previousData := core.BackTestMode, config.Data
	core.BackTestMode = true
	config.Data = config.Config{BTNoKlineDownload: false}
	t.Cleanup(func() { core.BackTestMode, config.Data = previousMode, previousData })

	downloads, reads := 0, 0
	_, _, err := autoFetchSeries("1h", 1_699_999_200_001, 1_700_006_400_001, 7, false, nil,
		func(string, int64, int64) *errs.Error {
			downloads++
			return nil
		}, func(int64, int64, int, bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
			reads++
			return nil, nil, nil
		})
	if err != nil || downloads != 1 || reads != 1 {
		t.Fatalf("err=%v downloads=%d reads=%d", err, downloads, reads)
	}
}

func TestHistoricalCoverageIntervalsRejectUnknownTail(t *testing.T) {
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: 1000,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			"BTC/USDT:USDT": {"1h": {{StartMS: 100, StopMS: 1000}}},
		},
	}
	if got := historicalCoverageIntervals(coverage, "BTC/USDT:USDT", "5m", 0, 2000); len(got) != 0 {
		t.Fatalf("unknown timeframe tail intervals=%v", got)
	}
	if got := historicalCoverageIntervals(coverage, "NEW/USDT:USDT", "1h", 0, 2000); len(got) != 0 {
		t.Fatalf("unknown symbol tail intervals=%v", got)
	}
	got := historicalCoverageIntervals(coverage, "BTC/USDT:USDT", "1h", 0, 2000)
	want := []historicalCoverageInterval{{StartMS: 100, StopMS: 2000}}
	if !slices.Equal(got, want) {
		t.Fatalf("known tail intervals=%v want=%v", got, want)
	}
}

func TestHistoricalCoverageIntervalsCapExtensionAtBacktestEnd(t *testing.T) {
	previous := config.TimeRange
	config.TimeRange = &config.TimeTuple{StartMS: 100, EndMS: 1500}
	t.Cleanup(func() { config.TimeRange = previous })
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: 1000,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			"BTC/USDT:USDT": {"1h": {{StartMS: 100, StopMS: 1000}}},
		},
	}
	want := []historicalCoverageInterval{{StartMS: 100, StopMS: 1500}}
	if got := historicalCoverageIntervals(coverage, "BTC/USDT:USDT", "1h", 0, 2000); !slices.Equal(got, want) {
		t.Fatalf("capped intervals=%v want=%v", got, want)
	}
	if got := historicalCoverageIntervals(coverage, "BTC/USDT:USDT", "1h", 1500, 2000); len(got) != 0 {
		t.Fatalf("future-only intervals=%v", got)
	}
}

func TestExportedSeriesReadsFailClosedBeforeRawQuery(t *testing.T) {
	previousMode, previousCoverage := core.BackTestMode, config.HistoricalCoverage
	core.BackTestMode = true
	config.HistoricalCoverage = &config.HistoricalCoverageConfig{
		BaselineEndMS: 1000,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			"BTC/USDT:USDT": {"1h": {{StartMS: 100, StopMS: 500}}},
		},
	}
	t.Cleanup(func() { core.BackTestMode, config.HistoricalCoverage = previousMode, previousCoverage })

	q := &Queries{}
	unknown := &ExSymbol{ID: 2, Symbol: "NEW/USDT:USDT"}
	known := &ExSymbol{ID: 1, Symbol: "BTC/USDT:USDT"}
	assertEmpty := func(name string, rows []*DataSeries, err *errs.Error) {
		t.Helper()
		if err != nil || len(rows) != 0 {
			t.Fatalf("%s rows=%v err=%v", name, seriesTimes(rows), err)
		}
	}

	rows, err := q.QuerySeries(unknown, "1h", 1000, 2000, 0, false)
	assertEmpty("QuerySeries unknown tail", rows, err)
	rows, err = q.QuerySeriesFields(known, "1h", []string{"signal"}, 100, 500, 0, false)
	if err == nil || !strings.Contains(err.Error(), "physical field proof") || len(rows) != 0 {
		t.Fatalf("custom field rows=%v err=%v", rows, err)
	}
	rows, err = q.QuerySeriesFields(known, "5m", nil, 1000, 2000, 0, false)
	assertEmpty("QuerySeriesFields unknown timeframe tail", rows, err)
	rows, err = q.QuerySeriesFields(known, "1h", nil, 500, 900, 0, false)
	assertEmpty("QuerySeriesFields baseline gap", rows, err)

	_, rows, err = q.GetSeriesFields(unknown, "1h", nil, 1000, 2000, 0, false)
	assertEmpty("GetSeriesFields unknown tail", rows, err)
	_, rows, err = q.GetSeries(unknown, "1h", 1000, 2000, 0, false)
	assertEmpty("GetSeries unknown tail", rows, err)
	adj := &AdjInfo{ExSymbol: unknown, StartMS: 0, StopMS: 2000}
	rows, err = q.GetAdjSeriesFields([]*AdjInfo{adj}, "1h", nil, 1000, 2000, 0, false)
	assertEmpty("GetAdjSeriesFields unknown tail", rows, err)
	rows, err = q.GetAdjSeries([]*AdjInfo{adj}, "1h", 1000, 2000, 0, false)
	assertEmpty("GetAdjSeries unknown tail", rows, err)

	batchCalls := 0
	err = q.QuerySeriesBatch(map[int32]*ExSymbol{unknown.ID: unknown}, "1h", 1000, 2000, 0,
		func(_ int32, rows []*DataSeries) {
			batchCalls++
			if len(rows) != 0 {
				t.Fatalf("QuerySeriesBatch returned rows=%v", seriesTimes(rows))
			}
		})
	if err != nil || batchCalls != 1 {
		t.Fatalf("QuerySeriesBatch err=%v calls=%d", err, batchCalls)
	}
	batchCalls = 0
	err = q.QuerySeriesBatchFields(map[int32]*ExSymbol{unknown.ID: unknown}, "1h", []string{"close"},
		1000, 2000, 0, func(_ int32, rows []*DataSeries) {
			batchCalls++
			if len(rows) != 0 {
				t.Fatalf("QuerySeriesBatchFields returned rows=%v", seriesTimes(rows))
			}
		})
	if err != nil || batchCalls != 1 {
		t.Fatalf("QuerySeriesBatchFields err=%v calls=%d", err, batchCalls)
	}
	order := make([]int32, 0, 3)
	err = q.QuerySeriesBatchFields(map[int32]*ExSymbol{
		3: {ID: 3, Symbol: "C/USDT:USDT"},
		1: {ID: 1, Symbol: "A/USDT:USDT"},
		2: {ID: 2, Symbol: "B/USDT:USDT"},
	}, "1h", nil, 1000, 2000, 0, func(sid int32, _ []*DataSeries) {
		order = append(order, sid)
	})
	if err != nil || !slices.Equal(order, []int32{1, 2, 3}) {
		t.Fatalf("coverage batch order=%v err=%v", order, err)
	}
}

func TestHistoricalCoverageIsolatedBySymbol(t *testing.T) {
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: 1000,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			"BTC/USDT:USDT": {"1h": {{StartMS: 100, StopMS: 500}}},
			"ETH/USDT:USDT": {"1h": {{StartMS: 500, StopMS: 900}}},
		},
	}
	previous := config.HistoricalCoverage
	config.HistoricalCoverage = coverage
	t.Cleanup(func() { config.HistoricalCoverage = previous })
	btc := config.HistoricalCoverageFor("BTC/USDT:USDT")
	if !btc.Allows("1h", 200) || btc.Allows("1h", 700) {
		t.Fatal("BTC coverage used another symbol's ranges")
	}
}

func TestFastBulkHistoricalCoverageAndNoDownloadGate(t *testing.T) {
	previousMode, previousData, previousCoverage := core.BackTestMode, config.Data, config.HistoricalCoverage
	core.BackTestMode = true
	config.Data = config.Config{BTNoKlineDownload: true}
	config.HistoricalCoverage = &config.HistoricalCoverageConfig{
		BaselineEndMS: 1000,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			"BTC/USDT:USDT": {"1h": {{StartMS: 100, StopMS: 500}}},
		},
	}
	t.Cleanup(func() {
		core.BackTestMode, config.Data, config.HistoricalCoverage = previousMode, previousData, previousCoverage
	})
	if allowImplicitKlineDownload() {
		t.Fatal("strict historical backtest allowed FastBulk download")
	}
	if _, err := (&Queries{}).DownOHLCV2DB(nil, nil, "1m", 100, 200, nil); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "download is disabled") {
		t.Fatalf("low-level implicit download error=%v", err)
	}
	rows := filterHistoricalCoverageKlines("BTC/USDT:USDT", "1h", []*banexg.Kline{{Time: 200}, {Time: 700}})
	if len(rows) != 1 || rows[0].Time != 200 {
		t.Fatalf("filtered K-lines=%v", rows)
	}
	core.BackTestMode = false
	if !allowImplicitKlineDownload() || historicalCoverageForQuery("BTC/USDT:USDT") != nil ||
		len(filterHistoricalCoverageKlines("BTC/USDT:USDT", "1h",
			[]*banexg.Kline{{Time: 200}, {Time: 700}})) != 2 {
		t.Fatal("live/raw K-line reads were constrained by historical coverage")
	}
}

func TestStrictBacktestRejectsEveryPublicKlineDownloadEntry(t *testing.T) {
	previousMode, previousData := core.BackTestMode, config.Data
	core.BackTestMode = true
	config.Data = config.Config{BTNoKlineDownload: true}
	t.Cleanup(func() { core.BackTestMode, config.Data = previousMode, previousData })

	tests := []struct {
		name string
		run  func() *errs.Error
	}{
		{"FetchApiOHLCV", func() *errs.Error {
			return FetchApiOHLCV(context.Background(), nil, "BTC/USDT:USDT", "1m", 1, 2, nil)
		}},
		{"BulkDownOHLCV", func() *errs.Error {
			return BulkDownOHLCV(nil, nil, "1m", 1, 2, 0, nil)
		}},
		{"DownOHLCV2DB", func() *errs.Error {
			_, err := (&Queries{}).DownOHLCV2DB(nil, nil, "1m", 1, 2, nil)
			return err
		}},
		{"FastBulkOHLCV", func() *errs.Error {
			return FastBulkOHLCV(nil, nil, "1m", 1, 2, 0, nil)
		}},
		{"EnsureListDates", func() *errs.Error {
			return EnsureListDates(nil, nil, nil, nil)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.run()
			if err == nil || !strings.Contains(err.Error(), test.name) ||
				!strings.Contains(strings.ToLower(err.Error()), "download is disabled") {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestNonBacktestFetchRetainsNetDisableSemantics(t *testing.T) {
	previousMode, previousData, previousNetDisable := core.BackTestMode, config.Data, core.NetDisable
	core.BackTestMode = false
	config.Data = config.Config{BTNoKlineDownload: true}
	core.NetDisable = true
	t.Cleanup(func() {
		core.BackTestMode, config.Data, core.NetDisable = previousMode, previousData, previousNetDisable
	})
	if err := FetchApiOHLCV(context.Background(), nil, "BTC/USDT:USDT", "1m", 1, 2, nil); err != nil {
		t.Fatalf("non-backtest fetch was rejected: %v", err)
	}
}

func TestFastBulkHistoricalCoverageUsesFixedWindow(t *testing.T) {
	previousMode, previousCoverage := core.BackTestMode, config.HistoricalCoverage
	core.BackTestMode = true
	config.HistoricalCoverage = &config.HistoricalCoverageConfig{
		BaselineEndMS: 1000,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			"BTC/USDT:USDT": {"1h": {{StartMS: 100, StopMS: 500}, {StartMS: 700, StopMS: 1000}}},
		},
	}
	t.Cleanup(func() { core.BackTestMode, config.HistoricalCoverage = previousMode, previousCoverage })

	adj := &AdjInfo{}
	var got []*banexg.Kline
	deliverFastBulkOHLCV(func(_ string, _ string, rows []*banexg.Kline, adjs []*AdjInfo) {
		got = rows
		if len(adjs) != 1 || adjs[0] != adj {
			t.Fatalf("adjustments changed: %v", adjs)
		}
	}, "BTC/USDT:USDT", "1h", []*banexg.Kline{
		{Time: 500}, {Time: 600}, {Time: 700}, {Time: 800}, {Time: 900},
	}, []*AdjInfo{adj})
	if times := klineTimes(got); !slices.Equal(times, []int64{700, 800, 900}) {
		t.Fatalf("fixed-window rows=%v; FastBulk must not backfill from earlier coverage ranges", times)
	}
}

func seriesTimes(rows []*DataSeries) []int64 {
	times := make([]int64, len(rows))
	for index, row := range rows {
		times[index] = row.TimeMS
	}
	return times
}

func klineTimes(rows []*banexg.Kline) []int64 {
	times := make([]int64, len(rows))
	for index, row := range rows {
		times[index] = row.Time
	}
	return times
}
