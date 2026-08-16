package orm

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

func enableStrictHistoricalCoverageTest(t *testing.T) {
	t.Helper()
	previousMode, previousData := core.BackTestMode, config.Data
	t.Cleanup(func() { core.BackTestMode, config.Data = previousMode, previousData })
	core.BackTestMode = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true
}

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

func TestLegacyHistoricalCoverageRestoresPhysicallyProvedListingBucket(t *testing.T) {
	const hour = int64(60 * 60 * 1000)
	symbol := "WLD/USDT:USDT"
	exs := &ExSymbol{ID: 7, Exchange: "binance", Symbol: symbol, ListMs: 4 * hour}
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: 24 * hour,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {
				"8h": {{StartMS: 4 * hour, StopMS: 24 * hour}},
				"1h": {{StartMS: 4 * hour, StopMS: 24 * hour}},
				"1m": {{StartMS: 4 * hour, StopMS: 24 * hour}},
			},
		},
	}
	enableStrictHistoricalCoverageTest(t)

	base := historicalCoverageIntervals(coverage, symbol, "8h", 0, 24*hour)
	got := extendLegacyListingCoverage(coverage, exs, "8h", 0, base)
	if len(got) != 1 || got[0] != (historicalCoverageInterval{StartMS: 0, StopMS: 24 * hour}) {
		t.Fatalf("legacy intervals=%v", got)
	}
	if !HistoricalCoverageAllows(coverage, exs, "8h", 0) {
		t.Fatal("physically proved listing bucket was rejected by the runtime coverage filter")
	}
	physicalStart, physicalStop, constrained, err := historicalPhysicalCoverageBounds(
		coverage, symbol, "8h", got[0].StartMS, got[0].StopMS)
	if err != nil || !constrained || physicalStart != exs.ListMs || physicalStop != 24*hour {
		t.Fatalf("physical bounds=%d:%d constrained=%v err=%v", physicalStart, physicalStop, constrained, err)
	}
	for _, test := range []struct {
		name            string
		archivedStartMS int64
		wantStartMS     int64
	}{
		{name: "bucket label", archivedStartMS: 0, wantStartMS: 0},
		{name: "pre-list label", archivedStartMS: 3 * hour, wantStartMS: 0},
		{name: "exact list", archivedStartMS: 4 * hour, wantStartMS: 0},
		{name: "post-list label", archivedStartMS: 5 * hour, wantStartMS: 0},
		{name: "next full bucket", archivedStartMS: 8 * hour, wantStartMS: 0},
		{name: "outside first bucket", archivedStartMS: 9 * hour, wantStartMS: 9 * hour},
	} {
		t.Run(test.name, func(t *testing.T) {
			coverage.Bars[symbol]["8h"] = []config.HistoricalCoverageRange{{
				StartMS: test.archivedStartMS, StopMS: 24 * hour,
			}}
			input := historicalCoverageIntervals(coverage, symbol, "8h", 0, 24*hour)
			got := extendLegacyListingCoverage(coverage, exs, "8h", 0, input)
			if len(got) != 1 || got[0].StartMS != test.wantStartMS {
				t.Fatalf("intervals=%v want start=%d", got, test.wantStartMS)
			}
		})
	}

	for _, test := range []struct {
		name   string
		change func()
	}{
		{name: "non-backtest", change: func() { core.BackTestMode = false }},
		{name: "non-strict replay", change: func() { config.Data.BTStrict = false }},
		{name: "download-enabled replay", change: func() { config.Data.BTNoKlineDownload = false }},
		{name: "explicit later query", change: func() {}},
		{name: "missing runtime prefix", change: func() {
			coverage.Bars[symbol]["1h"] = []config.HistoricalCoverageRange{{StartMS: 5 * hour, StopMS: 24 * hour}}
			coverage.ListingPrefixes = map[string]map[string][]config.HistoricalCoverageRange{
				symbol: {"1m": {{StartMS: 4 * hour, StopMS: 24 * hour}}},
			}
		}},
		{name: "missing minute proof", change: func() {
			coverage.Bars[symbol]["1m"] = []config.HistoricalCoverageRange{{StartMS: 5 * hour, StopMS: 24 * hour}}
			coverage.ListingPrefixes = map[string]map[string][]config.HistoricalCoverageRange{
				symbol: {"1h": {{StartMS: 4 * hour, StopMS: 24 * hour}}},
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			core.BackTestMode = true
			config.Data.BTStrict = true
			config.Data.BTNoKlineDownload = true
			coverage.ListingPrefixes = nil
			coverage.Bars[symbol]["8h"] = []config.HistoricalCoverageRange{{StartMS: 4 * hour, StopMS: 24 * hour}}
			coverage.Bars[symbol]["1h"] = []config.HistoricalCoverageRange{{StartMS: 4 * hour, StopMS: 24 * hour}}
			coverage.Bars[symbol]["1m"] = []config.HistoricalCoverageRange{{StartMS: 4 * hour, StopMS: 24 * hour}}
			test.change()
			requestedStart := int64(0)
			requestedIntervals := base
			if test.name == "explicit later query" {
				requestedStart = 8 * hour
				requestedIntervals = historicalCoverageIntervals(coverage, symbol, "8h", requestedStart, 24*hour)
			}
			got := extendLegacyListingCoverage(coverage, exs, "8h", requestedStart, requestedIntervals)
			if !slices.Equal(got, requestedIntervals) {
				t.Fatalf("intervals=%v want unchanged %v", got, requestedIntervals)
			}
			if test.name != "explicit later query" && HistoricalCoverageAllows(coverage, exs, "8h", 0) {
				t.Fatal("unproved listing bucket was allowed by the runtime coverage filter")
			}
		})
	}
	if got := extendLegacyListingCoverage(nil, exs, "8h", 0, base); !slices.Equal(got, base) {
		t.Fatalf("nil coverage changed intervals: %v", got)
	}
}

func TestHistoricalListingPrefixRestoresExactDerivedOHLCV(t *testing.T) {
	const (
		minute = int64(60_000)
		hour   = 60 * minute
		base   = int64(1_699_977_600_000)
	)
	symbol := "WLD/USDT:USDT"
	listMS := base + 4*hour + 17*minute
	exs := &ExSymbol{ID: 7, Exchange: "binance", Symbol: symbol, ListMs: listMS}
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: base + 16*hour,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {
				"8h": {{StartMS: listMS, StopMS: base + 16*hour}},
				"1h": {{StartMS: base + 5*hour, StopMS: base + 16*hour}},
				"1m": {{StartMS: listMS, StopMS: base + 16*hour}},
			},
		},
	}
	enableStrictHistoricalCoverageTest(t)

	intervals := historicalCoverageIntervals(coverage, symbol, "8h", 0, base+16*hour)
	prefix, ok := legacyListingPrefixProof(coverage, exs, "8h", 0, intervals)
	if !ok {
		t.Fatal("listing prefix proof was rejected")
	}
	minuteRows := make([]*DataSeries, 0, 43)
	for timestamp := listMS; timestamp < base+5*hour; timestamp += minute {
		value := float64(timestamp-listMS)/float64(minute) + 10
		minuteRows = append(minuteRows, NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
			Time: timestamp, Open: value, High: value + 2, Low: value - 2, Close: value + 1,
			Volume: 1, Quote: 2, BuyVolume: 0.5, TradeNum: 1,
		}, nil, false, true))
	}
	physicalRows := []*DataSeries{
		NewDataSeriesFromKline(exs, "1h", &banexg.Kline{
			Time: base + 5*hour, Open: 100, High: 120, Low: 90, Close: 110,
			Volume: 2, Quote: 20, BuyVolume: 2, TradeNum: 2,
		}, nil, false, true),
		NewDataSeriesFromKline(exs, "1h", &banexg.Kline{
			Time: base + 6*hour, Open: 110, High: 130, Low: 80, Close: 120,
			Volume: 3, Quote: 30, BuyVolume: 3, TradeNum: 3,
		}, nil, false, true),
		NewDataSeriesFromKline(exs, "1h", &banexg.Kline{
			Time: base + 7*hour, Open: 120, High: 140, Low: 70, Close: 130,
			Volume: 4, Quote: 40, BuyVolume: 4, TradeNum: 4,
		}, nil, false, true),
	}
	storageRows, prefixErr := prependHistoricalListingPrefix(exs, prefix, minuteRows, physicalRows)
	if prefixErr != nil {
		t.Fatal(prefixErr)
	}
	rows, finished, resampleErr := ResampleDataSeries(exs, "8h", storageRows, nil, 8*hour, 0, hour, 0, false)
	if resampleErr != nil || !finished || len(rows) != 1 {
		t.Fatalf("derived rows=%v finished=%v err=%v", seriesTimes(rows), finished, resampleErr)
	}
	bar, valueErr := rows[0].OHLCV(exs)
	if valueErr != nil || rows[0].TimeMS != base || bar.Open != 10 || bar.High != 140 || bar.Low != 8 ||
		bar.Close != 130 || bar.Volume != 52 || bar.Quote != 176 || bar.BuyVolume != 30.5 || bar.TradeNum != 52 {
		t.Fatalf("restored listing bar time=%d values=%+v err=%v", rows[0].TimeMS, bar, valueErr)
	}
}

func TestHistoricalListingPrefixAcceptsExplicitNoTradeHead(t *testing.T) {
	const (
		minute = int64(60_000)
		hour   = 60 * minute
		base   = int64(1_699_977_600_000)
	)
	symbol := "NO-TRADE-HEAD/USDT:USDT"
	listMS := base + 4*hour + 17*minute
	firstMinuteMS := base + 5*hour
	storagePrefixMS := base + 6*hour
	fullStart := base + 8*hour
	exs := &ExSymbol{ID: 7, Exchange: "binance", Symbol: symbol, ListMs: listMS}
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: base + 16*hour,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {
				"8h": {{StartMS: fullStart, StopMS: base + 16*hour}},
			},
		},
		ListingPrefixes: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {
				"1m": {{StartMS: firstMinuteMS, StopMS: fullStart}},
				"1h": {{StartMS: storagePrefixMS, StopMS: fullStart}},
			},
		},
	}
	enableStrictHistoricalCoverageTest(t)

	intervals := historicalCoverageIntervals(coverage, symbol, "8h", 0, base+16*hour)
	prefix, ok := legacyListingPrefixProof(coverage, exs, "8h", 0, intervals)
	if !ok || prefix.minuteStartMS != firstMinuteMS || prefix.storageStartMS != storagePrefixMS {
		t.Fatalf("no-trade-head prefix=%+v ok=%v", prefix, ok)
	}
	minuteRows := make([]*DataSeries, 0, 60)
	for timestamp := firstMinuteMS; timestamp < storagePrefixMS; timestamp += minute {
		minuteRows = append(minuteRows, NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
			Time: timestamp, Open: 10, High: 12, Low: 8, Close: 11, Volume: 1,
		}, nil, false, true))
	}
	rows, err := prependHistoricalListingPrefix(exs, prefix, minuteRows, nil)
	if err != nil || len(rows) != 1 || rows[0].TimeMS != firstMinuteMS {
		t.Fatalf("no-trade-head rows=%v err=%v", seriesTimes(rows), err)
	}
}

func TestHistoricalListingPrefixRejectsUnalignedOrSplitEvidence(t *testing.T) {
	const (
		minute = int64(60_000)
		hour   = 60 * minute
		base   = int64(1_699_977_600_000)
	)
	symbol := "INVALID-LISTING-PREFIX/USDT:USDT"
	listMS := base + 4*hour + 17*minute
	fullStart := base + 8*hour
	exs := &ExSymbol{ID: 7, Exchange: "binance", Symbol: symbol, ListMs: listMS}
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: base + 16*hour,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {"8h": {{StartMS: fullStart, StopMS: base + 16*hour}}},
		},
		ListingPrefixes: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {"1m": {{StartMS: listMS - minute, StopMS: fullStart}}},
		},
	}
	coverage.ListingPrefixes[symbol]["1h"] = []config.HistoricalCoverageRange{{
		StartMS: base + 5*hour, StopMS: fullStart,
	}}
	enableStrictHistoricalCoverageTest(t)
	intervals := historicalCoverageIntervals(coverage, symbol, "8h", 0, base+16*hour)
	if prefix, ok := legacyListingPrefixProof(coverage, exs, "8h", 0, intervals); ok {
		t.Fatalf("listing prefix starting before aligned listing time was accepted: %+v", prefix)
	}

	coverage.ListingPrefixes[symbol]["1m"] = []config.HistoricalCoverageRange{
		{StartMS: base + 5*hour, StopMS: base + 6*hour},
		{StartMS: base + 6*hour, StopMS: fullStart},
	}
	if prefix, ok := legacyListingPrefixProof(coverage, exs, "8h", 0, intervals); ok {
		t.Fatalf("split listing prefix was accepted: %+v", prefix)
	}
}

func TestHistoricalListingPrefixRestoresPhysicalListingBucket(t *testing.T) {
	const (
		minute = int64(60_000)
		hour   = 60 * minute
		base   = int64(1_710_576_000_000)
	)
	symbol := "BOME/USDT:USDT"
	listMS := base + 4*hour + 30*minute
	exs := &ExSymbol{ID: 1939, Exchange: "binance", Symbol: symbol, ListMs: listMS}
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: base + 16*hour,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {
				"1h": {{StartMS: base + 5*hour, StopMS: base + 16*hour}},
				"1m": {{StartMS: listMS, StopMS: base + 16*hour}},
			},
		},
	}
	enableStrictHistoricalCoverageTest(t)

	intervals := historicalCoverageIntervals(coverage, symbol, "1h", 0, base+16*hour)
	prefix, ok := legacyListingPrefixProof(coverage, exs, "1h", 0, intervals)
	if !ok || prefix.bucketStartMS != base+4*hour || prefix.storageStartMS != base+5*hour {
		t.Fatalf("physical listing prefix=%+v ok=%v", prefix, ok)
	}
	extended := extendLegacyListingCoverage(coverage, exs, "1h", 0, intervals)
	if len(extended) != 1 || extended[0].StartMS != base+4*hour {
		t.Fatalf("physical listing interval=%v", extended)
	}

	minuteRows := make([]*DataSeries, 0, 30)
	for timestamp := listMS; timestamp < base+5*hour; timestamp += minute {
		value := float64(timestamp-listMS)/float64(minute) + 10
		minuteRows = append(minuteRows, NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
			Time: timestamp, Open: value, High: value + 2, Low: value - 2, Close: value + 1,
			Volume: 1, Quote: 2, BuyVolume: 0.5, TradeNum: 1,
		}, nil, false, true))
	}
	physicalRows := []*DataSeries{
		NewDataSeriesFromKline(exs, "1h", &banexg.Kline{
			Time: base + 4*hour, Open: 999, High: 999, Low: 999, Close: 999, Volume: 999,
		}, nil, false, true),
		NewDataSeriesFromKline(exs, "1h", &banexg.Kline{
			Time: base + 5*hour, Open: 40, High: 42, Low: 38, Close: 41, Volume: 2,
		}, nil, false, true),
	}
	rows, prefixErr := prependHistoricalListingPrefix(exs, prefix, minuteRows, physicalRows)
	if prefixErr != nil || len(rows) != 2 || rows[0].TimeMS != base+4*hour || rows[1].TimeMS != base+5*hour {
		t.Fatalf("physical listing rows=%v err=%v", seriesTimes(rows), prefixErr)
	}
	bar, valueErr := rows[0].OHLCV(exs)
	if valueErr != nil || bar.Open != 10 || bar.High != 41 || bar.Low != 8 || bar.Close != 40 ||
		bar.Volume != 30 || bar.Quote != 60 || bar.BuyVolume != 15 || bar.TradeNum != 30 {
		t.Fatalf("physical listing bar=%+v err=%v", bar, valueErr)
	}
}

func TestHistoricalListingPrefixDoesNotFallbackWhenEvidenceDomainIsPresent(t *testing.T) {
	const hour = int64(3_600_000)
	symbol := "CFX/USDT:USDT"
	exs := &ExSymbol{Exchange: "binance", Symbol: symbol, ListMs: 4*hour + 30*60_000}
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: 16 * hour,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {
				"1h": {{StartMS: 5 * hour, StopMS: 16 * hour}},
				"1m": {{StartMS: exs.ListMs, StopMS: 5 * hour}},
			},
		},
		ListingPrefixes: map[string]map[string][]config.HistoricalCoverageRange{},
	}
	enableStrictHistoricalCoverageTest(t)

	intervals := historicalCoverageIntervals(coverage, symbol, "1h", 0, 16*hour)
	if _, ok := legacyListingPrefixProof(coverage, exs, "1h", 0, intervals); ok {
		t.Fatal("new coverage format reused ordinary bars as missing listing-prefix evidence")
	}
	coverage.ListingPrefixes[symbol] = map[string][]config.HistoricalCoverageRange{
		"1m": {{StartMS: exs.ListMs, StopMS: 5 * hour}},
	}
	if _, ok := legacyListingPrefixProof(coverage, exs, "1h", 0, intervals); !ok {
		t.Fatal("explicit listing-prefix evidence was rejected")
	}
}

func TestHistoricalCoverageForPreservesLegacyListingPrefixFallback(t *testing.T) {
	const hour = int64(3_600_000)
	symbol := "CFX/USDT:USDT"
	previous := config.HistoricalCoverage
	config.HistoricalCoverage = &config.HistoricalCoverageConfig{
		BaselineEndMS: 16 * hour,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {
				"1h": {{StartMS: 5 * hour, StopMS: 16 * hour}},
				"1m": {{StartMS: 4*hour + 30*60_000, StopMS: 5 * hour}},
			},
		},
	}
	t.Cleanup(func() { config.HistoricalCoverage = previous })
	enableStrictHistoricalCoverageTest(t)

	coverage := historicalCoverageForQuery(symbol)
	if coverage.ListingPrefixes != nil {
		t.Fatal("legacy listing-prefix fallback was converted to a non-nil evidence domain")
	}
	exs := &ExSymbol{Exchange: "binance", Symbol: symbol, ListMs: 4*hour + 30*60_000}
	intervals := historicalCoverageIntervals(coverage, symbol, "1h", 0, 16*hour)
	if _, ok := legacyListingPrefixProof(coverage, exs, "1h", 0, intervals); !ok {
		t.Fatal("legacy runtime query lost listing-prefix fallback")
	}
}

func TestListingPrefixEvidenceDoesNotAuthorizePhysicalExtensionTail(t *testing.T) {
	const minute = int64(60_000)
	symbol := "TEST/USDT:USDT"
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: 60 * minute,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {"10m": {{StartMS: 0, StopMS: 60 * minute}}},
		},
		ListingPrefixes: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {"5m": {{StartMS: 50 * minute, StopMS: 60 * minute}}},
		},
	}
	_, _, constrained, err := historicalPhysicalCoverageBounds(
		coverage, symbol, "10m", 60*minute, 70*minute)
	if !constrained || err == nil {
		t.Fatalf("prefix-only storage evidence authorized extension tail: constrained=%v err=%v", constrained, err)
	}
}

func TestDerivedHistoricalCoverageExtendsAuditedPhysicalTail(t *testing.T) {
	const baseline = int64(1_000)
	symbol := "TEST/USDT:USDT"
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: baseline,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {"10m": {{StartMS: 100, StopMS: baseline}}},
		},
		PhysicalBars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {"5m": {{StartMS: 100, StopMS: baseline}}},
		},
	}
	start, stop, constrained, err := historicalPhysicalCoverageBounds(coverage, symbol, "10m", 900, 1_500)
	if err != nil || !constrained || start != 900 || stop != 1_500 {
		t.Fatalf("derived extension bounds=%d:%d constrained=%v err=%v", start, stop, constrained, err)
	}
	if len(historicalCoverageIntervals(coverage, symbol, "5m", 0, 1_500)) != 0 || coverage.Allows("5m", 1_200) {
		t.Fatal("physical evidence authorized direct access to the derived storage timeframe")
	}
}

func TestDerivedHistoricalCoverageExtendsConsumerTailAcrossBaseline(t *testing.T) {
	const baseline = int64(100)
	symbol := "TEST/USDT:USDT"
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: baseline,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {"10m": {{StartMS: 0, StopMS: 200}}},
		},
		PhysicalBars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {"5m": {{StartMS: 0, StopMS: baseline}}},
		},
	}
	start, stop, constrained, err := historicalPhysicalCoverageBounds(
		coverage, symbol, "10m", 0, 200)
	if err != nil || !constrained || start != 0 || stop != 200 {
		t.Fatalf("derived consumer tail bounds=%d:%d constrained=%v err=%v", start, stop, constrained, err)
	}
}

func TestLegacyDerivedHistoricalCoverageExtendsConsumerTailAcrossBaseline(t *testing.T) {
	const baseline = int64(100)
	symbol := "TEST/USDT:USDT"
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: baseline,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {
				"10m": {{StartMS: 0, StopMS: baseline}},
				"5m":  {{StartMS: 0, StopMS: baseline}},
			},
		},
	}

	intervals := historicalPhysicalCoverageIntervals(coverage, symbol, "5m", "10m", 0, 200)
	if len(intervals) != 2 || intervals[0] != (historicalCoverageInterval{StartMS: 0, StopMS: baseline}) ||
		intervals[1] != (historicalCoverageInterval{StartMS: baseline, StopMS: 200}) {
		t.Fatalf("legacy derived coverage intervals=%v", intervals)
	}
	if direct := historicalPhysicalCoverageIntervals(coverage, symbol, "5m", "5m", 0, 200); len(direct) != 1 || direct[0] != (historicalCoverageInterval{StartMS: 0, StopMS: baseline}) {
		t.Fatalf("direct storage coverage crossed the baseline: %v", direct)
	}
}

func TestDirectStorageCoverageRejectsConsumerTailExtension(t *testing.T) {
	const baseline = int64(100)
	symbol := "TEST/USDT:USDT"
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: baseline,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {"10m": {{StartMS: 0, StopMS: 200}}},
		},
		PhysicalBars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {"5m": {{StartMS: 0, StopMS: baseline}}},
		},
	}
	intervals := historicalPhysicalCoverageIntervals(coverage, symbol, "5m", "5m", 0, 200)
	if len(intervals) != 1 || intervals[0] != (historicalCoverageInterval{StartMS: 0, StopMS: baseline}) {
		t.Fatalf("direct storage coverage crossed baseline: intervals=%v", intervals)
	}
}

func TestPhysicalLoaderReadsProvedCanonicalStorageWithoutExpandingBars(t *testing.T) {
	const hour = int64(3_600_000)
	symbol := "BTC/USDT:USDT"
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: 8 * hour,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {"4h": {{StartMS: 0, StopMS: 8 * hour}}},
		},
		PhysicalBars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {"1h": {{StartMS: hour, StopMS: 8 * hour}}},
		},
	}
	if got := historicalCoverageIntervals(coverage, symbol, "1h", 0, 8*hour); len(got) != 0 {
		t.Fatalf("ordinary 1h read was expanded by physical evidence: %v", got)
	}
	intervals := historicalPhysicalCoverageIntervals(coverage, symbol, "1h", "1h", 0, 8*hour)
	if len(intervals) != 1 || intervals[0] != (historicalCoverageInterval{StartMS: hour, StopMS: 8 * hour}) {
		t.Fatalf("physical loader intervals=%v", intervals)
	}
	_, rows, err := readHistoricalCoverageIntervals(coverage, &ExSymbol{Symbol: symbol}, "1h", 0, 8*hour,
		3, false, intervals, func(startMS, endMS int64, limit int, _ bool, reverse bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
			if startMS != hour || endMS != 8*hour || limit != 3 || !reverse {
				t.Fatalf("physical loader read=%d:%d limit=%d reverse=%v", startMS, endMS, limit, reverse)
			}
			return nil, []*DataSeries{{TimeMS: 5 * hour}, {TimeMS: 6 * hour}, {TimeMS: 7 * hour}}, nil
		})
	if err != nil || !slices.Equal(seriesTimes(rows), []int64{5 * hour, 6 * hour, 7 * hour}) {
		t.Fatalf("physical loader rows=%v err=%v", seriesTimes(rows), err)
	}
}

func TestHistoricalPhysicalCoverageNilIsSafe(t *testing.T) {
	if got := historicalPhysicalCoverageIntervals(nil, "TEST/USDT:USDT", "1h", "1h", 0, 100); got != nil {
		t.Fatalf("nil coverage intervals=%v, want nil", got)
	}
}

func TestDerivedHistoricalCoverageRejectsMissingCompleteFirstBucket(t *testing.T) {
	const hour = int64(3_600_000)
	symbol := "TEST/USDT:USDT"
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: 8 * hour,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {
				"4h": {{StartMS: 0, StopMS: 8 * hour}},
				"1h": {{StartMS: 4 * hour, StopMS: 8 * hour}},
			},
		},
		ListingPrefixes: map[string]map[string][]config.HistoricalCoverageRange{},
	}
	_, _, constrained, err := historicalPhysicalCoverageBounds(coverage, symbol, "4h", 0, 8*hour)
	if !constrained || err == nil {
		t.Fatalf("ordinary derived coverage accepted a missing first bucket: constrained=%v err=%v", constrained, err)
	}
}

func TestHistoricalListingPrefixRequiresArchivedPhysicalBucket(t *testing.T) {
	prefix := historicalListingPrefix{
		bucketStartMS:  100,
		storageStartMS: 200,
		storageTF:      "1h",
	}
	if !shouldRestoreHistoricalListingPrefix("1h", "", prefix, []*DataSeries{{TimeMS: 100}}) {
		t.Fatal("archived physical listing bucket was not restored")
	}
	if shouldRestoreHistoricalListingPrefix("1h", "", prefix, []*DataSeries{{TimeMS: 200}}) {
		t.Fatal("missing physical listing bucket was synthesized from minute coverage")
	}
	if !shouldRestoreHistoricalListingPrefix("8h", "1h", prefix, []*DataSeries{{TimeMS: 200}}) {
		t.Fatal("derived listing bucket was not restored before its first complete storage row")
	}
}

func TestHistoricalListingPrefixRejectsPhysicalMinuteGap(t *testing.T) {
	const (
		minute = int64(60_000)
		hour   = 60 * minute
		base   = int64(1_699_977_600_000)
	)
	exs := &ExSymbol{ID: 7, Exchange: "binance", Symbol: "WLD/USDT:USDT", ListMs: base + 4*hour + 58*minute}
	prefix := historicalListingPrefix{
		minuteStartMS:  exs.ListMs,
		storageStartMS: base + 5*hour,
		storageTF:      "1h",
	}
	minuteRows := []*DataSeries{
		NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
			Time: exs.ListMs, Open: 1, High: 1, Low: 1, Close: 1, Volume: 1,
		}, nil, false, true),
	}
	if _, err := prependHistoricalListingPrefix(exs, prefix, minuteRows, nil); err == nil ||
		!strings.Contains(err.Error(), "continuous physical 1m") {
		t.Fatalf("missing minute gap error=%v", err)
	}
}

func TestDerivedHistoricalCoverageSelectsExactlyOnePhysicalSegment(t *testing.T) {
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
	if !constrained || err == nil || !strings.Contains(err.Error(), "exactly one matching continuous physical segment") {
		t.Fatalf("constrained=%v err=%v", constrained, err)
	}
	start, stop, constrained, err := historicalPhysicalCoverageBounds(
		coverage, "BTC/USDT:USDT", "4h", 100, 400)
	if err != nil || !constrained || start != 100 || stop != 400 {
		t.Fatalf("first physical segment=%d:%d constrained=%v err=%v", start, stop, constrained, err)
	}
	start, stop, constrained, err = historicalPhysicalCoverageBounds(
		coverage, "BTC/USDT:USDT", "4h", 500, 1000)
	if err != nil || !constrained || start != 500 || stop != 1000 {
		t.Fatalf("second physical segment=%d:%d constrained=%v err=%v", start, stop, constrained, err)
	}
}

func TestDerivedHistoricalCoverageReverseReadNeverCrossesSegmentLowerBound(t *testing.T) {
	const hour = int64(3_600_000)
	symbol := "BTC/USDT:USDT"
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: 48 * hour,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {
				"4h": {
					{StartMS: 16 * hour, StopMS: 28 * hour},
					{StartMS: 32 * hour, StopMS: 48 * hour},
				},
				"1h": {
					{StartMS: 17 * hour, StopMS: 28 * hour},
					{StartMS: 32 * hour, StopMS: 48 * hour},
				},
			},
		},
	}
	calls := 0
	_, rows, err := readHistoricalCoverageSeries(coverage, &ExSymbol{Symbol: symbol}, "4h", 0, 48*hour, 5, false,
		func(startMS, endMS int64, limit int, _ bool, reverse bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
			calls++
			if !reverse {
				t.Fatal("reverse historical request used a forward raw read")
			}
			physicalStart, physicalStop, constrained, boundsErr := historicalPhysicalCoverageBounds(
				coverage, symbol, "4h", startMS, endMS)
			wantConsumerStart := []int64{32 * hour, 16 * hour}[calls-1]
			wantPhysicalStart := []int64{32 * hour, 17 * hour}[calls-1]
			if boundsErr != nil || !constrained || startMS != wantConsumerStart ||
				physicalStart != wantPhysicalStart || physicalStop != endMS {
				t.Fatalf("call %d consumer=%d:%d physical=%d:%d constrained=%v err=%v",
					calls, startMS, endMS, physicalStart, physicalStop, constrained, boundsErr)
			}
			available := []*DataSeries{}
			for timestamp := startMS; timestamp < endMS; timestamp += 4 * hour {
				available = append(available, &DataSeries{TimeMS: timestamp})
			}
			if len(available) > limit {
				available = available[len(available)-limit:]
			}
			return nil, available, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{24 * hour, 32 * hour, 36 * hour, 40 * hour, 44 * hour}
	if calls != 2 || !slices.Equal(seriesTimes(rows), want) {
		t.Fatalf("calls=%d rows=%v want=%v", calls, seriesTimes(rows), want)
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
	_, rows, err := readHistoricalCoverageSeries(coverage, &ExSymbol{Symbol: "BTC/USDT:USDT"}, "1h", 0, 1000, 5, false,
		func(startMS, endMS int64, limit int, _ bool, reverse bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
			reads++
			if !reverse || startMS != []int64{700, 100}[reads-1] {
				t.Fatalf("reverse read %d bounds=%d:%d reverse=%v", reads, startMS, endMS, reverse)
			}
			eligible := make([]*DataSeries, 0)
			for _, row := range physical {
				if row.TimeMS >= startMS && row.TimeMS < endMS {
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
	_, rows, err := readHistoricalCoverageSeries(coverage, &ExSymbol{Symbol: "BTC/USDT:USDT"}, "1h", 100, 1000, 5, false,
		func(startMS, endMS int64, limit int, _ bool, reverse bool) ([]*AdjInfo, []*DataSeries, *errs.Error) {
			reads++
			if reverse {
				t.Fatal("forward historical read requested reverse mode")
			}
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
	previousMode, previousData, previousCoverage := core.BackTestMode, config.Data, config.HistoricalCoverage
	core.BackTestMode = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true
	config.HistoricalCoverage = &config.HistoricalCoverageConfig{
		BaselineEndMS: 1000,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			"BTC/USDT:USDT": {"1h": {{StartMS: 100, StopMS: 500}}},
		},
	}
	t.Cleanup(func() {
		core.BackTestMode, config.Data, config.HistoricalCoverage = previousMode, previousData, previousCoverage
	})

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
	config.Data = config.Config{BTStrict: true, BTNoKlineDownload: true}
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
	core.BackTestMode = false
	if !allowImplicitKlineDownload() || historicalCoverageForQuery("BTC/USDT:USDT") != nil {
		t.Fatal("live/raw K-line reads were constrained by historical coverage")
	}
}

func TestHistoricalCoverageReadGateRequiresStrictHistoricalReplay(t *testing.T) {
	previousMode, previousData, previousCoverage := core.BackTestMode, config.Data, config.HistoricalCoverage
	coverage := &config.HistoricalCoverageConfig{
		BaselineEndMS: 1000,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			"BTC/USDT:USDT": {"1h": {{StartMS: 100, StopMS: 500}}},
		},
	}
	t.Cleanup(func() {
		core.BackTestMode, config.Data, config.HistoricalCoverage = previousMode, previousData, previousCoverage
	})

	for _, test := range []struct {
		name         string
		backtest     bool
		strict       bool
		noDownload   bool
		wantCoverage bool
	}{
		{name: "strict historical replay", backtest: true, strict: true, noDownload: true, wantCoverage: true},
		{name: "non-strict backtest", backtest: true, noDownload: true},
		{name: "download-enabled backtest", backtest: true, strict: true},
		{name: "live", strict: true, noDownload: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			core.BackTestMode = test.backtest
			config.Data = config.Config{BTStrict: test.strict, BTNoKlineDownload: test.noDownload}
			config.HistoricalCoverage = coverage
			got := historicalCoverageForQuery("BTC/USDT:USDT")
			if (got != nil) != test.wantCoverage {
				t.Fatalf("coverage enabled = %v, want %v", got != nil, test.wantCoverage)
			}
		})
	}
}

func TestStrictBacktestRejectsEveryPublicKlineDownloadEntry(t *testing.T) {
	previousMode, previousData, previousCoverage := core.BackTestMode, config.Data, config.HistoricalCoverage
	core.BackTestMode = true
	config.Data = config.Config{BTNoKlineDownload: true}
	config.HistoricalCoverage = nil
	t.Cleanup(func() {
		core.BackTestMode, config.Data, config.HistoricalCoverage = previousMode, previousData, previousCoverage
	})

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

func TestStrictFastBulkAllowsAuditedDatabaseReadPath(t *testing.T) {
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

	if err := FastBulkOHLCV(nil, nil, "1h", 100, 500, 0, nil); err != nil {
		t.Fatalf("audited local FastBulk read was rejected: %v", err)
	}
}

func TestStrictEnsureListDatesAllowsOnlyReadOnlyMetadataPaths(t *testing.T) {
	previousMode, previousData, previousExchange := core.BackTestMode, config.Data, config.Exchange
	core.BackTestMode = true
	config.Data = config.Config{BTNoKlineDownload: true}
	config.Exchange = &config.ExchangeConfig{Name: "binance", Items: map[string]map[string]interface{}{}}
	t.Cleanup(func() {
		core.BackTestMode, config.Data, config.Exchange = previousMode, previousData, previousExchange
	})

	linear, err := exg.GetWith("binance", "linear", "")
	if err != nil {
		t.Fatal(err)
	}
	unknown := &ExSymbol{ID: 1, Symbol: "NEW/USDT:USDT"}
	known := &ExSymbol{ID: 2, Symbol: "BTC/USDT", ListMs: 100}
	if ensureErr := EnsureListDates(nil, linear, map[int32]*ExSymbol{known.ID: known}, nil); ensureErr != nil {
		t.Fatalf("linear list-date no-op was rejected: %v", ensureErr)
	}
	if ensureErr := EnsureListDates(nil, linear, map[int32]*ExSymbol{unknown.ID: unknown}, nil); ensureErr == nil ||
		!strings.Contains(strings.ToLower(ensureErr.Error()), "download is disabled") {
		t.Fatalf("unknown linear list date error=%v", ensureErr)
	}

	spot, err := exg.GetWith("binance", "spot", "")
	if err != nil {
		t.Fatal(err)
	}
	if ensureErr := EnsureListDates(nil, spot, map[int32]*ExSymbol{known.ID: known}, nil); ensureErr != nil {
		t.Fatalf("known spot list date was rejected: %v", ensureErr)
	}
	if ensureErr := EnsureListDates(nil, spot, map[int32]*ExSymbol{unknown.ID: unknown}, nil); ensureErr == nil ||
		!strings.Contains(strings.ToLower(ensureErr.Error()), "download is disabled") {
		t.Fatalf("unknown spot list date error=%v", ensureErr)
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

func TestFastBulkDoesNotRefilterAuditedQueryRows(t *testing.T) {
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
	if times := klineTimes(got); !slices.Equal(times, []int64{500, 600, 700, 800, 900}) {
		t.Fatalf("FastBulk refiltered rows already audited by QuerySeries: %v", times)
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
