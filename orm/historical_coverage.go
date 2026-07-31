package orm

import (
	"slices"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
)

type historicalCoverageInterval struct {
	StartMS int64
	StopMS  int64
}

type historicalListingPrefix struct {
	bucketStartMS  int64
	minuteStartMS  int64
	storageStartMS int64
	storageTF      string
}

type seriesFieldsReader func(startMS, endMS int64, limit int, withUnFinish bool) ([]*AdjInfo, []*DataSeries, *errs.Error)

type historicalSeriesFieldsReader func(startMS, endMS int64, limit int, withUnFinish,
	reverse bool,
) ([]*AdjInfo, []*DataSeries, *errs.Error)

func historicalCoverageForQuery(symbol string) *config.HistoricalCoverageConfig {
	if !config.StrictHistoricalReplay(config.HistoricalCoverage) {
		return nil
	}
	return config.HistoricalCoverageFor(symbol)
}

func validateHistoricalCoverageFields(coverage *config.HistoricalCoverageConfig, fields []string) *errs.Error {
	if coverage == nil {
		return nil
	}
	allowed := DefaultKlineFields()
	for _, field := range NormalizeSeriesFields(SeriesSourceKline, fields) {
		if !slices.Contains(allowed, field) {
			return errs.NewMsg(core.ErrBadConfig,
				"historical coverage has no physical field proof for %q", field)
		}
	}
	return nil
}

func historicalCoverageIntervals(coverage *config.HistoricalCoverageConfig, symbol, timeframe string,
	startMS, endMS int64,
) []historicalCoverageInterval {
	if coverage == nil {
		return nil
	}
	if endMS == 0 {
		endMS = btime.TimeMS()
	}
	if config.TimeRange != nil && config.TimeRange.EndMS > 0 {
		endMS = min(endMS, config.TimeRange.EndMS)
	}
	if endMS <= startMS {
		return nil
	}
	intervals := make([]historicalCoverageInterval, 0)
	if timeframes := coverage.Bars[symbol]; timeframes != nil {
		for _, item := range timeframes[timeframe] {
			start := max(startMS, item.StartMS)
			stop := min(endMS, item.StopMS, coverage.BaselineEndMS)
			if stop > start {
				intervals = append(intervals, historicalCoverageInterval{StartMS: start, StopMS: stop})
			}
		}
	}
	if endMS > coverage.BaselineEndMS && historicalCoverageHasTimeframe(coverage, symbol, timeframe) {
		start := max(startMS, coverage.BaselineEndMS)
		if endMS > start {
			intervals = append(intervals, historicalCoverageInterval{StartMS: start, StopMS: endMS})
		}
	}
	slices.SortFunc(intervals, func(left, right historicalCoverageInterval) int {
		if left.StartMS < right.StartMS {
			return -1
		}
		if left.StartMS > right.StartMS {
			return 1
		}
		return 0
	})
	merged := intervals[:0]
	for _, item := range intervals {
		if len(merged) == 0 || item.StartMS > merged[len(merged)-1].StopMS {
			merged = append(merged, item)
			continue
		}
		merged[len(merged)-1].StopMS = max(merged[len(merged)-1].StopMS, item.StopMS)
	}
	return merged
}

func historicalCoverageHasTimeframe(coverage *config.HistoricalCoverageConfig, symbol, timeframe string) bool {
	if coverage == nil {
		return false
	}
	timeframes := coverage.Bars[symbol]
	_, exists := timeframes[timeframe]
	return exists
}

func historicalPhysicalCoverageBounds(coverage *config.HistoricalCoverageConfig, symbol, timeframe string,
	startMS, endMS int64,
) (int64, int64, bool, *errs.Error) {
	if coverage == nil {
		return startMS, endMS, false, nil
	}
	storageTF, err := PhysicalKlineStorageTimeframe(timeframe)
	if err != nil {
		return 0, 0, false, err
	}
	if storageTF == timeframe {
		return startMS, endMS, false, nil
	}
	consumerSecs, tfErr := utils2.TFToSecSafe(timeframe)
	if tfErr != nil || consumerSecs <= 0 {
		return 0, 0, true, errs.NewMsg(core.ErrInvalidTF, "invalid timeframe: %s", timeframe)
	}
	consumerStepMS := int64(consumerSecs * 1000)
	intervals := historicalCoverageIntervals(coverage, symbol, storageTF, 0, endMS)
	matches := intervals[:0]
	for _, item := range intervals {
		if item.StopMS <= startMS || item.StartMS >= endMS {
			continue
		}
		matches = append(matches, item)
	}
	if len(matches) != 1 || matches[0].StopMS < endMS || matches[0].StartMS >= startMS+consumerStepMS {
		return 0, 0, true, errs.NewMsg(core.ErrBadConfig,
			"derived historical coverage requires exactly one matching continuous physical segment for %s %s [%d,%d) via %s",
			symbol, timeframe, startMS, endMS, storageTF)
	}
	return max(startMS, matches[0].StartMS), endMS, true, nil
}

func readHistoricalCoverageSeries(coverage *config.HistoricalCoverageConfig, exs *ExSymbol, timeframe string,
	startMS, endMS int64, limit int, withUnFinish bool, read historicalSeriesFieldsReader,
) ([]*AdjInfo, []*DataSeries, *errs.Error) {
	symbol := exs.Symbol
	intervals := historicalCoverageIntervals(coverage, symbol, timeframe, startMS, endMS)
	intervals = extendLegacyListingCoverage(coverage, exs, timeframe, startMS, intervals)
	if len(intervals) == 0 {
		return nil, nil, nil
	}
	if limit <= 0 {
		var adjs []*AdjInfo
		var result []*DataSeries
		for index, item := range intervals {
			partAdjs, rows, err := read(item.StartMS, item.StopMS, 0,
				withUnFinish && index == len(intervals)-1, false)
			if err != nil {
				return nil, nil, err
			}
			if adjs == nil {
				adjs = partAdjs
			}
			result = append(result, filterSeriesInterval(rows, item)...)
		}
		return adjs, result, nil
	}
	var adjs []*AdjInfo
	result := make([]*DataSeries, 0, limit)
	if startMS == 0 {
		for index := len(intervals) - 1; index >= 0 && len(result) < limit; index-- {
			item := intervals[index]
			partAdjs, rows, err := read(item.StartMS, item.StopMS, limit-len(result),
				withUnFinish && index == len(intervals)-1, true)
			if err != nil {
				return nil, nil, err
			}
			if adjs == nil {
				adjs = partAdjs
			}
			rows = filterSeriesInterval(rows, item)
			result = append(rows, result...)
		}
		if len(result) > limit {
			result = result[len(result)-limit:]
		}
		return adjs, result, nil
	}
	for index, item := range intervals {
		partAdjs, rows, err := read(item.StartMS, item.StopMS, limit-len(result),
			withUnFinish && index == len(intervals)-1, false)
		if err != nil {
			return nil, nil, err
		}
		if adjs == nil {
			adjs = partAdjs
		}
		result = append(result, filterSeriesInterval(rows, item)...)
		if len(result) >= limit {
			return adjs, result[:limit], nil
		}
	}
	return adjs, result, nil
}

func extendLegacyListingCoverage(coverage *config.HistoricalCoverageConfig, exs *ExSymbol, timeframe string,
	requestedStartMS int64, intervals []historicalCoverageInterval,
) []historicalCoverageInterval {
	prefix, ok := legacyListingPrefixProof(coverage, exs, timeframe, requestedStartMS, intervals)
	if !ok {
		return intervals
	}
	result := slices.Clone(intervals)
	result[0].StartMS = prefix.bucketStartMS
	return result
}

func legacyListingPrefixProof(coverage *config.HistoricalCoverageConfig, exs *ExSymbol, timeframe string,
	requestedStartMS int64, intervals []historicalCoverageInterval,
) (historicalListingPrefix, bool) {
	if !config.StrictHistoricalReplay(coverage) || exs == nil || exs.ListMs <= 0 || len(intervals) == 0 {
		return historicalListingPrefix{}, false
	}
	storageTF, err := PhysicalKlineStorageTimeframe(timeframe)
	if err != nil {
		return historicalListingPrefix{}, false
	}
	consumerSecs, tfErr := utils2.TFToSecSafe(timeframe)
	if tfErr != nil || consumerSecs <= 0 {
		return historicalListingPrefix{}, false
	}
	consumerStepMS := int64(consumerSecs * 1000)
	_, consumerOffsetSecs := utils2.GetTfAlignOrigin(consumerSecs)
	consumerOffsetMS := int64(consumerOffsetSecs * 1000)
	bucketStart := alignPhysicalKlineFloor(exs.ListMs, consumerStepMS, consumerOffsetMS)
	fullStart := alignPhysicalKlineCeil(exs.ListMs, consumerStepMS, consumerOffsetMS)
	ranges := coverage.Bars[exs.Symbol][timeframe]
	if bucketStart >= exs.ListMs || requestedStartMS > bucketStart || len(ranges) == 0 ||
		intervals[0].StartMS != ranges[0].StartMS ||
		ranges[0].StartMS <= bucketStart || ranges[0].StartMS > fullStart {
		return historicalListingPrefix{}, false
	}
	minuteStart := alignPhysicalKlineCeil(exs.ListMs, 60_000,
		int64(exg.GetAlignOff(exs.Exchange, 60)*1000))
	minutes := historicalCoverageIntervals(coverage, exs.Symbol, "1m", minuteStart, fullStart)
	if len(minutes) != 1 || minutes[0].StartMS != minuteStart || minutes[0].StopMS < fullStart {
		return historicalListingPrefix{}, false
	}
	storageSecs, storageErr := utils2.TFToSecSafe(storageTF)
	if storageErr != nil || storageSecs <= 0 {
		return historicalListingPrefix{}, false
	}
	storageStepMS := int64(storageSecs * 1000)
	storageOffsetMS := int64(exg.GetAlignOff(exs.Exchange, storageSecs) * 1000)
	storageStart := alignPhysicalKlineCeil(exs.ListMs, storageStepMS, storageOffsetMS)
	if storageTF != timeframe {
		physical := historicalCoverageIntervals(coverage, exs.Symbol, storageTF, storageStart, fullStart)
		if len(physical) != 1 || physical[0].StartMS != storageStart || physical[0].StopMS < fullStart {
			return historicalListingPrefix{}, false
		}
	}
	return historicalListingPrefix{
		bucketStartMS:  bucketStart,
		minuteStartMS:  minuteStart,
		storageStartMS: storageStart,
		storageTF:      storageTF,
	}, true
}

func prependHistoricalListingPrefix(exs *ExSymbol, prefix historicalListingPrefix,
	minuteRows, storageRows []*DataSeries,
) ([]*DataSeries, *errs.Error) {
	if prefix.minuteStartMS >= prefix.storageStartMS {
		return storageRows, nil
	}
	const minuteMS = int64(60_000)
	expectedMS := prefix.minuteStartMS
	for _, row := range minuteRows {
		if row == nil || row.TimeMS != expectedMS {
			return nil, errs.NewMsg(core.ErrBadConfig,
				"historical listing prefix requires continuous physical 1m rows for %s [%d,%d)",
				exs.Symbol, prefix.minuteStartMS, prefix.storageStartMS)
		}
		expectedMS += minuteMS
	}
	if expectedMS != prefix.storageStartMS {
		return nil, errs.NewMsg(core.ErrBadConfig,
			"historical listing prefix requires continuous physical 1m rows for %s [%d,%d)",
			exs.Symbol, prefix.minuteStartMS, prefix.storageStartMS)
	}
	storageSecs, err := utils2.TFToSecSafe(prefix.storageTF)
	if err != nil || storageSecs <= 0 {
		return nil, errs.NewMsg(core.ErrInvalidTF, "invalid historical storage timeframe: %s", prefix.storageTF)
	}
	storageMS := int64(storageSecs * 1000)
	offsetMS := int64(exg.GetAlignOff(exs.Exchange, storageSecs) * 1000)
	aggregated, finished, aggErr := ResampleDataSeries(exs, prefix.storageTF, minuteRows, nil,
		storageMS, 0, minuteMS, offsetMS, false)
	wantTimeMS := alignPhysicalKlineFloor(prefix.minuteStartMS, storageMS, offsetMS)
	if aggErr != nil || !finished || len(aggregated) != 1 || aggregated[0].TimeMS != wantTimeMS {
		return nil, errs.NewMsg(core.ErrInvalidBars,
			"unable to aggregate historical listing prefix for %s %s [%d,%d): %v",
			exs.Symbol, prefix.storageTF, prefix.minuteStartMS, prefix.storageStartMS, aggErr)
	}
	result := make([]*DataSeries, 0, len(storageRows)+1)
	result = append(result, aggregated[0])
	if len(storageRows) > 0 && storageRows[0] != nil && storageRows[0].TimeMS == aggregated[0].TimeMS {
		storageRows = storageRows[1:]
	}
	result = append(result, storageRows...)
	return result, nil
}

// HistoricalCoverageAllows preserves a physically proved partial listing
// bucket that starts before the exchange listing timestamp.
func HistoricalCoverageAllows(coverage *config.HistoricalCoverageConfig, exs *ExSymbol,
	timeframe string, timeMS int64,
) bool {
	if coverage == nil || coverage.Allows(timeframe, timeMS) {
		return true
	}
	if exs == nil {
		return false
	}
	intervals := historicalCoverageIntervals(coverage, exs.Symbol, timeframe, 0, coverage.BaselineEndMS)
	intervals = extendLegacyListingCoverage(coverage, exs, timeframe, 0, intervals)
	return len(intervals) > 0 && timeMS >= intervals[0].StartMS && timeMS < intervals[0].StopMS
}

func filterSeriesInterval(rows []*DataSeries, interval historicalCoverageInterval) []*DataSeries {
	return slices.DeleteFunc(slices.Clone(rows), func(row *DataSeries) bool {
		return row == nil || row.TimeMS < interval.StartMS || row.TimeMS >= interval.StopMS
	})
}
