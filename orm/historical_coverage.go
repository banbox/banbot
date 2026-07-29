package orm

import (
	"slices"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type historicalCoverageInterval struct {
	StartMS int64
	StopMS  int64
}

type seriesFieldsReader func(startMS, endMS int64, limit int, withUnFinish bool) ([]*AdjInfo, []*DataSeries, *errs.Error)

func historicalCoverageForQuery(symbol string) *config.HistoricalCoverageConfig {
	if !core.BackTestMode {
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

func readHistoricalCoverageSeries(coverage *config.HistoricalCoverageConfig, symbol, timeframe string,
	startMS, endMS int64, limit int, withUnFinish bool, read seriesFieldsReader,
) ([]*AdjInfo, []*DataSeries, *errs.Error) {
	intervals := historicalCoverageIntervals(coverage, symbol, timeframe, startMS, endMS)
	if len(intervals) == 0 {
		return nil, nil, nil
	}
	if limit <= 0 {
		var adjs []*AdjInfo
		var result []*DataSeries
		for index, item := range intervals {
			partAdjs, rows, err := read(item.StartMS, item.StopMS, 0,
				withUnFinish && index == len(intervals)-1)
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
			partAdjs, rows, err := read(0, item.StopMS, limit-len(result), withUnFinish && index == len(intervals)-1)
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
			withUnFinish && index == len(intervals)-1)
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

func filterSeriesInterval(rows []*DataSeries, interval historicalCoverageInterval) []*DataSeries {
	return slices.DeleteFunc(slices.Clone(rows), func(row *DataSeries) bool {
		return row == nil || row.TimeMS < interval.StartMS || row.TimeMS >= interval.StopMS
	})
}

func filterHistoricalCoverageKlines(symbol, timeframe string, rows []*banexg.Kline) []*banexg.Kline {
	coverage := historicalCoverageForQuery(symbol)
	if coverage == nil {
		return rows
	}
	return slices.DeleteFunc(slices.Clone(rows), func(row *banexg.Kline) bool {
		return row == nil || !coverage.Allows(timeframe, row.Time)
	})
}
