package config

import (
	"fmt"
	"slices"
)

type HistoricalCoverageRange struct {
	StartMS int64 `yaml:"start_ms" mapstructure:"start_ms"`
	StopMS  int64 `yaml:"stop_ms" mapstructure:"stop_ms"`
}

type HistoricalCoverageConfig struct {
	BaselineEndMS   int64                                           `yaml:"baseline_end_ms" mapstructure:"baseline_end_ms"`
	Bars            map[string]map[string][]HistoricalCoverageRange `yaml:"bars" mapstructure:"bars"`
	ListingPrefixes map[string]map[string][]HistoricalCoverageRange `yaml:"listing_prefixes" mapstructure:"listing_prefixes"`
}

func (c *HistoricalCoverageConfig) Normalize(runRange *TimeTuple) error {
	if runRange == nil || c.BaselineEndMS <= runRange.StartMS || c.BaselineEndMS > runRange.EndMS {
		return fmt.Errorf("historical_coverage baseline_end_ms must be after the start and at or before the end of the backtest range")
	}
	if len(c.Bars) == 0 {
		return fmt.Errorf("historical_coverage bars are required")
	}
	if err := normalizeHistoricalCoverageRanges(c.Bars, c.BaselineEndMS); err != nil {
		return err
	}
	return normalizeHistoricalCoverageRanges(c.ListingPrefixes, c.BaselineEndMS)
}

func normalizeHistoricalCoverageRanges(bars map[string]map[string][]HistoricalCoverageRange,
	baselineEndMS int64,
) error {
	for symbol, timeframes := range bars {
		if symbol == "" || len(timeframes) == 0 {
			return fmt.Errorf("historical_coverage contains an empty symbol or timeframe set")
		}
		for timeframe, ranges := range timeframes {
			if timeframe == "" || len(ranges) == 0 {
				return fmt.Errorf("historical_coverage contains an empty timeframe or range set")
			}
			for _, item := range ranges {
				if item.StartMS < 0 || item.StopMS <= item.StartMS || item.StopMS > baselineEndMS {
					return fmt.Errorf("historical_coverage has an invalid range for %s %s", symbol, timeframe)
				}
			}
			slices.SortFunc(ranges, func(a, b HistoricalCoverageRange) int {
				if a.StartMS < b.StartMS {
					return -1
				}
				if a.StartMS > b.StartMS {
					return 1
				}
				return 0
			})
			merged := ranges[:0]
			for _, item := range ranges {
				if len(merged) == 0 || item.StartMS > merged[len(merged)-1].StopMS {
					merged = append(merged, item)
					continue
				}
				merged[len(merged)-1].StopMS = max(merged[len(merged)-1].StopMS, item.StopMS)
			}
			timeframes[timeframe] = merged
		}
	}
	return nil
}

func (c *HistoricalCoverageConfig) Clone() *HistoricalCoverageConfig {
	if c == nil {
		return nil
	}
	clone := &HistoricalCoverageConfig{BaselineEndMS: c.BaselineEndMS}
	clone.Bars = cloneHistoricalCoverageRanges(c.Bars)
	clone.ListingPrefixes = cloneHistoricalCoverageRanges(c.ListingPrefixes)
	return clone
}

func cloneHistoricalCoverageRanges(bars map[string]map[string][]HistoricalCoverageRange) map[string]map[string][]HistoricalCoverageRange {
	if bars == nil {
		return nil
	}
	clone := make(map[string]map[string][]HistoricalCoverageRange, len(bars))
	for symbol, timeframes := range bars {
		clone[symbol] = make(map[string][]HistoricalCoverageRange, len(timeframes))
		for timeframe, ranges := range timeframes {
			clone[symbol][timeframe] = slices.Clone(ranges)
		}
	}
	return clone
}

func HistoricalCoverageFor(symbol string) *HistoricalCoverageConfig {
	if HistoricalCoverage == nil {
		return nil
	}
	timeframes := HistoricalCoverage.Bars[symbol]
	if len(timeframes) == 0 {
		return &HistoricalCoverageConfig{BaselineEndMS: HistoricalCoverage.BaselineEndMS}
	}
	result := &HistoricalCoverageConfig{BaselineEndMS: HistoricalCoverage.BaselineEndMS,
		Bars: map[string]map[string][]HistoricalCoverageRange{symbol: timeframes}}
	if HistoricalCoverage.ListingPrefixes != nil {
		result.ListingPrefixes = map[string]map[string][]HistoricalCoverageRange{
			symbol: HistoricalCoverage.ListingPrefixes[symbol],
		}
	}
	return result
}

func (c *HistoricalCoverageConfig) Allows(timeframe string, timeMS int64) bool {
	if c == nil {
		return true
	}
	if TimeRange != nil && TimeRange.EndMS > 0 && timeMS >= TimeRange.EndMS {
		return false
	}
	for _, timeframes := range c.Bars {
		ranges, exists := timeframes[timeframe]
		if !exists {
			continue
		}
		// The archived bar plan is also the allow-list for the extension tail.
		// A newly requested symbol/timeframe must never gain access merely because
		// its timestamp is after the historical baseline.
		if timeMS >= c.BaselineEndMS {
			return true
		}
		index, found := slices.BinarySearchFunc(ranges, timeMS, func(item HistoricalCoverageRange, target int64) int {
			if item.StopMS <= target {
				return -1
			}
			if item.StartMS > target {
				return 1
			}
			return 0
		})
		if found && index < len(ranges) {
			return true
		}
	}
	return false
}
