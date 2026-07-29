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
	BaselineEndMS int64                                           `yaml:"baseline_end_ms" mapstructure:"baseline_end_ms"`
	Bars          map[string]map[string][]HistoricalCoverageRange `yaml:"bars" mapstructure:"bars"`
}

func (c *HistoricalCoverageConfig) Normalize(runRange *TimeTuple) error {
	if runRange == nil || c.BaselineEndMS <= runRange.StartMS || c.BaselineEndMS > runRange.EndMS {
		return fmt.Errorf("historical_coverage baseline_end_ms must be after the start and at or before the end of the backtest range")
	}
	if len(c.Bars) == 0 {
		return fmt.Errorf("historical_coverage bars are required")
	}
	for symbol, timeframes := range c.Bars {
		if symbol == "" || len(timeframes) == 0 {
			return fmt.Errorf("historical_coverage contains an empty symbol or timeframe set")
		}
		for timeframe, ranges := range timeframes {
			if timeframe == "" || len(ranges) == 0 {
				return fmt.Errorf("historical_coverage contains an empty timeframe or range set")
			}
			for _, item := range ranges {
				if item.StartMS < 0 || item.StopMS <= item.StartMS || item.StopMS > c.BaselineEndMS {
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
	clone := &HistoricalCoverageConfig{BaselineEndMS: c.BaselineEndMS, Bars: make(map[string]map[string][]HistoricalCoverageRange, len(c.Bars))}
	for symbol, timeframes := range c.Bars {
		clone.Bars[symbol] = make(map[string][]HistoricalCoverageRange, len(timeframes))
		for timeframe, ranges := range timeframes {
			clone.Bars[symbol][timeframe] = slices.Clone(ranges)
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
	return &HistoricalCoverageConfig{BaselineEndMS: HistoricalCoverage.BaselineEndMS, Bars: map[string]map[string][]HistoricalCoverageRange{symbol: timeframes}}
}

func (c *HistoricalCoverageConfig) Allows(timeframe string, timeMS int64) bool {
	if c == nil || timeMS >= c.BaselineEndMS {
		return true
	}
	for _, timeframes := range c.Bars {
		ranges := timeframes[timeframe]
		index, found := slices.BinarySearchFunc(ranges, timeMS, func(item HistoricalCoverageRange, target int64) int {
			if item.StopMS <= target {
				return -1
			}
			if item.StartMS > target {
				return 1
			}
			return 0
		})
		return found && index < len(ranges)
	}
	return false
}
