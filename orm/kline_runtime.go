package orm

import (
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
)

// KlineRuntimeOptions contains the low-frequency policy needed by K-line
// loading. Explicit runners bind it once from their Runtime state; the bar
// processing path only reads these concrete fields.
type KlineRuntimeOptions struct {
	Backtest           bool
	Live               bool
	StrictReplay       bool
	NoDownload         bool
	NetDisable         bool
	HistoricalCoverage *config.HistoricalCoverageConfig
	TimeRangeEndMS     int64
	ConcurNum          int
	NowMS              int64
	// ClockValid distinguishes an instance clock from a caller-supplied
	// timestamp. Explicit ORM operations must not treat a zero/missing clock as
	// Unix epoch data or silently borrow the process clock.
	ClockValid bool
	Storage    *Storage
	Sleep      func(time.Duration) bool
}

// LegacyKlineRuntimeOptions snapshots the package facade for compatibility
// callers. New runners should construct options from their own state.
func LegacyKlineRuntimeOptions() KlineRuntimeOptions {
	return KlineRuntimeOptions{
		Backtest:           core.BackTestMode,
		Live:               core.LiveMode,
		StrictReplay:       config.StrictBacktest() && config.Data.BTNoKlineDownload,
		NoDownload:         config.Data.BTNoKlineDownload,
		NetDisable:         core.NetDisable,
		HistoricalCoverage: config.HistoricalCoverage,
		TimeRangeEndMS:     timeRangeEnd(config.TimeRange),
		ConcurNum:          core.ConcurNum,
		NowMS:              btime.TimeMS(),
		ClockValid:         true,
		Sleep:              core.Sleep,
	}
}

// NewKlineRuntimeOptions binds K-line policy from explicit runtime fields.
func NewKlineRuntimeOptions(runtimeCore *core.State, cfg *config.Config, nowMS int64, storage *Storage) KlineRuntimeOptions {
	options := KlineRuntimeOptions{
		Storage:    storage,
		NowMS:      nowMS,
		ClockValid: nowMS > 0,
		ConcurNum:  2,
	}
	if runtimeCore != nil {
		options.Backtest = runtimeCore.BackTestMode
		options.Live = runtimeCore.LiveMode
		options.NetDisable = runtimeCore.NetDisable
		if runtimeCore.ConcurNum > 0 {
			options.ConcurNum = runtimeCore.ConcurNum
		}
		options.Sleep = runtimeCore.Sleep
	}
	if cfg != nil {
		options.NoDownload = cfg.BTNoKlineDownload
		options.HistoricalCoverage = cfg.HistoricalCoverage
		options.StrictReplay = runtimeCore != nil && runtimeCore.BackTestMode && cfg.BTStrict &&
			cfg.BTNoKlineDownload && cfg.HistoricalCoverage != nil
		options.TimeRangeEndMS = timeRangeEnd(cfg.TimeRange)
		if cfg.ConcurNum > 0 && runtimeCore == nil {
			options.ConcurNum = cfg.ConcurNum
		}
	}
	return options
}

func timeRangeEnd(timeRange *config.TimeTuple) int64 {
	if timeRange == nil {
		return 0
	}
	return timeRange.EndMS
}

func (o KlineRuntimeOptions) allowDownload() bool {
	return !o.Backtest || !o.NoDownload
}

func validateKlineRuntimeOptions(options KlineRuntimeOptions) *errs.Error {
	if !options.ClockValid || options.NowMS <= 0 {
		return errs.NewMsg(core.ErrBadConfig, "explicit K-line runtime requires a runtime clock")
	}
	return nil
}

func (o KlineRuntimeOptions) nowMS() int64 {
	return o.NowMS
}

func (o KlineRuntimeOptions) strictHistoricalReplay() bool {
	return o.StrictReplay && o.HistoricalCoverage != nil
}

func (o KlineRuntimeOptions) concurrency() int {
	if o.ConcurNum > 0 {
		return o.ConcurNum
	}
	return 2
}

func (o KlineRuntimeOptions) sleep(delay time.Duration) bool {
	if o.Sleep != nil {
		return o.Sleep(delay)
	}
	time.Sleep(delay)
	return true
}
