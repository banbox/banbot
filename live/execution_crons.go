package live

import (
	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
)

// ExecutionRobotCronOptions tunes StartExecutionRobotCrons.
type ExecutionRobotCronOptions struct {
	// HourOnlySymbols returns the symbols whose finest kline subscription is
	// 1h or coarser. Their bars are only delivered by the hourly repair job.
	// The execution-backed provider isolates its own symbol state, so the
	// runtime symbol state never lists them; entries derive the set from their
	// subscriptions. Nil falls back to the runtime symbol state.
	HourOnlySymbols func() map[int32]*orm.ExSymbol
	// Received is the feed progress the delay monitor reads: when the last row
	// arrived and, per pair, its bar end and timeframe. The caller must update
	// it from the rows this runtime actually feeds to strategies. The
	// execution-backed provider keeps its own isolated market state, so the
	// trader's state is never written on this path; reading it would report a
	// permanent timeout and, through close_on_stuck, close every open order.
	// Nil disables the delay monitor.
	Received *com.PairCopiedState
}

// StartExecutionRobotCrons registers the maintenance jobs that are meaningful
// for a runtime on the execution-backed path and starts its scheduler. The
// scheduler belongs to the Runtime and is stopped by Runtime.Close.
//
// Only jobs that need nothing beyond public market data and the runtime's own
// strategy state are registered: hourly kline repair, market reload, kline
// delay monitor, receive summary and strategy output dump. Pair rotation,
// fatal-loss check, trigger-order verification, balance/position polling,
// wallet snapshots and the live-vs-backtest comparison depend on the legacy
// wallet/order manager or on private exchange endpoints and are never
// registered here.
//
// Call it once per runtime after the provider is installed. A failure here is
// auxiliary: callers log it and keep trading.
func StartExecutionRobotCrons(trader *CryptoTrader, dp *data.LiveProvider, opts ExecutionRobotCronOptions) *errs.Error {
	if trader == nil || dp == nil {
		return errs.NewMsg(core.ErrBadConfig, "execution robot crons require a trader and a live provider")
	}
	deps := trader.RuntimeDependencies()
	if deps == nil {
		return errs.NewMsg(core.ErrBadConfig, "execution robot crons require a runtime-bound trader")
	}
	scheduler := trader.schedulerForRun()
	if scheduler == nil {
		return errs.NewMsg(core.ErrBadConfig, "execution robot crons require the runtime scheduler")
	}
	var catalog *data.DataSourceCatalog
	if trader.dataDeps != nil {
		catalog = trader.dataDeps.Catalog
	}
	runExecutionRobotCrons(scheduler, deps, dp, catalog, opts.Received, trader.currentTimeMS, opts)
	return nil
}

func runExecutionRobotCrons(scheduler com.Scheduler, deps *biz.RuntimeDeps, dp *data.LiveProvider,
	catalog *data.DataSourceCatalog, copied *com.PairCopiedState, clock func() int64, opts ExecutionRobotCronOptions) {
	registerExecutionRobotCrons(scheduler, deps, dp, catalog, copied, clock, opts)
	scheduler.Start()
}

func registerExecutionRobotCrons(scheduler com.Scheduler, deps *biz.RuntimeDeps, dp *data.LiveProvider,
	catalog *data.DataSourceCatalog, copied *com.PairCopiedState, clock func() int64, opts ExecutionRobotCronOptions) {
	hourOnly := opts.HourOnlySymbols
	if hourOnly == nil && deps.Symbols != nil {
		hourOnly = deps.Symbols.GetHourOnlySymbols
	}
	fetchHourKlinesFromWithRuntime(scheduler, dp, deps, catalog, hourOnly)
	cronLoadMarketsWithRuntime(scheduler, deps.Exchange, deps.Symbols, deps.Config, deps.Core)
	cronKlineDelaysWithRuntime(scheduler, dp, copied, clock, *deps)
	cronKlineSummaryWithRuntime(scheduler, deps.Core)
	if cfg := runtimeConfig(deps); cfg != nil {
		dataDir := ""
		if deps.Config != nil {
			dataDir = deps.Config.DataDir
		}
		cronDumpStratOutputsWithRuntime(scheduler, deps.Strategies, cfg, dataDir)
	}
}
