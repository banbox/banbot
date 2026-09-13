package data

import (
	"context"
	"fmt"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CallbackTracker lets a composition root account for provider callbacks
// without making data depend on the runtime package.
type CallbackTracker interface {
	EnterCallback() bool
	LeaveCallback()
}

// LifecycleRegistrar is the optional owner-side lifecycle surface used when a
// provider is constructed directly from a Runtime. It is deliberately narrow
// so the data package does not depend on runtime or a dynamic service bag.
type LifecycleRegistrar interface {
	OnClose(func())
	OnCloseWait(func())
}

// RuntimeDeps is the narrow, typed dependency set needed by data providers.
// It is stored on a provider/feeder instance, never discovered dynamically.
// A nil dependency set means that the caller is using the legacy package facade.
type RuntimeDeps struct {
	Core       *core.State
	Clock      *btime.ClockState
	Config     *config.Snapshot
	Market     *com.MarketState
	Symbols    *orm.SymbolState
	Storage    *orm.Storage
	Strategies *strat.State
	Catalog    *DataSourceCatalog
	Dump       *orm.DumpSink
	Callbacks  CallbackTracker
	Exchange   banexg.BanExchange
	// IdentityErr records a fail-closed adapter metadata error discovered while
	// binding this dependency set. Keeping it on the typed view preserves the
	// original failure instead of silently treating a panic as empty identity.
	IdentityErr error

	ExchangeName string
	MarketType   string
}

func (d *RuntimeDeps) storage() *orm.Storage {
	if d == nil {
		return nil
	}
	if d.Storage != nil {
		return d.Storage
	}
	if d.Symbols != nil {
		return d.Symbols.Storage()
	}
	return nil
}

func (d *RuntimeDeps) conn() (*orm.Queries, *pgxpool.Conn, *errs.Error) {
	if d == nil {
		return orm.Conn(nil)
	}
	storage := d.storage()
	if storage == nil {
		return nil, nil, errs.NewMsg(core.ErrDbConnFail, "explicit data storage is required")
	}
	if d.Clock == nil {
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "explicit data runtime clock is required")
	}
	sess, conn, err := storage.Conn(d.context())
	if err == nil && d.Exchange != nil {
		sess = sess.WithExchange(d.Exchange)
	}
	if err == nil && d.Symbols != nil {
		sess = sess.WithSeriesSymbolState(d.Symbols)
	}
	if err == nil {
		sess = sess.WithKlineRuntimeOptions(d.KlineOptions())
	}
	return sess, conn, err
}

func (d *RuntimeDeps) isQuestDB() bool {
	if d == nil {
		return orm.IsQuestDB
	}
	storage := d.storage()
	return storage != nil && storage.IsQuestDB()
}

func (d *RuntimeDeps) configView() *config.Config {
	if d == nil || d.Config == nil {
		return nil
	}
	return d.Config.View()
}

func (d *RuntimeDeps) timeRange() *config.TimeTuple {
	if cfg := d.configView(); cfg != nil {
		return cfg.TimeRange
	}
	return nil
}

func (d *RuntimeDeps) timeMS() int64 {
	if d == nil {
		return btime.TimeMS()
	}
	if d.Clock != nil {
		return d.Clock.TimeMS()
	}
	// Explicit dependencies must carry their own clock. Returning zero keeps
	// callers deterministic and lets validation/scheduling paths fail closed;
	// it must never silently switch a simulated run to wall-clock time.
	return 0
}

func (d *RuntimeDeps) utcStamp() int64 {
	// ClockState.TimeMS uses the simulated clock only for backtests and the
	// wall clock for live runs, so it is the correct instance-local equivalent
	// for data scheduling too.
	return d.timeMS()
}

func (d *RuntimeDeps) identity() (string, string) {
	name, market, err := d.ResolveIdentity()
	if err != nil {
		return "", ""
	}
	return name, market
}

// ResolveIdentity returns the complete exchange/market identity proven by the
// explicit adapter and dependency fields. Callers at construction boundaries
// should use the error result; hot-path compatibility callers can continue to
// use identity(), which maps an invalid identity to an empty pair.
func (d *RuntimeDeps) ResolveIdentity() (string, string, error) {
	if d == nil {
		return core.ExgName, core.Market, nil
	}
	if d.IdentityErr != nil {
		return "", "", d.IdentityErr
	}
	name, market := "", ""
	if d.ExchangeName != "" || d.MarketType != "" {
		if d.ExchangeName == "" || d.MarketType == "" {
			return "", "", fmt.Errorf("runtime data identity requires both exchange name and market")
		}
		name, market = d.ExchangeName, d.MarketType
	} else if d.Core != nil {
		name, market = d.Core.ExgName, d.Core.Market
		if (name == "") != (market == "") {
			return "", "", fmt.Errorf("runtime core identity is incomplete")
		}
	}
	if d.Exchange != nil {
		info, err := runtimeExchangeInfo(d.Exchange)
		if err != nil {
			return "", "", err
		}
		if info == nil || info.ID == "" || info.MarketType == "" {
			return "", "", fmt.Errorf("runtime adapter identity metadata is incomplete")
		}
		if (name != "" && name != info.ID) || (market != "" && market != info.MarketType) {
			return "", "", fmt.Errorf("runtime identity %q/%q does not match adapter %q/%q",
				name, market, info.ID, info.MarketType)
		}
		if name == "" {
			name = info.ID
		}
		if market == "" {
			market = info.MarketType
		}
	}
	if name == "" || market == "" {
		return "", "", fmt.Errorf("runtime data identity is incomplete")
	}
	return name, market, nil
}

func runtimeExchangeInfo(exchange banexg.BanExchange) (info *banexg.ExgInfo, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			info = nil
			err = fmt.Errorf("adapter Info panicked: %v", recovered)
		}
	}()
	if exchange == nil {
		return nil, fmt.Errorf("adapter is nil")
	}
	info = exchange.Info()
	if info == nil {
		return nil, fmt.Errorf("adapter Info returned nil")
	}
	return info, nil
}

func (d *RuntimeDeps) exchange() banexg.BanExchange {
	if d != nil && d.Exchange != nil {
		return d.Exchange
	}
	return nil
}

func (d *RuntimeDeps) isBacktest() bool {
	if d != nil {
		return d.Core != nil && d.Core.BackTestMode
	}
	return core.BackTestMode
}

func (d *RuntimeDeps) isLive() bool {
	if d != nil {
		return d.Core != nil && d.Core.LiveMode
	}
	return core.LiveMode
}

func (d *RuntimeDeps) numTACache() int {
	if d != nil && d.Core != nil && d.Core.NumTaCache > 0 {
		return d.Core.NumTaCache
	}
	if d != nil {
		return 1500
	}
	return core.NumTaCache
}

func (d *RuntimeDeps) setTimeMS(timeMS int64) {
	if d != nil && d.Clock != nil {
		d.Clock.SetTimeMS(timeMS)
	}
}

func (d *RuntimeDeps) sleep(delay time.Duration) bool {
	if d != nil && d.Core != nil {
		return d.Core.Sleep(delay)
	}
	time.Sleep(delay)
	return true
}

// KlineOptions snapshots the low-frequency K-line policy for ORM download
// helpers. The returned value is concrete and can be passed through hot
// callback setup without a dynamic lookup or context.Value access.
func (d *RuntimeDeps) KlineOptions() orm.KlineRuntimeOptions {
	if d == nil {
		return orm.LegacyKlineRuntimeOptions()
	}
	return orm.NewKlineRuntimeOptions(d.Core, d.configView(), d.timeMS(), d.storage())
}

func (d *RuntimeDeps) context() context.Context {
	if d != nil && d.Core != nil {
		if ctx := d.Core.Context(); ctx != nil {
			return ctx
		}
	}
	return context.Background()
}

func (d *RuntimeDeps) dataDir() string {
	if d != nil && d.Config != nil {
		return d.Config.DataDir
	}
	return ""
}

func (d *RuntimeDeps) spiderAddr() string {
	if cfg := d.configView(); cfg != nil {
		return cfg.SpiderAddr
	}
	return ""
}

func (d *RuntimeDeps) preFire() float64 {
	if cfg := d.configView(); cfg != nil {
		return cfg.PreFire
	}
	return 0
}

func (d *RuntimeDeps) coverage(symbol string) *config.HistoricalCoverageConfig {
	cfg := d.configView()
	if cfg == nil || cfg.HistoricalCoverage == nil {
		return nil
	}
	coverage := cfg.HistoricalCoverage
	result := &config.HistoricalCoverageConfig{
		BaselineEndMS:         coverage.BaselineEndMS,
		HistoricalResultEndMS: coverage.HistoricalResultEndMS,
	}
	if timeframes := coverage.Bars[symbol]; len(timeframes) > 0 {
		result.Bars = map[string]map[string][]config.HistoricalCoverageRange{symbol: timeframes}
	}
	if coverage.PhysicalBars != nil {
		result.PhysicalBars = map[string]map[string][]config.HistoricalCoverageRange{
			symbol: coverage.PhysicalBars[symbol],
		}
	}
	if coverage.ListingPrefixes != nil {
		result.ListingPrefixes = map[string]map[string][]config.HistoricalCoverageRange{
			symbol: coverage.ListingPrefixes[symbol],
		}
	}
	return result
}

func (d *RuntimeDeps) priceState() *com.PriceState {
	if d != nil && d.Market != nil {
		return d.Market.Prices
	}
	return nil
}

func (d *RuntimeDeps) pairCopiedState() *com.PairCopiedState {
	if d != nil && d.Market != nil {
		return d.Market.PairCopied
	}
	return nil
}
