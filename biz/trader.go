package biz

import (
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/banbox/banbot/com"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	ta "github.com/banbox/banta"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
)

type Trader struct {
	// Keep Trader copyable: callers pass it by value. The pointer is accessed
	// atomically so a zero-value Trader can still lazily initialize its queue.
	batchState unsafe.Pointer
	runtime    *RuntimeDeps
}

// RuntimeDeps is the mutable runtime state consumed directly by Trader.
// Each explicit trader owns its strategy, order, trading, market, and clock
// state; legacy package facades are used only by traders without RuntimeDeps.
type RuntimeDeps struct {
	Core       *core.State
	Clock      *btime.ClockState
	Market     *com.MarketState
	Batch      *strat.BatchState
	Strategies *strat.State
	Orders     *ormo.OrderState
	Trading    *TradingState
	Config     *config.Snapshot
	// Accounts is the immutable, execution-facing account map. For non-real
	// runs it is normalized to DefaultAccount once at construction, matching
	// the historical single-account semantics without a hot-path global read.
	Accounts       map[string]*config.AccountConfig
	Symbols        *orm.SymbolState
	Storage        *orm.Storage
	Exchange       banexg.BanExchange
	Scheduler      com.Scheduler
	Notifications  *rpc.Session
	Dump           *orm.DumpSink
	DefaultAccount string
}

// ConfigView returns the immutable configuration owned by this runtime. A
// nil result is intentional for an explicitly constructed runtime without a
// configuration; callers must not substitute the process-wide config there.
func (d *RuntimeDeps) ConfigView() *config.Config {
	if d == nil || d.Config == nil {
		return nil
	}
	return d.Config.View()
}

// StrictBacktest reports the execution policy bound to this runtime. The
// legacy facade remains available only when no runtime dependencies exist.
func (d *RuntimeDeps) StrictBacktest() bool {
	if d == nil {
		return config.StrictBacktest()
	}
	cfg := d.ConfigView()
	return d.Core != nil && d.Core.BackTestMode && cfg != nil && cfg.BTStrict
}

// StrictHistoricalReplay reports the legacy order-metric compatibility mode
// using only this runtime's configuration snapshot.
func (d *RuntimeDeps) StrictHistoricalReplay() bool {
	if d == nil {
		return config.StrictHistoricalReplay(config.HistoricalCoverage)
	}
	cfg := d.ConfigView()
	return d.StrictBacktest() && cfg != nil && cfg.BTNoKlineDownload && cfg.HistoricalCoverage != nil
}

// AccountConfigs returns the immutable account configuration owned by this
// runtime. A nil result is intentional when an explicit runtime has no
// configuration; callers must not fall back to config.Accounts in that case.
func (d *RuntimeDeps) AccountConfigs() map[string]*config.AccountConfig {
	if d == nil || d.Config == nil {
		if d != nil {
			return d.Accounts
		}
		return nil
	}
	if d.Accounts != nil {
		return d.Accounts
	}
	cfg := d.Config.View()
	if cfg == nil {
		return nil
	}
	return cfg.Accounts
}

type parallelError struct {
	key string
	err *errs.Error
}

func selectParallelError(errCh <-chan parallelError) *errs.Error {
	var selected parallelError
	for result := range errCh {
		if result.err != nil && (selected.err == nil || result.key < selected.key) {
			selected = result
		}
	}
	return selected.err
}

func NewTrader(batchState *strat.BatchState) Trader {
	if batchState == nil {
		batchState = strat.NewBatchState()
	}
	return Trader{batchState: unsafe.Pointer(batchState)}
}

// NewTraderWithRuntimeDeps creates an isolated typed-state trader. Missing
// dependencies get private state and never fall back to package facades.
func NewTraderWithRuntimeDeps(deps RuntimeDeps) Trader {
	if deps.Core == nil {
		var err *errs.Error
		deps.Core, err = core.NewState(nil)
		if err != nil {
			panic(err)
		}
	}
	if deps.Clock == nil {
		deps.Clock = btime.NewClockState(deps.Core.BackTestMode, nil)
	}
	if deps.Market == nil {
		deps.Market = com.NewMarketStateWithExchange(deps.Core.ExgName, deps.Exchange)
	} else if deps.Market.Prices == nil {
		deps.Market.Prices = com.NewPriceStateWithExchange(deps.Core.ExgName, deps.Exchange)
	}
	if deps.Market.PairCopied == nil {
		deps.Market.PairCopied = com.NewPairCopiedState()
	}
	if deps.Batch == nil {
		deps.Batch = strat.NewBatchState()
	}
	if deps.Strategies == nil || strat.IsLegacyState(deps.Strategies) {
		deps.Strategies = strat.NewState()
	}
	if deps.Orders == nil || deps.Orders == ormo.LegacyState() {
		deps.Orders = ormo.NewOrderState()
	}
	deps.Orders.BindCore(deps.Core)
	deps.Orders.BindRuntime(deps.Clock, deps.Market.Prices, deps.Exchange, deps.ConfigView())
	if cfg := deps.ConfigView(); cfg != nil {
		if deps.Config.DataDir != "" {
			deps.Orders.BindTradesPath(filepath.Join(deps.Config.DataDir, fmt.Sprintf("orders_%s.db", cfg.Name)))
		}
		deps.Orders.BindExecutionOptions(ormo.ExecutionOptions{
			StrictBacktest:     deps.StrictBacktest(),
			LegacyOrderMetrics: deps.StrictHistoricalReplay() && cfg.BTLegacyOrderMetrics,
		})
	}
	if deps.Trading == nil {
		deps.Trading = NewTradingState()
	} else {
		deps.Trading.ensure()
	}
	if deps.DefaultAccount == "" && !deps.Core.EnvReal {
		deps.DefaultAccount = "default"
	}
	deps.Accounts = normalizeRuntimeAccounts(deps)
	deps.Strategies.BindRuntime(deps.Core, deps.Clock, deps.ConfigView(), deps.Symbols, deps.Exchange)
	for account := range deps.Accounts {
		// Explicit strategy states start empty. Seed their account registries at
		// construction time so the loader can populate them without consulting
		// the legacy package maps or allocating on the bar-processing path.
		deps.Strategies.Jobs(account)
		deps.Strategies.InfoJobs(account)
	}
	return Trader{batchState: unsafe.Pointer(deps.Batch), runtime: &deps}
}

func normalizeRuntimeAccounts(deps RuntimeDeps) map[string]*config.AccountConfig {
	if deps.Accounts != nil {
		return deps.Accounts
	}
	accounts := map[string]*config.AccountConfig(nil)
	if deps.Config != nil {
		if cfg := deps.Config.View(); cfg != nil {
			accounts = cfg.Accounts
		}
	}
	if deps.Core == nil || deps.Core.EnvReal {
		return accounts
	}
	account := deps.DefaultAccount
	if account == "" {
		account = "default"
	}
	if len(accounts) == 0 {
		return map[string]*config.AccountConfig{account: {}}
	}
	if selected, ok := accounts[account]; ok {
		return map[string]*config.AccountConfig{account: selected}
	}
	// Config files may name credentials for a live account while backtests use
	// the historical default key. Pick the same deterministic first account
	// that the legacy config initializer uses when no explicit default exists.
	names := make([]string, 0, len(accounts))
	for name, selected := range accounts {
		if selected != nil {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	if len(names) == 0 {
		return map[string]*config.AccountConfig{account: {}}
	}
	return map[string]*config.AccountConfig{account: accounts[names[0]]}
}

// RuntimeDependencies reports the explicit dependencies, or nil for legacy
// and zero-value traders.
func (t *Trader) RuntimeDependencies() *RuntimeDeps {
	if t == nil {
		return nil
	}
	return t.runtime
}

func (t *Trader) strategyState() *strat.State {
	if t == nil || t.runtime == nil {
		return strat.LegacyState()
	}
	if t.runtime.Strategies == nil {
		return nil
	}
	return t.runtime.Strategies
}

func (t *Trader) accountName(account string) string {
	if t != nil && t.runtime != nil {
		if t.runtime.Core != nil && !t.runtime.Core.EnvReal && t.runtime.DefaultAccount != "" {
			return t.runtime.DefaultAccount
		}
	}
	return account
}

func (t *Trader) orderManager(account string) IOrderMgr {
	if t != nil && t.runtime != nil && t.runtime.Trading != nil {
		return t.runtime.Trading.OrderManager(t.accountName(account))
	}
	return GetOdMgr(account)
}

func (t *Trader) openOrders(account string) (map[int64]*ormo.InOutOrder, *deadlock.Mutex) {
	if t != nil && t.runtime != nil && t.runtime.Orders != nil {
		return t.runtime.Orders.GetOpenODs(t.accountName(account))
	}
	return ormo.GetOpenODs(account)
}

func (t *Trader) allOrderManagers() map[string]IOrderMgr {
	if t != nil && t.runtime != nil && t.runtime.Trading != nil {
		result := make(map[string]IOrderMgr, len(t.runtime.Trading.OrderManagers))
		for account, manager := range t.runtime.Trading.OrderManagers {
			result[account] = manager
		}
		return result
	}
	return GetAllOdMgr()
}

func (t *Trader) BatchState() *strat.BatchState {
	if t == nil {
		return nil
	}
	if state := (*strat.BatchState)(atomic.LoadPointer(&t.batchState)); state != nil {
		return state
	}
	state := strat.NewBatchState()
	if atomic.CompareAndSwapPointer(&t.batchState, nil, unsafe.Pointer(state)) {
		return state
	}
	return (*strat.BatchState)(atomic.LoadPointer(&t.batchState))
}

func (t *Trader) coreState() *core.State {
	if t != nil && t.runtime != nil {
		return t.runtime.Core
	}
	return nil
}

func (t *Trader) clockState() *btime.ClockState {
	if t != nil && t.runtime != nil {
		return t.runtime.Clock
	}
	return nil
}

func (t *Trader) marketState() *com.MarketState {
	if t != nil && t.runtime != nil {
		return t.runtime.Market
	}
	return nil
}

func (t *Trader) bindJobRuntime(job *strat.StratJob) {
	if job == nil || t == nil || t.runtime == nil {
		return
	}
	var prices *com.PriceState
	if t.runtime.Market != nil {
		prices = t.runtime.Market.Prices
	}
	job.BindRuntimeMarket(prices, t.runtime.Clock)
}

func (t *Trader) TimeMS() int64 {
	if clock := t.clockState(); clock != nil {
		return clock.TimeMS()
	}
	return btime.TimeMS()
}

func (t *Trader) SetTimeMS(timeMS int64) {
	if clock := t.clockState(); clock != nil {
		clock.SetTimeMS(timeMS)
		return
	}
	btime.SetTimeMS(timeMS)
}

func (t *Trader) liveMode() bool {
	if state := t.coreState(); state != nil {
		return state.LiveMode
	}
	return core.LiveMode
}

func (t *Trader) parallelOnBar() bool {
	if state := t.coreState(); state != nil {
		return state.ParallelOnBar
	}
	return core.ParallelOnBar
}

func (t *Trader) setBotRunning(running bool) {
	if state := t.coreState(); state != nil {
		state.BotRunning = running
		return
	}
	core.BotRunning = running
}

// ResolveDataSeriesSymbol resolves a series for this trader. Typed traders
// never consult the legacy symbol catalog; legacy traders retain the old
// resolver for compatibility.
func (t *Trader) ResolveDataSeriesSymbol(evt *orm.DataSeries) (*orm.ExSymbol, *errs.Error) {
	if evt == nil {
		return nil, nil
	}
	if t == nil || t.runtime == nil {
		return orm.ResolveSeriesExSymbol(evt), nil
	}
	deps := t.runtime
	if deps.Symbols == nil && evt.ExSymbol == nil {
		return nil, invalidRuntimeSeriesSymbol(evt)
	}
	exs := orm.ResolveSeriesExSymbolWithSymbolState(deps.Symbols, evt)
	if exs == nil {
		return nil, invalidRuntimeSeriesSymbol(evt)
	}

	// A typed SID is meaningful only inside the supplied symbol state. When an
	// event carries a symbol too, compare its logical identity before replacing
	// it with the state-owned canonical value.
	sid := evt.Sid
	if sid == 0 {
		sid = exs.ID
	}
	if sid > 0 {
		if deps.Symbols == nil {
			return nil, invalidRuntimeSeriesSymbol(evt)
		}
		owned := deps.Symbols.GetSymbolByID(sid)
		if owned == nil || !sameRuntimeSymbol(owned, exs) {
			return nil, invalidRuntimeSeriesSymbol(evt)
		}
		exs = owned
	}
	if deps.Symbols == nil && exs.ID > 0 {
		return nil, invalidRuntimeSeriesSymbol(evt)
	}
	if exs.Exchange != "" || exs.Market != "" {
		if exs.Exchange == "" || exs.Market == "" || deps.Symbols == nil ||
			deps.Symbols.GetExSymbol2(exs.Exchange, exs.Market, exs.Symbol) == nil {
			return nil, invalidRuntimeSeriesSymbol(evt)
		}
	}
	if state := t.coreState(); state != nil {
		if state.ExgName != "" && exs.Exchange != "" && state.ExgName != exs.Exchange ||
			state.Market != "" && exs.Market != "" && state.Market != exs.Market {
			return nil, invalidRuntimeSeriesSymbol(evt)
		}
	}
	evt.ExSymbol = exs
	if evt.Sid == 0 && exs.ID > 0 {
		evt.Sid = exs.ID
	}
	return exs, nil
}

func invalidRuntimeSeriesSymbol(evt *orm.DataSeries) *errs.Error {
	return errs.NewMsg(core.ErrInvalidSymbol, "series sid %d is not valid for runtime symbol state", evt.Sid)
}

func sameRuntimeSymbol(owned, actual *orm.ExSymbol) bool {
	if owned == nil || actual == nil || (actual.ID > 0 && owned.ID != actual.ID) || owned.Symbol != actual.Symbol {
		return false
	}
	if actual.Exchange != "" && actual.Exchange != owned.Exchange {
		return false
	}
	return actual.Market == "" || actual.Market == owned.Market
}

func (t *Trader) seriesSymbol(evt *orm.DataSeries) string {
	if evt == nil {
		return ""
	}
	if t != nil && t.runtime != nil {
		if evt.ExSymbol == nil {
			return ""
		}
		return evt.ExSymbol.Symbol
	}
	return evt.Symbol()
}

func (t *Trader) OnEnvSeries(evt *orm.DataSeries) (*ta.BarEnv, *errs.Error) {
	if evt == nil {
		return nil, nil
	}
	exs, errSymbol := t.ResolveDataSeriesSymbol(evt)
	if errSymbol != nil {
		return nil, errSymbol
	}
	if exs == nil {
		return nil, errs.NewMsg(core.ErrInvalidSymbol, "series sid %d not found", evt.Sid)
	}
	symbol := exs.Symbol
	envKey := strings.Join([]string{symbol, evt.TimeFrame}, "_")
	strategyState := t.strategyState()
	if strategyState == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "runtime strategy state is required for data series")
	}
	env, ok := strategyState.Env(envKey)
	if !ok {
		// 额外订阅1h没有对应的env，无需处理
		return nil, nil
	}
	if t.liveMode() {
		if env.TimeStop > evt.TimeMS {
			// This bar has expired, ignore it, the crawler may push the processed expired bar when starting
			return nil, nil
		} else if env.TimeStop > 0 && env.TimeStop < evt.TimeMS {
			lackNum := int(math.Round(float64(evt.TimeMS-env.TimeStop) / float64(env.TFMSecs)))
			if lackNum > 0 {
				log.Warn("taEnv bar lack", zap.Int("num", lackNum), zap.String("env", envKey))
			}
		}
	}
	openVal, err := evt.OpenValue()
	if err != nil {
		return nil, errs.New(core.ErrInvalidBars, err)
	}
	highVal, err := evt.HighValue()
	if err != nil {
		return nil, errs.New(core.ErrInvalidBars, err)
	}
	lowVal, err := evt.LowValue()
	if err != nil {
		return nil, errs.New(core.ErrInvalidBars, err)
	}
	closeVal, err := evt.CloseValue()
	if err != nil {
		return nil, errs.New(core.ErrInvalidBars, err)
	}
	volumeVal, err := evt.VolumeValue()
	if err != nil {
		return nil, errs.New(core.ErrInvalidBars, err)
	}
	err = env.OnBar(evt.TimeMS, openVal, highVal, lowVal, closeVal, volumeVal, evt.QuoteValue(), evt.BuyVolumeValue(), evt.TradeNumValue())
	if err != nil {
		return nil, errs.New(errs.CodeRunTime, err)
	}
	return env, nil
}

func (t *Trader) FeedDataSeries(evt *orm.DataSeries) *errs.Error {
	if evt == nil {
		return nil
	}
	var exs *orm.ExSymbol
	var errSymbol *errs.Error
	if t != nil && t.runtime != nil {
		exs, errSymbol = t.ResolveDataSeriesSymbol(evt)
	} else {
		exs = evt.EnsureExSymbol()
	}
	if errSymbol != nil {
		return errSymbol
	}
	if exs == nil && orm.NormalizeSeriesSource(evt.Source) == orm.SeriesSourceKline {
		return errs.NewMsg(core.ErrInvalidSymbol, "series sid %d not found", evt.Sid)
	}
	if t != nil && t.runtime != nil && exs == nil {
		return invalidRuntimeSeriesSymbol(evt)
	}
	if orm.NormalizeSeriesSource(evt.Source) != orm.SeriesSourceKline || !evt.HasOHLCV() {
		return t.feedDataOnlySeries(evt, exs)
	}
	return t.feedClosedSeries(evt, exs)
}

func (t *Trader) FeedSeries(evt *orm.DataSeries) *errs.Error {
	return t.FeedDataSeries(evt)
}

func (t *Trader) feedDataOnlySeries(evt *orm.DataSeries, resolved ...*orm.ExSymbol) *errs.Error {
	if evt == nil {
		return nil
	}
	strategyState := t.strategyState()
	if strategyState == nil {
		return errs.NewMsg(core.ErrRunTime, "runtime strategy state is required for data series")
	}
	var exs *orm.ExSymbol
	if len(resolved) > 0 {
		exs = resolved[0]
	} else if t != nil && t.runtime != nil {
		var err *errs.Error
		exs, err = t.ResolveDataSeriesSymbol(evt)
		if err != nil {
			return err
		}
	} else {
		exs = evt.EnsureExSymbol()
	}
	subKey := strat.DataSubKey(evt.Source, evt.Sid, evt.TimeFrame)
	dispatched := false
	accounts := executionAccountConfigs(t.runtime)
	for account := range executionAccountNames(t.runtime) {
		cfg := accounts[account]
		if cfg == nil || cfg.NoTrade {
			continue
		}
		dispatched = true
		var jobMap map[string]*strat.StratJob
		if t.runtime != nil {
			jobMap = strategyState.InfoJobs(t.accountName(account))[subKey]
		} else {
			strat.LockJobsRead()
			jobMap, _ = strat.GetInfoJobs(account)[subKey]
			strat.UnlockJobsRead()
		}
		deliverDataOnlySeries(jobMap, evt, exs, t.runtime)
	}
	if !dispatched {
		var jobMap map[string]*strat.StratJob
		if t.runtime != nil {
			jobMap = strategyState.InfoJobs(t.runtime.DefaultAccount)[subKey]
		} else {
			strat.LockJobsRead()
			jobMap, _ = strat.GetInfoJobs(config.DefAcc)[subKey]
			strat.UnlockJobsRead()
		}
		deliverDataOnlySeries(jobMap, evt, exs, t.runtime)
	}
	return nil
}

func deliverDataOnlySeries(jobMap map[string]*strat.StratJob, evt *orm.DataSeries, exs *orm.ExSymbol, deps *RuntimeDeps) {
	for job := range executionStratJobs(jobMap, deps) {
		if job.Strat.OnData == nil {
			continue
		}
		fields := job.SetData(evt)
		job.IsWarmUp = evt.IsWarmUp
		num1, num2 := strat.GetJobInOutNum(job)
		job.Strat.OnData(job, strat.DataEvent{
			DataFields: fields,
			Role:       strat.DataRoleCustom,
			Symbol:     exs,
		})
		strat.CheckJobInOutNum(job, "OnData", num1, num2)
	}
}

func (t *Trader) feedClosedSeries(evt *orm.DataSeries, resolved ...*orm.ExSymbol) *errs.Error {
	var exs *orm.ExSymbol
	if len(resolved) > 0 {
		exs = resolved[0]
	} else if t != nil && t.runtime != nil {
		var err *errs.Error
		exs, err = t.ResolveDataSeriesSymbol(evt)
		if err != nil {
			return err
		}
	} else {
		exs = evt.EnsureExSymbol()
	}
	if exs == nil {
		return errs.NewMsg(core.ErrInvalidSymbol, "series sid %d not found", evt.Sid)
	}
	symbol := exs.Symbol
	closeVal, closeErr := evt.CloseValue()
	if closeErr != nil {
		return errs.New(core.ErrInvalidBars, closeErr)
	}
	state := t.coreState()
	var odMatch bool
	if state == nil {
		core.LockOdMatch.RLock()
		_, odMatch = core.OrderMatchTfs[evt.TimeFrame]
		core.LockOdMatch.RUnlock()
	} else {
		state.LockOdMatch.RLock()
		_, odMatch = state.OrderMatchTfs[evt.TimeFrame]
		state.LockOdMatch.RUnlock()
	}
	var accOrders map[string][]*ormo.InOutOrder
	if market := t.marketState(); market != nil {
		if market.Prices != nil {
			market.Prices.SetBarPriceAt(t.TimeMS(), symbol, closeVal)
		}
	} else {
		com.SetBarPrice(symbol, closeVal)
	}
	if odMatch && !evt.IsWarmUp {
		accOrders = make(map[string][]*ormo.InOutOrder)
		accounts := executionAccountConfigs(t.runtime)
		for account := range executionAccountNames(t.runtime) {
			cfg := accounts[account]
			if cfg == nil || cfg.NoTrade {
				continue
			}
			openOds, lock := t.openOrders(account)
			lock.Lock()
			allOrders := executionOpenOrders(openOds, t.runtime)
			lock.Unlock()
			odMgr := t.orderManager(account)
			if len(allOrders) > 0 {
				if updateErr := odMgr.UpdateByDataSeries(allOrders, evt); updateErr != nil {
					return updateErr
				}
				newOpens := make([]*ormo.InOutOrder, 0, len(allOrders))
				for _, od := range allOrders {
					if od.Status < ormo.InOutStatusFullExit {
						newOpens = append(newOpens, od)
					}
				}
				accOrders[account] = newOpens
			}
		}
	}
	env, errEnv := t.OnEnvSeries(evt)
	if errEnv != nil {
		log.Error(fmt.Sprintf("%s/%s OnEnvSeries fail", symbol, evt.TimeFrame), zap.Error(errEnv))
		return errEnv
	} else if env == nil {
		return nil
	}
	tfSecs := utils2.TFToSecs(evt.TimeFrame)
	delaySecs := int((t.TimeMS()-evt.TimeMS)/1000) - tfSecs
	barExpired := delaySecs >= max(60, tfSecs/2)
	if barExpired {
		if t.liveMode() && !evt.IsWarmUp {
			log.Warn(fmt.Sprintf("%s/%s delay %v s, open order disabled for this data series", symbol, evt.TimeFrame, delaySecs))
		} else {
			barExpired = false
		}
	}
	if t.parallelOnBar() && !strictBacktestFor(t.runtime) {
		return t.feedClosedSeriesParallel(evt, env, symbol, odMatch, accOrders, barExpired)
	}
	return t.feedClosedSeriesSerial(evt, env, symbol, odMatch, accOrders, barExpired)
}

func (t *Trader) feedClosedSeriesSerial(evt *orm.DataSeries, env *ta.BarEnv, symbol string,
	odMatch bool, accOrders map[string][]*ormo.InOutOrder, barExpired bool) *errs.Error {
	var runErr *errs.Error
	var accOdArr []string
	accounts := executionAccountConfigs(t.runtime)
	if t.liveMode() {
		accOdArr = make([]string, 0, len(accounts))
	}
	for account := range executionAccountNames(t.runtime) {
		cfg := accounts[account]
		if cfg == nil || cfg.NoTrade {
			continue
		}
		allOrders, _ := accOrders[account]
		if !odMatch {
			openOds, lock := t.openOrders(account)
			lock.Lock()
			allOrders = executionOpenOrders(openOds, t.runtime)
			lock.Unlock()
		}
		var curOrders []*ormo.InOutOrder
		for _, od := range allOrders {
			if od.Status < ormo.InOutStatusFullExit && od.Symbol == symbol && od.Timeframe == evt.TimeFrame {
				curOrders = append(curOrders, od)
			}
		}
		if t.liveMode() && !evt.IsWarmUp {
			accOdArr = append(accOdArr, fmt.Sprintf("%s: %d/%d", account, len(curOrders), len(allOrders)))
		}
		if curErr := t.onAccountDataSeries(account, env, evt, allOrders, barExpired); curErr != nil {
			if runErr != nil {
				log.Error("onAccountDataSeries fail", zap.String("account", account), zap.Error(curErr))
			} else {
				runErr = curErr
			}
		}
	}
	if t.liveMode() && len(accOdArr) > 0 {
		log.Info("OnSeries", zap.String("pair", symbol), zap.String("tf", evt.TimeFrame),
			zap.Strings("accOdNums", accOdArr))
	}
	return runErr
}

func (t *Trader) feedClosedSeriesParallel(evt *orm.DataSeries, env *ta.BarEnv, symbol string,
	odMatch bool, accOrders map[string][]*ormo.InOutOrder, barExpired bool) *errs.Error {
	var accOdArr []string
	accounts := executionAccountConfigs(t.runtime)
	if t.liveMode() {
		accOdArr = make([]string, 0, len(accounts))
	}
	errCh := make(chan parallelError, len(accounts))
	var wg sync.WaitGroup
	for account := range executionAccountNames(t.runtime) {
		cfg := accounts[account]
		if cfg == nil || cfg.NoTrade {
			continue
		}
		allOrders, _ := accOrders[account]
		if !odMatch {
			openOds, lock := t.openOrders(account)
			lock.Lock()
			allOrders = executionOpenOrders(openOds, t.runtime)
			lock.Unlock()
		}
		var curOrders []*ormo.InOutOrder
		for _, od := range allOrders {
			if od.Status < ormo.InOutStatusFullExit && od.Symbol == symbol && od.Timeframe == evt.TimeFrame {
				curOrders = append(curOrders, od)
			}
		}
		if t.liveMode() && !evt.IsWarmUp {
			accOdArr = append(accOdArr, fmt.Sprintf("%s: %d/%d", account, len(curOrders), len(allOrders)))
		}
		wg.Add(1)
		go func(acc string, ods []*ormo.InOutOrder) {
			defer wg.Done()
			if err := t.onAccountDataSeries(acc, env, evt, ods, barExpired); err != nil {
				errCh <- parallelError{key: acc, err: err}
			}
		}(account, allOrders)
	}
	wg.Wait()
	close(errCh)
	if err := selectParallelError(errCh); err != nil {
		return err
	}
	if t.liveMode() && len(accOdArr) > 0 {
		log.Info("OnSeries", zap.String("pair", symbol), zap.String("tf", evt.TimeFrame),
			zap.Strings("accOdNums", accOdArr))
	}
	return nil
}

func (t *Trader) onAccountDataSeries(account string, env *ta.BarEnv, evt *orm.DataSeries, curOrders []*ormo.InOutOrder, barExpired bool) *errs.Error {
	if t.parallelOnBar() && !strictBacktestFor(t.runtime) {
		return t.onAccountDataSeriesParallel(account, env, evt, curOrders, barExpired)
	}
	return t.onAccountDataSeriesSerial(account, env, evt, curOrders, barExpired)
}

func (t *Trader) onAccountDataSeriesSerial(account string, env *ta.BarEnv, evt *orm.DataSeries,
	curOrders []*ormo.InOutOrder, barExpired bool) *errs.Error {
	symbol := t.seriesSymbol(evt)
	envKey := symbol + "_" + evt.TimeFrame
	var jobs map[string]*strat.StratJob
	var infoJobMap map[string]map[string]*strat.StratJob
	strategyState := t.strategyState()
	if strategyState == nil {
		return errs.NewMsg(core.ErrRunTime, "runtime strategy state is required for data series")
	}
	if t.runtime != nil {
		jobs = strategyState.Jobs(t.accountName(account))[envKey]
		infoJobMap = strategyState.InfoJobs(t.accountName(account))
	} else {
		strat.LockJobsRead()
		jobs, _ = strat.GetJobs(account)[envKey]
		infoJobMap = strat.GetInfoJobs(account)
	}
	var infoJobs map[string]*strat.StratJob
	if len(infoJobMap) > 0 {
		infoJobs = infoJobMap[strat.DataSubKey(evt.Source, evt.Sid, evt.TimeFrame)]
	}
	if t.runtime == nil {
		strat.UnlockJobsRead()
	}
	if len(jobs) == 0 && len(infoJobs) == 0 {
		return nil
	}
	odMgr := t.orderManager(account)
	isWarmup := evt.IsWarmUp
	var handledJobs map[*strat.StratJob]bool
	if len(infoJobs) > 0 {
		handledJobs = make(map[*strat.StratJob]bool, len(jobs))
	}
	for job := range executionStratJobs(jobs, t.runtime) {
		if handledJobs != nil {
			handledJobs[job] = true
		}
		var fields *strat.DataFields
		if job.Strat.OnData != nil {
			fields = job.SetData(evt)
		}
		job.IsWarmUp = isWarmup
		job.InitBar(curOrders)
		if err := t.onAccountDataSeriesJob(odMgr, job, evt, fields, barExpired); err != nil {
			return err
		}
	}
	return t.onAccountInfoSeries(account, env, evt, infoJobs, handledJobs)
}

func (t *Trader) onAccountInfoSeries(account string, env *ta.BarEnv, evt *orm.DataSeries,
	infoJobs map[string]*strat.StratJob, handledJobs map[*strat.StratJob]bool) *errs.Error {
	symbol := t.seriesSymbol(evt)
	isWarmup := evt.IsWarmUp
	for job := range executionStratJobs(infoJobs, t.runtime) {
		t.bindJobRuntime(job)
		if handledJobs[job] {
			continue
		}
		fields := job.SetData(evt)
		job.IsWarmUp = isWarmup
		num1, num2 := strat.GetJobInOutNum(job)
		if job.Strat.OnData != nil {
			job.Strat.OnData(job, strat.DataEvent{
				DataFields: fields,
				Role:       strat.DataRoleInfo,
				Symbol:     evt.ExSymbol,
			})
			strat.CheckJobInOutNum(job, "OnData", num1, num2)
		} else if job.Strat.OnInfoBar != nil {
			job.Strat.OnInfoBar(job, env, symbol, evt.TimeFrame)
			strat.CheckJobInOutNum(job, "OnInfoBar", num1, num2)
		}
		if job.Strat.BatchInfo && job.Strat.OnBatchInfos != nil {
			AddBatchJobWithRuntimeDeps(t.runtime, t.BatchState(), account, evt.TimeFrame, job, env)
		}
	}
	if env.VNum > 1000 && !isWarmup {
		keyAt := "first_hit_at"
		keyNum := "first_hit_vnum"
		if cacheVal, ok := env.Data.Load(keyAt); ok {
			firstAt, _ := cacheVal.(int)
			firstNumVal, _ := env.Data.Load(keyNum)
			firstNum, _ := firstNumVal.(int)
			if env.BarNum-firstAt > 10 {
				if env.VNum-firstNum > 0 {
					addNum := env.VNum - firstNum
					addFor := env.BarNum - firstAt
					errMsg := "series too many (total %v), new add %v in %v bars, try replace `NewSeries` with `Series.To`"
					t.setBotRunning(false)
					return errs.NewMsg(errs.CodeRunTime, errMsg, env.VNum, addNum, addFor)
				}
			}
		} else {
			env.Data.Store(keyAt, env.BarNum)
			env.Data.Store(keyNum, env.VNum)
		}
	}
	return nil
}

func (t *Trader) onAccountDataSeriesParallel(account string, env *ta.BarEnv, evt *orm.DataSeries, curOrders []*ormo.InOutOrder, barExpired bool) *errs.Error {
	symbol := t.seriesSymbol(evt)
	envKey := symbol + "_" + evt.TimeFrame
	var jobs map[string]*strat.StratJob
	var infoJobMap map[string]map[string]*strat.StratJob
	strategyState := t.strategyState()
	if strategyState == nil {
		return errs.NewMsg(core.ErrRunTime, "runtime strategy state is required for data series")
	}
	if t.runtime != nil {
		jobs = strategyState.Jobs(t.accountName(account))[envKey]
		infoJobMap = strategyState.InfoJobs(t.accountName(account))
	} else {
		strat.LockJobsRead()
		jobs, _ = strat.GetJobs(account)[envKey]
		infoJobMap = strat.GetInfoJobs(account)
	}
	var infoJobs map[string]*strat.StratJob
	if len(infoJobMap) > 0 {
		infoJobs = infoJobMap[strat.DataSubKey(evt.Source, evt.Sid, evt.TimeFrame)]
	}
	if t.runtime == nil {
		strat.UnlockJobsRead()
	}
	if len(jobs) == 0 && len(infoJobs) == 0 {
		return nil
	}
	odMgr := t.orderManager(account)
	isWarmup := evt.IsWarmUp
	var wg sync.WaitGroup
	parallelOnBar := t.parallelOnBar() && !strictBacktestFor(t.runtime)
	jobKeys := make([]string, 0, len(jobs))
	for key := range jobs {
		jobKeys = append(jobKeys, key)
	}
	if strictBacktestFor(t.runtime) {
		sort.Strings(jobKeys)
	}
	var errCh chan parallelError
	if parallelOnBar {
		errCh = make(chan parallelError, len(jobKeys))
	}
	var handledJobs map[*strat.StratJob]bool
	if len(infoJobs) > 0 {
		handledJobs = make(map[*strat.StratJob]bool, len(jobs))
	}
	for _, jobKey := range jobKeys {
		job := jobs[jobKey]
		if handledJobs != nil {
			handledJobs[job] = true
		}
		var fields *strat.DataFields
		if job.Strat.OnData != nil {
			fields = job.SetData(evt)
		}
		job.IsWarmUp = isWarmup
		job.InitBar(curOrders)
		if !parallelOnBar {
			if err := t.onAccountDataSeriesJob(odMgr, job, evt, fields, barExpired); err != nil {
				return err
			}
		} else {
			wg.Add(1)
			go func(key string, j *strat.StratJob, data *strat.DataFields) {
				defer wg.Done()
				if errCur := t.onAccountDataSeriesJob(odMgr, j, evt, data, barExpired); errCur != nil {
					errCh <- parallelError{key: key, err: errCur}
				}
			}(jobKey, job, fields)
		}
	}
	if parallelOnBar {
		wg.Wait()
		close(errCh)
		if err := selectParallelError(errCh); err != nil {
			return err
		}
	}
	for job := range executionStratJobs(infoJobs, t.runtime) {
		if handledJobs[job] {
			continue
		}
		fields := job.SetData(evt)
		job.IsWarmUp = isWarmup
		num1, num2 := strat.GetJobInOutNum(job)
		if job.Strat.OnData != nil {
			job.Strat.OnData(job, strat.DataEvent{
				DataFields: fields,
				Role:       strat.DataRoleInfo,
				Symbol:     evt.ExSymbol,
			})
			strat.CheckJobInOutNum(job, "OnData", num1, num2)
		} else if job.Strat.OnInfoBar != nil {
			job.Strat.OnInfoBar(job, env, symbol, evt.TimeFrame)
			strat.CheckJobInOutNum(job, "OnInfoBar", num1, num2)
		}
		if job.Strat.BatchInfo && job.Strat.OnBatchInfos != nil {
			AddBatchJobWithRuntimeDeps(t.runtime, t.BatchState(), account, evt.TimeFrame, job, env)
		}
	}
	if env.VNum > 1000 && !isWarmup {
		keyAt := "first_hit_at"
		keyNum := "first_hit_vnum"
		if cacheVal, ok := env.Data.Load(keyAt); ok {
			firstAt, _ := cacheVal.(int)
			firstNumVal, _ := env.Data.Load(keyNum)
			firstNum, _ := firstNumVal.(int)
			if env.BarNum-firstAt > 10 {
				if env.VNum-firstNum > 0 {
					addNum := env.VNum - firstNum
					addFor := env.BarNum - firstAt
					errMsg := "series too many (total %v), new add %v in %v bars, try replace `NewSeries` with `Series.To`"
					t.setBotRunning(false)
					return errs.NewMsg(errs.CodeRunTime, errMsg, env.VNum, addNum, addFor)
				}
			}
		} else {
			env.Data.Store(keyAt, env.BarNum)
			env.Data.Store(keyNum, env.VNum)
		}
	}
	return nil
}

func (t *Trader) onAccountDataSeriesJob(odMgr IOrderMgr, job *strat.StratJob, evt *orm.DataSeries, fields *strat.DataFields, barExpired bool) *errs.Error {
	t.bindJobRuntime(job)
	account := job.Account
	if job.Strat.OnData != nil {
		job.Strat.OnData(job, strat.DataEvent{
			DataFields: fields,
			Role:       strat.DataRoleMain,
			Symbol:     job.Symbol,
		})
	} else if job.Strat.OnBar != nil {
		job.Strat.OnBar(job)
	}
	isWarmup := job.IsWarmUp
	isBatch := job.Strat.BatchInOut && job.Strat.OnBatchJobs != nil
	if !barExpired {
		if isBatch {
			AddBatchJobWithRuntimeDeps(t.runtime, t.BatchState(), account, evt.TimeFrame, job, nil)
		}
	} else {
		entryNum := len(job.Entrys)
		if t.liveMode() && !isWarmup && entryNum > 0 {
			log.Info("skip open orders by bar expired", zap.String("acc", account),
				zap.String("pair", t.seriesSymbol(evt)), zap.String("tf", evt.TimeFrame),
				zap.Int("num", entryNum))
			if t.runtime != nil {
				if strategyState := t.strategyState(); strategyState != nil {
					strategyState.AddAccFailOpens(account, strat.FailOpenBarTooLate, entryNum)
				}
			} else {
				strat.AddAccFailOpens(account, strat.FailOpenBarTooLate, entryNum)
			}
			job.Entrys = nil
		}
	}
	if !isWarmup {
		err := strat.CheckCustomExits(job)
		if err != nil {
			return err
		}
		_, _, err = odMgr.ProcessOrders(job)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Trader) OnEnvEnd(evt *orm.DataSeries) {
	symbol := ""
	if evt != nil {
		exs, err := t.ResolveDataSeriesSymbol(evt)
		if err != nil {
			log.Warn("resolve series symbol on env end fail", zap.Int32("sid", evt.Sid), zap.Error(err))
			return
		}
		if exs == nil {
			log.Warn("series symbol missing on env end", zap.Int32("sid", evt.Sid))
			return
		}
		symbol = exs.Symbol
	}
	mgrs := t.allOrderManagers()
	for acc := range executionMapKeys(mgrs) {
		mgr := mgrs[acc]
		err := mgr.OnEnvEnd(evt)
		if err != nil {
			log.Warn("close orders on env end fail", zap.String("acc", acc), zap.Error(err))
		}
	}
	if evt == nil {
		return
	}
	envKey := strings.Join([]string{symbol, evt.TimeFrame}, "_")
	strategyState := t.strategyState()
	if strategyState == nil {
		return
	}
	env, ok := strategyState.Env(envKey)
	if ok {
		env.Reset()
	}
}
