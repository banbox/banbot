package live

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg/utils"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/web"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

type CryptoTrader struct {
	biz.Trader
	dp               *data.LiveProvider
	retiredProviders []*data.LiveProvider
	symbols          *orm.SymbolState
	dataDeps         *data.RuntimeDeps
	scheduler        com.Scheduler
	startup          CryptoTraderStartupFunc
	runtimeCtx       context.Context
	runtime          RuntimeLifecycle
	runLock          sync.Mutex
	runtimeLock      sync.Mutex
	runtimeCount     int
	runtimeCond      *sync.Cond
	runtimeTimers    map[*time.Timer]struct{}
	runtimeStopped   atomic.Bool
	shutdownLock     sync.Mutex
	shutdownJoins    []func()
	shutdownStops    []func()
	shutdownReady    bool
	shutdownStarted  bool
	cleanupDone      bool
	legacyBackup     *biz.VarsBackup
	previousUnWatch  func(map[string][]string)
	providerBindings map[*data.LiveProvider]struct{}
	initFn           func() *errs.Error
	startJobsFn      func()
	loopMainFn       func() *errs.Error
	seriesRuntime    *data.SeriesRuntime
	nowMSFn          func() int64
	collectJobsFn    func() []*strat.StratJob
	runtimeDepsErr   *errs.Error
}

type CryptoTraderStartupFunc func(ctx context.Context, trader *CryptoTrader) error

var webStartAPIWithLifecycle = func(lifecycle RuntimeLifecycle) *errs.Error {
	_, err := web.StartApiWithLifecycleInLegacySession(lifecycle)
	return err
}

// RuntimeLifecycle is the small lifecycle surface a live trader needs.
type RuntimeLifecycle interface {
	Context() context.Context
	OnClose(func())
	OnCloseWait(func())
}

// runtimeShutdownLifecycle keeps component join callbacks in the trader's
// explicit shutdown phase. Runtime.OnCloseWait registration order is otherwise
// coupled to startup timing.
type runtimeShutdownLifecycle struct {
	owner *CryptoTrader
}

func (l *runtimeShutdownLifecycle) Context() context.Context {
	if l == nil || l.owner == nil || l.owner.runtime == nil {
		return nil
	}
	return l.owner.runtime.Context()
}

func (l *runtimeShutdownLifecycle) Exchange() banexg.BanExchange {
	if l == nil || l.owner == nil {
		return nil
	}
	return l.owner.exchangeBinding()
}

func (l *runtimeShutdownLifecycle) WalletRuntimeDeps() biz.RuntimeDeps {
	if l == nil || l.owner == nil {
		return biz.RuntimeDeps{}
	}
	if deps := l.owner.RuntimeDependencies(); deps != nil {
		return *deps
	}
	return biz.RuntimeDeps{}
}

func (l *runtimeShutdownLifecycle) OnClose(call func()) {
	if l == nil || l.owner == nil || l.owner.runtime == nil || call == nil {
		return
	}
	// The callback is registered with both the runtime cancellation phase and
	// the trader's explicit stop phase. Keep one logical callback idempotent so
	// either phase can own execution without double-stopping a component.
	var once sync.Once
	onceCall := func() {
		once.Do(call)
	}
	l.owner.registerShutdownStop(onceCall)
	l.owner.runtime.OnClose(onceCall)
}

func (l *runtimeShutdownLifecycle) OnCloseWait(call func()) {
	if l == nil || l.owner == nil {
		return
	}
	l.owner.registerShutdownJoin(call)
}

func (l *runtimeShutdownLifecycle) EnterCallback() bool {
	if l == nil || l.owner == nil || l.owner.runtime == nil {
		return false
	}
	tracker, ok := l.owner.runtime.(data.CallbackTracker)
	if !ok {
		return true
	}
	return tracker.EnterCallback()
}

func (l *runtimeShutdownLifecycle) LeaveCallback() {
	if l == nil || l.owner == nil || l.owner.runtime == nil {
		return
	}
	if tracker, ok := l.owner.runtime.(data.CallbackTracker); ok {
		tracker.LeaveCallback()
	}
}

func NewCryptoTrader() *CryptoTrader {
	return NewCryptoTraderWithBatchState(strat.NewBatchState())
}

func NewCryptoTraderWith(startup CryptoTraderStartupFunc) *CryptoTrader {
	trader := NewCryptoTraderWithBatchState(strat.NewBatchState())
	trader.startup = startup
	return trader
}

// NewCryptoTraderWithRuntime binds the trader to a runtime lifecycle and batch state.
// A nil lifecycle keeps the constructor's private-state behavior.
func NewCryptoTraderWithRuntime(lifecycle RuntimeLifecycle, batchState *strat.BatchState, startup CryptoTraderStartupFunc) *CryptoTrader {
	return NewCryptoTraderWithRuntimeState(lifecycle, batchState, nil, startup)
}

// NewCryptoTraderWithRuntimeState binds the live trader to explicit runtime
// symbol indexes as well as its lifecycle and batch state.
func NewCryptoTraderWithRuntimeState(lifecycle RuntimeLifecycle, batchState *strat.BatchState, symbols *orm.SymbolState, startup CryptoTraderStartupFunc) *CryptoTrader {
	trader := NewCryptoTraderWithBatchAndSymbolState(batchState, symbols)
	return bindCryptoTraderRuntime(trader, lifecycle, startup)
}

// NewCryptoTraderWithRuntimeDeps binds lifecycle, typed mutable state, and
// symbol indexes to an isolated live trader.
func NewCryptoTraderWithRuntimeDeps(lifecycle RuntimeLifecycle, deps biz.RuntimeDeps, symbols *orm.SymbolState, startup CryptoTraderStartupFunc) *CryptoTrader {
	resolvedSymbols, symbolsErr := resolveCryptoTraderSymbols(deps.Symbols, symbols, nil)
	symbols = resolvedSymbols
	deps.Symbols = symbols
	trader := &CryptoTrader{Trader: biz.NewTraderWithRuntimeDeps(deps), symbols: symbols}
	trader.dataDeps = makeDataRuntimeDeps(trader.RuntimeDependencies(), nil, symbols, callbackTracker(lifecycle))
	trader.runtimeDepsErr = symbolsErr
	if trader.runtimeDepsErr == nil {
		trader.runtimeDepsErr = validateDataRuntimeDeps(trader.RuntimeDependencies(), symbols, nil)
	}
	return bindCryptoTraderRuntime(trader, lifecycle, startup)
}

// NewCryptoTraderWithRuntimeDataDeps is the explicit composition-root entry
// point. The data dependency set is copied so provider state is fixed at the
// constructor boundary and does not fall back to package facades.
func NewCryptoTraderWithRuntimeDataDeps(lifecycle RuntimeLifecycle, deps biz.RuntimeDeps, symbols *orm.SymbolState, startup CryptoTraderStartupFunc, dataDeps *data.RuntimeDeps) *CryptoTrader {
	var suppliedSymbols *orm.SymbolState
	if dataDeps != nil {
		suppliedSymbols = dataDeps.Symbols
	}
	resolvedSymbols, symbolsErr := resolveCryptoTraderSymbols(deps.Symbols, symbols, suppliedSymbols)
	symbols = resolvedSymbols
	deps.Symbols = symbols
	trader := &CryptoTrader{Trader: biz.NewTraderWithRuntimeDeps(deps), symbols: symbols}
	trader.dataDeps = makeDataRuntimeDeps(trader.RuntimeDependencies(), dataDeps, symbols, callbackTracker(lifecycle))
	trader.runtimeDepsErr = symbolsErr
	if trader.runtimeDepsErr == nil {
		trader.runtimeDepsErr = validateDataRuntimeDeps(trader.RuntimeDependencies(), symbols, dataDeps)
	}
	return bindCryptoTraderRuntime(trader, lifecycle, startup)
}

// resolveCryptoTraderSymbols ensures every constructor argument that can own
// the symbol catalog refers to the same runtime instance. The first non-nil
// value is retained; conflicting pointers fail closed instead of being
// silently overwritten by a later dependency set.
func resolveCryptoTraderSymbols(depsSymbols, explicitSymbols, suppliedSymbols *orm.SymbolState) (*orm.SymbolState, *errs.Error) {
	resolved := explicitSymbols
	if depsSymbols != nil {
		if resolved != nil && resolved != depsSymbols {
			return nil, errs.NewMsg(core.ErrRunTime, "trader runtime symbols do not match dependencies")
		}
		resolved = depsSymbols
	}
	if suppliedSymbols != nil {
		if resolved != nil && resolved != suppliedSymbols {
			return nil, errs.NewMsg(core.ErrRunTime, "data runtime symbols do not match trader runtime")
		}
		resolved = suppliedSymbols
	}
	return resolved, nil
}

// validateDataRuntimeDeps rejects a second dependency set that points at a
// different runtime. Catalogs and callback trackers are intentionally omitted:
// they are provider-local extensions, while the state pointers below must be
// the same objects used by Trader.
func validateDataRuntimeDeps(deps *biz.RuntimeDeps, symbols *orm.SymbolState, supplied *data.RuntimeDeps) *errs.Error {
	if deps == nil || supplied == nil {
		return nil
	}
	if supplied.Core != nil && supplied.Core != deps.Core {
		return errs.NewMsg(core.ErrRunTime, "data runtime core does not match trader runtime")
	}
	if supplied.Clock != nil && supplied.Clock != deps.Clock {
		return errs.NewMsg(core.ErrRunTime, "data runtime clock does not match trader runtime")
	}
	if supplied.Config != nil && supplied.Config != deps.Config {
		return errs.NewMsg(core.ErrRunTime, "data runtime config does not match trader runtime")
	}
	if supplied.Market != nil && supplied.Market != deps.Market {
		return errs.NewMsg(core.ErrRunTime, "data runtime market does not match trader runtime")
	}
	if supplied.Symbols != nil && supplied.Symbols != symbols {
		return errs.NewMsg(core.ErrRunTime, "data runtime symbols do not match trader runtime")
	}
	if supplied.Storage != nil && supplied.Storage != deps.Storage {
		return errs.NewMsg(core.ErrRunTime, "data runtime storage does not match trader runtime")
	}
	if supplied.Strategies != nil && supplied.Strategies != deps.Strategies {
		return errs.NewMsg(core.ErrRunTime, "data runtime strategies do not match trader runtime")
	}
	if supplied.Exchange != nil && supplied.Exchange != deps.Exchange {
		return errs.NewMsg(core.ErrRunTime, "data runtime exchange does not match trader runtime")
	}
	if supplied.Dump != nil && supplied.Dump != deps.Dump {
		return errs.NewMsg(core.ErrRunTime, "data runtime dump does not match trader runtime")
	}
	if supplied.ExchangeName != "" && deps.Core != nil && supplied.ExchangeName != deps.Core.ExgName {
		return errs.NewMsg(core.ErrRunTime, "data runtime exchange name does not match trader runtime")
	}
	if supplied.MarketType != "" && deps.Core != nil && supplied.MarketType != deps.Core.Market {
		return errs.NewMsg(core.ErrRunTime, "data runtime market type does not match trader runtime")
	}
	return nil
}

func makeDataRuntimeDeps(deps *biz.RuntimeDeps, supplied *data.RuntimeDeps, symbols *orm.SymbolState, trackers ...data.CallbackTracker) *data.RuntimeDeps {
	if deps == nil {
		return nil
	}
	result := &data.RuntimeDeps{
		Core:       deps.Core,
		Clock:      deps.Clock,
		Config:     deps.Config,
		Market:     deps.Market,
		Symbols:    symbols,
		Storage:    deps.Storage,
		Strategies: deps.Strategies,
		Dump:       deps.Dump,
		Exchange:   deps.Exchange,
	}
	if supplied != nil {
		copyDeps := *supplied
		if copyDeps.Core != nil {
			result.Core = copyDeps.Core
		}
		if copyDeps.Clock != nil {
			result.Clock = copyDeps.Clock
		}
		if copyDeps.Config != nil {
			result.Config = copyDeps.Config
		}
		if copyDeps.Storage != nil {
			result.Storage = copyDeps.Storage
		}
		if copyDeps.Market != nil {
			result.Market = copyDeps.Market
		}
		if copyDeps.Strategies != nil {
			result.Strategies = copyDeps.Strategies
		}
		if copyDeps.Catalog != nil {
			result.Catalog = copyDeps.Catalog
		}
		if copyDeps.Dump != nil {
			result.Dump = copyDeps.Dump
		}
		if copyDeps.Callbacks != nil {
			result.Callbacks = copyDeps.Callbacks
		}
		if copyDeps.Exchange != nil {
			result.Exchange = copyDeps.Exchange
		}
		if copyDeps.ExchangeName != "" {
			result.ExchangeName = copyDeps.ExchangeName
		}
		if copyDeps.MarketType != "" {
			result.MarketType = copyDeps.MarketType
		}
	}
	if len(trackers) > 0 && trackers[0] != nil {
		// The lifecycle owner is the admission barrier. A supplied data tracker
		// may carry unrelated accounting, but must not replace that barrier.
		result.Callbacks = trackers[0]
	}
	result.Symbols = symbols
	return result
}

func callbackTracker(lifecycle RuntimeLifecycle) data.CallbackTracker {
	tracker, _ := lifecycle.(data.CallbackTracker)
	return tracker
}

func bindCryptoTraderRuntime(trader *CryptoTrader, lifecycle RuntimeLifecycle, startup CryptoTraderStartupFunc) *CryptoTrader {
	trader.startup = startup
	if lifecycle != nil {
		trader.runtime = lifecycle
		trader.runtimeCtx = lifecycle.Context()
		if owner, ok := lifecycle.(interface{ Scheduler() com.Scheduler }); ok {
			trader.scheduler = owner.Scheduler()
		}
		if trader.scheduler == nil {
			if deps := trader.RuntimeDependencies(); deps != nil {
				trader.scheduler = deps.Scheduler
			}
		}
		lifecycle.OnClose(trader.cancelRuntime)
		lifecycle.OnCloseWait(trader.runShutdownPhases)
	}
	return trader
}

// NewCryptoTraderWithBatchState binds the trader to a composition root's
// batch queue. A nil state creates a private state and never uses LegacyBatchState.
func NewCryptoTraderWithBatchState(batchState *strat.BatchState) *CryptoTrader {
	return NewCryptoTraderWithBatchAndSymbolState(batchState, nil)
}

// NewCryptoTraderWithBatchAndSymbolState binds the trader to explicit mutable
// runtime state. A nil symbol state preserves the legacy facade.
func NewCryptoTraderWithBatchAndSymbolState(batchState *strat.BatchState, symbols *orm.SymbolState) *CryptoTrader {
	if batchState == nil {
		batchState = strat.NewBatchState()
	}
	if symbols != nil {
		return &CryptoTrader{
			Trader:  biz.NewTraderWithRuntimeDeps(biz.RuntimeDeps{Batch: batchState, Symbols: symbols}),
			symbols: symbols,
		}
	}
	return &CryptoTrader{Trader: biz.NewTrader(batchState), symbols: symbols}
}

func (t *CryptoTrader) Init() *errs.Error {
	if t.runtimeDepsErr != nil {
		return t.runtimeDepsErr
	}
	if deps := t.RuntimeDependencies(); deps != nil && t.runtime == nil {
		return errs.NewMsg(core.ErrRunTime, "explicit live runtime requires a lifecycle")
	}
	if deps := t.RuntimeDependencies(); deps != nil {
		if t.symbols == nil {
			t.symbols = deps.Symbols
		}
		if deps.Symbols == nil {
			deps.Symbols = t.symbols
		}
		if err := validateCryptoTraderRuntimeDeps(deps, t.symbols); err != nil {
			return err
		}
	}
	if deps := t.RuntimeDependencies(); deps != nil {
		if cfg := deps.ConfigView(); cfg != nil {
			config.LoadPerfsWithCoreState(deps.Config.DataDir, deps.Core, cfg.StratPerf)
		}
	} else {
		config.LoadPerfs(config.GetDataDir())
	}
	if t.envReal() {
		if _, err := exg.RequireOrderEventCapability(t.exchangeForRun(), true); err != nil {
			return err
		}
	}
	var dp *data.LiveProvider
	var err *errs.Error
	if t.dataDeps != nil {
		dp, err = data.NewLiveProviderWithRuntimeDeps(t.dataDeps, t.FeedDataSeries, t.onEnvEnd)
	} else {
		dp, err = data.NewLiveProviderWithSymbolState(t.symbols, t.FeedDataSeries, t.onEnvEnd)
	}
	if err != nil {
		return err
	}
	if !t.setProvider(dp) {
		return errs.NewMsg(core.ErrRunTime, "runtime is stopped")
	}
	if t.RuntimeDependencies() == nil {
		t.previousUnWatch = strat.WsSubUnWatch
	}
	pairHooks := strat.PairUpdateHooks{
		SubWarmPairs: dp.SubWarmPairs,
		SymbolState:  t.symbols,
		Core:         t.coreStateForRun(),
		StrategyState: func() *strat.State {
			if deps := t.RuntimeDependencies(); deps != nil {
				return deps.Strategies
			}
			return nil
		}(),
		LookupSymbol: func(pair string) (*orm.ExSymbol, *errs.Error) {
			if t.RuntimeDependencies() != nil && t.symbols == nil {
				return nil, errs.NewMsg(core.ErrRunTime, "runtime symbol state is required to resolve %s", pair)
			}
			if t.symbols == nil {
				return orm.GetExSymbolCur(pair)
			}
			return t.symbols.GetExSymbolCur(pair)
		},
		ExitOrders: func(acc string, orders []*ormo.InOutOrder, req *strat.ExitReq) *errs.Error {
			var mgr biz.IOrderMgr
			if deps := t.RuntimeDependencies(); deps != nil {
				mgr = biz.GetOdMgrWithState(deps.Trading, acc)
			} else {
				mgr = biz.GetOdMgr(acc)
			}
			if mgr == nil {
				return errs.NewMsg(core.ErrRunTime, "order manager is required for pair update: %s", acc)
			}
			return mgr.ExitAndFill(orders, req)
		},
	}
	if deps := t.RuntimeDependencies(); deps != nil {
		if deps.Strategies == nil || strat.IsLegacyState(deps.Strategies) {
			return errs.NewMsg(core.ErrRunTime, "explicit live runtime requires a private strategy state")
		}
		deps.Strategies.SetPairUpdateHooks(pairHooks)
	} else {
		strat.SetPairUpdateHooks(pairHooks)
	}
	if deps := t.RuntimeDependencies(); deps != nil && deps.Orders != nil {
		accounts := make([]string, 0)
		for account := range deps.AccountConfigs() {
			accounts = append(accounts, account)
		}
		if len(accounts) == 0 {
			accounts = []string{deps.DefaultAccount}
		}
		cfg := deps.ConfigView()
		if cfg == nil {
			return errs.NewMsg(core.ErrBadConfig, "runtime config is required")
		}
		err = ormo.InitLiveTasksWithState(deps.Orders, accounts, cfg.Name, deps.Core.EnvReal)
	} else {
		err = ormo.InitTask(true, config.GetDataDir())
	}
	if err != nil {
		return err
	}
	// Trading pair initialization
	// 交易对初始化
	err = orm.InitListDatesWithExchange(t.symbols, t.exchangeForRun())
	if err != nil {
		return err
	}
	// 初始化 Telegram 订单管理器
	if t.RuntimeDependencies() == nil {
		biz.InitTelegramOrderManager()
	}
	err = t.startWebAPI()
	if err != nil {
		return err
	}
	if t.envReal() {
		if deps := t.RuntimeDependencies(); deps != nil {
			CheckLiveAccountsWithRuntimeDeps(*deps)
		} else {
			CheckLiveAccounts()
		}
	}
	// Order Manager initialization
	// 订单管理器初始化
	err = t.initOdMgr()
	if err != nil {
		return err
	}
	err = t.refreshPairJobs(true)
	if err != nil {
		return err
	}
	// add exit callback
	if t.RuntimeDependencies() == nil && t.runtime == nil {
		core.AddExitCall(t.finishRunCleanup)
	}
	unwatch := func(m map[string][]string) {
		for msgType, pairs := range m {
			exgName, market := t.marketIdentity()
			err2 := dp.UnWatchJobs(exgName, market, msgType, pairs)
			if err2 != nil {
				log.Error("UnWatchJobs fail", zap.String("type", msgType), zap.Error(err2))
			}
		}
	}
	if deps := t.RuntimeDependencies(); deps != nil {
		if deps.Strategies == nil || strat.IsLegacyState(deps.Strategies) {
			return errs.NewMsg(core.ErrRunTime, "explicit live runtime requires a private strategy state")
		}
		deps.Strategies.SetWsSubUnWatch(unwatch)
	} else {
		strat.WsSubUnWatch = unwatch
	}
	return nil
}

func validateCryptoTraderRuntimeDeps(deps *biz.RuntimeDeps, symbols *orm.SymbolState) *errs.Error {
	if deps == nil {
		return nil
	}
	missing := make([]string, 0, 8)
	if deps.Core == nil {
		missing = append(missing, "core")
	}
	if deps.Clock == nil {
		missing = append(missing, "clock")
	}
	if deps.Strategies == nil || strat.IsLegacyState(deps.Strategies) {
		missing = append(missing, "strategy state")
	}
	if deps.Orders == nil {
		missing = append(missing, "order state")
	}
	if deps.Trading == nil {
		missing = append(missing, "trading state")
	}
	if symbols == nil {
		missing = append(missing, "symbol state")
	}
	if deps.Exchange == nil {
		missing = append(missing, "exchange")
	}
	if deps.Config == nil || deps.Config.View() == nil {
		missing = append(missing, "config")
	}
	if len(missing) > 0 {
		return errs.NewMsg(core.ErrRunTime, "explicit live runtime requires %s", strings.Join(missing, ", "))
	}
	return nil
}

func (t *CryptoTrader) coreStateForRun() *core.State {
	if t == nil {
		return nil
	}
	if t.dataDeps != nil && t.dataDeps.Core != nil {
		return t.dataDeps.Core
	}
	if deps := t.RuntimeDependencies(); deps != nil {
		return deps.Core
	}
	return nil
}

func (t *CryptoTrader) refreshPairJobs(isFirst bool) *errs.Error {
	if t == nil {
		return errs.NewMsg(core.ErrRunTime, "crypto trader is required")
	}
	state := t.coreStateForRun()
	if state == nil {
		if t.symbols != nil || t.dataDeps != nil {
			return errs.NewMsg(core.ErrBadConfig,
				"explicit live pair refresh requires complete runtime dependencies")
		}
		// Preserve the public legacy path for callers that have no Runtime.
		return opt.RefreshPairJobsWithSymbolState(t.provider(), t.symbols, true, isFirst, nil)
	}
	var clock *btime.ClockState
	if t.dataDeps != nil {
		clock = t.dataDeps.Clock
	}
	if clock == nil {
		if deps := t.RuntimeDependencies(); deps != nil {
			clock = deps.Clock
		}
	}
	return refreshPairJobsWithRuntime(t.provider(), t.symbols, t.RuntimeDependencies(), clock, t.exchangeForRun(), true, isFirst)
}

func (t *CryptoTrader) startWebAPI() *errs.Error {
	// The current Web API handlers are legacy process-global handlers. Starting
	// them from an explicit Runtime would expose another runtime's config,
	// strategy registry, orders, and storage, so the explicit runner leaves the
	// legacy API disabled until the handlers receive typed dependencies.
	if t.RuntimeDependencies() != nil {
		if deps := t.RuntimeDependencies(); deps != nil {
			if cfg := deps.ConfigView(); cfg != nil && cfg.APIServer != nil && cfg.APIServer.Enable {
				return errs.NewMsg(core.ErrRunTime, "api_server.enable is not supported by an explicit runtime yet")
			}
		}
		return nil
	}
	if t.runtime != nil {
		return webStartAPIWithLifecycle(&runtimeShutdownLifecycle{owner: t})
	}
	return web.StartApiInLegacySession()
}

func (t *CryptoTrader) initOdMgr() *errs.Error {
	dp := t.provider()
	if dp == nil {
		return errs.NewMsg(core.ErrRunTime, "live provider is required")
	}
	deps := t.RuntimeDependencies()
	if !t.envReal() {
		if deps != nil {
			if !biz.RestoreDryRunWalletSnapshotWithRuntimeDeps(*deps) {
				biz.InitFakeWalletsWithRuntimeDeps(*deps)
			}
			biz.InitLocalLiveOrderMgrWithRuntimeDeps(*deps, t.orderCB, true)
			dp.OnDataSeries = func(msg *data.SeriesMsg, rows []*orm.DataSeries) *errs.Error {
				return biz.CallLocalLiveOdMgrsDataWithRuntime(*deps, msg, rows)
			}
			t.bindProviderRuntimeCallbacks(dp)
			return nil
		}
		if !biz.RestoreDryRunWalletSnapshot(config.DefAcc) {
			biz.InitFakeWallets()
		}
		biz.InitLocalLiveOrderMgr(t.orderCB, true)
		dp.OnDataSeries = biz.CallLocalLiveOdMgrsData
		t.bindProviderRuntimeCallbacks(dp)
		return nil
	}
	if deps != nil {
		biz.InitLiveOrderMgrWithRuntimeDeps(*deps, t.orderCB)
	} else {
		biz.InitLiveOrderMgr(t.orderCB)
	}
	t.bindProviderRuntimeCallbacks(dp)
	var accounts map[string]*config.AccountConfig
	if deps != nil {
		accounts = deps.AccountConfigs()
	} else {
		accounts = config.Accounts
	}
	for account, cfg := range accounts {
		if cfg == nil || cfg.NoTrade {
			continue
		}
		var odMgr *biz.LiveOrderMgr
		if deps != nil {
			odMgr = biz.GetLiveOdMgrWithState(deps.Trading, account)
		} else {
			odMgr = biz.GetLiveOdMgr(account)
		}
		if odMgr == nil {
			return errs.NewMsg(core.ErrRunTime, "live order manager is not initialized: %s", account)
		}
		oldList, newList, delList, err := odMgr.SyncExgOrders()
		if err != nil {
			return err
		}
		var orders *ormo.OrderState
		if deps != nil {
			orders = deps.Orders
		} else {
			orders = ormo.LegacyState()
		}
		openOds, lock := orders.GetOpenODs(account)
		lock.Lock()
		msg := fmt.Sprintf("orders: %d restored, %d deleted, %d added, %d opened", len(oldList), len(delList), len(newList), len(openOds))
		lock.Unlock()
		sendRuntimeMessage(deps, map[string]interface{}{
			"type":    rpc.MsgTypeStatus,
			"account": account,
			"status":  msg,
		})
	}
	return nil
}

func (t *CryptoTrader) Run() *errs.Error {
	return t.runWithDeps()
}

func (t *CryptoTrader) bootstrapThirdPartySources(ctx context.Context) error {
	_, err := t.thirdPartyRuntime().SyncLive(ctx, t.runtimeJobs(), t.currentTimeMS())
	return err
}

func (t *CryptoTrader) runtimeJobs() []*strat.StratJob {
	if t.collectJobsFn != nil {
		return t.collectJobsFn()
	}
	if deps := t.RuntimeDependencies(); deps != nil {
		if deps.Strategies == nil || strat.IsLegacyState(deps.Strategies) {
			return nil
		}
		return deps.Strategies.CollectJobs()
	}
	if state := t.strategyStateForRun(); state != nil {
		return state.CollectJobs()
	}
	seen := make(map[*strat.StratJob]bool)
	var jobs []*strat.StratJob
	strat.LockJobsRead()
	defer strat.UnlockJobsRead()
	accounts := make([]string, 0, len(strat.AccJobs))
	for acc := range strat.AccJobs {
		accounts = append(accounts, acc)
	}
	sort.Strings(accounts)
	for _, acc := range accounts {
		jobMap := strat.AccJobs[acc]
		envKeys := make([]string, 0, len(jobMap))
		for envKey := range jobMap {
			envKeys = append(envKeys, envKey)
		}
		sort.Strings(envKeys)
		for _, envKey := range envKeys {
			stgMap := jobMap[envKey]
			stgNames := make([]string, 0, len(stgMap))
			for stgName := range stgMap {
				stgNames = append(stgNames, stgName)
			}
			sort.Strings(stgNames)
			for _, stgName := range stgNames {
				job := stgMap[stgName]
				if job == nil || seen[job] {
					continue
				}
				seen[job] = true
				jobs = append(jobs, job)
			}
		}
	}
	return jobs
}

func (t *CryptoTrader) currentTimeMS() int64 {
	if t.nowMSFn != nil {
		return t.nowMSFn()
	}
	if t.dataDeps != nil && t.dataDeps.Clock != nil {
		return t.dataDeps.Clock.TimeMS()
	}
	return t.TimeMS()
}

func (t *CryptoTrader) envReal() bool {
	if deps := t.dataDeps; deps != nil && deps.Core != nil {
		return deps.Core.EnvReal
	}
	if deps := t.RuntimeDependencies(); deps != nil {
		return deps.Core != nil && deps.Core.EnvReal
	}
	return core.EnvReal
}

func (t *CryptoTrader) exchangeForRun() banexg.BanExchange {
	if t == nil {
		return nil
	}
	if t.dataDeps != nil || t.RuntimeDependencies() != nil || t.runtime != nil {
		return t.exchangeBinding()
	}
	return exg.Default
}

func (t *CryptoTrader) exchangeBinding() banexg.BanExchange {
	if t == nil {
		return nil
	}
	if deps := t.dataDeps; deps != nil && deps.Exchange != nil {
		return deps.Exchange
	}
	if deps := t.RuntimeDependencies(); deps != nil && deps.Exchange != nil {
		return deps.Exchange
	}
	if provider, ok := t.runtime.(interface{ Exchange() banexg.BanExchange }); ok {
		return provider.Exchange()
	}
	return nil
}

func (t *CryptoTrader) marketIdentity() (string, string) {
	if deps := t.dataDeps; deps != nil {
		if deps.ExchangeName != "" || deps.MarketType != "" {
			return deps.ExchangeName, deps.MarketType
		}
		if deps.Core != nil {
			return deps.Core.ExgName, deps.Core.Market
		}
		if deps.Exchange != nil {
			if info := deps.Exchange.Info(); info != nil {
				return info.ID, info.MarketType
			}
		}
		return "", ""
	}
	if deps := t.RuntimeDependencies(); deps != nil {
		if deps.Core != nil {
			return deps.Core.ExgName, deps.Core.Market
		}
		if deps.Exchange != nil {
			if info := deps.Exchange.Info(); info != nil {
				return info.ID, info.MarketType
			}
		}
		return "", ""
	}
	return core.ExgName, core.Market
}

func (t *CryptoTrader) runContext() context.Context {
	if deps := t.dataDeps; deps != nil && deps.Core != nil {
		if ctx := deps.Core.Context(); ctx != nil {
			return ctx
		}
	}
	if t.runtimeCtx != nil {
		return t.runtimeCtx
	}
	return context.Background()
}

func (t *CryptoTrader) thirdPartyRuntime() *data.SeriesRuntime {
	if t.seriesRuntime == nil {
		if t.dataDeps != nil {
			t.seriesRuntime = data.NewSeriesRuntimeWithRuntimeDeps(t.dataDeps, t)
		} else {
			t.seriesRuntime = data.NewSeriesRuntime(t)
		}
		if deps := t.RuntimeDependencies(); deps != nil {
			t.seriesRuntime.Repo = orm.NewSeriesRepo(deps.Storage)
		}
	}
	if t.seriesRuntime.Sink == nil {
		t.seriesRuntime.Sink = t
	}
	return t.seriesRuntime
}

func (t *CryptoTrader) runWithDeps() (runErr *errs.Error) {
	t.runLock.Lock()
	defer t.runLock.Unlock()
	if t.RuntimeDependencies() != nil && t.runtime == nil {
		return errs.NewMsg(core.ErrRunTime, "explicit live runtime requires a lifecycle")
	}
	if !t.resetRuntimeState() {
		return errs.NewMsg(core.ErrRunTime, "runtime is stopped")
	}
	// Publish cleanup ownership before admitting the run callback. A Runtime
	// close can race with init/startup and must observe the cleanup barrier.
	t.prepareRunCleanup()
	if !t.beginRuntimeCounter() {
		return errs.NewMsg(core.ErrRunTime, "runtime is stopped")
	}
	defer func() {
		if runErr != nil {
			t.rollbackRunCleanup()
		}
		t.endRuntimeCounter()
		t.joinRuntimeCallbacks()
		if runErr != nil {
			t.finishRunCleanup()
		}
	}()
	initFn := t.initFn
	if initFn == nil {
		initFn = t.Init
	}
	err := initFn()
	if err != nil {
		return err
	}
	if !t.runtimeActive() {
		return errs.NewMsg(core.ErrRunTime, "runtime is stopped")
	}
	runCtx := t.runContext()
	if t.startup != nil {
		if errRun := t.startup(runCtx, t); errRun != nil {
			return errs.New(core.ErrRunTime, errRun)
		}
		if !t.runtimeActive() {
			return errs.NewMsg(core.ErrRunTime, "runtime is stopped")
		}
	}
	if errRun := t.bootstrapThirdPartySources(runCtx); errRun != nil {
		return errs.New(core.ErrRunTime, errRun)
	}
	if !t.runtimeActive() {
		return errs.NewMsg(core.ErrRunTime, "runtime is stopped")
	}
	startJobsFn := t.startJobsFn
	if startJobsFn == nil {
		startJobsFn = t.startJobs
	}
	startJobsFn()
	if !t.runtimeActive() {
		return errs.NewMsg(core.ErrRunTime, "runtime is stopped")
	}
	loopMainFn := t.loopMainFn
	if loopMainFn == nil {
		dp := t.provider()
		if dp == nil {
			return errs.NewMsg(core.ErrRunTime, "live provider is required")
		}
		loopMainFn = dp.LoopMain
	}
	err = loopMainFn()
	if err != nil {
		return err
	}
	// clean CallBacks already to core.ExitCalls
	return nil
}

func (t *CryptoTrader) resetRuntimeState() bool {
	t.joinRuntimeCallbacks()
	t.runtimeLock.Lock()
	defer t.runtimeLock.Unlock()
	if t.runtime != nil {
		if ctx := t.runtimeCtx; ctx != nil {
			select {
			case <-ctx.Done():
				t.runtimeStopped.Store(true)
				return false
			default:
			}
		}
	}
	t.runtimeStopped.Store(false)
	return true
}

func (t *CryptoTrader) FeedDataSeries(evt *orm.DataSeries) {
	if evt == nil {
		return
	}
	if !t.beginRuntimeCallback() {
		return
	}
	defer t.endRuntimeCallback()
	if err := t.feedDataSeries(evt); err != nil {
		log.Error("handle data series fail", zap.Int32("sid", evt.Sid), zap.Error(err))
	}
}

func (t *CryptoTrader) feedDataSeries(evt *orm.DataSeries) *errs.Error {
	if evt == nil {
		return nil
	}
	exs, errSymbol := t.Trader.ResolveDataSeriesSymbol(evt)
	if errSymbol != nil {
		return errSymbol
	}
	view, errView := evt.OHLCV(exs)
	if errView != nil {
		if evt.HasOHLCV() {
			return errs.New(core.ErrInvalidBars, errView)
		}
		return t.Trader.FeedDataSeries(evt)
	}
	if view.IsWarmUp {
		t.handleWarmupSeries(view)
	} else {
		t.handleLiveSeries(view)
	}
	return t.Trader.FeedDataSeries(evt)
}

func (t *CryptoTrader) Emit(sub *strat.DataSub, rows []*orm.DataRecord) error {
	if t == nil {
		return fmt.Errorf("crypto trader is required")
	}
	if !t.beginRuntimeCallback() {
		return nil
	}
	defer t.endRuntimeCallback()
	dp := t.provider()
	if dp == nil {
		return fmt.Errorf("live provider is required")
	}
	if sub == nil {
		return fmt.Errorf("data sub is required")
	}
	if sub.ExSymbol == nil || sub.ExSymbol.ID <= 0 {
		return fmt.Errorf("data sub exsymbol is required")
	}
	if sub.TimeFrame == "" {
		return fmt.Errorf("data sub timeframe is required")
	}
	if len(rows) == 0 {
		return nil
	}
	exgName, market := t.marketIdentity()
	msg := &data.SeriesMsg{
		ExgName: exgName,
		Market:  market,
		Pair:    sub.ExSymbol.Symbol,
		NotifySeries: data.NotifySeries{
			TFSecs:   utils.TFToSecs(sub.TimeFrame),
			Interval: utils.TFToSecs(sub.TimeFrame),
		},
	}
	seriesRows := make([]*orm.DataSeries, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		evt := &orm.DataSeries{
			Source:    orm.NormalizeSeriesSource(sub.Source),
			Sid:       sub.ExSymbol.ID,
			TimeMS:    row.TimeMS,
			EndMS:     row.EndMS,
			TimeFrame: sub.TimeFrame,
			Closed:    row.Closed,
			Values:    cloneSeriesValues(row.Values),
			ExSymbol:  sub.ExSymbol,
		}
		seriesRows = append(seriesRows, evt)
	}
	if len(seriesRows) == 0 {
		return nil
	}
	msg.Rows = seriesRows
	for _, evt := range seriesRows {
		if err := t.feedDataSeries(evt); err != nil {
			return err
		}
	}
	if dp.OnDataSeries != nil {
		if err := dp.OnDataSeries(msg, seriesRows); err != nil {
			return err
		}
	}
	return nil
}

func (t *CryptoTrader) onEnvEnd(evt *orm.DataSeries) {
	if !t.beginRuntimeCallback() {
		return
	}
	defer t.endRuntimeCallback()
	t.Trader.OnEnvEnd(evt)
}

func (t *CryptoTrader) handleWarmupSeries(view *orm.SeriesOHLCV) {
	if view == nil {
		return
	}
	tfMSecs := int64(utils.TFToSecs(view.TimeFrame) * 1000)
	barEndMS := view.Time + tfMSecs
	batchState := t.batchStateForRun()
	if batchState == nil {
		return
	}
	if barEndMS > batchState.LastBatchMS() {
		// Enter the next timeframe and trigger the batch entry callback
		// 进入下一个时间帧，触发批量入场回调
		execMS := barEndMS + core.DelayBatchMS + 1
		var waitNum int
		if deps := t.RuntimeDependencies(); deps != nil {
			waitNum = biz.TryFireBatchesWithRuntimeDeps(deps, batchState, execMS, view.IsWarmUp)
		} else {
			waitNum = biz.TryFireBatchesWithState(batchState, execMS, view.IsWarmUp)
		}
		if waitNum > 0 {
			log.Warn(fmt.Sprintf("batch job exec fail, wait: %v", waitNum))
		}
		batchState.SetLastBatchMS(barEndMS)
	}
}

func (t *CryptoTrader) handleLiveSeries(view *orm.SeriesOHLCV) {
	if view == nil {
		return
	}
	t.delayExecBatch()
	envKey := strings.Join([]string{view.Symbol(), view.TimeFrame}, "_")
	if bar := view.Bar(); bar != nil {
		if deps := t.RuntimeDependencies(); deps != nil {
			if deps.Dump != nil {
				deps.Dump.Add(orm.DumpKline, envKey, *bar)
			}
		} else {
			orm.AddDumpRow(orm.DumpKline, envKey, *bar)
		}
	}
}

func cloneSeriesValues(values map[string]any) map[string]any {
	if len(values) == 0 {
		return nil
	}
	cp := make(map[string]any, len(values))
	for key, val := range values {
		cp[key] = val
	}
	return cp
}

func (t *CryptoTrader) delayExecBatch() {
	t.delayExecBatchAfter(time.Millisecond * core.DelayBatchMS)
}

func (t *CryptoTrader) delayExecBatchAfter(delay time.Duration) {
	if t == nil {
		return
	}
	if t.runtime == nil {
		time.AfterFunc(delay, t.runDelayedBatch)
		return
	}
	t.runtimeLock.Lock()
	if !t.runtimeCallbackActiveLocked() {
		t.runtimeLock.Unlock()
		return
	}
	if t.runtimeTimers == nil {
		t.runtimeTimers = make(map[*time.Timer]struct{})
	}
	t.runtimeCount++
	if t.runtimeCond == nil {
		t.runtimeCond = sync.NewCond(&t.runtimeLock)
	}
	var timer *time.Timer
	timer = time.AfterFunc(delay, func() {
		t.runtimeLock.Lock()
		delete(t.runtimeTimers, timer)
		active := t.runtimeCallbackActiveLocked()
		t.runtimeLock.Unlock()
		if !active {
			t.endRuntimeCounter()
			return
		}
		if tracker := t.runtimeCallbackTracker(); tracker != nil && !tracker.EnterCallback() {
			t.endRuntimeCounter()
			return
		}
		defer t.endRuntimeCallback()
		t.runDelayedBatch()
	})
	t.runtimeTimers[timer] = struct{}{}
	t.runtimeLock.Unlock()
}

func (t *CryptoTrader) runDelayedBatch() {
	batchState := t.batchStateForRun()
	if batchState == nil {
		return
	}
	nowMS := t.currentTimeMS()
	var waitNum int
	if deps := t.RuntimeDependencies(); deps != nil {
		waitNum = biz.TryFireBatchesWithRuntimeDeps(deps, batchState, nowMS, false)
	} else {
		waitNum = biz.TryFireBatchesWithState(batchState, nowMS, false)
	}
	if waitNum > 0 {
		// There are TF cycles that have not yet been completed, and they are postponed for a few seconds to trigger again
		// 有尚未完成的tf周期，推迟几秒再次触发
		t.delayExecBatch()
	} else {
		if deps := t.RuntimeDependencies(); deps != nil {
			if deps.Dump != nil {
				if err := deps.Dump.Flush(); err != nil {
					log.Error("flush runtime dump fail", zap.Error(err))
				}
			}
		} else {
			orm.FlushDumps()
		}
	}
}

func (t *CryptoTrader) runtimeCallbackActiveLocked() bool {
	if t.runtime == nil {
		return true
	}
	if t.runtimeStopped.Load() {
		return false
	}
	if ctx := t.runtimeCtx; ctx != nil {
		select {
		case <-ctx.Done():
			return false
		default:
		}
	}
	return true
}

func (t *CryptoTrader) runtimeActive() bool {
	if t == nil {
		return false
	}
	t.runtimeLock.Lock()
	defer t.runtimeLock.Unlock()
	return t.runtimeCallbackActiveLocked()
}

func (t *CryptoTrader) provider() *data.LiveProvider {
	if t == nil {
		return nil
	}
	t.runtimeLock.Lock()
	dp := t.dp
	t.runtimeLock.Unlock()
	return dp
}

func (t *CryptoTrader) setProvider(dp *data.LiveProvider) bool {
	if t == nil || dp == nil {
		return false
	}
	t.bindProviderRuntimeCallbacks(dp)
	t.runtimeLock.Lock()
	accepted := t.runtimeCallbackActiveLocked()
	var old *data.LiveProvider
	if accepted {
		old = t.dp
		if old != nil && old != dp {
			t.retiredProviders = appendProviderUnique(t.retiredProviders, old)
		}
		t.dp = dp
	}
	t.runtimeLock.Unlock()
	if !accepted {
		// A provider callback can replace itself after Runtime cancellation. Do
		// not synchronously Join here: the current handler may be the provider's
		// last admitted callback. The owner joins retired providers later.
		t.runtimeLock.Lock()
		t.retiredProviders = appendProviderUnique(t.retiredProviders, dp)
		t.runtimeLock.Unlock()
		closeProviderInstance(dp, false)
		return false
	}
	if old != nil && old != dp {
		// Stop seals the old provider immediately; the owner joins retired
		// providers during shutdown/reset so a handler can replace itself.
		closeProviderInstance(old, false)
	}
	return accepted
}

// bindProviderRuntimeCallbacks gives a provider built through the standalone
// watcher constructor the same Runtime admission barrier as a typed provider.
// The watcher may otherwise invoke websocket callbacks without data deps, and
// Runtime.Close could reset state synchronously from inside that callback.
func (t *CryptoTrader) bindProviderRuntimeCallbacks(dp *data.LiveProvider) {
	if t == nil || dp == nil || t.runtime == nil {
		return
	}
	t.runtimeLock.Lock()
	if t.providerBindings == nil {
		t.providerBindings = make(map[*data.LiveProvider]struct{})
	}
	if _, ok := t.providerBindings[dp]; ok {
		t.runtimeLock.Unlock()
		return
	}
	t.providerBindings[dp] = struct{}{}
	wrapSeries := func(callback func(*data.SeriesMsg)) func(*data.SeriesMsg) {
		if callback == nil {
			return nil
		}
		return func(msg *data.SeriesMsg) {
			if !t.beginRuntimeCallback() {
				return
			}
			defer t.endRuntimeCallback()
			callback(msg)
		}
	}
	wrapTrades := func(callback func(string, string, string, []*banexg.Trade)) func(string, string, string, []*banexg.Trade) {
		if callback == nil {
			return nil
		}
		return func(exgName, market, pair string, trades []*banexg.Trade) {
			if !t.beginRuntimeCallback() {
				return
			}
			defer t.endRuntimeCallback()
			callback(exgName, market, pair, trades)
		}
	}
	wrapDepth := func(callback func(*banexg.OrderBook)) func(*banexg.OrderBook) {
		if callback == nil {
			return nil
		}
		return func(book *banexg.OrderBook) {
			if !t.beginRuntimeCallback() {
				return
			}
			defer t.endRuntimeCallback()
			callback(book)
		}
	}
	if dp.SeriesWatcher != nil {
		dp.OnDataMsg = wrapSeries(dp.OnDataMsg)
		dp.OnTrades = wrapTrades(dp.OnTrades)
		dp.OnDepth = wrapDepth(dp.OnDepth)
	}
	if dp.OnDataSeries != nil {
		callback := dp.OnDataSeries
		dp.OnDataSeries = func(msg *data.SeriesMsg, rows []*orm.DataSeries) *errs.Error {
			if !t.beginRuntimeCallback() {
				return nil
			}
			defer t.endRuntimeCallback()
			return callback(msg, rows)
		}
	}
	t.runtimeLock.Unlock()
}

func (t *CryptoTrader) beginRuntimeCallback() bool {
	if t == nil {
		return false
	}
	if !t.beginRuntimeCounter() {
		return false
	}
	if t.runtime == nil {
		return true
	}
	if tracker := t.runtimeCallbackTracker(); tracker != nil && !tracker.EnterCallback() {
		t.endRuntimeCounter()
		return false
	}
	return true
}

func (t *CryptoTrader) beginRuntimeCounter() bool {
	if t == nil {
		return false
	}
	if t.runtime == nil {
		return true
	}
	t.runtimeLock.Lock()
	if !t.runtimeCallbackActiveLocked() {
		t.runtimeLock.Unlock()
		return false
	}
	t.runtimeCount++
	if t.runtimeCond == nil {
		t.runtimeCond = sync.NewCond(&t.runtimeLock)
	}
	t.runtimeLock.Unlock()
	return true
}

func (t *CryptoTrader) endRuntimeCallback() {
	if t == nil {
		return
	}
	if tracker := t.runtimeCallbackTracker(); tracker != nil {
		tracker.LeaveCallback()
	}
	t.endRuntimeCounter()
}

func (t *CryptoTrader) endRuntimeCounter() {
	if t == nil {
		return
	}
	if t.runtime != nil {
		t.runtimeLock.Lock()
		if t.runtimeCount > 0 {
			t.runtimeCount--
		}
		if t.runtimeCond != nil {
			t.runtimeCond.Broadcast()
		}
		t.runtimeLock.Unlock()
	}
}

func (t *CryptoTrader) runtimeCallbackTracker() data.CallbackTracker {
	if t == nil || t.runtime == nil {
		return nil
	}
	tracker, _ := t.runtime.(data.CallbackTracker)
	return tracker
}

func (t *CryptoTrader) cancelRuntime() {
	t.runtimeLock.Lock()
	t.stopRuntimeLocked()
	t.detachCurrentProviderLocked()
	providers := t.providerInstancesLocked()
	t.runtimeLock.Unlock()
	closeProviderInstances(providers, false)
}

func (t *CryptoTrader) registerShutdownJoin(call func()) {
	if t == nil || call == nil {
		return
	}
	t.shutdownLock.Lock()
	if t.cleanupDone || t.shutdownStarted {
		t.shutdownLock.Unlock()
		call()
		return
	}
	t.shutdownJoins = append(t.shutdownJoins, call)
	t.shutdownLock.Unlock()
}

func (t *CryptoTrader) registerShutdownStop(call func()) {
	if t == nil || call == nil {
		return
	}
	t.shutdownLock.Lock()
	if t.cleanupDone || t.shutdownStarted {
		t.shutdownLock.Unlock()
		call()
		return
	}
	t.shutdownStops = append(t.shutdownStops, call)
	t.shutdownLock.Unlock()
}

func (t *CryptoTrader) prepareRunCleanup() {
	if t == nil {
		return
	}
	var backup *biz.VarsBackup
	if t.runtime == nil && t.RuntimeDependencies() == nil {
		backup = biz.BackupVars()
	}
	t.shutdownLock.Lock()
	t.shutdownReady = true
	t.cleanupDone = false
	t.shutdownJoins = nil
	t.shutdownStops = nil
	t.legacyBackup = backup
	if backup != nil {
		t.previousUnWatch = strat.WsSubUnWatch
	}
	t.shutdownLock.Unlock()
}

// stopAndJoinRegisteredResources closes every resource acquired by Init and
// startJobs. Stops run before joins so callbacks cannot admit new work while a
// wait is in progress.
func (t *CryptoTrader) stopAndJoinRegisteredResources() {
	if t == nil {
		return
	}
	t.shutdownLock.Lock()
	stops := append([]func(){}, t.shutdownStops...)
	joins := append([]func(){}, t.shutdownJoins...)
	t.shutdownStops = nil
	t.shutdownJoins = nil
	t.shutdownLock.Unlock()
	for i := len(stops) - 1; i >= 0; i-- {
		if stops[i] != nil {
			stops[i]()
		}
	}
	for i := len(joins) - 1; i >= 0; i-- {
		if joins[i] != nil {
			joins[i]()
		}
	}
}

func (t *CryptoTrader) rollbackRunCleanup() {
	t.stopAndJoinRegisteredResources()
}

func (t *CryptoTrader) finishRunCleanup() {
	if t == nil {
		return
	}
	t.shutdownLock.Lock()
	if t.cleanupDone || !t.shutdownReady {
		t.shutdownLock.Unlock()
		return
	}
	t.cleanupDone = true
	backup := t.legacyBackup
	previousUnWatch := t.previousUnWatch
	t.shutdownLock.Unlock()

	// All admitted callbacks have been joined by the caller before this point.
	// Keep process-facing cleanup in one idempotent path, then restore any
	// compatibility state that was present before this trader started.
	if deps := t.RuntimeDependencies(); deps != nil {
		exitCleanUpWithRuntime(nil, t.exchangeForRun(), t.strategyStateForRun(), deps)
	} else {
		exitCleanUpWithExchange(nil, t.exchangeForRun(), t.strategyStateForRun())
	}
	if deps := t.RuntimeDependencies(); deps != nil && deps.Strategies != nil {
		deps.Strategies.SetWsSubUnWatch(nil)
	} else {
		strat.WsSubUnWatch = previousUnWatch
	}
	if backup != nil {
		biz.RestoreVars(backup)
	}
}

func (t *CryptoTrader) strategyStateForRun() *strat.State {
	if t == nil {
		return nil
	}
	if deps := t.RuntimeDependencies(); deps != nil {
		return deps.Strategies
	}
	return nil
}

func (t *CryptoTrader) enableRuntimeCleanup() {
	if t == nil {
		return
	}
	t.shutdownLock.Lock()
	t.shutdownReady = true
	t.shutdownLock.Unlock()
}

// runShutdownPhases is the sole runtime wait hook. Component stop hooks run
// during runtime cancellation; this hook joins the admitted workers first and
// only then runs the process-facing cleanup.
func (t *CryptoTrader) runShutdownPhases() {
	if t == nil {
		return
	}
	t.shutdownLock.Lock()
	if t.shutdownStarted {
		t.shutdownLock.Unlock()
		return
	}
	t.shutdownStarted = true
	cleanup := t.shutdownReady
	t.shutdownLock.Unlock()

	t.stopAndJoinRegisteredResources()
	// Provider callbacks and trader timers are joined after domain workers, so
	// exitCleanUp cannot observe an in-flight callback or reset state early.
	t.joinRuntimeCallbacks()
	if cleanup {
		t.finishRunCleanup()
	}
}

func (t *CryptoTrader) joinRuntimeCallbacks() {
	t.runtimeLock.Lock()
	t.stopRuntimeLocked()
	t.detachCurrentProviderLocked()
	t.runtimeLock.Unlock()
	for {
		t.runtimeLock.Lock()
		providers := t.providerInstancesLocked()
		t.runtimeLock.Unlock()
		closeProviderInstances(providers, true)
		t.runtimeLock.Lock()
		t.removeProvidersLocked(providers)
		more := len(t.retiredProviders) > 0 || t.dp != nil
		t.runtimeLock.Unlock()
		if !more {
			break
		}
	}
	t.waitRuntimeCallbacks()

	// A callback can create a provider after the first provider snapshot but
	// before its own lease is released. Recheck after the lease barrier so a
	// rejected provider cannot outlive Runtime reset.
	for {
		t.runtimeLock.Lock()
		more := len(t.retiredProviders) > 0 || t.dp != nil
		t.runtimeLock.Unlock()
		if !more {
			return
		}
		t.runtimeLock.Lock()
		providers := t.providerInstancesLocked()
		t.runtimeLock.Unlock()
		closeProviderInstances(providers, true)
		t.runtimeLock.Lock()
		t.removeProvidersLocked(providers)
		t.runtimeLock.Unlock()
		t.waitRuntimeCallbacks()
	}
}

func (t *CryptoTrader) detachCurrentProviderLocked() {
	if t.dp == nil {
		return
	}
	t.retiredProviders = appendProviderUnique(t.retiredProviders, t.dp)
	t.dp = nil
}

func (t *CryptoTrader) providerInstancesLocked() []*data.LiveProvider {
	providers := make([]*data.LiveProvider, 0, len(t.retiredProviders)+1)
	for _, dp := range t.retiredProviders {
		providers = appendProviderUnique(providers, dp)
	}
	return appendProviderUnique(providers, t.dp)
}

func (t *CryptoTrader) removeProvidersLocked(done []*data.LiveProvider) {
	if len(done) == 0 || len(t.retiredProviders) == 0 {
		return
	}
	kept := t.retiredProviders[:0]
	for _, dp := range t.retiredProviders {
		if !providerInList(done, dp) {
			kept = append(kept, dp)
		}
	}
	t.retiredProviders = kept
}

func appendProviderUnique(providers []*data.LiveProvider, dp *data.LiveProvider) []*data.LiveProvider {
	if dp == nil || providerInList(providers, dp) {
		return providers
	}
	return append(providers, dp)
}

func providerInList(providers []*data.LiveProvider, target *data.LiveProvider) bool {
	for _, dp := range providers {
		if dp == target {
			return true
		}
	}
	return false
}

func (t *CryptoTrader) stopRuntimeLocked() {
	t.runtimeStopped.Store(true)
	for timer := range t.runtimeTimers {
		if timer.Stop() {
			delete(t.runtimeTimers, timer)
			if t.runtimeCount > 0 {
				t.runtimeCount--
			}
			if t.runtimeCond != nil {
				t.runtimeCond.Broadcast()
			}
		}
	}
}

func (t *CryptoTrader) waitRuntimeCallbacks() {
	t.runtimeLock.Lock()
	for t.runtimeCount > 0 {
		if t.runtimeCond == nil {
			t.runtimeCond = sync.NewCond(&t.runtimeLock)
		}
		t.runtimeCond.Wait()
	}
	t.runtimeLock.Unlock()
}

func (t *CryptoTrader) schedulerForRun() com.Scheduler {
	if t == nil {
		return nil
	}
	if t.runtime == nil && t.RuntimeDependencies() == nil {
		return legacyScheduler(nil)
	}
	return t.scheduler
}

func (t *CryptoTrader) pairCopiedStateForRun() *com.PairCopiedState {
	if t == nil {
		return nil
	}
	if t.dataDeps != nil && t.dataDeps.Market != nil {
		return t.dataDeps.Market.PairCopied
	}
	if deps := t.RuntimeDependencies(); deps != nil && deps.Market != nil {
		return deps.Market.PairCopied
	}
	return nil
}

func closeProviderInstance(dp *data.LiveProvider, join bool) {
	if dp == nil {
		return
	}
	if err := dp.Stop(); err != nil {
		log.Error("stop live provider fail", zap.Error(err))
	}
	if join {
		dp.Join()
	}
}

func closeProviderInstances(providers []*data.LiveProvider, join bool) {
	for _, dp := range providers {
		closeProviderInstance(dp, join)
	}
}

func (t *CryptoTrader) batchStateForRun() *strat.BatchState {
	return t.Trader.BatchState()
}

func (t *CryptoTrader) orderCB(od *ormo.InOutOrder, isEnter bool) {
	var orders *ormo.OrderState
	var notifications *rpc.Session
	if deps := t.RuntimeDependencies(); deps != nil {
		orders = deps.Orders
		notifications = deps.Notifications
	}
	sendOrderMsgWithRuntime(od, isEnter, orders, notifications)
}

func (t *CryptoTrader) startJobs() {
	dp := t.provider()
	if dp == nil {
		return
	}
	deps := t.RuntimeDependencies()
	if deps != nil && t.runtime == nil {
		log.Error("explicit live runtime jobs require a lifecycle")
		return
	}
	if t.envReal() {
		// Listen to account order flow, process user orders, and consume order queues
		// 监听账户订单流、处理用户下单、消费订单队列
		if deps != nil {
			lifecycle := &runtimeShutdownLifecycle{owner: t}
			lifecycle.OnClose(func() { biz.StopLiveOdMgrWithRuntimeDeps(*deps) })
			biz.StartLiveOdMgrWithRuntimeDeps(*deps, t.runContext())
			lifecycle.OnCloseWait(func() { biz.JoinLiveOdMgrWithRuntimeDeps(*deps) })
		} else if t.runtime != nil {
			lifecycle := &runtimeShutdownLifecycle{owner: t}
			lifecycle.OnClose(biz.StopLiveOdMgr)
			biz.StartLiveOdMgrWithContext(t.runContext())
			lifecycle.OnCloseWait(biz.JoinLiveOdMgr)
		} else {
			biz.StartLiveOdMgr()
		}
	}
	scheduler := t.schedulerForRun()
	t.markUnWarm()
	// Refresh trading pairs regularly
	// 定期刷新交易对
	cronRefresh := func() error {
		return t.bootstrapThirdPartySources(t.runContext())
	}
	if deps != nil && t.coreStateForRun() != nil {
		cronRefreshPairsWithRuntime(scheduler, t, dp, deps, cronRefresh)
		var catalog *data.DataSourceCatalog
		if t.dataDeps != nil {
			catalog = t.dataDeps.Catalog
		}
		fetchHourKlinesWithRuntime(scheduler, dp, deps, catalog)
		cronLoadMarketsWithRuntime(scheduler, deps.Exchange, deps.Symbols, deps.Config, deps.Core)
		cronFatalLossCheckWithRuntime(scheduler, *deps, t.currentTimeMS)
		cronKlineDelaysWithRuntime(scheduler, dp, t.pairCopiedStateForRun(), t.currentTimeMS, *deps)
		cronKlineSummaryWithRuntime(scheduler, deps.Core)
		if cfg := runtimeConfig(deps); cfg != nil {
			dataDir := ""
			if deps.Config != nil {
				dataDir = deps.Config.DataDir
			}
			cronDumpStratOutputsWithRuntime(scheduler, deps.Strategies, cfg, dataDir)
		}
	} else {
		cronRefreshPairs(scheduler, dp, t.symbols, cronRefresh)
		// 定时加载1h及以上周期K线
		fetchHourKlines(scheduler, dp, t.symbols)
		// Refresh the market regularly
		cronLoadMarkets(scheduler)
		// Check every 5 minutes to see if the global stop loss is triggered
		cronFatalLossCheck(scheduler)
		// Regularly check the candlestick timeout, updated every minute
		cronKlineDelays(scheduler, dp, t.pairCopiedStateForRun(), t.currentTimeMS)
		// The timer output is executed every 5 minutes: 01:30 06:30 11:30
		cronKlineSummary(scheduler)
		// 每分钟定时输出策略Outputs信息到BanDataDir/logs/[name]_[strat].log
		cronDumpStratOutputs(scheduler)
	}
	// 实盘中定期回测对比
	if deps != nil {
		cronBacktestInLiveWithRuntime(scheduler, *deps)
	} else {
		cronBacktestInLive(scheduler)
	}
	if t.envReal() {
		// Check if the limit order submission is triggered at 15th secs of every minute
		// 每分钟第15s检查是否触发限价单提交
		if deps != nil {
			cronCheckTriggerOdsWithRuntime(scheduler, *deps)
		} else {
			cronCheckTriggerOds(scheduler)
		}
		// Regularly update balance and synchronize exchange positions with local orders
		// 定期更新余额，同步交易所持仓到本地订单
		if deps != nil {
			lifecycle := &runtimeShutdownLifecycle{owner: t}
			StartLoopBalancePositionsWithRuntime(lifecycle, *deps)
		} else if deps == nil {
			StartLoopBalancePositions()
		}
		// 定期保存实盘钱包快照
		if deps != nil {
			biz.StartLiveWalletSnapshotsWithRuntimeDeps(*deps, &runtimeShutdownLifecycle{owner: t})
		} else if deps == nil && t.runtime != nil {
			biz.StartLiveWalletSnapshots(&runtimeShutdownLifecycle{owner: t})
		} else if deps == nil {
			biz.StartLiveWalletSnapshots()
		}
	}
	if scheduler != nil {
		scheduler.Start()
	}
}

func (t *CryptoTrader) markUnWarm() {
	if deps := t.RuntimeDependencies(); deps != nil {
		state := deps.Strategies
		if state == nil || strat.IsLegacyState(state) {
			return
		}
		for _, job := range state.CollectJobs() {
			job.SetWarmUp(false)
		}
		return
	}
	if state := t.strategyStateForRun(); state != nil {
		for _, job := range state.CollectJobs() {
			job.SetWarmUp(false)
		}
		return
	}
	strat.LockJobsRead()
	for _, accMap := range strat.AccJobs {
		for _, jobMap := range accMap {
			for _, job := range jobMap {
				job.SetWarmUp(false)
			}
		}
	}
	strat.UnlockJobsRead()
}

func exitCleanUp(scheduler com.Scheduler) {
	exitCleanUpWithExchange(scheduler, exg.Default, nil)
}

func exitCleanUpWithExchange(scheduler com.Scheduler, exchange banexg.BanExchange, strategyState *strat.State) {
	exitCleanUpWithRuntime(scheduler, exchange, strategyState, nil)
}

func exitCleanUpWithRuntime(scheduler com.Scheduler, exchange banexg.BanExchange, strategyState *strat.State, deps *biz.RuntimeDeps) {
	if scheduler != nil {
		if stop := scheduler.Stop(); stop != nil {
			<-stop.Done()
		}
	}
	if deps == nil {
		orm.FlushDumps()
		orm.CloseDump()
	}
	var err *errs.Error
	if deps != nil {
		err = biz.CleanUpOdMgrWithState(deps.Trading)
	} else {
		err = biz.CleanUpOdMgr()
	}
	if err != nil {
		log.Error("clean odMgr fail", zap.Error(err))
	}
	if deps != nil && deps.Dump != nil {
		if dumpErr := deps.Dump.Flush(); dumpErr != nil {
			log.Error("flush runtime dump fail", zap.Error(dumpErr))
		}
	}
	if deps == nil {
		strat.ExitStratJobsWithState(strategyState)
	} else if strategyState != nil && !strat.IsLegacyState(strategyState) {
		strat.ExitStratJobsWithState(strategyState)
	}
	if deps == nil && exchange != nil {
		err = exchange.Close()
	}
	if err != nil {
		log.Error("close exg fail", zap.Error(err))
	}
	var accounts map[string]*config.AccountConfig
	var orders *ormo.OrderState
	if deps != nil {
		accounts = deps.AccountConfigs()
		orders = deps.Orders
	} else {
		accounts = config.Accounts
		orders = ormo.LegacyState()
	}
	for account, cfg := range accounts {
		if cfg == nil || cfg.NoTrade {
			continue
		}
		if orders == nil {
			continue
		}
		openOds, lock := orders.GetOpenODs(account)
		lock.Lock()
		openNum := len(openOds)
		lock.Unlock()
		msg := fmt.Sprintf("bot stop, %d orders opened", openNum)
		sendRuntimeMessage(deps, map[string]interface{}{
			"type":    rpc.MsgTypeStatus,
			"account": account,
			"status":  msg,
		})
	}
	if deps == nil {
		rpc.CleanUp()
	} else if deps.Notifications != nil {
		deps.Notifications.Close()
	}
}

func sendRuntimeMessage(deps *biz.RuntimeDeps, message map[string]interface{}) {
	if deps == nil {
		rpc.SendMsg(message)
	} else if deps.Notifications != nil {
		deps.Notifications.SendMsg(message)
	}
}
