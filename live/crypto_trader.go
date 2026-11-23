package live

import (
	"context"
	"sync"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/web"
	"github.com/banbox/banexg/errs"
)

// CryptoTrader is the shell an execution-backed robot borrows from the live
// package: it owns the dashboard API server, the runtime scheduler handle and
// the shutdown phases. It places no orders and drives no data provider; the
// legacy live order stack that used to run here (LiveOrderMgr, Init/Run) was
// removed, see doc/project/05_biz_orders_live.md.
type CryptoTrader struct {
	biz.Trader
	dataDeps        *data.RuntimeDeps
	apiServer       *web.ApiServer
	scheduler       com.Scheduler
	runtime         RuntimeLifecycle
	shutdownLock    sync.Mutex
	shutdownJoins   []func()
	shutdownStops   []func()
	shutdownStarted bool
	nowMSFn         func() int64
}

// CryptoTraderStartupFunc is invoked by the entries once the trader exists and
// before the robot starts feeding data.
type CryptoTraderStartupFunc func(ctx context.Context, trader *CryptoTrader) error

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

// NewCryptoTraderWithRuntimeDeps binds the trader shell to one explicit
// runtime. The lifecycle must come from the same dependency owner.
func NewCryptoTraderWithRuntimeDeps(deps biz.RuntimeDeps) (*CryptoTrader, *errs.Error) {
	boundTrader, err := biz.NewTraderWithRuntimeDeps(deps)
	if err != nil {
		return nil, err
	}
	lifecycle, ok := deps.Callbacks.(RuntimeLifecycle)
	if !ok {
		return nil, errs.NewMsg(core.ErrRunTime, "explicit live runtime callback owner must provide its lifecycle")
	}
	trader := &CryptoTrader{Trader: boundTrader}
	bound := trader.RuntimeDependencies()
	if err := validateCryptoTraderRuntimeDeps(bound); err != nil {
		return nil, err
	}
	trader.dataDeps = bound.DataDeps()
	if trader.dataDeps.IdentityErr != nil {
		return nil, errs.New(core.ErrRunTime, trader.dataDeps.IdentityErr)
	}
	return bindCryptoTraderRuntime(trader, lifecycle), nil
}

func bindCryptoTraderRuntime(trader *CryptoTrader, lifecycle RuntimeLifecycle) *CryptoTrader {
	if lifecycle != nil {
		trader.runtime = lifecycle
		if owner, ok := lifecycle.(interface{ Scheduler() com.Scheduler }); ok {
			trader.scheduler = owner.Scheduler()
		}
		if trader.scheduler == nil {
			if deps := trader.RuntimeDependencies(); deps != nil {
				trader.scheduler = deps.Scheduler
			}
		}
		lifecycle.OnCloseWait(trader.runShutdownPhases)
	}
	return trader
}

func validateCryptoTraderRuntimeDeps(deps *biz.RuntimeDeps) *errs.Error {
	if deps == nil {
		return nil
	}
	if deps.Exchange == nil {
		return errs.NewMsg(core.ErrRunTime, "explicit live runtime requires exchange")
	}
	return nil
}

// StartWebAPI starts the dashboard API of an execution-backed robot: it drives
// its own provider and order manager and only borrows the trader for its API
// server and shutdown phases. It is a no-op when api_server is disabled.
func (t *CryptoTrader) StartWebAPI() *errs.Error {
	if t.RuntimeDependencies() == nil {
		return errs.NewMsg(core.ErrBadConfig, "web api requires a runtime-bound trader")
	}
	return t.startWebAPI()
}

// PublishSeries pushes one live row to dashboard subscribers. It is the
// observer an execution-backed runtime connects to its feed; without a running
// API server it does nothing.
func (t *CryptoTrader) PublishSeries(msg *data.SeriesMsg) {
	if t != nil && t.apiServer != nil && msg != nil {
		t.apiServer.PublishSeries(msg)
	}
}

func (t *CryptoTrader) startWebAPI() *errs.Error {
	deps := t.RuntimeDependencies()
	if deps == nil {
		return errs.NewMsg(core.ErrBadConfig, "web api requires a runtime-bound trader")
	}
	var err *errs.Error
	t.apiServer, err = web.StartApiWithRuntimeDeps(&runtimeShutdownLifecycle{owner: t}, *deps)
	return err
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

func (t *CryptoTrader) schedulerForRun() com.Scheduler {
	if t == nil {
		return nil
	}
	return t.scheduler
}

func (t *CryptoTrader) registerShutdownJoin(call func()) {
	if t == nil || call == nil {
		return
	}
	t.shutdownLock.Lock()
	if t.shutdownStarted {
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
	if t.shutdownStarted {
		t.shutdownLock.Unlock()
		call()
		return
	}
	t.shutdownStops = append(t.shutdownStops, call)
	t.shutdownLock.Unlock()
}

// stopAndJoinRegisteredResources closes every registered resource. Stops run
// before joins so callbacks cannot admit new work while a wait is in progress.
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

// runShutdownPhases is the sole runtime wait hook: it stops the registered
// components, then joins them.
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
	t.shutdownLock.Unlock()
	t.stopAndJoinRegisteredResources()
}
