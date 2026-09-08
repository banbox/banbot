package runtime

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
)

const unconfiguredIdentity = "<runtime-unconfigured>"

type closePhase uint8

const (
	closeOpen closePhase = iota
	closeStopping
	closeWaiting
	closeResetting
	closeClosed
)

// Process owns only process-scoped construction state. Runtime data never
// lives here, so one Process can create independent runtimes.
type Process struct {
	nextID atomic.Uint64

	runtimeMu           sync.Mutex
	runtimes            []*Runtime
	runtimeConstructing int
	runtimeCond         *sync.Cond
	closed              bool
	closeDone           chan struct{}

	symbolAllocatorMu sync.Mutex
	symbolAllocators  map[string]*orm.SIDAllocator
	sidRegistryMu     sync.Mutex
	sidRegistries     map[string]*orm.SymbolSIDRegistry
}

func NewProcess() *Process {
	process := &Process{
		symbolAllocators: make(map[string]*orm.SIDAllocator),
		sidRegistries:    make(map[string]*orm.SymbolSIDRegistry),
	}
	process.runtimeCond = sync.NewCond(&process.runtimeMu)
	return process
}

func (p *Process) runtimeConditionLocked() *sync.Cond {
	if p.runtimeCond == nil {
		p.runtimeCond = sync.NewCond(&p.runtimeMu)
	}
	return p.runtimeCond
}

func (p *Process) beginRuntimeConstruction() error {
	p.runtimeMu.Lock()
	defer p.runtimeMu.Unlock()
	if p.closed {
		return fmt.Errorf("runtime: process is closed")
	}
	p.runtimeConstructing++
	return nil
}

func (p *Process) finishRuntimeConstruction() {
	p.runtimeMu.Lock()
	p.runtimeConstructing--
	if p.runtimeConstructing == 0 {
		p.runtimeConditionLocked().Broadcast()
	}
	p.runtimeMu.Unlock()
}

func (p *Process) initSIDRegistry(url string, autoCreate bool) (*orm.SymbolSIDRegistry, error) {
	if p == nil || strings.TrimSpace(url) == "" {
		return nil, nil
	}
	url = strings.TrimSpace(url)
	p.sidRegistryMu.Lock()
	defer p.sidRegistryMu.Unlock()
	if p.sidRegistries == nil {
		p.sidRegistries = make(map[string]*orm.SymbolSIDRegistry)
	}
	if registry := p.sidRegistries[url]; registry != nil {
		if registry.AutoCreate() != autoCreate {
			return nil, fmt.Errorf("runtime: SID registry %q already uses auto-create=%t, requested %t", url, registry.AutoCreate(), autoCreate)
		}
		return registry, nil
	}
	registry, err := orm.NewSymbolSIDRegistry(url, autoCreate)
	if err != nil {
		return nil, err
	}
	p.sidRegistries[url] = registry
	return registry, nil
}

func (p *Process) initSymbolAllocator(namespace, dataDir string, registry *orm.SymbolSIDRegistry) (*orm.SIDAllocator, error) {
	if p == nil {
		return nil, fmt.Errorf("runtime: nil Process")
	}
	p.symbolAllocatorMu.Lock()
	defer p.symbolAllocatorMu.Unlock()
	if p.symbolAllocators == nil {
		p.symbolAllocators = make(map[string]*orm.SIDAllocator)
	}
	allocator := p.symbolAllocators[namespace]
	if allocator == nil {
		allocator = orm.NewSIDAllocatorForStorageWithRegistry(namespace, dataDir, registry)
		p.symbolAllocators[namespace] = allocator
	} else if err := allocator.BindSIDRegistry(registry); err != nil {
		return nil, err
	}
	return allocator, nil
}

// Close releases process-owned low-frequency dependencies after all runtimes
// created by this Process have stopped. NewRuntime calls already in progress
// are allowed to finish construction, then are either registered or closed if
// this Process has started closing.
func (p *Process) Close() {
	if p == nil {
		return
	}
	p.runtimeMu.Lock()
	if p.closed {
		done := p.closeDone
		p.runtimeMu.Unlock()
		if done != nil {
			<-done
		}
		return
	}
	p.closed = true
	p.closeDone = make(chan struct{})
	done := p.closeDone
	condition := p.runtimeConditionLocked()
	for p.runtimeConstructing > 0 {
		condition.Wait()
	}
	runtimes := slices.Clone(p.runtimes)
	p.runtimeMu.Unlock()

	for _, runtime := range runtimes {
		if runtime != nil {
			runtime.Close()
		}
	}
	for _, runtime := range runtimes {
		if runtime != nil {
			runtime.Join()
		}
	}

	p.sidRegistryMu.Lock()
	registries := make([]*orm.SymbolSIDRegistry, 0, len(p.sidRegistries))
	for _, registry := range p.sidRegistries {
		registries = append(registries, registry)
	}
	p.sidRegistries = make(map[string]*orm.SymbolSIDRegistry)
	p.sidRegistryMu.Unlock()
	for _, registry := range registries {
		registry.Close()
	}
	close(done)
}

type Options struct {
	ID          string
	Mode        string
	Env         string
	StartAt     int64
	Context     context.Context
	Config      *config.Config
	DataDir     string
	StrategyDir string
	// StorageNamespace explicitly distinguishes allocator ownership. When empty,
	// allocator ownership is derived from canonical database identity; DataDir
	// remains a recovery root and must not split allocators for the same DB.
	StorageNamespace string
	Exchange         banexg.BanExchange
	ExchangeName     string
	Market           string
	ContractType     string
	DisplayLocation  *time.Location
	NumTaCache       int
	ConcurNum        int
	Pairs            []string
	NetDisable       bool
	ParallelOnBar    bool
	Scheduler        com.Scheduler
}

// Runtime is the typed composition root for the first migration slice. The
// remaining domain managers will be added here as they leave their legacy
// facades; no domain package imports runtime.
type Runtime struct {
	Process    *Process
	ID         string
	Core       *core.State
	Config     *config.Snapshot
	Clock      *btime.ClockState
	Market     *com.MarketState
	Symbols    *orm.SymbolState
	Batch      *strat.BatchState
	Strategies *strat.State
	Orders     *ormo.OrderState
	Trading    *biz.TradingState
	Cron       com.Scheduler
	// Exchange is a runtime dependency, not an ownership claim. The entry
	// layer decides when the adapter session is closed.
	Exchange banexg.BanExchange

	closeMu           sync.Mutex
	closeDone         chan struct{}
	closeWaiters      []func()
	closeWaitNotify   chan struct{}
	schedulerStop     context.Context
	schedulerStopping bool
	schedulerStopped  bool
	stopDone          chan struct{}
	closePhase        closePhase
	stopActive        bool
	closeRequested    bool
	activeCallbacks   atomic.Int64
	callbackWait      sync.WaitGroup
}

func (p *Process) NewRuntime(opts Options) (*Runtime, error) {
	if p == nil {
		return nil, fmt.Errorf("runtime: nil Process")
	}
	if err := p.beginRuntimeConstruction(); err != nil {
		return nil, err
	}
	defer p.finishRuntimeConstruction()

	runtimeOrdinal := p.nextID.Add(1)
	contractTypeExplicit := opts.ContractType != ""
	snapshot := config.NewSnapshotWithDirs(opts.Config, opts.DataDir, opts.StrategyDir)
	snapshotConfig := snapshot.View()
	if snapshotConfig != nil {
		if opts.ExchangeName == "" && snapshotConfig.Exchange != nil {
			opts.ExchangeName = snapshotConfig.Exchange.Name
		}
		if opts.Market == "" {
			opts.Market = snapshotConfig.MarketType
		}
		if opts.ContractType == "" {
			opts.ContractType = snapshotConfig.ContractType
		}
		if opts.Env == "" {
			opts.Env = snapshotConfig.Env
		}
		if opts.ConcurNum <= 0 && snapshotConfig.ConcurNum > 0 {
			opts.ConcurNum = snapshotConfig.ConcurNum
		}
	}
	if (opts.ExchangeName == "") != (opts.Market == "") {
		return nil, fmt.Errorf("runtime: exchange and market must be provided together")
	}
	symbolExchange, symbolMarket := opts.ExchangeName, opts.Market
	if symbolExchange == "" {
		// Keep identity-free runtimes usable for non-symbol capabilities without
		// allowing SymbolState to fall back to package globals.
		symbolExchange, symbolMarket = unconfiguredIdentity, unconfiguredIdentity
	}
	if !contractTypeExplicit && !banexg.IsContract(opts.Market) {
		opts.ContractType = ""
	}
	if opts.ContractType == "" && banexg.IsContract(opts.Market) {
		opts.ContractType = banexg.MarketSwap
	}
	scheduler := opts.Scheduler
	if scheduler == nil {
		scheduler = com.NewScheduler()
	}
	id := opts.ID
	if id == "" {
		id = fmt.Sprintf("runtime-%d", runtimeOrdinal)
	}
	storageNamespace := runtimeStorageNamespace(opts, snapshot)
	if storageNamespace == "" {
		// No storage identity means this runtime has no safe sharing boundary.
		// Keep its allocator private instead of merging it into a literal default.
		storageNamespace = fmt.Sprintf("runtime:%d", runtimeOrdinal)
	}
	var sidRegistry *orm.SymbolSIDRegistry
	if snapshotConfig != nil && snapshotConfig.Database != nil {
		var registryErr error
		sidRegistry, registryErr = p.initSIDRegistry(snapshotConfig.Database.SIDRegistryURL, snapshotConfig.Database.AutoCreate)
		if registryErr != nil {
			return nil, fmt.Errorf("runtime: initialize SID registry: %w", registryErr)
		}
	}
	coreState, err := core.NewState(opts.Context)
	if err != nil {
		return nil, err
	}
	activePairs := opts.Pairs
	if activePairs == nil && snapshotConfig != nil {
		activePairs = snapshotConfig.Pairs
	}
	activePairs = slices.Clone(activePairs)
	if opts.Exchange != nil {
		// Runtime pairs are already canonical symbols. The exchange metadata
		// parser expects raw exchange IDs, so use the runtime-aware resolver at
		// this boundary instead of mapping canonical symbols a second time.
		for _, pair := range activePairs {
			if _, err := exg.ResolveRuntimePriceSymbol(opts.Exchange, pair); err != nil {
				return nil, fmt.Errorf("runtime: validate configured symbols: %w", err)
			}
		}
	}
	if opts.Mode == "" {
		opts.Mode = core.RunModeOther
	}
	if opts.Env == "" {
		opts.Env = core.RunEnvDryRun
	}
	coreState.SetRunMode(opts.Mode)
	coreState.SetRunEnv(opts.Env)
	coreState.StartAt = opts.StartAt
	coreState.ExgName = opts.ExchangeName
	coreState.Market = opts.Market
	coreState.ContractType = opts.ContractType
	coreState.IsContract = banexg.IsContract(opts.Market)
	coreState.NetDisable = opts.NetDisable
	coreState.ParallelOnBar = opts.ParallelOnBar
	if opts.NumTaCache > 0 {
		coreState.NumTaCache = opts.NumTaCache
	}
	if opts.ConcurNum > 0 {
		coreState.ConcurNum = opts.ConcurNum
	}
	coreState.SetPairs(activePairs, nil)
	clock := btime.NewClockState(coreState.BackTestMode, opts.DisplayLocation)
	if opts.StartAt != 0 {
		clock.SetTimeMS(opts.StartAt)
	}
	allocator, allocatorErr := p.initSymbolAllocator(storageNamespace, opts.DataDir, sidRegistry)
	if allocatorErr != nil {
		coreState.Close()
		return nil, allocatorErr
	}
	symbols := orm.NewSymbolStateWithAllocatorAndIdentity(allocator, symbolExchange, symbolMarket)
	if opts.DataDir != "" {
		if err := orm.BindExSymbolRecoveryDir(symbols, opts.DataDir); err != nil {
			coreState.Close()
			return nil, fmt.Errorf("runtime: bind symbol recovery directory: %w", err)
		}
	}
	runtime := &Runtime{
		Process:    p,
		ID:         id,
		Core:       coreState,
		Config:     snapshot,
		Clock:      clock,
		Market:     com.NewMarketStateWithExchange(opts.ExchangeName, opts.Exchange),
		Symbols:    symbols,
		Batch:      strat.NewBatchState(),
		Strategies: strat.NewState(),
		Orders:     ormo.NewOrderState(),
		Trading:    biz.NewTradingState(),
		Cron:       scheduler,
		Exchange:   opts.Exchange,
		closeDone:  make(chan struct{}),
	}
	runtime.Orders.BindCore(coreState)

	p.runtimeMu.Lock()
	if p.closed {
		p.runtimeMu.Unlock()
		runtime.Close()
		runtime.Join()
		return nil, fmt.Errorf("runtime: process is closed")
	}
	p.runtimes = append(p.runtimes, runtime)
	p.runtimeMu.Unlock()
	return runtime, nil
}

func runtimeStorageNamespace(opts Options, snapshot *config.Snapshot) string {
	if namespace := strings.TrimSpace(opts.StorageNamespace); namespace != "" {
		return "explicit:" + namespace
	}
	databaseIdentity := ""
	if snapshot != nil {
		cfg := snapshot.View()
		if cfg != nil && cfg.Database != nil {
			databaseIdentity = orm.CanonicalDatabaseIdentityForType(cfg.Database.Url, snapshot.DataDir, cfg.Database.DbType)
		}
	}
	if databaseIdentity != "" {
		return databaseIdentity
	}
	dataDir := strings.TrimSpace(opts.DataDir)
	if snapshot != nil && strings.TrimSpace(snapshot.DataDir) != "" {
		dataDir = strings.TrimSpace(snapshot.DataDir)
	}
	if dataDir == "" {
		return ""
	}
	dataDir = filepath.Clean(dataDir)
	if absolute, err := filepath.Abs(dataDir); err == nil {
		dataDir = absolute
	}
	return "data-dir:" + dataDir
}

func (r *Runtime) Context() context.Context {
	if r == nil || r.Core == nil {
		return nil
	}
	return r.Core.Context()
}

// EnterCallback admits a callback that may call Runtime.Close. Close changes
// the phase before it waits for callbacks, so a callback closing its own
// runtime can request asynchronous close without waiting for itself.
func (r *Runtime) EnterCallback() bool {
	if r == nil {
		return false
	}
	r.closeMu.Lock()
	if r.closePhase != closeOpen {
		r.closeMu.Unlock()
		return false
	}
	r.activeCallbacks.Add(1)
	r.callbackWait.Add(1)
	r.closeMu.Unlock()
	return true
}

// LeaveCallback releases a callback admitted by EnterCallback.
func (r *Runtime) LeaveCallback() {
	if r == nil {
		return
	}
	r.activeCallbacks.Add(-1)
	r.callbackWait.Done()
}

func (r *Runtime) Done() <-chan struct{} {
	if r == nil || r.Core == nil {
		return nil
	}
	return r.Core.Done()
}

// Scheduler returns the scheduler owned by this Runtime. Legacy callers keep
// using com.Cron; typed runners must schedule work on this instance.
func (r *Runtime) Scheduler() com.Scheduler {
	if r == nil {
		return nil
	}
	return r.Cron
}

// OnClose registers a low-frequency lifecycle hook. Hooks run once after the
// runtime cancellation signal is published and before owned state is reset.
// A hook registered after shutdown runs immediately.
func (r *Runtime) OnClose(call func()) {
	if r == nil || r.Core == nil {
		return
	}
	r.Core.OnExit(call)
}

// OnCloseWait registers an owner-side join hook. Stop callbacks must remain
// non-blocking because they can be invoked by a worker callback; Close runs
// these hooks after cancellation and before resetting Runtime-owned state.
// A hook registered while waiting is tracked before reset; one registered after
// reset starts waits for Close to finish and then runs immediately.
func (r *Runtime) OnCloseWait(call func()) {
	if r == nil || call == nil {
		return
	}
	r.closeMu.Lock()
	switch r.closePhase {
	case closeOpen, closeStopping, closeWaiting:
		r.closeWaiters = append(r.closeWaiters, call)
		if r.closePhase == closeWaiting {
			if r.closeWaitNotify == nil {
				r.closeWaitNotify = make(chan struct{})
			}
			close(r.closeWaitNotify)
			r.closeWaitNotify = make(chan struct{})
		}
		r.closeMu.Unlock()
		return
	case closeResetting:
		done := r.closeDone
		r.closeMu.Unlock()
		if done != nil {
			<-done
		}
		call()
		return
	case closeClosed:
		r.closeMu.Unlock()
		call()
	default:
		r.closeMu.Unlock()
		call()
	}
}

// Stop publishes cancellation and runs OnClose hooks. It does not wait for
// OnCloseWait hooks or reset Runtime-owned state; Close owns that phase. Stop
// is non-blocking for a concurrent or recursive call while the owner is
// running the synchronous stop hooks.
func (r *Runtime) Stop() {
	if r == nil {
		return
	}
	r.closeMu.Lock()
	if r.stopActive {
		r.closeMu.Unlock()
		return
	}
	r.stopActive = true
	r.stopDone = make(chan struct{})
	r.closeMu.Unlock()
	defer r.finishStop()
	r.stopScheduler()
	if r.Core != nil {
		r.Core.Stop()
	}
}

// finishStop releases the stop owner. A Close request that arrived while the
// stop hooks were running is promoted to the close owner here, after all stop
// callbacks have returned.
func (r *Runtime) finishStop() {
	if r == nil {
		return
	}
	var takeClose bool
	r.closeMu.Lock()
	r.stopActive = false
	if done := r.stopDone; done != nil {
		close(done)
		r.stopDone = nil
	}
	if r.closeRequested {
		if r.closePhase == closeOpen {
			if r.closeDone == nil {
				r.closeDone = make(chan struct{})
			}
			r.closePhase = closeStopping
			takeClose = true
		}
	}
	r.closeMu.Unlock()
	if takeClose {
		// Stop may be running from a scheduler/provider callback. Its join
		// context can include that callback, so the callback cannot own the
		// continuation or wait for closeOwned itself.
		go r.closeOwned()
	}
}

// Close has one owner: the first caller runs Stop, joins OnCloseWait hooks, and
// resets owned state. Calls arriving while Stop is active record a close
// request and return so an OnClose hook can safely reenter Close. Calls from
// an active callback lease also return; Join waits for the owner to finish.
func (r *Runtime) Close() {
	if r == nil {
		return
	}
	r.closeMu.Lock()
	if r.stopActive {
		if r.closeDone == nil {
			r.closeDone = make(chan struct{})
		}
		r.closeRequested = true
		r.closeMu.Unlock()
		return
	}
	switch r.closePhase {
	case closeOpen:
		if r.closeDone == nil {
			r.closeDone = make(chan struct{})
		}
		r.closePhase = closeStopping
		async := r.activeCallbacks.Load() > 0
		r.closeMu.Unlock()
		if async {
			// Publish cancellation before returning to a callback. The callback
			// lease keeps reset in closeOwned until the callback has unwound.
			r.Stop()
			go r.closeOwned()
			return
		}
		r.closeOwned()
	case closeStopping:
		if r.activeCallbacks.Load() > 0 {
			r.closeMu.Unlock()
			return
		}
		// Reentrant calls from a stop callback are handled by the stopActive
		// branch above. Once Stop has released that flag, every other caller
		// must wait for the close owner to finish.
		done := r.closeDone
		r.closeMu.Unlock()
		if done != nil {
			<-done
		}
	case closeWaiting:
		if r.activeCallbacks.Load() > 0 {
			r.closeMu.Unlock()
			return
		}
		done := r.closeDone
		r.closeMu.Unlock()
		if done != nil {
			<-done
		}
	case closeResetting, closeClosed:
		if r.closePhase == closeResetting && r.activeCallbacks.Load() > 0 {
			r.closeMu.Unlock()
			return
		}
		done := r.closeDone
		r.closeMu.Unlock()
		if done != nil {
			<-done
		}
	}
}

// Join waits for a requested Close to finish. It is a no-op before Close starts
// and during an unrequested synchronous stop phase. Stop-hook owner callbacks
// must not call Join: a requested close can only finish after the callback
// returns, so doing so would wait on the caller itself.
func (r *Runtime) Join() {
	if r == nil {
		return
	}
	r.closeMu.Lock()
	if r.closePhase == closeOpen && !r.closeRequested {
		r.closeMu.Unlock()
		return
	}
	done := r.closeDone
	r.closeMu.Unlock()
	if done != nil {
		<-done
	}
}

func (r *Runtime) closeOwned() {
	defer r.finishClose()

	// Stop scheduler admission before canceling domain components. Stop runs the
	// Core hooks synchronously and preserves their registration order.
	r.Stop()
	r.waitStop()

	r.closeMu.Lock()
	r.closePhase = closeWaiting
	if r.closeWaitNotify == nil {
		r.closeWaitNotify = make(chan struct{})
	}
	schedulerJoined := r.schedulerStop == nil
	r.closeMu.Unlock()
	for {
		r.closeMu.Lock()
		waiters := r.closeWaiters
		r.closeWaiters = nil
		r.closeMu.Unlock()
		for _, wait := range waiters {
			if wait != nil {
				wait()
			}
		}
		if len(waiters) > 0 {
			continue
		}

		r.closeMu.Lock()
		if len(r.closeWaiters) > 0 {
			r.closeMu.Unlock()
			continue
		}
		if schedulerJoined {
			r.closePhase = closeResetting
			r.closeMu.Unlock()
			break
		}
		stop := r.schedulerStop
		notify := r.closeWaitNotify
		r.closeMu.Unlock()

		select {
		case <-stop.Done():
			schedulerJoined = true
		case <-notify:
		}
	}
	// Provider joins normally cover the callback source, but keep the lease
	// barrier here so every callback source obeys the same reset-before-return
	// invariant.
	r.callbackWait.Wait()
	if r.Market != nil {
		r.Market.Reset()
	}
	if r.Symbols != nil {
		r.Symbols.Reset()
	}
	if r.Batch != nil {
		r.Batch.Reset()
	}
	if r.Strategies != nil {
		r.Strategies.Reset()
	}
	if r.Orders != nil {
		r.Orders.Reset()
	}
	if r.Trading != nil {
		r.Trading.Reset()
	}
	if r.Core != nil {
		r.Core.Close()
	}
}

func (r *Runtime) waitStop() {
	if r == nil {
		return
	}
	r.closeMu.Lock()
	done := r.stopDone
	active := r.stopActive
	r.closeMu.Unlock()
	if active && done != nil {
		<-done
	}
}

func (r *Runtime) stopScheduler() {
	if r == nil || r.Cron == nil {
		return
	}
	r.closeMu.Lock()
	if r.schedulerStopped || r.schedulerStopping {
		r.closeMu.Unlock()
		return
	}
	r.schedulerStopping = true
	scheduler := r.Cron
	r.closeMu.Unlock()
	stop := scheduler.Stop()
	r.closeMu.Lock()
	r.schedulerStop = stop
	r.schedulerStopping = false
	r.schedulerStopped = true
	r.closeMu.Unlock()
}

func (r *Runtime) finishClose() {
	r.closeMu.Lock()
	if r.closePhase != closeClosed {
		if r.closeDone == nil {
			r.closeDone = make(chan struct{})
		}
		r.closeRequested = false
		r.closePhase = closeClosed
		close(r.closeDone)
	}
	r.closeMu.Unlock()
}
