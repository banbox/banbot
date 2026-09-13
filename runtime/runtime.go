package runtime

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
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
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"go.uber.org/zap"
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
	registered          bool
	closed              bool
	closeDone           chan struct{}

	symbolAllocatorMu sync.Mutex
	symbolAllocators  map[string]*orm.SIDAllocator
	sidRegistryMu     sync.Mutex
	sidRegistries     map[string]*orm.SymbolSIDRegistry
	schedulerMu       sync.Mutex
	schedulerClaims   []*schedulerClaim
}

// schedulerClaim records one Process binding to an externally supplied
// scheduler. A scheduler can be shared only when every binding explicitly
// marks it borrowed; otherwise Runtime.Close would have no safe way to stop
// only its own jobs.
type schedulerClaim struct {
	scheduler com.Scheduler
	borrowed  bool
	refs      int
}

// activeProcesses is a low-frequency lifecycle registry used by process
// signal handling. It contains only Process owners with at least one active
// Runtime; runtime data and dependencies remain owned by the Process itself.
var activeProcesses = struct {
	sync.Mutex
	items map[*Process]struct{}
}{items: make(map[*Process]struct{})}

func registerActiveProcess(process *Process) {
	if process == nil {
		return
	}
	activeProcesses.Lock()
	activeProcesses.items[process] = struct{}{}
	activeProcesses.Unlock()
}

func unregisterActiveProcess(process *Process) {
	if process == nil {
		return
	}
	activeProcesses.Lock()
	delete(activeProcesses.items, process)
	activeProcesses.Unlock()
}

// StopProcesses publishes cancellation to every active Process. The snapshot
// keeps registry locking out of Runtime stop hooks and preserves isolation
// between Runtime instances.
func StopProcesses() {
	activeProcesses.Lock()
	processes := make([]*Process, 0, len(activeProcesses.items))
	for process := range activeProcesses.items {
		processes = append(processes, process)
	}
	activeProcesses.Unlock()
	for _, process := range processes {
		process.Stop()
	}
}

// StopAndWaitProcesses publishes cancellation and waits for every active
// process owner to finish its Runtime cleanup. Callers use this at process
// shutdown; ordinary cancellation should continue to use StopProcesses when
// the caller owns the subsequent Close/Join boundary.
func StopAndWaitProcesses() {
	activeProcesses.Lock()
	processes := make([]*Process, 0, len(activeProcesses.items))
	for process := range activeProcesses.items {
		processes = append(processes, process)
	}
	activeProcesses.Unlock()
	for _, process := range processes {
		process.Stop()
		process.Close()
	}
}

func NewProcess() *Process {
	process := &Process{
		symbolAllocators: make(map[string]*orm.SIDAllocator),
		sidRegistries:    make(map[string]*orm.SymbolSIDRegistry),
	}
	process.runtimeCond = sync.NewCond(&process.runtimeMu)
	return process
}

// runtimeExecutionAccounts selects and clones the mutable account state once
// at the Runtime composition root. Downstream packages receive this owned map
// directly and never infer it from an immutable config snapshot.
func runtimeExecutionAccounts(state *core.State, snapshot *config.Snapshot, accounts map[string]*config.AccountConfig, defaultAccount string) map[string]*config.AccountConfig {
	if accounts == nil && snapshot != nil && snapshot.View() != nil {
		accounts = snapshot.View().Accounts
	}
	if state == nil || state.EnvReal {
		return config.CloneAccountConfigsForRuntime(accounts)
	}
	if defaultAccount == "" {
		defaultAccount = "default"
	}
	if len(accounts) == 0 {
		return map[string]*config.AccountConfig{defaultAccount: {}}
	}
	if account := accounts[defaultAccount]; account != nil {
		return config.CloneAccountConfigsForRuntime(map[string]*config.AccountConfig{defaultAccount: account})
	}
	names := make([]string, 0, len(accounts))
	for name, account := range accounts {
		if account != nil {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	if len(names) == 0 {
		return map[string]*config.AccountConfig{defaultAccount: {}}
	}
	return config.CloneAccountConfigsForRuntime(map[string]*config.AccountConfig{defaultAccount: accounts[names[0]]})
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

func (p *Process) unregisterRuntime(target *Runtime) {
	if p == nil || target == nil {
		return
	}
	p.runtimeMu.Lock()
	for index, runtime := range p.runtimes {
		if runtime != target {
			continue
		}
		copy(p.runtimes[index:], p.runtimes[index+1:])
		p.runtimes[len(p.runtimes)-1] = nil
		p.runtimes = p.runtimes[:len(p.runtimes)-1]
		if len(p.runtimes) == 0 && p.runtimeConstructing == 0 && p.registered {
			p.registered = false
			// Keep the process registry transition in the same critical
			// section as the owner state transition. NewRuntime uses this
			// lock order too; moving this call after Unlock loses a newly
			// registered runtime in a close/new interleaving.
			unregisterActiveProcess(p)
		}
		p.runtimeMu.Unlock()
		return
	}
	p.runtimeMu.Unlock()
}

func (p *Process) initSIDRegistry(url string, autoCreate bool) (*orm.SymbolSIDRegistry, error) {
	registry, _, err := p.initSIDRegistryOwned(url, autoCreate)
	return registry, err
}

// initSIDRegistryOwned is the construction-boundary variant of
// initSIDRegistry. The created flag lets NewRuntime roll back a registry that
// was allocated for a failed construction, while keeping shared registries
// alive for sibling runtimes.
func (p *Process) initSIDRegistryOwned(url string, autoCreate bool) (*orm.SymbolSIDRegistry, bool, error) {
	if p == nil || strings.TrimSpace(url) == "" {
		return nil, false, nil
	}
	url = strings.TrimSpace(url)
	p.sidRegistryMu.Lock()
	defer p.sidRegistryMu.Unlock()
	if p.sidRegistries == nil {
		p.sidRegistries = make(map[string]*orm.SymbolSIDRegistry)
	}
	if registry := p.sidRegistries[url]; registry != nil {
		if registry.AutoCreate() != autoCreate {
			return nil, false, fmt.Errorf("runtime: SID registry %q already uses auto-create=%t, requested %t", url, registry.AutoCreate(), autoCreate)
		}
		return registry, false, nil
	}
	registry, err := orm.NewSymbolSIDRegistry(url, autoCreate)
	if err != nil {
		return nil, false, err
	}
	p.sidRegistries[url] = registry
	return registry, true, nil
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

func sameScheduler(left, right com.Scheduler) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftType, rightType := reflect.TypeOf(left), reflect.TypeOf(right)
	if leftType != rightType {
		return false
	}
	if leftType.Comparable() {
		return left == right
	}
	leftValue, rightValue := reflect.ValueOf(left), reflect.ValueOf(right)
	if leftValue.Kind() == reflect.Pointer {
		return leftValue.Pointer() == rightValue.Pointer()
	}
	return false
}

func (p *Process) claimScheduler(scheduler com.Scheduler, borrowed bool) (*schedulerClaim, error) {
	if p == nil || scheduler == nil {
		return nil, nil
	}
	p.schedulerMu.Lock()
	defer p.schedulerMu.Unlock()
	for _, claim := range p.schedulerClaims {
		if claim == nil || !sameScheduler(claim.scheduler, scheduler) {
			continue
		}
		if !borrowed || !claim.borrowed {
			return nil, fmt.Errorf("runtime: scheduler is already bound; shared schedulers must be explicitly borrowed by every runtime")
		}
		claim.refs++
		return claim, nil
	}
	claim := &schedulerClaim{scheduler: scheduler, borrowed: borrowed, refs: 1}
	p.schedulerClaims = append(p.schedulerClaims, claim)
	return claim, nil
}

func (p *Process) releaseSchedulerClaim(claim *schedulerClaim) {
	if p == nil || claim == nil {
		return
	}
	p.schedulerMu.Lock()
	if claim.refs > 0 {
		claim.refs--
	}
	if claim.refs == 0 {
		for index, current := range p.schedulerClaims {
			if current != claim {
				continue
			}
			copy(p.schedulerClaims[index:], p.schedulerClaims[index+1:])
			p.schedulerClaims[len(p.schedulerClaims)-1] = nil
			p.schedulerClaims = p.schedulerClaims[:len(p.schedulerClaims)-1]
			break
		}
	}
	p.schedulerMu.Unlock()
}

func (p *Process) waitForRuntimeConstructions() {
	if p == nil {
		return
	}
	p.runtimeMu.Lock()
	condition := p.runtimeConditionLocked()
	for p.runtimeConstructing > 0 {
		condition.Wait()
	}
	p.runtimeMu.Unlock()
}

// releaseFailedRuntimeDependencies removes process-scoped state created by a
// failed construction after all sibling constructors have quiesced. A shared
// allocator or registry remains alive when a successfully registered Runtime
// still references it.
func (p *Process) releaseFailedRuntimeDependencies(namespace string, allocator *orm.SIDAllocator,
	registryURL string, registry *orm.SymbolSIDRegistry) {
	if p == nil {
		return
	}
	p.runtimeMu.Lock()
	if p.runtimeConstructing != 0 {
		p.runtimeMu.Unlock()
		return
	}
	allocatorUsed := false
	for _, runtime := range p.runtimes {
		if runtime != nil && runtime.Symbols != nil && runtime.Symbols.SIDAllocator() == allocator {
			allocatorUsed = true
			break
		}
	}
	p.symbolAllocatorMu.Lock()
	if allocator != nil && !allocatorUsed && strings.TrimSpace(namespace) != "" &&
		p.symbolAllocators[namespace] == allocator {
		delete(p.symbolAllocators, namespace)
	}
	registryUsed := false
	if registry != nil {
		for _, current := range p.symbolAllocators {
			if current != nil && current.SIDRegistry() == registry {
				registryUsed = true
				break
			}
		}
	}
	p.symbolAllocatorMu.Unlock()
	if registry != nil && !registryUsed {
		p.sidRegistryMu.Lock()
		if strings.TrimSpace(registryURL) != "" && p.sidRegistries[registryURL] == registry {
			delete(p.sidRegistries, registryURL)
			registry.Close()
		}
		p.sidRegistryMu.Unlock()
	}
	p.runtimeMu.Unlock()
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
	p.runtimeMu.Lock()
	if p.registered {
		p.registered = false
		unregisterActiveProcess(p)
	}
	p.runtimeMu.Unlock()
	close(done)
}

// Stop publishes cancellation for every Runtime currently owned by this
// Process. It is intentionally non-owning: callers still use Close/Join to
// wait for component cleanup and release process-scoped resources.
func (p *Process) Stop() {
	if p == nil {
		return
	}
	p.runtimeMu.Lock()
	runtimes := slices.Clone(p.runtimes)
	p.runtimeMu.Unlock()
	for _, runtime := range runtimes {
		if runtime != nil {
			runtime.Stop()
		}
	}
}

type Options struct {
	Logger      *zap.Logger
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
	Storage          *orm.Storage
	Exchange         banexg.BanExchange
	ExchangeName     string
	Market           string
	ContractType     string
	DisplayLocation  *time.Location
	SchedulerLang    string
	NumTaCache       int
	ConcurNum        int
	Pairs            []string
	NetDisable       bool
	ParallelOnBar    bool
	Scheduler        com.Scheduler
	// SchedulerBorrowed must be true for every Runtime that shares an external
	// scheduler. Borrowed schedulers are never stopped by Runtime.Close; their
	// owner is responsible for stopping them after all runtimes have joined.
	SchedulerBorrowed bool
	Catalog           *data.DataSourceCatalog
	Dump              *orm.DumpSink
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
	Storage    *orm.Storage
	Batch      *strat.BatchState
	Strategies *strat.State
	// Accounts is the mutable execution account state shared by Trader, Wallet,
	// and Strategy for this Runtime. Config remains an immutable snapshot.
	Accounts      map[string]*config.AccountConfig
	accountsMu    sync.RWMutex
	Orders        *ormo.OrderState
	Trading       *biz.TradingState
	Cron          com.Scheduler
	Notifications *rpc.Session
	Catalog       *data.DataSourceCatalog
	// Exchange is a runtime dependency, not an ownership claim. The entry
	// layer decides when the adapter session is closed.
	Exchange          banexg.BanExchange
	Dump              *orm.DumpSink
	schedulerClaim    *schedulerClaim
	schedulerBorrowed bool

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
	var sidRegistryURL string
	var sidRegistry *orm.SymbolSIDRegistry
	var allocator *orm.SIDAllocator
	var allocatorNamespace string
	var schedulerClaim *schedulerClaim
	schedulerClaimReleased := false
	constructed := false
	defer func() {
		p.finishRuntimeConstruction()
		if !constructed {
			// Wait for sibling constructors before deciding whether shared state is
			// still referenced. This also lets the final failed constructor clean
			// up resources created by an earlier failed sibling.
			p.waitForRuntimeConstructions()
			p.releaseFailedRuntimeDependencies(allocatorNamespace, allocator, sidRegistryURL, sidRegistry)
			if schedulerClaim != nil && !schedulerClaimReleased {
				p.releaseSchedulerClaim(schedulerClaim)
			}
		}
	}()

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
	if err := validateRuntimeExchangeIdentity(opts.Exchange, opts.ExchangeName, opts.Market); err != nil {
		return nil, err
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
	catalog := opts.Catalog
	if catalog == nil {
		catalog = data.NewDataSourceCatalog()
	}
	if snapshotConfig != nil && snapshotConfig.Database != nil {
		dbCfg := snapshotConfig.Database
		if strings.TrimSpace(dbCfg.Url) != "" {
			if opts.Storage == nil {
				return nil, fmt.Errorf("runtime: storage is required when database configuration is provided")
			}
			if err := opts.Storage.ValidateConfig(dbCfg, snapshot.DataDir); err != nil {
				return nil, fmt.Errorf("runtime: storage/database mismatch: %w", err)
			}
		}
	}
	schedulerLocation := opts.DisplayLocation
	schedulerLang := opts.SchedulerLang
	if snapshot != nil {
		if schedulerLocation == nil {
			schedulerLocation = snapshot.Location()
		}
		if schedulerLang == "" && snapshotConfig != nil {
			schedulerLang = snapshotConfig.NTPLangCode
		}
	}
	scheduler := opts.Scheduler
	id := opts.ID
	if id == "" {
		id = fmt.Sprintf("runtime-%d", runtimeOrdinal)
	}
	storageNamespace := runtimeStorageNamespace(opts, snapshot)
	if opts.Storage != nil {
		if opts.Storage.Identity() == "" {
			return nil, fmt.Errorf("runtime: storage identity is required")
		}
		if requested := strings.TrimSpace(opts.StorageNamespace); requested != "" {
			// Explicit namespaces are stored with an internal prefix so they do
			// not collide with canonical database identities. Accept either the
			// caller's fully-qualified identity or its short option spelling.
			storageIdentity := opts.Storage.Identity()
			if storageIdentity != requested && storageIdentity != "explicit:"+requested {
				return nil, fmt.Errorf("runtime: allocator namespace does not match supplied storage")
			}
		}
		storageNamespace = opts.Storage.Identity()
	}
	if storageNamespace == "" {
		// No storage identity means this runtime has no safe sharing boundary.
		// Keep its allocator private instead of merging it into a literal default.
		storageNamespace = fmt.Sprintf("runtime:%d", runtimeOrdinal)
	}
	allocatorNamespace = storageNamespace
	if snapshotConfig != nil && snapshotConfig.Database != nil {
		var registryErr error
		sidRegistryURL = strings.TrimSpace(snapshotConfig.Database.SIDRegistryURL)
		sidRegistry, _, registryErr = p.initSIDRegistryOwned(sidRegistryURL, snapshotConfig.Database.AutoCreate)
		if registryErr != nil {
			return nil, fmt.Errorf("runtime: initialize SID registry: %w", registryErr)
		}
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
	coreState, err := core.NewState(opts.Context)
	if err != nil {
		return nil, err
	}
	if opts.Logger != nil {
		coreState.Logger = opts.Logger
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
	clock := btime.NewClockState(coreState.BackTestMode, schedulerLocation)
	if opts.StartAt != 0 {
		clock.SetTimeMS(opts.StartAt)
	}
	var allocatorErr error
	allocator, allocatorErr = p.initSymbolAllocator(storageNamespace, opts.DataDir, sidRegistry)
	if allocatorErr != nil {
		coreState.Close()
		return nil, allocatorErr
	}
	symbols := orm.NewSymbolStateWithAllocatorAndIdentity(allocator, symbolExchange, symbolMarket)
	if err := symbols.BindStorage(opts.Storage); err != nil {
		coreState.Close()
		return nil, fmt.Errorf("runtime: bind storage: %w", err)
	}
	if opts.DataDir != "" {
		if err := orm.BindExSymbolRecoveryDir(symbols, opts.DataDir); err != nil {
			coreState.Close()
			return nil, fmt.Errorf("runtime: bind symbol recovery directory: %w", err)
		}
	}
	if scheduler == nil {
		scheduler = com.NewSchedulerWithConfig(schedulerLocation, schedulerLang)
	}
	schedulerClaim, allocatorErr = p.claimScheduler(scheduler, opts.SchedulerBorrowed)
	if allocatorErr != nil {
		coreState.Close()
		return nil, allocatorErr
	}
	configuredAccounts := map[string]*config.AccountConfig(nil)
	if opts.Config != nil {
		configuredAccounts = opts.Config.Accounts
	} else if snapshotConfig != nil {
		configuredAccounts = snapshotConfig.Accounts
	}
	runtimeAccounts := runtimeExecutionAccounts(coreState, snapshot, configuredAccounts, snapshot.DefaultAccount())
	runtime := &Runtime{
		Process:           p,
		ID:                id,
		Core:              coreState,
		Config:            snapshot,
		Clock:             clock,
		Market:            com.NewMarketStateWithExchange(opts.ExchangeName, opts.Exchange),
		Symbols:           symbols,
		Storage:           opts.Storage,
		Batch:             strat.NewBatchState(),
		Strategies:        strat.NewState(),
		Accounts:          runtimeAccounts,
		Orders:            ormo.NewOrderState(),
		Trading:           biz.NewTradingState(),
		Cron:              scheduler,
		Exchange:          opts.Exchange,
		Catalog:           catalog,
		Dump:              opts.Dump,
		schedulerClaim:    schedulerClaim,
		schedulerBorrowed: opts.SchedulerBorrowed,
		closeDone:         make(chan struct{}),
	}
	runtime.Orders.SetLive(coreState.LiveMode)
	if bindErr := biz.BindRuntimeDeps(runtime.BizDeps()); bindErr != nil {
		runtime.Close()
		runtime.Join()
		schedulerClaimReleased = true
		return nil, fmt.Errorf("runtime: bind dependencies: %w", bindErr)
	}
	runtime.Notifications = biz.NewRuntimeNotifications(runtime.BizDeps())
	runtime.OnClose(runtime.Notifications.Stop)
	runtime.OnCloseWait(runtime.Notifications.Join)

	p.runtimeMu.Lock()
	if p.closed {
		p.runtimeMu.Unlock()
		runtime.Close()
		runtime.Join()
		schedulerClaimReleased = true
		return nil, fmt.Errorf("runtime: process is closed")
	}
	if !p.registered {
		p.registered = true
		registerActiveProcess(p)
	}
	p.runtimes = append(p.runtimes, runtime)
	p.runtimeMu.Unlock()
	constructed = true
	return runtime, nil
}

// validateRuntimeExchangeIdentity prevents a Runtime from pairing an adapter
// for one exchange/market with symbol and routing metadata for another. An
// explicit identity is only accepted when the adapter can prove both halves
// of that identity.
func validateRuntimeExchangeIdentity(exchange banexg.BanExchange, name, market string) error {
	if exchange == nil {
		return nil
	}
	// Identity-free runtimes are useful for non-symbol capabilities. Their
	// adapter metadata is still checked by data identity before symbol access.
	if name == "" && market == "" {
		return nil
	}
	info, err := runtimeExchangeInfo(exchange)
	if err != nil {
		return fmt.Errorf("runtime: adapter identity is unavailable: %w", err)
	}
	if info == nil || info.ID == "" || info.MarketType == "" {
		return fmt.Errorf("runtime: adapter identity metadata is incomplete")
	}
	if info.ID != "" && name != "" && info.ID != name {
		return fmt.Errorf("runtime: exchange identity %q does not match adapter %q", name, info.ID)
	}
	if info.MarketType != "" && market != "" && info.MarketType != market {
		return fmt.Errorf("runtime: market identity %q does not match adapter %q", market, info.MarketType)
	}
	return nil
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

// BizDeps returns the narrow, typed dependency view consumed by biz and live
// components. Keeping this conversion at the composition root prevents each
// caller from silently dropping a newly-added runtime dependency.
func (r *Runtime) BizDeps() biz.RuntimeDeps {
	if r == nil {
		return biz.RuntimeDeps{}
	}
	defaultAccount := ""
	if r.Config != nil {
		defaultAccount = r.Config.DefaultAccount()
	}
	return biz.RuntimeDeps{
		Core:           r.Core,
		Clock:          r.Clock,
		Market:         r.Market,
		Batch:          r.Batch,
		Strategies:     r.Strategies,
		Orders:         r.Orders,
		Trading:        r.Trading,
		Config:         r.Config,
		Accounts:       r.Accounts,
		AccountsMu:     &r.accountsMu,
		Symbols:        r.Symbols,
		Storage:        r.Storage,
		Exchange:       r.Exchange,
		Dump:           r.Dump,
		Scheduler:      r.Scheduler(),
		Notifications:  r.Notifications,
		DefaultAccount: defaultAccount,
		Catalog:        r.Catalog,
		Callbacks:      r,
	}
}

// DataDeps returns the narrow, typed dependency view consumed by data
// providers. The Runtime itself is the callback admission barrier.
func (r *Runtime) DataDeps() *data.RuntimeDeps {
	if r == nil {
		return nil
	}
	return r.BizDeps().DataDeps()
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
	if r.Dump != nil {
		_ = r.Dump.Close()
	}
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
	r.Catalog = nil
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
	if r == nil || r.Cron == nil || r.schedulerBorrowed {
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
	process := r.Process
	r.closeMu.Lock()
	if r.closePhase != closeClosed {
		if r.closeDone == nil {
			r.closeDone = make(chan struct{})
		}
		r.closeRequested = false
		r.closePhase = closeClosed
		close(r.closeDone)
		r.closeMu.Unlock()
		if process != nil {
			process.unregisterRuntime(r)
			process.releaseSchedulerClaim(r.schedulerClaim)
		}
		r.schedulerClaim = nil
		return
	}
	r.closeMu.Unlock()
}
