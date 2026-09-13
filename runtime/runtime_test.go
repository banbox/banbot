package runtime

import (
	"context"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/cron/v3"
)

type processTrackingScheduler struct {
	stopCalls atomic.Int32
}

func TestRuntimeOwnsIndependentDataSourceCatalogs(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	catalogA := data.NewDataSourceCatalog()
	catalogB := data.NewDataSourceCatalog()
	first, err := process.NewRuntime(Options{Catalog: catalogA})
	if err != nil {
		t.Fatal(err)
	}
	second, err := process.NewRuntime(Options{Catalog: catalogB})
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	if first.Catalog != catalogA || second.Catalog != catalogB || first.Catalog == second.Catalog {
		t.Fatalf("runtime catalogs are not independent: %p/%p", first.Catalog, second.Catalog)
	}
	first.Close()
	if second.Catalog != catalogB {
		t.Fatal("closing one runtime cleared another runtime's data source catalog")
	}
}

func TestRuntimeCloseUnregistersFromProcess(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	first, err := process.NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := process.NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}

	first.Close()
	process.runtimeMu.Lock()
	tracked := len(process.runtimes)
	remaining := tracked == 1 && process.runtimes[0] == second
	process.runtimeMu.Unlock()
	if !remaining {
		t.Fatalf("tracked runtimes after first close = %d, want only second runtime", tracked)
	}

	second.Close()
	process.runtimeMu.Lock()
	tracked = len(process.runtimes)
	process.runtimeMu.Unlock()
	if tracked != 0 {
		t.Fatalf("tracked runtimes after all closes = %d, want 0", tracked)
	}
}

func TestRuntimeSharesOwnedAccountExecutionStateWithTrader(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	source := &config.Config{
		Env: core.RunEnvProd,
		Accounts: map[string]*config.AccountConfig{
			"live": {StakePctAmt: 73},
		},
	}
	rt, err := process.NewRuntime(Options{Config: source, Env: core.RunEnvProd, Exchange: &banexg.Exchange{}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if rt.Accounts["live"] == nil || rt.Accounts["live"].StakePctAmt != 73 {
		t.Fatalf("runtime account state = %#v, want preserved StakePctAmt", rt.Accounts)
	}
	deps := rt.BizDeps()
	if deps.Accounts["live"] != rt.Accounts["live"] || deps.AccountsMu != &rt.accountsMu {
		t.Fatal("Runtime.BizDeps did not expose its owned account execution state")
	}
	trader, traderErr := biz.NewTraderWithRuntimeDeps(deps)
	if traderErr != nil {
		t.Fatal(traderErr)
	}
	traderDeps := trader.RuntimeDependencies()
	if traderDeps == nil || traderDeps.Accounts["live"] != rt.Accounts["live"] {
		t.Fatal("Trader did not retain the Runtime-owned account execution state")
	}
	rt.Accounts["live"].StakePctAmt = 11
	if got := traderDeps.Accounts["live"].StakePctAmt; got != 11 {
		t.Fatalf("Trader account state = %v, want shared value 11", got)
	}
}

func TestRuntimeBizDepsProjectsAllRuntimeOwners(t *testing.T) {
	wantExportedRuntimeFields := []string{
		"Process", "ID", "Core", "Config", "Clock", "Market", "Symbols", "Storage", "Batch", "Strategies",
		"Accounts", "Orders", "Trading", "Cron", "Notifications", "Catalog", "Exchange", "Dump",
	}
	runtimeType := reflect.TypeOf(Runtime{})
	gotExportedRuntimeFields := make([]string, 0, len(wantExportedRuntimeFields))
	for index := 0; index < runtimeType.NumField(); index++ {
		field := runtimeType.Field(index)
		if field.IsExported() {
			gotExportedRuntimeFields = append(gotExportedRuntimeFields, field.Name)
		}
	}
	if !reflect.DeepEqual(gotExportedRuntimeFields, wantExportedRuntimeFields) {
		t.Fatalf("Runtime exported fields = %v, want %v; classify every added owner in BizDeps", gotExportedRuntimeFields, wantExportedRuntimeFields)
	}
	process := NewProcess()
	defer process.Close()
	exchange := &banexg.Exchange{}
	catalog := data.NewDataSourceCatalog()
	dump := &orm.DumpSink{}
	rt, err := process.NewRuntime(Options{
		Config:   &config.Config{Accounts: map[string]*config.AccountConfig{"default": {}}},
		Exchange: exchange,
		Catalog:  catalog,
		Dump:     dump,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	deps := rt.BizDeps()
	if deps.Core != rt.Core || deps.Clock != rt.Clock || deps.Market != rt.Market || deps.Batch != rt.Batch ||
		deps.Strategies != rt.Strategies || deps.Orders != rt.Orders || deps.Trading != rt.Trading ||
		deps.Config != rt.Config || deps.Accounts == nil || deps.AccountsMu != &rt.accountsMu ||
		deps.Symbols != rt.Symbols || deps.Storage != rt.Storage || deps.Exchange != exchange || deps.Dump != dump ||
		deps.Catalog != catalog || deps.Callbacks != rt || deps.Scheduler != rt.Scheduler() ||
		deps.Notifications != rt.Notifications || deps.DefaultAccount != "default" {
		t.Fatalf("biz dependency projection dropped a sentinel: %#v", deps)
	}
}

func TestStopAndWaitProcessesJoinsRuntimeOwners(t *testing.T) {
	process := NewProcess()
	rt, err := process.NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	joined := make(chan struct{})
	rt.OnCloseWait(func() { close(joined) })

	StopAndWaitProcesses()

	select {
	case <-joined:
	default:
		t.Fatal("StopAndWaitProcesses returned before runtime owner joined")
	}
	process.runtimeMu.Lock()
	remaining := len(process.runtimes)
	process.runtimeMu.Unlock()
	if remaining != 0 {
		t.Fatalf("runtimes after StopAndWaitProcesses = %d, want 0", remaining)
	}
	// StopAndWaitProcesses owns this process' close; the call is intentionally
	// idempotent so test cleanup and signal handling can safely repeat it.
	process.Close()
}

func (s *processTrackingScheduler) AddFunc(string, func()) (cron.EntryID, error) {
	return 0, nil
}

func (s *processTrackingScheduler) Start() {}

func (s *processTrackingScheduler) Stop() context.Context {
	s.stopCalls.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

type processBlockingExchange struct {
	banexg.BanExchange
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (*processBlockingExchange) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: "test", MarketType: banexg.MarketSpot}
}

func (e *processBlockingExchange) PriceSymbolParts(string) ([4]string, *errs.Error) {
	e.once.Do(func() { close(e.entered) })
	<-e.release
	return [4]string{}, nil
}

func TestRuntimeStatesAreIndependent(t *testing.T) {
	process := NewProcess()
	cfg := &config.Config{
		ConcurNum:  5,
		Pairs:      []string{"BTC/USDT"},
		Exchange:   &config.ExchangeConfig{Name: "binance"},
		MarketType: "spot",
	}
	a, err := process.NewRuntime(Options{
		ID: "backtest-a", Mode: core.RunModeBackTest, Env: core.RunEnvDryRun,
		StartAt: 100, Config: cfg, ConcurNum: 7, Pairs: []string{"A/USDT"},
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := process.NewRuntime(Options{
		ID: "backtest-b", Mode: core.RunModeBackTest, Env: core.RunEnvDryRun,
		StartAt: 200, Config: cfg, Pairs: []string{"B/USDT"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	t.Cleanup(b.Close)

	if a.Core.ConcurNum != 7 || b.Core.ConcurNum != 5 {
		t.Fatalf("concur num precedence = %d/%d, want 7/5", a.Core.ConcurNum, b.Core.ConcurNum)
	}
	if a.Config.View().Pairs[0] != "BTC/USDT" || b.Config.View().Pairs[0] != "BTC/USDT" {
		t.Fatalf("snapshot configured pairs = %v/%v", a.Config.View().Pairs, b.Config.View().Pairs)
	}
	if a.Core.Pairs[0] != "A/USDT" || b.Core.Pairs[0] != "B/USDT" {
		t.Fatalf("runtime active pairs = %v/%v", a.Core.Pairs, b.Core.Pairs)
	}
	if !a.Core.PairEnabled("A/USDT") || a.Core.PairEnabled("B/USDT") ||
		!b.Core.PairEnabled("B/USDT") || b.Core.PairEnabled("A/USDT") {
		t.Fatalf("runtime pair maps leaked: %v/%v", a.Core.AdmissionPairs(), b.Core.AdmissionPairs())
	}
	a.Symbols.CacheExSymbol(&orm.ExSymbol{ID: 1, Exchange: "binance", Market: "spot", Symbol: "A/USDT"})
	b.Symbols.CacheExSymbol(&orm.ExSymbol{ID: 2, Exchange: "binance", Market: "spot", Symbol: "B/USDT"})
	if a.Symbols.GetSymbolByID(2) != nil || b.Symbols.GetSymbolByID(1) != nil {
		t.Fatal("runtime symbol states leaked across instances")
	}
	a.Clock.SetTimeMS(101)
	b.Clock.SetTimeMS(202)
	a.Market.Prices.SetBarPriceAt(a.Clock.TimeMS(), "A/USDT", 11)
	b.Market.Prices.SetBarPriceAt(b.Clock.TimeMS(), "B/USDT", 22)
	a.Market.PairCopied.SetPairMsAt(a.Clock.TimeMS(), "A/USDT", 101, 60)

	if got := a.Clock.TimeMS(); got != 101 {
		t.Fatalf("runtime A clock = %d", got)
	}
	if got := b.Clock.TimeMS(); got != 202 {
		t.Fatalf("runtime B clock = %d", got)
	}
	if got := a.Market.Prices.GetLastBarPriceAt("A/USDT"); got != 11 {
		t.Fatalf("runtime A price = %v", got)
	}
	if got := b.Market.Prices.GetLastBarPriceAt("A/USDT"); got != -1 {
		t.Fatalf("runtime B inherited A price = %v", got)
	}
	if got := len(b.Market.PairCopied.GetPairCopieds()); got != 0 {
		t.Fatalf("runtime B inherited A pair progress: %d", got)
	}
	if got := a.Config.View().Pairs[0]; got != "BTC/USDT" {
		t.Fatalf("snapshot changed unexpectedly: %s", got)
	}
	cfg.Pairs[0] = "ETH/USDT"
	if got := a.Config.View().Pairs[0]; got != "BTC/USDT" {
		t.Fatalf("snapshot shares source pairs: %s", got)
	}

	a.Stop()
	select {
	case <-a.Done():
	default:
		t.Fatal("runtime A was not stopped")
	}
	select {
	case <-b.Done():
		t.Fatal("runtime B was stopped with A")
	default:
	}
}

func TestRuntimeRejectsExchangeIdentityMismatch(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	adapter := &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "adapter", MarketType: banexg.MarketSpot}}

	if _, err := process.NewRuntime(Options{
		Exchange: adapter, ExchangeName: "runtime", Market: banexg.MarketSpot,
	}); err == nil || !strings.Contains(err.Error(), "exchange identity") {
		t.Fatalf("exchange mismatch error = %v, want identity error", err)
	}
	process.runtimeMu.Lock()
	tracked := len(process.runtimes)
	process.runtimeMu.Unlock()
	if tracked != 0 {
		t.Fatalf("mismatched runtime was registered: %d", tracked)
	}
}

func TestProcessStopOnlyStopsItsRuntimes(t *testing.T) {
	firstProcess := NewProcess()
	secondProcess := NewProcess()
	firstA, err := firstProcess.NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	firstB, err := firstProcess.NewRuntime(Options{})
	if err != nil {
		firstProcess.Close()
		t.Fatal(err)
	}
	second, err := secondProcess.NewRuntime(Options{})
	if err != nil {
		firstProcess.Close()
		secondProcess.Close()
		t.Fatal(err)
	}
	t.Cleanup(firstProcess.Close)
	t.Cleanup(secondProcess.Close)

	firstProcess.Stop()
	for name, runtime := range map[string]*Runtime{"first-a": firstA, "first-b": firstB} {
		select {
		case <-runtime.Done():
		default:
			t.Fatalf("Process.Stop did not stop %s", name)
		}
	}
	select {
	case <-second.Done():
		t.Fatal("stopping one Process stopped a Runtime owned by another Process")
	default:
	}
}

func TestRuntimeCloseResetsAllOwnedState(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{Config: &config.Config{}, Exchange: &banexg.Exchange{}})
	if err != nil {
		t.Fatal(err)
	}
	rt.Strategies.SetVersion("runtime", 1)
	rt.Orders.SetSyncStamp("account", 123)
	rt.Trading.Wallet("account")
	rt.Batch.SetLastBatchMS(456)

	rt.Close()

	if versions := rt.Strategies.VersionsSnapshot(); len(versions) != 0 {
		t.Fatalf("strategy state survived close: %v", versions)
	}
	if got := rt.Orders.GetSyncStamp("account"); got != 0 {
		t.Fatalf("order state sync stamp survived close: %d", got)
	}
	if wallets := rt.Trading.WalletsSnapshot(); len(wallets) != 0 {
		t.Fatalf("trading state survived close: %v", wallets)
	}
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("batch state survived close: %d", got)
	}
}

func TestRuntimeAllocatorUsesComputedStorageNamespace(t *testing.T) {
	process := NewProcess()
	first, err := process.NewRuntime(Options{
		StorageNamespace: "catalog-a", ExchangeName: "binance", Market: banexg.MarketSpot,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := process.NewRuntime(Options{
		StorageNamespace: "catalog-a", ExchangeName: "binance", Market: banexg.MarketSpot,
	})
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	isolated, err := process.NewRuntime(Options{
		StorageNamespace: "catalog-b", ExchangeName: "binance", Market: banexg.MarketSpot,
	})
	if err != nil {
		first.Close()
		second.Close()
		t.Fatal(err)
	}
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)
	t.Cleanup(isolated.Close)

	firstAllocator := process.symbolAllocators["explicit:catalog-a"]
	if firstAllocator == nil {
		t.Fatal("computed storage namespace has no allocator")
	}
	if got := firstAllocator.Namespace(); got != "explicit:catalog-a" {
		t.Fatalf("allocator namespace = %q, want %q", got, "explicit:catalog-a")
	}
	if got := process.symbolAllocators["explicit:catalog-b"].Namespace(); got != "explicit:catalog-b" {
		t.Fatalf("isolated allocator namespace = %q, want %q", got, "explicit:catalog-b")
	}
	if err := first.Symbols.CacheExSymbolChecked(&orm.ExSymbol{
		ID: 7, Exchange: "binance", Market: banexg.MarketSpot, Symbol: "BTC/USDT",
	}); err != nil {
		t.Fatal(err)
	}
	if err := second.Symbols.CacheExSymbolChecked(&orm.ExSymbol{
		ID: 7, Exchange: "binance", Market: banexg.MarketSpot, Symbol: "ETH/USDT",
	}); err == nil {
		t.Fatal("same namespace accepted a conflicting SID reservation")
	}
	if err := isolated.Symbols.CacheExSymbolChecked(&orm.ExSymbol{
		ID: 7, Exchange: "binance", Market: banexg.MarketSpot, Symbol: "ETH/USDT",
	}); err != nil {
		t.Fatalf("isolated namespace rejected an independent SID reservation: %v", err)
	}
}

func TestProcessRejectsSIDRegistryAutoCreatePolicyConflict(t *testing.T) {
	process := NewProcess()
	const url = "postgresql://registry.example/banbot"
	if _, err := process.initSIDRegistry(url, false); err != nil {
		t.Fatal(err)
	}
	if _, err := process.initSIDRegistry(url, true); err == nil {
		t.Fatal("Process reused a SID registry with a conflicting auto-create policy")
	}
	process.Close()
}

func TestRuntimeAllocatorUsesCanonicalDatabaseIdentityNotRecoveryRoot(t *testing.T) {
	process := NewProcess()
	cfg := &config.Config{
		Database: &config.DatabaseConfig{Url: "postgresql://user:pass@quest.example/banbot"},
	}
	storage := orm.NewStorage(nil, false, "database:quest.example:5432/banbot")
	firstDir := t.TempDir()
	secondDir := t.TempDir()
	first, err := process.NewRuntime(Options{
		Config: cfg, DataDir: firstDir, Storage: storage, ExchangeName: "binance", Market: banexg.MarketSpot,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := process.NewRuntime(Options{
		Config: cfg, DataDir: secondDir, Storage: storage, ExchangeName: "binance", Market: banexg.MarketSpot,
	})
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)

	if len(process.symbolAllocators) != 1 {
		t.Fatalf("different recovery roots split the DB allocator: %v", process.symbolAllocators)
	}
	allocator := process.symbolAllocators["database:quest.example:5432/banbot"]
	if allocator == nil {
		t.Fatalf("canonical DB allocator missing: %v", process.symbolAllocators)
	}
	if first.Symbols.NextSID() != 1 || second.Symbols.NextSID() != 2 {
		t.Fatal("same database did not share SID allocation across recovery roots")
	}
	if first.Config.DataDir != firstDir || second.Config.DataDir != secondDir || first.Config.DataDir == second.Config.DataDir {
		t.Fatalf("recovery roots were not kept per runtime: %q/%q", first.Config.DataDir, second.Config.DataDir)
	}
}

func TestRuntimeAllocatorCanonicalizesEquivalentDatabaseURLs(t *testing.T) {
	process := NewProcess()
	storage := orm.NewStorage(nil, false, "database:quest.example:5432/banbot")
	first, err := process.NewRuntime(Options{
		Config: &config.Config{Database: &config.DatabaseConfig{
			Url: "postgresql://user:password@QUEST.EXAMPLE/banbot",
		}}, Storage: storage,
		DataDir: t.TempDir(), ExchangeName: "binance", Market: banexg.MarketSpot,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := process.NewRuntime(Options{
		Config: &config.Config{Database: &config.DatabaseConfig{
			Url: "postgres://another:credential@quest.example:5432/banbot",
		}}, Storage: storage,
		DataDir: t.TempDir(), ExchangeName: "binance", Market: banexg.MarketSpot,
	})
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)

	if len(process.symbolAllocators) != 1 {
		t.Fatalf("equivalent database URLs split the allocator: %v", process.symbolAllocators)
	}
	if first.Symbols.NextSID() != 1 || second.Symbols.NextSID() != 2 {
		t.Fatal("equivalent database URLs did not share SID allocation")
	}
}

func TestRuntimeSymbolStateDoesNotSwitchLegacyFacade(t *testing.T) {
	restore, err := orm.InstallFrozenExSymbols([]*orm.ExSymbol{{
		ID: 77, Exchange: "legacy", Market: "spot", Symbol: "LEGACY/USDT",
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restore)

	rt, err := NewProcess().NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	rt.Symbols.CacheExSymbol(&orm.ExSymbol{ID: 88, Exchange: "runtime", Market: "spot", Symbol: "RUNTIME/USDT"})

	if rt.Symbols.GetSymbolByID(77) != nil {
		t.Fatal("runtime inherited the legacy package symbol state")
	}
	if orm.GetSymbolByID(77) == nil || orm.GetSymbolByID(88) != nil {
		t.Fatal("runtime write switched or polluted the legacy package facade")
	}
}

func TestRuntimeWithoutIdentityDoesNotReadLegacySymbolGlobals(t *testing.T) {
	restore, err := orm.InstallFrozenExSymbols([]*orm.ExSymbol{{
		ID: 77, Exchange: "legacy", Market: "spot", Symbol: "LEGACY/USDT",
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restore)

	rt, err := NewProcess().NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)

	got, resolveErr := rt.Symbols.GetExSymbolCur("LEGACY/USDT")
	if resolveErr == nil || got != nil {
		t.Fatalf("unconfigured runtime resolved legacy symbol: symbol=%+v err=%v", got, resolveErr)
	}
}

func TestRuntimeRejectsPartialIdentity(t *testing.T) {
	for name, opts := range map[string]Options{
		"exchange only": {ExchangeName: "binance"},
		"market only":   {Market: banexg.MarketSpot},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewProcess().NewRuntime(opts); err == nil {
				t.Fatal("NewRuntime accepted a partial identity")
			}
		})
	}
}

func TestRuntimeSymbolStateUsesOptionsIdentity(t *testing.T) {
	oldExchange, oldMarket, oldDefault := core.ExgName, core.Market, exg.Default
	t.Cleanup(func() {
		core.ExgName, core.Market, exg.Default = oldExchange, oldMarket, oldDefault
	})

	process := NewProcess()
	first, err := process.NewRuntime(Options{ExchangeName: "binance", Market: banexg.MarketSpot})
	if err != nil {
		t.Fatal(err)
	}
	second, err := process.NewRuntime(Options{ExchangeName: "okx", Market: banexg.MarketLinear})
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)
	first.Symbols.CacheExSymbol(&orm.ExSymbol{ID: 1, Exchange: "binance", Market: banexg.MarketSpot, Symbol: "BTC/USDT"})
	second.Symbols.CacheExSymbol(&orm.ExSymbol{ID: 2, Exchange: "okx", Market: banexg.MarketLinear, Symbol: "BTC/USDT"})

	core.ExgName, core.Market = "global", "future"
	exg.Default = nil
	if got, err := first.Symbols.GetExSymbolCur("BTC/USDT"); err != nil || got == nil || got.ID != 1 {
		t.Fatalf("first runtime identity lookup = %+v, err=%v", got, err)
	}
	if got, err := second.Symbols.GetExSymbolCur("BTC/USDT"); err != nil || got == nil || got.ID != 2 {
		t.Fatalf("second runtime identity lookup = %+v, err=%v", got, err)
	}
}

func TestRuntimeUsesConfigAndDefaultConcurNum(t *testing.T) {
	process := NewProcess()
	cfg := &config.Config{ConcurNum: 6, Pairs: []string{"CFG/USDT"}}
	fromConfig, err := process.NewRuntime(Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	withDefault, err := process.NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	withExplicitEmptyPairs, err := process.NewRuntime(Options{Config: cfg, Pairs: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fromConfig.Close)
	t.Cleanup(withDefault.Close)
	t.Cleanup(withExplicitEmptyPairs.Close)

	if fromConfig.Core.ConcurNum != 6 || withDefault.Core.ConcurNum != 2 {
		t.Fatalf("concur num config/default = %d/%d, want 6/2", fromConfig.Core.ConcurNum, withDefault.Core.ConcurNum)
	}
	if len(fromConfig.Core.Pairs) != 1 || fromConfig.Core.Pairs[0] != "CFG/USDT" ||
		!fromConfig.Core.PairEnabled("CFG/USDT") || fromConfig.Config.View().Pairs[0] != "CFG/USDT" {
		t.Fatalf("config pairs were not installed as active pairs: %#v/%v", fromConfig.Core.Pairs, fromConfig.Core.AdmissionPairs())
	}
	if len(withExplicitEmptyPairs.Core.Pairs) != 0 || len(withExplicitEmptyPairs.Core.AdmissionPairs()) != 0 ||
		withExplicitEmptyPairs.Config.View().Pairs[0] != "CFG/USDT" {
		t.Fatalf("explicit empty pairs did not override active config pairs: %#v/%v", withExplicitEmptyPairs.Core.Pairs, withExplicitEmptyPairs.Core.AdmissionPairs())
	}
}

func TestRuntimeDefaultAccountUsesEffectiveEnvironment(t *testing.T) {
	tests := []struct {
		name         string
		configEnv    string
		runtimeEnv   string
		wantAccount  string
		wantAccounts []string
	}{
		{name: "prod config overridden by dry run", configEnv: core.RunEnvProd, runtimeEnv: core.RunEnvDryRun, wantAccount: "default", wantAccounts: []string{"default"}},
		{name: "dry run config overridden by prod", configEnv: core.RunEnvDryRun, runtimeEnv: core.RunEnvProd, wantAccount: "alpha", wantAccounts: []string{"alpha", "beta"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			process := NewProcess()
			rt, err := process.NewRuntime(Options{
				Env: test.runtimeEnv,
				Config: &config.Config{Env: test.configEnv, Accounts: map[string]*config.AccountConfig{
					"beta": {}, "alpha": {},
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(process.Close)

			deps := rt.BizDeps()
			if deps.DefaultAccount != test.wantAccount {
				t.Fatalf("default account = %q, want %q", deps.DefaultAccount, test.wantAccount)
			}
			accounts := slices.Sorted(maps.Keys(rt.Accounts))
			if !slices.Equal(accounts, test.wantAccounts) {
				t.Fatalf("runtime accounts = %v, want %v", accounts, test.wantAccounts)
			}
		})
	}
}

func TestRuntimeSnapshotUsesExplicitDirectories(t *testing.T) {
	oldDataDir := config.DataDir
	config.DataDir = "/legacy/data"
	t.Cleanup(func() { config.DataDir = oldDataDir })

	rt, err := NewProcess().NewRuntime(Options{
		Config:      &config.Config{ConcurNum: 4, Pairs: []string{"CFG/USDT"}},
		DataDir:     "/runtime/data",
		StrategyDir: "/runtime/strategy",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)

	if rt.Config.DataDir != "/runtime/data" || rt.Config.StrategyDir != "/runtime/strategy" {
		t.Fatalf("runtime snapshot directories = %q/%q", rt.Config.DataDir, rt.Config.StrategyDir)
	}
	if rt.Config.View().ConcurNum != 4 || rt.Config.View().Pairs[0] != "CFG/USDT" {
		t.Fatalf("runtime snapshot lost config values: %#v", rt.Config.View())
	}

	config.DataDir = "/changed/data"
	if rt.Config.DataDir != "/runtime/data" || rt.Config.StrategyDir != "/runtime/strategy" {
		t.Fatalf("runtime snapshot changed with legacy directory: %q/%q", rt.Config.DataDir, rt.Config.StrategyDir)
	}

	withoutConfig, err := NewProcess().NewRuntime(Options{DataDir: "/runtime/without-config"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(withoutConfig.Close)
	if withoutConfig.Config == nil || withoutConfig.Config.DataDir != "/runtime/without-config" {
		t.Fatalf("runtime without config lost explicit data directory: %#v", withoutConfig.Config)
	}

	withoutDirs, err := NewProcess().NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(withoutDirs.Close)
	if withoutDirs.Config == nil || withoutDirs.Config.DataDir != "" || withoutDirs.Config.StrategyDir != "" {
		t.Fatalf("runtime without explicit directories read legacy state: %#v", withoutDirs.Config)
	}
}

func TestRuntimeBindsSymbolRecoveryToOptionsDataDir(t *testing.T) {
	oldDataDir := config.DataDir
	config.DataDir = t.TempDir()
	t.Cleanup(func() { config.DataDir = oldDataDir })
	runtimeDataDir := t.TempDir()

	process := NewProcess()
	rt, err := process.NewRuntime(Options{DataDir: runtimeDataDir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)

	config.DataDir = t.TempDir()
	if err := orm.BindExSymbolRecoveryDir(rt.Symbols, config.DataDir); err == nil {
		t.Fatal("runtime symbol recovery root followed legacy config.DataDir")
	}
	second, err := process.NewRuntime(Options{DataDir: runtimeDataDir})
	if err != nil {
		t.Fatalf("shared Process rejected the same recovery root: %v", err)
	}
	t.Cleanup(second.Close)
}

func TestExplicitRuntimeWithoutDataDirDoesNotUseLegacyRecoveryRoot(t *testing.T) {
	oldQuest, oldDataDir := orm.IsQuestDB, config.DataDir
	orm.IsQuestDB = true
	config.DataDir = t.TempDir()
	t.Cleanup(func() {
		orm.IsQuestDB = oldQuest
		config.DataDir = oldDataDir
	})

	rt, err := NewProcess().NewRuntime(Options{ExchangeName: "test", Market: banexg.MarketSpot})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)

	_, err = orm.New(nil).WithSymbolState(rt.Symbols).AddSymbols(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "no recovery directory") {
		t.Fatalf("explicit runtime without DataDir used legacy recovery root: %v", err)
	}
}

func TestRuntimeInitializesAndClearsBatchState(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rt.Batch == nil {
		t.Fatal("runtime batch state is nil")
	}
	t.Cleanup(rt.Close)
	rt.Batch.SetLastBatchMS(123)
	if got := rt.Batch.LastBatchMS(); got != 123 {
		t.Fatalf("runtime batch timestamp = %d, want 123", got)
	}

	rt.Close()
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("closed runtime batch timestamp = %d, want 0", got)
	}
	if got := rt.Batch.PendingCount(); got != 0 {
		t.Fatalf("closed runtime batch pending count = %d, want 0", got)
	}
}

func TestProcessTracksAndJoinsRuntimesBeforeClosingSIDRegistries(t *testing.T) {
	process := NewProcess()
	const registryURL = "postgresql://registry.example/process-runtime-order"
	cfg := &config.Config{Database: &config.DatabaseConfig{SIDRegistryURL: registryURL}}
	first, err := process.NewRuntime(Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	second, err := process.NewRuntime(Options{Config: cfg})
	if err != nil {
		process.Close()
		t.Fatal(err)
	}
	first.Batch.SetLastBatchMS(101)
	second.Batch.SetLastBatchMS(202)

	process.runtimeMu.Lock()
	tracked := len(process.runtimes)
	process.runtimeMu.Unlock()
	if tracked != 2 {
		t.Fatalf("tracked runtimes = %d, want 2", tracked)
	}

	registryOpenDuringClose := false
	first.OnClose(func() {
		process.sidRegistryMu.Lock()
		registryOpenDuringClose = process.sidRegistries[registryURL] != nil
		process.sidRegistryMu.Unlock()
	})
	releaseWait := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseWait:
		default:
			close(releaseWait)
		}
		process.Close()
	})
	first.OnCloseWait(func() {
		<-releaseWait
	})

	closeDone := make(chan struct{})
	go func() {
		process.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		t.Fatal("Process.Close returned before the tracked runtime joined")
	case <-time.After(50 * time.Millisecond):
	}
	if got := first.Batch.LastBatchMS(); got != 101 {
		t.Fatalf("first runtime reset before join completed: %d", got)
	}

	close(releaseWait)
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Process.Close did not finish after tracked runtimes were released")
	}
	if !registryOpenDuringClose {
		t.Fatal("SID registry was closed before tracked runtime shutdown completed")
	}
	if got := first.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("first runtime batch state = %d, want 0", got)
	}
	if got := second.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("second runtime batch state = %d, want 0", got)
	}
	process.sidRegistryMu.Lock()
	remainingRegistries := len(process.sidRegistries)
	process.sidRegistryMu.Unlock()
	if remainingRegistries != 0 {
		t.Fatalf("SID registries after Process.Close = %d, want 0", remainingRegistries)
	}
}

func TestProcessCloseRejectsConcurrentRuntimeConstructionWithoutLeak(t *testing.T) {
	process := NewProcess()
	exchange := &processBlockingExchange{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	scheduler := &processTrackingScheduler{}
	t.Cleanup(func() {
		select {
		case <-exchange.release:
		default:
			close(exchange.release)
		}
		process.Close()
	})
	result := make(chan struct {
		runtime *Runtime
		err     error
	}, 1)
	go func() {
		rt, err := process.NewRuntime(Options{
			Exchange:     exchange,
			ExchangeName: "test",
			Market:       banexg.MarketSpot,
			Pairs:        []string{"BTC/USDT"},
			Scheduler:    scheduler,
		})
		result <- struct {
			runtime *Runtime
			err     error
		}{rt, err}
	}()

	select {
	case <-exchange.entered:
	case <-time.After(time.Second):
		t.Fatal("NewRuntime did not reach the construction barrier")
	}

	closeDone := make(chan struct{})
	go func() {
		process.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		t.Fatal("Process.Close returned while NewRuntime was constructing")
	case <-time.After(50 * time.Millisecond):
	}

	close(exchange.release)
	constructed := <-result
	if constructed.runtime != nil {
		constructed.runtime.Close()
		t.Fatal("NewRuntime returned a runtime after Process.Close started")
	}
	if constructed.err == nil || !strings.Contains(constructed.err.Error(), "process is closed") {
		t.Fatalf("NewRuntime error after concurrent Process.Close = %v", constructed.err)
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Process.Close did not finish after the concurrent constructor exited")
	}
	if got := scheduler.stopCalls.Load(); got != 1 {
		t.Fatalf("newly constructed runtime scheduler Stop calls = %d, want 1", got)
	}
}

func TestProcessNewRuntimeRejectsAfterClose(t *testing.T) {
	process := NewProcess()
	process.Close()

	rt, err := process.NewRuntime(Options{})
	if err == nil || rt != nil {
		if rt != nil {
			rt.Close()
		}
		t.Fatalf("NewRuntime after Process.Close returned runtime=%v err=%v", rt, err)
	}
}

func TestRuntimeBatchCanBackTrader(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{Config: &config.Config{}, Exchange: &banexg.Exchange{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)

	trader, traderErr := biz.NewTraderWithRuntimeDeps(rt.BizDeps())
	if traderErr != nil {
		t.Fatal(traderErr)
	}
	if trader.BatchState() != rt.Batch {
		t.Fatal("trader did not retain the runtime batch state")
	}
}

func TestRuntimeBizDepsBindingIsIdempotent(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{Config: &config.Config{}, Exchange: &banexg.Exchange{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	if err := biz.BindRuntimeDeps(rt.BizDeps()); err != nil {
		t.Fatalf("rebind identical Runtime.BizDeps: %v", err)
	}
}

func TestRuntimeTraderBatchStatesDoNotLeak(t *testing.T) {
	process := NewProcess()
	first, err := process.NewRuntime(Options{Config: &config.Config{}, Exchange: &banexg.Exchange{}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := process.NewRuntime(Options{Config: &config.Config{}, Exchange: &banexg.Exchange{}})
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)

	firstTrader, firstTraderErr := biz.NewTraderWithRuntimeDeps(first.BizDeps())
	if firstTraderErr != nil {
		t.Fatal(firstTraderErr)
	}
	secondTrader, secondTraderErr := biz.NewTraderWithRuntimeDeps(second.BizDeps())
	if secondTraderErr != nil {
		t.Fatal(secondTraderErr)
	}
	firstTask := &strat.JobEnv{Job: &strat.StratJob{Strat: &strat.TradeStrat{Name: "first"}}}
	secondTask := &strat.JobEnv{Job: &strat.StratJob{Strat: &strat.TradeStrat{Name: "second"}}}
	firstTrader.BatchState().AddTask("1m_default_first", "BTC/USDT_main", firstTask, 60_000, 100)
	secondTrader.BatchState().AddTask("1m_default_second", "ETH/USDT_main", secondTask, 60_000, 200)
	firstTrader.BatchState().SetLastBatchMS(101)
	secondTrader.BatchState().SetLastBatchMS(202)

	firstTrader.BatchState().Reset()
	if got := firstTrader.BatchState().PendingCount(); got != 0 {
		t.Fatalf("reset first runtime pending count = %d, want 0", got)
	}
	if got := firstTrader.BatchState().LastBatchMS(); got != 0 {
		t.Fatalf("reset first runtime last batch ms = %d, want 0", got)
	}
	if got := secondTrader.BatchState().PendingCount(); got != 1 {
		t.Fatalf("second runtime pending count after first reset = %d, want 1", got)
	}
	if got := secondTrader.BatchState().LastBatchMS(); got != 202 {
		t.Fatalf("second runtime last batch ms after first reset = %d, want 202", got)
	}
}

func TestTraderRejectsStatesBoundToAnotherRuntime(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	first, err := process.NewRuntime(Options{Config: &config.Config{}, Exchange: &banexg.Exchange{}})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := process.NewRuntime(Options{Config: &config.Config{}, Exchange: &banexg.Exchange{}})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	strategyDeps := second.BizDeps()
	strategyDeps.Strategies = first.Strategies
	if _, err := biz.NewTraderWithRuntimeDeps(strategyDeps); err == nil || !strings.Contains(err.Error(), "strategy state") {
		t.Fatalf("cross-runtime strategy state error = %v", err)
	}
	if first.Strategies.Core != first.Core {
		t.Fatal("rejected bind changed the first runtime strategy state")
	}

	orderDeps := second.BizDeps()
	// Keep strategy bindings valid for second so this case reaches the order
	// ownership verification.
	orderDeps.Strategies = second.Strategies
	orderDeps.Orders = first.Orders
	if _, err := biz.NewTraderWithRuntimeDeps(orderDeps); err == nil || !strings.Contains(err.Error(), "order state") {
		t.Fatalf("cross-runtime order state error = %v", err)
	}
}

func TestRuntimeDefaultsContractTypeForConfiguredContractMarket(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{Config: &config.Config{
		Exchange:   &config.ExchangeConfig{Name: "test"},
		MarketType: banexg.MarketLinear,
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)

	if rt.Core.ContractType != banexg.MarketSwap {
		t.Fatalf("contract type = %q, want %q", rt.Core.ContractType, banexg.MarketSwap)
	}

	overridden, err := NewProcess().NewRuntime(Options{
		Config:       &config.Config{Exchange: &config.ExchangeConfig{Name: "test"}, MarketType: banexg.MarketLinear},
		ContractType: banexg.MarketFuture,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(overridden.Close)
	if overridden.Core.ContractType != banexg.MarketFuture {
		t.Fatalf("explicit contract type = %q, want %q", overridden.Core.ContractType, banexg.MarketFuture)
	}
}

func TestRuntimeClearsConfigContractTypeForSpotMarket(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{Config: &config.Config{
		Exchange:     &config.ExchangeConfig{Name: "test"},
		MarketType:   banexg.MarketSpot,
		ContractType: banexg.MarketFuture,
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)

	if rt.Core.IsContract || rt.Core.ContractType != "" {
		t.Fatalf("spot runtime contract state = %t/%q, want false/empty", rt.Core.IsContract, rt.Core.ContractType)
	}
}

func TestRuntimeCloseAllowsReentrantOnClose(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}

	rt.Batch.SetLastBatchMS(123)
	order := make([]string, 0, 2)
	callbackDone := make(chan struct{})
	rt.OnClose(func() {
		order = append(order, "first")
		rt.Close()
		if got := rt.Batch.LastBatchMS(); got != 123 {
			t.Errorf("reentrant Close reset batch timestamp to %d, want 123", got)
		}
		close(callbackDone)
	})
	rt.OnClose(func() {
		order = append(order, "second")
		if got := rt.Batch.LastBatchMS(); got != 123 {
			t.Errorf("later close hook observed batch timestamp %d, want 123", got)
		}
	})
	closeDone := make(chan struct{})
	go func() {
		rt.Close()
		close(closeDone)
	}()

	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-closeDone:
	case <-timer.C:
		t.Fatal("Runtime.Close deadlocked during reentrant OnClose callback")
	}
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("reentrant OnClose callback did not complete")
	}
	if got, want := strings.Join(order, ","), "first,second"; got != want {
		t.Fatalf("close hook order = %q, want %q", got, want)
	}
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("batch timestamp after reentrant close = %d, want 0", got)
	}
}

func TestRuntimeOnCloseRunsBeforeOwnedStateReset(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	rt.Batch.SetLastBatchMS(123)
	called := false
	rt.OnClose(func() {
		called = true
		if got := rt.Batch.LastBatchMS(); got != 123 {
			t.Fatalf("close hook observed batch timestamp %d, want 123", got)
		}
	})

	rt.Close()
	if !called {
		t.Fatal("runtime close hook was not called")
	}
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("batch timestamp after close = %d, want 0", got)
	}
}

func TestRuntimeCloseConcurrentCallersJoinReset(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	rt.Batch.SetLastBatchMS(123)
	hookEntered := make(chan struct{})
	releaseHook := make(chan struct{})
	rt.OnCloseWait(func() {
		close(hookEntered)
		<-releaseHook
	})

	firstDone := make(chan struct{})
	go func() {
		rt.Close()
		close(firstDone)
	}()
	<-hookEntered
	secondDone := make(chan struct{})
	go func() {
		rt.Close()
		close(secondDone)
	}()
	select {
	case <-secondDone:
		t.Fatal("concurrent Close returned before close hooks and reset completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseHook)
	for name, done := range map[string]<-chan struct{}{"first": firstDone, "second": secondDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s Close did not return", name)
		}
	}
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("joined Close observed batch timestamp %d, want reset", got)
	}
}

func TestRuntimeCloseAndJoinWaitDuringStoppingPhase(t *testing.T) {
	rt := &Runtime{
		closeDone:  make(chan struct{}),
		closePhase: closeStopping,
	}

	closeDone := make(chan struct{})
	go func() {
		rt.Close()
		close(closeDone)
	}()
	joinDone := make(chan struct{})
	go func() {
		rt.Join()
		close(joinDone)
	}()

	select {
	case <-closeDone:
		t.Fatal("Close returned while the close owner was still stopping")
	case <-joinDone:
		t.Fatal("Join returned while the close owner was still stopping")
	case <-time.After(50 * time.Millisecond):
	}

	close(rt.closeDone)
	for name, done := range map[string]<-chan struct{}{"Close": closeDone, "Join": joinDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s did not return after close completion", name)
		}
	}
}

func TestRuntimeStopAllowsReentrantOnClose(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	rt.Batch.SetLastBatchMS(123)
	callbackDone := make(chan struct{})
	rt.OnClose(func() {
		rt.Stop()
		rt.Close()
		if got := rt.Batch.LastBatchMS(); got != 123 {
			t.Errorf("reentrant Stop/Close reset batch timestamp to %d, want 123", got)
		}
		close(callbackDone)
	})

	stopDone := make(chan struct{})
	go func() {
		rt.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Runtime.Stop deadlocked during reentrant OnClose callback")
	}
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("reentrant Stop/Close callback did not complete")
	}
	rt.Join()
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("requested close left batch timestamp at %d, want reset", got)
	}

	rt.Close()
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("Close left batch timestamp at %d, want 0", got)
	}
}

func TestRuntimeStopCloseOverlapTransfersCloseOwnership(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	rt.Batch.SetLastBatchMS(123)
	hookEntered := make(chan struct{})
	releaseHook := make(chan struct{})
	rt.OnClose(func() {
		close(hookEntered)
		<-releaseHook
	})

	stopDone := make(chan struct{})
	go func() {
		rt.Stop()
		close(stopDone)
	}()
	select {
	case <-hookEntered:
	case <-time.After(time.Second):
		t.Fatal("Stop did not enter its hook")
	}

	closeDone := make(chan struct{})
	go func() {
		rt.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("concurrent Close did not return its request")
	}
	close(releaseHook)

	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not complete after its hook was released")
	}
	select {
	case <-rt.closeDone:
	case <-time.After(time.Second):
		t.Fatal("concurrent Close request was not completed by the Stop owner")
	}
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("overlap close left batch timestamp at %d, want reset", got)
	}
}

func TestRuntimeJoinWaitsForCloseRequestedDuringStop(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	rt.Batch.SetLastBatchMS(123)
	hookEntered := make(chan struct{})
	releaseHook := make(chan struct{})
	rt.OnClose(func() {
		close(hookEntered)
		<-releaseHook
	})
	waitEntered := make(chan struct{})
	releaseWait := make(chan struct{})
	rt.OnCloseWait(func() {
		close(waitEntered)
		<-releaseWait
	})
	t.Cleanup(func() {
		select {
		case <-releaseHook:
		default:
			close(releaseHook)
		}
		select {
		case <-releaseWait:
		default:
			close(releaseWait)
		}
		rt.Close()
		rt.Join()
	})

	stopDone := make(chan struct{})
	go func() {
		rt.Stop()
		close(stopDone)
	}()
	select {
	case <-hookEntered:
	case <-time.After(time.Second):
		t.Fatal("Stop did not enter its blocking hook")
	}

	closeDone := make(chan struct{})
	go func() {
		rt.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not record its request during Stop")
	}

	joinDone := make(chan struct{})
	go func() {
		rt.Join()
		close(joinDone)
	}()
	select {
	case <-joinDone:
		t.Fatal("Join returned before the blocking OnClose hook was released")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseHook)
	select {
	case <-waitEntered:
	case <-time.After(time.Second):
		t.Fatal("requested Close did not reach its owner wait hook")
	}
	select {
	case <-joinDone:
		t.Fatal("Join returned before the owner-side reset could complete")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseWait)

	select {
	case <-joinDone:
	case <-time.After(time.Second):
		t.Fatal("Join did not return after the requested Close completed")
	}
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("Join returned before batch reset: got %d, want 0", got)
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after the requested Close completed")
	}
}

func TestRuntimeJoinWaitsForCloseOwner(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	rt.Batch.SetLastBatchMS(123)
	hookEntered := make(chan struct{})
	releaseHook := make(chan struct{})
	rt.OnCloseWait(func() {
		close(hookEntered)
		<-releaseHook
	})
	t.Cleanup(func() {
		select {
		case <-releaseHook:
		default:
			close(releaseHook)
		}
		rt.Close()
	})

	closeDone := make(chan struct{})
	go func() {
		rt.Close()
		close(closeDone)
	}()
	select {
	case <-hookEntered:
	case <-time.After(time.Second):
		t.Fatal("Close did not enter its join hook")
	}

	joinDone := make(chan struct{})
	go func() {
		rt.Join()
		close(joinDone)
	}()
	select {
	case <-joinDone:
		t.Fatal("Join returned before the Close owner completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseHook)
	for name, done := range map[string]<-chan struct{}{
		"Close": closeDone,
		"Join":  joinDone,
	} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s did not return after the owner completed", name)
		}
	}
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("Join observed batch timestamp %d, want reset", got)
	}
}

func TestRuntimeStopThenCloseJoinsAndResets(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	rt.Batch.SetLastBatchMS(123)

	stopCalls := 0
	rt.OnClose(func() {
		stopCalls++
		if got := rt.Batch.LastBatchMS(); got != 123 {
			t.Errorf("stop hook observed batch timestamp %d, want 123", got)
		}
	})
	waitEntered := make(chan struct{})
	releaseWait := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseWait:
		default:
			close(releaseWait)
		}
	})
	rt.OnCloseWait(func() {
		close(waitEntered)
		<-releaseWait
	})

	rt.Stop()
	select {
	case <-rt.Done():
	default:
		t.Fatal("Runtime.Stop did not publish cancellation")
	}
	if stopCalls != 1 {
		t.Fatalf("stop hook calls = %d, want 1", stopCalls)
	}
	if got := rt.Batch.LastBatchMS(); got != 123 {
		t.Fatalf("Stop reset batch timestamp to %d, want 123", got)
	}

	closeDone := make(chan struct{})
	go func() {
		rt.Close()
		close(closeDone)
	}()
	select {
	case <-waitEntered:
	case <-time.After(time.Second):
		t.Fatal("Close did not enter its join hook after Stop")
	}
	select {
	case <-closeDone:
		t.Fatal("Close returned before its join hook completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseWait)
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after its join hook completed")
	}
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("Close left batch timestamp at %d, want 0", got)
	}
}

func TestRuntimePostCloseHooksRunImmediately(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	rt.Close()

	closeHookCalled := false
	rt.OnClose(func() { closeHookCalled = true })
	if !closeHookCalled {
		t.Fatal("OnClose registered after Close did not run immediately")
	}

	waitHookCalled := false
	rt.OnCloseWait(func() { waitHookCalled = true })
	if !waitHookCalled {
		t.Fatal("OnCloseWait registered after Close did not run immediately")
	}
}

func BenchmarkRuntimeClock(b *testing.B) {
	process := NewProcess()
	rt, err := process.NewRuntime(Options{Mode: core.RunModeBackTest, StartAt: 100})
	if err != nil {
		b.Fatal(err)
	}
	defer rt.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rt.Clock.SetTimeMS(int64(i + 1))
		_ = rt.Clock.TimeMS()
	}
}
