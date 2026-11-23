package live

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	runtimepkg "github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
)

type testRuntimeLifecycle struct {
	ctx       context.Context
	cancel    context.CancelFunc
	exchange  banexg.BanExchange
	hooks     []func()
	waitHooks []func()
}

type runtimeDepsCallbackTracker struct{}

func (runtimeDepsCallbackTracker) EnterCallback() bool { return true }
func (runtimeDepsCallbackTracker) LeaveCallback()      {}

func completeCryptoTraderDepsForTest(deps biz.RuntimeDeps) biz.RuntimeDeps {
	if deps.Core == nil {
		deps.Core = &core.State{}
	}
	if deps.Clock == nil {
		deps.Clock = btime.NewClockState(deps.Core.BackTestMode, nil)
	}
	if deps.Market == nil {
		deps.Market = com.NewMarketState(deps.Core.ExgName)
	}
	if deps.Batch == nil {
		deps.Batch = strat.NewBatchState()
	}
	if deps.Strategies == nil {
		deps.Strategies = strat.NewState()
	}
	if deps.Orders == nil {
		deps.Orders = ormo.NewOrderState()
	}
	if deps.Trading == nil {
		deps.Trading = biz.NewTradingState()
	}
	if deps.Config == nil {
		deps.Config = config.NewSnapshot(&config.Config{Accounts: map[string]*config.AccountConfig{"default": {}}})
	}
	if deps.Accounts == nil {
		deps.Accounts = config.CloneAccountConfigsForRuntime(deps.Config.View().Accounts)
	}
	if len(deps.Accounts) == 0 {
		deps.Accounts = map[string]*config.AccountConfig{"default": {}}
	}
	if deps.AccountsMu == nil {
		deps.AccountsMu = &sync.RWMutex{}
	}
	if deps.Symbols == nil {
		deps.Symbols = orm.NewSymbolState()
	}
	if deps.Exchange == nil {
		deps.Exchange = &banexg.Exchange{}
	}
	if deps.DefaultAccount == "" {
		deps.DefaultAccount = "default"
	}
	return deps
}

// newCryptoTraderForTest constructs intentionally incomplete fixtures for
// lower-level lifecycle tests. Production callers use the fail-fast exported
// constructor.
func newCryptoTraderForTest(deps biz.RuntimeDeps) *CryptoTrader {
	deps = completeCryptoTraderDepsForTest(deps)
	if err := biz.BindRuntimeDeps(deps); err != nil {
		panic(err)
	}
	traderState, err := biz.NewTraderWithRuntimeDeps(deps)
	if err != nil {
		panic(err)
	}
	trader := &CryptoTrader{Trader: traderState}
	bound := trader.RuntimeDependencies()
	trader.dataDeps = bound.DataDeps()
	return trader
}

func newBareCryptoTraderForTest() *CryptoTrader {
	return &CryptoTrader{Trader: biz.Trader{}}
}

func newBoundCryptoTraderForTest(lifecycle RuntimeLifecycle) *CryptoTrader {
	return bindCryptoTraderRuntime(newBareCryptoTraderForTest(), lifecycle)
}

func newTestRuntimeLifecycle() *testRuntimeLifecycle {
	ctx, cancel := context.WithCancel(context.Background())
	return &testRuntimeLifecycle{ctx: ctx, cancel: cancel}
}

func (r *testRuntimeLifecycle) Context() context.Context {
	return r.ctx
}

func (r *testRuntimeLifecycle) Exchange() banexg.BanExchange {
	return r.exchange
}

func (r *testRuntimeLifecycle) OnClose(call func()) {
	if call != nil {
		r.hooks = append(r.hooks, call)
	}
}

func (r *testRuntimeLifecycle) OnCloseWait(call func()) {
	if call != nil {
		r.waitHooks = append(r.waitHooks, call)
	}
}

func (r *testRuntimeLifecycle) close() {
	r.cancel()
	for _, call := range r.hooks {
		call()
	}
}

func (r *testRuntimeLifecycle) closeAndWait() {
	r.close()
	for _, call := range r.waitHooks {
		call()
	}
}

func TestNewCryptoTraderOwnsBatchState(t *testing.T) {
	first := newBareCryptoTraderForTest()
	second := newBareCryptoTraderForTest()
	firstState := first.Trader.BatchState()
	secondState := second.Trader.BatchState()
	if firstState == nil || secondState == nil || firstState == secondState {
		t.Fatal("crypto traders must own distinct batch states")
	}
}

func TestRuntimeShutdownLifecycleOnCloseRunsOnce(t *testing.T) {
	lifecycle := newTestRuntimeLifecycle()
	trader := newBoundCryptoTraderForTest(lifecycle)
	shutdown := &runtimeShutdownLifecycle{owner: trader}
	var calls int
	shutdown.OnClose(func() { calls++ })

	lifecycle.closeAndWait()

	if calls != 1 {
		t.Fatalf("shutdown callback calls = %d, want 1", calls)
	}
}

func TestCryptoTraderExplicitRuntimeRejectsIncompleteWebAPIDeps(t *testing.T) {
	state, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	cfg := &config.Config{
		Exchange:      &config.ExchangeConfig{Name: "binance"},
		StakeCurrency: []string{"USDT"},
		APIServer:     &config.APIServerConfig{Enable: true},
	}
	snapshot := config.NewSnapshotWithDirs(cfg, t.TempDir(), "")
	trader := newCryptoTraderForTest(biz.RuntimeDeps{Core: state, Config: snapshot})
	if err := trader.startWebAPI(); err == nil || !strings.Contains(err.Error(), "runtime") {
		t.Fatalf("startWebAPI error = %v, want missing runtime dependencies error", err)
	}
}

func TestCryptoTraderWithRuntimeDepsRejectsMissingState(t *testing.T) {
	state, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	if _, err := NewCryptoTraderWithRuntimeDeps(biz.RuntimeDeps{Core: state}); err == nil ||
		!strings.Contains(err.Error(), "clock") {
		t.Fatalf("explicit live constructor error = %v, want missing-state error", err)
	}
}

func TestCryptoTraderWithRuntimeDepsRequiresCallbackOwnerLifecycle(t *testing.T) {
	rt, err := runtimepkg.NewProcess().NewRuntime(runtimepkg.Options{Config: &config.Config{}, Exchange: &banexg.Exchange{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	deps := rt.BizDeps()
	deps.Callbacks = runtimeDepsCallbackTracker{}
	if _, runErr := NewCryptoTraderWithRuntimeDeps(deps); runErr == nil ||
		!strings.Contains(runErr.Error(), "callback owner") {
		t.Fatalf("explicit live constructor error = %v, want callback owner lifecycle", runErr)
	}
}

func TestCryptoTraderExplicitRuntimeRequiresExchangeInsteadOfGlobalFallback(t *testing.T) {
	oldDefault := exg.Default
	global := &banexg.Exchange{}
	exg.Default = global
	t.Cleanup(func() { exg.Default = oldDefault })

	deps := completeCryptoTraderDepsForTest(biz.RuntimeDeps{})
	deps.Exchange = nil
	if _, err := NewCryptoTraderWithRuntimeDeps(deps); err == nil || !strings.Contains(err.Error(), "exchange") {
		t.Fatalf("explicit live constructor error = %v, want missing exchange", err)
	}
}

func TestExplicitPairRefreshRequiresOwnedOrderState(t *testing.T) {
	deps := completeCryptoTraderDepsForTest(biz.RuntimeDeps{Exchange: &banexg.Exchange{}})
	deps.Orders = nil
	_, err := NewCryptoTraderWithRuntimeDeps(deps)
	if err == nil || !strings.Contains(err.Error(), "order state") {
		t.Fatalf("missing runtime order state error = %v", err)
	}
}

func TestCryptoTraderRuntimeDepsRetainOneSymbolState(t *testing.T) {
	symbols := orm.NewSymbolState()
	catalog := data.NewDataSourceCatalog()
	callbacks := runtimeDepsCallbackTracker{}
	trader := newCryptoTraderForTest(biz.RuntimeDeps{
		Symbols: symbols, Catalog: catalog, Callbacks: callbacks,
	})
	if trader.RuntimeDependencies() == nil || trader.RuntimeDependencies().Symbols != symbols {
		t.Fatal("crypto trader did not retain the supplied symbol state")
	}
	if trader.dataDeps.Symbols != symbols || trader.dataDeps.Catalog != catalog || trader.dataDeps.Callbacks != callbacks {
		t.Fatal("runtime data projection did not retain its catalog, callback tracker, and symbol state")
	}
}
