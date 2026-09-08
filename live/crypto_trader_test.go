package live

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/legacygate"
	"github.com/banbox/banbot/orm"
	runtimepkg "github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	banutils "github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	ta "github.com/banbox/banta"
)

type stubTraderDataSource struct {
	info           *orm.SeriesInfo
	subscribeCount int
	subscribedSubs [][]*strat.DataSub
	emissions      [][]*orm.DataRecord
	subscribeErr   error
}

type testRuntimeLifecycle struct {
	ctx       context.Context
	cancel    context.CancelFunc
	exchange  banexg.BanExchange
	hooks     []func()
	waitHooks []func()
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

func (r *testRuntimeLifecycle) stop() {
	r.close()
}

func (r *testRuntimeLifecycle) closeAndWait() {
	r.close()
	for _, call := range r.waitHooks {
		call()
	}
}

type blockingReadConn struct {
	net.Conn
	readStarted chan struct{}
	readOnce    sync.Once
}

func (c *blockingReadConn) Read(buf []byte) (int, error) {
	c.readOnce.Do(func() { close(c.readStarted) })
	return c.Conn.Read(buf)
}

func newStubLiveProvider(conn net.Conn) *data.LiveProvider {
	client := &banutils.ClientIO{BanConn: banutils.BanConn{
		Conn:    conn,
		Data:    map[string]interface{}{},
		Listens: map[string]banutils.ConnCB{},
		Ready:   true,
	}}
	return &data.LiveProvider{SeriesWatcher: &data.SeriesWatcher{ClientIO: client}}
}

func TestNewCryptoTraderOwnsBatchState(t *testing.T) {
	first := NewCryptoTrader()
	second := NewCryptoTrader()
	firstState := first.Trader.BatchState()
	secondState := second.Trader.BatchState()
	if firstState == nil || secondState == nil || firstState == secondState {
		t.Fatal("crypto traders must own distinct batch states")
	}
	zero := &CryptoTrader{}
	zeroState := zero.batchStateForRun()
	if zeroState == nil || zeroState != zero.Trader.BatchState() {
		t.Fatal("zero-value crypto trader did not lazily create a private batch state")
	}
}

func TestCryptoTraderCanUseCompositionRootBatchState(t *testing.T) {
	state := strat.NewBatchState()
	trader := NewCryptoTraderWithBatchState(state)
	if trader.Trader.BatchState() != state {
		t.Fatal("crypto trader did not use the supplied batch state")
	}
}

func TestCryptoTraderCanUseRuntimeBatchState(t *testing.T) {
	lifecycle := newTestRuntimeLifecycle()
	state := strat.NewBatchState()
	trader := NewCryptoTraderWithRuntime(lifecycle, state, nil)
	if trader.Trader.BatchState() != state {
		t.Fatal("crypto trader did not use the runtime batch state")
	}
}

func TestCryptoTraderRuntimeWebAPIUsesShutdownLifecycle(t *testing.T) {
	oldStartAPI, oldStartAPIWithLifecycle := webStartAPI, webStartAPIWithLifecycle
	t.Cleanup(func() {
		webStartAPI, webStartAPIWithLifecycle = oldStartAPI, oldStartAPIWithLifecycle
	})

	lifecycle := newTestRuntimeLifecycle()
	trader := NewCryptoTraderWithRuntime(lifecycle, strat.NewBatchState(), nil)
	legacyCalled := false
	webStartAPI = func() *errs.Error {
		legacyCalled = true
		return nil
	}
	var webLifecycle RuntimeLifecycle
	webStartAPIWithLifecycle = func(got RuntimeLifecycle) *errs.Error {
		webLifecycle = got
		return nil
	}

	if err := trader.startWebAPI(); err != nil {
		t.Fatalf("startWebAPI failed: %v", err)
	}
	if legacyCalled {
		t.Fatal("runtime web API used the legacy start entry")
	}
	got, ok := webLifecycle.(*runtimeShutdownLifecycle)
	if !ok || got.owner != trader {
		t.Fatalf("runtime web lifecycle = %#v, want shutdown lifecycle owned by trader", webLifecycle)
	}

	stopped, joined := false, false
	webLifecycle.OnClose(func() { stopped = true })
	webLifecycle.OnCloseWait(func() { joined = true })
	lifecycle.closeAndWait()
	if !stopped || !joined {
		t.Fatalf("web shutdown callbacks: stopped=%v joined=%v, want both true", stopped, joined)
	}
}

func TestRuntimeShutdownLifecycleOnCloseRunsOnce(t *testing.T) {
	lifecycle := newTestRuntimeLifecycle()
	trader := NewCryptoTraderWithRuntime(lifecycle, strat.NewBatchState(), nil)
	shutdown := &runtimeShutdownLifecycle{owner: trader}
	var calls int
	shutdown.OnClose(func() { calls++ })

	lifecycle.closeAndWait()

	if calls != 1 {
		t.Fatalf("shutdown callback calls = %d, want 1", calls)
	}
}

func TestCryptoTraderRuntimeWebAPIDoesNotReacquireLegacyGate(t *testing.T) {
	oldConfig := config.APIServer
	config.APIServer = nil
	t.Cleanup(func() { config.APIServer = oldConfig })

	trader := NewCryptoTraderWithRuntime(newTestRuntimeLifecycle(), strat.NewBatchState(), nil)
	unlock := legacygate.Lock()
	done := make(chan *errs.Error, 1)
	go func() { done <- trader.startWebAPI() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runtime web API start failed: %v", err)
		}
	case <-time.After(time.Second):
		unlock()
		t.Fatal("runtime web API start reacquired the legacy gate")
	}
	unlock()
}

func TestCryptoTraderRuntimeDepsRetainOneSymbolState(t *testing.T) {
	symbols := orm.NewSymbolState()
	trader := NewCryptoTraderWithRuntimeDeps(nil, biz.RuntimeDeps{Symbols: symbols}, nil, nil)
	if trader.symbols != symbols || trader.RuntimeDependencies() == nil || trader.RuntimeDependencies().Symbols != symbols {
		t.Fatal("crypto trader did not retain the supplied symbol state")
	}

	dataTrader := NewCryptoTraderWithRuntimeDataDeps(nil, biz.RuntimeDeps{}, nil, nil, &data.RuntimeDeps{Symbols: symbols})
	if dataTrader.symbols != symbols || dataTrader.dataDeps.Symbols != symbols ||
		dataTrader.RuntimeDependencies() == nil || dataTrader.RuntimeDependencies().Symbols != symbols {
		t.Fatal("runtime data dependencies did not share the supplied symbol state")
	}
}

func TestCryptoTraderExplicitRuntimeDoesNotFallBackToGlobalExchange(t *testing.T) {
	oldDefault := exg.Default
	global := &banexg.Exchange{}
	exg.Default = global
	t.Cleanup(func() { exg.Default = oldDefault })

	trader := NewCryptoTraderWithRuntimeDeps(nil, biz.RuntimeDeps{}, nil, nil)
	if got := trader.exchangeForRun(); got != nil {
		t.Fatalf("explicit runtime exchange = %T, want nil", got)
	}
}

func TestCryptoTraderExplicitRuntimeUsesBoundExchange(t *testing.T) {
	exchange := &banexg.Exchange{}
	trader := NewCryptoTraderWithRuntimeDeps(nil, biz.RuntimeDeps{Exchange: exchange}, nil, nil)
	if got := trader.exchangeForRun(); got != exchange {
		t.Fatalf("explicit runtime exchange = %p, want %p", got, exchange)
	}
}

func TestCryptoTraderRuntimeLifecycleUsesBoundExchange(t *testing.T) {
	oldDefault := exg.Default
	legacy := &banexg.Exchange{}
	bound := &banexg.Exchange{}
	exg.Default = legacy
	t.Cleanup(func() { exg.Default = oldDefault })

	lifecycle := newTestRuntimeLifecycle()
	lifecycle.exchange = bound
	trader := NewCryptoTraderWithRuntime(lifecycle, strat.NewBatchState(), nil)
	if got := trader.exchangeForRun(); got != bound {
		t.Fatalf("runtime lifecycle exchange = %p, want bound exchange %p", got, bound)
	}
}

func TestCryptoTraderEmitRejectsForeignRuntimeSymbol(t *testing.T) {
	symbols := orm.NewSymbolStateWithIdentity("runtime", "spot")
	if err := symbols.SetExSymbols([]*orm.ExSymbol{{
		ID: 7, Exchange: "runtime", Market: "spot", Symbol: "RUNTIME/USDT",
	}}); err != nil {
		t.Fatal(err)
	}
	trader := NewCryptoTraderWithRuntimeDeps(nil, biz.RuntimeDeps{Symbols: symbols}, symbols, nil)
	trader.dp = &data.LiveProvider{}
	var notifications int
	trader.dp.OnDataSeries = func(*data.SeriesMsg, []*orm.DataSeries) *errs.Error {
		notifications++
		return nil
	}

	err := trader.Emit(&strat.DataSub{
		Source:    "macro",
		ExSymbol:  &orm.ExSymbol{ID: 7, Exchange: "legacy", Market: "spot", Symbol: "LEGACY/USDT"},
		TimeFrame: "1d",
	}, []*orm.DataRecord{{TimeMS: 1, EndMS: 2}})
	if err == nil {
		t.Fatal("Emit accepted a foreign runtime symbol")
	}
	if notifications != 0 {
		t.Fatalf("foreign Emit sent %d notifications", notifications)
	}
}

func TestCryptoTraderEmitPropagatesTraderFeedError(t *testing.T) {
	symbols := orm.NewSymbolStateWithIdentity("runtime", "spot")
	exs := &orm.ExSymbol{ID: 7, Exchange: "runtime", Market: "spot", Symbol: "RUNTIME/USDT"}
	if err := symbols.SetExSymbols([]*orm.ExSymbol{exs}); err != nil {
		t.Fatal(err)
	}
	trader := NewCryptoTraderWithRuntimeDeps(nil, biz.RuntimeDeps{Symbols: symbols}, symbols, nil)
	trader.dp = &data.LiveProvider{}
	notifications := 0
	trader.dp.OnDataSeries = func(*data.SeriesMsg, []*orm.DataSeries) *errs.Error {
		notifications++
		return nil
	}

	err := trader.Emit(&strat.DataSub{ExSymbol: exs, TimeFrame: "1m"}, []*orm.DataRecord{{
		TimeMS: 1, EndMS: 2,
		Values: map[string]any{
			"open": 1.0, "high": 2.0, "low": 1.0, "close": "bad", "volume": 1.0,
		},
	}})
	if err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("Emit feed error = %v, want invalid bar error", err)
	}
	if notifications != 0 {
		t.Fatalf("provider callback count after feed error = %d, want 0", notifications)
	}
}

func TestCryptoTraderRuntimeCloseSkipsPendingBatchTimer(t *testing.T) {
	lifecycle := newTestRuntimeLifecycle()
	t.Cleanup(lifecycle.close)
	state := strat.NewBatchState()

	called := make(chan struct{}, 1)
	job := &strat.StratJob{
		Strat: &strat.TradeStrat{
			Name: "pending-timer",
			OnBatchInfos: func(string, map[string]*strat.JobEnv) {
				called <- struct{}{}
			},
		},
		Symbol: &orm.ExSymbol{Symbol: "BTC/USDT"},
	}
	state.AddTask("1m_default_pending-timer", "BTC/USDT_info", &strat.JobEnv{
		Job: job, Env: &ta.BarEnv{}, Symbol: "BTC/USDT",
	}, 60_000, 0)
	trader := NewCryptoTraderWithRuntime(lifecycle, state, nil)
	trader.delayExecBatchAfter(100 * time.Millisecond)
	lifecycle.closeAndWait()
	trader.runtimeLock.Lock()
	pending := len(trader.runtimeTimers)
	trader.runtimeLock.Unlock()
	if pending != 0 {
		t.Fatalf("pending runtime timers after close = %d, want 0", pending)
	}

	select {
	case <-called:
		t.Fatal("closed runtime timer executed a batch callback")
	case <-time.After(150 * time.Millisecond):
	}
}

func TestCryptoTraderRuntimeCloseWaitsForReadyBatchTimer(t *testing.T) {
	lifecycle := newTestRuntimeLifecycle()
	t.Cleanup(lifecycle.close)
	state := strat.NewBatchState()

	entered := make(chan struct{})
	release := make(chan struct{})
	job := &strat.StratJob{
		Strat: &strat.TradeStrat{
			Name: "ready-timer",
			OnBatchInfos: func(string, map[string]*strat.JobEnv) {
				close(entered)
				<-release
			},
		},
		Symbol: &orm.ExSymbol{Symbol: "BTC/USDT"},
	}
	state.AddTask("1m_default_ready-timer", "BTC/USDT_info", &strat.JobEnv{
		Job: job, Env: &ta.BarEnv{}, Symbol: "BTC/USDT",
	}, 60_000, 0)
	trader := NewCryptoTraderWithRuntime(lifecycle, state, nil)
	trader.delayExecBatchAfter(0)

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("ready timer did not enter its batch callback")
	}
	closeDone := make(chan struct{})
	go func() {
		lifecycle.close()
		trader.joinRuntimeCallbacks()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		t.Fatal("runtime shutdown joined before ready timer callback completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)

	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("runtime shutdown did not join ready timer callback")
	}
}

func TestCryptoTraderRuntimeStopFromBatchCallbacksDoesNotDeadlock(t *testing.T) {
	oldAccounts, oldEnvReal := config.Accounts, core.EnvReal
	config.Accounts = map[string]*config.AccountConfig{config.DefAcc: {}}
	core.EnvReal = false
	biz.InitLocalOrderMgr(nil, false)
	t.Cleanup(func() {
		biz.ResetVars()
		config.Accounts = oldAccounts
		core.EnvReal = oldEnvReal
	})

	lifecycle := newTestRuntimeLifecycle()
	state := strat.NewBatchState()
	infoDone := make(chan struct{})
	mainDone := make(chan struct{})
	strategy := &strat.TradeStrat{
		Name: "runtime-stop-batch",
		OnBatchInfos: func(string, map[string]*strat.JobEnv) {
			lifecycle.stop()
			close(infoDone)
		},
		OnBatchJobs: func([]*strat.StratJob) {
			lifecycle.stop()
			close(mainDone)
		},
	}
	symbol := &orm.ExSymbol{Symbol: "BTC/USDT"}
	infoJob := &strat.StratJob{Strat: strategy, Env: &ta.BarEnv{}, Symbol: symbol}
	mainJob := &strat.StratJob{Strat: strategy, Env: &ta.BarEnv{}, Symbol: symbol}
	state.AddTask("1m_default_runtime-stop-batch", "BTC/USDT_info", &strat.JobEnv{
		Job: infoJob, Env: &ta.BarEnv{}, Symbol: symbol.Symbol,
	}, 60_000, 0)
	state.AddTask("1m_default_runtime-stop-batch", "BTC/USDT_main", &strat.JobEnv{
		Job: mainJob, Symbol: symbol.Symbol,
	}, 60_000, 0)
	trader := NewCryptoTraderWithRuntime(lifecycle, state, nil)
	trader.delayExecBatchAfter(0)

	select {
	case <-infoDone:
	case <-time.After(time.Second):
		t.Fatal("Runtime.Stop from OnBatchInfos deadlocked")
	}
	select {
	case <-mainDone:
	case <-time.After(time.Second):
		t.Fatal("Runtime.Stop from OnBatchJobs did not return")
	}
	trader.joinRuntimeCallbacks()
}

func TestCryptoTraderRuntimeStopClosesLiveProviderLoop(t *testing.T) {
	oldMode, oldLive := core.RunMode, core.LiveMode
	core.RunMode = core.RunModeLive
	core.LiveMode = true
	t.Cleanup(func() {
		core.RunMode, core.LiveMode = oldMode, oldLive
	})

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { _ = serverConn.Close() })
	conn := &blockingReadConn{Conn: clientConn, readStarted: make(chan struct{})}
	provider := newStubLiveProvider(conn)
	lifecycle := newTestRuntimeLifecycle()
	trader := NewCryptoTraderWithRuntime(lifecycle, strat.NewBatchState(), nil)
	trader.dp = provider
	loopDone := make(chan struct{})
	go func() {
		_ = provider.LoopMain()
		close(loopDone)
	}()
	select {
	case <-conn.readStarted:
	case <-time.After(time.Second):
		t.Fatal("live provider loop did not start reading")
	}

	lifecycle.stop()
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("Runtime.Stop did not terminate live provider loop")
	}
}

func TestCryptoTraderRuntimeStopWaitsForProviderHandlers(t *testing.T) {
	oldMode, oldLive := core.RunMode, core.LiveMode
	core.RunMode = core.RunModeLive
	core.LiveMode = true
	t.Cleanup(func() { core.RunMode, core.LiveMode = oldMode, oldLive })

	serverConn, clientConn := net.Pipe()
	server := &banutils.BanConn{Conn: serverConn, Ready: true}
	t.Cleanup(func() { _ = server.Close() })
	entered := make(chan struct{})
	release := make(chan struct{})
	provider := newStubLiveProvider(clientConn)
	provider.Listens["block"] = func(*banutils.IOMsgRaw) {
		close(entered)
		<-release
	}
	lifecycle := newTestRuntimeLifecycle()
	trader := NewCryptoTraderWithRuntime(lifecycle, strat.NewBatchState(), nil)
	trader.dp = provider
	go func() { _ = provider.LoopMain() }()
	if err := server.Write(&banutils.IOMsgRaw{Action: "block"}); err != nil {
		t.Fatalf("write blocking message: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("provider handler did not start")
	}

	stopDone := make(chan struct{})
	go func() {
		lifecycle.closeAndWait()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("runtime stop returned before provider handler completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("runtime stop did not join provider handler")
	}
	trader.joinRuntimeCallbacks()
}

func TestCryptoTraderRuntimeCloseJoinsHandlerBeforeReset(t *testing.T) {
	oldMode, oldLive := core.RunMode, core.LiveMode
	core.RunMode = core.RunModeLive
	core.LiveMode = true
	t.Cleanup(func() { core.RunMode, core.LiveMode = oldMode, oldLive })

	rt, err := runtimepkg.NewProcess().NewRuntime(runtimepkg.Options{
		Mode: core.RunModeLive,
		Env:  core.RunEnvDryRun,
	})
	if err != nil {
		t.Fatal(err)
	}

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	server := &banutils.BanConn{Conn: serverConn, Ready: true}
	provider := newStubLiveProvider(clientConn)
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseHandler)
	provider.Listens["block"] = func(*banutils.IOMsgRaw) {
		if got := rt.Batch.LastBatchMS(); got != 123 {
			t.Errorf("handler observed batch state %d before close completed, want 123", got)
		}
		close(entered)
		<-release
		if got := rt.Batch.LastBatchMS(); got != 123 {
			t.Errorf("handler observed batch state %d while in flight, want 123", got)
		}
	}
	rt.Batch.SetLastBatchMS(123)
	trader := NewCryptoTraderWithRuntimeDeps(rt, biz.RuntimeDeps{
		Core:   rt.Core,
		Clock:  rt.Clock,
		Market: rt.Market,
		Batch:  rt.Batch,
	}, rt.Symbols, nil)
	trader.dp = provider

	loopDone := make(chan struct{})
	go func() {
		_ = provider.LoopMain()
		close(loopDone)
	}()
	if err := server.Write(&banutils.IOMsgRaw{Action: "block"}); err != nil {
		t.Fatalf("write blocking message: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("provider handler did not start")
	}

	closeDone := make(chan struct{})
	go func() {
		rt.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		t.Fatal("runtime close returned before provider handler completed")
	case <-time.After(50 * time.Millisecond):
	}
	if got := rt.Batch.LastBatchMS(); got != 123 {
		t.Fatalf("batch state reset while handler was in flight: got %d, want 123", got)
	}
	releaseHandler()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("runtime close did not join provider handler")
	}
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("provider loop did not exit after runtime close")
	}
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("batch state after runtime close = %d, want 0", got)
	}

	secondCloseDone := make(chan struct{})
	go func() {
		rt.Close()
		close(secondCloseDone)
	}()
	select {
	case <-secondCloseDone:
	case <-time.After(time.Second):
		t.Fatal("repeated runtime close did not return")
	}
}

func TestCryptoTraderDropsEmitAfterRuntimeClose(t *testing.T) {
	lifecycle := newTestRuntimeLifecycle()
	trader := NewCryptoTraderWithRuntime(lifecycle, strat.NewBatchState(), nil)
	trader.dp = &data.LiveProvider{}
	var notifications int
	trader.dp.OnDataSeries = func(*data.SeriesMsg, []*orm.DataSeries) *errs.Error {
		notifications++
		return nil
	}
	sub := &strat.DataSub{
		Source:    "macro",
		ExSymbol:  &orm.ExSymbol{ID: 1, Symbol: "CPI_US"},
		TimeFrame: "1d",
	}
	rows := []*orm.DataRecord{{TimeMS: 1, EndMS: 2}}
	if err := trader.Emit(sub, rows); err != nil {
		t.Fatalf("Emit before close returned error: %v", err)
	}
	if notifications != 1 {
		t.Fatalf("notifications before close = %d, want 1", notifications)
	}

	lifecycle.closeAndWait()
	if err := trader.Emit(sub, rows); err != nil {
		t.Fatalf("Emit after close returned error: %v", err)
	}
	if notifications != 1 {
		t.Fatalf("callback ran after runtime close: notifications=%d, want 1", notifications)
	}
	lifecycle.closeAndWait()
}

func TestCryptoTraderRuntimeStopFromProviderHandlerDoesNotDeadlock(t *testing.T) {
	oldMode, oldLive := core.RunMode, core.LiveMode
	core.RunMode = core.RunModeLive
	core.LiveMode = true
	t.Cleanup(func() {
		core.RunMode, core.LiveMode = oldMode, oldLive
	})

	rt, err := runtimepkg.NewProcess().NewRuntime(runtimepkg.Options{
		Mode: core.RunModeLive,
		Env:  core.RunEnvDryRun,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { _ = serverConn.Close() })
	server := &banutils.BanConn{Conn: serverConn, Ready: true}
	provider := newStubLiveProvider(clientConn)
	stopDone := make(chan struct{})
	provider.Listens["stop"] = func(*banutils.IOMsgRaw) {
		rt.Stop()
		close(stopDone)
	}
	trader := NewCryptoTraderWithRuntime(rt, strat.NewBatchState(), nil)
	trader.dp = provider
	loopDone := make(chan struct{})
	go func() {
		_ = provider.LoopMain()
		close(loopDone)
	}()
	if err := server.Write(&banutils.IOMsgRaw{Action: "stop"}); err != nil {
		t.Fatalf("write stop message: %v", err)
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Runtime.Stop from provider handler deadlocked")
	}
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("provider loop did not exit after Runtime.Stop")
	}
}

func TestCryptoTraderRuntimeCloseFromProviderHandlerJoinsBeforeReset(t *testing.T) {
	oldMode, oldLive := core.RunMode, core.LiveMode
	core.RunMode = core.RunModeLive
	core.LiveMode = true
	t.Cleanup(func() {
		core.RunMode, core.LiveMode = oldMode, oldLive
	})

	rt, err := runtimepkg.NewProcess().NewRuntime(runtimepkg.Options{
		Mode: core.RunModeLive,
		Env:  core.RunEnvDryRun,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	rt.Batch.SetLastBatchMS(123)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()
	runtimeConfig := config.NewSnapshot(&config.Config{SpiderAddr: listener.Addr().String()})
	handlerDone := make(chan struct{})
	trader := NewCryptoTraderWithRuntimeDeps(rt, biz.RuntimeDeps{
		Core:       rt.Core,
		Clock:      rt.Clock,
		Market:     rt.Market,
		Batch:      rt.Batch,
		Strategies: rt.Strategies,
		Config:     runtimeConfig,
	}, rt.Symbols, nil)
	// Keep this provider's websocket registry unfiltered so the test can invoke
	// the real OnTrades wrapper without constructing a full symbol job.
	trader.dataDeps.Symbols = nil
	tradeJob := &strat.StratJob{Strat: &strat.TradeStrat{
		OnWsTrades: func(*strat.StratJob, string, []*banexg.Trade) {
			rt.Close()
			if got := rt.Batch.LastBatchMS(); got != 123 {
				t.Errorf("runtime state reset while provider handler was active: got %d, want 123", got)
			}
			close(handlerDone)
		},
	}}
	rt.Strategies.WsSubJobs[core.WsSubTrade] = map[string]map[*strat.StratJob]bool{
		"BTC/USDT": {
			tradeJob: true,
		},
	}
	provider, providerErr := data.NewLiveProviderWithRuntimeDeps(trader.dataDeps, nil, nil)
	if providerErr != nil {
		t.Fatal(providerErr)
	}
	_ = listener.Close()
	var serverConn net.Conn
	select {
	case serverConn = <-accepted:
	case acceptErr := <-acceptErr:
		t.Fatal(acceptErr)
	case <-time.After(time.Second):
		t.Fatal("typed live provider did not connect")
	}
	t.Cleanup(func() {
		if serverConn != nil {
			_ = serverConn.Close()
		}
	})
	trader.dp = provider

	go func() {
		provider.OnTrades("runtime", "spot", "BTC/USDT", []*banexg.Trade{{}})
	}()
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("Runtime.Close from provider handler did not return")
	}
	rt.Join()
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("batch state after Runtime.Close = %d, want 0", got)
	}
}

func TestCryptoTraderTypedCleanupUsesRuntimeStrategyState(t *testing.T) {
	oldLegacyJobs, oldUnWatch := strat.AccJobs, strat.WsSubUnWatch
	t.Cleanup(func() {
		strat.AccJobs = oldLegacyJobs
		strat.WsSubUnWatch = oldUnWatch
	})

	legacyCalled := false
	strat.AccJobs = map[string]map[string]map[string]*strat.StratJob{
		"legacy": {
			"legacy_env": {
				"legacy": {Strat: &strat.TradeStrat{
					OnShutDown: func(*strat.StratJob) { legacyCalled = true },
				}},
			},
		},
	}
	typedCalled := false
	typedState := strat.NewState()
	typedState.AccJobs["typed"] = map[string]map[string]*strat.StratJob{
		"typed_env": {
			"typed": {Strat: &strat.TradeStrat{
				OnShutDown: func(*strat.StratJob) { typedCalled = true },
			}},
		},
	}

	trader := NewCryptoTraderWithRuntimeDeps(nil, biz.RuntimeDeps{Strategies: typedState}, nil, nil)
	trader.enableRuntimeCleanup()
	trader.finishRunCleanup()
	if !typedCalled {
		t.Fatal("typed strategy shutdown callback did not run")
	}
	if legacyCalled {
		t.Fatal("typed live cleanup fell back to legacy strategy state")
	}
}

func TestCryptoTraderSetProviderFromProviderHandlerDefersJoinToOwner(t *testing.T) {
	oldMode, oldLive := core.RunMode, core.LiveMode
	core.RunMode = core.RunModeLive
	core.LiveMode = true
	t.Cleanup(func() { core.RunMode, core.LiveMode = oldMode, oldLive })

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	server := &banutils.BanConn{Conn: serverConn, Ready: true}
	oldProvider := newStubLiveProvider(clientConn)
	newProvider := &data.LiveProvider{}
	lifecycle := newTestRuntimeLifecycle()
	trader := NewCryptoTraderWithRuntime(lifecycle, strat.NewBatchState(), nil)
	trader.dp = oldProvider

	handlerStarted := make(chan struct{})
	swapped := make(chan struct{})
	handlerBlocked := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseHandler)
	accepted := make(chan bool, 1)
	oldProvider.SeriesWatcher.ClientIO.Listens["replace"] = func(*banutils.IOMsgRaw) {
		close(handlerStarted)
		accepted <- trader.setProvider(newProvider)
		close(swapped)
		close(handlerBlocked)
		<-release
	}

	loopDone := make(chan struct{})
	go func() {
		_ = oldProvider.LoopMain()
		close(loopDone)
	}()
	if err := server.Write(&banutils.IOMsgRaw{Action: "replace"}); err != nil {
		t.Fatalf("write provider replacement message: %v", err)
	}
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("provider handler did not start")
	}
	select {
	case <-swapped:
	case <-time.After(time.Second):
		t.Fatal("setProvider waited for the replacing provider handler")
	}
	if !<-accepted {
		t.Fatal("setProvider rejected the active runtime replacement")
	}
	if !oldProvider.SeriesWatcher.ClientIO.IsClosed() {
		t.Fatal("setProvider did not stop the retired provider immediately")
	}
	if got := trader.provider(); got != newProvider {
		t.Fatalf("current provider = %p, want replacement %p", got, newProvider)
	}
	select {
	case <-handlerBlocked:
	case <-time.After(time.Second):
		t.Fatal("provider handler did not reach its blocking point")
	}

	shutdownDone := make(chan struct{})
	go func() {
		lifecycle.closeAndWait()
		close(shutdownDone)
	}()
	select {
	case <-shutdownDone:
		t.Fatal("owner shutdown joined before the provider handler returned")
	case <-time.After(50 * time.Millisecond):
	}
	releaseHandler()
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("owner shutdown did not join the retired provider")
	}
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("retired provider loop did not exit")
	}

	trader.runtimeLock.Lock()
	deferred := providerInList(trader.retiredProviders, oldProvider)
	current := trader.dp
	trader.runtimeLock.Unlock()
	if deferred || current != nil {
		t.Fatalf("provider ownership after shutdown: retired=%v current=%p", deferred, current)
	}
}

func TestCryptoTraderRuntimeStateResetsBetweenRuns(t *testing.T) {
	lifecycle := newTestRuntimeLifecycle()
	t.Cleanup(lifecycle.close)
	trader := NewCryptoTraderWithRuntime(lifecycle, strat.NewBatchState(), nil)
	trader.initFn = func() *errs.Error {
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.collectJobsFn = func() []*strat.StratJob { return nil }
	trader.startJobsFn = func() {}
	var runs int
	trader.loopMainFn = func() *errs.Error {
		runs++
		if trader.runtimeStopped.Load() {
			t.Fatalf("runtime stop state leaked into run %d", runs)
		}
		return nil
	}
	for range 2 {
		if err := trader.runWithDeps(); err != nil {
			t.Fatalf("runWithDeps failed: %v", err)
		}
	}
	if runs != 2 {
		t.Fatalf("runs = %d, want 2", runs)
	}
}

func TestCryptoTraderRuntimeCloseWaitsForInitBeforeReset(t *testing.T) {
	rt, err := runtimepkg.NewProcess().NewRuntime(runtimepkg.Options{})
	if err != nil {
		t.Fatal(err)
	}
	rt.Batch.SetLastBatchMS(123)

	initStarted := make(chan struct{})
	releaseInit := make(chan struct{})
	startCalled := make(chan struct{}, 1)
	loopCalled := make(chan struct{}, 1)
	trader := NewCryptoTraderWithRuntimeDeps(rt, biz.RuntimeDeps{
		Core:   rt.Core,
		Clock:  rt.Clock,
		Market: rt.Market,
		Batch:  rt.Batch,
	}, rt.Symbols, nil)
	trader.initFn = func() *errs.Error {
		close(initStarted)
		<-releaseInit
		if !trader.setProvider(&data.LiveProvider{}) {
			return errs.NewMsg(core.ErrRunTime, "runtime is stopped")
		}
		return nil
	}
	trader.startJobsFn = func() { startCalled <- struct{}{} }
	trader.loopMainFn = func() *errs.Error {
		loopCalled <- struct{}{}
		return nil
	}

	traderDone := make(chan *errs.Error, 1)
	go func() { traderDone <- trader.runWithDeps() }()
	select {
	case <-initStarted:
	case <-time.After(time.Second):
		t.Fatal("trader init did not start")
	}

	closeDone := make(chan struct{})
	go func() {
		rt.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		t.Error("runtime close returned before in-flight init completed")
	case <-time.After(50 * time.Millisecond):
	}
	if got := rt.Batch.LastBatchMS(); got != 123 {
		t.Errorf("batch state reset while init was in flight: got %d, want 123", got)
	}

	close(releaseInit)
	select {
	case runErr := <-traderDone:
		if runErr == nil {
			t.Error("trader run continued after runtime close")
		}
	case <-time.After(time.Second):
		t.Fatal("trader run did not finish after init release")
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("runtime close did not finish after init release")
	}
	select {
	case <-startCalled:
		t.Error("startJobs ran after runtime close")
	default:
	}
	select {
	case <-loopCalled:
		t.Error("loopMain ran after runtime close")
	default:
	}
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Errorf("batch state after runtime close = %d, want 0", got)
	}
}

func TestCryptoTraderRunUsesRuntimeContext(t *testing.T) {
	lifecycle := newTestRuntimeLifecycle()
	state := strat.NewBatchState()
	wantCtx := lifecycle.Context()
	var startupCtx, bootstrapCtx context.Context
	trader := NewCryptoTraderWithRuntime(lifecycle, state, func(ctx context.Context, _ *CryptoTrader) error {
		startupCtx = ctx
		return nil
	})
	trader.initFn = func() *errs.Error {
		trader.dp = &data.LiveProvider{}
		return nil
	}
	job := &strat.StratJob{
		Symbol: &orm.ExSymbol{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
		Strat: &strat.TradeStrat{
			OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d"}}
			},
		},
	}
	trader.collectJobsFn = func() []*strat.StratJob { return []*strat.StratJob{job} }
	trader.nowMSFn = func() int64 { return 200_000_000 }
	trader.seriesRuntime = data.NewSeriesRuntime(trader)
	trader.seriesRuntime.EnsureFunc = func(ctx context.Context, _ *data.ThirdPartySeriesBootstrap) *errs.Error {
		bootstrapCtx = ctx
		return nil
	}
	trader.seriesRuntime.ActivateFn = func(context.Context, []*strat.DataSub, data.DataSink) ([]*strat.DataSub, error) {
		return nil, nil
	}
	trader.startJobsFn = func() {}
	trader.loopMainFn = func() *errs.Error { return nil }
	if err := trader.runWithDeps(); err != nil {
		t.Fatalf("runWithDeps failed: %v", err)
	}
	if startupCtx != wantCtx || bootstrapCtx != wantCtx {
		t.Fatalf("runtime context was not propagated: startup=%p bootstrap=%p want=%p", startupCtx, bootstrapCtx, wantCtx)
	}

	backgroundTrader := NewCryptoTrader()
	if got := backgroundTrader.runContext(); got != context.Background() {
		t.Fatalf("trader without runtime context = %p, want context.Background", got)
	}
}

func newStubTraderDataSource(name string) *stubTraderDataSource {
	return &stubTraderDataSource{
		info: &orm.SeriesInfo{
			Name:      name,
			TimeFrame: "1d",
			Binding: orm.SeriesBinding{
				Table:      name,
				TimeColumn: "ts",
				EndColumn:  "end_ms",
				SIDColumn:  "sid",
				Fields: []orm.SeriesField{
					{Name: "open", Type: "float", Role: "open"},
					{Name: "high", Type: "float", Role: "high"},
					{Name: "low", Type: "float", Role: "low"},
					{Name: "close", Type: "float", Role: "close"},
					{Name: "volume", Type: "float", Role: "volume"},
				},
			},
		},
	}
}

func traderTestSourceName(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("%s_%s", t.Name(), suffix)
}

func (s *stubTraderDataSource) Info() *orm.SeriesInfo {
	return s.info
}

func (s *stubTraderDataSource) FetchHistory(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error) {
	return nil, nil
}

func (s *stubTraderDataSource) SubscribeLive(ctx context.Context, subs []*strat.DataSub, sink data.DataSink) error {
	s.subscribeCount++
	cp := make([]*strat.DataSub, 0, len(subs))
	for _, sub := range subs {
		if sub == nil {
			cp = append(cp, nil)
			continue
		}
		dup := *sub
		cp = append(cp, &dup)
	}
	s.subscribedSubs = append(s.subscribedSubs, cp)
	if sink != nil && len(s.emissions) > 0 {
		for _, batch := range s.emissions {
			if err := sink.Emit(subs[0], batch); err != nil {
				return err
			}
		}
	}
	return s.subscribeErr
}

func TestCryptoTraderEmitRequiresStartupProvider(t *testing.T) {
	trader := &CryptoTrader{}
	err := trader.Emit(&strat.DataSub{ExSymbol: &orm.ExSymbol{ID: 1}, TimeFrame: "1d"}, []*orm.DataRecord{{TimeMS: 1, EndMS: 2}})
	if err == nil || err.Error() != "live provider is required" {
		t.Fatalf("expected missing live provider error, got %v", err)
	}
}

func TestCryptoTraderEmitRoutesThirdPartyRowsThroughOnData(t *testing.T) {
	oldAccounts := strat.AccInfoJobs
	strat.AccInfoJobs = map[string]map[string]map[string]*strat.StratJob{
		"default": {},
	}
	t.Cleanup(func() {
		strat.AccInfoJobs = oldAccounts
	})

	var got []*strat.DataFields
	job := &strat.StratJob{
		Strat: &strat.TradeStrat{
			OnData: func(s *strat.StratJob, fields strat.DataEvent) {
				got = append(got, fields.DataFields)
			},
		},
		DataHub: strat.NewDataHub(),
		Symbol:  &orm.ExSymbol{ID: 77, Exchange: "macro", Market: "macro", Symbol: "CPI_US"},
		Account: "default",
	}
	key := strat.DataSubKey("macro", 77, "1d")
	strat.AccInfoJobs["default"][key] = map[string]*strat.StratJob{"job": job}

	trader := &CryptoTrader{dp: &data.LiveProvider{}}
	notified := 0
	trader.dp.OnDataSeries = func(msg *data.SeriesMsg, rows []*orm.DataSeries) *errs.Error {
		notified++
		if msg == nil || msg.Pair != "CPI_US" || msg.TFSecs != 86400 {
			t.Fatalf("unexpected series notification: %+v", msg)
		}
		if len(rows) != 2 || rows[0].Source != "macro" || rows[1].TimeMS != 200 {
			t.Fatalf("unexpected emitted rows: %+v", rows)
		}
		return nil
	}

	err := trader.Emit(&strat.DataSub{Source: "macro", ExSymbol: job.Symbol, TimeFrame: "1d"}, []*orm.DataRecord{
		{TimeMS: 100, EndMS: 200, Closed: true, Values: map[string]any{"value": 10.0}},
		{TimeMS: 200, EndMS: 300, Closed: true, Values: map[string]any{"value": 11.0}},
	})
	if err != nil {
		t.Fatalf("Emit returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected OnData called twice, got %d", len(got))
	}
	if got[0].Source != "macro" || got[0].Sid != 77 || got[0].TimeFrame != "1d" {
		t.Fatalf("unexpected first OnData event: %+v", got[0])
	}
	if job.IsWarmUp {
		t.Fatalf("expected emitted live rows to stay non-warmup")
	}
	latest := job.DataHub.Get("1d", "macro", 77)
	if latest == nil || latest.TimeMS != 200 {
		t.Fatalf("expected latest macro row at 200, got %+v", latest)
	}
	series := latest.Series("value")
	if series == nil || series.Len() != 2 || series.Get(1) != 10 || series.Get(0) != 11 {
		t.Fatalf("unexpected macro series after emit: %+v", series)
	}
	if notified != 1 {
		t.Fatalf("expected provider callback once, got %d", notified)
	}
}

func TestCryptoTraderRunSkipsStartupWhenCallbackNil(t *testing.T) {
	trader := NewCryptoTrader()
	initCalls := 0
	startCalls := 0
	loopCalls := 0
	trader.initFn = func() *errs.Error {
		initCalls++
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.collectJobsFn = func() []*strat.StratJob { return nil }
	trader.startJobsFn = func() {
		startCalls++
	}
	trader.loopMainFn = func() *errs.Error {
		loopCalls++
		return nil
	}

	if err := trader.runWithDeps(); err != nil {
		t.Fatalf("runWithDeps failed: %v", err)
	}
	if initCalls != 1 {
		t.Fatalf("expected Init once, got %d", initCalls)
	}
	if startCalls != 1 || loopCalls != 1 {
		t.Fatalf("expected startJobs and loopMain once, got start=%d loop=%d", startCalls, loopCalls)
	}
}

func TestCryptoTraderRunEnsuresThirdPartyBeforeActivateAndLoop(t *testing.T) {
	alpha := newStubTraderDataSource(traderTestSourceName(t, "alpha"))
	if err := data.RegisterDataSource(alpha); err != nil {
		t.Fatalf("RegisterDataSource alpha failed: %v", err)
	}
	job := &strat.StratJob{
		Symbol: &orm.ExSymbol{ID: 101, Symbol: "BTC/USDT"},
		Strat: &strat.TradeStrat{
			Name: "stg",
			OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{{Source: alpha.info.Name, ExSymbol: s.Symbol, TimeFrame: alpha.info.TimeFrame, WarmupNum: 4}}
			},
		},
	}
	collected := []*strat.DataSub{{
		Source: alpha.info.Name, ExSymbol: &orm.ExSymbol{ID: 101, Symbol: "BTC/USDT"},
		TimeFrame: alpha.info.TimeFrame, WarmupNum: 4,
		Fields:       []string{"open", "high", "low", "close", "volume"},
		SeriesFields: []string{"open", "high", "low", "close", "volume"},
	}}
	trader := NewCryptoTrader()
	steps := make([]string, 0, 4)
	trader.initFn = func() *errs.Error {
		trader.dp = &data.LiveProvider{}
		steps = append(steps, "init")
		return nil
	}
	trader.collectJobsFn = func() []*strat.StratJob {
		steps = append(steps, "collect")
		return []*strat.StratJob{job}
	}
	trader.nowMSFn = func() int64 { return 50_000 }
	trader.seriesRuntime = data.NewSeriesRuntime(trader)
	trader.seriesRuntime.EnsureFunc = func(ctx context.Context, plan *data.ThirdPartySeriesBootstrap) *errs.Error {
		steps = append(steps, fmt.Sprintf("ensure:%d:%d", plan.StartMS, plan.EndMS))
		if !reflect.DeepEqual(plan.Subs, collected) {
			t.Fatalf("unexpected subs passed to ensure: %+v", plan.Subs)
		}
		return nil
	}
	trader.seriesRuntime.ActivateFn = func(ctx context.Context, subs []*strat.DataSub, sink data.DataSink) ([]*strat.DataSub, error) {
		steps = append(steps, "activate")
		if sink != trader {
			t.Fatalf("expected trader sink")
		}
		if !reflect.DeepEqual(subs, collected) {
			t.Fatalf("unexpected subs passed to activate: %+v", subs)
		}
		return subs, nil
	}
	trader.startJobsFn = func() { steps = append(steps, "start") }
	trader.loopMainFn = func() *errs.Error {
		steps = append(steps, "loop")
		return nil
	}

	if err := trader.runWithDeps(); err != nil {
		t.Fatalf("runWithDeps failed: %v", err)
	}
	want := []string{"init", "collect", "ensure:-345550000:50000", "activate", "start", "loop"}
	if !reflect.DeepEqual(steps, want) {
		t.Fatalf("unexpected startup order\nwant: %+v\n got: %+v", want, steps)
	}
	if alpha.subscribeCount != 0 {
		t.Fatalf("expected injected activation seam to bypass real data source calls, got %d", alpha.subscribeCount)
	}
}

func TestCryptoTraderRunEnsureFailureStopsBeforeActivateAndLoop(t *testing.T) {
	trader := NewCryptoTrader()
	trader.initFn = func() *errs.Error {
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.collectJobsFn = func() []*strat.StratJob {
		return []*strat.StratJob{{
			Symbol: &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT"},
			Strat: &strat.TradeStrat{
				Name: "stg",
				OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
					return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 1}}
				},
			},
		}}
	}
	trader.nowMSFn = func() int64 { return 100_000 }
	trader.seriesRuntime = data.NewSeriesRuntime(trader)
	trader.seriesRuntime.EnsureFunc = func(ctx context.Context, plan *data.ThirdPartySeriesBootstrap) *errs.Error {
		return errs.NewMsg(1001, "bootstrap ensure source=macro sid=7 tf=1d phase=ensure: boom")
	}
	activateCalls := 0
	trader.seriesRuntime.ActivateFn = func(ctx context.Context, subs []*strat.DataSub, sink data.DataSink) ([]*strat.DataSub, error) {
		activateCalls++
		return subs, nil
	}
	startCalls := 0
	loopCalls := 0
	trader.startJobsFn = func() { startCalls++ }
	trader.loopMainFn = func() *errs.Error {
		loopCalls++
		return nil
	}

	err := trader.runWithDeps()
	if err == nil || !strings.Contains(err.Message(), "phase=ensure") {
		t.Fatalf("expected ensure-phase startup error, got %v", err)
	}
	if activateCalls != 0 || startCalls != 0 || loopCalls != 0 {
		t.Fatalf("expected ensure failure to stop before activate/start/loop, got activate=%d start=%d loop=%d", activateCalls, startCalls, loopCalls)
	}
}

func TestCryptoTraderRunActivateFailureStopsBeforeLoop(t *testing.T) {
	trader := NewCryptoTrader()
	trader.initFn = func() *errs.Error {
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.collectJobsFn = func() []*strat.StratJob {
		return []*strat.StratJob{{
			Symbol: &orm.ExSymbol{ID: 9, Symbol: "BTC/USDT"},
			Strat: &strat.TradeStrat{
				Name: "stg",
				OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
					return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 2}}
				},
			},
		}}
	}
	trader.nowMSFn = func() int64 { return 100_000 }
	trader.seriesRuntime = data.NewSeriesRuntime(trader)
	trader.seriesRuntime.EnsureFunc = func(ctx context.Context, plan *data.ThirdPartySeriesBootstrap) *errs.Error {
		return nil
	}
	trader.seriesRuntime.ActivateFn = func(ctx context.Context, subs []*strat.DataSub, sink data.DataSink) ([]*strat.DataSub, error) {
		return nil, fmt.Errorf("activate data source \"macro\": bad source")
	}
	startCalls := 0
	loopCalls := 0
	trader.startJobsFn = func() { startCalls++ }
	trader.loopMainFn = func() *errs.Error {
		loopCalls++
		return nil
	}

	err := trader.runWithDeps()
	if err == nil || !strings.Contains(err.Message(), "phase=activate") {
		t.Fatalf("expected activate-phase startup error, got %v", err)
	}
	if trader.seriesRuntime.Active(strat.DataSubKey("macro", 9, "1d")) {
		t.Fatalf("expected failed activation to remain retryable")
	}
	if startCalls != 0 || loopCalls != 0 {
		t.Fatalf("expected activation failure to stop before start/loop, got start=%d loop=%d", startCalls, loopCalls)
	}
}

func TestCryptoTraderRunMarksPartiallyActivatedSourcesRetryable(t *testing.T) {
	trader := NewCryptoTrader()
	trader.initFn = func() *errs.Error {
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.collectJobsFn = func() []*strat.StratJob {
		return []*strat.StratJob{{
			Symbol: &orm.ExSymbol{ID: 9, Symbol: "BTC/USDT"},
			Strat: &strat.TradeStrat{
				Name: "stg",
				OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
					return []*strat.DataSub{
						{Source: "alpha", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 2},
						{Source: "beta", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 2},
					}
				},
			},
		}}
	}
	trader.nowMSFn = func() int64 { return 100_000 }
	trader.seriesRuntime = data.NewSeriesRuntime(trader)
	trader.seriesRuntime.EnsureFunc = func(ctx context.Context, plan *data.ThirdPartySeriesBootstrap) *errs.Error {
		return nil
	}
	trader.seriesRuntime.ActivateFn = func(ctx context.Context, subs []*strat.DataSub, sink data.DataSink) ([]*strat.DataSub, error) {
		if len(subs) != 2 {
			t.Fatalf("expected alpha and beta activation attempt, got %+v", subs)
		}
		return subs[:1], fmt.Errorf("activate data source \"beta\": bad source")
	}
	trader.startJobsFn = func() { t.Fatal("startJobs should not run after activation failure") }
	trader.loopMainFn = func() *errs.Error {
		t.Fatal("loopMain should not run after activation failure")
		return nil
	}

	err := trader.runWithDeps()
	if err == nil || !strings.Contains(err.Message(), "phase=activate") {
		t.Fatalf("expected activate-phase startup error, got %v", err)
	}
	if !trader.seriesRuntime.Active(strat.DataSubKey("alpha", 9, "1d")) {
		t.Fatalf("expected successfully activated alpha sub to be marked active")
	}
	if trader.seriesRuntime.Active(strat.DataSubKey("beta", 9, "1d")) {
		t.Fatalf("expected failed beta sub to remain retryable")
	}
}

func TestCryptoTraderRunStartupActivatesSelectedSourcesOnce(t *testing.T) {
	alpha := newStubTraderDataSource(traderTestSourceName(t, "alpha"))
	beta := newStubTraderDataSource(traderTestSourceName(t, "beta"))
	if err := data.RegisterDataSource(alpha); err != nil {
		t.Fatalf("RegisterDataSource alpha failed: %v", err)
	}
	if err := data.RegisterDataSource(beta); err != nil {
		t.Fatalf("RegisterDataSource beta failed: %v", err)
	}

	trader := NewCryptoTraderWith(func(ctx context.Context, trader *CryptoTrader) error {
		_, err := data.ActivateDataSources(ctx, []*strat.DataSub{
			{Source: alpha.info.Name, ExSymbol: &orm.ExSymbol{ID: 101, Symbol: "BTC/USDT"}, TimeFrame: alpha.info.TimeFrame},
			{Source: beta.info.Name, ExSymbol: &orm.ExSymbol{ID: 202, Symbol: "ETH/USDT"}, TimeFrame: beta.info.TimeFrame},
			{Source: alpha.info.Name, ExSymbol: &orm.ExSymbol{ID: 303, Symbol: "SOL/USDT"}, TimeFrame: alpha.info.TimeFrame},
		}, trader)
		return err
	})
	initCalls := 0
	startCalls := 0
	loopCalls := 0
	trader.initFn = func() *errs.Error {
		initCalls++
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.startJobsFn = func() {
		startCalls++
	}
	trader.loopMainFn = func() *errs.Error {
		loopCalls++
		return nil
	}

	if err := trader.runWithDeps(); err != nil {
		t.Fatalf("runWithDeps failed: %v", err)
	}
	if initCalls != 1 {
		t.Fatalf("expected Init once, got %d", initCalls)
	}
	if alpha.subscribeCount != 1 || beta.subscribeCount != 1 {
		t.Fatalf("expected grouped source activation once per source, got alpha=%d beta=%d", alpha.subscribeCount, beta.subscribeCount)
	}
	if len(alpha.subscribedSubs) != 1 || len(alpha.subscribedSubs[0]) != 2 {
		t.Fatalf("expected alpha activation group of 2 subs, got %+v", alpha.subscribedSubs)
	}
	if len(beta.subscribedSubs) != 1 || len(beta.subscribedSubs[0]) != 1 {
		t.Fatalf("expected beta activation group of 1 sub, got %+v", beta.subscribedSubs)
	}
	if startCalls != 1 || loopCalls != 1 {
		t.Fatalf("expected startup completion before loop, got start=%d loop=%d", startCalls, loopCalls)
	}
}

func TestCryptoTraderRunStartupReturnsUnknownSourceError(t *testing.T) {
	trader := NewCryptoTraderWith(func(ctx context.Context, trader *CryptoTrader) error {
		_, err := data.ActivateDataSources(ctx, []*strat.DataSub{{
			Source:    "bot_missing_source",
			ExSymbol:  &orm.ExSymbol{ID: 404, Symbol: "BTC/USDT"},
			TimeFrame: "1d",
		}}, trader)
		return err
	})
	startCalls := 0
	loopCalls := 0
	trader.initFn = func() *errs.Error {
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.startJobsFn = func() {
		startCalls++
	}
	trader.loopMainFn = func() *errs.Error {
		loopCalls++
		return nil
	}

	err := trader.runWithDeps()
	if err == nil {
		t.Fatalf("expected unknown source startup error")
	}
	if startCalls != 0 || loopCalls != 0 {
		t.Fatalf("expected startup error to stop before jobs/loop, got start=%d loop=%d", startCalls, loopCalls)
	}
}

func TestCryptoTraderRunStartupReturnsCallbackError(t *testing.T) {
	want := errors.New("startup boom")
	trader := NewCryptoTraderWith(func(ctx context.Context, trader *CryptoTrader) error {
		return want
	})
	startCalls := 0
	loopCalls := 0
	trader.initFn = func() *errs.Error {
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.startJobsFn = func() {
		startCalls++
	}
	trader.loopMainFn = func() *errs.Error {
		loopCalls++
		return nil
	}

	err := trader.runWithDeps()
	if err == nil || err.Message() != want.Error() {
		t.Fatalf("expected startup callback error %q, got %v", want.Error(), err)
	}
	if startCalls != 0 || loopCalls != 0 {
		t.Fatalf("expected startup error to stop before jobs/loop, got start=%d loop=%d", startCalls, loopCalls)
	}
}
