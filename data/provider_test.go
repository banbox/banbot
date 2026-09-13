package data

import (
	"fmt"
	"math"
	"net"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type providerBlockingConn struct {
	net.Conn
	readStarted chan struct{}
	once        sync.Once
}

func TestRuntimeProvidersRequireExplicitStrategyState(t *testing.T) {
	if provider, err := NewHistProviderWithRuntimeDeps(&RuntimeDeps{}, nil, nil, nil, false, nil); err == nil || provider != nil {
		t.Fatalf("historical provider = %#v, error = %v", provider, err)
	}
	if provider, err := NewLiveProviderWithRuntimeDeps(&RuntimeDeps{}, nil, nil); err == nil || provider != nil {
		t.Fatalf("live provider = %#v, error = %v", provider, err)
	}
}

func (c *providerBlockingConn) Read(buf []byte) (int, error) {
	c.once.Do(func() { close(c.readStarted) })
	return c.Conn.Read(buf)
}

func TestLiveProviderCloseConcurrentWithRunForever(t *testing.T) {
	oldMode, oldLive := core.RunMode, core.LiveMode
	core.RunMode = core.RunModeLive
	core.LiveMode = true
	t.Cleanup(func() { core.RunMode, core.LiveMode = oldMode, oldLive })

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { _ = serverConn.Close() })
	conn := &providerBlockingConn{Conn: clientConn, readStarted: make(chan struct{})}
	client := &utils.ClientIO{BanConn: utils.BanConn{
		Conn:      conn,
		Data:      map[string]interface{}{},
		Listens:   map[string]utils.ConnCB{},
		Ready:     true,
		DoConnect: func(*utils.BanConn) {},
	}}
	provider := &LiveProvider{SeriesWatcher: &SeriesWatcher{ClientIO: client}}
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

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := provider.Close(); err != nil {
				t.Errorf("close live provider: %v", err)
			}
		}()
	}
	wg.Wait()

	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("concurrent Close did not terminate live provider loop")
	}
	if !client.IsClosed() {
		t.Fatal("client connection remained open after Close")
	}
}

func TestLiveProviderCloseWaitsForSocketHandler(t *testing.T) {
	oldMode, oldLive := core.RunMode, core.LiveMode
	core.RunMode = core.RunModeLive
	core.LiveMode = true
	t.Cleanup(func() { core.RunMode, core.LiveMode = oldMode, oldLive })

	serverConn, clientConn := net.Pipe()
	server := &utils.BanConn{Conn: serverConn, Ready: true}
	t.Cleanup(func() { _ = server.Close() })
	entered := make(chan struct{})
	release := make(chan struct{})
	client := &utils.ClientIO{BanConn: utils.BanConn{
		Conn: clientConn,
		Data: map[string]interface{}{},
		Listens: map[string]utils.ConnCB{"block": func(*utils.IOMsgRaw) {
			close(entered)
			<-release
		}},
		Ready: true,
	}}
	provider := &LiveProvider{SeriesWatcher: &SeriesWatcher{ClientIO: client}}
	loopDone := make(chan struct{})
	go func() {
		_ = provider.LoopMain()
		close(loopDone)
	}()
	if err := server.Write(&utils.IOMsgRaw{Action: "block"}); err != nil {
		t.Fatalf("write blocking message: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("socket handler did not start")
	}

	closeDone := make(chan struct{})
	go func() {
		_ = provider.Close()
		provider.Join()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		t.Fatal("Close returned before socket handler completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not join socket handler")
	}
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not terminate socket loop")
	}
}

type stubDataFeeder struct {
	symbol       string
	states       []*PairTFCache
	warmLog      *[]string
	onNewDataFn  func()
	onNewDataErr *errs.Error
	waitStarted  chan struct{}
	waitRelease  <-chan struct{}
	waitOnce     sync.Once
}

type providerCallbackTracker struct {
	entered atomic.Int32
	left    atomic.Int32
}

type providerLifecycleRecorder struct {
	close func()
	wait  func()
}

func (r *providerLifecycleRecorder) EnterCallback() bool { return true }

func (r *providerLifecycleRecorder) LeaveCallback() {}

func (r *providerLifecycleRecorder) OnClose(call func()) {
	r.close = call
}

func (r *providerLifecycleRecorder) OnCloseWait(call func()) {
	r.wait = call
}

func TestLiveProviderRegistersDirectRuntimeLifecycle(t *testing.T) {
	recorder := &providerLifecycleRecorder{}
	provider := &LiveProvider{deps: &RuntimeDeps{Callbacks: recorder}}
	provider.registerLifecycle()
	provider.registerLifecycle()
	if recorder.close == nil || recorder.wait == nil {
		t.Fatal("provider did not register both lifecycle phases")
	}
	recorder.close()
	if !provider.handlerStop {
		t.Fatal("provider stop hook did not close handler admission")
	}
	recorder.wait()
	// The once guard must not replace the callbacks on a repeated registration.
	if recorder.close == nil || recorder.wait == nil {
		t.Fatal("provider lifecycle registration was not stable")
	}
}

func (t *providerCallbackTracker) EnterCallback() bool {
	t.entered.Add(1)
	return true
}

func (t *providerCallbackTracker) LeaveCallback() {
	t.left.Add(1)
}

func (f *stubDataFeeder) getSymbol() string { return f.symbol }
func (f *stubDataFeeder) getWaitData() *orm.DataSeries {
	if f.waitStarted != nil {
		f.waitOnce.Do(func() { close(f.waitStarted) })
		<-f.waitRelease
	}
	return nil
}
func (f *stubDataFeeder) setWaitData(*orm.DataSeries) {}
func (f *stubDataFeeder) getStates() []*PairTFCache   { return f.states }
func (f *stubDataFeeder) onNewData(int64, []*orm.DataSeries) (bool, *errs.Error) {
	if f.onNewDataFn != nil {
		f.onNewDataFn()
	}
	return false, f.onNewDataErr
}
func (f *stubDataFeeder) SubTfs(tfs []string, _ bool) []string { return tfs }
func (f *stubDataFeeder) WarmTfs(_ int64, tfNums map[string]int, _ *utils.PrgBar) (int64, map[string][2]int, *errs.Error) {
	if f.warmLog != nil {
		*f.warmLog = append(*f.warmLog, fmt.Sprintf("%s:%d", f.symbol, tfNums["1h"]))
	}
	return 0, nil, nil
}

func TestLiveProviderHandlerStopsAfterFeederError(t *testing.T) {
	called := 0
	provider := &LiveProvider{
		OnDataSeries: func(*SeriesMsg, []*orm.DataSeries) *errs.Error {
			called++
			return nil
		},
	}
	hold := &stubDataFeeder{onNewDataErr: errs.NewMsg(core.ErrDbReadFail, "enrichment failed")}
	provider.handlerWait.Add(1)
	provider.runHandler(hold, 60_000, &SeriesMsg{Pair: "BTC/USDT"}, []*orm.DataSeries{{TimeMS: 100}})
	if called != 0 {
		t.Fatalf("OnDataSeries called after feeder error: %d", called)
	}
}

func TestLiveProviderConcurrentCloseWaitsForSeriesHandlerAndStopsAdmission(t *testing.T) {
	oldExgName, oldMarket := core.ExgName, core.Market
	core.ExgName, core.Market = "test", "spot"
	t.Cleanup(func() { core.ExgName, core.Market = oldExgName, oldMarket })

	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	feeder := &stubDataFeeder{symbol: "BTC/USDT", onNewDataFn: func() {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
	}}
	provider := &LiveProvider{
		Provider: Provider[IDataFeeder]{holders: map[string]IDataFeeder{"BTC/USDT": feeder}},
		SeriesWatcher: &SeriesWatcher{ClientIO: &utils.ClientIO{BanConn: utils.BanConn{
			Data:    map[string]interface{}{},
			Listens: map[string]utils.ConnCB{},
		}}},
	}
	handle := makeOnSeriesMsg(provider)
	msg := &SeriesMsg{
		ExgName: "test", Market: "spot", Pair: "BTC/USDT",
		NotifySeries: NotifySeries{TFSecs: 60, Interval: 60, Rows: []*orm.DataSeries{{TimeMS: 1}}},
	}
	handle(msg)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("series handler did not start")
	}

	const closeCount = 8
	closeDone := make(chan struct{}, closeCount)
	for range closeCount {
		go func() {
			_ = provider.Close()
			provider.Join()
			closeDone <- struct{}{}
		}()
	}
	select {
	case <-closeDone:
		t.Fatal("concurrent Close returned before series handler completed")
	case <-time.After(50 * time.Millisecond):
	}
	handle(msg)
	close(release)
	for range closeCount {
		select {
		case <-closeDone:
		case <-time.After(time.Second):
			t.Fatal("concurrent Close did not join series handler")
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("series handlers admitted after Close: calls=%d, want 1", got)
	}
}

func TestLiveProviderCallbackLeaseCoversHandlerLifetime(t *testing.T) {
	oldExgName, oldMarket := core.ExgName, core.Market
	core.ExgName, core.Market = "test", "spot"
	t.Cleanup(func() { core.ExgName, core.Market = oldExgName, oldMarket })

	tracker := &providerCallbackTracker{}
	started := make(chan struct{})
	release := make(chan struct{})
	provider := &LiveProvider{
		Provider: Provider[IDataFeeder]{holders: map[string]IDataFeeder{
			"BTC/USDT": &stubDataFeeder{symbol: "BTC/USDT"},
		}},
		deps: &RuntimeDeps{
			Callbacks:    tracker,
			ExchangeName: "test",
			MarketType:   "spot",
		},
		SeriesWatcher: &SeriesWatcher{ClientIO: &utils.ClientIO{BanConn: utils.BanConn{
			Data:    map[string]interface{}{},
			Listens: map[string]utils.ConnCB{},
		}}},
	}
	provider.OnDataSeries = func(*SeriesMsg, []*orm.DataSeries) *errs.Error {
		close(started)
		<-release
		return nil
	}

	msg := &SeriesMsg{
		ExgName: "test", Market: "spot", Pair: "BTC/USDT",
		NotifySeries: NotifySeries{TFSecs: 60, Interval: 60, Rows: []*orm.DataSeries{{TimeMS: 1}}},
	}
	makeOnSeriesMsg(provider)(msg)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("provider callback did not start")
	}

	joined := make(chan struct{})
	go func() {
		_ = provider.Stop()
		provider.Join()
		close(joined)
	}()
	select {
	case <-joined:
		t.Fatal("provider joined before callback release")
	case <-time.After(50 * time.Millisecond):
	}
	if got := tracker.entered.Load(); got != 1 {
		t.Fatalf("callback lease enters = %d, want 1", got)
	}
	if got := tracker.left.Load(); got != 0 {
		t.Fatalf("callback lease left while handler was blocked: %d", got)
	}

	close(release)
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("provider did not join callback after release")
	}
	if got := tracker.left.Load(); got != 1 {
		t.Fatalf("callback lease leaves = %d, want 1", got)
	}
}

func TestLiveProviderCloseFromSeriesHandlerDoesNotDeadlock(t *testing.T) {
	for _, stop := range []struct {
		name string
		call func(*LiveProvider) *errs.Error
	}{
		{name: "Close", call: func(provider *LiveProvider) *errs.Error { return provider.Close() }},
		{name: "Stop", call: func(provider *LiveProvider) *errs.Error { return provider.Stop() }},
	} {
		t.Run(stop.name, func(t *testing.T) {
			oldExgName, oldMarket := core.ExgName, core.Market
			core.ExgName, core.Market = "test", "spot"
			t.Cleanup(func() { core.ExgName, core.Market = oldExgName, oldMarket })

			closed := make(chan struct{})
			feeder := &stubDataFeeder{symbol: "BTC/USDT"}
			provider := &LiveProvider{
				Provider: Provider[IDataFeeder]{holders: map[string]IDataFeeder{"BTC/USDT": feeder}},
				SeriesWatcher: &SeriesWatcher{ClientIO: &utils.ClientIO{BanConn: utils.BanConn{
					Data:    map[string]interface{}{},
					Listens: map[string]utils.ConnCB{},
				}}},
			}
			provider.OnDataSeries = func(*SeriesMsg, []*orm.DataSeries) *errs.Error {
				_ = stop.call(provider)
				close(closed)
				return nil
			}
			msg := &SeriesMsg{
				ExgName: "test", Market: "spot", Pair: "BTC/USDT",
				NotifySeries: NotifySeries{TFSecs: 60, Interval: 60, Rows: []*orm.DataSeries{{TimeMS: 1}}},
			}
			makeOnSeriesMsg(provider)(msg)
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatalf("LiveProvider.%s from series handler deadlocked", stop.name)
			}
			provider.Join()
		})
	}
}

func TestLiveProviderSeriesAdmissionCarriesIntoAsyncHandler(t *testing.T) {
	oldExgName, oldMarket := core.ExgName, core.Market
	core.ExgName, core.Market = "test", "spot"
	t.Cleanup(func() { core.ExgName, core.Market = oldExgName, oldMarket })

	waitStarted := make(chan struct{})
	releaseWait := make(chan struct{})
	var calls atomic.Int32
	feeder := &stubDataFeeder{
		symbol:      "BTC/USDT",
		waitStarted: waitStarted,
		waitRelease: releaseWait,
		onNewDataFn: func() { calls.Add(1) },
	}
	provider := &LiveProvider{
		Provider: Provider[IDataFeeder]{holders: map[string]IDataFeeder{"BTC/USDT": feeder}},
		SeriesWatcher: &SeriesWatcher{ClientIO: &utils.ClientIO{BanConn: utils.BanConn{
			Data:    map[string]interface{}{},
			Listens: map[string]utils.ConnCB{},
		}}},
	}
	msg := &SeriesMsg{
		ExgName: "test", Market: "spot", Pair: "BTC/USDT",
		NotifySeries: NotifySeries{
			TFSecs: 60, Interval: 10,
			Rows: []*orm.DataSeries{{TimeMS: 1, EndMS: 2, Values: map[string]any{
				"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0,
			}}},
		},
	}
	handlerDone := make(chan struct{})
	go func() {
		makeOnSeriesMsg(provider)(msg)
		close(handlerDone)
	}()
	select {
	case <-waitStarted:
	case <-time.After(time.Second):
		t.Fatal("series handler did not reach the admission boundary")
	}

	joinDone := make(chan struct{})
	go func() {
		if err := provider.Stop(); err != nil {
			t.Errorf("stop live provider: %v", err)
		}
		provider.Join()
		close(joinDone)
	}()
	select {
	case <-joinDone:
		t.Fatal("Stop/Join returned while admitted series handler was blocked")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseWait)
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("series handler did not finish after release")
	}
	select {
	case <-joinDone:
	case <-time.After(time.Second):
		t.Fatal("Stop/Join did not wait for admitted async series handler")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("admitted series handler calls = %d, want 1", got)
	}
}

func TestLiveProviderStopJoinSealsCallbackBeforeStateReset(t *testing.T) {
	oldExgName, oldMarket := core.ExgName, core.Market
	core.ExgName, core.Market = "test", "spot"
	t.Cleanup(func() { core.ExgName, core.Market = oldExgName, oldMarket })

	batch := strat.NewBatchState()
	batch.SetLastBatchMS(123)
	entered := make(chan struct{})
	var calls atomic.Int32
	var resetTouches atomic.Int32
	provider := &LiveProvider{
		Provider: Provider[IDataFeeder]{holders: map[string]IDataFeeder{
			"BTC/USDT": &stubDataFeeder{symbol: "BTC/USDT"},
		}},
		OnDataSeries: func(*SeriesMsg, []*orm.DataSeries) *errs.Error {
			if batch.LastBatchMS() != 123 {
				resetTouches.Add(1)
			}
			if calls.Add(1) == 1 {
				close(entered)
			}
			return nil
		},
	}
	msg := &SeriesMsg{
		ExgName: "test", Market: "spot", Pair: "BTC/USDT",
		NotifySeries: NotifySeries{TFSecs: 60, Interval: 60, Rows: []*orm.DataSeries{{TimeMS: 1}}},
	}

	makeOnSeriesMsg(provider)(msg)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("series callback did not start")
	}
	if err := provider.Stop(); err != nil {
		t.Fatal(err)
	}
	provider.Join()

	batch.Reset()
	makeOnSeriesMsg(provider)(msg)
	if got := calls.Load(); got != 1 {
		t.Fatalf("callbacks after Stop/Join = %d, want 1", got)
	}
	if got := resetTouches.Load(); got != 0 {
		t.Fatalf("callback touched reset state %d times", got)
	}
}

func TestLiveProviderCloseJoinsWsCallbacksAndStopsAdmission(t *testing.T) {
	oldJobs := strat.WsSubJobs
	t.Cleanup(func() {
		strat.LockJobsWrite()
		strat.WsSubJobs = oldJobs
		strat.UnlockJobsWrite()
		strat.RefreshWsSubJobsSnapshot()
	})

	for _, msgType := range []string{core.WsSubTrade, core.WsSubDepth} {
		for _, stop := range []struct {
			name string
			call func(*LiveProvider) *errs.Error
		}{
			{name: "Close", call: func(provider *LiveProvider) *errs.Error { return provider.Close() }},
			{name: "Stop", call: func(provider *LiveProvider) *errs.Error { return provider.Stop() }},
		} {
			t.Run(msgType+"/"+stop.name, func(t *testing.T) {
				entered := make(chan struct{})
				release := make(chan struct{})
				var calls atomic.Int32
				strategy := &strat.TradeStrat{}
				if msgType == core.WsSubTrade {
					strategy.OnWsTrades = func(*strat.StratJob, string, []*banexg.Trade) {
						calls.Add(1)
						close(entered)
						<-release
					}
				} else {
					strategy.OnWsDepth = func(*strat.StratJob, *banexg.OrderBook) {
						calls.Add(1)
						close(entered)
						<-release
					}
				}
				job := &strat.StratJob{Strat: strategy}
				strat.LockJobsWrite()
				strat.WsSubJobs = map[string]map[string]map[*strat.StratJob]bool{
					msgType: {"BTC/USDT": {job: true}},
				}
				strat.UnlockJobsWrite()
				strat.RefreshWsSubJobsSnapshot()
				provider := &LiveProvider{}
				invoke := func() {
					if msgType == core.WsSubTrade {
						makeOnTrade(provider)("test", "spot", "BTC/USDT", []*banexg.Trade{{Symbol: "BTC/USDT"}})
					} else {
						makeOnDepth(provider)(&banexg.OrderBook{Symbol: "BTC/USDT"})
					}
				}
				go invoke()
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("websocket callback did not start")
				}

				joined := make(chan struct{})
				go func() {
					if err := stop.call(provider); err != nil {
						t.Errorf("stop live provider: %v", err)
					}
					provider.Join()
					close(joined)
				}()
				select {
				case <-joined:
					t.Fatal("Join returned before websocket callback completed")
				case <-time.After(50 * time.Millisecond):
				}
				invoke()
				close(release)
				select {
				case <-joined:
				case <-time.After(time.Second):
					t.Fatal("Join did not wait for websocket callback")
				}
				if got := calls.Load(); got != 1 {
					t.Fatalf("websocket callbacks admitted after %s: calls=%d, want 1", stop.name, got)
				}
			})
		}
	}
}

func TestProviderHoldersConcurrentWithSeriesRotation(t *testing.T) {
	oldExgName, oldMarket := core.ExgName, core.Market
	core.ExgName, core.Market = "test", "spot"
	t.Cleanup(func() { core.ExgName, core.Market = oldExgName, oldMarket })

	const iterations = 1000
	newFeeder := func(pair string, _ []string) (IDataFeeder, *errs.Error) {
		return &stubDataFeeder{
			symbol: pair,
			states: []*PairTFCache{{TimeFrame: "1m", TFSecs: 60, SubNextMS: 1}},
		}, nil
	}
	provider := &LiveProvider{
		Provider: Provider[IDataFeeder]{
			holders: map[string]IDataFeeder{"BTC/USDT": &stubDataFeeder{
				symbol: "BTC/USDT",
				states: []*PairTFCache{{TimeFrame: "1m", TFSecs: 60, SubNextMS: 1}},
			}},
			newFeeder: newFeeder,
		},
	}
	var callbackCalls atomic.Int32
	holderObserved := make(chan struct{})
	var holderObservedOnce sync.Once
	provider.OnDataSeries = func(*SeriesMsg, []*orm.DataSeries) *errs.Error {
		callbackCalls.Add(1)
		holderObservedOnce.Do(func() { close(holderObserved) })
		return nil
	}
	msg := &SeriesMsg{
		ExgName: "test", Market: "spot", Pair: "BTC/USDT",
		NotifySeries: NotifySeries{
			TFSecs:   60,
			Interval: 60,
			Rows:     []*orm.DataSeries{{TimeMS: 1}},
		},
	}
	handle := makeOnSeriesMsg(provider)
	// Establish the initial holder-observed state before rotation starts. Without
	// this handshake, a valid schedule can remove the only holder before the
	// series goroutine reaches getHolder.
	handle(msg)
	select {
	case <-holderObserved:
	case <-time.After(time.Second):
		t.Fatal("series callback did not observe the initial holder")
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	errsCh := make(chan *errs.Error, 1)
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for range iterations {
			handle(msg)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		items := map[string]map[string]int{"BTC/USDT": {"1m": 1}}
		for i := 0; i < iterations; i++ {
			if i%2 == 0 {
				provider.Provider.UnSubPairs("BTC/USDT")
			} else {
				_, _, _, err := provider.Provider.SubWarmPairs(items, false, nil)
				if err != nil {
					select {
					case errsCh <- err:
					default:
					}
				}
			}
		}
	}()
	close(start)
	wg.Wait()
	if err := provider.Stop(); err != nil {
		t.Fatal(err)
	}
	provider.Join()
	select {
	case err := <-errsCh:
		t.Fatal(err)
	default:
	}
	if callbackCalls.Load() == 0 {
		t.Fatal("series callback never observed a holder")
	}
}

func TestWsCallbacksConcurrentWithSubscriptionRotation(t *testing.T) {
	oldJobs := strat.WsSubJobs
	t.Cleanup(func() {
		strat.LockJobsWrite()
		strat.WsSubJobs = oldJobs
		strat.UnlockJobsWrite()
	})
	var callbacks atomic.Int32
	job := &strat.StratJob{Strat: &strat.TradeStrat{
		OnWsTrades: func(*strat.StratJob, string, []*banexg.Trade) {
			callbacks.Add(1)
			runtime.Gosched()
		},
		OnWsDepth: func(*strat.StratJob, *banexg.OrderBook) {
			callbacks.Add(1)
			runtime.Gosched()
		},
	}}
	setJobs := func(pair string) {
		strat.LockJobsWrite()
		strat.WsSubJobs = map[string]map[string]map[*strat.StratJob]bool{
			core.WsSubTrade: {pair: {job: true}},
			core.WsSubDepth: {pair: {job: true}},
		}
		strat.UnlockJobsWrite()
		strat.RefreshWsSubJobsSnapshot()
	}
	setJobs("BTC/USDT")
	provider := &LiveProvider{}
	onTrade := makeOnTrade(provider)
	onDepth := makeOnDepth(provider)

	const iterations = 1000
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for range iterations {
			onTrade("test", "spot", "BTC/USDT", []*banexg.Trade{{Symbol: "BTC/USDT"}})
			onDepth(&banexg.OrderBook{Symbol: "BTC/USDT"})
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			if i%2 == 0 {
				setJobs("BTC/USDT")
			} else {
				setJobs("ETH/USDT")
			}
		}
	}()
	close(start)
	wg.Wait()
	if callbacks.Load() == 0 {
		t.Fatal("websocket callbacks never observed a subscription")
	}
}

func TestLiveProviderCallbacksUseInstanceWsRegistries(t *testing.T) {
	oldJobs := strat.WsSubJobs
	t.Cleanup(func() {
		strat.LockJobsWrite()
		strat.WsSubJobs = oldJobs
		strat.UnlockJobsWrite()
		strat.RefreshWsSubJobsSnapshot()
	})

	var callsA, callsB atomic.Int32
	jobA := &strat.StratJob{Strat: &strat.TradeStrat{
		OnWsTrades: func(*strat.StratJob, string, []*banexg.Trade) { callsA.Add(1) },
	}}
	jobB := &strat.StratJob{Strat: &strat.TradeStrat{
		OnWsTrades: func(*strat.StratJob, string, []*banexg.Trade) { callsB.Add(1) },
	}}
	setJobs := func(job *strat.StratJob) {
		strat.LockJobsWrite()
		strat.WsSubJobs = map[string]map[string]map[*strat.StratJob]bool{
			core.WsSubTrade: {"BTC/USDT": {job: true}},
		}
		strat.UnlockJobsWrite()
	}
	setJobs(jobA)
	registryA := strat.NewWsSubJobRegistry(nil)
	setJobs(jobB)
	registryB := strat.NewWsSubJobRegistry(nil)

	providerA := &LiveProvider{Provider: Provider[IDataFeeder]{wsSubs: registryA}}
	providerB := &LiveProvider{Provider: Provider[IDataFeeder]{wsSubs: registryB}}
	makeOnTrade(providerA)("test", "spot", "BTC/USDT", []*banexg.Trade{{Symbol: "BTC/USDT"}})
	makeOnTrade(providerB)("test", "spot", "BTC/USDT", []*banexg.Trade{{Symbol: "BTC/USDT"}})
	if err := (&TradeFeeder{wsSubs: registryA}).RunBatch(TradeBatch{{Symbol: "BTC/USDT"}}); err != nil {
		t.Fatal(err)
	}
	if err := (&TradeFeeder{wsSubs: registryB}).RunBatch(TradeBatch{{Symbol: "BTC/USDT"}}); err != nil {
		t.Fatal(err)
	}
	if got := callsA.Load(); got != 2 {
		t.Fatalf("runtime A callbacks = %d, want 2", got)
	}
	if got := callsB.Load(); got != 2 {
		t.Fatalf("runtime B callbacks = %d, want 2", got)
	}
	if err := providerA.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := providerB.Stop(); err != nil {
		t.Fatal(err)
	}
	providerA.Join()
	providerB.Join()
}

func TestExplicitProviderWsRegistryDoesNotUseLegacyJobs(t *testing.T) {
	oldJobs := strat.WsSubJobs
	t.Cleanup(func() {
		strat.LockJobsWrite()
		strat.WsSubJobs = oldJobs
		strat.UnlockJobsWrite()
		strat.RefreshWsSubJobsSnapshot()
	})
	job := &strat.StratJob{}
	strat.LockJobsWrite()
	strat.WsSubJobs = map[string]map[string]map[*strat.StratJob]bool{
		core.WsSubTrade: {"BTC/USDT": {job: true}},
	}
	strat.UnlockJobsWrite()
	strat.RefreshWsSubJobsSnapshot()

	provider := &Provider[IDataFeeder]{deps: &RuntimeDeps{}}
	if got := provider.wsRegistry().Pairs(core.WsSubTrade); len(got) != 0 {
		t.Fatalf("explicit provider inherited legacy websocket pairs: %v", got)
	}
}

func TestSubWarmPairsUsesStablePairOrder(t *testing.T) {
	var created, warmed []string
	p := &Provider[IDataFeeder]{
		holders: make(map[string]IDataFeeder),
		newFeeder: func(pair string, _ []string) (IDataFeeder, *errs.Error) {
			created = append(created, pair)
			return &stubDataFeeder{symbol: pair, warmLog: &warmed}, nil
		},
	}
	items := map[string]map[string]int{
		"SOL/USDT": {"1h": 10},
		"BTC/USDT": {"1h": 30},
		"ETH/USDT": {"1h": 20},
	}
	_, _, _, err := p.SubWarmPairs(items, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(created, []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}) ||
		!reflect.DeepEqual(warmed, []string{"BTC/USDT:30", "ETH/USDT:20", "SOL/USDT:10"}) {
		t.Fatalf("unstable warmup order: created=%v warmed=%v", created, warmed)
	}
}

func TestSortedTimeframesUsesDurationThenName(t *testing.T) {
	got := sortedTimeframes(map[string]int{"4h": 1, "15m": 1, "1h": 1, "60m": 1})
	want := []string{"15m", "1h", "60m", "4h"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("timeframes = %v, want %v", got, want)
	}
}

func TestComparePairTFCacheUsesTimeframeTieBreak(t *testing.T) {
	states := []*PairTFCache{
		{TimeFrame: "4h", TFSecs: 14400},
		{TimeFrame: "60m", TFSecs: 3600},
		{TimeFrame: "1h", TFSecs: 3600},
		{TimeFrame: "15m", TFSecs: 900},
	}
	slices.SortFunc(states, comparePairTFCache)
	got := make([]string, len(states))
	for i, state := range states {
		got[i] = state.TimeFrame
	}
	if want := []string{"15m", "1h", "60m", "4h"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("timeframe states = %v, want %v", got, want)
	}
}

func TestSubTfsReusesStableMinimumAlias(t *testing.T) {
	oldExchange := config.Exchange
	config.Exchange = &config.ExchangeConfig{Name: "binance", Items: map[string]map[string]interface{}{}}
	t.Cleanup(func() { config.Exchange = oldExchange })
	for range 100 {
		feeder := &Feeder{
			ExSymbol: &orm.ExSymbol{Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
			States: []*PairTFCache{
				{TimeFrame: "900s", TFSecs: 900},
				{TimeFrame: "15m", TFSecs: 900},
			},
		}
		feeder.SubTfs([]string{"30m", "45m"}, true)
		if len(feeder.States) != 3 || feeder.States[0].TimeFrame != "15m" {
			t.Fatalf("states start with %v", feeder.States)
		}
	}
}

func TestDBSeriesFeederPhysicalConsumerAuthorization(t *testing.T) {
	oldExchange := config.Exchange
	oldTimeRange := config.TimeRange
	config.Exchange = &config.ExchangeConfig{Name: "binance", Items: map[string]map[string]interface{}{}}
	config.TimeRange = &config.TimeTuple{EndMS: 24 * 60 * 60 * 1000}
	t.Cleanup(func() { config.Exchange, config.TimeRange = oldExchange, oldTimeRange })
	symbol := &orm.ExSymbol{Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"}
	feeder := &DBSeriesFeeder{
		SeriesFeeder:   SeriesFeeder{Feeder: Feeder{ExSymbol: symbol}},
		TfSeriesLoader: &TfSeriesLoader{EndMS: 1_700_000_000_000},
	}

	feeder.SubTfs([]string{"4h"}, false)
	if feeder.Timeframe != "1h" || feeder.hour != nil || !feeder.allowPhysicalRead ||
		feeder.physicalConsumerTimeframe != "4h" || len(feeder.States) != 2 || !feeder.States[0].physicalOnly {
		t.Fatalf("4h physical loader state=%#v timeframe=%q hour=%v physical=%v consumer=%q",
			feeder.States, feeder.Timeframe, feeder.hour, feeder.allowPhysicalRead, feeder.physicalConsumerTimeframe)
	}

	feeder.SubTfs([]string{"1h"}, true)
	if feeder.allowPhysicalRead || feeder.physicalConsumerTimeframe != "" || feeder.States[0].physicalOnly {
		t.Fatalf("explicit 1h retained physical authorization: physical=%v consumer=%q state=%#v",
			feeder.allowPhysicalRead, feeder.physicalConsumerTimeframe, feeder.States[0])
	}
}

func TestHistProviderMakeFeedersUsesStableCategoryAndKeyOrder(t *testing.T) {
	symbol := &orm.ExSymbol{ID: 1, Symbol: "SAME/USDT"}
	holderA := &DBSeriesFeeder{SeriesFeeder: SeriesFeeder{Feeder: Feeder{ExSymbol: symbol}}}
	holderZ := &DBSeriesFeeder{SeriesFeeder: SeriesFeeder{Feeder: Feeder{ExSymbol: symbol}}}
	tradeA, tradeZ := &TradeFeeder{ExSymbol: symbol}, &TradeFeeder{ExSymbol: symbol}
	seriesA := &HistSeriesFeeder{info: &orm.SeriesInfo{Name: "macro", TimeFrame: "1m"}, target: symbol}
	seriesZ := &HistSeriesFeeder{info: &orm.SeriesInfo{Name: "macro", TimeFrame: "1m"}, target: symbol}
	provider := &HistProvider{
		Provider: Provider[IHistDataFeeder]{holders: map[string]IHistDataFeeder{"z": holderZ, "a": holderA}},
		trades:   map[string]*TradeFeeder{"z": tradeZ, "a": tradeA},
		series:   map[string]*HistSeriesFeeder{"z": seriesZ, "a": seriesA},
	}
	got := provider.makeFeeders()
	want := []IHistFeeder{holderA, holderZ, tradeA, tradeZ, seriesA, seriesZ}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unstable feeder order: got %v, want %v", got, want)
	}
}

type histFeederBatch struct {
	symbol string
	timeMS int64
}

func (b histFeederBatch) TimeMS() int64 {
	return b.timeMS
}

type stubHistFeeder struct {
	name   string
	symbol string
	times  []int64
	index  int
	runs   *[]histFeederBatch
	onRun  func()
}

func (f *stubHistFeeder) getNextMS() int64 {
	if f.index >= len(f.times) {
		return math.MaxInt64
	}
	return f.times[f.index]
}

func (f *stubHistFeeder) SetSeek(int64) {}

func (f *stubHistFeeder) SetEndMS(int64) {}

func (f *stubHistFeeder) GetBatch() Batch {
	if f.index >= len(f.times) {
		return nil
	}
	name := f.name
	if name == "" {
		name = f.symbol
	}
	return histFeederBatch{symbol: name, timeMS: f.times[f.index]}
}

func (f *stubHistFeeder) RunBatch(batch Batch) *errs.Error {
	*f.runs = append(*f.runs, batch.(histFeederBatch))
	if f.onRun != nil {
		f.onRun()
	}
	return nil
}

func (f *stubHistFeeder) CallNext() {
	f.index++
}

func (f *stubHistFeeder) Type() string {
	return "stub"
}

func (f *stubHistFeeder) getSymbol() string {
	return f.symbol
}

func TestRunHistFeedersRotatesExactTies(t *testing.T) {
	var runs []histFeederBatch
	feeders := []IHistFeeder{
		&stubHistFeeder{name: "first", symbol: "same", times: []int64{100, 100}, runs: &runs},
		&stubHistFeeder{name: "second", symbol: "same", times: []int64{100, 100}, runs: &runs},
	}

	err := RunHistFeeders(func() []IHistFeeder { return feeders }, make(chan int, 1), nil)
	if err != nil {
		t.Fatalf("RunHistFeeders returned error: %v", err)
	}

	want := []histFeederBatch{
		{symbol: "first", timeMS: 100},
		{symbol: "second", timeMS: 100},
		{symbol: "first", timeMS: 100},
		{symbol: "second", timeMS: 100},
	}
	if !reflect.DeepEqual(runs, want) {
		t.Fatalf("unexpected exact-tie order: got %v, want %v", runs, want)
	}
}

func TestRunHistFeedersOrdersByTimeAndSymbol(t *testing.T) {
	var runs []histFeederBatch
	feeders := []IHistFeeder{
		&stubHistFeeder{symbol: "z", times: []int64{100, 300}, runs: &runs},
		&stubHistFeeder{symbol: "a", times: []int64{100, 200}, runs: &runs},
	}

	err := RunHistFeeders(func() []IHistFeeder { return feeders }, make(chan int, 1), nil)
	if err != nil {
		t.Fatalf("RunHistFeeders returned error: %v", err)
	}

	want := []histFeederBatch{
		{symbol: "a", timeMS: 100},
		{symbol: "z", timeMS: 100},
		{symbol: "a", timeMS: 200},
		{symbol: "z", timeMS: 300},
	}
	if !reflect.DeepEqual(runs, want) {
		t.Fatalf("unexpected replay order: got %v, want %v", runs, want)
	}
}

func TestRunHistFeedersRebuildsHeapOnNewVersion(t *testing.T) {
	versions := make(chan int, 1)
	var runs []histFeederBatch
	base := &stubHistFeeder{symbol: "base", times: []int64{100, 300}, runs: &runs}
	added := &stubHistFeeder{symbol: "added", times: []int64{200}, runs: &runs}
	base.onRun = func() {
		base.onRun = nil
		versions <- 1
	}
	makeCalls := 0

	err := RunHistFeeders(func() []IHistFeeder {
		makeCalls++
		if makeCalls == 1 {
			return []IHistFeeder{base}
		}
		return []IHistFeeder{base, added}
	}, versions, nil)
	if err != nil {
		t.Fatalf("RunHistFeeders returned error: %v", err)
	}

	want := []histFeederBatch{
		{symbol: "base", timeMS: 100},
		{symbol: "added", timeMS: 200},
		{symbol: "base", timeMS: 300},
	}
	if makeCalls != 2 || !reflect.DeepEqual(runs, want) {
		t.Fatalf("unexpected version reload: calls=%d, got %v, want %v", makeCalls, runs, want)
	}
}

func TestRunHistFeedersHandlesEmptyFeeders(t *testing.T) {
	err := RunHistFeeders(func() []IHistFeeder { return nil }, make(chan int, 1), nil)
	if err != nil {
		t.Fatalf("RunHistFeeders returned error: %v", err)
	}
}

func TestRunHistFeedersWithRuntimeDepsRejectsNilDeps(t *testing.T) {
	err := RunHistFeedersWithRuntimeDeps(nil, func() []IHistFeeder { return nil }, make(chan int, 1), nil)
	if err == nil || !strings.Contains(err.Error(), "runtime dependencies") {
		t.Fatalf("nil runtime dependencies error = %v, want explicit dependency error", err)
	}
}
