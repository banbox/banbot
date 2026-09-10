package data

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type wsArchiveExchangeStub struct {
	banexg.BanExchange
	url string
}

func (s *wsArchiveExchangeStub) BuildArchiveURL(string, string, string, string) (string, *errs.Error) {
	return s.url, nil
}

func testWsLoaderSymbol() *WsSymbol {
	return &WsSymbol{
		ExgId:     "binance",
		Market:    "spot",
		WsType:    core.WsSubTrade,
		Symbol:    "BTC/USDT",
		RawSymbol: "BTCUSDT",
		Date:      "2025-01-01",
		dataType:  "aggTrades",
	}
}

func TestWsDataLoaderStopCancelsPendingLoad(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	loader := &WsDataLoader{
		ctx:        ctx,
		cancel:     cancel,
		cacheDir:   t.TempDir(),
		tasks:      make(map[*WsSymbol]chan *errs.Error),
		chanDown:   make(chan *WsSymbol),
		chanSplit:  make(chan *WsSymbol),
		httpClient: http.DefaultClient,
	}
	t.Cleanup(loader.Join)

	info := testWsLoaderSymbol()
	loadDone := make(chan *errs.Error, 1)
	go func() {
		_, err := loader.LoadTrades(info)
		loadDone <- err
	}()

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		loader.lockTasks.Lock()
		_, pending := loader.tasks[info]
		loader.lockTasks.Unlock()
		if pending {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("load task was not admitted")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	if err := loader.Stop(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-loadDone:
		if err == nil || err.Code != errs.CodeCancel {
			t.Fatalf("LoadTrades error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not unblock pending LoadTrades")
	}

	loader.Stop()
	loader.Join()
}

func TestWsDataLoaderRuntimeContextIgnoresLegacyContext(t *testing.T) {
	oldContext := core.Ctx
	legacyContext, cancelLegacy := context.WithCancel(context.Background())
	cancelLegacy()
	core.Ctx = legacyContext
	t.Cleanup(func() { core.Ctx = oldContext })

	runtimeState, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtimeState.Close)
	loader, loaderErr := NewWsDataLoaderWithRuntimeDeps(&RuntimeDeps{Core: runtimeState})
	if loaderErr != nil {
		t.Fatal(loaderErr)
	}
	select {
	case <-loader.ctx.Done():
		t.Fatal("runtime websocket loader inherited canceled legacy context")
	default:
	}
	loader.Stop()
	loader.Join()
}

func TestWsSymbolArchiveURLUsesAdapterCapabilityAndSupportsOverrides(t *testing.T) {
	info := testWsLoaderSymbol()
	want := "https://archive.example.test/trades.zip"
	deps := &RuntimeDeps{Exchange: &wsArchiveExchangeStub{url: want}}
	got, err := info.archiveURLForDeps(deps)
	if err != nil || got != want {
		t.Fatalf("adapter archive URL = %q, %v; want %q", got, err, want)
	}

	if got := info.DownUrl(); got != "" {
		t.Fatalf("legacy archive URL without capability = %q, want empty", got)
	}
	if _, err := info.archiveURL(); err == nil || err.Code != errs.CodeNotSupport {
		t.Fatalf("unsupported archive error = %v, want CodeNotSupport", err)
	}

	info.ArchiveURL = "https://archive.example.test/trades.zip"
	got, err = info.archiveURL()
	if err != nil || got != info.ArchiveURL {
		t.Fatalf("overridden archive URL = %q, %v", got, err)
	}
}

func TestWsDataLoaderUnsupportedArchiveReturnsStructuredError(t *testing.T) {
	loader := &WsDataLoader{cacheDir: t.TempDir(), httpClient: http.DefaultClient}
	info := testWsLoaderSymbol()
	info.ExgId = "okx"
	if _, err := loader.downloadJob(info); err == nil || err.Code != errs.CodeNotSupport {
		t.Fatalf("download error = %v, want CodeNotSupport", err)
	}
}

type wsLoaderBlockingTransport struct {
	started chan struct{}
	once    sync.Once
}

func (t *wsLoaderBlockingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.once.Do(func() { close(t.started) })
	<-req.Context().Done()
	return nil, req.Context().Err()
}

func TestWsDataLoaderJoinWaitsForCanceledWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	transport := &wsLoaderBlockingTransport{started: make(chan struct{})}
	loader := &WsDataLoader{
		ctx:        ctx,
		cancel:     cancel,
		cacheDir:   t.TempDir(),
		tasks:      make(map[*WsSymbol]chan *errs.Error),
		chanDown:   make(chan *WsSymbol, 1),
		chanSplit:  make(chan *WsSymbol, 1),
		httpClient: &http.Client{Transport: transport},
	}
	loader.workerWait.Add(1)
	go func() {
		defer loader.workerWait.Done()
		loader.downloadWorker()
	}()
	t.Cleanup(loader.Join)

	loadDone := make(chan *errs.Error, 1)
	go func() {
		info := testWsLoaderSymbol()
		info.ArchiveURL = "https://archive.example.test/trades.zip"
		_, err := loader.LoadTrades(info)
		loadDone <- err
	}()
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("download worker did not start the request")
	}

	joined := make(chan struct{})
	go func() {
		loader.Join()
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("Join did not wait for the canceled worker")
	}
	select {
	case err := <-loadDone:
		if err == nil || err.Code != errs.CodeCancel {
			t.Fatalf("LoadTrades error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Join left LoadTrades blocked")
	}

	loader.Stop()
	loader.Join()
}
