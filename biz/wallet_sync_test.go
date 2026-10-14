package biz

import (
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type walletSyncExchangeStub struct {
	banexg.BanExchange
	mu    sync.Mutex
	calls int
	free  float64
	fail  bool
}

func (e *walletSyncExchangeStub) FetchBalance(map[string]interface{}) (*banexg.Balances, *errs.Error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	if e.fail {
		return nil, errs.NewMsg(core.ErrRunTime, "balance test")
	}
	return &banexg.Balances{Assets: map[string]*banexg.Asset{
		"USDT": {Code: "USDT", Free: e.free, Total: e.free},
	}}, nil
}

func newSyncTestWallet(exchange banexg.BanExchange) *BanWallets {
	return &BanWallets{
		Items:        make(map[string]*ItemWallet),
		Account:      "acc",
		runtimeExg:   exchange,
		runtimeCore:  &core.State{EnvReal: true},
		runtimeBound: true,
	}
}

func TestWalletRefreshIfStaleFetchesOnlyWhenStale(t *testing.T) {
	exchange := &walletSyncExchangeStub{free: 100}
	wallet := newSyncTestWallet(exchange)

	if err := wallet.RefreshIfStale(WalletStaleMS); err != nil {
		t.Fatal(err)
	}
	if exchange.calls != 1 || wallet.Items["USDT"].Available != 100 || wallet.LastSyncMS() == 0 {
		t.Fatalf("never-synced wallet must fetch: calls=%d", exchange.calls)
	}

	exchange.free = 200
	if err := wallet.RefreshIfStale(WalletStaleMS); err != nil {
		t.Fatal(err)
	}
	if exchange.calls != 1 || wallet.Items["USDT"].Available != 100 {
		t.Fatalf("fresh wallet must not fetch: calls=%d", exchange.calls)
	}

	wallet.syncMS.Store(time.Now().UnixMilli() - WalletStaleMS - 1)
	if err := wallet.RefreshIfStale(WalletStaleMS); err != nil {
		t.Fatal(err)
	}
	if exchange.calls != 2 || wallet.Items["USDT"].Available != 200 {
		t.Fatalf("stale wallet must fetch: calls=%d", exchange.calls)
	}
}

func TestWalletRefreshIfStaleKeepsStaleMarkOnFailure(t *testing.T) {
	exchange := &walletSyncExchangeStub{fail: true}
	wallet := newSyncTestWallet(exchange)
	if err := wallet.RefreshIfStale(WalletStaleMS); err == nil {
		t.Fatal("expected fetch error")
	}
	if wallet.LastSyncMS() != 0 {
		t.Fatal("failed fetch must not mark wallet as synced")
	}
	exchange.fail = false
	exchange.free = 50
	if err := wallet.RefreshIfStale(WalletStaleMS); err != nil {
		t.Fatal(err)
	}
	if exchange.calls != 2 || wallet.Items["USDT"].Available != 50 {
		t.Fatalf("retry after failure must fetch: calls=%d", exchange.calls)
	}
}

func TestWalletRefreshIfStaleSkipsNonRealEnv(t *testing.T) {
	exchange := &walletSyncExchangeStub{free: 100}
	wallet := newSyncTestWallet(exchange)
	wallet.runtimeCore = &core.State{}
	if err := wallet.RefreshIfStale(WalletStaleMS); err != nil {
		t.Fatal(err)
	}
	if exchange.calls != 0 {
		t.Fatalf("non-real env must not fetch: calls=%d", exchange.calls)
	}
}

func TestWalletRefreshIfStaleSharesConcurrentFetch(t *testing.T) {
	exchange := &walletSyncExchangeStub{free: 100}
	wallet := newSyncTestWallet(exchange)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = wallet.RefreshIfStale(WalletStaleMS)
		}()
	}
	wg.Wait()
	if exchange.calls != 1 {
		t.Fatalf("concurrent stale refresh must fetch once: calls=%d", exchange.calls)
	}
}
