package execution

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

type hubLimitProvider struct {
	*PaperAdapter
	calls atomic.Int32
}

func (p *hubLimitProvider) DefaultLimitPrice(ctx context.Context, id string, side OrderSide, seconds int) (float64, error) {
	p.calls.Add(1)
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if id != "BTC" || side != Sell || seconds != 7 {
		return 0, errors.New("unexpected owner liquidity inputs")
	}
	return 123, nil
}

func TestHubDefaultLimitPriceUsesAdapterLiquidityProvider(t *testing.T) {
	request := hubTestRegistration()
	registry := &AccountRegistry{}
	owner, err := registry.Acquire(request.Account)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	paper, err := NewPaperAdapter(decimal.NewFromInt(1000), decimal.Zero, decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	provider := &hubLimitProvider{PaperAdapter: paper}
	dir := t.TempDir()
	service, err := NewSharedAccount(owner, SharedExecutionOptions{StorePath: filepath.Join(dir, "ledger.db"), SenderLeaseDir: filepath.Join(dir, "leases"), Adapter: provider, AuthoritativeSnapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	hub, err := NewHub(func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		return HubAccountBinding{Account: service, Shutdown: func() error { return nil }}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	if err := hub.Register(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	price, err := hub.DefaultLimitPrice(context.Background(), request.Account, "BTC", Sell, 7)
	if err != nil || price != 123 || provider.calls.Load() != 1 {
		t.Fatalf("owner algorithm not used: %v, %v, %d", price, err, provider.calls.Load())
	}
}

func TestHubDefaultLimitPriceRPCUsesOwnerQuoteAndRejectsStale(t *testing.T) {
	var closed atomic.Int32
	var stale atomic.Bool
	hub, err := NewHub(func(ctx context.Context, registration HubAccountRegistration) (HubAccountBinding, error) {
		binding := hubTestBinding(t, registration.Account, &closed)
		borrow := binding.Account.Borrow()
		defer borrow.Release()
		err := borrow.RegisterAccountQuotes([]Instrument{ledgerInstrument()}, func(ctx context.Context, _ string, now int64) (VisibleQuote, error) {
			if err := ctx.Err(); err != nil {
				return VisibleQuote{}, err
			}
			validUntil := now + 60000
			if stale.Load() {
				validUntil = now - 1
			}
			return VisibleQuote{Bid: intentPrice("99"), Ask: intentPrice("101"), AtMS: now, ReceivedMS: now, ValidUntilMS: validUntil}, nil
		}, func() int64 { return time.Now().UnixMilli() })
		return binding, err
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	server, err := ListenHubUnix(hubTestSocket(t), hub)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := NewHubClient(server.path)
	defer client.Close()
	registration := hubTestRegistration()
	if err := client.Register(context.Background(), registration); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		side OrderSide
		want float64
	}{{Buy, 101}, {Sell, 99}} {
		price, err := client.DefaultLimitPrice(context.Background(), registration.Account, ledgerInstrument().ID, tc.side, 3)
		if err != nil || price != tc.want {
			t.Fatalf("%s: price=%v err=%v", tc.side, price, err)
		}
	}
	for _, tc := range []struct {
		id      string
		side    OrderSide
		seconds int
	}{{"", Buy, 3}, {"missing", Buy, 3}, {ledgerInstrument().ID, "other", 3}, {ledgerInstrument().ID, Buy, -1}} {
		if price, err := client.DefaultLimitPrice(context.Background(), registration.Account, tc.id, tc.side, tc.seconds); err == nil || price != 0 {
			t.Fatalf("invalid request returned %v, %v", price, err)
		}
	}
	stale.Store(true)
	if price, err := client.DefaultLimitPrice(context.Background(), registration.Account, ledgerInstrument().ID, Buy, 3); err == nil || price != 0 {
		t.Fatalf("stale quote returned %v, %v", price, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.DefaultLimitPrice(ctx, registration.Account, ledgerInstrument().ID, Buy, 3); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
