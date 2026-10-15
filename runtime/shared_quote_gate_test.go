package runtime

import (
	"context"
	"errors"
	"github.com/banbox/banbot/execution"
	"github.com/shopspring/decimal"
	"testing"
	"time"
)

func TestAccountVisibleQuoteCancellationIsJoinedBeforeAdapterClose(t *testing.T) {
	a := &sharedTestAdapter{}
	key, opts := sharedTestOptions(t, a)
	p := NewProcess()
	defer p.Close()
	rt, err := p.NewRuntime(Options{AccountOwnerKey: &key, SharedExecution: opts})
	if err != nil {
		t.Fatal(err)
	}
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	i := execution.Instrument{ID: "BTC", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USDT", QuantityStep: decimal.NewFromInt(1), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1)}
	if err := rt.SharedExecution().RegisterAccountQuotes([]execution.Instrument{i}, func(ctx context.Context, _ string, _ int64) (execution.VisibleQuote, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return execution.VisibleQuote{}, ctx.Err()
	}, nil); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, err := rt.SharedExecution().VisibleQuote(context.Background(), "BTC", 1); result <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("quote not admitted")
	}
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("owner stop did not cancel quote")
	}
	if a.closed.Load() != 0 {
		t.Fatal("adapter closed before quote joined")
	}
	select {
	case <-closed:
		t.Fatal("process closed before quote joined")
	default:
	}
	close(release)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation evidence lost", err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("process did not join quote")
	}
	if a.closed.Load() != 1 {
		t.Fatal("adapter close count", a.closed.Load())
	}
}

func TestBorrowerQuoteCancellationStopsIOAndPreservesSibling(t *testing.T) {
	for _, mode := range []string{"caller", "deadline", "runtime-stop"} {
		t.Run(mode, func(t *testing.T) {
			a := &sharedTestAdapter{}
			key, opts := sharedTestOptions(t, a)
			p := NewProcess()
			defer p.Close()
			rt, err := p.NewRuntime(Options{AccountOwnerKey: &key, SharedExecution: opts})
			if err != nil {
				t.Fatal(err)
			}
			sibling, err := p.NewRuntime(Options{AccountOwnerKey: &key, SharedExecution: opts})
			if err != nil {
				t.Fatal(err)
			}
			started := make(chan struct{})
			i := execution.Instrument{ID: "BTC", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USDT", QuantityStep: decimal.NewFromInt(1), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1)}
			if err := rt.SharedExecution().RegisterAccountQuotes([]execution.Instrument{i}, func(ctx context.Context, _ string, now int64) (execution.VisibleQuote, error) {
				if now == 2 {
					return execution.VisibleQuote{AtMS: 2}, nil
				}
				close(started)
				<-ctx.Done()
				return execution.VisibleQuote{}, ctx.Err()
			}, nil); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "deadline" {
				ctx, cancel = context.WithTimeout(context.Background(), 30*time.Millisecond)
				defer cancel()
			}
			result := make(chan error, 1)
			go func() { _, err := rt.SharedExecution().VisibleQuote(ctx, "BTC", 1); result <- err }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("quote not admitted")
			}
			if mode == "caller" {
				cancel()
			}
			if mode == "runtime-stop" {
				rt.Stop()
			}
			// Models the provider Join barrier: it cannot complete until its callback exits.
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("quote context did not cancel", err)
				}
			case <-time.After(time.Second):
				t.Fatal("provider callback join hung")
			}
			if q, err := sibling.SharedExecution().VisibleQuote(context.Background(), "BTC", 2); err != nil || q.AtMS != 2 {
				t.Fatal("sibling borrower canceled", q, err)
			}
			if a.closed.Load() != 0 {
				t.Fatal("borrower cancellation closed shared adapter")
			}
		})
	}
}
