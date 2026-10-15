package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/execution"
)

type sharedTestAdapter struct {
	snapshot                   execution.VenueSnapshot
	closed                     atomic.Int32
	started, canceled, release chan struct{}
	closeFailure               error
}

func (a *sharedTestAdapter) Capabilities() execution.AdapterCapabilities {
	return execution.AdapterCapabilities{QueryClientID: true, StableTradeID: true}
}
func (a *sharedTestAdapter) Submit(context.Context, execution.OrderIntent, string) (execution.SubmitReceipt, error) {
	return execution.SubmitReceipt{}, errors.New("unused")
}
func (a *sharedTestAdapter) Cancel(context.Context, string) (bool, error) {
	return false, errors.New("unused")
}
func (a *sharedTestAdapter) Query(context.Context, string, string) (execution.QueryResult, error) {
	return execution.QueryResult{}, errors.New("unused")
}
func (a *sharedTestAdapter) Snapshot(ctx context.Context) (execution.VenueSnapshot, error) {
	if a.started != nil {
		close(a.started)
		<-ctx.Done()
		close(a.canceled)
		<-a.release
		return execution.VenueSnapshot{}, ctx.Err()
	}
	return a.snapshot, nil
}
func (a *sharedTestAdapter) Close() error { a.closed.Add(1); return a.closeFailure }

func TestProcessRetainsSharedCloseError(t *testing.T) {
	failure := errors.New("adapter close failure")
	a := &sharedTestAdapter{closeFailure: failure}
	key, opts := sharedTestOptions(t, a)
	p := NewProcess()
	if _, err := p.NewRuntime(Options{AccountOwnerKey: &key, SharedExecution: opts}); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if !errors.Is(p.CloseError(), failure) {
		t.Fatalf("process discarded shared close failure: %v", p.CloseError())
	}
	p.Close()
	if !errors.Is(p.CloseError(), failure) || a.closed.Load() != 1 {
		t.Fatal("repeated close lost error or closed twice")
	}
}
func sharedTestOptions(t *testing.T, a *sharedTestAdapter) (execution.AccountKey, *biz.SharedExecutionOptions) {
	t.Helper()
	dir := t.TempDir()
	return execution.AccountKey{VenueSessionIdentity: dir, Account: "paper", SettlementDomain: "USDT"}, &biz.SharedExecutionOptions{StorePath: filepath.Join(dir, "ledger.db"), SenderLeaseDir: filepath.Join(dir, "leases"), Adapter: a, AuthoritativeSnapshot: true}
}

func TestRuntimeSharedAccountRetainsOneServiceAndRejectsRebinding(t *testing.T) {
	p := NewProcess()
	defer p.Close()
	a := &sharedTestAdapter{snapshot: execution.VenueSnapshot{Cash: "0"}}
	key, opts := sharedTestOptions(t, a)
	one, err := p.NewRuntime(Options{AccountOwnerKey: &key, SharedExecution: opts})
	if err != nil {
		t.Fatal(err)
	}
	two, err := p.NewRuntime(Options{AccountOwnerKey: &key, SharedExecution: opts})
	if err != nil {
		t.Fatal(err)
	}
	if one.SharedExecution().Service() != two.SharedExecution().Service() {
		t.Fatal("account opened duplicate store/executor service")
	}
	changed := *opts
	changed.StorePath = filepath.Join(t.TempDir(), "other.db")
	if _, err := p.NewRuntime(Options{AccountOwnerKey: &key, SharedExecution: &changed}); err == nil {
		t.Fatal("same key rebound another database")
	}
	one.Close()
	one.Join()
	if _, err := one.SharedExecution().Snapshot(context.Background()); !errors.Is(err, execution.ErrReleased) {
		t.Fatalf("released borrow: %v", err)
	}
	if err := two.SharedExecution().Reconcile("startup", 1); err != nil {
		t.Fatal(err)
	}
	two.Close()
	two.Join()
	if a.closed.Load() != 0 {
		t.Fatal("borrower closed shared adapter")
	}
	three, err := p.NewRuntime(Options{AccountOwnerKey: &key, SharedExecution: opts})
	if err != nil {
		t.Fatal(err)
	}
	if three.SharedExecution().Service() != two.SharedExecution().Service() {
		t.Fatal("last release recreated owner service")
	}
	p.Close()
	if a.closed.Load() != 1 {
		t.Fatal("process did not close adapter exactly once")
	}
}

func TestRuntimeSharedAccountUnknownOpenOrderPersistsFreeze(t *testing.T) {
	p := NewProcess()
	defer p.Close()
	a := &sharedTestAdapter{snapshot: execution.VenueSnapshot{Cash: "0", OpenOrders: []execution.QueryResult{{Found: true, Authoritative: true, Complete: true, Receipt: execution.SubmitReceipt{ExchangeID: "manual"}}}}}
	key, opts := sharedTestOptions(t, a)
	rt, err := p.NewRuntime(Options{AccountOwnerKey: &key, SharedExecution: opts})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.SharedExecution().Reconcile("startup", 1); err == nil {
		t.Fatal("manual open order admitted")
	}
	snapshot, err := rt.SharedExecution().Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.RiskFrozen {
		t.Fatal("unknown open order did not persist risk freeze")
	}
	if err := rt.SharedExecution().Submit(execution.Plan{}, execution.OrderIntent{}, 1); err == nil {
		t.Fatal("unready service submitted")
	}
}

func TestProcessSharedAccountClosesAdapterOnlyAfterNetworkJoin(t *testing.T) {
	p := NewProcess()
	defer p.Close()
	a := &sharedTestAdapter{started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	key, opts := sharedTestOptions(t, a)
	rt, err := p.NewRuntime(Options{AccountOwnerKey: &key, SharedExecution: opts})
	if err != nil {
		t.Fatal(err)
	}
	returned := make(chan error, 1)
	go func() { returned <- rt.SharedExecution().Reconcile("startup", 1) }()
	<-a.started
	done := make(chan struct{})
	go func() { p.Close(); close(done) }()
	select {
	case <-a.canceled:
	case <-time.After(time.Second):
		t.Fatal("network context not canceled")
	}
	if a.closed.Load() != 0 {
		t.Fatal("adapter closed before report joined")
	}
	select {
	case <-done:
		t.Fatal("process returned before slow network result")
	default:
	}
	close(a.release)
	if err := <-returned; !errors.Is(err, context.Canceled) {
		t.Fatalf("network result: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("process did not join")
	}
	if a.closed.Load() != 1 {
		t.Fatal("adapter not closed after join")
	}
}
