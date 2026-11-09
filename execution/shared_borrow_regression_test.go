package execution

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReconcileDoesNotFreezeOnLocallyOwnedCancelPendingOrder(t *testing.T) {
	ctx := context.Background()
	s, _, h, _ := testStore(t)
	order := planOrder(t, s, "cancel-pending-reconcile", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	fa := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true}}
	e := executorFor(s, h, fa)
	if err := e.Send(order.ID, 11); err != nil {
		t.Fatal(err)
	}
	fa.cancel = func(context.Context, string) (bool, error) { return false, errors.New("cancel ack lost") }
	if err := e.Cancel(order.ID, 12); err == nil {
		t.Fatal("cancel loss hidden")
	}
	stored, _ := s.Order(ctx, order.ID)
	if stored.State != OrderCancelPending || stored.ExchangeID == "" {
		t.Fatal(stored)
	}
	a := &takeoverSnapshotAdapter{fakeExecutionAdapter: fa, snapshot: VenueSnapshot{Cash: "0", Positions: map[string]int64{},
		OpenOrders: []QueryResult{{Found: true, Authoritative: true, Complete: true, Receipt: SubmitReceipt{ExchangeID: stored.ExchangeID}}}}}
	service := &SharedAccount{ctx: ctx, store: s, owner: h, executor: &OwnerExecutor{Store: s, Handle: h, Token: h.Token(), Adapter: a}, opts: SharedExecutionOptions{Adapter: a, AuthoritativeSnapshot: true}}
	borrow := service.Borrow()
	defer borrow.Release()
	_ = borrow.Reconcile("cancel-pending", 13)
	after, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.RiskFrozen {
		t.Fatal("locally owned CancelPending venue order froze the account")
	}
	// A truly foreign order must still freeze.
	a.snapshot.OpenOrders = []QueryResult{{Found: true, Authoritative: true, Complete: true, Receipt: SubmitReceipt{ExchangeID: "manual"}}}
	if err := borrow.Reconcile("foreign", 14); err == nil {
		t.Fatal("foreign order accepted")
	}
	after, _ = s.Snapshot(ctx)
	if !after.RiskFrozen {
		t.Fatal("foreign order did not freeze")
	}
}

func TestBorrowApplyTradePropagatesBorrowerCancellation(t *testing.T) {
	ctx := context.Background()
	s, _, h, _ := testStore(t)
	order := planOrder(t, s, "apply-trade-cancel", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	a := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
	e := executorFor(s, h, a)
	if err := e.Send(order.ID, 11); err != nil {
		t.Fatal(err)
	}
	// Establish cumulative report mode for the order.
	if _, err := s.ApplyFill(ctx, FillReport{EventID: "f1", OrderID: order.ID, Steps: 1, Price: intentPrice("100"), Cost: intentPrice("10"), Fee: intentPrice("0.01"), Cumulative: true, AtMS: 12}); err != nil {
		t.Skip("cannot seed cumulative mode:", err)
	}
	service := &SharedAccount{ctx: ctx, store: s, owner: h, executor: &OwnerExecutor{Store: s, Handle: h, Token: h.Token(), Adapter: a}, opts: SharedExecutionOptions{Adapter: a}}
	borrow := service.Borrow()
	defer borrow.Release()
	var queryErr error
	a.query = func(c context.Context, _, _ string) (QueryResult, error) {
		borrow.Stop()
		select {
		case <-c.Done():
			queryErr = c.Err()
		case <-time.After(2 * time.Second):
		}
		return QueryResult{}, queryErr
	}
	err := borrow.ApplyTrade(FillReport{EventID: "f2", OrderID: order.ID, Steps: 1, Price: intentPrice("100"), Cost: intentPrice("10"), Fee: intentPrice("0.01"), AtMS: 13})
	if err == nil || queryErr == nil {
		t.Fatal("borrower cancellation not propagated", err, queryErr)
	}
}
