package execution

import (
	"context"
	"testing"
)

func refreshBorrow(t *testing.T, adapter *fakeExecutionAdapter) (*SharedAccountBorrow, *Store) {
	t.Helper()
	store, registry, owner, _ := testStore(t)
	service := &SharedAccount{ctx: context.Background(), owner: owner, store: store, executor: executorFor(store, owner, adapter), opts: SharedExecutionOptions{Adapter: adapter, AuthoritativeSnapshot: true}}
	borrow := service.Borrow()
	t.Cleanup(func() { borrow.Release(); registry.Close() })
	return borrow, store
}

func TestRefreshWorkingOrdersRecoversMissedFill(t *testing.T) {
	adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, AuthoritativeNotFound: true}}
	adapter.query = func(context.Context, string, string) (QueryResult, error) {
		return QueryResult{Found: true, Authoritative: true, Complete: true, Receipt: SubmitReceipt{ExchangeID: "venue-order", Fills: []FillReport{{EventID: "fill-1", OrderID: "order", Steps: 10, Price: intentPrice("100"), Cost: intentPrice("1000"), Cumulative: true, AuthoritativeSnapshot: true, AtMS: 20}}}}, nil
	}
	borrow, store := refreshBorrow(t, adapter)
	planOrder(t, store, "order", 1, Buy, EntryIntent, map[string]int64{"strategy": 10})
	if err := executorFor(store, borrow.service.owner, adapter).Send("order", 10); err != nil {
		t.Fatal(err)
	}
	if err := borrow.RefreshWorkingOrders(context.Background(), 20); err != nil {
		t.Fatal(err)
	}
	order, err := store.Order(context.Background(), "order")
	if err != nil {
		t.Fatal(err)
	}
	if order.State != OrderFilled || order.FilledSteps != 10 {
		t.Fatalf("missed fill not recovered: %+v", order)
	}
	if calls := adapter.calls(); len(calls) != 2 || calls[0] != "Submit:"+order.ClientID || calls[1] != "Query:"+order.ClientID {
		t.Fatalf("unexpected calls: %v", calls)
	}
}

func TestRefreshWorkingOrdersCancelsExpiredOrder(t *testing.T) {
	adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, AuthoritativeNotFound: true}}
	queryCount := 0
	adapter.query = func(context.Context, string, string) (QueryResult, error) {
		queryCount++
		if queryCount == 1 {
			return QueryResult{Found: true, Authoritative: true, Complete: true, Receipt: SubmitReceipt{ExchangeID: "venue-order"}}, nil
		}
		return QueryResult{Found: true, Authoritative: true, Complete: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: "venue-order"}}, nil
	}
	borrow, store := refreshBorrow(t, adapter)
	planOrder(t, store, "order", 1, Buy, EntryIntent, map[string]int64{"strategy": 10})
	if err := executorFor(store, borrow.service.owner, adapter).Send("order", 10); err != nil {
		t.Fatal(err)
	}
	if err := borrow.RefreshWorkingOrders(context.Background(), 2000); err != nil {
		t.Fatal(err)
	}
	order, err := store.Order(context.Background(), "order")
	if err != nil {
		t.Fatal(err)
	}
	if order.State != OrderCanceled {
		t.Fatalf("expired order not canceled: %+v", order)
	}
	calls := adapter.calls()
	submits := 0
	for _, call := range calls {
		if len(call) >= 7 && call[:7] == "Submit:" {
			submits++
		}
	}
	if submits != 1 {
		t.Fatalf("refresh resubmitted order: %v", calls)
	}
	foundCancel := false
	for _, call := range calls {
		if call == "Cancel:venue-order" {
			foundCancel = true
		}
	}
	if !foundCancel {
		t.Fatalf("refresh did not cancel expired order: %v", calls)
	}
}
