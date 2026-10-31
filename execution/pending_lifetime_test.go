package execution

import (
	"context"
	"math"
	"strings"
	"testing"
)

func TestPendingRestingLifetimeUsesSourceExpiryNotQuoteTTL(t *testing.T) {
	for _, memory := range []bool{false, true} {
		backend := "SQLite"
		if memory {
			backend = "Memory"
		}
		for _, expires := range []bool{false, true} {
			name := "no-source-expiry"
			if expires {
				name = "source-expiry"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				ctx := context.Background()
				borrow, paper := strategyService(t, memory)
				market := borrow.service.markets["BTC"]
				market.quote = func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
					return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 10, Bar: now}, nil
				}
				borrow.service.markets["BTC"] = market
				adapter := &fakeExecutionAdapter{capabilities: paper.Capabilities(), submit: paper.Submit, cancel: paper.Cancel, query: paper.Query}
				borrow.service.executor.Adapter = adapter
				pending := pendingTestOrder(borrow.service.store, "a")
				pending.Entry.Conditions.Limit = intentPrice("90")
				if expires {
					pending.Entry.Conditions.ExpiresAtMS = 5012
				}
				saveTestPendingBook(t, borrow.service.store, []PendingDefinition{{Order: *pending, Instrument: ledgerInstrument(), Policy: OrderPolicy{Mode: ExchangeFirst}}})
				if err := borrow.AdvancePendingExecution(ctx, 12); err != nil {
					t.Fatal(err)
				}
				snapshot, err := borrow.Snapshot(ctx)
				if err != nil || len(snapshot.Orders) != 1 {
					t.Fatal("pending limit did not rest", snapshot.Orders, err)
				}
				order := snapshot.Orders[0]
				plan, err := borrow.service.store.Plan(ctx, order.Intent.PlanID)
				if err != nil || plan.ExpiresMS != math.MaxInt64 {
					t.Fatal("pending plan lifetime inherited short quote TTL", plan.ExpiresMS, err)
				}
				if err := borrow.RefreshWorkingOrders(ctx, 5012); err != nil {
					t.Fatal(err)
				}
				settled, err := borrow.service.store.Order(ctx, order.Intent.ID)
				wantState := OrderAcknowledged
				if expires {
					wantState = OrderCanceled
				}
				if err != nil || settled.State != wantState || settled.FilledSteps != 0 {
					t.Fatal("recovery used quote expiry instead of source lifetime", settled, err)
				}
				if err := borrow.Reconcile("after-periodic-recovery", 5012); err != nil {
					t.Fatal(err)
				}
				if err := borrow.AdvancePendingExecution(ctx, 5013); err != nil {
					t.Fatal(err)
				}
				snapshot, err = borrow.Snapshot(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if expires && len(snapshot.Orders) != 0 || !expires && (len(snapshot.Orders) != 1 || snapshot.Orders[0].Intent.ID != order.Intent.ID) {
					t.Fatal("account tick changed accepted reservation lifetime", snapshot.Orders)
				}
				submits, cancels := 0, 0
				for _, call := range adapter.calls() {
					if strings.HasPrefix(call, "Submit:") {
						submits++
					}
					if strings.HasPrefix(call, "Cancel:") {
						cancels++
					}
				}
				wantCancels := 0
				if expires {
					wantCancels = 1
				}
				if submits != 1 || cancels != wantCancels || paper.Metrics().Fills != 0 {
					t.Fatalf("recovery/tick recreated pending limit: submits=%d cancels=%d fills=%+v", submits, cancels, paper.Metrics())
				}
			})
		}
	}
}
