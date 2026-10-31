package execution

import (
	"context"
	"strings"
	"testing"
)

func TestFactorRevisionRevokesSolePendingLimitAndSettlesFinalFill(t *testing.T) {
	for _, memory := range []bool{false, true} {
		storeName := "SQLite"
		if memory {
			storeName = "Memory"
		}
		for _, exit := range []bool{false, true} {
			kind := "entry"
			if exit {
				kind = "exit"
			}
			t.Run(storeName+"/"+kind, func(t *testing.T) {
				ctx := context.Background()
				borrow, _ := strategyService(t, memory)
				store := borrow.service.store
				price := "100"
				market := borrow.service.markets["BTC"]
				market.quote = func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
					return VisibleQuote{Bid: intentPrice(price), Ask: intentPrice(price), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
				}
				borrow.service.markets["BTC"] = market
				pending := pendingTestOrder(store, "a")
				pending.Entry.Conditions.CreatedBar = 10
				pending.Entry.Conditions.Limit = intentPrice("90")
				if exit {
					if err := borrow.RebalanceStrategy(strategyRequest("a", "initial-entry", 10, StrategyTargetsFull, ExecutableTarget{Lot: pending.Lot, SignedSteps: 10}), 10); err != nil {
						t.Fatal(err)
					}
					pending.Entry.FilledSteps, pending.Entry.State = 10, Canceled
					pending.Canceled = true
					source := pending.Entry
					source.ID, source.Kind, source.Side, source.State, source.FilledSteps = "pending-exit-a", ExitIntent, Sell, PendingCondition, 0
					source.Conditions.Limit = intentPrice("110")
					pending.Exit = &source
				}
				saveTestPendingBook(t, store, []PendingDefinition{{Order: *pending, Instrument: ledgerInstrument(), Policy: OrderPolicy{Mode: ExchangeFirst}}})
				if err := borrow.AdvancePendingExecution(ctx, 12); err != nil {
					t.Fatal(err)
				}
				snapshot, err := borrow.Snapshot(ctx)
				if err != nil || len(snapshot.Orders) != 1 || !snapshot.Orders[0].Intent.Limit.IsPositive() {
					t.Fatal("sole pending limit did not rest", snapshot.Orders, err)
				}
				order := snapshot.Orders[0]
				adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
				adapter.query = func(context.Context, string, string) (QueryResult, error) {
					price := "90"
					if exit {
						price = "110"
					}
					return QueryResult{Found: true, Complete: true, Authoritative: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: order.ExchangeID, Fills: []FillReport{{EventID: "cancel-race", OrderID: order.Intent.ID, Steps: 4, Price: intentPrice(price), Cost: intentPrice(price).Mul(intentPrice("0.4")), Cumulative: true, AtMS: 13}}}}, nil
				}
				borrow.service.executor.Adapter = adapter
				prepared, err := borrow.PrepareStrategy(strategyRequest("b", "new-factor-contributor", 13, StrategyTargetsFull, ExecutableTarget{Lot: "factor-lot", SignedSteps: 1}))
				if err != nil {
					t.Fatal(err)
				}
				calls := adapter.calls()
				if len(calls) != 2 || !strings.HasPrefix(calls[0], "Cancel:") || !strings.HasPrefix(calls[1], "Query:") {
					t.Fatal("factor preparation must cancel then query final fills before replacement planning", calls)
				}
				settled, err := store.Order(ctx, order.Intent.ID)
				if err != nil || settled.State != OrderCanceled || settled.FilledSteps != 4 {
					t.Fatal("final cancellation fill not settled", settled, err)
				}
				wantHeld := int64(4)
				wantDesired := int64(4)
				if exit {
					wantHeld = 6
					wantDesired = 0
				}
				book, err := store.LoadPendingBook(ctx)
				if err != nil || len(book) != 1 || book[0].Order.Desired != wantDesired || book[0].Order.Entry.VenueLimit != nil || book[0].Order.Exit != nil && book[0].Order.Exit.VenueLimit != nil {
					t.Fatal("revoked limit did not return to local final exposure", book, err)
				}
				found := false
				for _, target := range prepared.Plan.Targets {
					if target.Strategy == "a" && target.Lot == pending.Lot {
						found = true
						if target.SignedSteps != wantHeld {
							t.Fatalf("replacement inherited unfilled reservation: got=%d want=%d", target.SignedSteps, wantHeld)
						}
					}
				}
				if !found {
					t.Fatal("replacement plan lost settled pending lot")
				}
				adapter.submit = func(_ context.Context, order OrderIntent, _ string) (SubmitReceipt, error) {
					return SubmitReceipt{ExchangeID: "filled-" + order.ID, Fills: []FillReport{{EventID: "fill-" + order.ID, OrderID: order.ID, Steps: order.Steps, Price: order.Observation.Price, AtMS: order.Observation.AtMS}}}, nil
				}
				if err := borrow.WithState(func(service *SharedAccount) error { return service.SendPrepared(prepared, 13, ctx) }); err != nil {
					t.Fatal(err)
				}
				if err := borrow.AdvancePendingExecution(ctx, 14); err != nil {
					t.Fatal(err)
				}
				snapshot, err = borrow.Snapshot(ctx)
				if err != nil {
					t.Fatal(err)
				}
				held := int64(0)
				for _, lot := range snapshot.Lots {
					if lot.Strategy == "a" && lot.ID == pending.Lot {
						held += lot.SignedSteps
					}
				}
				if held != wantHeld {
					t.Fatalf("unmarketable account tick changed held position: got=%d want=%d", held, wantHeld)
				}
				if exit {
					price = "110"
					if err := borrow.AdvancePendingExecution(ctx, 15); err != nil {
						t.Fatal(err)
					}
					snapshot, err = borrow.Snapshot(ctx)
					if err != nil {
						t.Fatal(err)
					}
					for _, lot := range snapshot.Lots {
						if lot.Strategy == "a" && lot.ID == pending.Lot && lot.SignedSteps != 0 {
							t.Fatal("eligible account tick lost original exit request", lot)
						}
					}
				}
			})
		}
	}
}
