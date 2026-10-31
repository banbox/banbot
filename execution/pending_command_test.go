package execution

import (
	"context"
	"reflect"
	"testing"
)

func pendingCreate(id string, strategy StrategyID) PendingCommand {
	return PendingCommand{ID: id, Strategy: strategy, Lot: VirtualLotID("lot-" + string(strategy)), Action: PendingCreate,
		Create: &PendingCreateSpec{Instrument: "BTC", Side: Buy, QuantitySteps: 10, Conditions: IntentConditions{Limit: intentPrice("90")}}}
}

func TestPendingCommandAtomicReplayAndStrategyIsolation(t *testing.T) {
	for _, memory := range []bool{true, false} {
		t.Run(map[bool]string{true: "memory", false: "sqlite"}[memory], func(t *testing.T) {
			b, venue := strategyService(t, memory)
			ctx := context.Background()
			a := pendingCreate("create-a", "a")
			if revision, err := b.ApplyPendingCommand(ctx, a, 12); err != nil || revision != 1 {
				t.Fatal(revision, err)
			}
			if _, err := b.ApplyPendingCommand(ctx, pendingCreate("create-b", "b"), 13); err != nil {
				t.Fatal(err)
			}
			before, _ := b.Snapshot(ctx)
			metrics := venue.Metrics()
			if revision, err := b.ApplyPendingCommand(ctx, a, 14); err != nil || revision != 1 {
				t.Fatal(revision, err)
			}
			after, _ := b.Snapshot(ctx)
			if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(metrics, venue.Metrics()) {
				t.Fatal("replay changed account")
			}
			a.Create.QuantitySteps++
			if _, err := b.ApplyPendingCommand(ctx, a, 15); err == nil {
				t.Fatal("changed retry accepted")
			}
			cancel := PendingCommand{ID: "cancel-a", Strategy: "a", Lot: "lot-a", Action: PendingCancelEntry, ExpectedRevision: 1}
			if revision, err := b.ApplyPendingCommand(ctx, cancel, 16); err != nil || revision != 2 {
				t.Fatal(revision, err)
			}
			book, err := b.PendingOrders(ctx)
			if err != nil || len(book) != 2 {
				t.Fatal(book, err)
			}
			for _, d := range book {
				if d.Order.Strategy == "a" && !d.Order.Canceled || d.Order.Strategy == "b" && d.Order.Canceled {
					t.Fatal("cancel crossed strategy boundary", book)
				}
			}
			cancel.ID = "stale-cancel"
			if _, err := b.ApplyPendingCommand(ctx, cancel, 17); err == nil {
				t.Fatal("stale revision accepted")
			}
		})
	}
}

func TestPendingCommandInvalidAndFailedAdmissionLeaveNoReceipt(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	a := pendingCreate("create", "a")
	a.Create.Protections = []PositionProtection{{Kind: "invalid"}}
	if _, err := b.ApplyPendingCommand(ctx, a, 12); err == nil {
		t.Fatal("invalid protection accepted")
	}
	a.Create.Protections = nil
	a.Create.QuantitySteps = 1000000000
	a.Create.Conditions.Limit = intentPrice("100")
	if _, err := b.ApplyPendingCommand(ctx, a, 12); err == nil {
		t.Fatal("risk violation accepted")
	}
	book, err := b.PendingOrders(ctx)
	if err != nil || len(book) != 0 {
		t.Fatal("failed admission persisted book", book, err)
	}
	a.Create.QuantitySteps = 10
	a.Create.Conditions.Limit = intentPrice("90")
	if revision, err := b.ApplyPendingCommand(ctx, a, 12); err != nil || revision != 1 {
		t.Fatal("failed admission consumed identity", revision, err)
	}
}

func TestPendingCommandCancelSettlesFinalFillBeforeTarget(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	create := pendingCreate("create", "a")
	create.Create.Policy.Mode = ExchangeFirst
	if _, err := b.ApplyPendingCommand(ctx, create, 12); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := b.Snapshot(ctx)
	if len(snapshot.Orders) != 1 {
		t.Fatal(snapshot.Orders)
	}
	order := snapshot.Orders[0]
	adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
	adapter.query = func(context.Context, string, string) (QueryResult, error) {
		return QueryResult{Found: true, Complete: true, Authoritative: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: order.ExchangeID, Fills: []FillReport{{EventID: "cancel-race", OrderID: order.Intent.ID, Steps: 4, Price: intentPrice("90"), Cost: intentPrice("36"), Cumulative: true, AtMS: 13}}}}, nil
	}
	b.service.executor.Adapter = adapter
	if _, err := b.ApplyPendingCommand(ctx, PendingCommand{ID: "cancel", Strategy: "a", Lot: "lot-a", ExpectedRevision: 1, Action: PendingCancelEntry}, 13); err != nil {
		t.Fatal(err)
	}
	book, err := b.PendingOrders(ctx)
	if err != nil || len(book) != 1 || book[0].Order.Desired != 4 || book[0].Order.Entry.FilledSteps != 4 || !book[0].Order.Canceled {
		t.Fatal(book, err)
	}
	plan, err := b.LatestPlan(ctx)
	if err != nil || len(plan.Targets) != 1 || plan.Targets[0].SignedSteps != 4 {
		t.Fatal("cancel lost final fill", plan, err)
	}
}

func TestPendingCommandEditAndProtectionReplacement(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	create := pendingCreate("create", "a")
	create.Create.Conditions.Stop = intentPrice("110")
	if _, err := b.ApplyPendingCommand(ctx, create, 12); err != nil {
		t.Fatal(err)
	}
	edit := PendingCommand{ID: "edit", Strategy: "a", Lot: "lot-a", ExpectedRevision: 1, Action: PendingEditLimit, Limit: intentPrice("95")}
	if _, err := b.ApplyPendingCommand(ctx, edit, 13); err != nil {
		t.Fatal(err)
	}
	book, _ := b.PendingOrders(ctx)
	if !book[0].Order.Entry.Conditions.Limit.Equal(intentPrice("95")) || !book[0].Order.Entry.Conditions.Stop.Equal(intentPrice("110")) {
		t.Fatal("limit edit lost stop contract", book)
	}
	protection := PendingCommand{ID: "protect", Strategy: "a", Lot: "lot-a", ExpectedRevision: 2, Action: PendingSetProtections, Protections: []PositionProtection{{Kind: TrailingProtection, TrailingPercent: intentPrice("5")}}}
	if _, err := b.ApplyPendingCommand(ctx, protection, 14); err != nil {
		t.Fatal(err)
	}
	book, _ = b.PendingOrders(ctx)
	if len(book[0].Specs) != 1 || book[0].Specs[0].Kind != TrailingProtection {
		t.Fatal(book)
	}
}

func TestPendingCommandRetryResumesCommittedUnsentPlan(t *testing.T) {
	for _, supersede := range []bool{false, true} {
		t.Run(map[bool]string{false: "resume", true: "superseded"}[supersede], func(t *testing.T) {
			b, venue := strategyService(t, false)
			ctx := context.Background()
			if _, err := b.service.store.db.Exec(`CREATE TRIGGER fail_pending_send BEFORE UPDATE OF state ON exec_order WHEN NEW.state='Sending' BEGIN SELECT RAISE(ABORT,'injected pre-send failure'); END`); err != nil {
				t.Fatal(err)
			}
			create := pendingCreate("committed", "a")
			create.Create.Conditions.Limit = intentPrice("100")
			if _, err := b.ApplyPendingCommand(ctx, create, 12); err == nil {
				t.Fatal("pre-send failure not exercised")
			}
			snapshot, err := b.Snapshot(ctx)
			if err != nil || len(snapshot.Orders) != 1 || snapshot.Orders[0].State != OrderPrepared {
				t.Fatal(snapshot.Orders, err)
			}
			book, _ := b.PendingOrders(ctx)
			if len(book) != 1 || book[0].CommandRevision != 1 {
				t.Fatal("accepted command revision unavailable", book)
			}
			if _, err := b.service.store.db.Exec(`DROP TRIGGER fail_pending_send`); err != nil {
				t.Fatal(err)
			}
			if supersede {
				if _, err := b.ApplyPendingCommand(ctx, PendingCommand{ID: "cancel", Strategy: "a", Lot: "lot-a", ExpectedRevision: 1, Action: PendingCancelEntry}, 13); err != nil {
					t.Fatal(err)
				}
			}
			if revision, err := b.ApplyPendingCommand(ctx, create, 14); err != nil || revision != 1 {
				t.Fatal("retry did not resume committed plan", revision, err)
			}
			snapshot, _ = b.Snapshot(ctx)
			if supersede {
				if len(snapshot.Lots) != 0 {
					t.Fatal("replay resurrected superseded entry", snapshot.Lots)
				}
			} else if len(snapshot.Lots) != 1 || snapshot.Lots[0].SignedSteps != 10 {
				t.Fatal("prepared allocation never sent", snapshot.Lots)
			}
			metrics := venue.Metrics()
			if _, err := b.ApplyPendingCommand(ctx, create, 15); err != nil {
				t.Fatal(err)
			}
			if venue.Metrics() != metrics {
				t.Fatal("successful retry resent")
			}
		})
	}
}

func TestPendingCommandUncertainSendRetryDoesNotResubmit(t *testing.T) {
	b, _ := strategyService(t, true)
	adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
	adapter.submit = func(context.Context, OrderIntent, string) (SubmitReceipt, error) {
		return SubmitReceipt{}, context.DeadlineExceeded
	}
	b.service.executor.Adapter = adapter
	create := pendingCreate("uncertain", "a")
	create.Create.Conditions.Limit = intentPrice("100")
	if _, err := b.ApplyPendingCommand(context.Background(), create, 12); err == nil {
		t.Fatal("uncertain send reported success")
	}
	snapshot, err := b.Snapshot(context.Background())
	if err != nil || len(snapshot.Orders) != 1 || snapshot.Orders[0].State != OrderUnknown {
		t.Fatal(snapshot.Orders, err)
	}
	if _, err := b.ApplyPendingCommand(context.Background(), create, 13); err == nil {
		t.Fatal("unknown send bypassed recovery")
	}
	if len(adapter.calls()) != 1 {
		t.Fatal("unknown send retried transport", adapter.calls())
	}
	book, err := b.PendingOrders(context.Background())
	if err != nil || len(book) != 1 || book[0].CommandRevision != 1 {
		t.Fatal("unknown send lost durable admission", book, err)
	}
}

func TestPendingCommandProtectionRunsWithoutStrategyFacade(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	price := "100"
	market := b.service.markets["BTC"]
	market.quote = func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		return VisibleQuote{Bid: intentPrice(price), Ask: intentPrice(price), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
	}
	b.service.markets["BTC"] = market
	create := pendingCreate("protected", "a")
	create.Create.Conditions.Limit = intentPrice("100")
	create.Create.Protections = []PositionProtection{{Kind: TrailingProtection, TrailingPercent: intentPrice("5")}}
	if _, err := b.ApplyPendingCommand(ctx, create, 12); err != nil {
		t.Fatal(err)
	}
	price = "110"
	if err := b.AdvancePendingExecution(ctx, 13); err != nil {
		t.Fatal(err)
	}
	book, _ := b.PendingOrders(ctx)
	anchor := book[0].Order.Protections[string(TrailingProtection)].TrailingAnchor
	if !anchor.Equal(intentPrice("110")) {
		t.Fatal("account did not persist trailing anchor", anchor)
	}
	price = "104"
	if err := b.AdvancePendingExecution(ctx, 14); err != nil {
		t.Fatal(err)
	}
	snapshot, err := b.Snapshot(ctx)
	for _, lot := range snapshot.Lots {
		if lot.SignedSteps != 0 {
			t.Fatal("account-owned trailing failed to close", snapshot.Lots, err)
		}
	}
	book, _ = b.PendingOrders(ctx)
	if book[0].CommandRevision != 1 || book[0].Order.LastProtection != TrailingProtection {
		t.Fatal("autonomous tick lost command revision or trigger", book)
	}
}
