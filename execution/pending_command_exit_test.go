package execution

import (
	"context"
	"testing"
)

func TestPendingExitCommandsLifecycle(t *testing.T) {
	for _, memory := range []bool{true, false} {
		for _, short := range []bool{false, true} {
			t.Run(map[bool]string{true: "memory", false: "sqlite"}[memory]+map[bool]string{true: "/short", false: "/long"}[short], func(t *testing.T) {
				b, venue := strategyService(t, memory)
				ctx := context.Background()
				create := pendingCreate("enter", "a")
				create.Create.Conditions = IntentConditions{}
				if short {
					create.Create.Side = Sell
				}
				if _, err := b.ApplyPendingCommand(ctx, create, 12); err != nil {
					t.Fatal(err)
				}
				if _, err := b.ApplyPendingCommand(ctx, pendingCreate("other", "b"), 13); err != nil {
					t.Fatal(err)
				}
				limit := "110"
				if short {
					limit = "90"
				}
				exit := PendingCommand{ID: "exit", Strategy: "a", Lot: "lot-a", ExpectedRevision: 1, Action: PendingSetExit, Exit: &PendingExitSpec{QuantitySteps: 4, Conditions: IntentConditions{Limit: intentPrice(limit)}}}
				if rev, err := b.ApplyPendingCommand(ctx, exit, 14); err != nil || rev != 2 {
					t.Fatal(rev, err)
				}
				book, _ := b.PendingOrders(ctx)
				goal := int64(6)
				if short {
					goal = -goal
				}
				if book[0].Order.Desired != goal || book[0].Order.Exit.QuantitySteps != 4 || !book[0].Order.Canceled || book[1].Order.Canceled {
					t.Fatal(book)
				}
				metrics := venue.Metrics()
				if rev, err := b.ApplyPendingCommand(ctx, exit, 15); err != nil || rev != 2 {
					t.Fatal(rev, err)
				}
				if metrics != venue.Metrics() {
					t.Fatal("exit replay resent")
				}
				if _, err := b.ApplyPendingCommand(ctx, PendingCommand{ID: "cancel-exit", Strategy: "a", Lot: "lot-a", ExpectedRevision: 2, Action: PendingCancelExit}, 16); err != nil {
					t.Fatal(err)
				}
				book, _ = b.PendingOrders(ctx)
				if book[0].Order.Exit.State != Canceled || absSteps(book[0].Order.Desired) != 10 {
					t.Fatal(book)
				}
				exit.ID = "exit-again"
				exit.ExpectedRevision = 3
				if _, err := b.ApplyPendingCommand(ctx, exit, 17); err != nil {
					t.Fatal(err)
				}
				edit := PendingCommand{ID: "edit-exit", Strategy: "a", Lot: "lot-a", ExpectedRevision: 4, Action: PendingEditExitLimit, Limit: intentPrice("100")}
				if _, err := b.ApplyPendingCommand(ctx, edit, 18); err != nil {
					t.Fatal(err)
				}
				snapshot, err := b.Snapshot(ctx)
				if err != nil || len(snapshot.Lots) != 1 || snapshot.Lots[0].SignedSteps != goal {
					t.Fatal(snapshot.Lots, err)
				}
				close := PendingCommand{ID: "close", Strategy: "a", Lot: "lot-a", ExpectedRevision: 5, Action: PendingSetExit, Exit: &PendingExitSpec{}}
				if _, err := b.ApplyPendingCommand(ctx, close, 19); err != nil {
					t.Fatal(err)
				}
				snapshot, err = b.Snapshot(ctx)
				for _, lot := range snapshot.Lots {
					if lot.SignedSteps != 0 {
						t.Fatal("close reversed or retained inventory", snapshot.Lots, err)
					}
				}
			})
		}
	}
}

func TestPendingExitEditSettlesFinalFillWithoutSecondReduction(t *testing.T) {
	for _, filled := range []int64{2, 4} {
		t.Run(map[int64]string{2: "partial", 4: "complete"}[filled], func(t *testing.T) {
			b, _ := strategyService(t, true)
			ctx := context.Background()
			create := pendingCreate("enter", "a")
			create.Create.Conditions = IntentConditions{}
			create.Create.Policy.Mode = ExchangeFirst
			if _, err := b.ApplyPendingCommand(ctx, create, 12); err != nil {
				t.Fatal(err)
			}
			exit := PendingCommand{ID: "exit", Strategy: "a", Lot: "lot-a", ExpectedRevision: 1, Action: PendingSetExit, Exit: &PendingExitSpec{QuantitySteps: 4, Conditions: IntentConditions{Limit: intentPrice("110")}}}
			if _, err := b.ApplyPendingCommand(ctx, exit, 13); err != nil {
				t.Fatal(err)
			}
			snapshot, _ := b.Snapshot(ctx)
			var order StoredOrder
			for _, o := range snapshot.Orders {
				if o.State == OrderAcknowledged {
					order = o
				}
			}
			if order.Intent.ID == "" {
				t.Fatal("missing resting exit", snapshot.Orders)
			}
			adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
			var submitted int64
			adapter.submit = func(_ context.Context, intent OrderIntent, _ string) (SubmitReceipt, error) {
				submitted += intent.Steps
				return SubmitReceipt{ExchangeID: "edited-exit"}, nil
			}
			adapter.query = func(context.Context, string, string) (QueryResult, error) {
				return QueryResult{Found: true, Complete: true, Authoritative: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: order.ExchangeID, Fills: []FillReport{{EventID: "exit-cancel-final", OrderID: order.Intent.ID, Steps: filled, Price: intentPrice("110"), Cost: intentPrice("11").Mul(intentPrice(map[int64]string{2: "2", 4: "4"}[filled])), Cumulative: true, AtMS: 14}}}}, nil
			}
			b.service.executor.Adapter = adapter
			if _, err := b.ApplyPendingCommand(ctx, PendingCommand{ID: "edit", Strategy: "a", Lot: "lot-a", ExpectedRevision: 2, Action: PendingEditExitLimit, Limit: intentPrice("120")}, 14); err != nil {
				t.Fatal(err)
			}
			book, err := b.PendingOrders(ctx)
			if err != nil || book[0].Order.Desired != 6 {
				t.Fatal(book, err)
			}
			if filled == 4 && book[0].Order.Exit.State != Filled {
				t.Fatal("final fill did not complete exit", book)
			}
			if filled == 2 && book[0].Order.Exit.QuantitySteps != 2 {
				t.Fatal("edit repeated original reduction", book)
			}
			if submitted != 4-filled {
				t.Fatal("exchange-first edit did not submit exact residual", submitted, adapter.calls())
			}
		})
	}
}

func TestPendingExitInvalidPayloadDoesNotCancelEntry(t *testing.T) {
	b, venue := strategyService(t, true)
	ctx := context.Background()
	create := pendingCreate("enter", "a")
	create.Create.Policy.Mode = ExchangeFirst
	if _, err := b.ApplyPendingCommand(ctx, create, 12); err != nil {
		t.Fatal(err)
	}
	metrics := venue.Metrics()
	for _, spec := range []*PendingExitSpec{nil, {QuantitySteps: -1}, {Conditions: IntentConditions{Limit: intentPrice("-1")}}, {Conditions: IntentConditions{StopBars: 1}}} {
		if _, err := b.ApplyPendingCommand(ctx, PendingCommand{ID: "invalid", Strategy: "a", Lot: "lot-a", ExpectedRevision: 1, Action: PendingSetExit, Exit: spec}, 13); err == nil {
			t.Fatal("invalid exit accepted", spec)
		}
	}
	if metrics != venue.Metrics() {
		t.Fatal("invalid exit touched venue")
	}
	book, _ := b.PendingOrders(ctx)
	if book[0].Order.Canceled || book[0].CommandRevision != 1 {
		t.Fatal(book)
	}
}

func TestPendingExitUncertainSendRetryDoesNotResubmit(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	create := pendingCreate("enter", "a")
	create.Create.Conditions = IntentConditions{}
	if _, err := b.ApplyPendingCommand(ctx, create, 12); err != nil {
		t.Fatal(err)
	}
	adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
	adapter.submit = func(context.Context, OrderIntent, string) (SubmitReceipt, error) {
		return SubmitReceipt{}, context.DeadlineExceeded
	}
	b.service.executor.Adapter = adapter
	exit := PendingCommand{ID: "unknown-exit", Strategy: "a", Lot: "lot-a", ExpectedRevision: 1, Action: PendingSetExit, Exit: &PendingExitSpec{QuantitySteps: 4}}
	if _, err := b.ApplyPendingCommand(ctx, exit, 13); err == nil {
		t.Fatal("unknown send reported success")
	}
	if _, err := b.ApplyPendingCommand(ctx, exit, 14); err == nil {
		t.Fatal("unknown send bypassed recovery")
	}
	if len(adapter.calls()) != 1 {
		t.Fatal("unknown exit resubmitted", adapter.calls())
	}
	book, err := b.PendingOrders(ctx)
	if err != nil || book[0].CommandRevision != 2 || book[0].Order.Desired != 6 {
		t.Fatal(book, err)
	}
}

func TestPendingCancelExitSettlesFinalFill(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	create := pendingCreate("enter", "a")
	create.Create.Conditions = IntentConditions{}
	create.Create.Policy.Mode = ExchangeFirst
	if _, err := b.ApplyPendingCommand(ctx, create, 12); err != nil {
		t.Fatal(err)
	}
	exit := PendingCommand{ID: "exit", Strategy: "a", Lot: "lot-a", ExpectedRevision: 1, Action: PendingSetExit, Exit: &PendingExitSpec{QuantitySteps: 4, Conditions: IntentConditions{Limit: intentPrice("110")}}}
	if _, err := b.ApplyPendingCommand(ctx, exit, 13); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := b.Snapshot(ctx)
	var order StoredOrder
	for _, o := range snapshot.Orders {
		if o.State == OrderAcknowledged {
			order = o
		}
	}
	if order.Intent.ID == "" {
		t.Fatal("missing resting exit")
	}
	adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
	adapter.query = func(context.Context, string, string) (QueryResult, error) {
		return QueryResult{Found: true, Complete: true, Authoritative: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: order.ExchangeID, Fills: []FillReport{{EventID: "cancel-exit-final", OrderID: order.Intent.ID, Steps: 2, Price: intentPrice("110"), Cost: intentPrice("22"), Cumulative: true, AtMS: 14}}}}, nil
	}
	b.service.executor.Adapter = adapter
	if _, err := b.ApplyPendingCommand(ctx, PendingCommand{ID: "cancel", Strategy: "a", Lot: "lot-a", ExpectedRevision: 2, Action: PendingCancelExit}, 14); err != nil {
		t.Fatal(err)
	}
	book, err := b.PendingOrders(ctx)
	if err != nil || book[0].Order.Desired != 8 || book[0].Order.Exit.State != Canceled || book[0].Order.Exit.FilledSteps != 2 {
		t.Fatal(book, err)
	}
	if len(adapter.calls()) != 2 {
		t.Fatal("canceled exit sent a replacement", adapter.calls())
	}
}

func TestPendingExitRestartReplayAndLatchedLimitEdit(t *testing.T) {
	b, venue := strategyService(t, false)
	ctx := context.Background()
	create := pendingCreate("enter", "a")
	create.Create.Conditions = IntentConditions{}
	if _, err := b.ApplyPendingCommand(ctx, create, 12); err != nil {
		t.Fatal(err)
	}
	exit := PendingCommand{ID: "stop-limit-exit", Strategy: "a", Lot: "lot-a", ExpectedRevision: 1, Action: PendingSetExit, Exit: &PendingExitSpec{QuantitySteps: 4, Conditions: IntentConditions{Stop: intentPrice("95"), Limit: intentPrice("110")}}}
	if _, err := b.ApplyPendingCommand(ctx, exit, 13); err != nil {
		t.Fatal(err)
	}
	market := b.service.markets["BTC"]
	market.quote = func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		return VisibleQuote{Bid: intentPrice("90"), Ask: intentPrice("90"), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
	}
	b.service.markets["BTC"] = market
	if err := b.AdvancePendingExecution(ctx, 14); err != nil {
		t.Fatal(err)
	}
	opts := b.service.opts
	key := b.service.AccountKey()
	b.Release()
	if err := b.service.Close(); err != nil {
		t.Fatal(err)
	}
	registry := &AccountRegistry{}
	owner, err := registry.Acquire(key)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewSharedAccount(owner, opts)
	if err != nil {
		t.Fatal(err)
	}
	account := service.Borrow()
	t.Cleanup(func() { account.Release(); registry.Close(); service.Close() })
	if err := account.Reconcile("restart-exit", 15); err != nil {
		t.Fatal(err)
	}
	price := "100"
	if err := account.RegisterAccountQuotes([]Instrument{ledgerInstrument()}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		return VisibleQuote{Bid: intentPrice(price), Ask: intentPrice(price), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	metrics := venue.Metrics()
	if revision, err := account.ApplyPendingCommand(ctx, exit, 16); err != nil || revision != 2 {
		t.Fatal(revision, err)
	}
	if metrics != venue.Metrics() {
		t.Fatal("restart replay sent exit")
	}
	edit := PendingCommand{ID: "edit-latched", Strategy: "a", Lot: "lot-a", ExpectedRevision: 2, Action: PendingEditExitLimit, Limit: intentPrice("120")}
	if _, err := account.ApplyPendingCommand(ctx, edit, 17); err != nil {
		t.Fatal(err)
	}
	book, err := account.PendingOrders(ctx)
	if err != nil || !book[0].Order.Exit.Triggered || book[0].Order.Desired != 6 || book[0].CommandRevision != 3 {
		t.Fatal("restart/edit lost exit latch or goal", book, err)
	}
	price = "125"
	if err := account.AdvancePendingExecution(ctx, 18); err != nil {
		t.Fatal(err)
	}
	snapshot, err := account.Snapshot(ctx)
	if err != nil || len(snapshot.Lots) != 1 || snapshot.Lots[0].SignedSteps != 6 {
		t.Fatal("restart tick did not execute persisted exit", snapshot.Lots, err)
	}
}
