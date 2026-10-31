package execution

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func tsCreate(id string, lot VirtualLotID, orderID int64, market bool) PendingCommand {
	c := pendingCreate(id, "a")
	c.Lot = lot
	c.Metadata = &TimeSeriesMetadata{OrderID: orderID, StrategyName: "ts", Symbol: "BTC", TimeFrame: "1m", CreatedMS: 1, InitPrice: intentPrice("100"), Payload: json.RawMessage(`{"tag":"source"}`)}
	if market {
		c.Create.Conditions = IntentConditions{}
	}
	return c
}

func TestTimeSeriesAtomicMetadataAndExitBudget(t *testing.T) {
	for _, memory := range []bool{true, false} {
		t.Run(map[bool]string{true: "memory", false: "sqlite"}[memory], func(t *testing.T) {
			b, venue := strategyService(t, memory)
			ctx := context.Background()
			for i, lot := range []VirtualLotID{"first", "second"} {
				c := tsCreate(string(lot), lot, int64(i+1), true)
				if _, err := b.ApplyPendingCommand(ctx, c, int64(12+i)); err != nil {
					t.Fatal(err)
				}
				body, err := b.service.store.StrategyCheckpoint(ctx, "a", TimeSeriesMetadataName(lot))
				if err != nil {
					t.Fatal(err)
				}
				var m TimeSeriesMetadata
				if err = json.Unmarshal(body, &m); err != nil || m.OrderID != int64(i+1) || string(m.Payload) != `{"tag":"source"}` {
					t.Fatal(m, err)
				}
			}
			if _, err := b.ApplyPendingCommand(ctx, pendingCreate("other", "b"), 14); err != nil {
				t.Fatal(err)
			}
			c := TimeSeriesExitCommand{ID: "exit-budget", Strategy: "a", Force: true, Amount: intentPrice("1.3"), ExitTag: "user"}
			result, err := b.ApplyTimeSeriesExit(ctx, c, 15)
			if err != nil {
				t.Fatal(err)
			}
			// ledgerInstrument's quantity step is 0.1: one global 13-step budget.
			if len(result) != 2 || result[0].Lot != "first" || result[0].ReducedSteps != 10 || result[1].ReducedSteps != 3 {
				t.Fatal(result)
			}
			snapshot, err := b.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var held int64
			for _, lot := range snapshot.Lots {
				if lot.Strategy == "a" {
					held += lot.SignedSteps
				}
			}
			if held != 7 {
				t.Fatal(snapshot.Lots)
			}
			metrics := venue.Metrics()
			replay, err := b.ApplyTimeSeriesExit(ctx, c, 16)
			if err != nil || !reflect.DeepEqual(result, replay) || metrics != venue.Metrics() {
				t.Fatal(replay, err)
			}
			c.Amount = intentPrice("1.4")
			if _, err = b.ApplyTimeSeriesExit(ctx, c, 17); err == nil {
				t.Fatal("changed retry accepted")
			}
			book, err := b.PendingOrders(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range book {
				if d.Order.Strategy == "b" && d.Order.Canceled {
					t.Fatal("exit crossed strategy ownership")
				}
			}
		})
	}
}

func TestTimeSeriesPartialPendingCancelAndSelectors(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	first := tsCreate("first", "first", 1, false)
	first.Metadata.EnterTag = "selected"
	if _, err := b.ApplyPendingCommand(ctx, first, 12); err != nil {
		t.Fatal(err)
	}
	second := tsCreate("second", "second", 2, false)
	second.Metadata.EnterTag = "other"
	if _, err := b.ApplyPendingCommand(ctx, second, 13); err != nil {
		t.Fatal(err)
	}
	c := TimeSeriesExitCommand{ID: "cancel", Strategy: "a", EnterTag: "selected", UnFillOnly: true, Amount: intentPrice("0.3")}
	result, err := b.ApplyTimeSeriesExit(ctx, c, 14)
	if err != nil || len(result) != 1 || result[0].CanceledEntrySteps != 3 || result[0].ReducedSteps != 0 {
		t.Fatal(result, err)
	}
	book, err := b.PendingOrders(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range book {
		if d.Order.Lot == "first" && (d.Order.Entry.QuantitySteps != 7 || d.Order.Canceled) {
			t.Fatal(d)
		}
		if d.Order.Lot == "second" && d.Order.Entry.QuantitySteps != 10 {
			t.Fatal(d)
		}
	}
}

func TestTimeSeriesCloseDelayAndDirectOrderBypass(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	if _, err := b.ApplyPendingCommand(ctx, tsCreate("first", "first", 1, true), 12); err != nil {
		t.Fatal(err)
	}
	c := TimeSeriesExitCommand{ID: "delayed", Strategy: "a"}
	result, err := b.ApplyTimeSeriesExit(ctx, c, 14)
	if err != nil || len(result) != 0 {
		t.Fatal(result, err)
	}
	c.ID = "direct"
	c.OrderID = 1
	result, err = b.ApplyTimeSeriesExit(ctx, c, 15)
	if err != nil || len(result) != 1 || result[0].ReducedSteps != 10 {
		t.Fatal(result, err)
	}
}

func TestTimeSeriesMetadataAdmissionRollback(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	c := tsCreate("risk", "risk", 1, true)
	c.Create.QuantitySteps = 1000000000
	if _, err := b.ApplyPendingCommand(ctx, c, 12); err == nil {
		t.Fatal("risk violation accepted")
	}
	if _, err := b.service.store.StrategyCheckpoint(ctx, "a", TimeSeriesMetadataName("risk")); err == nil {
		t.Fatal("metadata leaked before atomic acceptance")
	}
	c = tsCreate("invalid", "invalid", 2, false)
	c.Metadata.Payload = json.RawMessage(`{`)
	if _, err := b.ApplyPendingCommand(ctx, c, 13); err == nil {
		t.Fatal("malformed metadata accepted")
	}
}

func TestTimeSeriesEmptyBookNoopReplay(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	c := TimeSeriesExitCommand{ID: "empty", Strategy: "a"}
	result, err := b.ApplyTimeSeriesExit(ctx, c, 12)
	if err != nil || len(result) != 0 {
		t.Fatal(result, err)
	}
	if _, err = b.ApplyPendingCommand(ctx, tsCreate("later", "later", 1, true), 13); err != nil {
		t.Fatal(err)
	}
	result, err = b.ApplyTimeSeriesExit(ctx, c, 14)
	if err != nil || len(result) != 0 {
		t.Fatal(result, err)
	}
	snapshot, _ := b.Snapshot(ctx)
	if len(snapshot.Lots) != 1 || snapshot.Lots[0].SignedSteps != 10 {
		t.Fatal("no-op retry closed subsequently created lot", snapshot.Lots)
	}
}

func TestTimeSeriesMetadataImmutableAndMalformedTimeframe(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	c := tsCreate("first", "first", 1, false)
	if _, err := b.ApplyPendingCommand(ctx, c, 12); err != nil {
		t.Fatal(err)
	}
	changed := *c.Metadata
	changed.OrderID = 2
	edit := PendingCommand{ID: "edit", Strategy: "a", Lot: "first", ExpectedRevision: 1, Action: PendingEditLimit, Limit: intentPrice("95"), Metadata: &changed}
	if _, err := b.ApplyPendingCommand(ctx, edit, 13); err == nil {
		t.Fatal("metadata identity changed")
	}
	edit.Metadata.Short = true
	if _, err := b.ApplyPendingCommand(ctx, edit, 14); err == nil {
		t.Fatal("metadata direction changed")
	}
	bad := tsCreate("bad", "bad", 3, false)
	bad.Metadata.TimeFrame = "invalid"
	if _, err := b.ApplyPendingCommand(ctx, bad, 15); err == nil {
		t.Fatal("invalid timeframe accepted")
	}
	if _, err := b.ApplyTimeSeriesExit(ctx, TimeSeriesExitCommand{ID: "bad-tf", Strategy: "a", TimeFrame: "invalid"}, 16); err == nil {
		t.Fatal("invalid exit timeframe accepted")
	}
}

func TestTimeSeriesSubStepBudgetDoesNotCancelPending(t *testing.T) {
	b, venue := strategyService(t, true)
	ctx := context.Background()
	c := tsCreate("first", "first", 1, false)
	c.Create.Policy.Mode = ExchangeFirst
	if _, err := b.ApplyPendingCommand(ctx, c, 12); err != nil {
		t.Fatal(err)
	}
	before, _ := b.Snapshot(ctx)
	metrics := venue.Metrics()
	results, err := b.ApplyTimeSeriesExit(ctx, TimeSeriesExitCommand{ID: "tiny", Strategy: "a", Amount: intentPrice("0.01"), Force: true}, 13)
	after, _ := b.Snapshot(ctx)
	if err != nil || len(results) != 0 || !reflect.DeepEqual(before.Orders, after.Orders) {
		t.Fatal(results, venue.Metrics(), metrics, err)
	}
	if _, err = b.ApplyTimeSeriesExit(ctx, TimeSeriesExitCommand{ID: "overflow", Strategy: "a", Amount: intentPrice("100000000000000000000000000000"), Force: true}, 14); err == nil {
		t.Fatal("overflow accepted")
	}
	after, _ = b.Snapshot(ctx)
	if !reflect.DeepEqual(before.Orders, after.Orders) {
		t.Fatal("invalid budget canceled venue order")
	}
}

func TestTimeSeriesExitCancellationFinalFillConsumesBudgetOnce(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	c := tsCreate("first", "first", 1, false)
	c.Create.Policy.Mode = ExchangeFirst
	if _, err := b.ApplyPendingCommand(ctx, c, 12); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := b.Snapshot(ctx)
	var resting StoredOrder
	for _, order := range snapshot.Orders {
		if order.State == OrderAcknowledged {
			resting = order
		}
	}
	if resting.Intent.ID == "" {
		t.Fatal("missing native entry")
	}
	adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
	adapter.query = func(context.Context, string, string) (QueryResult, error) {
		return QueryResult{Found: true, Complete: true, Authoritative: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: resting.ExchangeID, Fills: []FillReport{{EventID: "entry-cancel-final", OrderID: resting.Intent.ID, Steps: 4, Price: intentPrice("90"), Cost: intentPrice("36"), Cumulative: true, AtMS: 13}}}}, nil
	}
	adapter.submit = func(context.Context, OrderIntent, string) (SubmitReceipt, error) {
		return SubmitReceipt{ExchangeID: "exit"}, nil
	}
	b.service.executor.Adapter = adapter
	command := TimeSeriesExitCommand{ID: "exit", Strategy: "a", Amount: intentPrice("0.6"), Force: true, Conditions: IntentConditions{Limit: intentPrice("110")}}
	results, err := b.ApplyTimeSeriesExit(ctx, command, 14)
	if err != nil || len(results) != 1 || results[0].ReducedSteps != 4 || results[0].CanceledEntrySteps != 2 {
		t.Fatal(results, err)
	}
	book, err := b.PendingOrders(ctx)
	if err != nil || len(book) != 1 || book[0].Order.ResumeEntrySteps != 4 || book[0].Order.Desired != 0 || book[0].Order.Exit.QuantitySteps != 4 {
		t.Fatal(book, err)
	}
}

func TestTimeSeriesRateBudgetAcrossDifferentQuantitySteps(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	secondUnit := ledgerInstrument()
	secondUnit.ID = "ETH"
	secondUnit.QuantityStep = intentPrice("0.2")
	if err := b.RegisterAccountQuotes([]Instrument{secondUnit}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	first := tsCreate("first", "first", 1, true)
	if _, err := b.ApplyPendingCommand(ctx, first, 12); err != nil {
		t.Fatal(err)
	}
	second := tsCreate("second", "second", 2, true)
	second.Create.Instrument = "ETH"
	second.Metadata.Symbol = "ETH"
	if _, err := b.ApplyPendingCommand(ctx, second, 13); err != nil {
		t.Fatal(err)
	}
	results, err := b.ApplyTimeSeriesExit(ctx, TimeSeriesExitCommand{ID: "rate", Strategy: "a", Rate: intentPrice("0.5"), Amount: intentPrice("100"), Force: true}, 14)
	// Total quantity 3 * 0.5 = 1.5. BTC uses 1, ETH uses floor(0.5/0.2)*0.2.
	if err != nil || len(results) != 2 || results[0].Lot != "first" || results[0].ReducedSteps != 10 || results[1].ReducedSteps != 2 {
		t.Fatal(results, err)
	}
	snapshot, err := b.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, lot := range snapshot.Lots {
		if lot.ID == "first" && lot.SignedSteps != 0 || lot.ID == "second" && lot.SignedSteps != 8 {
			t.Fatal(snapshot.Lots)
		}
	}
}
