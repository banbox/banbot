package execution

import (
	"context"
	"reflect"
	"testing"
)

type admissionState struct {
	snapshot  AccountSnapshot
	book      []PendingDefinition
	metrics   PaperMetrics
	plan      Plan
	highwater int64
}

func tickPriceService(t *testing.T, price *string) (*SharedAccountBorrow, *PaperAdapter) {
	t.Helper()
	b, venue := strategyService(t, true)
	market := b.service.markets["BTC"]
	market.quote = func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		return VisibleQuote{Bid: intentPrice(*price), Ask: intentPrice(*price), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
	}
	b.service.markets["BTC"] = market
	return b, venue
}

func captureAdmission(t *testing.T, b *SharedAccountBorrow, venue *PaperAdapter) admissionState {
	t.Helper()
	ctx := context.Background()
	snapshot, err := b.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	book, err := b.PendingOrders(ctx)
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := b.service.store.LatestPlan(ctx)
	return admissionState{snapshot: snapshot, book: book, metrics: venue.Metrics(), plan: plan, highwater: snapshot.Checkpoint}
}

func requireOffTick(t *testing.T, b *SharedAccountBorrow, venue *PaperAdapter, command PendingCommand, now int64) {
	t.Helper()
	before := captureAdmission(t, b, venue)
	_, err := b.ApplyPendingCommand(context.Background(), command, now)
	if code, ok := RejectionCode(err); !ok || code != RejectLimitOffTick {
		t.Fatalf("%s: expected limit_off_tick, got %v", command.Action, err)
	}
	if after := captureAdmission(t, b, venue); !reflect.DeepEqual(before, after) {
		t.Fatalf("%s: rejected command changed account state", command.Action)
	}
	// No receipt: the same identity with a valid price is a fresh command.
	if _, err := b.service.store.StrategyCheckpoint(context.Background(), command.Strategy, "pending-command:"+command.ID); err == nil {
		t.Fatalf("%s: rejected command left a receipt", command.Action)
	}
}

func TestAdmissionRejectsOffTickOrderPrices(t *testing.T) {
	price := "100"
	b, venue := tickPriceService(t, &price)
	ctx := context.Background()
	offTick := intentPrice("90.005")

	create := pendingCreate("off-create", "a")
	create.Create.Conditions.Limit = offTick
	requireOffTick(t, b, venue, create, 12)

	create = pendingCreate("off-create-protection", "a")
	create.Create.Protections = []PositionProtection{{Kind: TakeProfitProtection, Level: intentPrice("120"), Limit: offTick}}
	requireOffTick(t, b, venue, create, 12)

	// Trigger levels, trailing activation and stops are thresholds: off-tick is fine.
	ok := pendingCreate("on-tick", "a")
	ok.Create.Protections = []PositionProtection{
		{Kind: StopLossProtection, Level: intentPrice("95.0037")},
		{Kind: TakeProfitProtection, Level: intentPrice("120.0021"), Limit: intentPrice("119.99")},
		{Kind: TrailingProtection, TrailingPercent: intentPrice("5"), ActivationPrice: intentPrice("110.005")},
	}
	ok.Create.Conditions.Stop = intentPrice("89.001")
	if revision, err := b.ApplyPendingCommand(ctx, ok, 12); err != nil || revision != 1 {
		t.Fatal(revision, err)
	}

	edit := PendingCommand{ID: "off-edit", Strategy: "a", Lot: "lot-a", ExpectedRevision: 1, Action: PendingEditLimit, Limit: offTick}
	requireOffTick(t, b, venue, edit, 13)
	edit.ID, edit.Limit = "on-edit", intentPrice("90.01")
	if revision, err := b.ApplyPendingCommand(ctx, edit, 13); err != nil || revision != 2 {
		t.Fatal(revision, err)
	}

	prot := PendingCommand{ID: "off-prot", Strategy: "a", Lot: "lot-a", ExpectedRevision: 2, Action: PendingSetProtections, Protections: []PositionProtection{{Kind: StopLossProtection, Level: intentPrice("80"), Limit: offTick}}}
	requireOffTick(t, b, venue, prot, 14)
	prot.ID = "on-prot"
	prot.Protections[0].Limit = intentPrice("79.99")
	if revision, err := b.ApplyPendingCommand(ctx, prot, 14); err != nil || revision != 3 {
		t.Fatal(revision, err)
	}

	// Exit commands need held inventory: fill a second lot at the market.
	fill := pendingCreate("fill", "b")
	fill.Create.Conditions = IntentConditions{}
	if _, err := b.ApplyPendingCommand(ctx, fill, 15); err != nil {
		t.Fatal(err)
	}
	exit := PendingCommand{ID: "off-exit", Strategy: "b", Lot: "lot-b", ExpectedRevision: 1, Action: PendingSetExit, Exit: &PendingExitSpec{QuantitySteps: 4, Conditions: IntentConditions{Limit: offTick}}}
	requireOffTick(t, b, venue, exit, 16)
	exit.ID, exit.Exit.Conditions = "on-exit", IntentConditions{Limit: intentPrice("110.01"), Stop: intentPrice("90.0004")}
	if revision, err := b.ApplyPendingCommand(ctx, exit, 16); err != nil || revision != 2 {
		t.Fatal(revision, err)
	}
	editExit := PendingCommand{ID: "off-edit-exit", Strategy: "b", Lot: "lot-b", ExpectedRevision: 2, Action: PendingEditExitLimit, Limit: offTick}
	requireOffTick(t, b, venue, editExit, 17)
	editExit.ID, editExit.Limit = "on-edit-exit", intentPrice("111")
	if revision, err := b.ApplyPendingCommand(ctx, editExit, 17); err != nil || revision != 3 {
		t.Fatal(revision, err)
	}
}

// A resting limit that is off the tick can no longer be accepted, so the
// autonomous tick that later reaches it cannot refuse its plan and freeze the
// account.
func TestAdmissionOffTickRestingLimitNeverReachesAutonomousTick(t *testing.T) {
	price := "100"
	b, _ := tickPriceService(t, &price)
	ctx := context.Background()
	create := pendingCreate("resting", "a")
	create.Create.Conditions.Limit = intentPrice("90.005")
	if _, err := b.ApplyPendingCommand(ctx, create, 12); err == nil {
		t.Fatal("off-tick resting limit accepted")
	}
	create.Create.Conditions.Limit = intentPrice("90")
	if _, err := b.ApplyPendingCommand(ctx, create, 12); err != nil {
		t.Fatal(err)
	}
	price = "89"
	if err := b.AdvancePendingExecution(ctx, 13); err != nil {
		t.Fatal("tick failed after admission", err)
	}
	snapshot, _ := b.Snapshot(ctx)
	if snapshot.RiskFrozen || len(snapshot.Lots) != 1 || snapshot.Lots[0].SignedSteps != 10 {
		t.Fatal("limit did not fill", snapshot.Lots)
	}
}

func TestAdmissionRejectsOffTickTimeSeriesExitLimit(t *testing.T) {
	price := "100"
	b, venue := tickPriceService(t, &price)
	ctx := context.Background()
	create := pendingCreate("ts-create", "a")
	create.Create.Conditions = IntentConditions{}
	create.Metadata = &TimeSeriesMetadata{StrategyName: "ts", Symbol: "BTC", TimeFrame: "1m", OrderID: 1, InitPrice: intentPrice("100"), CreatedMS: 12}
	if _, err := b.ApplyPendingCommand(ctx, create, 12); err != nil {
		t.Fatal(err)
	}
	before := captureAdmission(t, b, venue)
	command := TimeSeriesExitCommand{ID: "ts-off", Strategy: "a", Conditions: IntentConditions{Limit: intentPrice("110.005")}}
	_, err := b.ApplyTimeSeriesExit(ctx, command, 13)
	if code, ok := RejectionCode(err); !ok || code != RejectLimitOffTick {
		t.Fatal("expected limit_off_tick", err)
	}
	if after := captureAdmission(t, b, venue); !reflect.DeepEqual(before, after) {
		t.Fatal("rejected exit changed account state")
	}
	command.ID, command.Conditions.Limit = "ts-on", intentPrice("110.01")
	if _, err := b.ApplyTimeSeriesExit(ctx, command, 13); err != nil {
		t.Fatal(err)
	}
}
