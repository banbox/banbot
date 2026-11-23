package execution

import (
	"context"
	"testing"
)

// Grid shape: one strategy, two lots on one instrument. Lot one holds a long
// with a stop; lot two rests a buy limit below. A price gap crosses both in one
// autonomous tick, so the plan has a sell reduction and a buy entry of the
// SAME strategy.
func selfCrossGridTick(t *testing.T) (*SharedAccountBorrow, error) {
	t.Helper()
	price := "100"
	b, _ := tickPriceService(t, &price)
	ctx := context.Background()
	first := PendingCommand{ID: "lot-1", Strategy: "a", Lot: "lot-1", Action: PendingCreate,
		Create: &PendingCreateSpec{Instrument: "BTC", Side: Buy, QuantitySteps: 10, Protections: []PositionProtection{{Kind: StopLossProtection, Level: intentPrice("95")}}}}
	if _, err := b.ApplyPendingCommand(ctx, first, 12); err != nil {
		t.Fatal(err)
	}
	second := PendingCommand{ID: "lot-2", Strategy: "a", Lot: "lot-2", Action: PendingCreate,
		Create: &PendingCreateSpec{Instrument: "BTC", Side: Buy, QuantitySteps: 10, Conditions: IntentConditions{Limit: intentPrice("90")}}}
	if _, err := b.ApplyPendingCommand(ctx, second, 13); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := b.Snapshot(ctx)
	if len(snapshot.Lots) != 1 || snapshot.Lots[0].SignedSteps != 10 {
		t.Fatal("lot-1 did not fill", snapshot.Lots)
	}
	price = "89"
	return b, b.AdvancePendingExecution(ctx, 14)
}

// The tick succeeds without freezing the account: the stop of lot-1 is kept and
// the opposite-side entry of lot-2 is sequenced to a later tick (see
// sequenceOppositeLots).
func TestSelfCrossReachableFromAutonomousTick(t *testing.T) {
	b, err := selfCrossGridTick(t)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := b.Snapshot(context.Background())
	if snapshot.RiskFrozen {
		t.Fatal("account frozen")
	}
}
