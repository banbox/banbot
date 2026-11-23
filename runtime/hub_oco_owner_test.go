package runtime

import (
	"context"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

func hubOCOGroupOf(t *testing.T, f *hubTSFixture, od *ormo.InOutOrder) string {
	t.Helper()
	return hubPendingOf(t, f, od).Order.OCOGroup
}

// Two members of an OCO group are crossed by one gap. The owner cancels the
// later one in the commit that fills the earlier one; the robot sees exactly
// one fill, and its own post-fill cancel_entry is a harmless no-op.
func TestHubOCOOwnerCancelsSimultaneousSiblings(t *testing.T) {
	f := newHubTSFixture(t, nil)
	f.hub.SetPendingFeedAdmission(func(context.Context, execution.HubPendingFeed) error { return nil })
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	first, e := f.manager.EnterOrder(symbol, "1m", &strat.EnterReq{CommandID: "own-1", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopBars: 50, OCOGroup: "g"})
	if e != nil || first == nil {
		t.Fatal(first, e)
	}
	second, e := f.manager.EnterOrder(symbol, "1m", &strat.EnterReq{CommandID: "own-2", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 95, StopBars: 50, OCOGroup: "g"})
	if e != nil || second == nil {
		t.Fatal(second, e)
	}
	if hubOCOGroupOf(t, f, first) != "g" || hubOCOGroupOf(t, f, second) != "g" {
		t.Fatal("OCO group did not reach the owner")
	}
	hubSettle(t, f, 85, "first fills, second cancelled", func() bool {
		return hubTSOrder(t, f, first.ID).Enter.Filled == 1 && hubTSOrder(t, f, second.ID).Status == ormo.InOutStatusDelete
	})
	for i := 0; i < 3; i++ {
		hubSettle(t, f, 80, "idle", func() bool { return true })
	}
	if hubHeld(t, f) != 10 || hubTSOrder(t, f, second.ID).Enter.Filled != 0 {
		t.Fatal("exactly one member may fill", hubHeld(t, f))
	}
}

// A breakout bracket gapping through the buy-stop fills one leg only; the
// robot's post-fill cancel handles the resting sell-stop.
func TestHubOCOBreakoutBracketGapSingleFill(t *testing.T) {
	f := newHubTSFixture(t, nil)
	f.hub.SetPendingFeedAdmission(func(context.Context, execution.HubPendingFeed) error { return nil })
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	up, e := f.manager.EnterOrder(symbol, "1m", &strat.EnterReq{CommandID: "br-up", Amount: 1, Stop: 105, StopBars: 50, OCOGroup: "br"})
	if e != nil || up == nil {
		t.Fatal(up, e)
	}
	down, e := f.manager.EnterOrder(symbol, "1m", &strat.EnterReq{CommandID: "br-down", Short: true, Amount: 1, Stop: 95, StopBars: 50, OCOGroup: "br"})
	if e != nil || down == nil {
		t.Fatal(down, e)
	}
	hubSettle(t, f, 120, "up fills, down cancelled", func() bool {
		return hubTSOrder(t, f, up.ID).Enter.Filled == 1 && hubTSOrder(t, f, down.ID).Status == ormo.InOutStatusDelete
	})
	for i := 0; i < 3; i++ {
		hubSettle(t, f, 90, "idle", func() bool { return true })
	}
	if hubHeld(t, f) != 10 || hubTSOrder(t, f, down.ID).Enter.Filled != 0 {
		t.Fatal("bracket traded both legs", hubHeld(t, f))
	}
}
