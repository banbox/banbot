package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func hubHeld(t *testing.T, f *hubTSFixture) int64 {
	t.Helper()
	s, err := f.client.Snapshot(context.Background(), f.owner.key)
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, l := range s.Lots {
		n += l.SignedSteps
	}
	return n
}

func hubPendingOf(t *testing.T, f *hubTSFixture, od *ormo.InOutOrder) execution.PendingDefinition {
	t.Helper()
	defs, err := f.client.PendingOrders(context.Background(), f.owner.key)
	if err != nil {
		t.Fatal(err)
	}
	lot, _ := od.Info["shared_lot"].(string)
	for _, d := range defs {
		if string(d.Order.Lot) == lot {
			return d
		}
	}
	t.Fatal("missing pending definition")
	return execution.PendingDefinition{}
}

func hubSpec(d execution.PendingDefinition, kind execution.ProtectionKind) execution.PositionProtection {
	for _, s := range d.Specs {
		if s.Kind == kind {
			return s
		}
	}
	return execution.PositionProtection{Kind: kind}
}

// hubSettle drives price ticks and robot polls until cond holds.
func hubSettle(t *testing.T, f *hubTSFixture, price int64, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 40; i++ {
		hubTSObserve(t, f, price)
		if err := f.manager.UpdateByDataSeries(nil, nil); err != nil {
			t.Fatal(err)
		}
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timeout: ", what)
}

func ladderReq(id string, be bool) *strat.EnterReq {
	return &strat.EnterReq{CommandID: id, Amount: 1, StopLoss: 90, TakeProfits: []strat.TakeProfitTarget{
		{Price: 110, ExitRate: 0.5, Tag: "tp2"},
		{Price: 105, ExitRate: 0.5, MoveSLToBreakeven: be, Tag: "tp1"}, // unsorted on purpose
	}}
}

func TestHubLadderTwoRungsAndBreakeven(t *testing.T) {
	f := newHubTSFixture(t, nil)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, e := f.manager.EnterOrder(symbol, "ws", ladderReq("lad", true))
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	if got := hubSpec(hubPendingOf(t, f, od), execution.TakeProfitProtection); !got.Level.Equal(decimal.NewFromInt(105)) || !got.Rate.Equal(decimal.RequireFromString("0.5")) {
		t.Fatalf("first rung not installed at creation: %+v", got)
	}
	hubSettle(t, f, 106, "rung 1 reduction and rung 2 install", func() bool {
		d := hubPendingOf(t, f, od)
		return hubHeld(t, f) == 5 && hubSpec(d, execution.TakeProfitProtection).Level.Equal(decimal.NewFromInt(110))
	})
	d := hubPendingOf(t, f, od)
	tp, sl := hubSpec(d, execution.TakeProfitProtection), hubSpec(d, execution.StopLossProtection)
	if !tp.Rate.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("last rung must close the rest: %+v", tp)
	}
	if !sl.Level.Equal(decimal.NewFromInt(100)) {
		t.Fatalf("stop not tightened to break-even: %+v", sl)
	}
	// Rung 1 must not fire again at the same price.
	for i := 0; i < 5; i++ {
		hubSettle(t, f, 107, "idle", func() bool { return true })
	}
	if hubHeld(t, f) != 5 {
		t.Fatal("rung 1 re-armed, held", hubHeld(t, f))
	}
	hubSettle(t, f, 111, "rung 2 closes the rest", func() bool { return hubHeld(t, f) == 0 })
	if got := hubTSOrder(t, f, od.ID); got.Status != ormo.InOutStatusFullExit {
		t.Fatal("not closed", got.Status)
	}
}

func TestHubLadderBreakevenStopsRemainder(t *testing.T) {
	f := newHubTSFixture(t, nil)
	od, e := f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", ladderReq("lad-be", true))
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	hubSettle(t, f, 106, "rung 2 installed", func() bool {
		return hubHeld(t, f) == 5 && hubSpec(hubPendingOf(t, f, od), execution.TakeProfitProtection).Level.Equal(decimal.NewFromInt(110))
	})
	// 99 is above the original stop (90) but below break-even (100).
	hubSettle(t, f, 99, "break-even stop", func() bool { return hubHeld(t, f) == 0 })
}

func TestHubLadderRestartResumesNextRung(t *testing.T) {
	f := newHubTSFixture(t, nil)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, e := f.manager.EnterOrder(symbol, "ws", ladderReq("lad-restart", false))
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	if e := f.manager.CleanUp(); e != nil {
		t.Fatal(e)
	}
	// Rung 1 executes while no robot is running.
	hubTSObserve(t, f, 106)
	hubTSObserve(t, f, 106)
	if hubHeld(t, f) != 5 {
		t.Fatal("rung 1 did not execute offline", hubHeld(t, f))
	}
	restarted, err := biz.NewHubOrderMgr(f.rt.BizDeps(), f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.CleanUp()
	f.rt.Trading.SetOrderManager("default", restarted)
	f.manager = restarted
	hubSettle(t, f, 106, "rung 2 after restart", func() bool {
		return hubSpec(hubPendingOf(t, f, od), execution.TakeProfitProtection).Level.Equal(decimal.NewFromInt(110))
	})
	for i := 0; i < 5; i++ {
		hubSettle(t, f, 106, "idle", func() bool { return true })
	}
	if hubHeld(t, f) != 5 {
		t.Fatal("rung 1 was reinstalled after restart", hubHeld(t, f))
	}
	// A second restart must not advance again.
	restarted.CleanUp()
	again, err := biz.NewHubOrderMgr(f.rt.BizDeps(), f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer again.CleanUp()
	f.rt.Trading.SetOrderManager("default", again)
	f.manager = again
	hubSettle(t, f, 111, "rung 2 closes the rest", func() bool { return hubHeld(t, f) == 0 })
}

func TestHubLadderRateRungReresolvedFromFill(t *testing.T) {
	f := newHubTSFixture(t, nil)
	f.hub.SetPendingFeedAdmission(func(context.Context, execution.HubPendingFeed) error { return nil })
	od, e := f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "1m", &strat.EnterReq{CommandID: "rate", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 95, StopBars: 5,
		TakeProfits: []strat.TakeProfitTarget{{Rate: 0.1, ExitRate: 1}}})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	if got := hubSpec(hubPendingOf(t, f, od), execution.TakeProfitProtection).Level; got.Sub(decimal.RequireFromString("104.5")).Abs().GreaterThan(decimal.RequireFromString("0.0001")) {
		t.Fatalf("pre-fill level from the limit reference: %s", got)
	}
	hubSettle(t, f, 90, "filled and re-resolved", func() bool {
		return hubHeld(t, f) == 10 && hubSpec(hubPendingOf(t, f, od), execution.TakeProfitProtection).Level.Sub(decimal.RequireFromString("99")).Abs().LessThan(decimal.RequireFromString("0.0001"))
	})
}

func TestHubOCOCancelsUnfilledSibling(t *testing.T) {
	f := newHubTSFixture(t, nil)
	f.hub.SetPendingFeedAdmission(func(context.Context, execution.HubPendingFeed) error { return nil })
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	buy, e := f.manager.EnterOrder(symbol, "1m", &strat.EnterReq{CommandID: "oco-b", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopBars: 50, OCOGroup: "g"})
	if e != nil || buy == nil {
		t.Fatal(buy, e)
	}
	sell, e := f.manager.EnterOrder(symbol, "1m", &strat.EnterReq{CommandID: "oco-s", Short: true, Amount: 1, OrderType: core.OrderTypeLimit, Limit: 110, StopBars: 50, OCOGroup: "g"})
	if e != nil || sell == nil {
		t.Fatal(sell, e)
	}
	hubSettle(t, f, 89, "buy fills, sell cancelled", func() bool {
		return hubTSOrder(t, f, buy.ID).Enter.Filled == 1 && hubTSOrder(t, f, sell.ID).Status == ormo.InOutStatusDelete
	})
	got := hubTSOrder(t, f, sell.ID)
	if got.ExitTag != biz.ExitTagOCOCancel || got.Enter.Filled != 0 {
		t.Fatalf("sibling view: tag=%q filled=%v", got.ExitTag, got.Enter.Filled)
	}
	// The cancelled sibling can no longer fill.
	for i := 0; i < 3; i++ {
		hubSettle(t, f, 111, "idle", func() bool { return true })
	}
	if hubHeld(t, f) != 10 || hubTSOrder(t, f, sell.ID).Enter.Filled != 0 {
		t.Fatal("cancelled sibling filled", hubHeld(t, f))
	}
}
