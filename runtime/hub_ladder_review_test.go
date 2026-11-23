package runtime

import (
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestHubLadderShortTwoRungsAndBreakeven(t *testing.T) {
	f := newHubTSFixture(t, nil)
	od, e := f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", &strat.EnterReq{CommandID: "lad-short", Short: true, Amount: 1, StopLoss: 110, TakeProfits: []strat.TakeProfitTarget{
		{Price: 90, ExitRate: 0.5, Tag: "tp2"},
		{Price: 95, ExitRate: 0.5, MoveSLToBreakeven: true, Tag: "tp1"},
	}})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	if got := hubSpec(hubPendingOf(t, f, od), execution.TakeProfitProtection); !got.Level.Equal(decimal.NewFromInt(95)) {
		t.Fatalf("first short rung: %+v", got)
	}
	hubSettle(t, f, 94, "rung 1 and rung 2 install", func() bool {
		return hubHeld(t, f) == -5 && hubSpec(hubPendingOf(t, f, od), execution.TakeProfitProtection).Level.Equal(decimal.NewFromInt(90))
	})
	var sl decimal.Decimal
	hubSettle(t, f, 94, "stop at break-even", func() bool {
		sl = hubSpec(hubPendingOf(t, f, od), execution.StopLossProtection).Level
		return sl.Equal(decimal.NewFromInt(100))
	})
	// The stop must move DOWN to the average and never loosen again.
	for _, price := range []int64{98, 96, 99} {
		hubSettle(t, f, price, "idle", func() bool { return true })
		if got := hubSpec(hubPendingOf(t, f, od), execution.StopLossProtection).Level; got.GreaterThan(decimal.NewFromInt(100)) || hubHeld(t, f) != -5 {
			t.Fatalf("stop loosened or lot closed at %d: %s held %d", price, got, hubHeld(t, f))
		}
	}
	// 101 is below the original stop (110) but above break-even.
	hubSettle(t, f, 101, "break-even stop closes the remainder", func() bool { return hubHeld(t, f) == 0 })
	if got := hubTSOrder(t, f, od.ID); got.Status != ormo.InOutStatusFullExit {
		t.Fatal("not closed", got.Status)
	}
}

// A non-final rung below one quantity step must not leave the lot without a
// take-profit: its share is merged into the next rung.
func TestHubLadderTinyRungKeepsTakeProfit(t *testing.T) {
	f := newHubTSFixture(t, nil)
	od, e := f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", &strat.EnterReq{CommandID: "lad-tiny", Amount: 1, StopLoss: 90, TakeProfits: []strat.TakeProfitTarget{
		{Price: 105, ExitRate: 0.05},
		{Price: 110, ExitRate: 0.95},
	}})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	// 10 steps held: 0.05 -> 0 steps. The 110 rung must take over with the whole position.
	hubSettle(t, f, 100, "second rung installed", func() bool {
		tp := hubSpec(hubPendingOf(t, f, od), execution.TakeProfitProtection)
		return tp.Level.Equal(decimal.NewFromInt(110)) && tp.Rate.Equal(decimal.NewFromInt(1))
	})
	hubSettle(t, f, 111, "closed at the merged rung", func() bool { return hubHeld(t, f) == 0 })
}
