package runtime

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

// exitTagRig records the OnOrderChange exit callbacks and reads archived rows.
type exitTagRig struct {
	f      *hubTSFixture
	mu     sync.Mutex
	exits  []*ormo.InOutOrder
	taskID int64
}

func newExitTagRig(t *testing.T, bindDB bool) *exitTagRig {
	t.Helper()
	r := &exitTagRig{}
	r.f = newHubTSFixture(t, func(od *ormo.InOutOrder, isEnter bool) {
		if isEnter {
			return
		}
		r.mu.Lock()
		r.exits = append(r.exits, od.Clone())
		r.mu.Unlock()
	})
	if bindDB {
		r.bind(t)
	}
	return r
}

func (r *exitTagRig) bind(t *testing.T) {
	t.Helper()
	r.f.rt.Orders.BindTradesPath(filepath.Join(t.TempDir(), "orders.db"))
	biz.EnsureTimeSeriesOrderTask(r.f.rt.BizDeps())
	r.taskID = r.f.rt.Orders.GetTaskID("default")
	if r.taskID < 0 {
		t.Fatal("no trades task")
	}
}

func (r *exitTagRig) history(t *testing.T) []*ormo.InOutOrder {
	t.Helper()
	sess, conn, err := r.f.rt.Orders.Conn(false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rows, err := sess.GetOrders(ormo.GetOrdersArgs{TaskID: r.taskID, Status: 2})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func (r *exitTagRig) lastExit() *ormo.InOutOrder {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.exits) == 0 {
		return nil
	}
	return r.exits[len(r.exits)-1]
}

// expect asserts the tag in the final exit callback, the registry view and,
// when a database is bound, the archived row.
func (r *exitTagRig) expect(t *testing.T, od *ormo.InOutOrder, want string, archived bool) {
	t.Helper()
	reg := hubTSOrder(t, r.f, od.ID)
	if reg.Status != ormo.InOutStatusFullExit || reg.ExitTag != want {
		t.Fatalf("registry view status=%v tag=%q, want closed %q", reg.Status, reg.ExitTag, want)
	}
	cb := r.lastExit()
	if cb == nil || cb.Status != ormo.InOutStatusFullExit || cb.ExitTag != want {
		t.Fatalf("exit callback = %+v, want closed %q", cb, want)
	}
	if !archived {
		return
	}
	rows := r.history(t)
	if len(rows) != 1 || rows[0].ExitTag != want {
		t.Fatalf("archived rows=%d tag=%v, want 1 row %q", len(rows), rows, want)
	}
}

func TestHubExitTagStopLoss(t *testing.T) {
	for name, tag := range map[string]string{"generic": "", "custom": "my_sl"} {
		want := core.ExitTagStopLoss
		if tag != "" {
			want = tag
		}
		t.Run(name, func(t *testing.T) {
			r := newExitTagRig(t, true)
			od, e := r.f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", &strat.EnterReq{CommandID: "sl", Amount: 1, StopLoss: 90, StopLossTag: tag})
			if e != nil || od == nil {
				t.Fatal(od, e)
			}
			hubSettle(t, r.f, 89, "stop-loss", func() bool { return hubHeld(t, r.f) == 0 })
			hubSettle(t, r.f, 89, "archived", func() bool { return len(r.history(t)) == 1 })
			r.expect(t, od, want, true)
		})
	}
}

func TestHubExitTagTakeProfit(t *testing.T) {
	for name, tag := range map[string]string{"generic": "", "custom": "my_tp"} {
		want := core.ExitTagTakeProfit
		if tag != "" {
			want = tag
		}
		t.Run(name, func(t *testing.T) {
			r := newExitTagRig(t, true)
			od, e := r.f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", &strat.EnterReq{CommandID: "tp", Amount: 1, TakeProfit: 110, TakeProfitTag: tag})
			if e != nil || od == nil {
				t.Fatal(od, e)
			}
			hubSettle(t, r.f, 111, "take-profit", func() bool { return hubHeld(t, r.f) == 0 })
			hubSettle(t, r.f, 111, "archived", func() bool { return len(r.history(t)) == 1 })
			r.expect(t, od, want, true)
		})
	}
}

func TestHubExitTagLadderLastRungAndBreakeven(t *testing.T) {
	// Rung 2 closes the lot: final tag is the last reduction's rung tag, the
	// whole sequence is kept in Info.
	r := newExitTagRig(t, true)
	od, e := r.f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", ladderReq("lad-tag", true))
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	hubSettle(t, r.f, 106, "rung 1", func() bool { return hubHeld(t, r.f) == 5 && hubSpecLevel(t, r.f, od) == 110 })
	if mid := hubTSOrder(t, r.f, od.ID); mid.ExitTag != "" || mid.Status != ormo.InOutStatusPartExit {
		t.Fatalf("partial rung must stay open and untagged: %v %q", mid.Status, mid.ExitTag)
	}
	hubSettle(t, r.f, 111, "rung 2", func() bool { return hubHeld(t, r.f) == 0 })
	hubSettle(t, r.f, 111, "archived", func() bool { return len(r.history(t)) == 1 })
	r.expect(t, od, "tp2", true)
	if tags, _ := hubTSOrder(t, r.f, od.ID).Info["exit_tags"].([]string); len(tags) != 2 || tags[0] != "tp1" || tags[1] != "tp2" {
		t.Fatalf("exit_tags = %v", hubTSOrder(t, r.f, od.ID).Info["exit_tags"])
	}

	// The break-even stop closes the remainder: stop-loss tag.
	r2 := newExitTagRig(t, true)
	od2, e := r2.f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", ladderReq("lad-be-tag", true))
	if e != nil || od2 == nil {
		t.Fatal(od2, e)
	}
	hubSettle(t, r2.f, 106, "rung 1", func() bool { return hubHeld(t, r2.f) == 5 && hubSpecLevel(t, r2.f, od2) == 110 })
	hubSettle(t, r2.f, 99, "break-even stop", func() bool { return hubHeld(t, r2.f) == 0 })
	hubSettle(t, r2.f, 99, "archived", func() bool { return len(r2.history(t)) == 1 })
	r2.expect(t, od2, core.ExitTagStopLoss, true)
}

func hubSpecLevel(t *testing.T, f *hubTSFixture, od *ormo.InOutOrder) int64 {
	t.Helper()
	l := hubSpec(hubPendingOf(t, f, od), "take_profit").Level
	return l.IntPart()
}

func TestHubExitTagExplicitExitUnchanged(t *testing.T) {
	r := newExitTagRig(t, true)
	od, e := r.f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", &strat.EnterReq{CommandID: "ex", Amount: 1, StopLoss: 90, TakeProfit: 110, StopLossTag: "my_sl"})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	if _, e = r.f.manager.ExitOrder(od, &strat.ExitReq{CommandID: "ex-close", Tag: "user_exit"}); e != nil {
		t.Fatal(e)
	}
	hubSettle(t, r.f, 100, "archived", func() bool { return len(r.history(t)) == 1 })
	r.expect(t, od, "user_exit", true)
}

func TestHubExitTagSurvivesRestartBeforeArchive(t *testing.T) {
	// No trades database yet: the closed lot cannot be archived, the reason is
	// only in the robot journal. A restarted robot archives it with the tag.
	r := newExitTagRig(t, false)
	od, e := r.f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", ladderReq("lad-restart-tag", false))
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	hubSettle(t, r.f, 106, "rung 1", func() bool { return hubHeld(t, r.f) == 5 && hubSpecLevel(t, r.f, od) == 110 })
	hubSettle(t, r.f, 111, "rung 2", func() bool { return hubHeld(t, r.f) == 0 })
	r.expect(t, od, "tp2", false)
	if e := r.f.manager.CleanUp(); e != nil {
		t.Fatal(e)
	}
	r.bind(t)
	restarted, err := biz.NewHubOrderMgr(r.f.rt.BizDeps(), r.f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.CleanUp()
	r.f.rt.Trading.SetOrderManager("default", restarted)
	r.f.manager = restarted
	hubSettle(t, r.f, 111, "archived after restart", func() bool { return len(r.history(t)) == 1 })
	if rows := r.history(t); rows[0].ExitTag != "tp2" {
		t.Fatalf("restart lost the exit tag: %q", rows[0].ExitTag)
	}
	if reg := hubTSOrder(t, r.f, od.ID); reg.ExitTag != "tp2" {
		t.Fatalf("registry after restart: %q", reg.ExitTag)
	}
}
