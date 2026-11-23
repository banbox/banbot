package runtime

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

// ownerRefusals counts every failed hub operation, whatever its reply, so a test
// can prove a restart did not resend a dropped command.
func countOwnerRefusals(f *hubTSFixture) *atomic.Int32 {
	var n atomic.Int32
	f.server.SetErrorObserver(func(string, error) { n.Add(1) })
	return &n
}

// assertDroppedCleanly checks the robot-side contract shared by every dropped
// entry: (nil, nil), no sticky error, nothing at the venue.
func assertDroppedCleanly(t *testing.T, f *hubTSFixture, symbol *orm.ExSymbol, req *strat.EnterReq) {
	t.Helper()
	fills := f.owner.adapter.Metrics().Fills
	od, err := f.manager.EnterOrder(symbol, "ws", req)
	if err != nil || od != nil {
		t.Fatalf("rejected entry must be (nil, nil), got %v, %v", od, err)
	}
	if got := f.owner.adapter.Metrics().Fills; got != fills {
		t.Fatalf("rejected entry reached the venue: fills %d -> %d", fills, got)
	}
	if e := f.manager.LastError(); e != nil {
		t.Fatalf("rejection left a sticky error: %v", e)
	}
}

// assertSurvivesAndRestarts then proves the robot keeps working, and that a
// restart with the rejected command in the journal neither panics nor resends.
func assertSurvivesAndRestarts(t *testing.T, f *hubTSFixture, symbol *orm.ExSymbol, rejected *strat.EnterReq, refusals *atomic.Int32) {
	t.Helper()
	good, err := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "ok", Tag: "ok", Amount: 1})
	if err != nil || good == nil || good.Enter.Filled != 1 {
		t.Fatalf("valid entry after a rejection failed: %+v, %v", good, err)
	}
	if e := f.manager.LastError(); e != nil {
		t.Fatal(e)
	}
	before, fills := refusals.Load(), f.owner.adapter.Metrics().Fills
	if e := f.manager.CleanUp(); e != nil {
		t.Fatal(e)
	}
	restarted, rerr := biz.NewHubOrderMgr(f.rt.BizDeps(), f.bridge, nil)
	if rerr != nil {
		t.Fatalf("restart with a rejected command in the journal failed: %v", rerr)
	}
	defer restarted.CleanUp()
	if got := refusals.Load(); got != before {
		t.Fatalf("restart resent the rejected command: owner refusals %d -> %d", before, got)
	}
	// The replay of the rejected identity is still a quiet drop, not a resend.
	od, err := restarted.EnterOrder(symbol, "ws", rejected)
	if err != nil || od != nil {
		t.Fatalf("replay of a rejected entry after restart: %v, %v", od, err)
	}
	if got := refusals.Load(); got != before || f.owner.adapter.Metrics().Fills != fills {
		t.Fatalf("replay reached the owner: refusals %d -> %d, fills %d -> %d", before, got, fills, f.owner.adapter.Metrics().Fills)
	}
	if again, err := restarted.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "after-restart", Tag: "again", Amount: 1}); err != nil || again == nil {
		t.Fatalf("entry after restart: %v, %v", again, err)
	}
	if e := restarted.LastError(); e != nil {
		t.Fatal(e)
	}
}

// The robot rounds limits to its own tick; a robot whose descriptor is finer
// than the owner's (here 0.1 against 1) still produces an off-tick limit.
func TestHubRobotDropsEntryWithOffTickLimitAndRestarts(t *testing.T) {
	f := newHubTSFixture(t, nil)
	instrument := f.owner.bridge.Instruments["BTC"]
	instrument.PriceTick = decimal.RequireFromString("0.1")
	f.owner.bridge.Instruments["BTC"] = instrument
	refusals := countOwnerRefusals(f)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	req := &strat.EnterReq{CommandID: "off-tick", Tag: "tick", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 110.5}
	assertDroppedCleanly(t, f, symbol, req)
	if refusals.Load() != 1 {
		t.Fatalf("the owner should have refused exactly once, got %d", refusals.Load())
	}
	assertSurvivesAndRestarts(t, f, symbol, req, refusals)
}

// A strategy-gross refusal is a normal "not now" answer: it must not stop the
// robot, and the allowance frees up again once the position is closed.
func TestHubRobotDropsEntryOverRiskLimitAndRestarts(t *testing.T) {
	f := newHubTSFixture(t, nil)
	refusals := countOwnerRefusals(f)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	// The robot admits each entry on its own (1000 notional allowance); the
	// owner's gross budget (1000) covers only one of the two.
	first, err := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "big-1", Tag: "big", Amount: 9})
	if err != nil || first == nil {
		t.Fatalf("first entry: %v, %v", first, err)
	}
	req := &strat.EnterReq{CommandID: "big-2", Tag: "big", Amount: 9}
	assertDroppedCleanly(t, f, symbol, req)
	if refusals.Load() != 1 {
		t.Fatalf("the owner should have refused exactly once, got %d", refusals.Load())
	}
	if _, e := f.manager.ExitOrder(first, &strat.ExitReq{CommandID: "close-1", Tag: "done"}); e != nil {
		t.Fatal(e)
	}
	assertSurvivesAndRestarts(t, f, symbol, req, refusals)
}

// An owner refusal that is not classified keeps failing closed: the entry is an
// error (never a quiet drop), nothing is marked rejected, and the frozen command
// keeps blocking newer ones.
func TestHubRobotUnclassifiedOwnerRefusalStillFailsClosed(t *testing.T) {
	f := newHubTSFixture(t, nil)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	// Another writer takes over the strategy: the robot's epoch is now stale,
	// which the owner reports with the generic failure.
	lease, err := f.client.WriterLease(context.Background(), f.owner.key, "ts")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.client.ClaimStrategyWriter(context.Background(), f.owner.key, execution.StrategyWriterClaim{Strategy: "ts", WriterID: "usurper", ExpectedEpoch: lease.Epoch}); err != nil {
		t.Fatal(err)
	}
	fills := f.owner.adapter.Metrics().Fills
	od, e := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "stale", Tag: "stale", Amount: 1})
	if e == nil || od != nil {
		t.Fatalf("an unclassified refusal must be an error, got %v, %v", od, e)
	}
	// The same identity is retried (not dropped) and a new one is blocked behind it.
	if od, e = f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "stale", Tag: "stale", Amount: 1}); e == nil || od != nil {
		t.Fatalf("frozen command was dropped or accepted: %v, %v", od, e)
	}
	if od, e = f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "next", Tag: "next", Amount: 1}); e == nil || od != nil {
		t.Fatalf("a newer command passed a refused frozen one: %v, %v", od, e)
	}
	if f.owner.adapter.Metrics().Fills != fills {
		t.Fatal("refused command reached the venue")
	}
}
