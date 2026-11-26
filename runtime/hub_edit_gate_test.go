package runtime

import (
	"context"
	"sync"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

type hubEventLog struct {
	mu    sync.Mutex
	kinds []int
}

func (l *hubEventLog) count(kind int) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, k := range l.kinds {
		if k == kind {
			n++
		}
	}
	return n
}

func hubWatchEvents(f *hubTSFixture) *hubEventLog {
	l := &hubEventLog{}
	f.rt.BizDeps().Strategies.AddOdSub("*", func(_ string, _ *ormo.InOutOrder, evt int) {
		l.mu.Lock()
		l.kinds = append(l.kinds, evt)
		l.mu.Unlock()
	})
	return l
}

// hubEditable returns the registry view of an order bound like a strategy's copy.
func hubEditable(t *testing.T, f *hubTSFixture, id int64) *ormo.InOutOrder {
	t.Helper()
	// The runtime wires the listener in initHubOrderMgr; the fixture builds the manager directly.
	f.rt.Orders.SetSoftwareEditListener(f.manager.EditOrder)
	od := hubTSOrder(t, f, id)
	od.BindState(f.rt.Orders)
	return od
}

// Edits made through the order setters reach the Hub while the entry is still
// unfilled (Status Init), are rounded to the price tick, and never raise an
// "order created" callback.
func TestHubEditUnfilledOrderThroughSettersReachesOwner(t *testing.T) {
	f := newHubTSFixture(t, nil)
	events := hubWatchEvents(f)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, e := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "unfilled", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopLoss: 80})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	od = hubEditable(t, f, od.ID)
	if od.Status != ormo.InOutStatusInit {
		t.Fatal("entry should rest unfilled", od.Status)
	}
	created := events.count(strat.OdChgEnter)
	if created != 1 {
		t.Fatal("the entry must be announced once, got", created)
	}
	// Stop-loss with an off-tick limit: forwarded and rounded to the tick (1).
	if err := od.SetStopLoss(&ormo.ExitTrigger{Price: 85, Limit: 84.4, Tag: "edited"}); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.LastError(); err != nil {
		t.Fatal(err)
	}
	sl := hubSpec(hubPendingOf(t, f, od), execution.StopLossProtection)
	if !sl.Level.Equal(decimal.NewFromInt(85)) || !sl.Limit.Equal(decimal.NewFromInt(84)) {
		t.Fatalf("stop-loss edit not applied on the unfilled lot: %+v", sl)
	}
	if err := od.SetTakeProfit(&ormo.ExitTrigger{Price: 120, Limit: 119.6}); err != nil {
		t.Fatal(err)
	}
	tp := hubSpec(hubPendingOf(t, f, od), execution.TakeProfitProtection)
	if !tp.Level.Equal(decimal.NewFromInt(120)) || !tp.Limit.Equal(decimal.NewFromInt(120)) {
		t.Fatalf("take-profit edit not applied on the unfilled lot: %+v", tp)
	}
	if err := od.SetTrailingStop(1, 0); err != nil {
		t.Fatal(err)
	}
	if tr := hubSpec(hubPendingOf(t, f, od), execution.TrailingProtection); !tr.TrailingPercent.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("trailing edit not applied on the unfilled lot: %+v", tr)
	}
	// The resting limit moves to 95: the next quote at 94 fills it.
	if err := od.SetEnterLimit(95); err != nil {
		t.Fatal(err)
	}
	hubTSObserve(t, f, 94)
	if got := hubTSOrder(t, f, od.ID); got.Enter.Filled != 1 || got.Enter.Average != 94 {
		t.Fatal("limit edit on the unfilled order never reached the owner", got.Enter)
	}
	if n := events.count(strat.OdChgEnter); n != created {
		t.Fatalf("edits fired %d spurious order-created callbacks", n-created)
	}
	if n := events.count(strat.OdChgEnterFill); n != 1 {
		t.Fatal("the fill must still be delivered once, got", n)
	}
}

// An edit of an order whose lot is already gone is a logged no-op, not a stop.
func TestHubEditOfClosedLotDoesNotStopRobot(t *testing.T) {
	f := newHubTSFixture(t, nil)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, e := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "closing", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopLoss: 80})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	stale := hubEditable(t, f, od.ID)
	if _, e = f.manager.ExitOrder(od, &strat.ExitReq{CommandID: "closing-exit", Tag: "done"}); e != nil {
		t.Fatal(e)
	}
	hubSettle(t, f, 100, "lot closed", func() bool { return hubTSOrder(t, f, od.ID).Status != ormo.InOutStatusInit })
	if err := stale.SetStopLoss(&ormo.ExitTrigger{Price: 85}); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.LastError(); err != nil || f.rt.Core.Context().Err() != nil {
		t.Fatal("an edit of a closed lot stopped the robot", err)
	}
}

// An owner failure of an edit is dropped after bounded attempts and never
// stops the robot nor blocks later commands.
func TestHubEditRefusedByOwnerIsDroppedNotFatal(t *testing.T) {
	f := newHubTSFixture(t, nil)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, e := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "refused", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	od = hubEditable(t, f, od.ID)
	lease, err := f.client.WriterLease(context.Background(), f.owner.key, "ts")
	if err != nil {
		t.Fatal(err)
	}
	// Another writer takes the strategy: every command of this robot now fails.
	if _, err = f.client.ClaimStrategyWriter(context.Background(), f.owner.key, execution.StrategyWriterClaim{Strategy: "ts", WriterID: "usurper", ExpectedEpoch: lease.Epoch}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if e := od.SetStopLoss(&ormo.ExitTrigger{Price: float64(80 + i)}); e != nil {
			t.Fatal(e)
		}
		if err = f.manager.LastError(); err != nil || f.rt.Core.Context().Err() != nil {
			t.Fatal("a failed edit stopped the robot", i, err)
		}
	}
}

// A stop entry without a limit stays a plain stop entry under a limit-type
// default order_type; a rejected per-order leverage also applies in dry_run.
func TestHubStopEntryKeepsNoLimitAndLeverageRejected(t *testing.T) {
	f := newHubTSFixture(t, nil)
	deps := f.rt.BizDeps()
	deps.Config = config.NewSnapshot(&config.Config{OrderType: "limit"})
	f.manager.CleanUp()
	manager, err := biz.NewHubOrderMgr(deps, f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { manager.CleanUp() })
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, e := manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "stop-entry", Amount: 1, Stop: 105})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	cond := hubPendingOf(t, f, od).Order.Entry.Conditions
	if !cond.Stop.Equal(decimal.NewFromInt(105)) || cond.Limit.IsPositive() {
		t.Fatalf("stop entry was turned into a stop-limit: %+v", cond)
	}
	if od, e = manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "lev", Amount: 1, Leverage: 7}); e == nil || od != nil {
		t.Fatal("dry_run accepted a per-order leverage the account does not use", od, e)
	}
}

// An entry that passes admission but is then refused (here: below the tradable
// minimum) must not consume the strategy's simultaneous-open slot of the bar.
func TestHubAdmissionRejectedEntryKeepsStrategySlot(t *testing.T) {
	r := newHubAdmissionRig(t, &config.Config{RunPolicy: []*config.RunPolicyConfig{{Name: "legacy", MaxSimulOpen: 2}}})
	if !r.enter(t, "1m", "first") {
		t.Fatal("first entry refused")
	}
	for i := 0; i < 3; i++ {
		if od, err := r.manager.EnterOrder(r.symbol, "1m", &strat.EnterReq{CommandID: "tiny" + string(rune('a'+i)), Amount: 0.00001}); err != nil || od != nil {
			t.Fatal("tiny entry should be rejected quietly", od, err)
		}
	}
	if !r.enter(t, "1m", "second") {
		t.Fatal("rejected entries consumed the strategy's simultaneous-open slots")
	}
	if r.enter(t, "1m", "third") {
		t.Fatal("policy max_simul_open=2 not enforced")
	}
}

// Limits saved in the journal must not brick entries after a config change:
// the Hub adopts the configured account and policy limits.
func TestHubAdmissionConfigChangeIsAdopted(t *testing.T) {
	r := newHubAdmissionRig(t, &config.Config{MaxSimulOpen: 1, RunPolicy: []*config.RunPolicyConfig{{Name: "legacy", MaxSimulOpen: 1}}})
	if !r.enter(t, "1m", "first") {
		t.Fatal("first entry refused")
	}
	if r.enter(t, "1m", "blocked") {
		t.Fatal("max_simul_open=1 not enforced")
	}
	if e := r.manager.CleanUp(); e != nil {
		t.Fatal(e)
	}
	deps := r.f.rt.BizDeps()
	deps.Config = config.NewSnapshot(&config.Config{MaxSimulOpen: 3, RunPolicy: []*config.RunPolicyConfig{{Name: "legacy", MaxSimulOpen: 3}}})
	restarted, err := biz.NewHubOrderMgr(deps, r.f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.CleanUp()
	od, e := restarted.EnterOrder(r.symbol, "1m", &strat.EnterReq{CommandID: "after-config", Amount: 1})
	if e != nil || od == nil {
		t.Fatal("changed admission limits rejected the entry", od, e)
	}
}
