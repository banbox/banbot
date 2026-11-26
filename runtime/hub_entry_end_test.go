package runtime

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

// An accepted entry that dies unfilled, or an entry the Hub manager drops, ends in
// one OdChgExitFill with a Delete order and a LocalOrderMgr exit tag, so a
// strategy that waits for it (pending flag) is told what the backtest tells it.

type endEvent struct {
	Kind   int
	ID     int64
	Status int64
	Tag    string
	Msg    string
	Code   string
	Filled float64
}

type endLog struct {
	mu     sync.Mutex
	events []endEvent
}

func hubRecordEnds(f *hubTSFixture) *endLog {
	l := &endLog{}
	f.rt.BizDeps().Strategies.AddOdSub("*", func(_ string, od *ormo.InOutOrder, evt int) {
		e := endEvent{Kind: evt, ID: od.ID, Status: od.Status, Tag: od.ExitTag}
		if od.Enter != nil {
			e.Filled = od.Enter.Filled
		}
		if v, ok := od.Info[ormo.KeyStatusMsg].(string); ok {
			e.Msg = v
		}
		if v, ok := od.Info["reject_code"].(string); ok {
			e.Code = v
		}
		l.mu.Lock()
		l.events = append(l.events, e)
		l.mu.Unlock()
	})
	return l
}

func (l *endLog) exitFills(id int64) []endEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []endEvent
	for _, e := range l.events {
		if e.Kind == strat.OdChgExitFill && (id == 0 || e.ID == id) {
			out = append(out, e)
		}
	}
	return out
}

func (l *endLog) changed(id int64) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.events {
		if e.Kind == strat.OdChgOrderChanged && e.ID == id {
			n++
		}
	}
	return n
}

// waitExitFill polls the robot until the order has an ExitFill.
func hubWaitExitFill(t *testing.T, f *hubTSFixture, l *endLog, id int64) endEvent {
	t.Helper()
	for i := 0; i < 60; i++ {
		if err := f.manager.UpdateByDataSeries(nil, nil); err != nil {
			t.Fatal(err)
		}
		if got := l.exitFills(id); len(got) > 0 {
			return got[0]
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no OdChgExitFill for order", id)
	return endEvent{}
}

// hubIdle runs polls for a while so a (wrongly) duplicated callback would show.
func hubIdle(t *testing.T, f *hubTSFixture) {
	t.Helper()
	for i := 0; i < 8; i++ {
		if err := f.manager.UpdateByDataSeries(nil, nil); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Millisecond)
	}
}

func hubExpireFeed(t *testing.T, f *hubTSFixture, bars int64) {
	t.Helper()
	base := time.Now().UnixMilli()
	for i := int64(1); i <= bars; i++ {
		if _, err := f.owner.rt.SharedExecution().ObservePendingFeed(context.Background(), execution.PendingFeedObservation{Feed: "1/1m", EndMS: base + i*60000, Closed: true}); err != nil {
			t.Fatal(err)
		}
		if err := f.owner.rt.SharedExecution().AdvancePendingExecution(context.Background(), time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
}

func hubEndFixture(t *testing.T) *hubTSFixture {
	t.Helper()
	f := newHubTSFixture(t, nil)
	f.hub.SetPendingFeedAdmission(func(context.Context, execution.HubPendingFeed) error { return nil })
	return f
}

var endSymbol = &orm.ExSymbol{ID: 1, Symbol: "BTC"}

func TestHubEntryExpiryDeliversExitFillOnceAcrossRestart(t *testing.T) {
	f := hubEndFixture(t)
	ends := hubRecordEnds(f)
	od, e := f.manager.EnterOrder(endSymbol, "1m", &strat.EnterReq{CommandID: "expires", Tag: "x", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 50, StopBars: 2})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	hubExpireFeed(t, f, 2)
	got := hubWaitExitFill(t, f, ends, od.ID)
	if got.Status != ormo.InOutStatusDelete || got.Tag != core.ExitTagEntExp || got.Filled != 0 || got.Msg == "" {
		t.Fatalf("expiry must report a Delete order tagged ent_expire: %+v", got)
	}
	hubIdle(t, f)
	if n := len(ends.exitFills(od.ID)); n != 1 {
		t.Fatal("expiry delivered", n, "ExitFill callbacks")
	}
	if n := ends.changed(od.ID); n != 0 {
		t.Fatal("the dead order must not also raise OrderChanged", n)
	}
	// The registry view is consistent: Delete, tagged, never offered to the job as open.
	view := hubTSOrder(t, f, od.ID)
	if view.Status != ormo.InOutStatusDelete || view.ExitTag != core.ExitTagEntExp || view.Exit == nil {
		t.Fatal("registry view of the expired entry", view.Status, view.ExitTag)
	}
	// A restart replays nothing.
	f.manager.CleanUp()
	restarted, err := biz.NewHubOrderMgr(f.rt.BizDeps(), f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.CleanUp()
	job := &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy"}, Symbol: endSymbol, TimeFrame: "1m"}
	job.BindRuntimeState(f.rt.Strategies, f.rt.Core, f.rt.Clock)
	if err = restarted.BindJobs([]*strat.StratJob{job}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		_ = restarted.UpdateByDataSeries(nil, nil)
		time.Sleep(30 * time.Millisecond)
	}
	if n := len(ends.exitFills(od.ID)); n != 1 {
		t.Fatal("restart re-delivered the expiry", n)
	}
}

// The delivery is journaled together with the flag that produced it: a crash
// before it ran replays it with the same ID after restart, once.
func TestHubEntryEndDeliveryIsDurableUntilJobsBound(t *testing.T) {
	f := hubEndFixture(t)
	od, e := f.manager.EnterOrder(endSymbol, "1m", &strat.EnterReq{CommandID: "durable", Tag: "x", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 50, StopBars: 2})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	f.manager.CleanUp()
	body, err := os.ReadFile(f.bridge.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	var journal map[string]json.RawMessage
	if err = json.Unmarshal(body, &journal); err != nil {
		t.Fatal(err)
	}
	dead := od.Clone()
	dead.Status, dead.ExitTag = ormo.InOutStatusDelete, core.ExitTagEntExp
	journal["PendingDeliveries"], _ = json.Marshal([]map[string]any{{"Order": dead, "Kind": strat.OdChgExitFill, "ID": "end/replayed"}})
	body, _ = json.Marshal(journal)
	if err = os.WriteFile(f.bridge.StatePath, body, 0600); err != nil {
		t.Fatal(err)
	}
	restarted, err := biz.NewHubOrderMgr(f.rt.BizDeps(), f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.CleanUp()
	var mu sync.Mutex
	var tags []string
	job := &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy", OnOrderChange: func(_ *strat.StratJob, view *ormo.InOutOrder, kind int) {
		if kind == strat.OdChgExitFill {
			mu.Lock()
			tags = append(tags, view.ExitTag)
			mu.Unlock()
		}
	}}, Symbol: endSymbol, TimeFrame: "1m"}
	job.BindRuntimeState(f.rt.Strategies, f.rt.Core, f.rt.Clock)
	if err = restarted.BindJobs([]*strat.StratJob{job}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		_ = restarted.UpdateByDataSeries(nil, nil)
		time.Sleep(30 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(tags) != 1 || tags[0] != core.ExitTagEntExp {
		t.Fatal("replayed delivery", tags)
	}
}

func TestHubStrategyCancelOfUnfilledEntryDeliversItsTag(t *testing.T) {
	f := hubEndFixture(t)
	ends := hubRecordEnds(f)
	od, e := f.manager.EnterOrder(endSymbol, "1m", &strat.EnterReq{CommandID: "to-cancel", Tag: "x", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 50, StopBars: 20})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	if _, e = f.manager.ExitOrder(od, &strat.ExitReq{CommandID: "cancel-it", Tag: "my_cancel"}); e != nil {
		t.Fatal(e)
	}
	got := hubWaitExitFill(t, f, ends, od.ID)
	if got.Status != ormo.InOutStatusDelete || got.Tag != "my_cancel" {
		t.Fatalf("strategy cancel must carry its own tag: %+v", got)
	}
	hubIdle(t, f)
	if n := len(ends.exitFills(od.ID)); n != 1 {
		t.Fatal("cancel delivered", n, "ExitFill callbacks")
	}
}

func TestHubOCOLoserDeliversOCOCancelTag(t *testing.T) {
	f := hubEndFixture(t)
	ends := hubRecordEnds(f)
	first, e := f.manager.EnterOrder(endSymbol, "1m", &strat.EnterReq{CommandID: "w-1", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopBars: 50, OCOGroup: "g"})
	if e != nil || first == nil {
		t.Fatal(first, e)
	}
	second, e := f.manager.EnterOrder(endSymbol, "1m", &strat.EnterReq{CommandID: "w-2", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 95, StopBars: 50, OCOGroup: "g"})
	if e != nil || second == nil {
		t.Fatal(second, e)
	}
	hubSettle(t, f, 85, "winner fills", func() bool { return hubTSOrder(t, f, first.ID).Enter.Filled == 1 })
	got := hubWaitExitFill(t, f, ends, second.ID)
	if got.Status != ormo.InOutStatusDelete || got.Tag != biz.ExitTagOCOCancel {
		t.Fatalf("OCO loser must be tagged oco_cancel: %+v", got)
	}
	for i := 0; i < 3; i++ {
		hubSettle(t, f, 80, "idle", func() bool { return true })
	}
	if n := len(ends.exitFills(second.ID)); n != 1 {
		t.Fatal("OCO loser delivered", n, "ExitFill callbacks")
	}
	if n := len(ends.exitFills(first.ID)); n != 0 {
		t.Fatal("the winner must not get an ExitFill", n)
	}
}

func TestHubEntryDroppedBelowMinimumDeliversExitFill(t *testing.T) {
	f := newHubMinimumFixture(t, 10)
	ends := hubRecordEnds(f)
	od, err := f.manager.EnterOrder(endSymbol, "ws", &strat.EnterReq{CommandID: "tiny", Tag: "tiny", Amount: 0.5})
	if err != nil || od != nil {
		t.Fatal(od, err)
	}
	got := ends.exitFills(0)
	if len(got) != 1 || got[0].Status != ormo.InOutStatusDelete || got[0].Tag != core.ExitTagFatalErr || got[0].Code != "size_rejected" || got[0].Msg == "" || got[0].ID <= 0 {
		t.Fatalf("size drop must report one Delete ExitFill with a reason: %+v", got)
	}
	// Replaying the same request id does not queue a second callback in the outbox.
	hubIdle(t, f)
	if n := len(ends.exitFills(0)); n != 1 {
		t.Fatal("dropped entry delivered", n)
	}
}

func TestHubEntryRefusedByOwnerDeliversExitFill(t *testing.T) {
	f := newHubTSFixture(t, nil)
	ends := hubRecordEnds(f)
	first, err := f.manager.EnterOrder(endSymbol, "ws", &strat.EnterReq{CommandID: "big-1", Tag: "big", Amount: 9})
	if err != nil || first == nil {
		t.Fatal(first, err)
	}
	od, err := f.manager.EnterOrder(endSymbol, "ws", &strat.EnterReq{CommandID: "big-2", Tag: "big2", Amount: 9})
	if err != nil || od != nil {
		t.Fatal(od, err)
	}
	got := ends.exitFills(0)
	if len(got) != 1 || got[0].Status != ormo.InOutStatusDelete || got[0].Tag != core.ExitTagForceExit || got[0].Code != execution.RejectRiskLimit {
		t.Fatalf("owner refusal must report one Delete ExitFill: %+v", got)
	}
	// A retry of the refused identity and a restart stay quiet.
	if od, err = f.manager.EnterOrder(endSymbol, "ws", &strat.EnterReq{CommandID: "big-2", Tag: "big2", Amount: 9}); err != nil || od != nil {
		t.Fatal(od, err)
	}
	f.manager.CleanUp()
	restarted, rerr := biz.NewHubOrderMgr(f.rt.BizDeps(), f.bridge, nil)
	if rerr != nil {
		t.Fatal(rerr)
	}
	defer restarted.CleanUp()
	job := &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy"}, Symbol: endSymbol, TimeFrame: "ws"}
	job.BindRuntimeState(f.rt.Strategies, f.rt.Core, f.rt.Clock)
	_ = restarted.BindJobs([]*strat.StratJob{job})
	time.Sleep(250 * time.Millisecond)
	if n := len(ends.exitFills(0)); n != 1 {
		t.Fatal("owner refusal re-delivered", n)
	}
}

// LocalOrderMgr refuses max_open_orders silently: no order, no callback.
func TestHubEntryRefusedByAdmissionStaysSilent(t *testing.T) {
	r := newHubAdmissionRig(t, &config.Config{MaxOpenOrders: 2})
	ends := hubRecordEnds(r.f)
	if err := r.peerEnter(t, "p1", "ws", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if !r.enter(t, "ws", "r1") {
		t.Fatal("first entry refused")
	}
	if r.enter(t, "ws", "r2") { // owner-side account limit across robots
		t.Fatal("limit not enforced")
	}
	r.peerCancel(t, "p1")
	if !r.enter(t, "ws", "r3") {
		t.Fatal("not recovered")
	}
	if r.enter(t, "ws", "r4") { // robot-side limit
		t.Fatal("robot limit not enforced")
	}
	time.Sleep(250 * time.Millisecond)
	if got := ends.exitFills(0); len(got) != 0 {
		t.Fatalf("admission refusals must not call back: %+v", got)
	}
}

// A callback that opens a new order from inside the ExitFill gets it submitted:
// through the job queue (flushed like other callback requests) and by calling the
// manager directly.
func TestHubEntryEndCallbackCanReenter(t *testing.T) {
	f := hubEndFixture(t)
	f.manager.CleanUp()
	deps := f.rt.BizDeps()
	var mu sync.Mutex
	var reentered int
	mgr, err := biz.NewHubOrderMgr(deps, f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.CleanUp() })
	f.manager = mgr
	job := &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy"}, Symbol: endSymbol, TimeFrame: "1m", Account: "default", CloseLong: true, CloseShort: true}
	job.Strat.OnOrderChange = func(j *strat.StratJob, od *ormo.InOutOrder, kind int) {
		if kind == strat.OdChgExitFill && od.ExitTag == core.ExitTagEntExp && od.Status == ormo.InOutStatusDelete {
			mu.Lock()
			reentered++
			mu.Unlock()
			var e *errs.Error = j.OpenOrder(&strat.EnterReq{Tag: "reentry", Amount: 1})
			if e != nil {
				t.Error(e)
			}
		}
	}
	job.BindRuntimeState(f.rt.Strategies, f.rt.Core, f.rt.Clock)
	job.BindRuntimeMarket(f.rt.Market.Prices, f.rt.Clock)
	if err = mgr.BindJobs([]*strat.StratJob{job}); err != nil {
		t.Fatal(err)
	}
	od, e := mgr.EnterOrder(endSymbol, "1m", &strat.EnterReq{CommandID: "will-expire", Tag: "x", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 50, StopBars: 2})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	hubExpireFeed(t, f, 2)
	deadline := time.Now().Add(5 * time.Second)
	for {
		_ = mgr.UpdateByDataSeries(nil, nil)
		if hubHeld(t, f) == 10 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the order opened from the ExitFill callback was never submitted")
		}
		time.Sleep(50 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if reentered != 1 {
		t.Fatal("callback ran", reentered, "times")
	}
}

// A dropped entry's callback may itself queue a request; it is flushed by the
// enclosing drain without recursion or deadlock.
func TestHubDroppedEntryCallbackRequestIsFlushed(t *testing.T) {
	f := newHubMinimumFixture(t, 10)
	f.manager.CleanUp()
	deps := f.rt.BizDeps()
	mgr, err := biz.NewHubOrderMgr(deps, f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.CleanUp() })
	var mu sync.Mutex
	drops := 0
	job := &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy"}, Symbol: endSymbol, TimeFrame: "1m", Account: "default", CloseLong: true, CloseShort: true}
	job.Strat.OnOrderChange = func(j *strat.StratJob, od *ormo.InOutOrder, kind int) {
		if kind == strat.OdChgExitFill && od.Status == ormo.InOutStatusDelete && od.EnterTag == "tiny" {
			mu.Lock()
			drops++
			mu.Unlock()
			_ = j.OpenOrder(&strat.EnterReq{Tag: "retry-bigger", Amount: 1})
		}
	}
	job.BindRuntimeState(f.rt.Strategies, f.rt.Core, f.rt.Clock)
	job.BindRuntimeMarket(f.rt.Market.Prices, f.rt.Clock)
	if err = mgr.BindJobs([]*strat.StratJob{job}); err != nil {
		t.Fatal(err)
	}
	if e := job.OpenOrder(&strat.EnterReq{Tag: "tiny", Amount: 0.5}); e != nil {
		t.Fatal(e)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, _, e := mgr.ProcessOrders(job); e != nil {
			t.Error(e)
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("dropped-entry callback deadlocked ProcessOrders")
	}
	mu.Lock()
	defer mu.Unlock()
	if drops != 1 || hubHeld(t, f) != 10 {
		t.Fatal("request queued from the dropped-entry callback was not submitted", drops, hubHeld(t, f))
	}
}
