package runtime

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

type fakeNotifySink struct {
	mu   sync.Mutex
	msgs []map[string]interface{}
}

func (s *fakeNotifySink) send(msg map[string]interface{}) {
	s.mu.Lock()
	s.msgs = append(s.msgs, msg)
	s.mu.Unlock()
}

func (s *fakeNotifySink) byType(typ string) []map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	var res []map[string]interface{}
	for _, m := range s.msgs {
		if m["type"] == typ {
			res = append(res, m)
		}
	}
	return res
}

// newNotifyHubFixture builds the Hub fixture with the production callback bound
// to a fake notification sink.
func newNotifyHubFixture(t *testing.T, sink func(map[string]interface{})) *hubTSFixture {
	t.Helper()
	var f *hubTSFixture
	f = newHubTSFixture(t, func(od *ormo.InOutOrder, isEnter bool) { f.rt.OrderNotifyCallback()(od, isEnter) })
	notifiersMu.Lock()
	notifierSink[f.rt] = sink
	notifiersMu.Unlock()
	return f
}

func TestHubFillSendsOneEntryAndOneExitMessage(t *testing.T) {
	sink := &fakeNotifySink{}
	f := newNotifyHubFixture(t, sink.send)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, err := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "open", Tag: "remote"})
	if err != nil || od == nil {
		t.Fatal(od, err)
	}
	if !f.rt.Notifier().Flush(5 * time.Second) {
		t.Fatal("flush timed out")
	}
	if got := len(sink.byType(rpc.MsgTypeEntry)); got != 1 {
		t.Fatalf("entry messages = %d, want 1", got)
	}
	if got := len(sink.byType(rpc.MsgTypeExit)); got != 0 {
		t.Fatalf("exit messages before exit = %d", got)
	}
	closedOd, err := f.manager.ExitOrder(od, &strat.ExitReq{CommandID: "close", Tag: "done"})
	if err != nil {
		t.Fatal(err)
	}
	f.rt.Notifier().Flush(5 * time.Second)
	entries, exits := sink.byType(rpc.MsgTypeEntry), sink.byType(rpc.MsgTypeExit)
	if len(entries) != 1 || len(exits) != 1 {
		t.Fatalf("messages entry=%d exit=%d, want 1/1 (all=%v)", len(entries), len(exits), sink.msgs)
	}
	e := entries[0]
	if e["account"] != "default" || e["action"] != "Open Long" || e["pair"] != "BTC" || e["enter_tag"] != "remote" || e["amount"].(float64) != 1 || e["price"].(float64) != 100 {
		t.Fatalf("entry payload: %v", e)
	}
	if x := exits[0]; x["action"] != "Close Long" || x["exit_tag"] != "done" || x["account"] != "default" {
		t.Fatalf("exit payload: %v", x)
	}
	// The payload is the one the legacy path builds.
	want, ok := biz.NewOrderMessage(closedOd, false, "default")
	if !ok || fmt.Sprint(want["action"]) != "Close Long" {
		t.Fatalf("builder: %v %v", want, ok)
	}
}

func TestNotifyBlockingOrFailingChannelNeverAffectsOrders(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var calls int
	var mu sync.Mutex
	f := newNotifyHubFixture(t, func(msg map[string]interface{}) {
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			panic("channel exploded")
		}
		<-release // every later message blocks the worker
	})
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	done := make(chan error, 1)
	go func() {
		// errs.Error values stay in dedicated variables (typed-nil rule).
		od, enterErr := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "open", Tag: "remote"})
		if enterErr != nil {
			done <- fmt.Errorf("enter: %v", enterErr)
			return
		}
		if _, exitErr := f.manager.ExitOrder(od, &strat.ExitReq{CommandID: "close", Tag: "done"}); exitErr != nil {
			done <- fmt.Errorf("exit: %v", exitErr)
			return
		}
		if updErr := f.manager.UpdateByDataSeries(nil, &orm.DataSeries{Sid: 1, TimeFrame: "ws"}); updErr != nil {
			done <- fmt.Errorf("update: %v", updErr)
			return
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("order path blocked by a failing notification channel")
	}
	// Overflowing the bounded queue drops instead of blocking.
	n := f.rt.Notifier()
	start := time.Now()
	dropped := 0
	for i := 0; i < notifyQueueSize*3; i++ {
		if !n.Post(map[string]interface{}{"type": rpc.MsgTypeStatus}) {
			dropped++
		}
	}
	if time.Since(start) > 2*time.Second || dropped == 0 {
		t.Fatalf("post blocked or never dropped: %v dropped=%d", time.Since(start), dropped)
	}
	// Shutdown notice waits only a bounded time even though the worker is stuck.
	start = time.Now()
	f.rt.NotifyShutdown(300 * time.Millisecond)
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("shutdown notification took %v", took)
	}
}

func TestNotifyStartedAndShutdownMessages(t *testing.T) {
	sink := &fakeNotifySink{}
	f := newNotifyHubFixture(t, sink.send)
	od, err := f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", &strat.EnterReq{CommandID: "open", Tag: "remote"})
	if err != nil || od == nil {
		t.Fatal(od, err)
	}
	f.rt.NotifyStarted()
	f.rt.NotifyShutdown(5 * time.Second)
	st := sink.byType(rpc.MsgTypeStatus)
	if len(st) != 2 || st[0]["status"] != "orders: 1 restored, 0 deleted, 0 added, 1 opened" || st[1]["status"] != "bot stop, 1 orders opened" {
		t.Fatalf("status messages: %v", st)
	}
}

// Telegram commands on a Hub robot: listing and closing work on the in-memory
// projections of the shared open-order registry.
func TestHubTelegramOrderManagerListsAndClosesProjectedOrders(t *testing.T) {
	f := newHubTSFixture(t, nil)
	mgr := biz.NewTelegramOrderManagerWithRuntimeDeps(f.rt.BizDeps())
	od, err := f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", &strat.EnterReq{CommandID: "open", Tag: "remote"})
	if err != nil || od == nil {
		t.Fatal(od, err)
	}
	list, listErr := mgr.GetActiveOrders("default")
	if listErr != nil || len(list) != 1 || list[0].ID != od.ID || list[0].Symbol != "BTC" || list[0].Amount != 1 {
		t.Fatalf("active orders: %+v %v", list, listErr)
	}
	if long, short, statErr := mgr.GetOrderStats("default"); statErr != nil || long != 1 || short != 0 {
		t.Fatalf("stats: %d %d %v", long, short, statErr)
	}
	f.owner.price = decimal.NewFromInt(110)
	if closeErr := mgr.CloseOrder("default", od.ID); closeErr != nil {
		t.Fatal(closeErr)
	}
	if updErr := f.manager.UpdateByDataSeries(nil, &orm.DataSeries{Sid: 1, TimeFrame: "ws"}); updErr != nil {
		t.Fatal(updErr)
	}
	if f.owner.adapter.Metrics().Fills != 2 {
		t.Fatalf("telegram close did not fill: %d", f.owner.adapter.Metrics().Fills)
	}
	if list, listErr = mgr.GetActiveOrders("default"); listErr != nil || len(list) != 0 {
		t.Fatalf("orders after close: %+v %v", list, listErr)
	}
	// Close-all with a closed projection still in the registry and a new open order.
	second, enterErr := f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", &strat.EnterReq{CommandID: "open2", Tag: "second"})
	if enterErr != nil || second == nil {
		t.Fatal(second, enterErr)
	}
	// The remote command service rate-limits repeated commands; a fresh manager has its own limiter.
	closedNum, failNum, allErr := biz.NewTelegramOrderManagerWithRuntimeDeps(f.rt.BizDeps()).CloseAllOrders("default")
	if allErr != nil || failNum != 0 {
		t.Fatalf("close all: closed=%d failed=%d err=%v", closedNum, failNum, allErr)
	}
	if updErr := f.manager.UpdateByDataSeries(nil, &orm.DataSeries{Sid: 1, TimeFrame: "ws"}); updErr != nil {
		t.Fatal(updErr)
	}
	// closedNum also counts the already-closed projection (its exit is an idempotent no-op, no venue fill).
	if closedNum < 1 {
		t.Fatalf("close all closed nothing: %d", closedNum)
	}
	if f.owner.adapter.Metrics().Fills != 4 {
		t.Fatalf("close all produced extra venue fills: %d, want 4", f.owner.adapter.Metrics().Fills)
	}
}
