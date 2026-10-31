package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
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

type hubTSFixture struct {
	owner   *sharedTriggerFixture
	rt      *Runtime
	client  *execution.HubClient
	bridge  *biz.HubOrderBridgeConfig
	manager *biz.HubOrderMgr
	hub     *execution.Hub
	server  *execution.HubServer
}

func newHubTSFixture(t *testing.T, callback biz.FnOdCb) *hubTSFixture {
	t.Helper()
	owner := newSharedTriggerFixtureWithAdapter(t, nil, true)
	hub, err := execution.NewHub(func(context.Context, execution.HubAccountRegistration) (execution.HubAccountBinding, error) {
		var service *execution.SharedAccount
		err := owner.rt.SharedExecution().WithState(func(s *execution.SharedAccount) error { service = s; return nil })
		return execution.HubAccountBinding{Account: service, Shutdown: func() error { return nil }}, err
	})
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("", "hub-ts-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	server, err := execution.ListenHubUnix(filepath.Join(dir, "s"), hub)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	client := execution.NewHubClient(filepath.Join(dir, "s"))
	t.Cleanup(func() { client.Close() })
	if err = client.Register(context.Background(), execution.HubAccountRegistration{Account: owner.key, Env: core.RunEnvDryRun, Configuration: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	bridge := &biz.HubOrderBridgeConfig{Client: client, Account: owner.key, Bridge: *owner.bridge, WriterID: "test-robot", StatePath: filepath.Join(dir, "robot.json")}
	rt, err := owner.process.NewRuntime(Options{Mode: core.RunModeLive, Env: core.RunEnvDryRun, HubOrderBridge: bridge})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := biz.NewHubOrderMgr(rt.BizDeps(), bridge, callback)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { manager.CleanUp() })
	rt.Trading.SetOrderManager("default", manager)
	return &hubTSFixture{owner: owner, rt: rt, client: client, bridge: bridge, manager: manager, hub: hub, server: server}
}
func TestHubOrderBridgeRemoteEntryExitRestartAndNoLocalOwner(t *testing.T) {
	f := newHubTSFixture(t, nil)
	if f.rt.SharedExecution() != nil || f.rt.accountOwner != nil || f.rt.BizDeps().SharedOrderBridge != nil {
		t.Fatal("remote robot acquired local execution state")
	}
	biz.InitLocalOrderMgrWithRuntimeDeps(f.rt.BizDeps(), nil, false)
	if got := biz.GetOdMgrWithState(f.rt.Trading, "default"); got != f.manager {
		t.Fatalf("job loader replaced remote facade with %T", got)
	}
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	req := &strat.EnterReq{CommandID: "open", Tag: "remote"}
	od, err := f.manager.EnterOrder(symbol, "ws", req)
	if err != nil || od == nil {
		t.Fatal(od, err)
	}
	if od.Enter.Amount != 1 || od.Enter.Filled != 1 || od.Enter.Average != 100 {
		t.Fatalf("default remote sizing/projection: %+v", od.Enter)
	}
	if req.Amount != 0 || req.StratName != "" {
		t.Fatal("request mutated")
	}
	replay, e := f.manager.EnterOrder(symbol, "ws", req)
	if e != nil || replay.ID != od.ID || f.owner.adapter.Metrics().Fills != 1 {
		t.Fatal("entry retry was not idempotent", replay, e)
	}
	if od, e = f.manager.ExitOrder(od, &strat.ExitReq{CommandID: "close", Tag: "done"}); e != nil {
		t.Fatal(e)
	}
	if od.Status != ormo.InOutStatusFullExit || od.Exit == nil || od.Exit.Filled != 1 || od.Exit.Average != 100 {
		t.Fatalf("remote exit projection: %+v", od)
	}
	if e := f.manager.CleanUp(); e != nil {
		t.Fatal(e)
	}
	restarted, e2 := biz.NewHubOrderMgr(f.rt.BizDeps(), f.bridge, nil)
	if e2 != nil {
		t.Fatal(e2)
	}
	defer restarted.CleanUp()
	restored, e := restarted.EnterOrder(symbol, "ws", req)
	if e != nil || restored.ID != od.ID || f.owner.adapter.Metrics().Fills != 2 {
		t.Fatal("restart lost frozen command identity", restored, e)
	}
	info, e2 := os.Stat(f.bridge.StatePath)
	if e2 != nil || info.Mode().Perm() != 0600 {
		t.Fatal("journal not private", info, e2)
	}
}
func TestHubOrderBridgeReentrantFillAndJournalLease(t *testing.T) {
	var mu sync.Mutex
	var manager *biz.HubOrderMgr
	var reentered *ormo.InOutOrder
	var reenterErr error
	var seen int
	f := newHubTSFixture(t, func(od *ormo.InOutOrder, enter bool) {
		if !enter || od.EnterTag != "first" {
			return
		}
		mu.Lock()
		seen++
		mu.Unlock()
		next, e := manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", &strat.EnterReq{CommandID: "nested", Tag: "nested", Amount: 1})
		mu.Lock()
		reentered = next
		if e != nil {
			reenterErr = e
		}
		mu.Unlock()
	})
	manager = f.manager
	if duplicate, e := biz.NewHubOrderMgr(f.rt.BizDeps(), f.bridge, nil); e == nil {
		duplicate.CleanUp()
		t.Fatal("two robots acquired same journal")
	}
	done := make(chan error, 1)
	go func() {
		_, e := manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", &strat.EnterReq{CommandID: "first", Tag: "first", Amount: 1})
		if e != nil {
			done <- e
		} else {
			done <- nil
		}
	}()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reentrant remote callback deadlocked")
	}
	mu.Lock()
	defer mu.Unlock()
	if seen != 1 || reentered == nil || reenterErr != nil {
		t.Fatal("reentrant command lost facade", seen, reentered, reenterErr)
	}
}
func TestHubOrderBridgeRejectsBacktestAndLocalOwner(t *testing.T) {
	f := newHubTSFixture(t, nil)
	for _, opts := range []Options{{Mode: core.RunModeBackTest, Env: core.RunEnvDryRun, HubOrderBridge: f.bridge}, {Mode: core.RunModeLive, Env: core.RunEnvDryRun, HubOrderBridge: f.bridge, AccountOwnerKey: &f.owner.key}, {Mode: core.RunModeLive, Env: core.RunEnvProd, HubOrderBridge: f.bridge, SharedExecution: f.owner.opts}} {
		if rt, e := f.owner.process.NewRuntime(opts); e == nil {
			rt.Close()
			rt.Join()
			t.Fatal("invalid Hub composition admitted")
		}
	}
}

func hubTSOrder(t *testing.T, f *hubTSFixture, id int64) *ormo.InOutOrder {
	t.Helper()
	if err := f.manager.UpdateByDataSeries(nil, nil); err != nil {
		t.Fatal(err)
	}
	orders, lock := f.rt.Orders.GetOpenODs("default")
	lock.Lock()
	defer lock.Unlock()
	od := orders[id]
	if od == nil {
		t.Fatal("missing projected order", id)
	}
	return od.Clone()
}
func hubTSObserve(t *testing.T, f *hubTSFixture, price int64) {
	t.Helper()
	now := time.Now().UnixMilli()
	p := decimal.NewFromInt(price)
	if err := f.owner.rt.SharedExecution().WithState(func(*execution.SharedAccount) error { f.owner.price = p; return nil }); err != nil {
		t.Fatal(err)
	}
	q := execution.VisibleQuote{Bid: p, Ask: p, AtMS: now, ReceivedMS: now, ValidUntilMS: now + 1000}
	if err := f.owner.rt.SharedExecution().AdvancePaperQuote(context.Background(), "BTC", q, now); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.rt.SharedExecution().AdvancePendingExecution(context.Background(), now); err != nil {
		t.Fatal(err)
	}
}
func TestHubOrderBridgeLimitProtectionEditsAndFeedExpiry(t *testing.T) {
	f := newHubTSFixture(t, nil)
	f.hub.SetPendingFeedAdmission(func(context.Context, execution.HubPendingFeed) error { return nil })
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, e := f.manager.EnterOrder(symbol, "1m", &strat.EnterReq{CommandID: "limit", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopBars: 2, StopLoss: 80})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	if od.Enter.Filled != 0 || f.owner.adapter.Metrics().Fills != 0 {
		t.Fatal("remote dormant limit submitted prematurely")
	}
	edited := od.Clone()
	edited.Enter.Price = 95
	f.manager.EditOrder(edited, ormo.OdActionLimitEnter)
	if err := f.manager.LastError(); err != nil {
		t.Fatal(err)
	}
	hubTSObserve(t, f, 94)
	od = hubTSOrder(t, f, od.ID)
	if od.Enter.Filled != 1 || od.Enter.Average != 94 {
		t.Fatal("Hub did not own limit advancement", od.Enter)
	}
	edited = od.Clone()
	edited.Info[ormo.OdInfoStopLoss] = &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: 90, Tag: "edited-sl"}}
	f.manager.EditOrder(edited, ormo.OdInfoStopLoss)
	if err := f.manager.LastError(); err != nil {
		t.Fatal(err)
	}
	hubTSObserve(t, f, 89)
	od = hubTSOrder(t, f, od.ID)
	if od.Status != ormo.InOutStatusFullExit || od.Exit == nil || od.Exit.Filled != 1 {
		t.Fatal("Hub did not own edited stop loss", od)
	}
	expired, e := f.manager.EnterOrder(symbol, "1m", &strat.EnterReq{CommandID: "expires", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 50, StopBars: 2})
	if e != nil {
		t.Fatal(e)
	}
	for i := int64(1); i <= 2; i++ {
		if _, err := f.owner.rt.SharedExecution().ObservePendingFeed(context.Background(), execution.PendingFeedObservation{Feed: "1/1m", EndMS: time.Now().UnixMilli() + i*60000, Closed: true}); err != nil {
			t.Fatal(err)
		}
		if err := f.owner.rt.SharedExecution().AdvancePendingExecution(context.Background(), time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	expired = hubTSOrder(t, f, expired.ID)
	if expired.Status != ormo.InOutStatusDelete || expired.Enter.Filled != 0 {
		t.Fatal("Hub did not expire unfilled limit", expired)
	}
}
func TestHubOrderBridgeLostOutboxAckRestartsWithoutDuplicateFill(t *testing.T) {
	f := newHubTSFixture(t, nil)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	req := &strat.EnterReq{CommandID: "lost-ack", Amount: 1}
	od, e := f.manager.EnterOrder(symbol, "ws", req)
	if e != nil {
		t.Fatal(e)
	}
	f.manager.CleanUp()
	// Simulate a crash after the Hub commit but before local ACK persistence.
	body, err := os.ReadFile(f.bridge.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	var journal map[string]json.RawMessage
	if err = json.Unmarshal(body, &journal); err != nil {
		t.Fatal(err)
	}
	var commands map[string]map[string]json.RawMessage
	if err = json.Unmarshal(journal["Commands"], &commands); err != nil {
		t.Fatal(err)
	}
	for _, command := range commands {
		command["Done"] = json.RawMessage(`false`)
	}
	journal["Commands"], _ = json.Marshal(commands)
	body, _ = json.Marshal(journal)
	if err = os.WriteFile(f.bridge.StatePath, body, 0600); err != nil {
		t.Fatal(err)
	}
	restarted, err := biz.NewHubOrderMgr(f.rt.BizDeps(), f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.CleanUp()
	again, e := restarted.EnterOrder(symbol, "ws", req)
	if e != nil || again.ID != od.ID || f.owner.adapter.Metrics().Fills != 1 {
		t.Fatal("lost ACK resubmitted a second fill", again, e)
	}
}
func TestHubOrderBridgeDisconnectStopsRobotAndExposesFailure(t *testing.T) {
	f := newHubTSFixture(t, nil)
	if err := f.server.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.rt.Core.Context().Done():
	case <-time.After(3 * time.Second):
		t.Fatal("robot ignored disconnected Hub")
	}
	if f.manager.LastError() == nil {
		t.Fatal("Hub disconnect failure was swallowed")
	}
}

func TestHubOrderBridgeRestartDefersOfflineFillUntilJobsBound(t *testing.T) {
	f := newHubTSFixture(t, nil)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, e := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "offline-limit", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
	if e != nil {
		t.Fatal(e)
	}
	f.manager.CleanUp()
	hubTSObserve(t, f, 85)
	var mu sync.Mutex
	fills, jobFills := 0, 0
	restarted, err := biz.NewHubOrderMgr(f.rt.BizDeps(), f.bridge, func(_ *ormo.InOutOrder, enter bool) {
		if enter {
			mu.Lock()
			fills++
			mu.Unlock()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.CleanUp()
	time.Sleep(220 * time.Millisecond)
	mu.Lock()
	before := fills
	mu.Unlock()
	if before != 0 {
		t.Fatal("restart ACKed offline fills before binding jobs", before)
	}
	job := &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy", OnOrderChange: func(_ *strat.StratJob, view *ormo.InOutOrder, kind int) {
		if kind == strat.OdChgEnterFill {
			mu.Lock()
			jobFills++
			mu.Unlock()
			if view.ID != od.ID || view.Enter.Filled != 1 || view.Enter.Average != 85 {
				t.Error("offline fill projection lost event values")
			}
		}
	}}, Symbol: symbol, TimeFrame: "ws"}
	if err = restarted.BindJobs([]*strat.StratJob{job}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if fills != 1 || jobFills != 1 {
		t.Fatal("offline fill callback lost or duplicated", fills, jobFills)
	}
}
func TestHubOrderBridgeRestartDefersDurableCallbackOutboxUntilJobsBound(t *testing.T) {
	f := newHubTSFixture(t, nil)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, e := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "outbox", Amount: 1, StopLoss: 90})
	if e != nil {
		t.Fatal(e)
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
	journal["PendingDeliveries"], err = json.Marshal([]map[string]any{{"Order": od, "Kind": strat.OdChgEnterFill, "ID": "pending-callback", "Fill": true, "IsEnter": true}})
	if err != nil {
		t.Fatal(err)
	}
	body, _ = json.Marshal(journal)
	if err = os.WriteFile(f.bridge.StatePath, body, 0600); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	calls := 0
	restarted, err := biz.NewHubOrderMgr(f.rt.BizDeps(), f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.CleanUp()
	time.Sleep(220 * time.Millisecond)
	body, err = os.ReadFile(f.bridge.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(body, &journal); err != nil {
		t.Fatal(err)
	}
	var pending []json.RawMessage
	if err = json.Unmarshal(journal["PendingDeliveries"], &pending); err != nil || len(pending) != 1 {
		t.Fatal("constructor consumed callback outbox before BindJobs", len(pending), err)
	}
	job := &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy", OnOrderChange: func(_ *strat.StratJob, view *ormo.InOutOrder, kind int) {
		if kind == strat.OdChgEnterFill {
			mu.Lock()
			calls++
			mu.Unlock()
			if view.GetStopLoss() == nil || view.GetStopLoss().Price != 90 {
				t.Error("restored callback lost typed trigger state")
			}
		}
	}}, Symbol: symbol, TimeFrame: "ws"}
	if err = restarted.BindJobs([]*strat.StratJob{job}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatal("durable callback outbox not delivered exactly once", calls)
	}
}
