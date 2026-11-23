package runtime

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

// flushProbe records OnOrderChange deliveries and queues a take-profit limit
// exit for the order that just filled.
type flushProbe struct {
	mu    sync.Mutex
	fills int
}

func (p *flushProbe) hook(t *testing.T, limit float64) func(*strat.StratJob, *ormo.InOutOrder, int) {
	return func(job *strat.StratJob, od *ormo.InOutOrder, kind int) {
		if kind != strat.OdChgEnterFill {
			return
		}
		p.mu.Lock()
		p.fills++
		p.mu.Unlock()
		if err := job.CloseOrders(&strat.ExitReq{OrderID: od.ID, Tag: "tp", Limit: limit}); err != nil {
			t.Error(err)
		}
	}
}

func (p *flushProbe) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fills
}

func hubFlushJob(f *hubTSFixture, stg *strat.TradeStrat) *strat.StratJob {
	job := &strat.StratJob{Strat: stg, Symbol: &orm.ExSymbol{ID: 1, Symbol: "BTC"}, TimeFrame: "1m", Account: "default", CloseLong: true, CloseShort: true}
	job.BindRuntimeState(f.rt.Strategies, f.rt.Core, f.rt.Clock)
	job.BindRuntimeMarket(f.rt.Market.Prices, f.rt.Clock)
	return job
}

// hubExitLimit returns the limit of the account-level pending exit of od, or 0.
func hubExitLimit(t *testing.T, f *hubTSFixture, od *ormo.InOutOrder) (float64, int64) {
	t.Helper()
	d := hubPendingOf(t, f, od)
	if d.Order.Exit == nil {
		return 0, int64(d.CommandRevision)
	}
	limit, _ := d.Order.Exit.Conditions.Limit.Float64()
	return limit, int64(d.CommandRevision)
}

func TestHubCallbackQueuedExitFlushedWithoutNextBar(t *testing.T) {
	f := newHubTSFixture(t, nil)
	f.hub.SetPendingFeedAdmission(func(context.Context, execution.HubPendingFeed) error { return nil })
	probe := &flushProbe{}
	job := hubFlushJob(f, &strat.TradeStrat{Name: "legacy", OnOrderChange: probe.hook(t, 120)})
	if err := f.manager.BindJobs([]*strat.StratJob{job}); err != nil {
		t.Fatal(err)
	}
	od, e := f.manager.EnterOrder(job.Symbol, "1m", &strat.EnterReq{CommandID: "tp-entry", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	if probe.count() != 0 {
		t.Fatal("limit entry filled early")
	}
	hubTSObserve(t, f, 89)
	got := hubTSOrder(t, f, od.ID)
	if probe.count() != 1 {
		t.Fatal("callback not delivered exactly once", probe.count(), got.Enter.Filled)
	}
	limit, rev := hubExitLimit(t, f, got)
	if limit != 120 {
		t.Fatalf("exit queued in OnOrderChange was not flushed promptly: limit %v", limit)
	}
	if r := job.ExecutionSnapshot(); len(r.Exits) != 0 || len(r.Entrys) != 0 {
		t.Fatal("requests left queued", r)
	}
	// Further polls must neither redeliver nor resubmit.
	hubTSObserve(t, f, 88)
	hubTSOrder(t, f, od.ID)
	if _, rev2 := hubExitLimit(t, f, got); probe.count() != 1 || rev2 != rev {
		t.Fatal("callback redelivered or exit resubmitted", probe.count(), rev, rev2)
	}
}

func hubInjectDelivery(t *testing.T, path string, od *ormo.InOutOrder, id string) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var journal map[string]json.RawMessage
	if err = json.Unmarshal(body, &journal); err != nil {
		t.Fatal(err)
	}
	journal["PendingDeliveries"], err = json.Marshal([]map[string]any{{"Order": od, "Kind": strat.OdChgEnterFill, "ID": id, "Fill": true, "IsEnter": true}})
	if err != nil {
		t.Fatal(err)
	}
	body, _ = json.Marshal(journal)
	if err = os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
}

// A delivery that was not acknowledged before a crash is redelivered with the
// same event ID: the callback runs again but the flushed exit keeps its
// event-derived command ID, so the account sees exactly one exit.
func TestHubRedeliveredEventFlushesExitOnce(t *testing.T) {
	f := newHubTSFixture(t, nil)
	f.hub.SetPendingFeedAdmission(func(context.Context, execution.HubPendingFeed) error { return nil })
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, e := f.manager.EnterOrder(symbol, "1m", &strat.EnterReq{CommandID: "redeliver", Amount: 1})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	view := hubTSOrder(t, f, od.ID)
	f.manager.CleanUp()
	probe := &flushProbe{}
	var rev int64
	for round := 1; round <= 2; round++ {
		hubInjectDelivery(t, f.bridge.StatePath, view, "event-1/1")
		m, err := biz.NewHubOrderMgr(f.rt.BizDeps(), f.bridge, nil)
		if err != nil {
			t.Fatal(err)
		}
		job := hubFlushJob(f, &strat.TradeStrat{Name: "legacy", OnOrderChange: probe.hook(t, 120)})
		if err = m.BindJobs([]*strat.StratJob{job}); err != nil {
			m.CleanUp()
			t.Fatal(err)
		}
		m.CleanUp()
		if probe.count() != round {
			t.Fatal("redelivery did not reach the callback", round, probe.count())
		}
		limit, r := hubExitLimit(t, f, view)
		if limit != 120 {
			t.Fatal("redelivered callback exit missing", round, limit)
		}
		if round == 1 {
			rev = r
		} else if r != rev {
			t.Fatal("redelivery resubmitted the exit", rev, r)
		}
	}
}

// History replayed while hydrating a fresh journal must not submit orders.
func TestHubRestoreReplayDoesNotFlushFollowups(t *testing.T) {
	f := newHubTSFixture(t, nil)
	f.hub.SetPendingFeedAdmission(func(context.Context, execution.HubPendingFeed) error { return nil })
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, e := f.manager.EnterOrder(symbol, "1m", &strat.EnterReq{CommandID: "history", Amount: 1})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	view := hubTSOrder(t, f, od.ID)
	f.manager.CleanUp()
	fresh := *f.bridge
	fresh.StatePath = f.bridge.StatePath + ".fresh"
	m, err := biz.NewHubOrderMgr(f.rt.BizDeps(), &fresh, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.CleanUp()
	probe := &flushProbe{}
	job := hubFlushJob(f, &strat.TradeStrat{Name: "legacy", OnOrderChange: probe.hook(t, 120)})
	if err = m.BindJobs([]*strat.StratJob{job}); err != nil {
		t.Fatal(err)
	}
	if probe.count() != 0 {
		t.Fatal("historical fill reached the strategy", probe.count())
	}
	if r := job.ExecutionSnapshot(); len(r.Exits) != 0 {
		t.Fatal("historical replay queued requests")
	}
	if limit, _ := hubExitLimit(t, f, view); limit != 0 {
		t.Fatal("historical replay submitted a follow-up exit", limit)
	}
}

// Shared: an exit queued while a limit entry fills on a data event (no
// ProcessOrders in flight) is submitted at once, with the event-derived ID.
func TestSharedCallbackQueuedExitFlushedWithoutNextBar(t *testing.T) {
	f := newSharedTriggerFixture(t)
	var mu sync.Mutex
	var fills int
	var generated *strat.ExitReq
	f.job.Strat.OnOrderChange = func(job *strat.StratJob, od *ormo.InOutOrder, kind int) {
		if kind != strat.OdChgEnterFill {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		fills++
		if err := job.CloseOrders(&strat.ExitReq{OrderID: od.ID, Tag: "tp", ExitRate: .5}); err != nil {
			t.Error(err)
			return
		}
		if r := job.ExecutionSnapshot().Exits; len(r) == 1 {
			generated = r[0].Clone()
		}
	}
	od := f.entry(t, &strat.EnterReq{Tag: "tp", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
	if od.Enter.Filled != 0 || f.steps(t) != 0 {
		t.Fatal("limit entry filled early")
	}
	f.observe(t, 89, 2)
	mu.Lock()
	n, gen := fills, generated
	mu.Unlock()
	if n != 1 || gen == nil || gen.CommandID == "" {
		t.Fatal("entry callback missing or lacks event provenance", n, gen)
	}
	if got := f.steps(t); got != 5 {
		t.Fatal("exit queued in OnOrderChange was not flushed without another bar", got)
	}
	if r := f.job.ExecutionSnapshot(); len(r.Exits) != 0 || len(r.Entrys) != 0 {
		t.Fatal("requests left queued", r)
	}
	// Redelivery of the event regenerates the same command: no second exit.
	if err := f.job.CloseOrders(gen); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.manager.ProcessOrders(f.job); err != nil {
		t.Fatal(err)
	}
	f.observe(t, 88, 3)
	if got := f.steps(t); got != 5 || f.adapter.Metrics().Fills != 2 {
		t.Fatal("redelivered event traded twice", got, f.adapter.Metrics().Fills)
	}
}

// HedgeOff on the execution-backed path: a filled entry closes the opposite
// side of the same symbol through the legacy closeSideOrders helper.
func TestSharedHedgeOffClosesOppositeSideOnEntryFill(t *testing.T) {
	for _, hedgeOff := range []bool{false, true} {
		f := newSharedTriggerFixture(t)
		f.job.Strat.HedgeOff = hedgeOff
		long := f.entry(t, &strat.EnterReq{Tag: "long", Amount: 1})
		f.entry(t, &strat.EnterReq{Tag: "short", Amount: 1, Short: true})
		rows, lock := f.rt.Orders.GetOpenODs("default")
		lock.Lock()
		lg := rows[long.ID]
		closed := lg == nil || lg.Status >= ormo.InOutStatusFullExit
		lock.Unlock()
		if closed != hedgeOff {
			t.Fatalf("HedgeOff=%v: opposite side closed=%v", hedgeOff, closed)
		}
	}
}

func TestHubHedgeOffClosesOppositeSideOnEntryFill(t *testing.T) {
	for _, hedgeOff := range []bool{false, true} {
		f := newHubTSFixture(t, nil)
		f.hub.SetPendingFeedAdmission(func(context.Context, execution.HubPendingFeed) error { return nil })
		job := hubFlushJob(f, &strat.TradeStrat{Name: "legacy", HedgeOff: hedgeOff})
		if err := f.manager.BindJobs([]*strat.StratJob{job}); err != nil {
			t.Fatal(err)
		}
		long, e := f.manager.EnterOrder(job.Symbol, "1m", &strat.EnterReq{CommandID: "long", Amount: 1})
		if e != nil || long == nil {
			t.Fatal(long, e)
		}
		if _, e = f.manager.EnterOrder(job.Symbol, "1m", &strat.EnterReq{CommandID: "short", Amount: 1, Short: true}); e != nil {
			t.Fatal(e)
		}
		hubTSOrder(t, f, long.ID) // the first poll commits the flushed exit; the second folds it
		got := hubTSOrder(t, f, long.ID)
		closed := got.Status >= ormo.InOutStatusFullExit
		if closed != hedgeOff {
			t.Fatalf("HedgeOff=%v: opposite side closed=%v status=%v", hedgeOff, closed, got.Status)
		}
	}
}
