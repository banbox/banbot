package runtime

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"path/filepath"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

// closeRecorder wraps the runtime callback and records every fill view, so
// the expectation is computed from exactly the orders the manager delivered.
type closeRecorder struct {
	mu     sync.Mutex
	rt     *Runtime
	closed []*ormo.InOutOrder // views of final exit fills
	exits  int                // every exit-side callback, partial or final
}

func (c *closeRecorder) callback(od *ormo.InOutOrder, isEnter bool) {
	c.mu.Lock()
	if !isEnter {
		c.exits++
		if od.Status == ormo.InOutStatusFullExit {
			c.closed = append(c.closed, od.Clone())
		}
	}
	rt := c.rt
	c.mu.Unlock()
	if rt != nil {
		rt.OrderNotifyCallback()(od, isEnter)
	}
}

func (c *closeRecorder) snapshot() []*ormo.InOutOrder {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*ormo.InOutOrder(nil), c.closed...)
}

func enablePerf(t *testing.T, rt *Runtime, cfg *config.StratPerfConfig) {
	t.Helper()
	old := rt.Strategies.Config
	next := &config.Config{}
	if old != nil {
		cp := *old
		next = &cp
	}
	next.StratPerf = cfg
	rt.Strategies.Config = next
	t.Cleanup(func() { rt.Strategies.Config = old })
}

func perfTestConfig(enable bool) *config.StratPerfConfig {
	return &config.StratPerfConfig{Enable: enable, MinOdNum: 1, MaxOdNum: 3, MinJobNum: 1, MidWeight: 0.7, BadWeight: 0.4}
}

// legacyExpectation feeds the same closed views to the unchanged legacy
// function through an OrderState history, on fresh core state.
func legacyExpectation(t *testing.T, rt *Runtime, views []*ormo.InOutOrder, symbol, tf, stgy string) (*core.State, *core.JobPerf) {
	t.Helper()
	cs, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := range views {
		state := ormo.NewOrderState()
		state.SetTask("default", &ormo.BotTask{ID: 1})
		for j := 0; j <= i; j++ {
			od := views[j].Clone()
			if od.ProfitRate == 0 {
				price := od.InitPrice
				if od.Enter.Average > 0 {
					price = od.Enter.Average
				}
				if n := price * od.Enter.Filled; n > 0 {
					od.ProfitRate = od.Profit / n
				}
			}
			od.ID, od.TaskID = int64(j+1), 1
			state.AddHistoricalOrder(od)
		}
		if e := strat.CalcJobScoresWithState(rt.Strategies, cs, state, "default", symbol, tf, stgy); e != nil {
			t.Fatal(e)
		}
	}
	return cs, cs.JobPerf(core.KeyStratPairTf(stgy, symbol, tf))
}

func requirePerfEqual(t *testing.T, rt *Runtime, rec *closeRecorder, symbol, tf, stgy string, wantClosed int) {
	t.Helper()
	tr := lookupPerfTracker(rt)
	if tr == nil || !tr.flush(5*time.Second) {
		t.Fatal("perf tracker missing or not drained")
	}
	views := rec.snapshot()
	if len(views) != wantClosed {
		t.Fatalf("delivered closed views = %d, want %d", len(views), wantClosed)
	}
	wantState, want := legacyExpectation(t, rt, views, symbol, tf, stgy)
	got := rt.Core.JobPerf(core.KeyStratPairTf(stgy, symbol, tf))
	if got == nil || want == nil {
		t.Fatalf("perf missing: got=%v want=%v", got, want)
	}
	if got.Num != want.Num || math.Abs(got.TotProfit-want.TotProfit) > 1e-12 || got.Score != want.Score {
		t.Fatalf("perf = %+v, legacy = %+v", got, want)
	}
	if g, w := perfOdNum(rt, stgy), perfOdNum2(wantState, stgy); g != w || g != wantClosed {
		t.Fatalf("OdNum = %d, legacy %d, closed %d", g, w, wantClosed)
	}
	if wantClosed > 0 && got.TotProfit == 0 {
		t.Fatal("test orders carried no profit; expectation is vacuous")
	}
}

func TestPerfUpdateHubClosedOrders(t *testing.T) {
	rec := &closeRecorder{}
	f := newHubTSFixture(t, rec.callback)
	rec.mu.Lock()
	rec.rt = f.rt
	rec.mu.Unlock()
	enablePerf(t, f.rt, perfTestConfig(true))
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}

	closeOne := func(i int, exitPrice int64) {
		id := string(rune('a' + i))
		od, e := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "open" + id, Tag: "t" + id, Amount: 1})
		if e != nil || od == nil {
			t.Fatal(od, e)
		}
		hubTSObserve(t, f, exitPrice)
		if _, e = f.manager.ExitOrder(od, &strat.ExitReq{CommandID: "close" + id, Tag: "done"}); e != nil {
			t.Fatal(e)
		}
		hubTSObserve(t, f, 100)
	}
	for i, px := range []int64{110, 95, 120, 104} {
		closeOne(i, px)
		requirePerfEqual(t, f.rt, rec, "BTC", "ws", "legacy", i+1)
	}
	got := f.rt.Core.JobPerf(core.KeyStratPairTf("legacy", "BTC", "ws"))
	if got.Num != 3 {
		t.Fatalf("ring must be bounded by max_od_num=3, Num=%d", got.Num)
	}
}

func hubPerfFixture(t *testing.T, enable bool) (*hubTSFixture, *closeRecorder) {
	rec := &closeRecorder{}
	f := newHubTSFixture(t, rec.callback)
	rec.mu.Lock()
	rec.rt = f.rt
	rec.mu.Unlock()
	if enable {
		enablePerf(t, f.rt, perfTestConfig(true))
	}
	return f, rec
}

func hubPerfClose(t *testing.T, f *hubTSFixture, id string, exitPrice int64) *ormo.InOutOrder {
	t.Helper()
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, e := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "open" + id, Tag: "t" + id, Amount: 1})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	hubTSObserve(t, f, exitPrice)
	od, e = f.manager.ExitOrder(od, &strat.ExitReq{CommandID: "close" + id, Tag: "done"})
	if e != nil {
		t.Fatal(e)
	}
	hubTSObserve(t, f, 100)
	return od
}

func TestPerfUpdatePartialReductionAndRedeliveryDoNotCount(t *testing.T) {
	f, rec := hubPerfFixture(t, true)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, e := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "openp", Tag: "p", Amount: 1})
	if e != nil || od == nil {
		t.Fatal(od, e)
	}
	hubTSObserve(t, f, 110)
	if od, e = f.manager.ExitOrder(od, &strat.ExitReq{CommandID: "halfp", Tag: "half", ExitRate: 0.5}); e != nil {
		t.Fatal(e)
	}
	if od.Status != ormo.InOutStatusPartExit {
		t.Fatalf("expected a partial reduction, status=%d", od.Status)
	}
	rec.mu.Lock()
	exits := rec.exits
	rec.mu.Unlock()
	if exits == 0 {
		t.Fatal("partial exit never reached the callback; test is vacuous")
	}
	if tr := lookupPerfTracker(f.rt); tr != nil {
		tr.flush(5 * time.Second)
	}
	if n := perfOdNum(f.rt, "legacy"); n != 0 || f.rt.Core.JobPerf(core.KeyStratPairTf("legacy", "BTC", "ws")) != nil {
		t.Fatalf("partial reduction counted as finished order: OdNum=%d", n)
	}
	// Close the rest: now exactly one finished order.
	if od, e = f.manager.ExitOrder(od, &strat.ExitReq{CommandID: "restp", Tag: "rest"}); e != nil {
		t.Fatal(e)
	}
	hubTSObserve(t, f, 100)
	requirePerfEqual(t, f.rt, rec, "BTC", "ws", "legacy", 1)

	// Redelivery of the same close (crash replay) must not count again.
	views := rec.snapshot()
	cb := f.rt.OrderNotifyCallback()
	cb(views[0], false)
	cb(views[0].Clone(), false)
	lookupPerfTracker(f.rt).flush(5 * time.Second)
	if n := perfOdNum(f.rt, "legacy"); n != 1 {
		t.Fatalf("redelivered close double counted: OdNum=%d", n)
	}
	// Entry-side callbacks never count.
	cb(views[0], true)
	lookupPerfTracker(f.rt).flush(5 * time.Second)
	if n := perfOdNum(f.rt, "legacy"); n != 1 {
		t.Fatalf("entry callback counted: OdNum=%d", n)
	}
}

func TestPerfUpdateDisabledIsInert(t *testing.T) {
	for name, cfg := range map[string]*config.StratPerfConfig{"nil": nil, "disabled": perfTestConfig(false)} {
		t.Run(name, func(t *testing.T) {
			f, rec := hubPerfFixture(t, false)
			if cfg != nil {
				enablePerf(t, f.rt, cfg)
			}
			f.rt.Orders.BindTradesPath(filepath.Join(t.TempDir(), "orders.db"))
			biz.EnsureTimeSeriesOrderTask(f.rt.BizDeps())
			hubPerfClose(t, f, "a", 110)
			hubPerfClose(t, f, "b", 90)
			if len(rec.snapshot()) != 2 {
				t.Fatal("closes were not delivered; test is vacuous")
			}
			if lookupPerfTracker(f.rt) != nil {
				t.Fatal("tracker (ring/worker/db) created while strat_perf is disabled")
			}
			if n := perfOdNum(f.rt, "legacy"); n != 0 || f.rt.Core.JobPerf(core.KeyStratPairTf("legacy", "BTC", "ws")) != nil {
				t.Fatalf("performance changed while disabled: OdNum=%d", n)
			}
		})
	}
}

// After a restart the ring is empty; it is seeded once from the trades
// database (Hub archive) without counting the archived order of the closing
// callback twice.
func TestPerfUpdateHubRestartSeedsFromArchive(t *testing.T) {
	f, rec := hubPerfFixture(t, true)
	f.rt.Orders.BindTradesPath(filepath.Join(t.TempDir(), "orders.db"))
	biz.EnsureTimeSeriesOrderTask(f.rt.BizDeps())
	if f.rt.Orders.GetTaskID("default") < 0 {
		t.Fatal("no trades task")
	}
	hubPerfClose(t, f, "a", 110)
	hubPerfClose(t, f, "b", 95)
	// Make sure both are archived, then drop the in-memory ring as a restart does.
	if err := f.manager.UpdateByDataSeries(nil, &orm.DataSeries{Sid: 1, TimeFrame: "ws"}); err != nil {
		t.Fatal(err)
	}
	requirePerfEqual(t, f.rt, rec, "BTC", "ws", "legacy", 2)
	old := lookupPerfTracker(f.rt)
	old.once.Do(func() { close(old.stop) })
	perfTrackersMu.Lock()
	delete(perfTrackers, f.rt)
	perfTrackersMu.Unlock()

	hubPerfClose(t, f, "c", 120)
	if err := f.manager.UpdateByDataSeries(nil, &orm.DataSeries{Sid: 1, TimeFrame: "ws"}); err != nil {
		t.Fatal(err)
	}
	requirePerfEqual(t, f.rt, rec, "BTC", "ws", "legacy", 3)
	if n := f.rt.Core.JobPerf(core.KeyStratPairTf("legacy", "BTC", "ws")).Num; n != 3 {
		t.Fatalf("seeded history lost: Num=%d, want 3", n)
	}
}

func sharedPerfFixture(t *testing.T, enable bool) (*sharedTriggerFixture, *closeRecorder) {
	t.Helper()
	rec := &closeRecorder{}
	f := newSharedTriggerFixtureWithAdapter(t, nil, true)
	rec.rt = f.rt
	biz.InitLocalOrderMgrWithRuntimeDeps(f.rt.BizDeps(), rec.callback, false)
	f.manager = biz.GetOdMgrWithState(f.rt.Trading, "default")
	if enable {
		enablePerf(t, f.rt, perfTestConfig(true))
	}
	return f, rec
}

func sharedPerfClose(t *testing.T, f *sharedTriggerFixture, bar int64, exitPrice float64, partialFirst bool) {
	t.Helper()
	f.observe(t, 100, bar)
	f.entry(t, &strat.EnterReq{Tag: "e", Amount: 1})
	f.observe(t, exitPrice, bar+1)
	if partialFirst {
		if err := f.job.CloseOrders(&strat.ExitReq{Tag: "half", ExitRate: 0.5}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := f.manager.ProcessOrders(f.job); err != nil {
			t.Fatal(err)
		}
		f.observe(t, exitPrice, bar+2)
	}
	if err := f.job.CloseOrders(&strat.ExitReq{Tag: "all"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.manager.ProcessOrders(f.job); err != nil {
		t.Fatal(err)
	}
	f.observe(t, exitPrice, bar+3)
}

func TestPerfUpdateSharedClosedOrders(t *testing.T) {
	f, rec := sharedPerfFixture(t, true)
	bar := int64(2)
	for i, px := range []float64{110, 95, 120, 104} {
		sharedPerfClose(t, f, bar, px, i == 1)
		bar += 10
		requirePerfEqual(t, f.rt, rec, "BTC", "ws", "legacy", i+1)
	}
	if got := f.rt.Core.JobPerf(core.KeyStratPairTf("legacy", "BTC", "ws")); got.Num != 3 {
		t.Fatalf("ring must be bounded by max_od_num=3, Num=%d", got.Num)
	}
	rec.mu.Lock()
	exits, closed := rec.exits, len(rec.closed)
	rec.mu.Unlock()
	if exits <= closed {
		t.Fatalf("partial reduction never reached the callback (exits=%d closed=%d)", exits, closed)
	}
	views := rec.snapshot()
	f.rt.OrderNotifyCallback()(views[len(views)-1], false)
	lookupPerfTracker(f.rt).flush(5 * time.Second)
	if n := perfOdNum(f.rt, "legacy"); n != 4 {
		t.Fatalf("redelivered close double counted: OdNum=%d", n)
	}
}

func TestPerfUpdateSharedDisabledIsInert(t *testing.T) {
	f, rec := sharedPerfFixture(t, false)
	sharedPerfClose(t, f, 2, 110, false)
	if len(rec.snapshot()) != 1 {
		t.Fatal("close not delivered; test is vacuous")
	}
	if lookupPerfTracker(f.rt) != nil || perfOdNum(f.rt, "legacy") != 0 {
		t.Fatal("work done while strat_perf is disabled")
	}
}

func perfOdNum(rt *Runtime, stgy string) int { return perfOdNum2(rt.Core, stgy) }

func perfOdNum2(cs *core.State, stgy string) int {
	if sta := cs.PerfSta(stgy); sta != nil {
		return sta.OdNum
	}
	return 0
}
