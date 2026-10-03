package runtime

import (
	"context"
	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"testing"
)

func TestSharedDistinctTimeframeJobsOwnProjectionCallbacksAndRestore(t *testing.T) {
	f := newSharedTriggerFixture(t)
	var callbacks []string
	jobs := func() (*strat.StratJob, *strat.StratJob) {
		build := func(tf string) *strat.StratJob {
			j := &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy"}, Symbol: &orm.ExSymbol{ID: 1, Symbol: "BTC"}, TimeFrame: tf, Account: "default", CloseLong: true, CloseShort: true}
			j.BindRuntimeState(f.rt.Strategies, f.rt.Core, f.rt.Clock)
			j.BindRuntimeMarket(f.rt.Market.Prices, f.rt.Clock)
			j.Strat.OnOrderChange = func(_ *strat.StratJob, od *ormo.InOutOrder, kind int) {
				if kind != strat.OdChgEnterFill && kind != strat.OdChgExitFill {
					return
				}
				if od.Timeframe != tf {
					t.Errorf("callback owned by %s received %s order", tf, od.Timeframe)
				}
				callbacks = append(callbacks, tf)
			}
			return j
		}
		return build("1m"), build("5m")
	}
	fast, slow := jobs()
	manager := f.manager.(*biz.SharedOrderMgr)
	if err := manager.BindJobs([]*strat.StratJob{fast, slow}); err != nil {
		t.Fatal(err)
	}
	enter := func(j *strat.StratJob, req *strat.EnterReq) *ormo.InOutOrder {
		t.Helper()
		if err := j.OpenOrder(req); err != nil {
			t.Fatal(err)
		}
		rows, _, err := manager.ProcessOrders(j)
		if err != nil || len(rows) != 1 {
			t.Fatal("job entry", rows, err)
		}
		return rows[0]
	}
	fastOrder := enter(fast, &strat.EnterReq{Tag: "fast", Amount: 1})
	enter(slow, &strat.EnterReq{Tag: "slow", Amount: 1})
	check := func(j *strat.StratJob, want int) {
		t.Helper()
		snapshot := j.ExecutionSnapshot()
		if len(snapshot.LongOrders) != want {
			t.Fatalf("%s projection count=%d want%d", j.TimeFrame, len(snapshot.LongOrders), want)
		}
		for _, od := range snapshot.LongOrders {
			if od.Timeframe != j.TimeFrame {
				t.Fatal("foreign timeframe projection", j.TimeFrame, od.Timeframe)
			}
		}
	}
	check(fast, 1)
	check(slow, 1)
	if len(callbacks) != 2 || callbacks[0] != "1m" || callbacks[1] != "5m" {
		t.Fatal("wrong callback routing", callbacks)
	}
	// SDK-style zero bars exercise persisted per-job countdown independently.
	original := f.bridge.Quote
	f.bridge.Quote = func(id string, now int64) (q execution.VisibleQuote, err error) {
		q, err = original(id, now)
		q.Bar = 0
		return
	}
	enter(fast, &strat.EnterReq{Tag: "fast-pending", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopBars: 2})
	enter(slow, &strat.EnterReq{Tag: "slow-pending", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopBars: 2})
	emit := func(tf string, end int64) {
		t.Helper()
		if err := manager.UpdateByDataSeries(nil, &orm.DataSeries{Source: "kline", Sid: 1, TimeFrame: tf, EndMS: end, Closed: true, Values: map[string]any{"close": 100.0}}); err != nil {
			t.Fatal(err)
		}
	}
	emit("1m", 101)
	check(fast, 2)
	check(slow, 2)
	f.process.Close()
	p := NewProcess()
	defer p.Close()
	rt, err := p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
	if err != nil {
		t.Fatal(err)
	}
	f.rt = rt
	rt.Clock.SetTimeMS(102)
	if err := rt.SharedExecution().Reconcile("job-restore", 102); err != nil {
		t.Fatal(err)
	}
	before := len(callbacks)
	biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), nil, false)
	manager = biz.GetOdMgrWithState(rt.Trading, "default").(*biz.SharedOrderMgr)
	fast, slow = jobs()
	if err := manager.BindJobs([]*strat.StratJob{fast, slow}); err != nil {
		t.Fatal(err)
	}
	check(fast, 2)
	check(slow, 2)
	if len(callbacks) != before {
		t.Fatal("restoration replayed fills")
	}
	emit("1m", 102)
	check(fast, 1)
	check(slow, 2)
	rt.Clock.SetTimeMS(60103)
	if _, err := manager.ExitOpenOrders("BTC", &strat.ExitReq{StratName: "legacy", OrderID: fastOrder.ID, Tag: "fast-exit"}); err != nil {
		t.Fatal(err)
	}
	check(fast, 0)
	check(slow, 2)
	if len(callbacks) != before+1 || callbacks[len(callbacks)-1] != "1m" {
		t.Fatal("restart exit sent to wrong job", callbacks)
	}
	snapshot, err := rt.SharedExecution().Snapshot(context.Background())
	if err != nil || len(snapshot.Lots) != 1 || snapshot.Lots[0].SignedSteps != 10 {
		t.Fatal("job ownership changed strategy ledger aggregation", snapshot.Lots, err)
	}
}
