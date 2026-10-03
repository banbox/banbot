package runtime

import (
	"context"
	"fmt"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestSharedProjectionDrainsFillPagesAndReentrantTail(t *testing.T) {
	var adapter *partialRuntimeAdapter
	f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
		adapter = &partialRuntimeAdapter{paper: p, orders: map[string]*partialRuntimeOrder{}, partialEntry: true}
		return adapter
	}, true)
	f.price = decimal.NewFromInt(1)
	f.rt.Market.Prices.SetBarPriceAt(100, "BTC", 1)
	var manager *biz.SharedOrderMgr
	var managerEvents []*ormo.InOutOrder
	jobEvents := map[string][]*ormo.InOutOrder{}
	var inject bool
	deps := f.rt.BizDeps()
	var err error
	manager, err = biz.NewSharedOrderMgr(deps, deps.SharedExecution, deps.SharedOrderBridge, func(od *ormo.InOutOrder, _ bool) {
		managerEvents = append(managerEvents, od.Clone())
		if inject {
			inject = false
			if row, err := manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "1m", &strat.EnterReq{StratName: "legacy", Tag: "callback-tail", Amount: 1}); err != nil {
				t.Error(err)
			} else if row == nil || row.ID == 0 || row.Enter == nil || row.Enter.Amount != 1 || row.Enter.Filled != .5 {
				t.Errorf("nested entry returned no usable committed order: %+v", row)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	f.manager = manager
	var jobs []*strat.StratJob
	for _, tf := range []string{"1m", "5m"} {
		job := &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy"}, Symbol: &orm.ExSymbol{ID: 1, Symbol: "BTC"}, TimeFrame: tf, Account: "default"}
		job.BindRuntimeState(f.rt.Strategies, f.rt.Core, f.rt.Clock)
		job.BindRuntimeMarket(f.rt.Market.Prices, f.rt.Clock)
		job.Strat.OnOrderChange = func(job *strat.StratJob, od *ormo.InOutOrder, kind int) {
			if kind != strat.OdChgEnterFill && kind != strat.OdChgExitFill {
				return
			}
			if od.Timeframe != job.TimeFrame {
				t.Error("wrong timeframe owner", job.TimeFrame, od.Timeframe)
			}
			jobEvents[job.TimeFrame] = append(jobEvents[job.TimeFrame], od.Clone())
		}
		jobs = append(jobs, job)
	}
	if err := manager.BindJobs(jobs); err != nil {
		t.Fatal(err)
	}
	for _, tf := range []string{"1m", "5m"} {
		if _, err := manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, tf, &strat.EnterReq{StratName: "legacy", Tag: tf, Amount: 60}); err != nil {
			t.Fatal(err)
		}
	}
	if len(managerEvents) != 2 || len(adapter.trace) != 2 {
		t.Fatal("initial native fills missing", len(managerEvents), len(adapter.trace))
	}
	for n := 1; n <= 300; n++ {
		for orderIndex, order := range adapter.trace[:2] {
			stored := adapter.orders["paper:"+order.ID]
			part := order
			part.ID = fmt.Sprintf("page-paper/%d/%d", orderIndex, n)
			part.Steps = 1
			if _, err := adapter.paper.Submit(context.Background(), part, part.ID); err != nil {
				t.Fatal(err)
			}
			stored.fill.Steps++
			stored.fill.Cost = stored.fill.Cost.Add(decimal.RequireFromString("0.1"))
			stored.fill.Fee = stored.fill.Fee.Add(decimal.RequireFromString("0.001"))
			adapter.paper.ApplyCash(decimal.RequireFromString("-0.001"))
			report := execution.FillReport{EventID: fmt.Sprintf("page-fill/%d/%d", orderIndex, n), OrderID: order.ID, Steps: stored.fill.Steps, Price: decimal.NewFromInt(1), Cost: stored.fill.Cost, Fee: stored.fill.Fee, Cumulative: true, AuthoritativeSnapshot: true, AtMS: 103}
			if err := f.rt.SharedExecution().ApplyTrade(report); err != nil {
				t.Fatal(err)
			}
		}
		if n == 128 {
			order := adapter.trace[0]
			stored := adapter.orders["paper:"+order.ID]
			stored.fill.Fee = stored.fill.Fee.Add(decimal.RequireFromString("0.01"))
			adapter.paper.ApplyCash(decimal.RequireFromString("-0.01"))
			report := execution.FillReport{EventID: "page-boundary-fee-correction", OrderID: order.ID, Steps: stored.fill.Steps, Price: decimal.NewFromInt(1), Cost: stored.fill.Cost, Fee: stored.fill.Fee, Cumulative: true, AtMS: 103}
			for k := 0; k < 2; k++ {
				if err := f.rt.SharedExecution().ApplyTrade(report); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if err := f.rt.SharedExecution().Reconcile("page-batch-ready", 103); err != nil {
		t.Fatal(err)
	}
	inject = true
	f.observe(t, 1, 4)
	if len(managerEvents) != 603 || len(jobEvents["1m"]) != 302 || len(jobEvents["5m"]) != 301 {
		t.Fatal("later page or reentrant tail lost", len(managerEvents), len(jobEvents["1m"]), len(jobEvents["5m"]))
	}
	seen := map[string]bool{}
	for _, events := range [][]*ormo.InOutOrder{managerEvents, jobEvents["1m"], jobEvents["5m"]} {
		for _, od := range events {
			id, _ := od.Info["shared_event_id"].(string)
			if events[0] == managerEvents[0] {
				if seen[id] {
					t.Fatal("duplicate event callback", id)
				}
				seen[id] = true
			}
			if od.EnterTag == "callback-tail" {
				if od.Enter.Filled != .5 {
					t.Fatal("tail snapshot invalid", od.Enter)
				}
				continue
			}
			if od.Enter.Filled == 30 {
				continue
			}
			n := decimal.NewFromFloat(od.Enter.Filled).Sub(decimal.NewFromInt(30)).Div(decimal.RequireFromString("0.1")).IntPart()
			want := decimal.NewFromInt(n).Mul(decimal.RequireFromString("0.001"))
			if od.Timeframe == "1m" && n > 128 {
				want = want.Add(decimal.RequireFromString("0.01"))
			}
			if od.Info["shared_event_fee"] != "0.001" || od.Info["shared_cumulative_fee"] != want.String() || !decimal.NewFromFloat(od.Enter.FeeQuote).Equal(want) {
				t.Fatal("page event-time fee/highwater lost", od.Enter, od.Info, want)
			}
		}
	}
	if managerEvents[len(managerEvents)-1].EnterTag != "callback-tail" {
		t.Fatal("new tail crossed captured snapshot cutoff")
	}
	before := len(managerEvents)
	f.observe(t, 1, 5)
	if len(managerEvents) != before {
		t.Fatal("page cursor replayed callbacks")
	}
	f.process.Close()
	f.process = NewProcess()
	t.Cleanup(f.process.Close)
	f.rt, err = f.process.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rt.SharedExecution().RecoverPersisted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.rt.SharedExecution().Reconcile("page-restart", 106); err != nil {
		t.Fatal(err)
	}
	biz.InitLocalOrderMgrWithRuntimeDeps(f.rt.BizDeps(), func(od *ormo.InOutOrder, _ bool) { managerEvents = append(managerEvents, od.Clone()) }, false)
	manager = biz.GetOdMgrWithState(f.rt.Trading, "default").(*biz.SharedOrderMgr)
	for _, job := range jobs {
		job.BindRuntimeState(f.rt.Strategies, f.rt.Core, f.rt.Clock)
		job.BindRuntimeMarket(f.rt.Market.Prices, f.rt.Clock)
	}
	if err := manager.BindJobs(jobs); err != nil {
		t.Fatal(err)
	}
	if len(managerEvents) != 603 || len(jobEvents["1m"]) != 302 || len(jobEvents["5m"]) != 301 {
		t.Fatal("restart replayed historical callbacks")
	}
}
