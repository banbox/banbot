package runtime

import (
	"context"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestSharedEntryFeeCorrectionAccumulatesWithoutQuantityCallback(t *testing.T) {
	var adapter *partialRuntimeAdapter
	f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
		adapter = &partialRuntimeAdapter{paper: p, orders: map[string]*partialRuntimeOrder{}, partialEntry: true}
		return adapter
	}, true)
	var managerEvents, jobEvents []*ormo.InOutOrder
	deps := f.rt.BizDeps()
	manager, err := biz.NewSharedOrderMgr(deps, deps.SharedExecution, deps.SharedOrderBridge, func(od *ormo.InOutOrder, entry bool) {
		if !entry {
			t.Error("entry callback marked exit")
		}
		managerEvents = append(managerEvents, od.Clone())
	})
	if err != nil {
		t.Fatal(err)
	}
	f.manager = manager
	f.job.Strat.OnOrderChange = func(_ *strat.StratJob, od *ormo.InOutOrder, kind int) {
		if kind == strat.OdChgEnter || kind == strat.OdChgOrderChanged {
			return
		}
		if kind != strat.OdChgEnterFill {
			t.Error("wrong kind", kind)
		}
		jobEvents = append(jobEvents, od.Clone())
	}
	if err := manager.BindJobs([]*strat.StratJob{f.job}); err != nil {
		t.Fatal(err)
	}
	f.entry(t, &strat.EnterReq{Tag: "corrected-entry", Amount: 1})
	if len(managerEvents) != 1 || len(jobEvents) != 1 {
		t.Fatal("initial fill callback missing")
	}
	order := adapter.trace[0]
	var stored *partialRuntimeOrder
	for _, o := range adapter.orders {
		stored = o
	}
	correction := execution.FillReport{EventID: "entry-fee-correction", OrderID: order.ID, Steps: 5, Price: decimal.NewFromInt(100), Cost: decimal.NewFromInt(50), Fee: decimal.RequireFromString("0.01"), Cumulative: true, AuthoritativeSnapshot: true, AtMS: 102}
	for n := 0; n < 2; n++ {
		if err := f.rt.SharedExecution().ApplyTrade(correction); err != nil {
			t.Fatal(err)
		}
	}
	adapter.paper.ApplyCash(decimal.RequireFromString("-0.01"))
	stored.fill.Fee = correction.Fee
	// These cash/funding rows carry no order allocation and must not be
	// attributed as another entry or exit fee correction.
	fee := execution.CashEvent{ID: "strategy-cash-fee", Kind: execution.Fee, AccountDelta: decimal.RequireFromString("-0.04"), Postings: []execution.CashPosting{{Strategy: "ts", Amount: decimal.RequireFromString("-0.04")}}, AtMS: 102}
	if err := f.rt.SharedExecution().CashEvent(fee); err != nil {
		t.Fatal(err)
	}
	adapter.paper.ApplyCash(fee.AccountDelta)
	funding := execution.FundingSettlement{ID: "entry-funding", Instrument: f.bridge.Instruments["BTC"], Mark: decimal.NewFromInt(100), Rate: decimal.RequireFromString("0.001"), AccountAmount: decimal.RequireFromString("-0.05"), AtMS: 102}
	if _, err := f.rt.SharedExecution().ApplyFunding(funding); err != nil {
		t.Fatal(err)
	}
	adapter.paper.ApplyCash(funding.AccountAmount)
	if len(managerEvents) != 1 || len(jobEvents) != 1 {
		t.Fatal("fee-only rows emitted quantity callbacks")
	}
	rest := order
	rest.ID += "/fee-completion"
	rest.Steps = 2
	if _, err := adapter.paper.Submit(context.Background(), rest, rest.ID); err != nil {
		t.Fatal(err)
	}
	stored.fill.Steps = 7
	stored.fill.Cost = decimal.NewFromInt(70)
	stored.fill.Fee = decimal.RequireFromString("0.03")
	adapter.paper.ApplyCash(decimal.RequireFromString("-0.02"))
	fill := correction
	fill.EventID = "entry-next-fill"
	fill.Steps, fill.Cost, fill.Fee, fill.AtMS = 7, stored.fill.Cost, stored.fill.Fee, 103
	if err := f.rt.SharedExecution().ApplyTrade(fill); err != nil {
		t.Fatal(err)
	}
	if err := f.rt.SharedExecution().Reconcile("entry-correction-clear", 103); err != nil {
		t.Fatal(err)
	}
	f.observe(t, 100, 4)
	for _, events := range [][]*ormo.InOutOrder{managerEvents, jobEvents} {
		if len(events) != 2 || events[1].Enter.Filled != .7 || events[1].Enter.FeeQuote != .03 || events[1].Info["shared_event_fee"] != "0.02" || events[1].Info["shared_cumulative_fee"] != "0.03" {
			t.Fatal("entry correction not accumulated exactly", events)
		}
	}
	if err := f.rt.SharedExecution().ApplyTrade(fill); err != nil {
		t.Fatal(err)
	}
	f.observe(t, 100, 5)
	if len(managerEvents) != 2 || len(jobEvents) != 2 {
		t.Fatal("duplicate fill replayed callback")
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
	if err := f.rt.SharedExecution().Reconcile("entry-correction-restart", 106); err != nil {
		t.Fatal(err)
	}
	biz.InitLocalOrderMgrWithRuntimeDeps(f.rt.BizDeps(), func(od *ormo.InOutOrder, _ bool) { managerEvents = append(managerEvents, od.Clone()) }, false)
	manager = biz.GetOdMgrWithState(f.rt.Trading, "default").(*biz.SharedOrderMgr)
	f.job.BindRuntimeState(f.rt.Strategies, f.rt.Core, f.rt.Clock)
	f.job.BindRuntimeMarket(f.rt.Market.Prices, f.rt.Clock)
	if err := manager.BindJobs([]*strat.StratJob{f.job}); err != nil {
		t.Fatal(err)
	}
	if len(managerEvents) != 2 || len(jobEvents) != 2 {
		t.Fatal("entry correction restart replayed historical callbacks")
	}
}
