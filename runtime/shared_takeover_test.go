package runtime

import (
	"context"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestSharedExternalTakeoverRestoresStrategyLotAndCanExit(t *testing.T) {
	f := newSharedTriggerFixtureWithAdapter(t, nil, true)
	i := f.bridge.Instruments["BTC"]
	ctx := context.Background()
	// The independent simulated venue trade is authoritative source evidence;
	// discovery and external event recording are separate from strategy sending.
	if _, err := f.adapter.Submit(ctx, execution.OrderIntent{ID: "external", Instrument: i, Side: execution.Buy, Steps: 3, Observation: execution.ExecutionObservation{Price: decimal.NewFromInt(100), AtMS: 100}, SubmitAtMS: 100}, "external-client"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.rt.SharedExecution().ApplyExternalPosition(execution.ExternalPositionEvent{ID: "external-trade", Kind: execution.ExternalCashChange, Instrument: i, Side: execution.Buy, Steps: 3, Price: decimal.NewFromInt(100), AtMS: 100}); err != nil {
		t.Fatal(err)
	}
	f.process.Close()
	f.opts.TakeoverStrategy = "ts"
	newRuntime := func() (*Process, *Runtime, *biz.SharedOrderMgr, *strat.StratJob) {
		t.Helper()
		p := NewProcess()
		t.Cleanup(p.Close)
		rt, err := p.NewRuntime(Options{Config: &config.Config{TakeOverStrat: "legacy"}, Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
		if err != nil {
			t.Fatal(err)
		}
		rt.Clock.SetTimeMS(101)
		if err := rt.SharedExecution().Reconcile("external-restart", 101); err != nil {
			t.Fatal(err)
		}
		biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), nil, false)
		manager := biz.GetOdMgrWithState(rt.Trading, "default").(*biz.SharedOrderMgr)
		job := &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy"}, Symbol: &orm.ExSymbol{ID: 1, Symbol: "BTC"}, TimeFrame: "ws", Account: "default", CloseLong: true, CloseShort: true}
		job.BindRuntimeState(rt.Strategies, rt.Core, rt.Clock)
		job.BindRuntimeMarket(rt.Market.Prices, rt.Clock)
		if err := manager.BindJobs([]*strat.StratJob{job}); err != nil {
			t.Fatal(err)
		}
		return p, rt, manager, job
	}
	p, rt, _, job := newRuntime()
	orders := job.ExecutionSnapshot().LongOrders
	if len(orders) != 1 || orders[0].Enter.Filled != .3 || orders[0].EnterTag != core.EnterTagThird || orders[0].Enter.Average != 100 || orders[0].EnterAt != 101 || orders[0].Strategy != "legacy" || orders[0].Timeframe != "ws" || f.adapter.Metrics().Fills != 1 {
		t.Fatalf("adoption did not restore an independently owned strategy lot without sending: orders=%+v metrics=%+v", orders, f.adapter.Metrics())
	}
	id := orders[0].ID
	p.Close()
	_, rt, manager, job := newRuntime()
	orders = job.ExecutionSnapshot().LongOrders
	if len(orders) != 1 || orders[0].ID != id || f.adapter.Metrics().Fills != 1 {
		t.Fatal("takeover replay changed order identity or sent again", orders)
	}
	if _, err := manager.ExitOpenOrders("BTC", &strat.ExitReq{Tag: "adopted-close", StratName: "legacy", OrderID: id, Force: true, Dirt: core.OdDirtLong}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := rt.SharedExecution().Snapshot(ctx)
	if err != nil || len(snapshot.Lots) != 0 || len(snapshot.ActualPositions) != 1 || snapshot.ActualPositions[0].SignedSteps != 0 || f.adapter.Metrics().Fills != 2 {
		t.Fatal("strategy cannot close adopted inventory through execution", snapshot, err)
	}
}
