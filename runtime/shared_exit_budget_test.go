package runtime

import (
	"context"
	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/strat"
	"testing"
)

func TestSharedExitRequestConsumesOneGlobalBudget(t *testing.T) {
	for _, test := range []struct {
		name         string
		amount, rate float64
		want         int64
		pairs        string
	}{{"absolute", .5, 0, 15, "BTC"}, {"comma-pairs", .5, 0, 15, "BTC,OTHER"}, {"rate-precedes-amount", .25, .5, 10, "BTC"}} {
		t.Run(test.name, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			first := f.entry(t, &strat.EnterReq{Tag: "first", Amount: 1})
			second := f.entry(t, &strat.EnterReq{Tag: "second", Amount: 1})
			if _, err := f.manager.ExitOpenOrders(test.pairs, &strat.ExitReq{StratName: "legacy", Amount: test.amount, ExitRate: test.rate, Tag: "global"}); err != nil {
				t.Fatal(err)
			}
			snapshot, snapshotErr := f.rt.SharedExecution().Snapshot(context.Background())
			if snapshotErr != nil {
				t.Fatal(snapshotErr)
			}
			var got int64
			for _, lot := range snapshot.Lots {
				got += lot.SignedSteps
			}
			if got != test.want {
				t.Fatalf("global exit budget: remaining%d want%d", got, test.want)
			}
			rows, lock := f.rt.Orders.GetOpenODs("default")
			lock.Lock()
			a, b := rows[first.ID], rows[second.ID]
			lock.Unlock()
			if a.Exit == nil || b.Exit != nil {
				t.Fatal("global deterministic legacy order priority lost", a, b)
			}
		})
	}
}

func TestSharedGlobalExitBudgetRefreshesFinalCancelFillAndSurvivesRestart(t *testing.T) {
	var adapter *partialRuntimeAdapter
	f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
		adapter = &partialRuntimeAdapter{paper: p, orders: map[string]*partialRuntimeOrder{}, partialEntry: true}
		return &feedRevisionAdapter{adapter}
	})
	first := f.entry(t, &strat.EnterReq{Tag: "mixed", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 100})
	second := f.entry(t, &strat.EnterReq{Tag: "mixed", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
	adapter.cancelFillSteps = 2
	adapter.partialEntry = false
	if _, err := f.manager.ExitOpenOrders("BTC", &strat.ExitReq{StratName: "legacy", EnterTag: "mixed", Amount: .7, FilledOnly: true, Tag: "global"}); err != nil {
		t.Fatal(err)
	}
	if len(adapter.trace) != 2 || adapter.trace[1].Steps != 7 {
		t.Fatal("global budget ignored authoritative .5 to .7 cancel fill", adapter.trace)
	}
	rows, lock := f.rt.Orders.GetOpenODs("default")
	lock.Lock()
	a, b := rows[first.ID], rows[second.ID]
	lock.Unlock()
	if a.Enter.Filled != .7 || b.Enter.Amount != 1 || b.Exit != nil {
		t.Fatal("global mixed budget changed second lot", a, b)
	}
	f.process.Close()
	p := NewProcess()
	defer p.Close()
	rt, err := p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
	if err != nil {
		t.Fatal(err)
	}
	rt.Clock.SetTimeMS(101)
	if err := rt.SharedExecution().RecoverPersisted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := rt.SharedExecution().Reconcile("global-budget-restart", 101); err != nil {
		t.Fatal(err)
	}
	biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), nil, false)
	manager := biz.GetOdMgrWithState(rt.Trading, "default")
	before := len(adapter.trace)
	// A repeated cancellation of already-consumed pending quantity cannot
	// manufacture a reduction or consume another lot's request budget.
	if _, err := manager.ExitOpenOrders("BTC", &strat.ExitReq{StratName: "legacy", OrderID: first.ID, Amount: .2, UnFillOnly: true}); err != nil {
		t.Fatal(err)
	}
	if len(adapter.trace) != before {
		t.Fatal("restart duplicate cancellation emitted a trade", adapter.trace)
	}
}

func TestSharedGlobalExitRejectsInvalidOriginalBeforeNormalization(t *testing.T) {
	f := newSharedTriggerFixture(t)
	f.entry(t, &strat.EnterReq{Tag: "held", Amount: 1})
	for _, req := range []*strat.ExitReq{{Amount: .5, ExitRate: 2}, {Amount: .5, OrderType: 99}, {FilledOnly: true, UnFillOnly: true}} {
		if _, err := f.manager.ExitOpenOrders("BTC", req); err == nil {
			t.Fatal("invalid original request bypassed by local budget", req)
		}
	}
	if f.steps(t) != 10 {
		t.Fatal("invalid request changed held position")
	}
}

func TestSharedGlobalLimitExitRetainsLegacyFilledPriority(t *testing.T) {
	for _, takeProfit := range []bool{false, true} {
		name, limit := "ordinary", float64(95)
		if takeProfit {
			name, limit = "take-profit", 110
		}
		t.Run(name, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			pending := f.entry(t, &strat.EnterReq{Tag: "pending", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
			f.entry(t, &strat.EnterReq{Tag: "held", Amount: 1})
			if _, err := f.manager.ExitOpenOrders("BTC", &strat.ExitReq{Amount: .5, Limit: limit, OrderType: core.OrderTypeLimit, Dirt: core.OdDirtLong}); err != nil {
				t.Fatal(err)
			}
			if takeProfit {
				f.observe(t, 110, 2)
			}
			wantHeld, wantPending := int64(10), .5
			if takeProfit {
				wantHeld, wantPending = 5, 1
			}
			if got := f.steps(t); got != wantHeld {
				t.Fatal("legacy limit priority changed held lot", got, wantHeld)
			}
			rows, lock := f.rt.Orders.GetOpenODs("default")
			lock.Lock()
			amount := rows[pending.ID].Enter.Amount
			lock.Unlock()
			if amount != wantPending {
				t.Fatal("legacy limit priority changed pending lot", amount, wantPending)
			}
		})
	}
}

func TestSharedUnfilledGlobalBudgetPreservesHeldAndOtherPendingLots(t *testing.T) {
	f := newSharedTriggerFixture(t)
	f.entry(t, &strat.EnterReq{Tag: "held", Amount: 1})
	first := f.entry(t, &strat.EnterReq{Tag: "pending", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
	second := f.entry(t, &strat.EnterReq{Tag: "pending", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
	if _, err := f.manager.ExitOpenOrders("BTC", &strat.ExitReq{StratName: "legacy", EnterTag: "pending", Amount: .5, UnFillOnly: true, Tag: "cancel-budget"}); err != nil {
		t.Fatal(err)
	}
	if f.steps(t) != 10 {
		t.Fatal("unfilled selector reduced held lot")
	}
	plan, err := f.rt.SharedExecution().LatestPlan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	targets := map[string]int64{}
	for _, target := range plan.Targets {
		targets[string(target.Lot)] = target.SignedSteps
	}
	rows, lock := f.rt.Orders.GetOpenODs("default")
	lock.Lock()
	a, b := rows[first.ID], rows[second.ID]
	lock.Unlock()
	if targets[a.Info["shared_lot"].(string)] != 0 || targets[b.Info["shared_lot"].(string)] != 0 {
		t.Fatal("noneligible limit emitted entry target")
	}
	// Original pending quantities are exposed by the restored legacy facade.
	if a.Enter.Amount != .5 || b.Enter.Amount != 1 {
		t.Fatal("global pending cancellation multiplied budget", a.Enter.Amount, b.Enter.Amount)
	}
}
