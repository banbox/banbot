package runtime

import (
	"context"
	"errors"
	"fmt"
	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
	"testing"
)

type partialRuntimeOrder struct {
	intent   execution.OrderIntent
	client   string
	fill     execution.FillReport
	canceled bool
}
type partialRuntimeAdapter struct {
	paper                                               *runner.PaperAdapter
	orders                                              map[string]*partialRuntimeOrder
	trace                                               []execution.OrderIntent
	partialEntry, partialDecrease, complete, cancelLoss bool
	cancelFillSteps                                     int64
}

func (a *partialRuntimeAdapter) Capabilities() execution.AdapterCapabilities {
	return execution.AdapterCapabilities{QueryClientID: true, CumulativeReports: true, StableTradeID: true}
}
func (a *partialRuntimeAdapter) Submit(ctx context.Context, o execution.OrderIntent, client string) (execution.SubmitReceipt, error) {
	a.trace = append(a.trace, o)
	part := o
	if a.partialEntry && o.Side == execution.Buy || a.partialDecrease && o.ReduceOnly {
		part.Steps = max(int64(1), o.Steps/2)
	}
	receipt, err := a.paper.Submit(ctx, part, client)
	if err != nil {
		return receipt, err
	}
	fill := receipt.Fills[0]
	fill.EventID += "/partial"
	a.orders[receipt.ExchangeID] = &partialRuntimeOrder{intent: o, client: client, fill: fill}
	receipt.Fills = []execution.FillReport{fill}
	return receipt, nil
}
func (a *partialRuntimeAdapter) query(stored *partialRuntimeOrder) execution.QueryResult {
	fill := stored.fill
	fill.EventID = stored.intent.ID + "/cumulative"
	fill.Cumulative = true
	return execution.QueryResult{Found: true, Authoritative: true, Complete: true, Canceled: stored.canceled, Receipt: execution.SubmitReceipt{ExchangeID: "paper:" + stored.intent.ID, Fills: []execution.FillReport{fill}}}
}
func (a *partialRuntimeAdapter) Query(ctx context.Context, client, id string) (execution.QueryResult, error) {
	stored := a.orders[id]
	if stored == nil {
		for _, o := range a.orders {
			if o.client == client {
				stored = o
				break
			}
		}
	}
	if stored == nil {
		return execution.QueryResult{}, errors.New("unresolved")
	}
	if a.complete && !stored.canceled && stored.fill.Steps < stored.intent.Steps {
		rest := stored.intent
		rest.Steps -= stored.fill.Steps
		rest.ID += "/completion"
		receipt, err := a.paper.Submit(ctx, rest, client+"/completion")
		if err != nil {
			return execution.QueryResult{}, err
		}
		fill := receipt.Fills[0]
		stored.fill.Steps += fill.Steps
		stored.fill.Fee = stored.fill.Fee.Add(fill.Fee)
		stored.fill.Cost = stored.fill.Cost.Add(fill.Cost)
		stored.fill.AtMS = fill.AtMS
	}
	return a.query(stored), nil
}
func (a *partialRuntimeAdapter) Cancel(ctx context.Context, id string) (bool, error) {
	stored := a.orders[id]
	if stored == nil {
		return false, errors.New("unknown cancel")
	}
	if a.cancelFillSteps > 0 && stored.fill.Steps+a.cancelFillSteps <= stored.intent.Steps {
		rest := stored.intent
		rest.ID += "/cancel-fill"
		rest.Steps = a.cancelFillSteps
		receipt, err := a.paper.Submit(ctx, rest, stored.client+"/cancel-fill")
		if err != nil {
			return false, err
		}
		fill := receipt.Fills[0]
		stored.fill.Steps += fill.Steps
		stored.fill.Cost = stored.fill.Cost.Add(fill.Cost)
		stored.fill.Fee = stored.fill.Fee.Add(fill.Fee)
		stored.fill.AtMS = fill.AtMS
		a.cancelFillSteps = 0
	}
	stored.canceled = true
	if a.cancelLoss {
		return false, errors.New("cancel ACK lost")
	}
	return true, nil
}

func TestSharedCancelConcurrentFillRefreshesFacade(t *testing.T) {
	for _, mode := range []string{"filled-only", "unfilled-only", "protection", "expiry"} {
		t.Run(mode, func(t *testing.T) {
			var adapter *partialRuntimeAdapter
			f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
				adapter = &partialRuntimeAdapter{paper: p, orders: map[string]*partialRuntimeOrder{}, partialEntry: true}
				return adapter
			})
			req := &strat.EnterReq{Tag: mode, Amount: 1, OrderType: core.OrderTypeLimit, Limit: 100}
			if mode == "protection" {
				req.StopLoss = 99
			}
			if mode == "expiry" {
				req.StopBars = 2
			}
			od := f.entry(t, req)
			if f.steps(t) != 5 {
				t.Fatal("missing partial entry")
			}
			adapter.cancelFillSteps = 2
			if mode == "protection" || mode == "expiry" {
				price := float64(100)
				if mode == "protection" {
					price = 99
				}
				f.observe(t, price, 3)
			} else {
				_, err := f.manager.ExitOpenOrders("BTC", &strat.ExitReq{Tag: mode, StratName: "legacy", OrderID: od.ID, FilledOnly: mode == "filled-only", UnFillOnly: mode == "unfilled-only"})
				if err != nil {
					t.Fatal(err)
				}
			}
			want := int64(7)
			if mode == "filled-only" || mode == "protection" {
				want = 0
			}
			if got := f.steps(t); got != want {
				t.Fatalf("final fill used stale snapshot: got %d want %d, trace %+v", got, want, adapter.trace)
			}
			if want == 0 && (len(adapter.trace) != 2 || adapter.trace[1].Steps != 7) {
				t.Fatalf("reduction lost concurrent fill: %+v", adapter.trace)
			}
		})
	}
}
func (a *partialRuntimeAdapter) Snapshot(ctx context.Context) (execution.VenueSnapshot, error) {
	snap, err := a.paper.Snapshot(ctx)
	for _, o := range a.orders {
		if !o.canceled && o.fill.Steps < o.intent.Steps {
			snap.OpenOrders = append(snap.OpenOrders, a.query(o))
		}
	}
	return snap, err
}

func TestSharedPartialEntrySelectorsAndCancelRecovery(t *testing.T) {
	for _, filledOnly := range []bool{false, true} {
		name := "unfilled-only"
		if filledOnly {
			name = "filled-only"
		}
		t.Run(name, func(t *testing.T) {
			var adapter *partialRuntimeAdapter
			f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
				adapter = &partialRuntimeAdapter{paper: p, orders: map[string]*partialRuntimeOrder{}, partialEntry: true}
				return adapter
			})
			od := f.entry(t, &strat.EnterReq{Tag: "partial", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 100})
			if od.Enter.Filled != 0.5 || f.steps(t) != 5 || adapter.trace[0].Limit.String() != "100" {
				t.Fatal("partial real limit not preserved", od.Enter, adapter.trace)
			}
			adapter.cancelLoss = true
			req := &strat.ExitReq{Tag: name, StratName: "legacy", OrderID: od.ID, UnFillOnly: !filledOnly, FilledOnly: filledOnly}
			if _, err := f.manager.ExitOpenOrders("BTC", req); err == nil {
				t.Fatal("lost cancellation ACK bypassed")
			}
			if f.steps(t) != 5 || len(adapter.trace) != 1 {
				t.Fatal("unconfirmed cancel created reduction", adapter.trace)
			}
			snap, _ := f.rt.SharedExecution().Snapshot(context.Background())
			if len(snap.Orders) != 1 || snap.Orders[0].State != execution.OrderCancelPending {
				t.Fatal("cancel uncertainty not persisted", snap.Orders)
			}
			orderID := snap.Orders[0].Intent.ID
			f.process.Close()
			p := NewProcess()
			defer p.Close()
			rt, err := p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
			if err != nil {
				t.Fatal(err)
			}
			f.rt = rt
			rt.Clock.SetTimeMS(101)
			if err := rt.SharedExecution().Recover(orderID); err != nil {
				t.Fatal(err)
			}
			if err := rt.SharedExecution().Reconcile("cancel-recovered", 101); err != nil {
				t.Fatal(err)
			}
			replay := 0
			biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), func(*ormo.InOutOrder, bool) { replay++ }, false)
			f.manager = biz.GetOdMgrWithState(rt.Trading, "default")
			adapter.cancelLoss = false
			adapter.partialEntry = false
			if _, err := f.manager.ExitOpenOrders("BTC", req); err != nil {
				t.Fatal(err)
			}
			want := int64(5)
			if filledOnly {
				want = 0
			}
			if f.steps(t) != want {
				t.Fatal("selector changed wrong component", f.steps(t), want)
			}
			if !filledOnly && len(adapter.trace) != 1 {
				t.Fatal("UnFillOnly reduced held lot")
			}
			if filledOnly {
				f.observe(t, 101, 2)
				if f.steps(t) != 0 {
					t.Fatal("pending original limit filled at wrong price")
				}
				f.observe(t, 100, 3)
				if f.steps(t) != 5 {
					t.Fatal("FilledOnly discarded pending remainder")
				}
			}
			if replay > 2 {
				t.Fatal("restart replayed old callbacks", replay)
			}
		})
	}
}

func TestSharedPartialDecreaseGatesFactorReversal(t *testing.T) {
	var adapter *partialRuntimeAdapter
	f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
		adapter = &partialRuntimeAdapter{paper: p, orders: map[string]*partialRuntimeOrder{}}
		return adapter
	})
	od := f.entry(t, &strat.EnterReq{Tag: "stop", Amount: 1, StopLoss: 95})
	if err := f.rt.SharedExecution().CashEvent(execution.CashEvent{ID: "cs-allocation", Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Strategy: "ts", Amount: decimal.NewFromInt(-500)}, {Strategy: "cs", Amount: decimal.NewFromInt(500)}}, AtMS: 101}); err != nil {
		t.Fatal(err)
	}
	i := f.bridge.Instruments["BTC"]
	f.bridge.Risk.StrategyGrossLimits["cs"] = decimal.NewFromInt(1000)
	if err := f.rt.SharedExecution().RegisterRiskPolicy("shared-risk-v1", f.bridge.Risk); err != nil {
		t.Fatal(err)
	}
	q, _ := f.bridge.Quote("BTC", 101)
	risk := f.bridge.Risk
	risk.Marks = map[string]decimal.Decimal{"BTC": decimal.NewFromInt(100)}
	if err := f.rt.SharedExecution().Rebalance(execution.CombinedRebalance{PlanID: "factor-short", DecisionMS: 101, ExpiresMS: 1000, Requests: []execution.InstrumentRebalance{{Instrument: i, Quote: q, Targets: []execution.ExecutableTarget{{Strategy: "ts", Lot: execution.VirtualLotID(od.Info["shared_lot"].(string)), SignedSteps: 10}, {Strategy: "cs", Lot: "factor", SignedSteps: -6}}}}, Risk: risk}, 101); err != nil {
		t.Fatal(err)
	}
	adapter.partialDecrease = true
	f.price = decimal.NewFromInt(90)
	f.rt.Clock.SetTimeMS(102)
	if err := f.manager.UpdateByDataSeries(nil, &orm.DataSeries{Sid: 1, Closed: true, EndMS: 102}); err == nil {
		t.Fatal("partial decrease did not gate increase")
	}
	snap, _ := f.rt.SharedExecution().Snapshot(context.Background())
	if snap.ActualPositions[0].SignedSteps != 2 || len(adapter.trace) != 3 {
		t.Fatal("increase submitted before full decrease", snap, adapter.trace)
	}
	var decrease string
	for _, o := range snap.Orders {
		if o.Intent.ReduceOnly {
			decrease = o.Intent.ID
		}
	}
	if decrease == "" {
		t.Fatal("partial decrease missing")
	}
	adapter.complete = true
	if err := f.rt.SharedExecution().Recover(decrease); err != nil {
		t.Fatal(err)
	}
	if err := f.rt.SharedExecution().Reconcile("complete-decrease", 103); err != nil {
		t.Fatal(err)
	}
	adapter.partialDecrease = false
	f.observe(t, 90, 4)
	snap, _ = f.rt.SharedExecution().Snapshot(context.Background())
	if snap.ActualPositions[0].SignedSteps != -6 {
		t.Fatal("confirmed decrease did not permit factor short", snap.ActualPositions)
	}
}

func TestSharedRestoreDrainsMoreThan512EventsWithoutCallbacks(t *testing.T) {
	var adapter *partialRuntimeAdapter
	f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
		adapter = &partialRuntimeAdapter{paper: p, orders: map[string]*partialRuntimeOrder{}, partialEntry: true}
		return adapter
	})
	f.entry(t, &strat.EnterReq{Tag: "late-old-fill", Amount: 2})
	snap, _ := f.rt.SharedExecution().Snapshot(context.Background())
	orderID := snap.Orders[0].Intent.ID
	f.rt.Stop()
	borrower, err := f.process.NewRuntime(Options{Mode: core.RunModeOther, AccountOwnerKey: &f.key, SharedExecution: f.opts})
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 600; n++ {
		if err := borrower.SharedExecution().CashEvent(execution.CashEvent{ID: fmt.Sprintf("historical/%d", n), Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Strategy: "ts", Amount: decimal.NewFromInt(-1)}, {Amount: decimal.NewFromInt(1)}}, AtMS: 101}); err != nil {
			t.Fatal(err)
		}
	}
	adapter.complete = true
	if err := borrower.SharedExecution().Recover(orderID); err != nil {
		t.Fatal(err)
	}
	if err := borrower.SharedExecution().Reconcile("all-old-events", 102); err != nil {
		t.Fatal(err)
	}
	rt, err := f.process.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
	if err != nil {
		t.Fatal(err)
	}
	rt.Clock.SetTimeMS(103)
	callbacks := 0
	biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), func(*ormo.InOutOrder, bool) { callbacks++ }, false)
	manager := biz.GetOdMgrWithState(rt.Trading, "default")
	if err := manager.UpdateByDataSeries(nil, &orm.DataSeries{Sid: 1, Closed: true, EndMS: 103}); err != nil {
		t.Fatal(err)
	}
	if callbacks != 0 {
		t.Fatal("restore replayed old late-page trade", callbacks)
	}
	rows, lock := rt.Orders.GetOpenODs("default")
	lock.Lock()
	filled := rows[1].Enter.Filled
	lock.Unlock()
	if filled != 2 {
		t.Fatal("restore snapshot did not include old completed fill", filled)
	}
}
