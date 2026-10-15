package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

func TestSharedConcurrentEntryReturnsWhileProjectionCallbackBlocked(t *testing.T) {
	f := newSharedTriggerFixtureWithAdapter(t, nil, true)
	callbackEntered, releaseCallback := make(chan struct{}), make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseCallback)
		}
	}()
	manager, err := biz.NewSharedOrderMgr(f.rt.BizDeps(), f.rt.SharedExecution(), f.bridge, func(od *ormo.InOutOrder, _ bool) {
		if od.EnterTag == "outer" {
			close(callbackEntered)
			<-releaseCallback
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() {
		row, err := manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "1m", &strat.EnterReq{StratName: "legacy", Tag: "outer", Amount: 1})
		if err != nil {
			finished <- err
		} else if row == nil {
			finished <- errors.New("outer entry returned no order")
		} else {
			finished <- nil
		}
	}()
	select {
	case <-callbackEntered:
	case err := <-finished:
		t.Fatal("projection never entered callback", err)
	case <-time.After(10 * time.Second):
		t.Fatal("projection callback timed out")
	}
	row, enterErr := manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "1m", &strat.EnterReq{StratName: "legacy", Tag: "concurrent", Amount: 1})
	if enterErr != nil || row == nil || row.ID == 0 || row.Enter == nil || row.Enter.Filled != 1 {
		t.Fatal("concurrent entry returned no usable committed order", row, enterErr)
	}
	// The concurrent synchronous return does not require the outer callback to end.
	close(releaseCallback)
	released = true
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("outer projection did not drain the concurrent entry")
	}
}

type sharedReviewAckLossAdapter struct{ *partialRuntimeAdapter }

func (a *sharedReviewAckLossAdapter) Submit(ctx context.Context, order execution.OrderIntent, client string) (execution.SubmitReceipt, error) {
	if _, err := a.partialRuntimeAdapter.Submit(ctx, order, client); err != nil {
		return execution.SubmitReceipt{}, err
	}
	return execution.SubmitReceipt{}, errors.New("review: venue accepted but acknowledgement lost")
}

func TestSharedUnknownEntryRetainsCheckpointForRecovery(t *testing.T) {
	var adapter *sharedReviewAckLossAdapter
	f := newSharedTriggerFixtureWithAdapter(t, func(paper *runner.PaperAdapter) execution.ExecutionAdapter {
		adapter = &sharedReviewAckLossAdapter{&partialRuntimeAdapter{paper: paper, orders: map[string]*partialRuntimeOrder{}}}
		return adapter
	})
	row, err := f.manager.EnterOrder(f.job.Symbol, f.job.TimeFrame, &strat.EnterReq{StratName: "legacy", Tag: "unknown", Amount: 8})
	if err == nil || row != nil || len(adapter.trace) != 1 {
		t.Fatal("expected unknown result after exactly one submission", row, err, len(adapter.trace))
	}
	snapshot, snapErr := f.rt.SharedExecution().Snapshot(context.Background())
	if snapErr != nil || len(snapshot.Orders) != 1 || snapshot.Orders[0].State != execution.OrderUnknown {
		t.Fatal("unknown order recovery evidence missing", snapshot, snapErr)
	}
	f.process.Close()
	p := NewProcess()
	t.Cleanup(p.Close)
	f.rt, snapErr = p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
	if snapErr != nil {
		t.Fatal(snapErr)
	}
	f.rt.Clock.SetTimeMS(101)
	if err := f.rt.SharedExecution().RecoverPersisted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.rt.SharedExecution().Reconcile("review-ack-recovery", 101); err != nil {
		t.Fatal(err)
	}
	biz.InitLocalOrderMgrWithRuntimeDeps(f.rt.BizDeps(), nil, false)
	f.manager = biz.GetOdMgrWithState(f.rt.Trading, "default")
	orders, lock := f.rt.Orders.GetOpenODs("default")
	lock.Lock()
	recovered := orders[1]
	lock.Unlock()
	if recovered == nil || recovered.EnterTag != "unknown" || recovered.Enter.Filled != 8 {
		t.Fatal("accepted checkpoint was lost after unknown submission", recovered)
	}
	f.observe(t, 70, 2)
	f.observe(t, 70, 3)
	if steps := f.steps(t); steps != 80 || len(adapter.trace) != 1 {
		t.Fatal("recovery lost attribution or submitted again", steps, len(adapter.trace))
	}
}

func restartSharedReviewFixture(t *testing.T, f *sharedTriggerFixture) {
	t.Helper()
	f.process.Close()
	p := NewProcess()
	t.Cleanup(p.Close)
	rt, err := p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
	if err != nil {
		t.Fatal(err)
	}
	f.process, f.rt = p, rt
	rt.Clock.SetTimeMS(101)
	if err := rt.SharedExecution().Reconcile("restart-review", 101); err != nil {
		t.Fatal(err)
	}
	biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), nil, false)
	f.manager = biz.GetOdMgrWithState(rt.Trading, "default")
}

func TestSharedRejectedEntryDoesNotRevive(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%t", restart), func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			f.entry(t, &strat.EnterReq{Tag: "accepted", Amount: 8})
			row, err := f.manager.EnterOrder(f.job.Symbol, f.job.TimeFrame, &strat.EnterReq{StratName: "legacy", Tag: "rejected", Amount: 5})
			if err == nil || !strings.Contains(err.Error(), "strategy gross budget exceeded") || row != nil {
				t.Fatalf("expected local risk rejection and no order, got row=%v err=%v", row, err)
			}
			if restart {
				restartSharedReviewFixture(t, f)
			}
			f.observe(t, 70, 2)
			f.observe(t, 70, 3)
			snapshot, snapErr := f.rt.SharedExecution().Snapshot(context.Background())
			if snapErr != nil {
				t.Fatal(snapErr)
			}
			if len(snapshot.Lots) != 1 || snapshot.Lots[0].SignedSteps != 80 {
				t.Fatalf("rejected entry revived after market update: %+v", snapshot.Lots)
			}
		})
	}
}

func TestSharedCanceledProtectionDoesNotRevive(t *testing.T) {
	for _, stopLoss := range []bool{false, true} {
		for _, relative := range []bool{false, true} {
			for _, zero := range []bool{false, true} {
				for _, restart := range []bool{false, true} {
					name := fmt.Sprintf("stopLoss=%t/relative=%t/zero=%t/restart=%t", stopLoss, relative, zero, restart)
					t.Run(name, func(t *testing.T) {
						f := newSharedTriggerFixture(t)
						req := &strat.EnterReq{Tag: "cancel-protection", Amount: 8, StopLoss: 95, TakeProfit: 110}
						if relative {
							if stopLoss {
								req.StopLoss, req.StopLossVal = 0, 5
							} else {
								req.TakeProfit, req.TakeProfitVal = 0, 10
							}
						}
						od := f.entry(t, req)
						var cancel *ormo.ExitTrigger
						if zero {
							cancel = &ormo.ExitTrigger{Price: 0}
						}
						if stopLoss {
							if err := od.SetStopLoss(cancel); err != nil {
								t.Fatal(err)
							}
							if od.GetStopLoss() != nil || od.GetTakeProfit() == nil {
								t.Fatal("stop cancellation did not preserve take profit")
							}
						} else {
							if err := od.SetTakeProfit(cancel); err != nil {
								t.Fatal(err)
							}
							if od.GetTakeProfit() != nil || od.GetStopLoss() == nil {
								t.Fatal("take profit cancellation did not preserve stop loss")
							}
						}
						if err := f.manager.(*biz.SharedOrderMgr).LastError(); err != nil {
							t.Fatal(err)
						}
						if restart {
							restartSharedReviewFixture(t, f)
						}
						crossed, remaining := 111.0, 94.0
						if stopLoss {
							crossed, remaining = 94, 111
						}
						f.observe(t, crossed, 2)
						f.observe(t, crossed, 3)
						if steps := f.steps(t); steps != 80 {
							t.Fatalf("canceled protection triggered: steps=%d", steps)
						}
						f.observe(t, remaining, 4)
						if steps := f.steps(t); steps != 0 {
							t.Fatalf("remaining protection lost: steps=%d", steps)
						}
					})
				}
			}
		}
	}
}
