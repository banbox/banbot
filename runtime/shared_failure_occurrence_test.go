package runtime

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/strat"
)

type unmatchedReportSnapshotAdapter struct {
	*reportRuntimeAdapter
	unmatched atomic.Bool
	orderID   string
}

func (a *unmatchedReportSnapshotAdapter) Submit(ctx context.Context, order execution.OrderIntent, client string) (execution.SubmitReceipt, error) {
	a.orderID = order.ID
	return a.PaperAdapter.Submit(ctx, order, client)
}

func (a *unmatchedReportSnapshotAdapter) Snapshot(ctx context.Context) (execution.VenueSnapshot, error) {
	snapshot, err := a.PaperAdapter.Snapshot(ctx)
	if a.unmatched.Load() {
		snapshot.OpenOrders = append(snapshot.OpenOrders, execution.QueryResult{Found: true, Authoritative: true, Complete: true, Receipt: execution.SubmitReceipt{ExchangeID: "unmatched-native"}})
	}
	return snapshot, err
}

type closedOccurrenceReportAdapter struct {
	*runner.PaperAdapter
	source chan execution.BanexgStreamReport
}

func (a *closedOccurrenceReportAdapter) Reports(context.Context) (<-chan execution.BanexgStreamReport, error) {
	return a.source, nil
}
func (a *closedOccurrenceReportAdapter) Close() error { return nil }

func requireOccurrenceFrozen(t *testing.T, account *biz.SharedAccountBorrow, err error) {
	t.Helper()
	if err == nil || strings.Contains(err.Error(), "different content") {
		t.Fatal("failure evidence missing or immutable event conflict", err)
	}
	state, snapErr := account.Snapshot(context.Background())
	if snapErr != nil || !state.RiskFrozen {
		t.Fatal("failure did not persist risk freeze", state, snapErr)
	}
}

func TestSharedInternalFailureOccurrencesRefreezeAfterReconcile(t *testing.T) {
	for _, report := range []execution.BanexgStreamReport{{UnassignedExchangeID: "unknown"}, {Err: errors.New("source transport error")}, {OrderID: "missing-order"}} {
		name := report.UnassignedExchangeID + report.OrderID
		if name == "" {
			name = "report-error"
		}
		t.Run(name, func(t *testing.T) {
			var adapter *reportRuntimeAdapter
			f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
				adapter = &reportRuntimeAdapter{PaperAdapter: p, input: make(chan execution.BanexgStreamReport, 4), joined: make(chan struct{})}
				return adapter
			})
			account := f.rt.SharedExecution()
			if err := account.StartReports(); err != nil {
				t.Fatal(err)
			}
			failures, err := account.ReportErrors()
			if err != nil {
				t.Fatal(err)
			}
			for cycle := 0; cycle < 2; cycle++ {
				adapter.input <- report
				select {
				case failure := <-failures:
					requireOccurrenceFrozen(t, account, failure)
				case <-time.After(2 * time.Second):
					t.Fatal("failure swallowed")
				}
				if cycle == 0 {
					if err := account.Reconcile("occurrence-clear", 200); err != nil {
						t.Fatal(err)
					}
				}
			}
			f.process.Close()
			f.process = NewProcess()
			t.Cleanup(f.process.Close)
			rt, err := f.process.NewRuntime(Options{AccountOwnerKey: &f.key, SharedExecution: f.opts})
			if err != nil {
				t.Fatal(err)
			}
			requireOccurrenceFrozen(t, rt.SharedExecution(), errors.New("persisted report failure"))
		})
	}
	t.Run("report-reconcile-error", func(t *testing.T) {
		var adapter *unmatchedReportSnapshotAdapter
		f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
			adapter = &unmatchedReportSnapshotAdapter{reportRuntimeAdapter: &reportRuntimeAdapter{PaperAdapter: p, input: make(chan execution.BanexgStreamReport, 4), joined: make(chan struct{})}}
			return adapter
		})
		f.entry(t, &strat.EnterReq{Tag: "stream-reconcile", Amount: 1})
		account := f.rt.SharedExecution()
		if err := account.StartReports(); err != nil {
			t.Fatal(err)
		}
		failures, err := account.ReportErrors()
		if err != nil {
			t.Fatal(err)
		}
		for cycle := 0; cycle < 2; cycle++ {
			adapter.unmatched.Store(true)
			adapter.input <- execution.BanexgStreamReport{OrderID: adapter.orderID}
			select {
			case failure := <-failures:
				requireOccurrenceFrozen(t, account, failure)
			case <-time.After(2 * time.Second):
				t.Fatal("reconcile failure swallowed")
			}
			adapter.unmatched.Store(false)
			if cycle == 0 {
				if err := account.Reconcile("clear-unmatched-report", 200); err != nil {
					t.Fatal(err)
				}
			}
		}
	})
	t.Run("closed-stream-lifecycles", func(t *testing.T) {
		var adapter *closedOccurrenceReportAdapter
		f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
			adapter = &closedOccurrenceReportAdapter{PaperAdapter: p, source: make(chan execution.BanexgStreamReport)}
			return adapter
		})
		for cycle := 0; cycle < 2; cycle++ {
			account := f.rt.SharedExecution()
			if err := account.StartReports(); err != nil {
				t.Fatal(err)
			}
			failures, err := account.ReportErrors()
			if err != nil {
				t.Fatal(err)
			}
			close(adapter.source)
			select {
			case failure := <-failures:
				requireOccurrenceFrozen(t, account, failure)
			case <-time.After(2 * time.Second):
				t.Fatal("closed stream failure swallowed")
			}
			f.process.Close()
			f.process = NewProcess()
			t.Cleanup(f.process.Close)
			adapter.source = make(chan execution.BanexgStreamReport)
			f.rt, err = f.process.NewRuntime(Options{AccountOwnerKey: &f.key, SharedExecution: f.opts})
			if err != nil {
				t.Fatal(err)
			}
			requireOccurrenceFrozen(t, f.rt.SharedExecution(), errors.New("persisted closed stream failure"))
			if cycle == 0 {
				if err := f.rt.SharedExecution().Reconcile("clear-closed-stream", 200); err != nil {
					t.Fatal(err)
				}
			}
		}
	})
	t.Run("manager-same-clock", func(t *testing.T) {
		f := newSharedTriggerFixture(t)
		od := f.entry(t, &strat.EnterReq{Tag: "edit", Amount: 1})
		manager := f.manager.(*biz.SharedOrderMgr)
		for cycle := 0; cycle < 2; cycle++ {
			f.rt.Clock.SetTimeMS(100)
			manager.EditOrder(od, "unsupported-action")
			requireOccurrenceFrozen(t, f.rt.SharedExecution(), manager.LastError())
			if cycle == 0 {
				if err := f.rt.SharedExecution().Reconcile("edit-clear", 101); err != nil {
					t.Fatal(err)
				}
			}
		}
	})
	t.Run("runtime-same-clock", func(t *testing.T) {
		f := newSharedTriggerFixture(t)
		failures := make(chan error, 2)
		for cycle := 0; cycle < 2; cycle++ {
			f.rt.Clock.SetTimeMS(100)
			f.rt.failFactorLive(nil, nil, failures, errors.New("same factor source failure"))
			account := f.rt.SharedExecution()
			// A stopped runtime releases its borrow; a sibling owns inspection.
			sibling, err := f.process.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
			if err != nil {
				t.Fatal(err)
			}
			account = sibling.SharedExecution()
			requireOccurrenceFrozen(t, account, <-failures)
			if cycle == 0 {
				if err := account.Reconcile("factor-clear", 101); err != nil {
					t.Fatal(err)
				}
				f.rt = sibling
			}
		}
	})
}
