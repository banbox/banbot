package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
)

type canceledNonclosingSetupAdapter struct{ *neverClosingReportAdapter }

func (a *canceledNonclosingSetupAdapter) Reports(ctx context.Context) (<-chan execution.BanexgStreamReport, error) {
	<-ctx.Done()
	return a.reports, ctx.Err()
}

func TestSharedPrivateFreezeSurvivesTwoReconciledServiceLifecycles(t *testing.T) {
	for _, setup := range []bool{false, true} {
		name := "stream"
		if setup {
			name = "setup"
		}
		t.Run(name, func(t *testing.T) {
			var adapter *neverClosingReportAdapter
			f := newSharedTriggerFixtureWithAdapter(t, func(paper *runner.PaperAdapter) execution.ExecutionAdapter {
				adapter = &neverClosingReportAdapter{partialRuntimeAdapter: &partialRuntimeAdapter{paper: paper, orders: map[string]*partialRuntimeOrder{}}, reports: make(chan execution.BanexgStreamReport)}
				if setup {
					return &canceledNonclosingSetupAdapter{adapter}
				}
				return adapter
			})
			process, rt := f.process, f.rt
			for cycle := 0; cycle < 2; cycle++ {
				if setup {
					ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
					result := rt.SharedExecution().StartReportsContext(ctx)
					cancel()
					if !errors.Is(result, context.DeadlineExceeded) {
						t.Fatal("setup failure not retained", result)
					}
				} else if err := rt.SharedExecution().StartReports(); err != nil {
					t.Fatal(err)
				}
				process.Close()
				first := process.CloseError()
				if !errors.Is(first, context.DeadlineExceeded) || strings.Contains(first.Error(), "conflict") || strings.Contains(first.Error(), "different content") {
					t.Fatal("shutdown freeze failed or lost cause", cycle, first)
				}
				process.Close()
				if repeat := process.CloseError(); repeat == nil || repeat.Error() != first.Error() {
					t.Fatal("repeat close changed retained shutdown evidence", first, repeat)
				}
				if adapter.closed.Load() != int32(cycle+1) {
					t.Fatal("transport closed twice in one lifecycle", adapter.closed.Load())
				}
				process = NewProcess()
				t.Cleanup(process.Close)
				var err error
				rt, err = process.NewRuntime(Options{AccountOwnerKey: &f.key, SharedExecution: f.opts})
				if err != nil {
					t.Fatal(err)
				}
				state, err := rt.SharedExecution().Snapshot(context.Background())
				if err != nil || !state.RiskFrozen {
					t.Fatal("reopened risk was not frozen", cycle, state, err)
				}
				if cycle == 0 {
					if err := rt.SharedExecution().Reconcile("repeat-service-clear", 200); err != nil {
						t.Fatal(err)
					}
					state, err = rt.SharedExecution().Snapshot(context.Background())
					if err != nil || state.RiskFrozen {
						t.Fatal("test did not clear prior freeze", state, err)
					}
				}
			}
		})
	}
}
