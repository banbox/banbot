package runtime

import (
	"context"
	"errors"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/strat"
	"sync/atomic"
	"testing"
	"time"
)

// This hostile source owns an inert channel, not an external goroutine. Close
// proves the core barrier; real adapters must additionally join their transport.
type neverClosingReportAdapter struct {
	*partialRuntimeAdapter
	reports chan execution.BanexgStreamReport
	closed  atomic.Int32
}

func (a *neverClosingReportAdapter) Reports(context.Context) (<-chan execution.BanexgStreamReport, error) {
	return a.reports, nil
}
func (a *neverClosingReportAdapter) Close() error { a.closed.Add(1); return nil }

func TestSharedPrivateNeverClosingSourceHasBoundedCloseAndRecoveryMarker(t *testing.T) {
	for _, processClose := range []bool{false, true} {
		name := "service"
		if processClose {
			name = "process"
		}
		t.Run(name, func(t *testing.T) { testSharedPrivateNeverClosingSource(t, processClose) })
	}
}

func testSharedPrivateNeverClosingSource(t *testing.T, processClose bool) {
	var adapter *neverClosingReportAdapter
	f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
		adapter = &neverClosingReportAdapter{partialRuntimeAdapter: &partialRuntimeAdapter{paper: p, orders: map[string]*partialRuntimeOrder{}, partialEntry: true}, reports: make(chan execution.BanexgStreamReport, 1)}
		return adapter
	})
	f.entry(t, &strat.EnterReq{Tag: "partial", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 100})
	account := f.rt.SharedExecution()
	snapshot, err := account.Snapshot(context.Background())
	if err != nil || len(snapshot.Orders) != 1 {
		t.Fatal(snapshot.Orders, err)
	}
	orderID := snapshot.Orders[0].Intent.ID
	if err := account.StartReports(); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		if processClose {
			f.process.Close()
			f.process.runtimeMu.Lock()
			result <- f.process.closeErr
			f.process.runtimeMu.Unlock()
		} else {
			result <- account.Service().Close()
		}
	}()
	// A cancellation-time recovery hint never releases the durable uncertain
	// remainder without an accepted authoritative cumulative query.
	adapter.reports <- execution.BanexgStreamReport{OrderID: orderID}
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("source join timeout evidence lost", err)
		}
	case <-time.After(17 * time.Second):
		t.Fatal("nonclosing report source hung account Close")
	}
	if adapter.closed.Load() != 1 {
		t.Fatal("adapter close count", adapter.closed.Load())
	}
	if err := account.Service().Close(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("repeat close lost source timeout", err)
	}
	if adapter.closed.Load() != 1 {
		t.Fatal("repeat close doubled transport close")
	}
	f.process.Close()
	p := NewProcess()
	defer p.Close()
	rt, err := p.NewRuntime(Options{AccountOwnerKey: &f.key, SharedExecution: f.opts})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := rt.SharedExecution().Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !restored.RiskFrozen || len(restored.Orders) != 1 || restored.Orders[0].Intent.ID != orderID || restored.Orders[0].FilledSteps != 5 {
		t.Fatal("incomplete shutdown lost recovery marker/order highwater", restored)
	}
}
