package runtime

import (
	"context"
	"errors"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"testing"
	"time"
)

type blockedSetupReportAdapter struct {
	*sharedTestAdapter
	started  chan struct{}
	returned chan struct{}
}

func (a *blockedSetupReportAdapter) Reports(ctx context.Context) (<-chan execution.BanexgStreamReport, error) {
	close(a.started)
	<-ctx.Done()
	close(a.returned)
	return nil, ctx.Err()
}

func TestSharedPrivateReportSetupIsBoundedWithoutCallerDeadline(t *testing.T) {
	adapter := &blockedSetupReportAdapter{sharedTestAdapter: &sharedTestAdapter{}, started: make(chan struct{}), returned: make(chan struct{})}
	key, opts := sharedTestOptions(t, adapter.sharedTestAdapter)
	opts.Adapter = adapter
	p := NewProcess()
	defer p.Close()
	rt, err := p.NewRuntime(Options{AccountOwnerKey: &key, SharedExecution: opts})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- rt.SharedExecution().StartReportsContext(context.Background()) }()
	<-adapter.started
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("setup bound evidence missing", err)
		}
	case <-time.After(17 * time.Second):
		t.Fatal("report setup without caller deadline hung")
	}
	select {
	case <-adapter.returned:
	default:
		t.Fatal("setup returned before accepted IO joined")
	}
}

func TestSharedAccountCloseCancelsPrivateSetupBeforeMutexWait(t *testing.T) {
	adapter := &blockedSetupReportAdapter{sharedTestAdapter: &sharedTestAdapter{}, started: make(chan struct{}), returned: make(chan struct{})}
	key, opts := sharedTestOptions(t, adapter.sharedTestAdapter)
	opts.Adapter = adapter
	p := NewProcess()
	defer p.Close()
	rt, err := p.NewRuntime(Options{AccountOwnerKey: &key, SharedExecution: opts})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- rt.SharedExecution().StartReports() }()
	<-adapter.started
	closed := make(chan error, 1)
	go func() { closed <- rt.SharedExecution().Service().Close() }()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close waited mutex before canceling setup")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close failed to join setup")
	}
	if adapter.closed.Load() != 1 {
		t.Fatal("adapter closed before setup join or twice")
	}
}

func TestSharedAcceptedReportsSurviveStartupCallerAndBorrowerStop(t *testing.T) {
	var adapter *reportRuntimeAdapter
	f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
		adapter = &reportRuntimeAdapter{PaperAdapter: p, input: make(chan execution.BanexgStreamReport, 4), joined: make(chan struct{})}
		return adapter
	})
	sibling, err := f.process.NewRuntime(Options{AccountOwnerKey: &f.key, SharedExecution: f.opts})
	if err != nil {
		t.Fatal(err)
	}
	caller, cancel := context.WithCancel(context.Background())
	if err := f.rt.SharedExecution().StartReportsContext(caller); err != nil {
		t.Fatal(err)
	}
	cancel()
	f.rt.Stop()
	if err := sibling.SharedExecution().StartReports(); err != nil {
		t.Fatal(err)
	}
	if adapter.starts.Load() != 1 {
		t.Fatal("sibling started another private stream")
	}
	adapter.input <- execution.BanexgStreamReport{UnassignedExchangeID: "manual-after-startup-stop"}
	deadline := time.Now().Add(time.Second)
	for {
		snapshot, err := sibling.SharedExecution().Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.RiskFrozen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("accepted stream stopped with startup borrower")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-adapter.joined:
		t.Fatal("private stream canceled by borrower stop")
	default:
	}
}
