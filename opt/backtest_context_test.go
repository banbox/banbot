package opt

import (
	"context"
	"strings"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banexg/errs"
)

func TestBackTestCallerCancellationAfterProviderLoopIsFailure(t *testing.T) {
	process := runtime.NewProcess()
	defer process.Close()
	runner := newR1BacktestRunner(t, process, t.Name(), 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner.hook = func(*r1BacktestRunner) { cancel() }
	if !runner.feed([]r1BacktestBar{{timeMS: 101, price: 100}}) {
		t.Fatal("fixture did not consume the final provider event")
	}
	// Providers can return nil on cancellation. Completion must still fail
	// before synthesizing end-of-range closes or publishing a full report.
	b := &BackTest{BackTestLite: runner.lite}
	if err := b.resolveLoopErrorContext(ctx, nil); err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("canceled replay treated as complete: %v", err)
	}
	primary := errs.NewMsg(core.ErrRunTime, "original source failure")
	if err := b.resolveLoopErrorContext(ctx, primary); err != primary {
		t.Fatal("caller cancellation hid the original error", err)
	}
}

func TestBackTestLocalEarlyStopKeepsLegacyCompletion(t *testing.T) {
	process := runtime.NewProcess()
	defer process.Close()
	runner := newR1BacktestRunner(t, process, t.Name(), 100)
	runner.runtime.Core.Stop()
	b := &BackTest{BackTestLite: runner.lite}
	if err := b.resolveLoopErrorContext(context.Background(), nil); err != nil {
		t.Fatal("local strategy/liquidation stop confused with caller cancellation", err)
	}
}

func TestBackTestCanceledContextFailsBeforeInitialization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (&BackTest{}).RunContext(ctx); err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("canceled run entered initialization: %v", err)
	}
}
