package runtime

import (
	"context"
	"reflect"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/strat"
)

func TestSharedLiveRejectsStrategyLeverageBeforeAllocation(t *testing.T) {
	var capture *sharedCaptureAdapter
	f := newSharedTriggerFixtureWithAdapter(t, func(paper *runner.PaperAdapter) execution.ExecutionAdapter {
		capture = &sharedCaptureAdapter{ExecutionAdapter: paper}
		return capture
	})
	f.rt.Core.LiveMode = true
	f.rt.Core.SetRunEnv(core.RunEnvProd)
	before, err := f.rt.SharedExecution().Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, leverage := range []float64{1, 5} {
		_, err := f.manager.EnterOrder(f.job.Symbol, f.job.TimeFrame, &strat.EnterReq{StratName: "legacy", Tag: "leverage", Amount: 1, Leverage: leverage})
		if err == nil {
			t.Fatalf("strategy leverage %v bypassed unsupported live leverage policy", leverage)
		}
	}
	after, err := f.rt.SharedExecution().Snapshot(context.Background())
	if err != nil || !reflect.DeepEqual(before, after) || len(capture.orders) != 0 {
		t.Fatal("rejected leverage changed ledger or submitted a venue order", after, capture.orders, err)
	}
}
