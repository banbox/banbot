package runtime

import (
	"testing"

	"github.com/banbox/banbot/strat"
)

func TestSharedManualExitReplacesPartialProtectionTag(t *testing.T) {
	f := newSharedTriggerFixture(t)
	order := f.entry(t, &strat.EnterReq{Tag: "partial-protection", Amount: 1, StopLoss: 95, StopLossRate: .5})
	f.observe(t, 94, 2)
	if f.steps(t) != 5 {
		t.Fatal("partial stop did not reduce half")
	}
	if _, err := f.manager.ExitOrder(order, &strat.ExitReq{Tag: "manual-after-stop", Force: true}); err != nil {
		t.Fatal(err)
	}
	if order.ExitTag != "manual-after-stop" || f.steps(t) != 0 {
		t.Fatal("old protection tag replaced manual exit", order.ExitTag, f.steps(t))
	}
}
