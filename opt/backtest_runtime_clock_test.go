package opt

import (
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
)

func TestRuntimeOrderMatchStateCopiesRegistration(t *testing.T) {
	registered := map[string]bool{"1m": true, "5m": false}
	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)

	state.ReplaceOrderMatchTfs(registered)
	registered["1m"] = false
	registered["15m"] = true

	if !state.OrderMatchTfsSnapshot()["1m"] || state.OrderMatchTfsSnapshot()["5m"] || state.OrderMatchTfsSnapshot()["15m"] {
		t.Fatalf("runtime order-match snapshot = %#v", state.OrderMatchTfsSnapshot())
	}
}

func TestBackTestRuntimeClockDoesNotChangeProcessClock(t *testing.T) {
	oldTime := btime.CurTimeMS
	t.Cleanup(func() { btime.CurTimeMS = oldTime })

	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	state.SetRunMode(core.RunModeBackTest)
	clock := btime.NewClockState(true, nil)
	lite := newBackTestLiteForTest(t, biz.RuntimeDeps{Core: state, Clock: clock}, true, nil, nil, nil)

	lite.SetTimeMS(1_700_000_000_000)
	if got := lite.TimeMS(); got != 1_700_000_000_000 {
		t.Fatalf("typed backtest clock = %d, want %d", got, int64(1_700_000_000_000))
	}
	if btime.CurTimeMS != oldTime {
		t.Fatalf("process clock = %d, want %d", btime.CurTimeMS, oldTime)
	}
}
