package opt

import (
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
)

func TestRuntimeOrderMatchStateCopiesCompletedLegacyRegistration(t *testing.T) {
	old := core.OrderMatchTfs
	t.Cleanup(func() { core.OrderMatchTfs = old })
	core.OrderMatchTfs = map[string]bool{"1m": true, "5m": false}
	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)

	syncRuntimeOrderMatchState(state)
	core.OrderMatchTfs["1m"] = false
	core.OrderMatchTfs["15m"] = true

	state.LockOdMatch.RLock()
	defer state.LockOdMatch.RUnlock()
	if !state.OrderMatchTfs["1m"] || state.OrderMatchTfs["5m"] || state.OrderMatchTfs["15m"] {
		t.Fatalf("runtime order-match snapshot = %#v", state.OrderMatchTfs)
	}
}

func TestBackTestRuntimeClockSynchronizesLegacyProviderClock(t *testing.T) {
	oldTime := btime.CurTimeMS
	t.Cleanup(func() { btime.CurTimeMS = oldTime })

	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	state.SetRunMode(core.RunModeBackTest)
	clock := btime.NewClockState(true, nil)
	WithLegacySession(func(session LegacySession) struct{} {
		lite := NewBackTestLiteWithRuntimeDeps(session, biz.RuntimeDeps{Core: state, Clock: clock}, nil, true, nil, nil, nil)

		lite.SetTimeMS(1_700_000_000_000)
		if got := lite.TimeMS(); got != 1_700_000_000_000 {
			t.Fatalf("typed backtest clock = %d, want %d", got, int64(1_700_000_000_000))
		}
		if btime.CurTimeMS != 1_700_000_000_000 {
			t.Fatalf("legacy provider clock = %d, want %d", btime.CurTimeMS, int64(1_700_000_000_000))
		}
		return struct{}{}
	})
}
