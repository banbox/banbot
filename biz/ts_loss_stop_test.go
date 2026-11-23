package biz

import (
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/shopspring/decimal"
)

func lossStopDeps(t *testing.T, backtest bool, cfg *config.Config) RuntimeDeps {
	t.Helper()
	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.BackTestMode = backtest
	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(1_700_000_000_000)
	return RuntimeDeps{Core: state, Clock: clock, Config: config.NewSnapshotWithDirs(cfg, t.TempDir(), ""), DefaultAccount: "default"}
}

func TestTimeSeriesLossStopFromFatalStopConfig(t *testing.T) {
	deps := lossStopDeps(t, false, &config.Config{FatalStop: map[string]float64{"180": 0.2, " 30 ": 0.3}})
	policy, active, err := timeSeriesLossStop(deps)
	if err != nil || !active {
		t.Fatalf("active=%t err=%v", active, err)
	}
	if len(policy.Windows) != 2 || policy.Windows[0].WindowMS != 30*60_000 || policy.Windows[1].WindowMS != 180*60_000 ||
		!policy.Windows[0].MaxLossRate.Equal(decimal.RequireFromString("0.3")) || policy.PauseMS != 8*3600_000 {
		t.Fatalf("unexpected policy %+v", policy)
	}
	deps = lossStopDeps(t, false, &config.Config{FatalStop: map[string]float64{"60": 0.1}, FatalStopHours: 4})
	if policy, _, _ = timeSeriesLossStop(deps); policy.PauseMS != 4*3600_000 {
		t.Fatalf("fatal_stop_hours ignored: %+v", policy)
	}
	// No fatal_stop in a live runtime still reports active with an empty policy,
	// so a previously installed breaker is disabled.
	if policy, active, err = timeSeriesLossStop(lossStopDeps(t, false, &config.Config{})); err != nil || !active || len(policy.Windows) != 0 {
		t.Fatalf("empty config: %+v active=%t err=%v", policy, active, err)
	}
	if _, active, _ = timeSeriesLossStop(lossStopDeps(t, true, &config.Config{FatalStop: map[string]float64{"60": 0.1}})); active {
		t.Fatal("backtests never apply fatal_stop")
	}
	for _, bad := range []map[string]float64{{"abc": 0.1}, {"0": 0.1}, {"60": 0}, {"60": 1.5}} {
		if _, _, err = timeSeriesLossStop(lossStopDeps(t, false, &config.Config{FatalStop: bad})); err == nil {
			t.Fatalf("invalid fatal_stop %v accepted", bad)
		}
	}
}

func TestApplyLossStopPauseSetsEntrySwitchWithoutShortening(t *testing.T) {
	deps := lossStopDeps(t, false, &config.Config{})
	now := deps.Clock.TimeMS()
	if err := applyLossStopPause(deps, execution.LossStopStatus{PausedUntilMS: now - 1}); err != nil {
		t.Fatal(err)
	}
	if until, _ := deps.Core.NoEnterUntilFor("default"); until != 0 {
		t.Fatalf("an expired pause must not set the switch: %d", until)
	}
	if err := applyLossStopPause(deps, execution.LossStopStatus{PausedUntilMS: now + 1000}); err != nil {
		t.Fatal(err)
	}
	if until, _ := deps.Core.NoEnterUntilFor("default"); until != now+1000 {
		t.Fatalf("pause not mirrored: %d", until)
	}
	deps.Core.SetNoEnterUntil("default", now+5000)
	if err := applyLossStopPause(deps, execution.LossStopStatus{PausedUntilMS: now + 2000}); err != nil {
		t.Fatal(err)
	}
	if until, _ := deps.Core.NoEnterUntilFor("default"); until != now+5000 {
		t.Fatalf("a longer manual switch was shortened: %d", until)
	}
}
