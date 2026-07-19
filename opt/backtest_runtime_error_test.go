package opt

import (
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

func TestBackTestRuntimeErrorIsReturnedAfterLoop(t *testing.T) {
	runErr := errs.NewMsg(core.ErrRunTime, "series too many")
	bt := &BackTest{BackTestLite: &BackTestLite{runErr: runErr}}
	if got := bt.resolveLoopError(nil); got != runErr {
		t.Fatalf("resolveLoopError = %v, want runtime error", got)
	}
}

func TestBackTestLiteRuntimeErrorIsReturnedAfterProviderLoop(t *testing.T) {
	runErr := errs.NewMsg(core.ErrRunTime, "series too many")
	bt := &BackTestLite{runErr: runErr}
	if got := bt.resolveLoopError(nil); got != runErr {
		t.Fatalf("resolveLoopError = %v, want runtime error", got)
	}
}

func TestBackTestPreservesFirstRuntimeError(t *testing.T) {
	first := errs.NewMsg(core.ErrRunTime, "first")
	second := errs.NewMsg(core.ErrRunTime, "second")
	bt := &BackTestLite{}
	bt.setRunError(first)
	bt.setRunError(second)
	if bt.runErr != first {
		t.Fatalf("runErr = %v, want first error", bt.runErr)
	}
}

func TestBackTestLiteOptimizationLiquidationDoesNotStopLiveRuntime(t *testing.T) {
	originalChargeOnBomb := config.ChargeOnBomb
	originalStopAll := core.StopAll
	t.Cleanup(func() {
		config.ChargeOnBomb = originalChargeOnBomb
		core.StopAll = originalStopAll
	})
	config.ChargeOnBomb = false
	liveStopCalls := 0
	core.StopAll = func() { liveStopCalls++ }
	bt := &BackTestLite{
		isOpt: true,
		dp:    data.NewHistProvider(nil, nil, nil, false, nil),
	}

	bt.onLiquidation("BTC/USDT")

	if liveStopCalls != 0 {
		t.Fatalf("live StopAll calls = %d, want 0", liveStopCalls)
	}
}

func TestWithRelayBacktestStateRestoresLiveStateAfterError(t *testing.T) {
	originalBiz := biz.BackupVars()
	originalRange := config.TimeRange
	originalPolicies := config.RunPolicy
	originalAccounts := config.Accounts
	originalTime := btime.CurTimeMS
	originalMode := core.RunMode
	originalEnv := core.RunEnv
	originalStopAll := core.StopAll
	originalPairHooks := strat.SnapshotPairUpdateHooks()
	t.Cleanup(func() {
		biz.RestoreVars(originalBiz)
		config.TimeRange = originalRange
		config.RunPolicy = originalPolicies
		config.Accounts = originalAccounts
		btime.CurTimeMS = originalTime
		core.SetRunMode(originalMode)
		core.SetRunEnv(originalEnv)
		core.StopAll = originalStopAll
		strat.SetPairUpdateHooks(originalPairHooks)
		config.ClearRefineMap()
	})

	liveRange := &config.TimeTuple{StartMS: 100, EndMS: 200}
	livePolicy := &config.RunPolicyConfig{Name: "live-policy", RefineTF: "15m"}
	defaultAccount := &config.AccountConfig{}
	secondaryAccount := &config.AccountConfig{}
	liveForbidJobs := map[string]map[string]bool{"BTC/USDT_1h": {"live": true}}
	config.TimeRange = liveRange
	config.RunPolicy = []*config.RunPolicyConfig{livePolicy}
	config.Accounts = map[string]*config.AccountConfig{
		config.DefAcc: defaultAccount,
		"secondary":   secondaryAccount,
	}
	btime.CurTimeMS = 150
	core.Pairs = []string{"BTC/USDT"}
	core.TFSecs = map[string]int{"1h": 3600}
	core.StgPairTfs = map[string]map[string]string{"live-policy": {"BTC/USDT": "1h"}}
	core.OrderMatchTfs = map[string]bool{"15m": true}
	core.BotRunning = true
	core.CheckWallets = false
	strat.ForbidJobs = liveForbidJobs
	strat.Versions = map[string]int{"live-policy": 7}
	liveHookCalls := 0
	simHookCalls := 0
	strat.SetPairUpdateHooks(strat.PairUpdateHooks{
		SubWarmPairs: func(map[string]map[string]int, bool) *errs.Error {
			liveHookCalls++
			return nil
		},
	})
	liveStopCalls := 0
	core.StopAll = func() { liveStopCalls++ }
	core.SetRunMode(core.RunModeLive)
	core.SetRunEnv(core.RunEnvProd)
	config.ClearRefineMap()
	if tf := config.EnsureStratRefineTF(livePolicy.ID(), "1h"); tf != "15m" {
		t.Fatalf("live refine timeframe = %s, want 15m", tf)
	}

	runErr := errs.NewMsg(core.ErrRunTime, "relay backtest failed")
	got := withRelayBacktestState(func() *errs.Error {
		if core.RunMode != core.RunModeBackTest || core.EnvReal {
			t.Fatalf("temporary state mode=%s envReal=%v, want backtest", core.RunMode, core.EnvReal)
		}
		if len(config.Accounts) != 1 || config.Accounts[config.DefAcc] == defaultAccount {
			t.Fatalf("temporary accounts were not merged: %#v", config.Accounts)
		}
		core.StopAll()
		if liveStopCalls != 1 {
			t.Fatal("relay state wrapper hid the live StopAll callback")
		}
		strat.SetPairUpdateHooks(strat.PairUpdateHooks{
			SubWarmPairs: func(map[string]map[string]int, bool) *errs.Error {
				simHookCalls++
				return nil
			},
		})
		if err := strat.SnapshotPairUpdateHooks().SubWarmPairs(nil, true); err != nil {
			t.Fatal(err)
		}
		biz.ResetVars()
		config.TimeRange = &config.TimeTuple{StartMS: 1, EndMS: 2}
		config.RunPolicy = []*config.RunPolicyConfig{{Name: "live-policy", RefineTF: "5m"}}
		config.ClearRefineMap()
		if tf := config.EnsureStratRefineTF(livePolicy.ID(), "1h"); tf != "5m" {
			t.Fatalf("simulation refine timeframe = %s, want 5m", tf)
		}
		btime.CurTimeMS = 2
		core.Pairs = []string{"SIM/USDT"}
		core.TFSecs = map[string]int{"5m": 300}
		core.StgPairTfs = map[string]map[string]string{"simulation-policy": {"SIM/USDT": "5m"}}
		core.OrderMatchTfs = map[string]bool{"1m": true}
		core.BotRunning = false
		core.CheckWallets = true
		strat.ForbidJobs = map[string]map[string]bool{"SIM/USDT_1h": {"sim": true}}
		strat.Versions = map[string]int{"simulation-policy": 1}
		return runErr
	})

	if got != runErr {
		t.Fatalf("withRelayBacktestState error = %v, want original runtime error", got)
	}
	if config.TimeRange.StartMS != liveRange.StartMS || config.TimeRange.EndMS != liveRange.EndMS {
		t.Fatalf("time range = %#v, want %#v", config.TimeRange, liveRange)
	}
	if len(config.RunPolicy) != 1 || config.RunPolicy[0] != livePolicy {
		t.Fatalf("run policies were not restored: %#v", config.RunPolicy)
	}
	if len(config.Accounts) != 2 || config.Accounts[config.DefAcc] != defaultAccount || config.Accounts["secondary"] != secondaryAccount {
		t.Fatalf("accounts were not restored: %#v", config.Accounts)
	}
	if btime.CurTimeMS != 150 {
		t.Fatalf("current time = %d, want 150", btime.CurTimeMS)
	}
	if core.RunMode != core.RunModeLive || core.RunEnv != core.RunEnvProd || !core.LiveMode || !core.EnvReal {
		t.Fatalf("runtime state mode=%s env=%s live=%v real=%v", core.RunMode, core.RunEnv, core.LiveMode, core.EnvReal)
	}
	if len(core.Pairs) != 1 || core.Pairs[0] != "BTC/USDT" {
		t.Fatalf("pairs were not restored: %v", core.Pairs)
	}
	if core.TFSecs["1h"] != 3600 || core.StgPairTfs["live-policy"]["BTC/USDT"] != "1h" || !core.OrderMatchTfs["15m"] {
		t.Fatalf("strategy timeframe state was not restored: tfs=%v pairTfs=%v matchTfs=%v", core.TFSecs, core.StgPairTfs, core.OrderMatchTfs)
	}
	if !core.BotRunning || core.CheckWallets {
		t.Fatalf("runtime flags running=%v checkWallets=%v, want true/false", core.BotRunning, core.CheckWallets)
	}
	if strat.ForbidJobs["BTC/USDT_1h"]["live"] != true {
		t.Fatalf("forbidden jobs were not restored: %#v", strat.ForbidJobs)
	}
	if len(strat.Versions) != 1 || strat.Versions["live-policy"] != 7 {
		t.Fatalf("strategy versions were not restored: %#v", strat.Versions)
	}
	if err := strat.SnapshotPairUpdateHooks().SubWarmPairs(nil, true); err != nil {
		t.Fatal(err)
	}
	if liveHookCalls != 1 || simHookCalls != 1 {
		t.Fatalf("pair update hooks live=%d sim=%d, want 1/1", liveHookCalls, simHookCalls)
	}
	core.StopAll()
	if liveStopCalls != 2 {
		t.Fatalf("live StopAll calls = %d, want 2 after restore", liveStopCalls)
	}
	if tf := config.EnsureStratRefineTF(livePolicy.ID(), "1h"); tf != "15m" {
		t.Fatalf("restored refine timeframe = %s, want 15m", tf)
	}
}
