package runtime

import (
	"context"
	"fmt"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

func TestSharedAdmissionRejectsBeforeExecution(t *testing.T) {
	for _, reason := range []string{"banned", "paused", "other"} {
		t.Run(reason, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			var callbacks int
			manager, err := biz.NewSharedOrderMgr(f.rt.BizDeps(), f.rt.SharedExecution(), f.bridge, func(*ormo.InOutOrder, bool) { callbacks++ })
			if err != nil {
				t.Fatal(err)
			}
			switch reason {
			case "banned":
				f.rt.Core.SetPairBanUntil("BTC", 1000)
			case "paused":
				f.rt.Core.SetNoEnterUntil("default", 1000)
			case "other":
				f.rt.Core.RunMode = core.RunModeOther
			}
			before, err := f.rt.SharedExecution().Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			f.job.Entrys = []*strat.EnterReq{{Tag: "rejected", StratName: "legacy", Amount: 1}}
			entries, exits, e := manager.ProcessOrders(f.job)
			if e != nil {
				t.Fatal(e)
			}
			after, err := f.rt.SharedExecution().Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 || len(exits) != 0 || callbacks != 0 || after.Checkpoint != before.Checkpoint || f.adapter.Metrics().Fills != 0 {
				t.Fatalf("rejected %s produced effects: entries=%d callbacks=%d checkpoints=%d->%d", reason, len(entries), callbacks, before.Checkpoint, after.Checkpoint)
			}
		})
	}
}

func TestSharedAdmissionLimitsAndZeroDefaults(t *testing.T) {
	for _, limit := range []string{"account", "account-override", "simultaneous", "policy", "policy-simultaneous", "unlimited"} {
		t.Run(limit, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			cfg := &config.Config{}
			f.job.Strat.Policy = &config.RunPolicyConfig{Name: "legacy"}
			switch limit {
			case "account":
				cfg.MaxOpenOrders = 1
			case "account-override":
				cfg.MaxOpenOrders = 10
				cfg.Accounts = map[string]*config.AccountConfig{"default": {MaxOpenOrders: 1}}
			case "simultaneous":
				cfg.MaxSimulOpen = 1
			case "policy":
				f.job.Strat.Policy.MaxOpen = 1
			case "policy-simultaneous":
				f.job.Strat.Policy.MaxSimulOpen = 1
			}
			deps := f.rt.BizDeps()
			deps.Config = config.NewSnapshot(cfg)
			manager, err := biz.NewSharedOrderMgr(deps, f.rt.SharedExecution(), f.bridge, nil)
			if err != nil {
				t.Fatal(err)
			}
			f.job.Entrys = []*strat.EnterReq{{Tag: "first", Amount: 1}, {Tag: "second", Amount: 1}}
			entries, _, e := manager.ProcessOrders(f.job)
			if e != nil {
				t.Fatal(e)
			}
			want := 1
			if limit == "unlimited" {
				want = 2
			}
			if len(entries) != want || f.adapter.Metrics().Fills != want {
				t.Fatalf("%s accepted %d fills=%d, want %d", limit, len(entries), f.adapter.Metrics().Fills, want)
			}
			if limit != "unlimited" {
				od, e := manager.EnterOrder(f.job.Symbol, f.job.TimeFrame, &strat.EnterReq{StratName: "legacy", Tag: "direct", Amount: 1})
				if e != nil || od != nil {
					t.Fatalf("direct entry bypassed %s: order=%v err=%v", limit, od, e)
				}
			}
		})
	}
}

func TestSharedAdmissionCommitAndSharedBarCapacity(t *testing.T) {
	f := newSharedTriggerFixture(t)
	deps := f.rt.BizDeps()
	deps.Config = config.NewSnapshot(&config.Config{MaxSimulOpen: 1})
	build := func() *biz.SharedOrderMgr {
		manager, err := biz.NewSharedOrderMgr(deps, f.rt.SharedExecution(), f.bridge, nil)
		if err != nil {
			t.Fatal(err)
		}
		return manager
	}
	manager := build()
	f.rt.Clock.SetTimeMS(300000)
	// A rejected bridge command must roll back its staged admission counters.
	_, err := manager.EnterOrder(f.job.Symbol, "1m", &strat.EnterReq{StratName: "legacy", Tag: "invalid", Amount: 1, Limit: -1})
	if err == nil {
		t.Fatal("invalid command was accepted")
	}
	od, err := manager.EnterOrder(f.job.Symbol, "1m", &strat.EnterReq{StratName: "legacy", Tag: "first", Amount: 1})
	if err != nil || od == nil {
		t.Fatalf("failed request spent capacity: %v %v", od, err)
	}
	// A new facade and a different timeframe cannot acquire a second account
	// capacity slot at the same aligned boundary. Different request timeframes
	// need not share a boundary merely because they have the same wall-clock now.
	// Counters are in the committed checkpoint.
	od, err = build().EnterOrder(f.job.Symbol, "5m", &strat.EnterReq{StratName: "legacy", Tag: "other-timeframe", Amount: 1})
	if err != nil || od != nil {
		t.Fatalf("timeframe/facade bypassed capacity: %v %v", od, err)
	}
	f.rt.Clock.SetTimeMS(360000)
	od, err = build().EnterOrder(f.job.Symbol, "1m", &strat.EnterReq{StratName: "legacy", Tag: "next-bar", Amount: 1})
	if err != nil || od == nil {
		t.Fatalf("next bar did not replenish capacity: %v %v", od, err)
	}
}

func TestSharedAdmissionPreservesRequestTimeframeAlignment(t *testing.T) {
	// At now=360000, 5m aligns to 300000 and 1m to 360000. Existing TS
	// counters replenish only when the request's aligned boundary advances.
	// Equal wall-clock now does not make these requests share one boundary.
	for _, tc := range []struct {
		name       string
		timeframes []string
		accepted   []bool
	}{
		{name: "5m-before-1m", timeframes: []string{"5m", "1m", "5m"}, accepted: []bool{true, true, false}},
		{name: "1m-before-5m", timeframes: []string{"1m", "5m"}, accepted: []bool{true, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			deps := f.rt.BizDeps()
			deps.Config = config.NewSnapshot(&config.Config{MaxSimulOpen: 1})
			f.rt.Clock.SetTimeMS(360000)
			fills := 0
			for index, tf := range tc.timeframes {
				// Rebuild the facade before each request: admission counters must
				// survive in committed state rather than manager-local memory.
				manager, err := biz.NewSharedOrderMgr(deps, f.rt.SharedExecution(), f.bridge, nil)
				if err != nil {
					t.Fatal(err)
				}
				before, err := f.rt.SharedExecution().Snapshot(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				beforeFills := f.adapter.Metrics().Fills
				od, e := manager.EnterOrder(f.job.Symbol, tf, &strat.EnterReq{StratName: "legacy", Tag: fmt.Sprintf("%s-%d", tc.name, index), Amount: 1})
				if e != nil {
					t.Fatal(e)
				}
				after, err := f.rt.SharedExecution().Snapshot(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if tc.accepted[index] {
					fills++
					if od == nil || after.Checkpoint <= before.Checkpoint || f.adapter.Metrics().Fills != fills {
						t.Fatalf("request %d tf=%s was not committed: order=%v checkpoint=%d->%d fills=%d want=%d", index, tf, od, before.Checkpoint, after.Checkpoint, f.adapter.Metrics().Fills, fills)
					}
				} else if od != nil || after.Checkpoint != before.Checkpoint || f.adapter.Metrics().Fills != beforeFills {
					t.Fatalf("request %d tf=%s rejection changed state: order=%v checkpoint=%d->%d fills=%d->%d", index, tf, od, before.Checkpoint, after.Checkpoint, beforeFills, f.adapter.Metrics().Fills)
				}
			}
		})
	}
}

func TestSharedAdmissionCountsPendingAndReleasesCanceledSlot(t *testing.T) {
	f := newSharedTriggerFixture(t)
	deps := f.rt.BizDeps()
	deps.Config = config.NewSnapshot(&config.Config{MaxOpenOrders: 1})
	manager, err := biz.NewSharedOrderMgr(deps, f.rt.SharedExecution(), f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	pending, e := manager.EnterOrder(f.job.Symbol, "1m", &strat.EnterReq{StratName: "legacy", Tag: "limit", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
	if e != nil || pending == nil || pending.Enter.Filled != 0 {
		t.Fatalf("pending fixture failed: %v %v", pending, e)
	}
	od, e := manager.EnterOrder(f.job.Symbol, "1m", &strat.EnterReq{StratName: "legacy", Tag: "blocked", Amount: 1})
	if e != nil || od != nil {
		t.Fatal("pending entry did not consume account slot")
	}
	if _, e = manager.ExitOrder(pending, &strat.ExitReq{Tag: "cancel", UnFillOnly: true}); e != nil {
		t.Fatal(e)
	}
	od, e = manager.EnterOrder(f.job.Symbol, "1m", &strat.EnterReq{StratName: "legacy", Tag: "after-cancel", Amount: 1})
	if e != nil || od == nil {
		t.Fatalf("canceled pending entry retained account slot: %v %v", od, e)
	}
}

func TestSharedAdmissionRejectionStillAllowsExit(t *testing.T) {
	f := newSharedTriggerFixture(t)
	f.entry(t, &strat.EnterReq{Tag: "held", Amount: 1})
	f.rt.Core.SetPairBanUntil("BTC", 1000)
	f.job.Entrys = []*strat.EnterReq{{Tag: "blocked", Amount: 1}}
	f.job.Exits = []*strat.ExitReq{{Tag: "risk-reduction"}}
	entries, exits, err := f.manager.ProcessOrders(f.job)
	if err != nil || len(entries) != 0 || len(exits) != 1 || f.steps(t) != 0 {
		t.Fatalf("entry rejection blocked exit: entries=%d exits=%d error=%v", len(entries), len(exits), err)
	}
}

func TestSharedAdmissionRejectsConflictingBorrowerLimits(t *testing.T) {
	f := newSharedTriggerFixture(t)
	deps := f.rt.BizDeps()
	deps.Config = config.NewSnapshot(&config.Config{MaxOpenOrders: 1})
	manager, err := biz.NewSharedOrderMgr(deps, f.rt.SharedExecution(), f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	od, e := manager.EnterOrder(f.job.Symbol, "1m", &strat.EnterReq{StratName: "legacy", Tag: "first", Amount: 1})
	if e != nil || od == nil {
		t.Fatalf("first entry failed: %v %v", od, e)
	}
	before, err := f.rt.SharedExecution().Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	deps.Config = config.NewSnapshot(&config.Config{})
	other, err := biz.NewSharedOrderMgr(deps, f.rt.SharedExecution(), f.bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	od, e = other.EnterOrder(f.job.Symbol, "1m", &strat.EnterReq{StratName: "legacy", Tag: "relaxed", Amount: 1})
	if e == nil || od != nil {
		t.Fatal("conflicting borrower relaxed committed account limits")
	}
	after, err := f.rt.SharedExecution().Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after.Checkpoint != before.Checkpoint {
		t.Fatal("conflicting borrower committed a command")
	}
}

func TestSharedAdmissionRestoresStrategyLimitsWithoutMetadata(t *testing.T) {
	for _, kind := range []string{"open", "simultaneous"} {
		t.Run(kind, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			f.job.TimeFrame = "1m"
			f.job.Strat.Policy = &config.RunPolicyConfig{Name: "legacy"}
			if kind == "open" {
				f.job.Strat.Policy.MaxOpen = 1
			} else {
				f.job.Strat.Policy.MaxSimulOpen = 1
			}
			f.entry(t, &strat.EnterReq{Tag: "first", Amount: 1})
			before, err := f.rt.SharedExecution().Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			deps := f.rt.BizDeps()
			other, err := biz.NewSharedOrderMgr(deps, f.rt.SharedExecution(), f.bridge, nil)
			if err != nil {
				t.Fatal(err)
			}
			od, e := other.EnterOrder(f.job.Symbol, "1m", &strat.EnterReq{StratName: "legacy", Tag: "missing-metadata", Amount: 1})
			if e != nil || od != nil {
				t.Fatalf("missing metadata relaxed saved %s limit: %v %v", kind, od, e)
			}
			deps.Config = config.NewSnapshot(&config.Config{RunPolicy: []*config.RunPolicyConfig{{Name: "legacy"}}})
			other, err = biz.NewSharedOrderMgr(deps, f.rt.SharedExecution(), f.bridge, nil)
			if err != nil {
				t.Fatal(err)
			}
			od, e = other.EnterOrder(f.job.Symbol, "1m", &strat.EnterReq{StratName: "legacy", Tag: "explicit-zero", Amount: 1})
			if e == nil || od != nil {
				t.Fatalf("explicit zero relaxed saved %s limit", kind)
			}
			after, err := f.rt.SharedExecution().Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if before.Checkpoint != after.Checkpoint {
				t.Fatal("policy conflict changed committed state")
			}
		})
	}
}
