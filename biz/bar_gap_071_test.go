package biz

import (
	"math"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
)

func setup071(t *testing.T, slipBps, penaltyBps float64) *LocalOrderMgr {
	t.Helper()
	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.BackTestMode, state.EnvReal = true, true
	state.Market = banexg.MarketLinear
	cfg := config.NewSnapshotWithDirs(&config.Config{Accounts: map[string]*config.AccountConfig{"cleanup-test": {}},
		BTNetCost: 15, BTSlippageBps: slipBps, BTGapBars: 3, BTGapMinHours: 48, BTDelistPenaltyBps: penaltyBps}, t.TempDir(), "")
	trader := newCompleteTraderForTest(t, RuntimeDeps{Core: state, Clock: btime.NewClockState(true, nil), Config: cfg, Symbols: orm.NewSymbolState(), Exchange: &cleanupTestExchange{}, DefaultAccount: "cleanup-test"})
	mgr := &LocalOrderMgr{OrderMgr: OrderMgr{Account: "cleanup-test", callBack: func(*ormo.InOutOrder, bool) {}}, zeroAmts: make(map[string]int)}
	mgr.bindRuntimeDeps(*trader.RuntimeDependencies())
	return mgr
}

func open071(mgr *LocalOrderMgr, id int64, short bool, t0 int64) *ormo.InOutOrder {
	od := cleanupPendingExit(id, "DEAD/USDT:USDT")
	od.Timeframe, od.ExitTag, od.Exit, od.EnterAt, od.Short = "1h", "", nil, t0, short
	od.Enter.Average, od.Enter.Filled = 2.0, 1
	openOds, lock := mgr.orderState().GetOpenODs(mgr.Account)
	lock.Lock()
	openOds[id] = od
	lock.Unlock()
	return od
}

// 071: data_gap exit = worse of (last close, first post-gap open) for the position, booked at
// min(last + confirmation delay, recovery time); both directions, both orderings of the two prices.
func TestDataGapExitUsesWorsePriceAndConfirmTime(t *testing.T) {
	const hour = int64(3600000)
	t0 := int64(1_600_000_000_000)
	last := t0 + 10*hour
	for _, tc := range []struct {
		name     string
		short    bool
		nextOpen float64
		want     float64
	}{
		{"long gap down: next open lower is worse", false, 1.5, 1.5},
		{"long gap up: last close is worse", false, 3.0, 2.0},
		{"short gap up: next open higher is worse", true, 3.0, 3.0},
		{"short gap down: last close is worse", true, 1.5, 2.0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := setup071(t, 0, 100)
			recover := last + 100*hour // hole of 100h
			mgr.clock.SetTimeMS(recover)
			mgr.prices.SetBarPriceAt(last, "DEAD/USDT:USDT", 2.0)
			od := open071(mgr, 1, tc.short, t0)
			req := &strat.ExitReq{Tag: ExitTagDataGap, Dirt: core.OdDirtBoth, Force: true, GapNextOpen: tc.nextOpen, GapRecoverMS: recover}
			if err := mgr.ExitAndFill([]*ormo.InOutOrder{od}, req); err != nil {
				t.Fatal(err)
			}
			if math.Abs(od.Exit.Average-tc.want) > 1e-9 {
				t.Fatalf("exit price %v want %v", od.Exit.Average, tc.want)
			}
			if want := last + 48*hour + 1; od.ExitAt != want { // confirmation (48h) is earlier than recovery (100h)
				t.Fatalf("ExitAt %d want %d (last+confirm)", od.ExitAt, want)
			}
			if od.ExitTag != ExitTagDataGap {
				t.Fatalf("tag %q", od.ExitTag)
			}
		})
	}
	t.Run("recovery before confirmation delay uses the recovery time", func(t *testing.T) {
		mgr := setup071(t, 0, 100)
		// 1h tf: threshold is >48h; a hole booked via the guard could never be shorter, but the rule is min(.,.)
		recover := last + 30*hour
		mgr.clock.SetTimeMS(recover)
		mgr.prices.SetBarPriceAt(last, "DEAD/USDT:USDT", 2.0)
		od := open071(mgr, 1, false, t0)
		req := &strat.ExitReq{Tag: ExitTagDataGap, Dirt: core.OdDirtBoth, Force: true, GapNextOpen: 1.8, GapRecoverMS: recover}
		if err := mgr.ExitAndFill([]*ormo.InOutOrder{od}, req); err != nil {
			t.Fatal(err)
		}
		if od.ExitAt != recover {
			t.Fatalf("ExitAt %d want recovery %d", od.ExitAt, recover)
		}
	})
}

// 071: delist exit = last close moved against the position by bt_delist_penalty_bps (plus slippage), at the last bar.
func TestDelistExitPenalty(t *testing.T) {
	const day = int64(86400000)
	t0 := int64(1_600_000_000_000)
	last := t0 + 5*day
	for _, short := range []bool{false, true} {
		mgr := setup071(t, 10, 100)
		mgr.clock.SetTimeMS(last + 30*day)
		mgr.prices.SetBarPriceAt(last, "DEAD/USDT:USDT", 2.0)
		od := open071(mgr, 1, short, t0)
		if err := mgr.ExitAndFill([]*ormo.InOutOrder{od}, &strat.ExitReq{Tag: ExitTagDelist, Dirt: core.OdDirtBoth, Force: true}); err != nil {
			t.Fatal(err)
		}
		want := 2.0 * (1 - 0.01) * (1 - 0.001)
		if short {
			want = 2.0 * (1 + 0.01) * (1 + 0.001)
		}
		if math.Abs(od.Exit.Average-want) > 1e-9 {
			t.Fatalf("short=%v exit %v want %v", short, od.Exit.Average, want)
		}
		if od.ExitAt != last {
			t.Fatalf("ExitAt %d want last bar %d", od.ExitAt, last)
		}
	}
}
