package biz

import (
	"math"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
)

// rita round3: delist exit semantics on a symbol whose data ended early. Price/PnL/isolation are asserted as
// correct; ExitAt and funding are asserted as the CURRENT (defective) behaviour, see issue 066.
func TestRitaDelistExitTimestampAndFunding(t *testing.T) {
	const day = int64(86400000)
	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.BackTestMode, state.EnvReal = true, true
	state.Market = banexg.MarketLinear
	cfg := config.NewSnapshotWithDirs(&config.Config{Accounts: map[string]*config.AccountConfig{"cleanup-test": {}},
		BTNetCost: 15, BTSlippageBps: 10, BTGapBars: 3, BTGapMinHours: 48, BTFundingApr: 10}, t.TempDir(), "")
	trader := newCompleteTraderForTest(t, RuntimeDeps{Core: state, Clock: btime.NewClockState(true, nil), Config: cfg, Symbols: orm.NewSymbolState(), Exchange: &cleanupTestExchange{}, DefaultAccount: "cleanup-test"})
	mgr := &LocalOrderMgr{OrderMgr: OrderMgr{Account: "cleanup-test", callBack: func(*ormo.InOutOrder, bool) {}}, zeroAmts: make(map[string]int)}
	mgr.bindRuntimeDeps(*trader.RuntimeDependencies())

	t0 := int64(1_600_000_000_000)
	deadLast := t0 + 30*day     // data of DEAD ends 30d after entry...
	end := deadLast + 3*365*day // ...backtest ends 3 years later (FTT-like)
	mgr.clock.SetTimeMS(deadLast)
	mgr.prices.SetBarPriceAt(deadLast, "DEAD/USDT:USDT", 2.0)
	mgr.clock.SetTimeMS(end)
	mgr.prices.SetBarPriceAt(end, "ALIVE/USDT:USDT", 5.0)
	dead, alive := cleanupPendingExit(1, "DEAD/USDT:USDT"), cleanupPendingExit(2, "ALIVE/USDT:USDT")
	for _, od := range []*ormo.InOutOrder{dead, alive} {
		od.Timeframe, od.ExitTag, od.Exit, od.EnterAt = "1d", "", nil, t0
		od.Enter.Average, od.Enter.Filled = 10, 1
	}
	openOds, lock := mgr.orderState().GetOpenODs(mgr.Account)
	lock.Lock()
	openOds[1], openOds[2] = dead, alive
	lock.Unlock()
	if err := mgr.exitDelisted(); err != nil {
		t.Fatal(err)
	}
	if dead.ExitTag != ExitTagDelist {
		t.Fatalf("dead tag %q", dead.ExitTag)
	}
	wantPx := 2.0 * (1 - 0.001)
	if math.Abs(dead.Exit.Average-wantPx) > 1e-9 {
		t.Fatalf("dead exit px %v want %v", dead.Exit.Average, wantPx)
	}
	if alive.ExitTag == ExitTagDelist || alive.Status == ormo.InOutStatusFullExit {
		t.Fatalf("alive must be untouched: %q %d", alive.ExitTag, alive.Status)
	}
	// 066: the exit is stamped at the symbol's last valid bar, so funding follows the real 30d hold
	if dead.ExitAt != deadLast || dead.Exit.UpdateAt != deadLast {
		t.Fatalf("ExitAt=%d UpdateAt=%d want last bar %d (backtest end %d)", dead.ExitAt, dead.Exit.UpdateAt, deadLast, end)
	}
	fundingTrue := 10.0 * 0.10 * float64(30*day) / float64(365*day) // 30d held
	fundingGot := dead.GetInfoFloat64("funding")
	if math.Abs(fundingGot-fundingTrue) > 1e-9 {
		t.Fatalf("funding %.6f want %.6f (30d hold)", fundingGot, fundingTrue)
	}
}

// 066: the in-run sweep closes the dead symbol when data has been missing long enough, at the last bar's time,
// long before the backtest ends.
func TestDelistSweepInRun(t *testing.T) {
	const day = int64(86400000)
	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.BackTestMode, state.EnvReal = true, true
	state.Market = banexg.MarketLinear
	cfg := config.NewSnapshotWithDirs(&config.Config{Accounts: map[string]*config.AccountConfig{"cleanup-test": {}},
		BTNetCost: 15, BTSlippageBps: 10, BTGapBars: 3, BTGapMinHours: 48, BTFundingApr: 10}, t.TempDir(), "")
	trader := newCompleteTraderForTest(t, RuntimeDeps{Core: state, Clock: btime.NewClockState(true, nil), Config: cfg, Symbols: orm.NewSymbolState(), Exchange: &cleanupTestExchange{}, DefaultAccount: "cleanup-test"})
	mgr := &LocalOrderMgr{OrderMgr: OrderMgr{Account: "cleanup-test", callBack: func(*ormo.InOutOrder, bool) {}}, zeroAmts: make(map[string]int)}
	mgr.bindRuntimeDeps(*trader.RuntimeDependencies())
	t0 := int64(1_600_000_000_000)
	deadLast := t0 + 30*day
	mgr.clock.SetTimeMS(deadLast + 10*day)
	mgr.prices.SetBarPriceAt(deadLast, "DEAD/USDT:USDT", 2.0)
	dead := cleanupPendingExit(1, "DEAD/USDT:USDT")
	dead.Timeframe, dead.ExitTag, dead.Exit, dead.EnterAt = "1d", "", nil, t0
	dead.Enter.Average, dead.Enter.Filled = 10, 1
	openOds, lock := mgr.orderState().GetOpenODs(mgr.Account)
	lock.Lock()
	openOds[1] = dead
	lock.Unlock()
	if err := mgr.sweepDelisted(deadLast + 1*day); err != nil { // 1d of silence: not a delisting yet
		t.Fatal(err)
	}
	if dead.ExitTag != "" {
		t.Fatalf("closed too early: %q", dead.ExitTag)
	}
	mgr.lastDelistSweepMS = 0
	if err := mgr.sweepDelisted(deadLast + 10*day); err != nil {
		t.Fatal(err)
	}
	if dead.ExitTag != ExitTagDelist || dead.ExitAt != deadLast {
		t.Fatalf("tag %q ExitAt %d want %d", dead.ExitTag, dead.ExitAt, deadLast)
	}
}
