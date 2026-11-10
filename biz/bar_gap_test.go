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

func TestBarGapExceeded(t *testing.T) {
	const day = int64(86400000)
	cases := []struct {
		name          string
		prevStop, cur int64
		tf            int64
		bars          int
		minHours      float64
		wantHit       bool
	}{
		{"off", 10 * day, 40 * day, day, 0, 0, false},
		{"adjacent", 10 * day, 10 * day, day, 3, 0, false},
		{"1d small gap 2d", 10 * day, 12 * day, day, 3, 0, false},
		{"1d 20d gap (LUNA)", 10 * day, 30 * day, day, 3, 0, true},
		{"15m 3h gap below 48h", 10 * day, 10*day + 3*3600000, 900000, 3, 0, false},
		{"15m 3d gap", 10 * day, 13 * day, 900000, 3, 0, true},
		{"1d 3d gap, min 100h", 10 * day, 13 * day, day, 2, 100, false},
		{"no prev bar", 0, 30 * day, day, 3, 0, false},
	}
	for _, c := range cases {
		if _, hit := barGapExceeded(c.prevStop, c.cur, c.tf, c.bars, c.minHours); hit != c.wantHit {
			t.Errorf("%s: hit=%v want %v", c.name, hit, c.wantHit)
		}
	}
}

func setupDelistTest(t *testing.T, gapBars int, bps float64) *LocalOrderMgr {
	t.Helper()
	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.BackTestMode, state.EnvReal = true, true
	state.Market = banexg.MarketLinear
	cfg := config.NewSnapshotWithDirs(&config.Config{Accounts: map[string]*config.AccountConfig{"cleanup-test": {}},
		BTNetCost: 15, BTSlippageBps: bps, BTGapBars: gapBars, BTGapMinHours: 48}, t.TempDir(), "")
	trader := newCompleteTraderForTest(t, RuntimeDeps{Core: state, Clock: btime.NewClockState(true, nil), Config: cfg, Symbols: orm.NewSymbolState(), Exchange: &cleanupTestExchange{}, DefaultAccount: "cleanup-test"})
	mgr := &LocalOrderMgr{OrderMgr: OrderMgr{Account: "cleanup-test", callBack: func(*ormo.InOutOrder, bool) {}}, zeroAmts: make(map[string]int)}
	mgr.bindRuntimeDeps(*trader.RuntimeDependencies())
	return mgr
}

// the data of DEAD ended 30 days before the backtest end, ALIVE's ends at the end: only DEAD's position is a delist exit,
// closed at its last close less slippage (long).
func TestDelistExitAtLastBar(t *testing.T) {
	const day = int64(86400000)
	end := 1_700_000_000_000 + 40*day
	for _, tc := range []struct {
		name    string
		gapBars int
		want    string
	}{{"armed", 3, ExitTagDelist}, {"gap guard off", 0, ""}} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := setupDelistTest(t, tc.gapBars, 10)
			deadLast, aliveLast := 1_700_000_000_000, end
			mgr.clock.SetTimeMS(int64(deadLast))
			mgr.prices.SetBarPriceAt(mgr.clock.TimeMS(), "DEAD/USDT:USDT", 2.0)
			mgr.clock.SetTimeMS(int64(aliveLast))
			mgr.prices.SetBarPriceAt(mgr.clock.TimeMS(), "ALIVE/USDT:USDT", 5.0)
			dead, alive := cleanupPendingExit(1, "DEAD/USDT:USDT"), cleanupPendingExit(2, "ALIVE/USDT:USDT")
			for _, od := range []*ormo.InOutOrder{dead, alive} {
				od.Timeframe, od.ExitTag, od.Exit = "1d", "", nil
			}
			openOds, lock := mgr.orderState().GetOpenODs(mgr.Account)
			lock.Lock()
			openOds[1], openOds[2] = dead, alive
			lock.Unlock()
			if err := mgr.exitDelisted(); err != nil {
				t.Fatal(err)
			}
			if dead.ExitTag != tc.want {
				t.Fatalf("DEAD exit tag %q want %q", dead.ExitTag, tc.want)
			}
			if tc.want != "" {
				if w := 2.0 * (1 - 0.001); math.Abs(dead.Exit.Average-w) > 1e-9 || dead.Status != ormo.InOutStatusFullExit {
					t.Fatalf("DEAD exit avg=%v want %v status=%d", dead.Exit.Average, w, dead.Status)
				}
			}
			if alive.ExitTag == ExitTagDelist || alive.Status == ormo.InOutStatusFullExit {
				t.Fatalf("ALIVE must stay open for the normal cleanup: tag=%q status=%d", alive.ExitTag, alive.Status)
			}
		})
	}
}
