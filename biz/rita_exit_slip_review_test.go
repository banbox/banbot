package biz

import (
	"math"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
)

// rita round2: ExitAndFill (data-gap / bot_stop / force exits) must pay the configured slippage, adverse to the position.
func setupSlipExitTest(t *testing.T, bps float64) *LocalOrderMgr {
	t.Helper()
	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.BackTestMode, state.EnvReal = true, true
	state.Market = banexg.MarketLinear
	cfg := config.NewSnapshotWithDirs(&config.Config{Accounts: map[string]*config.AccountConfig{"cleanup-test": {}}, BTNetCost: 15, BTSlippageBps: bps}, t.TempDir(), "")
	trader := newCompleteTraderForTest(t, RuntimeDeps{Core: state, Clock: btime.NewClockState(true, nil), Config: cfg, Symbols: orm.NewSymbolState(), Exchange: &cleanupTestExchange{}, DefaultAccount: "cleanup-test"})
	mgr := &LocalOrderMgr{OrderMgr: OrderMgr{Account: "cleanup-test", callBack: func(*ormo.InOutOrder, bool) {}}, zeroAmts: make(map[string]int)}
	mgr.bindRuntimeDeps(*trader.RuntimeDependencies())
	return mgr
}

func TestRitaExitAndFillPaysSlippage(t *testing.T) {
	const last = 100.0
	for _, tc := range []struct {
		name  string
		bps   float64
		short bool
		want  float64
	}{
		{"long 10bps sells lower", 10, false, last * (1 - 0.001)},
		{"short 10bps buys higher", 10, true, last * (1 + 0.001)},
		{"long 0bps untouched", 0, false, last},
		{"short 0bps untouched", 0, true, last},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := setupSlipExitTest(t, tc.bps)
			sym := "RITA-SLIP/USDT:USDT"
			mgr.clock.SetTimeMS(1_700_000_000_000)
			mgr.prices.SetBarPriceAt(mgr.clock.TimeMS(), sym, last)
			mgr.clock.SetTimeMS(mgr.clock.TimeMS() + com.Day10MSecs + 1)
			od := cleanupPendingExit(1, sym)
			od.ExitTag, od.Exit, od.Short = "", nil, tc.short
			if err := mgr.ExitAndFill([]*ormo.InOutOrder{od}, &strat.ExitReq{Tag: core.ExitTagBotStop, Force: true}); err != nil {
				t.Fatal(err)
			}
			if od.Status != ormo.InOutStatusFullExit || math.Abs(od.Exit.Average-tc.want) > 1e-9 || math.Abs(od.Exit.Price-tc.want) > 1e-9 {
				t.Fatalf("exit avg=%v price=%v want=%v status=%d", od.Exit.Average, od.Exit.Price, tc.want, od.Status)
			}
		})
	}
}
