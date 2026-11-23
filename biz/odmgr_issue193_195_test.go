package biz

import (
	"testing"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
)

// A pending order created at a bar's close must not match that same bar
// (e.g. the refine bar ending at the decision bar's close); it fills from the next bar.
func TestPendingOrderSkipsBarEndingAtCreateAt(t *testing.T) {
	const startMS = int64(1_700_000_040_000)
	for _, test := range []struct {
		name      string
		createAt  int64
		wantCount int
	}{
		{"created at bar close skips bar", startMS + 60_000, 0},
		{"created before bar end fills", startMS + 30_000, 1},
		{"created at bar start fills", startMS, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			mgr := setupLocalCleanupTest(t, true, true)
			mgr.clock.SetTimeMS(startMS + 60_000)
			od := cleanupPendingExit(1, "OHLC/USDT:USDT")
			od.Exit.CreateAt, od.Exit.OrderType, od.Exit.Side = test.createAt, banexg.OdTypeMarket, banexg.OdSideSell
			mgr.callBack = func(*ormo.InOutOrder, bool) { mgr.runtimeCore.AddSimOrder() }
			event := orm.NewDataSeriesFromKline(&orm.ExSymbol{ID: 1, Symbol: od.Symbol}, "1m",
				&banexg.Kline{Time: startMS, Open: 100, High: 110, Low: 90, Close: 105}, nil, true, true)
			count, _, err := mgr.fillPendingOrdersPass([]*ormo.InOutOrder{od}, event)
			if err != nil {
				t.Fatal(err)
			}
			if count != test.wantCount {
				t.Fatalf("filled %d, want %d", count, test.wantCount)
			}
			if test.wantCount == 0 && od.Status == ormo.InOutStatusFullExit {
				t.Fatal("order filled from the bar before its creation")
			}
		})
	}
}

// Trailing must not start before the entry fills.
func TestUpdateByDataSeriesSkipsTrailingBeforeEntryFill(t *testing.T) {
	const startMS = int64(1_700_000_040_000)
	for _, test := range []struct {
		name   string
		status int64
		want   bool
	}{
		{"entry pending", ormo.InOutStatusInit, false},
		{"entry filled", ormo.InOutStatusFullEnter, true},
		{"entry part filled", ormo.InOutStatusPartEnter, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			mgr := setupLocalCleanupTest(t, true, true)
			mgr.clock.SetTimeMS(startMS + 60_000)
			od := cleanupPendingExit(1, "OHLC/USDT:USDT")
			od.Status, od.Exit, od.ExitTag = test.status, nil, ""
			od.SetInfo(ormo.OdInfoCallbackPct, 1.0)
			event := orm.NewDataSeriesFromKline(&orm.ExSymbol{ID: 1, Symbol: od.Symbol}, "1m",
				&banexg.Kline{Time: startMS, Open: 100, High: 110, Low: 90, Close: 105}, nil, true, true)
			if err := mgr.OrderMgr.UpdateByDataSeries([]*ormo.InOutOrder{od}, event); err != nil {
				t.Fatal(err)
			}
			if got := od.GetInfoFloat64(ormo.OdInfoTrailingBest) > 0; got != test.want {
				t.Fatalf("trailing started = %v, want %v", got, test.want)
			}
		})
	}
}
