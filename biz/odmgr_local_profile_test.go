package biz

import (
	"math"
	"testing"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
)

// These constants come from the existing TS profile, rather than another
// implementation of its algorithm. Keep the two historical paths distinct.
func TestOHLCProfileFixedNumericalBehavior(t *testing.T) {
	for _, test := range []struct {
		name                      string
		legacy                    bool
		close, price, triggerRate float64
	}{
		{"legacy rising", true, 105, 96.5, 5.0 / 35},
		{"current rising", false, 105, 101.75, 11.0 / 42.5},
		{"legacy falling", true, 95, 103.5, 25.0 / 35},
		{"current falling", false, 95, 98.25, 31.0 / 42.5},
	} {
		t.Run(test.name, func(t *testing.T) {
			bar := &orm.SeriesOHLCV{Time: 1000, Open: 100, High: 110, Low: 90, Close: test.close, Volume: 10, Quote: 1000, TradeNum: 7}
			if got := simMarketPriceWithLegacy(bar, .1, test.legacy); math.Abs(got-test.price) > 1e-12 {
				t.Fatalf("market price %.17g, want %.17g", got, test.price)
			}
			if got := simMarketRateWithLegacy(bar, 95, true, true, 0, test.legacy); math.Abs(got-test.triggerRate) > 1e-12 {
				t.Fatalf("trigger rate %.17g, want %.17g", got, test.triggerRate)
			}
			cut := cutSeriesFromRateWithLegacy(bar, 60_000, .1, test.legacy)
			if cut.Time != 7000 || cut.Volume != 9 || cut.Quote != 1000 || cut.TradeNum != 7 || cut.Close != test.close || math.Abs(cut.Open-test.price) > 1e-12 {
				t.Fatalf("cut metadata or price changed: %+v", cut)
			}
		})
	}
	flat := &orm.SeriesOHLCV{Open: 100, High: 100, Low: 100, Close: 100}
	for _, legacy := range []bool{true, false} {
		if simMarketPriceWithLegacy(flat, .5, legacy) != 100 || simMarketRateWithLegacy(flat, 100, true, true, 0, legacy) != .5 {
			t.Fatal("flat-bar historical behavior changed")
		}
		if simMarketRateWithLegacy(nil, 100, true, true, .7, legacy) != .7 || simMarketRateWithLegacy(flat, 110, true, true, .7, legacy) != .7 {
			t.Fatal("missing-bar/gap trigger behavior changed")
		}
	}
}

func TestOHLCProfilePendingExitFixedReplay(t *testing.T) {
	const startMS = int64(1_700_000_040_000)
	for _, test := range []struct {
		name, orderType, side string
		legacy                bool
		limit, wantPrice      float64
		wantOffset            int64
		wantCount             int
	}{
		{"legacy market", banexg.OdTypeMarket, banexg.OdSideSell, true, 0, 91.25, 15_000, 1},
		{"current market", banexg.OdTypeMarket, banexg.OdSideSell, false, 0, 95.375, 15_000, 1},
		{"legacy buy limit", banexg.OdTypeLimit, banexg.OdSideBuy, true, 95, 95, 8_000, 1},
		{"current buy limit", banexg.OdTypeLimit, banexg.OdSideBuy, false, 95, 95, 15_000, 1},
		{"legacy sell limit", banexg.OdTypeLimit, banexg.OdSideSell, true, 105, 105, 42_000, 1},
		{"current sell limit", banexg.OdTypeLimit, banexg.OdSideSell, false, 105, 105, 43_000, 1},
		{"buy price improvement", banexg.OdTypeLimit, banexg.OdSideBuy, true, 105, 100, 0, 1},
		{"sell price improvement", banexg.OdTypeLimit, banexg.OdSideSell, false, 95, 100, 0, 1},
		{"unfilled buy", banexg.OdTypeLimit, banexg.OdSideBuy, true, 85, 0, 0, 0},
		{"unfilled sell", banexg.OdTypeLimit, banexg.OdSideSell, false, 115, 0, 0, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			mgr := setupLocalCleanupTest(t, true, true)
			mgr.runtimeCfg.legacyIntrabar = test.legacy
			mgr.clock.SetTimeMS(startMS + 60_000)
			od := cleanupPendingExit(1, "OHLC/USDT:USDT")
			od.Exit.CreateAt, od.Exit.OrderType, od.Exit.Side, od.Exit.Price = startMS, test.orderType, test.side, test.limit
			callbacks := 0
			mgr.callBack = func(*ormo.InOutOrder, bool) {
				callbacks++
				mgr.runtimeCore.AddSimOrder()
			}
			event := orm.NewDataSeriesFromKline(&orm.ExSymbol{ID: 1, Symbol: od.Symbol}, "1m", &banexg.Kline{Time: startMS, Open: 100, High: 110, Low: 90, Close: 105}, nil, true, true)
			event.Values["custom"] = int64(17)
			count, newCount, err := mgr.fillPendingOrdersPass([]*ormo.InOutOrder{od}, event)
			if err != nil {
				t.Fatal(err)
			}
			if count != test.wantCount || callbacks != test.wantCount || newCount != test.wantCount {
				t.Fatalf("count/callback/new = %d/%d/%d", count, callbacks, newCount)
			}
			mgr.runtimeCore.AddSimOrder()
			if mgr.runtimeCore.NewSimOrderCount() != newCount {
				t.Fatal("matching pass remained active after callback completion")
			}
			if count != 0 && (od.Exit.Average != test.wantPrice || od.Exit.UpdateAt != startMS+test.wantOffset || od.Exit.Filled != 1 || od.Status != ormo.InOutStatusFullExit) {
				t.Fatalf("fill = %+v, expected price=%g at offset=%d", od.Exit, test.wantPrice, test.wantOffset)
			}
			if event.Values["custom"] != int64(17) {
				t.Fatal("custom series value changed")
			}
		})
	}
}

func TestOHLCProfileProtectiveExitPreservesTSCallbacks(t *testing.T) {
	const startMS = int64(1_700_000_040_000)
	for _, test := range []struct {
		name             string
		slLimit, tpLimit float64
		wantPrice        float64
		wantOffset       int64
		wantTag          string
		callbacks        int
	}{
		{"SL wins both hits", 0, 0, 95, 8_572, "fixed_sl", 1},
		{"SL limit retains maker", 100, 0, 100, 34_286, "fixed_sl", 1},
		{"blocked SL suppresses TP", 115, 0, 0, 0, "", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			mgr := setupLocalCleanupTest(t, true, true)
			mgr.runtimeCfg.legacyIntrabar = true
			mgr.clock.SetTimeMS(startMS + 60_000)
			od := cleanupPendingExit(1, "OHLC/USDT:USDT")
			od.Exit, od.ExitTag, od.Leverage = nil, "", 1
			od.Enter.Symbol = od.Symbol
			mgr.orderState().SetTask(mgr.Account, &ormo.BotTask{ID: 1})
			od.TaskID = 1
			od.BindState(mgr.orderState())
			od.SetInfo(ormo.OdInfoStopLoss, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: 95, Limit: test.slLimit, Tag: "fixed_sl"}})
			od.SetInfo(ormo.OdInfoTakeProfit, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: 105, Limit: test.tpLimit, Tag: "fixed_tp"}})
			callbacks := 0
			mgr.callBack = func(callbackOrder *ormo.InOutOrder, isEnter bool) {
				callbacks++
				if callbackOrder != od || isEnter || callbackOrder.Status != ormo.InOutStatusFullExit {
					t.Fatal("callback saw wrong order/phase")
				}
			}
			bar := &orm.SeriesOHLCV{Time: startMS, Open: 100, High: 110, Low: 90, Close: 105}
			if err := mgr.tryFillTriggers(od, bar, "1m", 0); err != nil {
				t.Fatal(err)
			}
			if callbacks != test.callbacks || !od.GetStopLoss().Hit || !od.GetTakeProfit().Hit || od.ExitTag != test.wantTag {
				t.Fatalf("callbacks=%d tag=%s", callbacks, od.ExitTag)
			}
			if callbacks > 0 {
				if math.Abs(od.Exit.Average-test.wantPrice) > 1e-12 || od.ExitAt != startMS+test.wantOffset || od.GetInfoString(ormo.OdInfoSLTP) != "yes" {
					t.Fatalf("protection exit=%+v at=%d", od.Exit, od.ExitAt)
				}
				wantType := banexg.OdTypeMarket
				if test.slLimit > 0 {
					wantType = banexg.OdTypeLimitMaker
				}
				if od.Exit.OrderType != wantType {
					t.Fatalf("order type=%s, want %s", od.Exit.OrderType, wantType)
				}
			}
		})
	}
}

func BenchmarkTSOHLCProfile(b *testing.B) {
	bar := &orm.SeriesOHLCV{Open: 100, High: 110, Low: 90, Close: 105}
	var result float64
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rate := simMarketRateWithLegacy(bar, 95, true, true, .1, i&1 == 0)
		result += simMarketPriceWithLegacy(bar, rate, i&1 == 0)
	}
	if result == 0 {
		b.Fatal("profile not evaluated")
	}
}
