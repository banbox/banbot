package biz

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type cleanupTestExchange struct {
	banexg.BanExchange
}

func (*cleanupTestExchange) CalculateFee(string, string, string, float64, float64, bool, map[string]interface{}) (*banexg.Fee, *errs.Error) {
	return &banexg.Fee{}, nil
}

func setupLocalCleanupTest(t *testing.T, backtest, envReal bool) *LocalOrderMgr {
	t.Helper()
	originalBiz := BackupVars()
	originalExchange := exg.Default
	originalBacktest := core.BackTestMode
	originalLive := core.LiveMode
	originalEnvReal := core.EnvReal
	originalTime := btime.CurTimeMS
	t.Cleanup(func() {
		RestoreVars(originalBiz)
		exg.Default = originalExchange
		core.BackTestMode = originalBacktest
		core.LiveMode = originalLive
		core.EnvReal = originalEnvReal
		btime.CurTimeMS = originalTime
	})

	ResetVars()
	exg.Default = &cleanupTestExchange{}
	core.BackTestMode = backtest
	core.LiveMode = false
	core.EnvReal = envReal
	return &LocalOrderMgr{
		OrderMgr: OrderMgr{
			Account:  "cleanup-test",
			callBack: func(*ormo.InOutOrder, bool) {},
		},
		zeroAmts: make(map[string]int),
	}
}

func cleanupPendingExit(id int64, symbol string) *ormo.InOutOrder {
	return &ormo.InOutOrder{
		IOrder: &ormo.IOrder{
			ID:        id,
			Symbol:    symbol,
			Status:    ormo.InOutStatusFullEnter,
			Timeframe: "1m",
			Strategy:  "cleanup-test",
			InitPrice: 10,
			EnterAt:   1,
			ExitTag:   core.ExitTagBotStop,
		},
		Enter: &ormo.ExOrder{
			Enter:     true,
			OrderType: banexg.OdTypeMarket,
			Side:      banexg.OdSideBuy,
			Price:     10,
			Average:   10,
			Amount:    1,
			Filled:    1,
			Status:    ormo.OdStatusClosed,
		},
		Exit: &ormo.ExOrder{
			OrderType: banexg.OdTypeMarket,
			Side:      banexg.OdSideSell,
			CreateAt:  1_700_000_000_000,
			Amount:    1,
			Status:    ormo.OdStatusInit,
		},
	}
}

func TestBacktestCleanupUsesLastHistoricalPrice(t *testing.T) {
	mgr := setupLocalCleanupTest(t, true, true)
	const symbol = "HIFI-CLEANUP/USDT:USDT"
	const lastPrice = 0.42
	btime.CurTimeMS = 1_700_000_000_000
	com.SetBarPrice(symbol, lastPrice)
	btime.CurTimeMS += com.Day10MSecs + 1
	od := cleanupPendingExit(1, symbol)

	affected, err := mgr.fillPendingOrders([]*ormo.InOutOrder{od}, nil)
	if err != nil {
		t.Fatalf("fill cleanup order: %v", err)
	}
	if affected != 1 || od.Status != ormo.InOutStatusFullExit || od.Exit.Average != lastPrice {
		t.Fatalf("stale historical price not used: affected=%d status=%d price=%v", affected, od.Status, od.Exit.Average)
	}
}

func TestCleanupMissingPriceDoesNotSkipFollowingOrder(t *testing.T) {
	mgr := setupLocalCleanupTest(t, true, true)
	missing := cleanupPendingExit(1, "NO-CLEANUP-PRICE/USDT:USDT")
	following := cleanupPendingExit(2, "USDT")

	affected, err := mgr.fillPendingOrders([]*ormo.InOutOrder{missing, following}, nil)
	if err != nil {
		t.Fatalf("fill cleanup batch: %v", err)
	}
	if affected != 1 || following.Status != ormo.InOutStatusFullExit {
		t.Fatalf("following order skipped: affected=%d status=%d", affected, following.Status)
	}
	if missing.Status == ormo.InOutStatusFullExit {
		t.Fatal("order without a cached price was unexpectedly filled")
	}
}

func TestNonBacktestCleanupStillRejectsStalePrice(t *testing.T) {
	mgr := setupLocalCleanupTest(t, true, true)
	const symbol = "LIVE-CLEANUP/USDT:USDT"
	btime.CurTimeMS = 1_700_000_000_000
	com.SetBarPrice(symbol, 12)
	btime.CurTimeMS += com.Day10MSecs + 1
	core.BackTestMode = false
	od := cleanupPendingExit(1, symbol)

	affected, err := mgr.fillPendingOrders([]*ormo.InOutOrder{od}, nil)
	if err != nil {
		t.Fatalf("fill non-backtest cleanup order: %v", err)
	}
	if affected != 0 || od.Status == ormo.InOutStatusFullExit {
		t.Fatalf("non-backtest accepted stale price: affected=%d status=%d", affected, od.Status)
	}
}

func TestCleanupFailsWithOpenOrders(t *testing.T) {
	mgr := setupLocalCleanupTest(t, true, false)
	openOds, lock := ormo.GetOpenODs(config.DefAcc)
	lock.Lock()
	openOds[1] = cleanupPendingExit(1, "NO-CLEANUP-PRICE-LEFT/USDT:USDT")
	lock.Unlock()

	err := mgr.CleanUp()
	if err == nil || !strings.Contains(err.Error(), "open orders") {
		t.Fatalf("cleanup error = %v, want open-order residue", err)
	}
}

func TestCleanupFailsWithFrozenWalletFunds(t *testing.T) {
	mgr := setupLocalCleanupTest(t, true, false)
	wallets := GetWallets(config.DefAcc)
	wallets.Items["USDT"] = &ItemWallet{
		Coin:     "USDT",
		Pendings: make(map[string]float64),
		Frozens:  map[string]float64{"stale-order": 10},
	}

	err := mgr.CleanUp()
	if err == nil || !strings.Contains(err.Error(), "frozen wallet") {
		t.Fatalf("cleanup error = %v, want frozen-wallet residue", err)
	}
}
