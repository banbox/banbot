package biz

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
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
	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.BackTestMode, state.EnvReal = backtest, envReal
	state.Market = banexg.MarketLinear
	cfg := config.NewSnapshotWithDirs(&config.Config{Accounts: map[string]*config.AccountConfig{"cleanup-test": {}}, BTNetCost: 15}, t.TempDir(), "")
	trader := newCompleteTraderForTest(t, RuntimeDeps{Core: state, Clock: btime.NewClockState(backtest, nil), Config: cfg, Symbols: orm.NewSymbolState(), Exchange: &cleanupTestExchange{}, DefaultAccount: "cleanup-test"})
	mgr := &LocalOrderMgr{OrderMgr: OrderMgr{Account: "cleanup-test", callBack: func(*ormo.InOutOrder, bool) {}}, zeroAmts: make(map[string]int)}
	mgr.bindRuntimeDeps(*trader.RuntimeDependencies())
	return mgr
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
	mgr.clock.SetTimeMS(1_700_000_000_000)
	mgr.prices.SetBarPriceAt(mgr.clock.TimeMS(), symbol, lastPrice)
	mgr.clock.SetTimeMS(mgr.clock.TimeMS() + com.Day10MSecs + 1)
	od := cleanupPendingExit(1, symbol)

	affected, err := mgr.fillPendingOrders([]*ormo.InOutOrder{od}, nil)
	if err != nil {
		t.Fatalf("fill cleanup order: %v", err)
	}
	if affected != 1 || od.Status != ormo.InOutStatusFullExit || od.Exit.Average != lastPrice {
		t.Fatalf("stale historical price not used: affected=%d status=%d price=%v", affected, od.Status, od.Exit.Average)
	}
}

func TestBacktestExitAndFillUsesLastHistoricalPrice(t *testing.T) {
	mgr := setupLocalCleanupTest(t, true, true)
	const symbol = "HIFI-ROTATION/USDT:USDT"
	const lastPrice = 0.42
	mgr.clock.SetTimeMS(1_700_000_000_000)
	mgr.prices.SetBarPriceAt(mgr.clock.TimeMS(), symbol, lastPrice)
	mgr.clock.SetTimeMS(mgr.clock.TimeMS() + com.Day10MSecs + 1)
	od := cleanupPendingExit(1, symbol)
	od.ExitTag = ""
	od.Exit = nil

	if err := mgr.ExitAndFill([]*ormo.InOutOrder{od}, &strat.ExitReq{Tag: core.ExitTagPairDel}); err != nil {
		t.Fatalf("fill rotation order: %v", err)
	}
	if od.Status != ormo.InOutStatusFullExit || od.Exit.Average != lastPrice {
		t.Fatalf("stale historical price not used: status=%d price=%v", od.Status, od.Exit.Average)
	}
}

func TestBacktestBaselineCleanupUsesExplicitCutoff(t *testing.T) {
	mgr := setupLocalCleanupTest(t, true, false)
	const cutoff = int64(1_700_000_000_000)
	const symbol = "BASELINE-CLEANUP/USDT:USDT"
	err := mgr.symbols.SetExSymbols([]*orm.ExSymbol{{
		ID: 1, Exchange: "binance", Market: banexg.MarketLinear, Symbol: symbol,
	}})
	if err != nil {
		t.Fatalf("install test symbol: %v", err)
	}
	mgr.clock.SetTimeMS(cutoff + com.Day10MSecs)
	mgr.prices.SetBarPriceAt(mgr.clock.TimeMS(), symbol, 12)
	od := cleanupPendingExit(1, symbol)
	od.Sid = 1
	od.ExitTag = ""
	od.Exit = nil
	orders := mgr.orderState()
	orders.SetTask(mgr.Account, &ormo.BotTask{ID: 1})
	od.TaskID = 1
	od.BindState(orders)
	if err := od.Save(); err != nil {
		t.Fatalf("save baseline order: %v", err)
	}
	if err := mgr.cleanUpAt(cutoff); err != nil {
		t.Fatalf("close baseline orders: %v", err)
	}
	if od.Status != ormo.InOutStatusFullExit || od.ExitTag != core.ExitTagBotStop ||
		od.ExitAt != cutoff+15_000 {
		t.Fatalf("baseline cleanup order=%+v, want bot_stop at %d", od, cutoff+15_000)
	}
	if mgr.clock.TimeMS() != cutoff+com.Day10MSecs {
		t.Fatalf("cleanup changed simulated clock: %d", mgr.clock.TimeMS())
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
	mgr.clock.SetTimeMS(1_700_000_000_000)
	mgr.prices.SetBarPriceAt(mgr.clock.TimeMS(), symbol, 12)
	mgr.clock.SetTimeMS(mgr.clock.TimeMS() + com.Day10MSecs + 1)
	mgr.runtimeCore.BackTestMode = false
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
	openOds, lock := mgr.orderState().GetOpenODs(mgr.Account)
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
	wallets := mgr.walletsForOrder()
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

func TestRuntimeCleanupFiltersOnlyOwnedHistory(t *testing.T) {
	manager := setupLocalCleanupTest(t, true, false)
	first, second := ormo.NewOrderState(), ormo.NewOrderState()
	unfilled := cleanupPendingExit(1001, "USDT")
	unfilled.Enter.Filled = 0
	filled := cleanupPendingExit(1002, "USDT")
	first.AddHistoricalOrder(unfilled)
	first.AddHistoricalOrder(filled)
	secondUnfilled := cleanupPendingExit(1003, "USDT")
	secondUnfilled.Enter.Filled = 0
	second.AddHistoricalOrder(secondUnfilled)
	manager.bindRuntimeDeps(RuntimeDeps{
		Core:           &core.State{BackTestMode: true},
		Config:         config.NewSnapshot(&config.Config{}),
		Orders:         first,
		Trading:        NewTradingState(),
		DefaultAccount: manager.Account,
	})
	if err := manager.CleanUp(); err != nil {
		t.Fatal(err)
	}
	if history := first.HistoricalOrders(); len(history) != 1 || history[0] != filled {
		t.Fatalf("unexpected cleaned history: %v", history)
	}
	if history := second.HistoricalOrders(); len(history) != 1 || history[0] != secondUnfilled {
		t.Fatal("cleanup changed sibling runtime history")
	}

	if !first.AddHistoricalOrder(unfilled) {
		t.Fatal("removed history ID remains reserved")
	}
}
