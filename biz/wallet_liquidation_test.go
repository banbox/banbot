package biz

import (
	"math"
	"testing"

	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
)

func TestLiquidationWalletResetPolicy(t *testing.T) {
	tests := []struct {
		name         string
		backtest     bool
		chargeOnBomb bool
		wantReset    bool
	}{
		{name: "ordinary backtest defers settlement", backtest: true, wantReset: false},
		{name: "recharging backtest preserves reset", backtest: true, chargeOnBomb: true, wantReset: true},
		{name: "non-backtest preserves reset", backtest: false, wantReset: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldBacktest, oldChargeOnBomb := core.BackTestMode, config.ChargeOnBomb
			t.Cleanup(func() {
				core.BackTestMode, config.ChargeOnBomb = oldBacktest, oldChargeOnBomb
			})
			core.BackTestMode, config.ChargeOnBomb = tc.backtest, tc.chargeOnBomb

			wallet := &ItemWallet{
				Coin: "USDT", Available: 1,
				Pendings: map[string]float64{"pending": 1},
				Frozens:  map[string]float64{"open": 98},
			}
			wallets := &BanWallets{Items: map[string]*ItemWallet{"USDT": wallet}}
			orders := []*ormo.InOutOrder{{IOrder: &ormo.IOrder{ID: 1, Profit: -100}}}

			err := wallets.UpdateOds(orders, "USDT")
			if err == nil || err.Code != core.ErrLiquidation {
				t.Fatalf("UpdateOds error = %v, want liquidation", err)
			}
			if tc.wantReset {
				if wallet.Total(false) != 0 || len(wallet.Pendings) != 0 || len(wallet.Frozens) != 0 {
					t.Fatalf("wallet was not reset: total=%v pending=%v frozen=%v",
						wallet.Total(false), wallet.Pendings, wallet.Frozens)
				}
				return
			}
			if wallet.Total(false) != 100 || wallet.Pendings["pending"] != 1 || wallet.Frozens["open"] != 98 {
				t.Fatalf("wallet settlement state lost: total=%v pending=%v frozen=%v",
					wallet.Total(false), wallet.Pendings, wallet.Frozens)
			}
		})
	}
}

func TestBacktestLiquidationCleanupSettlesContractOnce(t *testing.T) {
	mgr := setupLocalCleanupTest(t, true, false)

	const (
		symbol  = "LIQUIDATIONCLEANUP/USDT:USDT"
		initial = 100.0
	)
	err := mgr.symbols.SetExSymbols([]*orm.ExSymbol{{
		ID: 1, Exchange: "binance", Market: banexg.MarketLinear, Symbol: symbol,
	}})
	if err != nil {
		t.Fatalf("install test symbol: %v", err)
	}

	mgr.clock.SetTimeMS(1_700_000_000_000)
	mgr.prices.SetBarPriceAt(mgr.clock.TimeMS(), symbol, 0.5)
	od := &ormo.InOutOrder{
		IOrder: &ormo.IOrder{
			ID: 1, Sid: 1, Symbol: symbol, Status: ormo.InOutStatusFullEnter,
			Timeframe: "1m", Strategy: "liquidation-test", Leverage: 1,
			EnterAt: mgr.clock.TimeMS() - 60_000, Profit: -99.5,
		},
		Enter: &ormo.ExOrder{
			Enter: true, OrderType: banexg.OdTypeMarket, Side: banexg.OdSideBuy,
			Price: 100, Average: 100, Amount: 1, Filled: 1, Status: ormo.OdStatusClosed,
		},
	}
	wallets := mgr.walletsForOrder()
	wallets.Items["USDT"] = &ItemWallet{
		Coin: "USDT", Pendings: map[string]float64{}, Frozens: map[string]float64{od.Key(): initial},
	}
	orders := mgr.orderState()
	orders.SetTask(mgr.Account, &ormo.BotTask{ID: 1})
	od.TaskID = 1
	od.BindState(orders)
	if err := od.Save(); err != nil {
		t.Fatalf("save open liquidated order: %v", err)
	}

	liquidationErr := wallets.UpdateOds([]*ormo.InOutOrder{od}, "USDT")
	if liquidationErr == nil || liquidationErr.Code != core.ErrLiquidation {
		t.Fatalf("UpdateOds error = %v, want liquidation", liquidationErr)
	}
	if err := mgr.CleanUp(); err != nil {
		t.Fatalf("cleanup liquidated order: %v", err)
	}
	wantBalance := initial + od.Profit
	if got := wallets.Items["USDT"].Available; math.Abs(got-wantBalance) > 1e-9 {
		t.Fatalf("final balance = %.12f, want initial + profit = %.12f", got, wantBalance)
	}
}

// EquityWithOrders reads the open orders, not the wallet's cached UnrealizedPOL: after same-bar exits the cache still
// holds the unrealized loss of orders whose realized loss is already in the balance (issue 162).
func TestEquityWithOrdersIgnoresStaleUnrealizedCache(t *testing.T) {
	setContractMarketForTest(t, true)
	wallet := &ItemWallet{
		Coin: "USDT", Available: 1698.6,
		Pendings: map[string]float64{}, Frozens: map[string]float64{},
		UnrealizedPOL: -2022.68, // stale: the orders it described are closed
	}
	wallets := &BanWallets{Items: map[string]*ItemWallet{"USDT": wallet}}
	if got := wallets.TotalLegal(nil, true); got > 0 {
		t.Fatalf("fixture: cached equity should be negative, got %v", got)
	}
	if got := wallets.EquityWithOrders(nil); math.Abs(got-1698.6) > 1e-9 {
		t.Fatalf("equity without open orders = %v, want the balance 1698.6", got)
	}
	open := &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 2, Profit: -200, Status: ormo.InOutStatusFullEnter}}
	closed := &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 1, Profit: -999, Status: ormo.InOutStatusFullExit}}
	wallet.Frozens["open"] = 100
	if got := wallets.EquityWithOrders([]*ormo.InOutOrder{open, closed, nil}); math.Abs(got-(1698.6+100-200)) > 1e-9 {
		t.Fatalf("equity with one open order = %v", got)
	}
	// realized losses beyond the balance: equity <= 0 although UpdateOds never saw an unrealized loss
	wallet.Available, wallet.Frozens["open"] = -17.2, 0
	if got := wallets.EquityWithOrders(nil); got > 0 {
		t.Fatalf("depleted account equity = %v", got)
	}
}

func setContractMarketForTest(t *testing.T, contract bool) {
	t.Helper()
	old := core.IsContract
	t.Cleanup(func() { core.IsContract = old })
	core.IsContract = contract
}

// Spot: the base coin held in the wallet is already valued at the market price by TotalLegal, so the open order's
// profit must not be added again. A full-size long that loses half of its value is a -50% account, not a bankruptcy.
func TestEquityWithOrdersSpotDoesNotDoubleCountHolding(t *testing.T) {
	setContractMarketForTest(t, false)
	oldBT := core.BackTestMode
	t.Cleanup(func() { core.BackTestMode = oldBT })
	core.BackTestMode = true
	com.SetBarPrice("SPOTEQ/USDT", 50) // bought 2 SPOTEQ at 100 with the whole 200, the price fell to 50
	wallets := &BanWallets{Items: map[string]*ItemWallet{
		"USDT":   {Coin: "USDT", Pendings: map[string]float64{}, Frozens: map[string]float64{}},
		"SPOTEQ": {Coin: "SPOTEQ", Available: 2, Pendings: map[string]float64{}, Frozens: map[string]float64{}},
	}}
	open := &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 1, Profit: -100, Status: ormo.InOutStatusFullEnter}}
	got := wallets.EquityWithOrders([]*ormo.InOutOrder{open})
	if math.Abs(got-100) > 1e-9 {
		t.Fatalf("spot equity = %v, want 100 (2 coins at 50, order profit already inside)", got)
	}
}

// Contract: only margin is frozen, the loss lives in the order profit; losing the whole margin (and more) is
// equity <= 0.
func TestEquityWithOrdersContractLossToZeroIsDepleted(t *testing.T) {
	setContractMarketForTest(t, true)
	wallet := &ItemWallet{Coin: "USDT", Available: 0, Pendings: map[string]float64{}, Frozens: map[string]float64{"open": 200}}
	wallets := &BanWallets{Items: map[string]*ItemWallet{"USDT": wallet}}
	open := &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 1, Profit: -50, Status: ormo.InOutStatusFullEnter}}
	if got := wallets.EquityWithOrders([]*ormo.InOutOrder{open}); math.Abs(got-150) > 1e-9 {
		t.Fatalf("contract equity = %v, want 150", got)
	}
	open.Profit = -200
	if got := wallets.EquityWithOrders([]*ormo.InOutOrder{open}); got > 0 {
		t.Fatalf("contract equity after losing the whole margin = %v, want <= 0", got)
	}
}
