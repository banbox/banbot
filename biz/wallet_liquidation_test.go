package biz

import (
	"math"
	"testing"

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
