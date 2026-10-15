package biz

import (
	"math"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
)

func TestContractPartialExitPreservesWalletProfitIdentity(t *testing.T) {
	const (
		initial  = 1000.0
		margin   = 100.0
		enterFee = 2.0
		symbol   = "PARTIALFEE/USDT:USDT"
	)

	originalEnvReal := core.EnvReal
	core.EnvReal = false
	t.Cleanup(func() { core.EnvReal = originalEnvReal })
	restoreSymbols, err := orm.InstallFrozenExSymbols([]*orm.ExSymbol{{
		ID: 1, Exchange: "binance", Market: banexg.MarketLinear, Symbol: symbol,
	}})
	if err != nil {
		t.Fatalf("install test symbol: %v", err)
	}
	t.Cleanup(restoreSymbols)

	od := &ormo.InOutOrder{
		IOrder: &ormo.IOrder{
			ID: 1, Sid: 1, Symbol: symbol, Strategy: "partial-fee", Timeframe: "1m",
			Status: ormo.InOutStatusFullEnter, EnterAt: 1, Leverage: 10,
		},
		Enter: &ormo.ExOrder{
			Enter: true, Average: 100, Amount: 10, Filled: 10,
			Fee: enterFee, FeeQuote: enterFee, FeeType: "USDT", Status: ormo.OdStatusClosed,
		},
	}
	wallet := &ItemWallet{
		Coin: "USDT", Available: initial - margin,
		Pendings: map[string]float64{}, Frozens: map[string]float64{od.Key(): margin - enterFee},
		UnrealizedPOL: 200,
	}
	wallets := &BanWallets{Items: map[string]*ItemWallet{"USDT": wallet}}
	if err := wallet.SetMargin(od.Key(), margin); err != nil {
		t.Fatalf("back margin with unrealized profit: %v", err)
	}

	part := od.CutPart(od.Enter.Amount/2, 0)
	wallets.CutPart(part.Key(), od.Key(), "USDT", 0.5)
	closeContractPart(wallets, part, 110, 0.5)
	closeContractPart(wallets, od, 120, 1)

	totalProfit := part.Profit + od.Profit
	wantBalance := initial + totalProfit
	if diff := wallet.Available - wantBalance; math.Abs(diff) > 1e-9 {
		t.Fatalf("final balance identity differs by %.12f: final=%.12f initial=%.12f profit=%.12f entry_fees=%.12f",
			diff, wallet.Available, initial, totalProfit, part.Enter.FeeQuote+od.Enter.FeeQuote)
	}
}

func closeContractPart(wallets *BanWallets, od *ormo.InOutOrder, price, exitFee float64) {
	od.Exit = &ormo.ExOrder{Average: price, Amount: od.Enter.Filled, Filled: od.Enter.Filled, FeeQuote: exitFee}
	od.Status = ormo.InOutStatusFullExit
	od.UpdateProfits(price)
	wallets.ConfirmOdExit(od, price)
}
