package biz

import (
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
)

func TestLegacyWalletPriceUsesLastHistoricalBar(t *testing.T) {
	originalCompat := config.Data.BTLegacyWallet
	originalBacktest := core.BackTestMode
	originalTime := btime.CurTimeMS
	t.Cleanup(func() {
		config.Data.BTLegacyWallet = originalCompat
		core.BackTestMode = originalBacktest
		btime.CurTimeMS = originalTime
	})

	core.BackTestMode = true
	btime.CurTimeMS = 1_000_000
	symbol := "LEGACY-WALLET-PRICE/USDT:USDT"
	com.SetBarPrice(symbol, 123.45)
	btime.CurTimeMS += com.PriceExpireMS + 1
	if got := com.GetPriceSafe(symbol, ""); got != -1 {
		t.Fatalf("current stale price = %v, want -1", got)
	}
	config.Data.BTLegacyWallet = true
	if got := walletMarkPrice(symbol); got != 123.45 {
		t.Fatalf("legacy stale price = %v, want 123.45", got)
	}
}

func TestLegacyWalletOrdersAreStableWithoutMutatingInput(t *testing.T) {
	originalCompat := config.Data.BTLegacyWallet
	originalBacktest := core.BackTestMode
	t.Cleanup(func() {
		config.Data.BTLegacyWallet = originalCompat
		core.BackTestMode = originalBacktest
	})

	orders := []*ormo.InOutOrder{
		{IOrder: &ormo.IOrder{ID: 3}},
		{IOrder: &ormo.IOrder{ID: 1}},
		{IOrder: &ormo.IOrder{ID: 2}},
	}
	core.BackTestMode = true
	config.Data.BTLegacyWallet = true
	got := legacyWalletOrderView(orders)
	if got[0].ID != 1 || got[1].ID != 2 || got[2].ID != 3 {
		t.Fatalf("legacy wallet order = [%d %d %d], want [1 2 3]", got[0].ID, got[1].ID, got[2].ID)
	}
	if orders[0].ID != 3 || orders[1].ID != 1 || orders[2].ID != 2 {
		t.Fatalf("input order mutated: [%d %d %d]", orders[0].ID, orders[1].ID, orders[2].ID)
	}
}
