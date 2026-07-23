package biz

import (
	"math"
	"slices"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
)

func TestLegacyItemWalletUsedIsStableAcrossMapLayouts(t *testing.T) {
	originalCompat := config.Data.BTLegacyWallet
	originalBacktest := core.BackTestMode
	t.Cleanup(func() {
		config.Data.BTLegacyWallet = originalCompat
		core.BackTestMode = originalBacktest
	})
	core.BackTestMode = true
	config.Data.BTLegacyWallet = true

	want := (float64(1) + 1) + 1e16
	entries := []struct {
		key string
		val float64
	}{{"a", 1}, {"b", 1}, {"c", 1e16}}
	for _, target := range []string{"pendings", "frozens"} {
		for shift := range entries {
			values := make(map[string]float64, len(entries))
			for offset := range entries {
				entry := entries[(shift+offset)%len(entries)]
				values[entry.key] = entry.val
			}
			wallet := &ItemWallet{Pendings: map[string]float64{}, Frozens: map[string]float64{}}
			if target == "pendings" {
				wallet.Pendings = values
			} else {
				wallet.Frozens = values
			}
			for range 32 {
				if got := wallet.Used(); math.Float64bits(got) != math.Float64bits(want) {
					t.Fatalf("%s layout %d: Used() = %.17g, want %.17g", target, shift, got, want)
				}
			}
		}
	}
}

func TestLegacyWalletLegalValuesUseStableCoinOrder(t *testing.T) {
	originalCompat := config.Data.BTLegacyWallet
	originalBacktest := core.BackTestMode
	t.Cleanup(func() {
		config.Data.BTLegacyWallet = originalCompat
		core.BackTestMode = originalBacktest
	})

	core.BackTestMode = true
	config.Data.BTLegacyWallet = true
	wallets := &BanWallets{Items: map[string]*ItemWallet{
		"DET_A": {Coin: "DET_A", Available: 1e16},
		"DET_B": {Coin: "DET_B", Available: -1e16},
		"DET_C": {Coin: "DET_C", Available: 1},
	}}
	for _, coin := range []string{"DET_A", "DET_B", "DET_C"} {
		com.SetBarPrice(coin+"/USDT", 1)
	}
	for range 100 {
		_, coins, _ := wallets.calcLegal(LegalValueAvailable, nil, false)
		if !slices.Equal(coins, []string{"DET_A", "DET_B", "DET_C"}) {
			t.Fatalf("legacy wallet coin order = %v", coins)
		}
		if got := wallets.AvaLegal(nil); got != 1 {
			t.Fatalf("legacy wallet total = %v, want 1", got)
		}
	}
}

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
