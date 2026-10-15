package biz

import (
	"math"
	"slices"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
)

func TestDeterministicWalletOrderingIsOptIn(t *testing.T) {
	oldMode, oldData := core.BackTestMode, config.Data
	t.Cleanup(func() {
		core.BackTestMode, config.Data = oldMode, oldData
	})
	core.BackTestMode = true
	config.Data.BTLegacyWallet, config.Data.BTStrict = false, false
	orders := []*ormo.InOutOrder{
		{IOrder: &ormo.IOrder{ID: 2}},
		{IOrder: &ormo.IOrder{ID: 1}},
	}
	if got := legacyWalletOrderView(orders); got[0].ID != 2 || &got[0] != &orders[0] {
		t.Fatal("default backtest behavior unexpectedly reordered orders")
	}
	config.Data.BTStrict = true
	if got := legacyWalletOrderView(orders); got[0].ID != 1 || orders[0].ID != 2 {
		t.Fatal("deterministic order view did not sort a clone")
	}
}

func TestDeterministicWalletReductionsUseCanonicalOrder(t *testing.T) {
	enableStrictBacktest(t)
	wallets := &BanWallets{Items: map[string]*ItemWallet{
		"USD3": {Coin: "USD3", Available: 1e16},
		"USD1": {Coin: "USD1", Available: 1},
		"USD2": {Coin: "USD2", Available: 1},
	}}
	want := (float64(1) + 1) + 1e16

	for range 32 {
		if got := sumWalletMap(map[string]float64{"c": 1e16, "a": 1, "b": 1}); math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("sumWalletMap() = %.17g, want %.17g", got, want)
		}
		_, coins, _ := wallets.calcLegal(LegalValueAvailable, nil, false)
		if !slices.Equal(coins, []string{"USD1", "USD2", "USD3"}) {
			t.Fatalf("calcLegal coins = %v", coins)
		}
		if got := wallets.FiatValue(false); math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("FiatValue() = %.17g, want %.17g", got, want)
		}
	}
}

func TestDeterministicUpdateOdsUsesStableOrder(t *testing.T) {
	enableStrictBacktest(t)
	orders := []*ormo.InOutOrder{
		{IOrder: &ormo.IOrder{ID: 1, Profit: 1}},
		{IOrder: &ormo.IOrder{ID: 2, Profit: 1}},
		{IOrder: &ormo.IOrder{ID: 3, Profit: 1e16}},
	}
	want := (float64(1) + 1) + 1e16
	for i, permutation := range [][]*ormo.InOutOrder{
		{orders[0], orders[1], orders[2]},
		{orders[2], orders[0], orders[1]},
		{orders[1], orders[2], orders[0]},
	} {
		wallets := &BanWallets{Items: make(map[string]*ItemWallet)}
		if err := wallets.UpdateOds(permutation, "USDT"); err != nil {
			t.Fatalf("permutation %d: %v", i, err)
		}
		if got := wallets.Items["USDT"].UnrealizedPOL; math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("permutation %d: UnrealizedPOL = %.17g, want %.17g", i, got, want)
		}
	}
}
