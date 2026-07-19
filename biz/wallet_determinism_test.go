package biz

import (
	"math"
	"slices"
	"testing"

	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/orm/ormo"
)

func TestItemWalletUsedStableAcrossMapLayouts(t *testing.T) {
	want := (float64(1) + 1) + 1e16
	entries := []struct {
		key string
		val float64
	}{
		{key: "a", val: 1},
		{key: "b", val: 1},
		{key: "c", val: 1e16},
	}

	for _, target := range []string{"pendings", "frozens"} {
		for shift := range entries {
			values := make(map[string]float64, len(entries))
			for offset := range entries {
				entry := entries[(shift+offset)%len(entries)]
				values[entry.key] = entry.val
			}
			wallet := &ItemWallet{}
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

func TestFiatValueUsesStableCoinOrder(t *testing.T) {
	wallets := &BanWallets{Items: map[string]*ItemWallet{
		"USD3": {Coin: "USD3", Available: 1e16},
		"USD1": {Coin: "USD1", Available: 1},
		"USD2": {Coin: "USD2", Available: 1},
	}}
	want := (float64(1) + 1) + 1e16

	for range 32 {
		if got := wallets.FiatValue(false); math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("FiatValue() = %.17g, want %.17g", got, want)
		}
	}
}

func TestItemWalletUsedKeepsLiveAggregateShortCircuit(t *testing.T) {
	wallet := &ItemWallet{
		Pendings: map[string]float64{"*": 7, "stale-order": 1e16},
		Frozens:  map[string]float64{"*": 11, "stale-order": 1e16},
	}
	if got := wallet.Used(); got != 18 {
		t.Fatalf("Used() = %v, want live aggregates only (18)", got)
	}
}

func TestCalcLegalUsesStableCoinOrder(t *testing.T) {
	wallets := &BanWallets{Items: map[string]*ItemWallet{
		"USD3": {Available: 1e16},
		"USD1": {Available: 1},
		"USD2": {Available: 1},
	}}
	wantCoins := []string{"USD1", "USD2", "USD3"}
	wantTotal := (float64(1) + 1) + 1e16

	for range 32 {
		_, coins, _ := wallets.calcLegal(LegalValueAvailable, nil, false)
		if !slices.Equal(coins, wantCoins) {
			t.Fatalf("calcLegal coins = %v, want %v", coins, wantCoins)
		}
		if got := wallets.TotalLegal(nil, false); math.Float64bits(got) != math.Float64bits(wantTotal) {
			t.Fatalf("TotalLegal() = %.17g, want %.17g", got, wantTotal)
		}
	}
}

func TestUpdateOdsUsesStableOrder(t *testing.T) {
	orders := []*ormo.InOutOrder{
		{IOrder: &ormo.IOrder{ID: 1, Profit: 1}},
		{IOrder: &ormo.IOrder{ID: 2, Profit: 1}},
		{IOrder: &ormo.IOrder{ID: 3, Profit: 1e16}},
	}
	want := (float64(1) + 1) + 1e16
	permutations := [][]*ormo.InOutOrder{
		{orders[0], orders[1], orders[2]},
		{orders[2], orders[0], orders[1]},
		{orders[1], orders[2], orders[0]},
	}

	for i, permutation := range permutations {
		wallets := &BanWallets{Items: make(map[string]*ItemWallet)}
		if err := wallets.UpdateOds(permutation, "USDT"); err != nil {
			t.Fatalf("permutation %d: UpdateOds() error: %v", i, err)
		}
		got := wallets.Items["USDT"].UnrealizedPOL
		if math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("permutation %d: UnrealizedPOL = %.17g, want %.17g", i, got, want)
		}
	}
}

func TestUpdateOdsStabilizesSharedMarginAllocation(t *testing.T) {
	orders := []*ormo.InOutOrder{
		marginOrder(1, "BTC/USDT:USDT", 60, 20),
		marginOrder(2, "ETH/USDT:USDT", 50, 15),
		marginOrder(3, "SAND/USDT:USDT", 40, 15),
	}
	permutations := [][]*ormo.InOutOrder{
		{orders[0], orders[1], orders[2]},
		{orders[2], orders[0], orders[1]},
		{orders[1], orders[2], orders[0]},
	}

	for _, permutation := range permutations {
		wallet := &ItemWallet{
			Coin: "USDT", Available: 70, Pendings: map[string]float64{},
			Frozens: map[string]float64{},
		}
		wallets := &BanWallets{Items: map[string]*ItemWallet{"USDT": wallet}}
		if err := wallets.UpdateOds(permutation, "USDT"); err != nil {
			t.Fatalf("permutation %v: UpdateOds() error: %v", orderIDs(permutation), err)
		}

		if wallet.Available != 10 || wallet.UsedUPol != 50 || wallet.Frozens[orders[0].Key()] != 10 ||
			wallet.Frozens[orders[1].Key()] != 50 {
			t.Fatalf("permutation %v: unstable margin state: available=%v usedUPol=%v frozens=%v",
				orderIDs(permutation), wallet.Available, wallet.UsedUPol, wallet.Frozens)
		}
		if _, exists := wallet.Frozens[orders[2].Key()]; exists {
			t.Fatalf("permutation %v: order 3 unexpectedly received margin", orderIDs(permutation))
		}
		if _, err := wallets.CostAva("next-entry", "USDT", 20, false, 0.9); err == nil {
			t.Fatalf("permutation %v: downstream entry unexpectedly admitted", orderIDs(permutation))
		}
	}
}

func marginOrder(id int64, symbol string, margin, profit float64) *ormo.InOutOrder {
	com.SetBarPrice(symbol, 1)
	return &ormo.InOutOrder{
		IOrder: &ormo.IOrder{ID: id, Symbol: symbol, EnterAt: id, Leverage: 1, Profit: profit},
		Enter:  &ormo.ExOrder{Average: 0.5, Filled: margin},
	}
}

func orderIDs(orders []*ormo.InOutOrder) []int64 {
	ids := make([]int64, len(orders))
	for index, order := range orders {
		ids[index] = order.ID
	}
	return ids
}
