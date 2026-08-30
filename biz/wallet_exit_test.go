package biz

import (
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
)

func TestExitOdSkipsZeroAvailableSpotBase(t *testing.T) {
	oldEnvReal := core.EnvReal
	core.EnvReal = false
	t.Cleanup(func() { core.EnvReal = oldEnvReal })

	const symbol = "ZEROEXIT/USDT"
	restoreSymbols, err := orm.InstallFrozenExSymbols([]*orm.ExSymbol{{
		ID: 900001, Exchange: "binance", Market: banexg.MarketSpot, Symbol: symbol,
	}})
	if err != nil {
		t.Fatalf("install test symbol: %v", err)
	}
	t.Cleanup(restoreSymbols)

	od := &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 1, Sid: 900001, Symbol: symbol}}
	base := &ItemWallet{Coin: "ZEROEXIT", Pendings: map[string]float64{}, Frozens: map[string]float64{}}
	quote := &ItemWallet{
		Coin: "USDT", Pendings: map[string]float64{od.Key(): 5}, Frozens: map[string]float64{},
	}
	wallets := &BanWallets{Items: map[string]*ItemWallet{"ZEROEXIT": base, "USDT": quote}}

	wallets.ExitOd(od, 1)

	if base.Available != 0 {
		t.Fatalf("zero base balance changed to %v", base.Available)
	}
	if _, ok := base.Pendings[od.Key()]; ok {
		t.Fatal("zero base balance created a pending debit")
	}
	if _, ok := quote.Pendings[od.Key()]; ok {
		t.Fatal("quote pending was not cancelled")
	}
	if quote.Available != 5 {
		t.Fatalf("cancelled quote pending = %v, want 5", quote.Available)
	}
}

func TestExitOdPreservesPositiveSpotPartialExit(t *testing.T) {
	oldEnvReal := core.EnvReal
	core.EnvReal = false
	t.Cleanup(func() { core.EnvReal = oldEnvReal })

	const symbol = "PARTIALEXIT/USDT"
	restoreSymbols, err := orm.InstallFrozenExSymbols([]*orm.ExSymbol{{
		ID: 900002, Exchange: "binance", Market: banexg.MarketSpot, Symbol: symbol,
	}})
	if err != nil {
		t.Fatalf("install test symbol: %v", err)
	}
	t.Cleanup(restoreSymbols)

	od := &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 2, Sid: 900002, Symbol: symbol}}
	base := &ItemWallet{Coin: "PARTIALEXIT", Available: 2, Pendings: map[string]float64{}, Frozens: map[string]float64{}}
	quote := &ItemWallet{Coin: "USDT", Pendings: map[string]float64{}, Frozens: map[string]float64{}}
	wallets := &BanWallets{Items: map[string]*ItemWallet{"PARTIALEXIT": base, "USDT": quote}}

	wallets.ExitOd(od, 1)

	if base.Available != 1 {
		t.Fatalf("remaining base balance = %v, want 1", base.Available)
	}
	if got := base.Pendings[od.Key()]; got != 1 {
		t.Fatalf("pending exit amount = %v, want 1", got)
	}
}
