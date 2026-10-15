package biz

import (
	"math"
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

func TestConfirmOdExitUsesNetSpotBaseAfterEntryFee(t *testing.T) {
	oldEnvReal, oldIsContract := core.EnvReal, core.IsContract
	core.EnvReal = false
	core.IsContract = false
	t.Cleanup(func() {
		core.EnvReal = oldEnvReal
		core.IsContract = oldIsContract
	})

	const symbol = "BASEFEE/USDT"
	restoreSymbols, err := orm.InstallFrozenExSymbols([]*orm.ExSymbol{{
		ID: 900003, Exchange: "binance", Market: banexg.MarketSpot, Symbol: symbol,
	}})
	if err != nil {
		t.Fatalf("install test symbol: %v", err)
	}
	t.Cleanup(restoreSymbols)

	od := &ormo.InOutOrder{
		IOrder: &ormo.IOrder{ID: 3, Sid: 900003, Symbol: symbol, EnterAt: 1},
		Enter:  &ormo.ExOrder{Enter: true, Amount: 100, Filled: 100, Average: 100, FeeQuote: 10, FeeType: "BASEFEE"},
		Exit:   &ormo.ExOrder{Amount: 100, Filled: 100, Average: 110, FeeQuote: 11, FeeType: "USDT"},
	}
	quote := &ItemWallet{Coin: "USDT", Available: 10000, Pendings: map[string]float64{}, Frozens: map[string]float64{}}
	base := &ItemWallet{Coin: "BASEFEE", Pendings: map[string]float64{}, Frozens: map[string]float64{}}
	wallets := &BanWallets{Items: map[string]*ItemWallet{"BASEFEE": base, "USDT": quote}}

	if _, err := wallets.CostAva(od.Key(), "USDT", 10000, false, 0); err != nil {
		t.Fatalf("lock entry quote: %v", err)
	}
	wallets.ConfirmOdEnter(od, 100)
	wallets.ExitOd(od, od.Exit.Filled)
	wallets.ConfirmOdExit(od, 110)

	if got := base.Available; math.Abs(got) > 1e-12 || len(base.Pendings) != 0 {
		t.Fatalf("base balance after exit = %.12f, pending=%v; want zero", got, base.Pendings)
	}
	if got := quote.Available; math.Abs(got-10978) > 1e-9 {
		t.Fatalf("quote balance after exit = %.12f, want 10978", got)
	}
}
