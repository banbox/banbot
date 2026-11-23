package biz

import (
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type walletRuntimeExchangeStub struct {
	banexg.BanExchange
	calls        int
	cost         float64
	watchCalls   int
	watchAccount string
	watchOut     chan *banexg.Balances
}

func TestRuntimeWalletValuationUsesOwnPrices(t *testing.T) {
	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(10_000)
	makeWallet := func(price float64) *BanWallets {
		prices := com.NewPriceState("binance")
		prices.SetBarPriceAt(10_000, "BTC/USDT", price)
		wallet := &BanWallets{Items: map[string]*ItemWallet{
			"BTC": {Coin: "BTC", Available: 2, Pendings: map[string]float64{"entry": 1}},
		}}
		wallet.bindRuntimeDeps(RuntimeDeps{Clock: clock, Market: &com.MarketState{Prices: prices}})
		return wallet
	}
	first, second := makeWallet(100), makeWallet(200)
	if first.TotalLegal(nil, false) != 300 || second.TotalLegal(nil, false) != 600 {
		t.Fatal("wallet totals do not use runtime prices")
	}
	if first.FiatValue(false) != 300 || second.FiatValue(false) != 600 {
		t.Fatal("fiat valuation does not use runtime prices")
	}
	if first.GetAmountByLegal("BTC", 200) != 2 || second.GetAmountByLegal("BTC", 200) != 1 {
		t.Fatal("wallet conversion does not use runtime prices")
	}
}

func (s *walletRuntimeExchangeStub) CalcMaintMargin(_ string, cost float64) (float64, *errs.Error) {
	s.calls++
	s.cost = cost
	return 0, nil
}

func (s *walletRuntimeExchangeStub) WatchBalance(params map[string]interface{}) (chan *banexg.Balances, *errs.Error) {
	s.watchCalls++
	s.watchAccount, _ = params[banexg.ParamAccount].(string)
	return s.watchOut, nil
}

func TestRuntimeWalletUpdateOdsUsesBoundPriceAndExchange(t *testing.T) {
	const symbol = "RUNTIME/USDT:USDT"
	oldDefault := exg.Default
	legacyExchange := &walletRuntimeExchangeStub{}
	exg.Default = legacyExchange
	t.Cleanup(func() { exg.Default = oldDefault })

	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	state.SetRunMode(core.RunModeLive)
	state.SetRunEnv(core.RunEnvDryRun)
	clock := btime.NewClockState(false, nil)
	prices := com.NewPriceState("runtime")
	prices.SetBarPriceAt(btime.UTCStamp(), symbol, 10)
	exchange := &walletRuntimeExchangeStub{}

	wallets := &BanWallets{Items: map[string]*ItemWallet{
		"USDT": {Coin: "USDT", Available: 1000, Pendings: map[string]float64{}, Frozens: map[string]float64{}},
	}}
	wallets.bindRuntimeDeps(RuntimeDeps{
		Core:     state,
		Clock:    clock,
		Market:   &com.MarketState{Prices: prices},
		Config:   config.NewSnapshot(&config.Config{MarginAddRate: 0.5}),
		Exchange: exchange,
	})

	order := &ormo.InOutOrder{
		IOrder: &ormo.IOrder{ID: 1, Symbol: symbol, Profit: -1, Leverage: 1},
		Enter:  &ormo.ExOrder{Filled: 1, Average: 20},
	}
	if err := wallets.UpdateOds([]*ormo.InOutOrder{order}, "USDT"); err != nil {
		t.Fatal(err)
	}
	if exchange.calls != 1 || exchange.cost != 10 {
		t.Fatalf("runtime exchange calls/cost = %d/%.2f, want 1/10", exchange.calls, exchange.cost)
	}
	if legacyExchange.calls != 0 {
		t.Fatalf("legacy exchange was used %d times, want 0", legacyExchange.calls)
	}
}

func TestRuntimeWalletUpdateOdsRequiresBoundExchange(t *testing.T) {
	const symbol = "RUNTIME/USDT:USDT"
	oldDefault := exg.Default
	legacyExchange := &walletRuntimeExchangeStub{}
	exg.Default = legacyExchange
	t.Cleanup(func() { exg.Default = oldDefault })

	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	state.SetRunMode(core.RunModeLive)
	state.SetRunEnv(core.RunEnvDryRun)
	clock := btime.NewClockState(false, nil)
	prices := com.NewPriceState("runtime")
	prices.SetBarPriceAt(btime.UTCStamp(), symbol, 10)
	wallets := &BanWallets{Items: map[string]*ItemWallet{
		"USDT": {Coin: "USDT", Available: 1000, Pendings: map[string]float64{}, Frozens: map[string]float64{}},
	}}
	wallets.bindRuntimeDeps(RuntimeDeps{
		Core:   state,
		Clock:  clock,
		Market: &com.MarketState{Prices: prices},
		Config: config.NewSnapshot(&config.Config{MarginAddRate: 0.5}),
	})

	order := &ormo.InOutOrder{
		IOrder: &ormo.IOrder{ID: 1, Symbol: symbol, Profit: -1, Leverage: 1},
		Enter:  &ormo.ExOrder{Filled: 1, Average: 20},
	}
	if err := wallets.UpdateOds([]*ormo.InOutOrder{order}, "USDT"); err == nil || err.Code != core.ErrExgNotInit {
		t.Fatalf("UpdateOds error = %v, want missing runtime exchange", err)
	}
	if legacyExchange.calls != 0 {
		t.Fatalf("legacy exchange was used %d times, want 0", legacyExchange.calls)
	}
}

func TestRuntimeOrderMgrKeepsExplicitWalletAccount(t *testing.T) {
	oldEnvReal, oldDefAcc := core.EnvReal, config.DefAcc
	const account = "runtime-wallet-account"
	core.EnvReal = false
	config.DefAcc = "default"
	t.Cleanup(func() {
		core.EnvReal = oldEnvReal
		config.DefAcc = oldDefAcc
		delete(accWallets, account)
	})

	mgr := &OrderMgr{Account: account}
	mgr.bindRuntimeDeps(RuntimeDeps{Core: &core.State{}})
	if mgr.wallet == nil {
		t.Fatal("runtime manager did not bind a wallet")
	}
	if mgr.wallet.Account != account {
		t.Fatalf("runtime wallet account = %q, want %q", mgr.wallet.Account, account)
	}
}

func TestRuntimeWalletUpdateOdsUsesBoundCoreAndConfig(t *testing.T) {
	oldBacktest, oldCharge := core.BackTestMode, config.ChargeOnBomb
	t.Cleanup(func() { core.BackTestMode, config.ChargeOnBomb = oldBacktest, oldCharge })
	core.BackTestMode = false
	config.ChargeOnBomb = true

	state, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	state.SetRunMode(core.RunModeBackTest)
	wallet := &ItemWallet{
		Coin: "USDT", Available: 1,
		Pendings: map[string]float64{"pending": 1},
		Frozens:  map[string]float64{"frozen": 98},
	}
	wallets := &BanWallets{Items: map[string]*ItemWallet{"USDT": wallet}}
	wallets.bindRuntimeDeps(RuntimeDeps{
		Core:   state,
		Config: config.NewSnapshot(&config.Config{ChargeOnBomb: false}),
	})

	order := &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 1, Profit: -100}}
	if err := wallets.UpdateOds([]*ormo.InOutOrder{order}, "USDT"); err == nil || err.Code != core.ErrLiquidation {
		t.Fatalf("UpdateOds error = %v, want liquidation", err)
	}
	if wallet.Total(false) != 100 || len(wallet.Pendings) != 1 || len(wallet.Frozens) != 1 {
		t.Fatalf("runtime config/core reset wallet: total=%v pending=%v frozen=%v", wallet.Total(false), wallet.Pendings, wallet.Frozens)
	}
}
