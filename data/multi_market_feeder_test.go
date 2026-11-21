package data

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
)

func TestValidateFeederRuntimeIdentityMultiMarketOptIn(t *testing.T) {
	spotSym := &orm.ExSymbol{ID: 1, Exchange: "binance", Market: banexg.MarketSpot, Symbol: "BTC/USDT"}
	perpSym := &orm.ExSymbol{ID: 2, Exchange: "binance", Market: banexg.MarketLinear, Symbol: "BTC/USDT:USDT"}
	invSym := &orm.ExSymbol{ID: 3, Exchange: "binance", Market: banexg.MarketInverse, Symbol: "BTC/USD:BTC"}
	foreignSym := &orm.ExSymbol{ID: 4, Exchange: "okx", Market: banexg.MarketSpot, Symbol: "BTC/USDT"}

	strictSymbols := orm.NewSymbolStateWithIdentity("binance", banexg.MarketLinear)
	strictDeps := &RuntimeDeps{
		Core:         &core.State{ExgName: "binance", Market: banexg.MarketLinear},
		Clock:        btime.NewClockState(true, nil),
		Config:       config.NewSnapshotWithDirs(&config.Config{MarketType: banexg.MarketLinear}, "", ""),
		Symbols:      strictSymbols,
		ExchangeName: "binance",
		MarketType:   banexg.MarketLinear,
		Exchange:     &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear}},
	}
	if err := validateFeederRuntimeIdentity(strictDeps, perpSym); err != nil {
		t.Fatalf("primary linear symbol rejected: %v", err)
	}
	if err := validateFeederRuntimeIdentity(strictDeps, spotSym); err == nil || !strings.Contains(err.Error(), "does not match runtime") {
		t.Fatalf("expected strict single-market feeder to reject spot symbol, got %v", err)
	}

	multiSymbols := orm.NewSymbolStateWithIdentity("binance", banexg.MarketLinear)
	multiSymbols.AllowExtraMarkets(banexg.MarketSpot)
	multiDeps := &RuntimeDeps{
		Core:         &core.State{ExgName: "binance", Market: banexg.MarketLinear},
		Clock:        btime.NewClockState(true, nil),
		Config:       config.NewSnapshotWithDirs(&config.Config{MarketType: banexg.MarketLinear, ExtraMarkets: []string{banexg.MarketSpot}}, "", ""),
		Symbols:      multiSymbols,
		ExchangeName: "binance",
		MarketType:   banexg.MarketLinear,
		Exchange:     &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear}},
	}
	if err := validateFeederRuntimeIdentity(multiDeps, perpSym); err != nil {
		t.Fatalf("multi-market feeder rejected linear symbol: %v", err)
	}
	if err := validateFeederRuntimeIdentity(multiDeps, spotSym); err != nil {
		t.Fatalf("multi-market feeder rejected opted-in spot symbol: %v", err)
	}
	if err := validateFeederRuntimeIdentity(multiDeps, invSym); err == nil {
		t.Fatal("multi-market feeder accepted unconfigured inverse symbol")
	}
	if err := validateFeederRuntimeIdentity(multiDeps, foreignSym); err == nil {
		t.Fatal("multi-market feeder accepted foreign exchange symbol")
	}
}
