package strat

import (
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	ta "github.com/banbox/banta"
)

func TestStratJobCanOpenUsesRuntimeLocalMarket(t *testing.T) {
	oldMarket := core.Market
	t.Cleanup(func() { core.Market = oldMarket })
	core.Market = banexg.MarketSpot

	linear := &StratJob{
		Symbol:       &orm.ExSymbol{Exchange: "runtime-a", Market: banexg.MarketLinear},
		MaxOpenShort: 0,
		CloseLong:    true,
		CloseShort:   true,
	}
	spot := &StratJob{
		Symbol:       &orm.ExSymbol{Exchange: "runtime-b", Market: banexg.MarketSpot},
		MaxOpenShort: 0,
		CloseLong:    true,
		CloseShort:   true,
	}

	if !linear.CanOpen(true) {
		t.Fatal("linear runtime job inherited spot market from core.Market")
	}
	if spot.CanOpen(true) {
		t.Fatal("spot runtime job allowed short opening")
	}
}

func TestStratJobCanOpenUsesEnvBeforeLegacyMarket(t *testing.T) {
	oldMarket := core.Market
	t.Cleanup(func() { core.Market = oldMarket })
	core.Market = banexg.MarketLinear

	job := &StratJob{Env: &ta.BarEnv{MarketType: banexg.MarketSpot}}
	if job.CanOpen(true) {
		t.Fatal("job env market was ignored")
	}

	legacy := &StratJob{}
	if !legacy.CanOpen(true) {
		t.Fatal("legacy zero-value job no longer follows core.Market")
	}
}
