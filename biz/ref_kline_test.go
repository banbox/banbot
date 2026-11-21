package biz

import (
	"context"
	"math"
	"sync"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	ta "github.com/banbox/banta"
)

func TestCrossSymbolRefKlineFiltersAltcoinWithoutTradingRefSymbol(t *testing.T) {
	const (
		tradeSym = "ETH/USDT:USDT"
		refSym   = "BTC/USDT:USDT"
		mainTF   = "1h"
		refTF    = "1d"
		stratID  = "ts04_test:btc_regime_filter"
	)

	coreState, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coreState.Close)
	coreState.SetRunMode(core.RunModeBackTest)
	coreState.ExgName = "binance"
	coreState.Market = banexg.MarketLinear
	coreState.IsContract = true
	coreState.CheckWallets = true

	clock := btime.NewClockState(true, nil)
	dayMS := int64(86400_000)
	hourMS := int64(3600_000)
	baseMS := int64(1_700_006_400_000) // UTC day-aligned
	clock.SetTimeMS(baseMS)

	// Exchange knows both ETH and BTC, while SymbolState and trading `pairs` ONLY contain ETH initially!
	ex := newRiskSizingMockExchange(banexg.MarketLinear, tradeSym, refSym)
	market := com.NewMarketState("binance")
	symbols := orm.NewSymbolStateWithIdentity("binance", banexg.MarketLinear)
	ethExs := &orm.ExSymbol{ID: 10, Exchange: "binance", Market: banexg.MarketLinear, Symbol: tradeSym}
	if setErr := symbols.SetExSymbols([]*orm.ExSymbol{ethExs}); setErr != nil {
		t.Fatal(setErr)
	}
	if got := symbols.GetExSymbol2("binance", banexg.MarketLinear, refSym); got != nil {
		t.Fatalf("precondition failed: %s should not be pre-seeded in SymbolState", refSym)
	}

	var seenJobs []string
	var bullChecks, bearChecks int

	strat.RegisterStrategy(stratID, func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			WarmupNum:    5,
			EachMaxLong:  1,
			EachMaxShort: 0,
			OnStartUp: func(s *strat.StratJob) {
				seenJobs = append(seenJobs, s.Symbol.Symbol)
			},
			OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{s.RefKlineSub(refSym, refTF, 5)}
			},
			OnData: strat.RouteData(strat.DataHandlers{
				Info: func(s *strat.StratJob, d strat.DataEvent) {
					if !d.Closed {
						return
					}
					if btcEnv := s.RefEnv(refSym, refTF); btcEnv != nil {
						_ = ta.SMA(btcEnv.Close, 3)
						_ = ta.ATR(btcEnv.High, btcEnv.Low, btcEnv.Close, 3)
					}
				},
				Main: func(s *strat.StratJob, d strat.DataEvent) {
					btcEnv := s.RefEnv(refSym, refTF)
					if btcEnv == nil {
						return
					}
					btcSMA := ta.SMA(btcEnv.Close, 3).Get(0)
					btcATR := ta.ATR(btcEnv.High, btcEnv.Low, btcEnv.Close, 3).Get(0)
					if s.IsWarmUp || !d.Closed || math.IsNaN(btcSMA) || math.IsNaN(btcATR) {
						return
					}
					if btcEnv.Close.Get(0) > btcSMA {
						bullChecks++
						if s.GetOrderNum(0) == 0 && len(s.Entrys) == 0 {
							if oErr := s.OpenOrder(&strat.EnterReq{Tag: "btc_bull_eth_long", LegalCost: 200}); oErr != nil {
								t.Errorf("ETH OpenOrder failed: %v", oErr)
							}
						}
					} else {
						bearChecks++
						if s.GetOrderNum(1) > 0 {
							if cErr := s.CloseOrders(&strat.ExitReq{Tag: "btc_bear_exit", Dirt: core.OdDirtLong}); cErr != nil {
								t.Errorf("ETH CloseOrders failed: %v", cErr)
							}
						}
					}
				},
			}),
		}
	})
	t.Cleanup(func() { strat.UnregisterStrategy(stratID) })

	cfg := &config.Config{
		Name:          "ts04-ref-bt",
		Exchange:      &config.ExchangeConfig{Name: "binance"},
		MarketType:    banexg.MarketLinear,
		Leverage:      5,
		StakeAmount:   100,
		StakeCurrency: []string{"USDT"},
		OpenVolRate:   1,
		WalletAmounts: map[string]float64{"USDT": 10000},
		Pairs:         []string{tradeSym},
		RunTimeframes: []string{mainTF},
		Accounts:      map[string]*config.AccountConfig{"default": {Leverage: 5}},
		RunPolicy:     []*config.RunPolicyConfig{{Name: stratID, RunTimeframes: []string{mainTF}}},
	}
	deps := RuntimeDeps{
		Core:           coreState,
		Clock:          clock,
		Market:         market,
		Batch:          strat.NewBatchState(),
		Strategies:     strat.NewState(),
		Orders:         ormo.NewOrderState(),
		Trading:        NewTradingState(),
		Config:         config.NewSnapshot(cfg),
		Accounts:       map[string]*config.AccountConfig{"default": {Leverage: 5}},
		AccountsMu:     &sync.RWMutex{},
		Symbols:        symbols,
		Exchange:       ex,
		DefaultAccount: "default",
	}
	if bindErr := BindRuntimeDeps(deps); bindErr != nil {
		t.Fatal(bindErr)
	}
	deps.Orders.SetTask("default", &ormo.BotTask{ID: 1})
	InitFakeWalletsWithRuntimeDeps(deps)
	InitLocalOrderMgrWithRuntimeDeps(deps, func(*ormo.InOutOrder, bool) {}, false, coreState.Stop)
	trader, tErr := NewTraderWithRuntimeDeps(deps)
	if tErr != nil {
		t.Fatal(tErr)
	}

	// Only ETH is in pairs and tfScores!
	warms, _, loadErr := strat.LoadStratJobsWithState(
		deps.Strategies, coreState, symbols,
		[]string{tradeSym},
		map[string]map[string]float64{tradeSym: {mainTF: 1.0}},
		deps.Orders,
	)
	if loadErr != nil {
		t.Fatalf("LoadStratJobsWithState failed: %v", loadErr)
	}

	// Verify BTC was automatically ensured in SymbolState and added to warms for 1d,
	// while NO StratJob was created for BTC.
	btcExs := symbols.GetExSymbol2("binance", banexg.MarketLinear, refSym)
	if btcExs == nil || btcExs.ID <= 0 {
		t.Fatalf("expected %s to be ensured in SymbolState, got %+v", refSym, btcExs)
	}
	if warms[refSym][refTF] != 5 {
		t.Fatalf("expected warms[%s][%s] == 5, got %+v", refSym, refTF, warms)
	}
	if len(seenJobs) != 1 || seenJobs[0] != tradeSym {
		t.Fatalf("OnStartUp jobs = %v, want only [%s]", seenJobs, tradeSym)
	}
	for _, job := range deps.Strategies.CollectJobs() {
		if job.Symbol.Symbol != tradeSym {
			t.Fatalf("unexpected trading StratJob created for %s", job.Symbol.Symbol)
		}
	}

	makeBar := func(exs *orm.ExSymbol, tf string, startMS, durMS int64, closeP float64, warm bool) *orm.DataSeries {
		return &orm.DataSeries{
			Source:    orm.SeriesSourceKline,
			Sid:       exs.ID,
			ExSymbol:  exs,
			TimeFrame: tf,
			TimeMS:    startMS,
			EndMS:     startMS + durMS,
			IsWarmUp:  warm,
			Closed:    true,
			Values: map[string]any{
				"open":       closeP - 10,
				"high":       closeP + 20,
				"low":        closeP - 20,
				"close":      closeP,
				"volume":     1000.0,
				"quote":      closeP * 1000.0,
				"buy_volume": 500.0,
				"trade_num":  int64(200),
			},
		}
	}

	// 1. Feed 5 warmup daily BTC bars (bearish: 40000 -> 39000 -> 38000 -> 37000 -> 36000; close < SMA(3)).
	btcWarmPrices := []float64{40000, 39000, 38000, 37000, 36000}
	for i, p := range btcWarmPrices {
		ts := baseMS - int64(len(btcWarmPrices)-i)*dayMS
		clock.SetTimeMS(ts + dayMS)
		if fErr := trader.FeedDataSeries(makeBar(btcExs, refTF, ts, dayMS, p, true)); fErr != nil {
			t.Fatalf("feed BTC warmup bar %d failed: %v", i, fErr)
		}
	}

	// 2. Feed 2 live ETH 1h bars while BTC is still in bearish regime -> ETH must NOT open any order.
	for h := int64(0); h < 2; h++ {
		ts := baseMS + h*hourMS
		clock.SetTimeMS(ts + hourMS)
		if fErr := trader.FeedDataSeries(makeBar(ethExs, mainTF, ts, hourMS, 2000+float64(h)*10, false)); fErr != nil {
			t.Fatalf("feed ETH bar %d failed: %v", h, fErr)
		}
	}
	openOds, lock := deps.Orders.GetOpenODs("default")
	lock.Lock()
	countBear := len(openOds)
	lock.Unlock()
	if bearChecks != 2 || bullChecks != 0 || countBear != 0 {
		t.Fatalf("expected bearish BTC filter to block ETH entry: bear=%d bull=%d openOrders=%d", bearChecks, bullChecks, countBear)
	}

	// 3. Feed a bullish live BTC 1d bar (42000 > SMA(3) of [37000, 36000, 42000] = 38333.33).
	clock.SetTimeMS(baseMS + dayMS)
	if fErr := trader.FeedDataSeries(makeBar(btcExs, refTF, baseMS, dayMS, 42000, false)); fErr != nil {
		t.Fatalf("feed live BTC bar failed: %v", fErr)
	}

	// Confirm feeding BTC bar itself never opened any BTC order.
	lock.Lock()
	countAfterBTC := len(openOds)
	lock.Unlock()
	if countAfterBTC != 0 {
		t.Fatalf("BTC reference bar must not open orders, got %d", countAfterBTC)
	}

	// 4. Feed next ETH 1h bar + 1 more ETH bar to fill entry -> ETH opens long, BTC has 0 orders.
	for h := int64(24); h < 26; h++ {
		ts := baseMS + h*hourMS
		clock.SetTimeMS(ts + hourMS)
		if fErr := trader.FeedDataSeries(makeBar(ethExs, mainTF, ts, hourMS, 2100+float64(h), false)); fErr != nil {
			t.Fatalf("feed ETH bull bar %d failed: %v", h, fErr)
		}
	}

	lock.Lock()
	var symbolsWithOrders []string
	for _, od := range openOds {
		symbolsWithOrders = append(symbolsWithOrders, od.Symbol)
	}
	lock.Unlock()
	if bullChecks == 0 || len(symbolsWithOrders) != 1 || symbolsWithOrders[0] != tradeSym {
		t.Fatalf("expected 1 open order on %s under bullish BTC regime, bullChecks=%d orders=%v",
			tradeSym, bullChecks, symbolsWithOrders)
	}
}
