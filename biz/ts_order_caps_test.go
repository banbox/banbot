package biz

import (
	"context"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/shopspring/decimal"
	ta "github.com/banbox/banta"
)

type tsCapsMockExchange struct {
	banexg.BanExchange
	market *banexg.Market
}

func newTSCapsMockExchange(symbol string) *tsCapsMockExchange {
	m := &banexg.Market{
		ID: symbol, Symbol: symbol, Base: "ETH", Quote: "USDT", Settle: "USDT",
		Type: banexg.MarketLinear, Contract: true, Linear: true, Swap: true, Active: true,
		ContractSize: 1,
		Precision: &banexg.Precision{
			Amount: 0.0001, Price: 0.0001,
			ModeAmount: banexg.PrecModeTickSize, ModePrice: banexg.PrecModeTickSize,
		},
		Limits: &banexg.MarketLimits{
			Amount: &banexg.LimitRange{Min: 0.0001, Max: 1_000_000},
			Cost:   &banexg.LimitRange{Min: 1, Max: 10_000_000},
		},
	}
	baseEx := &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear, Markets: map[string]*banexg.Market{symbol: m}}}
	return &tsCapsMockExchange{BanExchange: baseEx, market: m}
}

func (e *tsCapsMockExchange) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear}
}

func (e *tsCapsMockExchange) GetMarket(string) (*banexg.Market, *errs.Error) {
	return e.market, nil
}

func (e *tsCapsMockExchange) PrecPrice(_ *banexg.Market, p float64) (float64, *errs.Error) {
	return math.Round(p*10000) / 10000, nil
}

func (e *tsCapsMockExchange) PrecAmount(_ *banexg.Market, a float64) (float64, *errs.Error) {
	return math.Round(a*10000) / 10000, nil
}

func (e *tsCapsMockExchange) PrecCost(_ *banexg.Market, c float64) (float64, *errs.Error) {
	return math.Round(c*10000) / 10000, nil
}

func (e *tsCapsMockExchange) CalcMaintMargin(_ string, cost float64) (float64, *errs.Error) {
	return cost * 0.005, nil
}

func (e *tsCapsMockExchange) GetLeverage(string, float64, string) (float64, float64) {
	return 5, 20
}

func (e *tsCapsMockExchange) PriceSymbolParts(string) ([4]string, *errs.Error) {
	return [4]string{"ETH", "USDT", "USDT", ""}, nil
}

func (e *tsCapsMockExchange) CalculateFee(string, string, string, float64, float64, bool, map[string]interface{}) (*banexg.Fee, *errs.Error) {
	return &banexg.Fee{Currency: "USDT", Cost: 0, QuoteCost: 0}, nil
}

func setupTSCapsBacktest(t *testing.T, symbol string, initPrice float64) (*LocalOrderMgr, *orm.ExSymbol, *strat.StratJob) {
	t.Helper()
	coreState, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coreState.Close)
	coreState.SetRunMode(core.RunModeBackTest)
	coreState.BackTestMode = true
	coreState.EnvReal = false
	coreState.Market = banexg.MarketLinear
	coreState.IsContract = true
	coreState.ExgName = "binance"

	clock := btime.NewClockState(true, nil)
	startMS := int64(1_700_000_040_000)
	clock.SetTimeMS(startMS)

	exs := &orm.ExSymbol{ID: 1, Exchange: "binance", Market: banexg.MarketLinear, Symbol: symbol}
	symbols := orm.NewSymbolState()
	if setErr := symbols.SetExSymbols([]*orm.ExSymbol{exs}); setErr != nil {
		t.Fatal(setErr)
	}

	cfgData := &config.Config{
		Name:          "ts-caps",
		MarketType:    banexg.MarketLinear,
		StakeAmount:   200,
		Leverage:      5,
		OpenVolRate:   1,
		BTNetCost:     0,
		WalletAmounts: map[string]float64{"USDT": 100_000},
		Accounts:      map[string]*config.AccountConfig{"default": {Leverage: 5}},
	}
	cfg := config.NewSnapshotWithDirs(cfgData, t.TempDir(), "")
	exgMock := newTSCapsMockExchange(symbol)
	market := com.NewMarketState("binance")
	deps := RuntimeDeps{
		Core:           coreState,
		Clock:          clock,
		Market:         market,
		Batch:          strat.NewBatchState(),
		Strategies:     strat.NewState(),
		Orders:         ormo.NewOrderState(),
		Trading:        NewTradingState(),
		Config:         cfg,
		Accounts:       map[string]*config.AccountConfig{"default": {Leverage: 5}},
		AccountsMu:     &sync.RWMutex{},
		Symbols:        symbols,
		Exchange:       exgMock,
		DefaultAccount: "default",
	}
	if bindErr := BindRuntimeDeps(deps); bindErr != nil {
		t.Fatal(bindErr)
	}
	deps.Orders.SetTask("default", &ormo.BotTask{ID: 1})
	InitFakeWalletsWithRuntimeDeps(deps)
	InitLocalOrderMgrWithRuntimeDeps(deps, func(*ormo.InOutOrder, bool) {}, false)
	deps.Market.Prices.SetBarPriceAt(startMS, symbol, initPrice)

	mgr := deps.Trading.OrderManager("default").(*LocalOrderMgr)

	env, envErr := ta.NewBarEnv("binance", banexg.MarketLinear, symbol, "1m")
	if envErr != nil {
		t.Fatal(envErr)
	}
	for i := 0; i < 5; i++ {
		ts := startMS - int64(5-i)*60_000
		if bErr := env.OnBar(ts, initPrice, initPrice+1, initPrice-1, initPrice, 1000, initPrice*1000, 500, 50); bErr != nil {
			t.Fatal(bErr)
		}
	}

	stgy := &strat.TradeStrat{Name: "test:caps"}
	job := &strat.StratJob{
		Strat:         stgy,
		Env:           env,
		DataHub:       strat.NewDataHub(),
		Symbol:        exs,
		TimeFrame:     "1m",
		Account:       "default",
		CloseLong:     true,
		CloseShort:    true,
		ExgStopLoss:   true,
		ExgTakeProfit: true,
	}
	job.BindRuntimeState(deps.Strategies, coreState, clock)
	job.BindRuntimeMarket(deps.Market.Prices, clock)
	return mgr, exs, job
}

func feedTSCapsBar(t *testing.T, mgr *LocalOrderMgr, exs *orm.ExSymbol, barMS int64, o, h, l, c float64) {
	t.Helper()
	endMS := barMS + 60_000
	mgr.clock.SetTimeMS(endMS)
	mgr.prices.SetBarPriceAt(endMS, exs.Symbol, c)
	evt := orm.NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
		Time: barMS, Open: o, High: h, Low: l, Close: c, Volume: 1000,
	}, nil, true, true)
	openMap, lock := mgr.openOrders()
	lock.Lock()
	opens := make([]*ormo.InOutOrder, 0, len(openMap))
	for _, od := range openMap {
		opens = append(opens, od)
	}
	lock.Unlock()
	if err := mgr.UpdateByDataSeries(opens, evt); err != nil {
		t.Fatalf("UpdateByDataSeries failed: %v", err)
	}
}

func TestStopLimitEntryLongAndShortScenarios(t *testing.T) {
	const symbol = "ETH/USDT:USDT"
	const baseMS = int64(1_700_000_040_000)

	t.Run("long triggered and filled", func(t *testing.T) {
		mgr, exs, job := setupTSCapsBacktest(t, symbol, 100)
		if err := job.OpenOrder(&strat.EnterReq{Tag: "long_sl_fill", Stop: 105, Limit: 106, Amount: 1}); err != nil {
			t.Fatal(err)
		}
		ents, _, err := mgr.ProcessOrders(job)
		if err != nil || len(ents) != 1 {
			t.Fatalf("ProcessOrders got %d err=%v", len(ents), err)
		}
		od := ents[0]
		// Bar 1 does not reach Stop=105 -> stays pending with Stop=105.
		feedTSCapsBar(t, mgr, exs, baseMS+60_000, 100, 104, 99, 103)
		if od.Status != ormo.InOutStatusInit || od.Stop != 105 {
			t.Fatalf("expected still pending before stop, got status=%d stop=%v", od.Status, od.Stop)
		}
		// Bar 2 crosses Stop=105 and satisfies Limit=106 -> fills at 105.
		feedTSCapsBar(t, mgr, exs, baseMS+120_000, 103, 108, 102, 107)
		if od.Status != ormo.InOutStatusFullEnter || math.Abs(od.Enter.Average-105) > 1e-9 {
			t.Fatalf("expected long stop-limit fill at 105, got status=%d avg=%v", od.Status, od.Enter.Average)
		}
	})

	t.Run("long triggered but limit not reached then fills on later pullback", func(t *testing.T) {
		mgr, exs, job := setupTSCapsBacktest(t, symbol, 100)
		if err := job.OpenOrder(&strat.EnterReq{Tag: "long_sl_pullback", Stop: 105, Limit: 102, Amount: 1}); err != nil {
			t.Fatal(err)
		}
		ents, _, err := mgr.ProcessOrders(job)
		if err != nil || len(ents) != 1 {
			t.Fatalf("ProcessOrders err=%v", err)
		}
		od := ents[0]
		// Bar 1 rallies 100 -> 98 -> 110 -> 108: 98 occurred BEFORE Stop=105 triggered; post-trigger low is 105 > 102.
		feedTSCapsBar(t, mgr, exs, baseMS+60_000, 100, 110, 98, 108)
		if od.Status != ormo.InOutStatusInit || od.Stop != 0 {
			t.Fatalf("expected stop triggered (Stop=0) while resting limit stays unfilled, got status=%d stop=%v", od.Status, od.Stop)
		}
		// Bar 2 pulls back to Low=101 <= Limit=102 -> resting limit fills at 102!
		feedTSCapsBar(t, mgr, exs, baseMS+120_000, 108, 109, 101, 104)
		if od.Status != ormo.InOutStatusFullEnter || math.Abs(od.Enter.Average-102) > 1e-9 {
			t.Fatalf("expected resting limit fill at 102, got status=%d avg=%v", od.Status, od.Enter.Average)
		}
	})

	t.Run("short triggered+filled and triggered+unfilled", func(t *testing.T) {
		mgr, exs, job := setupTSCapsBacktest(t, symbol, 100)
		if err := job.OpenOrder(&strat.EnterReq{Tag: "short_sl_fill", Short: true, Stop: 95, Limit: 94, Amount: 1}); err != nil {
			t.Fatal(err)
		}
		if err := job.OpenOrder(&strat.EnterReq{Tag: "short_sl_unfilled", Short: true, Stop: 95, Limit: 98, Amount: 1}); err != nil {
			t.Fatal(err)
		}
		ents, _, err := mgr.ProcessOrders(job)
		if err != nil || len(ents) != 2 {
			t.Fatalf("ProcessOrders got %d err=%v", len(ents), err)
		}
		odFill, odRest := ents[0], ents[1]
		// Falling bar 100 -> 101 -> 90 -> 92: Stop=95 triggers; Limit=94 fills at 95, while Limit=98 is above post-trigger [90,95] so rests.
		feedTSCapsBar(t, mgr, exs, baseMS+60_000, 100, 101, 90, 92)
		if odFill.Status != ormo.InOutStatusFullEnter || math.Abs(odFill.Enter.Average-95) > 1e-9 {
			t.Fatalf("short stop-limit fill failed: status=%d avg=%v", odFill.Status, odFill.Enter.Average)
		}
		if odRest.Status != ormo.InOutStatusInit || odRest.Stop != 0 {
			t.Fatalf("short stop-limit bounce should rest with Stop=0: status=%d stop=%v", odRest.Status, odRest.Stop)
		}
	})
}

func TestMultiTPLadderAndMoveSLToBreakeven(t *testing.T) {
	const symbol = "ETH/USDT:USDT"
	const baseMS = int64(1_700_000_040_000)

	t.Run("TP1 50% + MoveSLToBreakeven followed by TP2 50% hit", func(t *testing.T) {
		mgr, exs, job := setupTSCapsBacktest(t, symbol, 100)
		// Deliberately pass TP2 before TP1 to verify nearest-to-farthest sorting and actual fill price resolution.
		req := &strat.EnterReq{
			Tag:      "ladder_tp2",
			Amount:   2.0,
			StopLoss: 95,
			TakeProfits: []strat.TakeProfitTarget{
				{Rate: 0.10, ExitRate: 0.5, Tag: "tp2"},
				{Rate: 0.05, ExitRate: 0.5, MoveSLToBreakeven: true, Tag: "tp1"},
			},
		}
		if err := job.OpenOrder(req); err != nil {
			t.Fatal(err)
		}
		ents, _, err := mgr.ProcessOrders(job)
		if err != nil || len(ents) != 1 {
			t.Fatal(err)
		}
		// Bar 1 enters at ~100 (flat bar at 100).
		feedTSCapsBar(t, mgr, exs, baseMS+60_000, 100, 100, 100, 100)
		od := ents[0]
		if od.Status != ormo.InOutStatusFullEnter || math.Abs(od.Enter.Average-100) > 1e-9 {
			t.Fatalf("entry failed: status=%d avg=%v", od.Status, od.Enter.Average)
		}

		// Bar 2 reaches High=106 (hits TP1=105, misses TP2=110) and closes at 104 (> breakeven 100).
		feedTSCapsBar(t, mgr, exs, baseMS+120_000, 100, 106, 99.5, 104)
		if od.Status != ormo.InOutStatusFullEnter || math.Abs(od.HoldAmount()-1.0) > 1e-9 {
			t.Fatalf("after TP1 remaining hold=%v status=%d, want 1.0 FullEnter", od.HoldAmount(), od.Status)
		}
		if sl := od.GetStopLoss(); sl == nil || math.Abs(sl.Price-100) > 1e-9 {
			t.Fatalf("expected SL tightened to breakeven 100, got %+v", sl)
		}

		// Bar 3 reaches High=112 (hits TP2=110) -> remaining 1.0 exits cleanly with zero open dust.
		feedTSCapsBar(t, mgr, exs, baseMS+180_000, 104, 112, 103, 111)
		openMap, lock := mgr.openOrders()
		lock.Lock()
		openLeft := len(openMap)
		lock.Unlock()
		if openLeft != 0 {
			t.Fatalf("expected 0 open orders after TP2, got %d", openLeft)
		}
		hist := mgr.orderState().HistoricalOrders()
		if len(hist) != 2 {
			t.Fatalf("expected 2 closed ladder legs in history, got %d", len(hist))
		}
		tags := map[string]float64{}
		for _, h := range hist {
			tags[h.ExitTag] = h.Exit.Filled
		}
		if math.Abs(tags["tp1"]-1.0) > 1e-9 || math.Abs(tags["tp2"]-1.0) > 1e-9 {
			t.Fatalf("closed ladder tags/amounts = %+v, want tp1=1.0 tp2=1.0", tags)
		}
	})

	t.Run("TP1 50% + MoveSLToBreakeven followed by retracement hitting breakeven SL", func(t *testing.T) {
		mgr, exs, job := setupTSCapsBacktest(t, symbol, 100)
		req := &strat.EnterReq{
			Tag:      "ladder_be_sl",
			Amount:   2.0,
			StopLoss: 94,
			TakeProfits: []strat.TakeProfitTarget{
				{Val: 5, ExitRate: 0.5, MoveSLToBreakeven: true, Tag: "tp1"},
				{Val: 12, ExitRate: 0.5, Tag: "tp2"},
			},
		}
		if err := job.OpenOrder(req); err != nil {
			t.Fatal(err)
		}
		_, _, err := mgr.ProcessOrders(job)
		if err != nil {
			t.Fatal(err)
		}
		// Bar 1 enters at 100.
		feedTSCapsBar(t, mgr, exs, baseMS+60_000, 100, 100, 100, 100)
		// Bar 2 hits TP1=105 -> moves SL from 94 to breakeven 100.
		feedTSCapsBar(t, mgr, exs, baseMS+120_000, 101, 106, 101, 103)
		// Bar 3 retraces down to 99 (above original SL=94, but breaches breakeven SL=100!).
		feedTSCapsBar(t, mgr, exs, baseMS+180_000, 103, 103.5, 99, 99.5)

		openMap, lock := mgr.openOrders()
		lock.Lock()
		openLeft := len(openMap)
		lock.Unlock()
		if openLeft != 0 {
			t.Fatalf("expected remaining leg stopped out at breakeven, openLeft=%d", openLeft)
		}
		hist := mgr.orderState().HistoricalOrders()
		if len(hist) != 2 {
			t.Fatalf("expected 2 closed legs (tp1 + breakeven sl), got %d", len(hist))
		}
		foundTP1, foundBE := false, false
		for _, h := range hist {
			if h.ExitTag == "tp1" {
				foundTP1 = true
			}
			if (h.ExitTag == core.ExitTagSLTake || h.ExitTag == core.ExitTagStopLoss) && math.Abs(h.Exit.Average-100) < 1e-6 {
				foundBE = true
			}
		}
		if !foundTP1 || !foundBE {
			t.Fatalf("expected tp1 and breakeven SL at 100, got hist: %+v %+v", hist[0].IOrder, hist[1].IOrder)
		}
	})
}

func TestOCOGroupBreakoutStraddleSameBarChronology(t *testing.T) {
	const symbol = "ETH/USDT:USDT"
	const baseMS = int64(1_700_000_040_000)

	t.Run("rising bar breaches Sell Stop at Low first and cancels Buy Stop sibling", func(t *testing.T) {
		mgr, exs, job := setupTSCapsBacktest(t, symbol, 100)
		// Submit Buy Stop FIRST (lower ID), Sell Stop SECOND (higher ID).
		if err := job.OpenOrder(&strat.EnterReq{Tag: "brk_buy", Stop: 105, Amount: 1, OCOGroup: "straddle"}); err != nil {
			t.Fatal(err)
		}
		if err := job.OpenOrder(&strat.EnterReq{Tag: "brk_sell", Short: true, Stop: 95, Amount: 1, OCOGroup: "straddle"}); err != nil {
			t.Fatal(err)
		}
		ents, _, err := mgr.ProcessOrders(job)
		if err != nil || len(ents) != 2 {
			t.Fatalf("ProcessOrders got %d err=%v", len(ents), err)
		}
		buyOd, sellOd := ents[0], ents[1]

		// Rising bar (Open=100 <= Close=106): OHLC path visits Low=92 BEFORE High=108.
		feedTSCapsBar(t, mgr, exs, baseMS+60_000, 100, 108, 92, 106)
		if sellOd.Status != ormo.InOutStatusFullEnter {
			t.Fatalf("expected Sell Stop (first along rising-bar path) to fill, got status=%d", sellOd.Status)
		}
		if buyOd.ExitTag != ExitTagOCOCancel || buyOd.Enter.Filled != 0 {
			t.Fatalf("expected Buy Stop OCO sibling canceled unfilled, got status=%d exitTag=%q filled=%v",
				buyOd.Status, buyOd.ExitTag, buyOd.Enter.Filled)
		}
	})

	t.Run("falling bar breaches Buy Stop at High first and cancels Sell Stop sibling", func(t *testing.T) {
		mgr, exs, job := setupTSCapsBacktest(t, symbol, 100)
		// Submit Sell Stop FIRST (lower ID), Buy Stop SECOND (higher ID).
		if err := job.OpenOrder(&strat.EnterReq{Tag: "brk_sell", Short: true, Stop: 95, Amount: 1, OCOGroup: "straddle"}); err != nil {
			t.Fatal(err)
		}
		if err := job.OpenOrder(&strat.EnterReq{Tag: "brk_buy", Stop: 105, Amount: 1, OCOGroup: "straddle"}); err != nil {
			t.Fatal(err)
		}
		ents, _, err := mgr.ProcessOrders(job)
		if err != nil || len(ents) != 2 {
			t.Fatalf("ProcessOrders got %d err=%v", len(ents), err)
		}
		sellOd, buyOd := ents[0], ents[1]

		// Falling bar (Open=100 > Close=94): OHLC path visits High=108 BEFORE Low=92.
		feedTSCapsBar(t, mgr, exs, baseMS+60_000, 100, 108, 92, 94)
		if buyOd.Status != ormo.InOutStatusFullEnter {
			t.Fatalf("expected Buy Stop (first along falling-bar path) to fill, got status=%d", buyOd.Status)
		}
		if sellOd.ExitTag != ExitTagOCOCancel || sellOd.Enter.Filled != 0 {
			t.Fatalf("expected Sell Stop OCO sibling canceled unfilled, got status=%d exitTag=%q filled=%v",
				sellOd.Status, sellOd.ExitTag, sellOd.Enter.Filled)
		}
	})
}

func TestCallbackPctBacktestAndLiveLocalFallback(t *testing.T) {
	const symbol = "ETH/USDT:USDT"
	const baseMS = int64(1_700_000_040_000)

	// 1) Backtest at 0.05% and 25% trailing callback.
	for _, pct := range []float64{0.05, 25.0} {
		mgr, exs, job := setupTSCapsBacktest(t, symbol, 100)
		if err := job.OpenOrder(&strat.EnterReq{Tag: "trail_bt", Amount: 1, CallbackPct: pct}); err != nil {
			t.Fatalf("pct=%v OpenOrder err=%v", pct, err)
		}
		ents, _, err := mgr.ProcessOrders(job)
		if err != nil || len(ents) != 1 {
			t.Fatal(err)
		}
		// Enter at 100.
		feedTSCapsBar(t, mgr, exs, baseMS+60_000, 100, 100, 100, 100)
		// Peak rises to 200 -> trailing stop at 200 * (1 - pct/100).
		feedTSCapsBar(t, mgr, exs, baseMS+120_000, 100, 200, 100, 199.95)
		wantSL := 200 * (1 - pct/100)
		sl := ents[0].GetStopLoss()
		if sl == nil || math.Abs(sl.Price-wantSL) > 1e-6 {
			t.Fatalf("pct=%v trailing SL = %+v, want %v", pct, sl, wantSL)
		}
		// Retrace below wantSL -> triggers trailing_stop exit.
		feedTSCapsBar(t, mgr, exs, baseMS+180_000, 199.95, 199.95, wantSL-1, wantSL-0.5)
		if ents[0].Status != ormo.InOutStatusFullExit || ents[0].ExitTag != core.ExitTagTrailingStop {
			t.Fatalf("pct=%v expected trailing_stop exit, got status=%d tag=%q", pct, ents[0].Status, ents[0].ExitTag)
		}
	}

	// 2) LiveOrderMgr fallback to local price-monitored trailing stop when exchange rejects 25% callbackRate.
	oldBotName := config.Data.Name
	config.Data.Name = "ts-caps"
	t.Cleanup(func() { config.Data.Name = oldBotName })
	var exitSubmitted int
	exchange := &issue138Exchange{
		BanExchange: newTSCapsMockExchange(symbol),
		createOrder: func(sym, odType, side string, amount, price float64, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			if odType == banexg.OdTypeTrailingStopMarket {
				return nil, errs.NewMsg(errs.CodeParamInvalid, "callbackRate exceeds exchange max 5%%")
			}
			if odType == banexg.OdTypeMarket && side == banexg.OdSideSell {
				exitSubmitted++
				return &banexg.Order{
					ID: "live-trail-exit", Symbol: sym, Type: odType, Side: side,
					Status: banexg.OdStatusFilled, Amount: amount, Filled: amount, Average: 145, Timestamp: int64(btime.Time()*1000) + 1000,
				}, nil
			}
			return &banexg.Order{ID: "unexpected", Symbol: sym, Status: banexg.OdStatusOpen}, nil
		},
	}
	withIssue138Exchange(t, exchange)
	mgr := newLiveOrderMgr("live-trail-fallback", func(*ormo.InOutOrder, bool) {})
	od := issue138Order("filled-entry")
	od.ID = 901
	od.Symbol = symbol
	od.Timeframe = "1m"
	od.Status = ormo.InOutStatusFullEnter
	od.InitPrice = 100
	od.Enter.Symbol = symbol
	od.Enter.Amount = 1
	od.Enter.Filled = 1
	od.Enter.Average = 100
	od.Enter.Status = ormo.OdStatusClosed
	od.SetInfo(ormo.OdInfoCallbackPct, 25.0)
	openOds, lock := mgr.openOrders()
	lock.Lock()
	openOds[od.ID] = od
	lock.Unlock()
	t.Cleanup(func() {
		lock.Lock()
		delete(openOds, od.ID)
		lock.Unlock()
	})

	mgr.setTrailingStop(od)
	if od.GetInfoString(ormo.OdInfoTrailingID) != "" || od.GetInfoInt64(ormo.OdInfoLocalTrailing) != 1 {
		t.Fatalf("expected fallback to local trailing without failing order, got TrailingID=%q LocalTrailing=%d",
			od.GetInfoString(ormo.OdInfoTrailingID), od.GetInfoInt64(ormo.OdInfoLocalTrailing))
	}
	// Price advances to 200 (SL becomes 150), then drops to 145 <= 150 -> triggers local market exit!
	if err := mgr.CheckLocalTriggersByPrice(symbol, 200); err != nil {
		t.Fatal(err)
	}
	if exitSubmitted != 0 {
		t.Fatal("should not exit at peak 200")
	}
	if err := mgr.CheckLocalTriggersByPrice(symbol, 145); err != nil {
		t.Fatal(err)
	}
	if exitSubmitted != 1 || od.Status != ormo.InOutStatusFullExit || od.ExitTag != core.ExitTagTrailingStop {
		t.Fatalf("expected live local fallback trailing exit, exitSubmitted=%d status=%d tag=%q",
			exitSubmitted, od.Status, od.ExitTag)
	}
}

func TestLongShortTPPriceAndExgStopLossFalseLocalEnforcement(t *testing.T) {
	const symbol = "ETH/USDT:USDT"
	const baseMS = int64(1_700_000_040_000)

	// 1) Backtest: ExgStopLoss=false STILL triggers local stop-loss, while StopLoss==0 does NOT trigger.
	mgr, exs, job := setupTSCapsBacktest(t, symbol, 100)
	job.ExgStopLoss = false
	job.LongTPPrice = 120
	if err := job.OpenOrder(&strat.EnterReq{Tag: "local_sl_bt", Amount: 1, StopLoss: 95}); err != nil {
		t.Fatal(err)
	}
	job.LongTPPrice = 0
	if err := job.OpenOrder(&strat.EnterReq{Tag: "no_sl_bt", Amount: 1}); err != nil {
		t.Fatal(err)
	}
	ents, _, err := mgr.ProcessOrders(job)
	if err != nil || len(ents) != 2 {
		t.Fatal(err)
	}
	withSL, withoutSL := ents[0], ents[1]
	if tp := withSL.GetTakeProfit(); tp == nil || math.Abs(tp.Price-120) > 1e-9 {
		t.Fatalf("LongTPPrice not applied to order: %+v", tp)
	}
	feedTSCapsBar(t, mgr, exs, baseMS+60_000, 100, 100, 100, 100)
	// Dip to 94 breaches withSL's StopLoss=95, while withoutSL stays open!
	feedTSCapsBar(t, mgr, exs, baseMS+120_000, 100, 101, 94, 96)
	if withSL.Status != ormo.InOutStatusFullExit || (withSL.ExitTag != core.ExitTagStopLoss && withSL.ExitTag != core.ExitTagSLTake) || withSL.Profit >= 0 {
		t.Fatalf("ExgStopLoss=false order should locally stop out in backtest, got status=%d tag=%q profit=%v", withSL.Status, withSL.ExitTag, withSL.Profit)
	}
	if withoutSL.Status != ormo.InOutStatusFullEnter {
		t.Fatalf("StopLoss==0 order should remain open, got status=%d tag=%q", withoutSL.Status, withoutSL.ExitTag)
	}

	// 2) LiveOrderMgr: ExgStopLoss=false skips pre-posting remote stop_loss order, yet exits locally on price breach.
	oldBotName := config.Data.Name
	config.Data.Name = "ts-caps"
	t.Cleanup(func() { config.Data.Name = oldBotName })
	remoteStopOrders := 0
	marketExits := 0
	exchange := &issue138Exchange{
		BanExchange: newTSCapsMockExchange(symbol),
		createOrder: func(sym, odType, side string, amount, price float64, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			if strings.Contains(odType, "stop") {
				remoteStopOrders++
			}
			if odType == banexg.OdTypeMarket && side == banexg.OdSideSell {
				marketExits++
				return &banexg.Order{
					ID: "live-local-sl-exit", Symbol: sym, Type: odType, Side: side,
					Status: banexg.OdStatusFilled, Amount: amount, Filled: amount, Average: 94, Timestamp: int64(btime.Time()*1000) + 1000,
				}, nil
			}
			return &banexg.Order{ID: "other", Symbol: sym, Status: banexg.OdStatusOpen}, nil
		},
	}
	withIssue138Exchange(t, exchange)
	liveMgr := newLiveOrderMgr("live-local-sl", func(*ormo.InOutOrder, bool) {})
	od := issue138Order("live-entry-1")
	od.ID = 902
	od.Symbol = symbol
	od.Timeframe = "1m"
	od.Status = ormo.InOutStatusFullEnter
	od.InitPrice = 100
	od.Enter.Symbol = symbol
	od.Enter.Amount = 1
	od.Enter.Filled = 1
	od.Enter.Average = 100
	od.Enter.Status = ormo.OdStatusClosed
	od.SetInfo(ormo.OdInfoLocalStopLoss, "1")
	od.SetInfo(ormo.OdInfoStopLoss, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: 95}})
	openOds, lock := liveMgr.openOrders()
	lock.Lock()
	openOds[od.ID] = od
	lock.Unlock()
	t.Cleanup(func() {
		lock.Lock()
		delete(openOds, od.ID)
		lock.Unlock()
	})

	// Must NOT post remote stop-loss order when OdInfoLocalStopLoss == "1".
	liveMgr.editTriggerOd(od, ormo.OdActionStopLoss)
	liveMgr.healMissingFullEnterTriggers([]*ormo.InOutOrder{od}, nil)
	if remoteStopOrders != 0 {
		t.Fatalf("expected 0 remote stop-loss orders when ExgStopLoss=false, got %d", remoteStopOrders)
	}
	// Bar breach triggers local market stop-loss exit.
	evt := orm.NewDataSeriesFromKline(&orm.ExSymbol{ID: 1, Symbol: symbol}, "1m", &banexg.Kline{
		Time: baseMS, Open: 99, High: 100, Low: 94, Close: 94.5, Volume: 100,
	}, nil, true, true)
	if err := liveMgr.UpdateByDataSeries([]*ormo.InOutOrder{od}, evt); err != nil {
		t.Fatal(err)
	}
	if remoteStopOrders != 0 || marketExits != 1 || od.Status != ormo.InOutStatusFullExit || od.ExitTag != core.ExitTagStopLoss {
		t.Fatalf("live ExgStopLoss=false mismatch: remoteStops=%d marketExits=%d status=%d tag=%q",
			remoteStopOrders, marketExits, od.Status, od.ExitTag)
	}
}

func TestSharedOrderMgrStopLimitAndOCOGroup(t *testing.T) {
	const symbol = "ETH/USDT:USDT"
	key := execution.AccountKey{VenueSessionIdentity: "shared-caps", Account: "default", SettlementDomain: "USDT"}
	instrument := execution.Instrument{
		ID: "ETH-PERP", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USDT",
		QuantityStep: decimal.RequireFromString("0.1"), ContractSize: decimal.NewFromInt(1),
		PriceTick: decimal.RequireFromString("0.1"), MoneyScale: 4,
	}
	paper, err := execution.NewPaperAdapter(decimal.NewFromInt(10_000), decimal.Zero, decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	curBid, curAsk, curNow, curBar := decimal.NewFromInt(100), decimal.NewFromInt(100), int64(1000), int64(1)
	var registry execution.AccountRegistry
	t.Cleanup(registry.Close)
	owner, err := registry.Acquire(key)
	if err != nil {
		t.Fatal(err)
	}
	sharedAcc, err := execution.NewSharedAccount(owner, execution.SharedExecutionOptions{
		Memory:                true,
		Adapter:               paper,
		AuthoritativeSnapshot: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sharedAcc.Close() })
	borrow := sharedAcc.Borrow()
	t.Cleanup(borrow.Release)

	risk := execution.PortfolioRisk{
		MarginRate:          decimal.RequireFromString("0.1"),
		MaxAccountMargin:    decimal.NewFromInt(5000),
		MaxVirtualGross:     decimal.NewFromInt(10000),
		StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{"ts": decimal.NewFromInt(5000)},
	}
	if err := borrow.CashEvent(execution.CashEvent{ID: "deposit", Kind: execution.ExternalCashChange, AccountDelta: decimal.NewFromInt(10_000), Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(10_000)}}, AtMS: curNow}); err != nil {
		t.Fatal(err)
	}
	if err := borrow.CashEvent(execution.CashEvent{ID: "allocate", Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(-10_000)}, {Strategy: "ts", Amount: decimal.NewFromInt(10_000)}}, AtMS: curNow}); err != nil {
		t.Fatal(err)
	}
	if err := borrow.Reconcile("startup", curNow); err != nil {
		t.Fatal(err)
	}
	if err := borrow.RegisterRiskPolicy("shared-risk-v1", risk); err != nil {
		t.Fatal(err)
	}
	if err := borrow.RegisterAccountQuotes([]execution.Instrument{instrument}, func(_ context.Context, _ string, now int64) (execution.VisibleQuote, error) {
		return execution.VisibleQuote{Bid: curBid, Ask: curAsk, AtMS: now, ReceivedMS: now, ValidUntilMS: now + 60_000, Bar: curBar}, nil
	}, func() int64 { return curNow }); err != nil {
		t.Fatal(err)
	}

	coreState, _ := core.NewState(context.Background())
	t.Cleanup(coreState.Close)
	coreState.SetRunMode(core.RunModeBackTest)
	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(curNow)
	exs := &orm.ExSymbol{ID: 1, Exchange: "binance", Market: banexg.MarketLinear, Symbol: symbol}
	symbols := orm.NewSymbolState()
	_ = symbols.SetExSymbols([]*orm.ExSymbol{exs})
	cfg := config.NewSnapshotWithDirs(&config.Config{Accounts: map[string]*config.AccountConfig{"default": {}}}, t.TempDir(), "")
	deps := RuntimeDeps{
		Core: coreState, Clock: clock, Config: cfg, Symbols: symbols,
		Orders: ormo.NewOrderState(), Trading: NewTradingState(), Strategies: strat.NewState(),
		DefaultAccount: "default", SharedExecution: borrow,
	}
	deps.Orders.SetTask("default", &ormo.BotTask{ID: 1})
	bridgeCfg := &SharedOrderBridgeConfig{
		Version:     "v1",
		OrderPolicy: execution.OrderPolicy{Mode: execution.LocalFirst},
		Instruments: map[string]execution.Instrument{symbol: instrument},
		Strategies:  map[string]SharedStrategyBinding{"s1": {ID: "ts", StakeNAVFraction: decimal.RequireFromString("0.1"), MaxNotional: decimal.NewFromInt(5000)}},
		Risk:        risk,
		Quote: func(_ string, now int64) (execution.VisibleQuote, error) {
			return execution.VisibleQuote{Bid: curBid, Ask: curAsk, AtMS: now, ReceivedMS: now, ValidUntilMS: now + 60_000, Bar: curBar}, nil
		},
		IntentTTLMS: 60_000,
	}
	mgr, err := NewSharedOrderMgr(deps, borrow, bridgeCfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Place OCO straddle: Buy Stop-Limit (Stop=105, Limit=106) + Sell Stop (Stop=95).
	odBuy, bErr := mgr.EnterOrder(exs, "1m", &strat.EnterReq{Tag: "oco_buy_sl", StratName: "s1", Stop: 105, Limit: 106, Amount: 1, OCOGroup: "g1"})
	if bErr != nil {
		t.Fatal(bErr)
	}
	odSell, sErr := mgr.EnterOrder(exs, "1m", &strat.EnterReq{Tag: "oco_sell", StratName: "s1", Short: true, Stop: 95, Amount: 1, OCOGroup: "g1"})
	if sErr != nil {
		t.Fatal(sErr)
	}
	if odBuy.Status != ormo.InOutStatusInit || odSell.Status != ormo.InOutStatusInit {
		t.Fatalf("both OCO orders should start pending: buy=%d sell=%d", odBuy.Status, odSell.Status)
	}

	// Quote rises to 105.5 -> Buy Stop-Limit triggers and fills; Sell Stop sibling is canceled!
	curNow, curBar = 2000, 2
	clock.SetTimeMS(curNow)
	curBid, curAsk = decimal.RequireFromString("105.5"), decimal.RequireFromString("105.5")
	evt := orm.NewDataSeriesFromKline(exs, "1m", &banexg.Kline{Time: 1000, Open: 100, High: 106, Low: 100, Close: 105.5, Volume: 100}, nil, true, false)
	if uErr := mgr.UpdateByDataSeries(nil, evt); uErr != nil {
		t.Fatal(uErr)
	}
	// Trigger second pass so CoordinatePending submits & settles the triggered Buy Stop-Limit and cancels OCO sibling.
	curNow, curBar = 3000, 3
	clock.SetTimeMS(curNow)
	evt2 := orm.NewDataSeriesFromKline(exs, "1m", &banexg.Kline{Time: 2000, Open: 105.5, High: 106, Low: 105, Close: 105.5, Volume: 100}, nil, true, false)
	if uErr := mgr.UpdateByDataSeries(nil, evt2); uErr != nil {
		t.Fatal(uErr)
	}

	// Verify Sell Stop cannot fill even if quote later drops to 90!
	curNow, curBar = 4000, 4
	clock.SetTimeMS(curNow)
	curBid, curAsk = decimal.NewFromInt(90), decimal.NewFromInt(90)
	evt3 := orm.NewDataSeriesFromKline(exs, "1m", &banexg.Kline{Time: 3000, Open: 105, High: 105, Low: 90, Close: 90, Volume: 100}, nil, true, false)
	if uErr := mgr.UpdateByDataSeries(nil, evt3); uErr != nil {
		t.Fatal(uErr)
	}

	openMap, lock := deps.Orders.GetOpenODs("default")
	lock.Lock()
	buyView := openMap[odBuy.ID]
	sellView := openMap[odSell.ID]
	lock.Unlock()
	if buyView == nil || buyView.Status != ormo.InOutStatusFullEnter {
		t.Fatalf("expected Buy Stop-Limit filled in SharedOrderMgr, got %+v", buyView)
	}
	if sellView != nil && sellView.Enter.Filled > 0 {
		t.Fatalf("expected Sell Stop OCO sibling canceled without fill, got %+v", sellView.Enter)
	}
}
