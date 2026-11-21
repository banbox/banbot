package biz

import (
	"fmt"
	"math"
	"strings"
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
	"github.com/banbox/banexg/errs"
)

type tsGridFundingMockExchange struct {
	banexg.BanExchange
	markets map[string]*banexg.Market
	marketT string
}

func newTSGridFundingMockExchange(marketType string, symbols ...string) *tsGridFundingMockExchange {
	markets := make(map[string]*banexg.Market, len(symbols))
	for _, sym := range symbols {
		base, quote, settle, _ := core.SplitSymbol(sym)
		if settle == "" {
			settle = quote
		}
		markets[sym] = &banexg.Market{
			ID: sym, Symbol: sym, Base: base, Quote: quote, Settle: settle,
			Type: marketType, Contract: marketType != banexg.MarketSpot,
			Linear: marketType == banexg.MarketLinear, Inverse: marketType == banexg.MarketInverse,
			Spot: marketType == banexg.MarketSpot, Swap: marketType != banexg.MarketSpot,
			Active: true, ContractSize: 1,
			Precision: &banexg.Precision{
				Amount: 0.0001, Price: 0.0001,
				ModeAmount: banexg.PrecModeTickSize, ModePrice: banexg.PrecModeTickSize,
			},
			Limits: &banexg.MarketLimits{
				Amount: &banexg.LimitRange{Min: 0.0001, Max: 1_000_000},
				Cost:   &banexg.LimitRange{Min: 1, Max: 10_000_000},
			},
		}
	}
	baseEx := &banexg.Exchange{
		ExgInfo: &banexg.ExgInfo{
			ID:         "binance",
			MarketType: marketType,
			Markets:    markets,
		},
	}
	return &tsGridFundingMockExchange{BanExchange: baseEx, markets: markets, marketT: marketType}
}

func (e *tsGridFundingMockExchange) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: "binance", MarketType: e.marketT, Markets: e.markets}
}

func (e *tsGridFundingMockExchange) GetMarket(symbol string) (*banexg.Market, *errs.Error) {
	if m := e.markets[symbol]; m != nil {
		return m, nil
	}
	return nil, errs.NewMsg(core.ErrInvalidSymbol, "unknown symbol %s", symbol)
}

func (e *tsGridFundingMockExchange) PrecPrice(_ *banexg.Market, p float64) (float64, *errs.Error) {
	return math.Round(p*10000) / 10000, nil
}

func (e *tsGridFundingMockExchange) PrecAmount(_ *banexg.Market, a float64) (float64, *errs.Error) {
	return math.Round(a*10000) / 10000, nil
}

func (e *tsGridFundingMockExchange) PrecCost(_ *banexg.Market, c float64) (float64, *errs.Error) {
	return math.Round(c*10000) / 10000, nil
}

func (e *tsGridFundingMockExchange) CalcMaintMargin(_ string, cost float64) (float64, *errs.Error) {
	return cost * 0.005, nil
}

func (e *tsGridFundingMockExchange) GetLeverage(string, float64, string) (float64, float64) {
	return 5, 20
}

func (e *tsGridFundingMockExchange) PriceSymbolParts(symbol string) ([4]string, *errs.Error) {
	base, quote, settle, id := core.SplitSymbol(symbol)
	if settle == "" {
		settle = quote
	}
	return [4]string{base, quote, settle, id}, nil
}

func (e *tsGridFundingMockExchange) CalculateFee(string, string, string, float64, float64, bool, map[string]interface{}) (*banexg.Fee, *errs.Error) {
	return &banexg.Fee{Currency: "USDT", Cost: 0, QuoteCost: 0}, nil
}

func feedTestSeriesBar(t *testing.T, trader *Trader, exs *orm.ExSymbol, tf string, barMS, tfMS int64, o, h, l, c float64, warmup bool) {
	t.Helper()
	endMS := barMS + tfMS
	trader.SetTimeMS(endMS)
	evt := orm.NewDataSeriesFromKline(exs, tf, &banexg.Kline{
		Time: barMS, Open: o, High: h, Low: l, Close: c, Volume: 10_000, Quote: c * 10_000,
	}, nil, true, true)
	evt.EndMS = endMS
	evt.IsWarmUp = warmup
	if err := trader.FeedDataSeries(evt); err != nil {
		t.Fatalf("FeedDataSeries(%s @ %d) failed: %v", tf, barMS, err)
	}
}

// TestCoarse1hBarWith5mRefineTFKlineGridAndReplenishment verifies:
//  1. Strategy-level TradeStrat.RefineTF ("5m") under a "1h" main timeframe takes effect automatically
//     when YAML RunPolicyConfig.RefineTF is unset;
//  2. Multiple simultaneous buy-limit & sell-limit grid rungs match chronologically sub-bar by sub-bar
//     (and in chronological intrabar order when two rungs fill inside the same 5m sub-bar);
//  3. OnOrderChange observes refreshed StratJob order snapshots (GetOrders/GetOrderNum) and immediately
//     queues counter limit exits and replenished grid entries via ProcessOrders;
//  4. Strict causality (zero lookahead): a counter-limit order queued on sub-bar #2 when price drops to 98
//     NEVER matches an earlier peak (High=100.4) from the pre-fill portion of sub-bar #2, and instead fills
//     cleanly on later 5m sub-bars (#5 / #11) within the same 1h coarse bar, with zero double-matching when
//     the 1h bar itself closes at +60m.
func TestCoarse1hBarWith5mRefineTFKlineGridAndReplenishment(t *testing.T) {
	const (
		symbol   = "ETH/USDT:USDT"
		stratKey = "test_ts06:kline_grid_5m"
		hourMS   = int64(3600_000)
		fiveMS   = int64(300_000)
		baseHour = int64(1_704_067_200_000) // 2024-01-01 00:00:00 UTC
	)

	type fillEvent struct {
		kind     int
		tag      string
		exitTag  string
		short    bool
		price    float64
		atMS     int64
		createMS int64
	}
	var (
		mu         sync.Mutex
		events     []fillEvent
		reBuyCount int
	)

	strat.RegisterStrategy(stratKey, func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			WarmupNum:     2,
			RunTimeFrames: []string{"1h"},
			RefineTF:      "5m", // strategy-level opt-in default (YAML refine_tf is nil)
			EachMaxLong:   0,
			EachMaxShort:  0,
			OnData: strat.RouteData(strat.DataHandlers{
				Main: func(s *strat.StratJob, _ strat.DataEvent) {
					if s.IsWarmUp || s.Env.TimeStop != baseHour+hourMS {
						return
					}
					// Submit outer rung (98) BEFORE inner rung (99) to verify intrabar chronological fill ordering (99 must fill before 98).
					if err := s.OpenOrder(&strat.EnterReq{Tag: "grid_buy_98", Limit: 98.0, Amount: 1}); err != nil {
						t.Errorf("open grid_buy_98: %v", err)
					}
					if err := s.OpenOrder(&strat.EnterReq{Tag: "grid_buy_99", Limit: 99.0, Amount: 1}); err != nil {
						t.Errorf("open grid_buy_99: %v", err)
					}
					if err := s.OpenOrder(&strat.EnterReq{Tag: "grid_sell_101", Short: true, Limit: 101.0, Amount: 1}); err != nil {
						t.Errorf("open grid_sell_101: %v", err)
					}
					if err := s.OpenOrder(&strat.EnterReq{Tag: "grid_sell_102", Short: true, Limit: 102.0, Amount: 1}); err != nil {
						t.Errorf("open grid_sell_102: %v", err)
					}
				},
			}),
			OnOrderChange: func(s *strat.StratJob, od *ormo.InOutOrder, chgType int) {
				if chgType == strat.OdChgEnterFill {
					mu.Lock()
					events = append(events, fillEvent{
						kind: chgType, tag: od.EnterTag, short: od.Short,
						price: od.Enter.Average, atMS: od.Enter.UpdateAt, createMS: od.Enter.CreateAt,
					})
					mu.Unlock()

					// StratJob snapshot must already include this filled order so CloseOrders does not drop the exit!
					dir := 1.0
					if od.Short {
						dir = -1.0
					}
					if s.GetOrderNum(dir) == 0 {
						t.Errorf("OnOrderChange(OdChgEnterFill) for %s saw GetOrderNum(%v)=0; expected refreshed snapshot", od.EnterTag, dir)
					}

					switch od.EnterTag {
					case "grid_buy_99":
						if err := s.CloseOrders(&strat.ExitReq{
							Tag: "tp_from_99", OrderID: od.ID, Dirt: core.OdDirtLong, Limit: 100.5,
						}); err != nil {
							t.Errorf("queue tp_from_99: %v", err)
						}
					case "grid_buy_98":
						// Also place an opposite sell-limit order at 100.0 to test strict intrabar causality on sub-bar #2
						// (sub-bar #2 had High=100.4 BEFORE Low=97.5; this counter-order MUST NOT fill on sub-bar #2!).
						if err := s.CloseOrders(&strat.ExitReq{
							Tag: "tp_from_98", OrderID: od.ID, Dirt: core.OdDirtLong, Limit: 100.0,
						}); err != nil {
							t.Errorf("queue tp_from_98: %v", err)
						}
						if err := s.OpenOrder(&strat.EnterReq{
							Tag: "counter_short_100", Short: true, Limit: 100.0, Amount: 1,
						}); err != nil {
							t.Errorf("queue counter_short_100: %v", err)
						}
					case "counter_short_100":
						if err := s.CloseOrders(&strat.ExitReq{
							Tag: "tp_counter_short_98_5", OrderID: od.ID, Dirt: core.OdDirtShort, Limit: 98.5,
						}); err != nil {
							t.Errorf("queue tp_counter_short_98_5: %v", err)
						}
					case "grid_sell_101":
						if err := s.CloseOrders(&strat.ExitReq{
							Tag: "tp_from_101", OrderID: od.ID, Dirt: core.OdDirtShort, Limit: 99.0,
						}); err != nil {
							t.Errorf("queue tp_from_101: %v", err)
						}
					case "grid_re_buy_98":
						if err := s.CloseOrders(&strat.ExitReq{
							Tag: "tp_from_re_buy_98", OrderID: od.ID, Dirt: core.OdDirtLong, Limit: 101.0,
						}); err != nil {
							t.Errorf("queue tp_from_re_buy_98: %v", err)
						}
					}
				} else if chgType == strat.OdChgExitFill {
					mu.Lock()
					events = append(events, fillEvent{
						kind: chgType, tag: od.EnterTag, exitTag: od.ExitTag, short: od.Short,
						price: od.Exit.Average, atMS: od.Exit.UpdateAt, createMS: od.Exit.CreateAt,
					})
					shouldReplenish := od.ExitTag == "tp_from_98" && reBuyCount == 0
					if shouldReplenish {
						reBuyCount++
					}
					mu.Unlock()
					if shouldReplenish {
						// Replenish buy-98 rung after tp_from_98 fills during the mid-hour rally!
						if err := s.OpenOrder(&strat.EnterReq{
							Tag: "grid_re_buy_98", Limit: 98.0, Amount: 1,
						}); err != nil {
							t.Errorf("replenish grid_re_buy_98: %v", err)
						}
					}
				}
			},
		}
	})
	t.Cleanup(func() { strat.UnregisterStrategy(stratKey) })

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
	coreState.SetPairs([]string{symbol}, nil)

	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(baseHour - 2*hourMS)

	exs := &orm.ExSymbol{ID: 1, Exchange: "binance", Market: banexg.MarketLinear, Symbol: symbol}
	symbols := orm.NewSymbolStateWithAllocatorAndIdentity(orm.NewSIDAllocator(), "binance", banexg.MarketLinear)
	if setErr := symbols.CacheExSymbolChecked(exs); setErr != nil {
		t.Fatal(setErr)
	}

	cfgData := &config.Config{
		Name:          "ts-grid-refine",
		MarketType:    banexg.MarketLinear,
		StakeAmount:   100,
		Leverage:      5,
		OpenVolRate:   1,
		BTNetCost:     5, // 5s simulated latency
		MaxOpenOrders: 20,
		MaxSimulOpen:  20,
		BTFundingApr:  0, // isolate pure grid PnL in this test
		WalletAmounts: map[string]float64{"USDT": 10_000},
		Accounts:      map[string]*config.AccountConfig{"default": {Leverage: 5, MaxOpenOrders: 20}},
		RunTimeframes: []string{"1h"},
		Pairs:         []string{symbol},
		RunPolicy: []*config.RunPolicyConfig{
			{Name: stratKey}, // RefineTF intentionally omitted in YAML
		},
	}
	cfg := config.NewSnapshotWithDirs(cfgData, t.TempDir(), "")
	exgMock := newTSGridFundingMockExchange(banexg.MarketLinear, symbol)
	market := com.NewMarketStateWithExchange("binance", exgMock)
	deps := RuntimeDeps{
		Core:           coreState,
		Clock:          clock,
		Market:         market,
		Batch:          strat.NewBatchState(),
		Strategies:     strat.NewState(),
		Orders:         ormo.NewOrderState(),
		Trading:        NewTradingState(),
		Config:         cfg,
		Accounts:       map[string]*config.AccountConfig{"default": {Leverage: 5, MaxOpenOrders: 20}},
		AccountsMu:     &sync.RWMutex{},
		Symbols:        symbols,
		Exchange:       exgMock,
		DefaultAccount: "default",
	}
	if bindErr := BindRuntimeDeps(deps); bindErr != nil {
		t.Fatal(bindErr)
	}
	deps.Orders.SetTask("default", &ormo.BotTask{ID: 1})
	wallets := InitFakeWalletsWithRuntimeDeps(deps)
	InitLocalOrderMgrWithRuntimeDeps(deps, func(*ormo.InOutOrder, bool) {}, false)

	tfScores := map[string]map[string]float64{symbol: {"1h": 1.0}}
	warms, _, loadErr := strat.LoadStratJobsWithState(deps.Strategies, coreState, symbols, []string{symbol}, tfScores, deps.Orders)
	if loadErr != nil {
		t.Fatalf("LoadStratJobsWithState: %v", loadErr)
	}
	if _, has5m := warms[symbol]["5m"]; !has5m {
		t.Fatalf("expected TradeStrat.RefineTF='5m' to register 5m sub-bar subscription in warms, got %+v", warms)
	}
	if !coreState.OrderMatchTfsSnapshot()["5m"] {
		t.Fatalf("expected 5m enabled in OrderMatchTfsSnapshot, got %+v", coreState.OrderMatchTfsSnapshot())
	}

	InitOdSubsWithRuntimeDeps(&deps)
	trader, trErr := NewTraderWithRuntimeDeps(deps)
	if trErr != nil {
		t.Fatal(trErr)
	}

	// Warmup two 1h bars, then close the signal 1h bar [baseHour, baseHour+1h) at 100.0 -> places 4 grid orders.
	feedTestSeriesBar(t, &trader, exs, "1h", baseHour-2*hourMS, hourMS, 100, 101, 99, 100, true)
	feedTestSeriesBar(t, &trader, exs, "1h", baseHour-hourMS, hourMS, 100, 101, 99, 100, true)
	feedTestSeriesBar(t, &trader, exs, "1h", baseHour, hourMS, 100, 100.5, 99.5, 100, false)

	job := deps.Strategies.LookupJob("default", symbol+"_1h", stratKey)
	if job == nil {
		t.Fatal("expected StratJob registered")
	}
	if got := job.GetOrderNum(0); got != 4 {
		t.Fatalf("expected 4 resting grid orders in StratJob snapshot after 1h bar, got %d", got)
	}

	// Twelve 5m sub-bars inside [baseHour+1h, baseHour+2h):
	// Oscillating path: 100 -> 97.5 -> 101.5 -> 98.0 -> 102.0
	hour1Start := baseHour + hourMS
	subBars := [12][4]float64{
		// #1 [00..05m): flat 100.0 -> 99.6 (no fills yet)
		{100.0, 100.2, 99.6, 99.8},
		// #2 [05..10m): bearish candle Open=99.8, High=100.4 (EARLY peak!), Low=97.5, Close=98.1
		//   -> grid_buy_99 fills BEFORE grid_buy_98; OnOrderChange queues tp_from_98@100.0 & counter_short_100@100.0.
		//   -> MUST NOT fill @100.0 against the pre-fill High=100.4 on sub-bar #2!
		{99.8, 100.4, 97.5, 98.1},
		// #3 [10..15m): base consolidation 98.1 -> 99.2 (below 100.0)
		{98.1, 99.2, 97.9, 99.0},
		// #4 [15..20m): bounce 99.0 -> 99.9 (still below 100.0)
		{99.0, 99.9, 98.8, 99.8},
		// #5 [20..25m): rally 99.8 -> 100.8 -> fills tp_from_98@100.0, counter_short_100@100.0, tp_from_99@100.5!
		//   -> tp_from_98 exit also replenishes grid_re_buy_98@98.0!
		{99.8, 100.8, 99.7, 100.6},
		// #6 [25..30m): rally continues 100.6 -> 101.5 -> fills grid_sell_101@101.0 (queues tp_from_101@99.0)!
		{100.6, 101.5, 100.4, 101.2},
		// #7 [30..35m): pullback begins 101.2 -> 99.5
		{101.2, 101.3, 99.5, 99.6},
		// #8 [35..40m): drops 99.6 -> 98.0 -> fills tp_from_101@99.0, tp_counter_short_98_5@98.5, and replenished grid_re_buy_98@98.0!
		{99.6, 99.7, 98.0, 98.3},
		// #9 [40..45m): recovery 98.3 -> 99.8
		{98.3, 99.8, 98.2, 99.6},
		// #10 [45..50m): rally 99.6 -> 100.7
		{99.6, 100.7, 99.5, 100.5},
		// #11 [50..55m): surge 100.5 -> 101.4 -> fills tp_from_re_buy_98@101.0!
		{100.5, 101.4, 100.4, 101.2},
		// #12 [55..60m): breakout to 102.0 -> fills outer grid_sell_102@102.0!
		{101.2, 102.2, 101.1, 102.0},
	}

	for idx, sb := range subBars {
		sbStart := hour1Start + int64(idx)*fiveMS
		feedTestSeriesBar(t, &trader, exs, "5m", sbStart, fiveMS, sb[0], sb[1], sb[2], sb[3], false)
		if idx == 1 {
			// Immediately after sub-bar #2 [05..10m): verify grid_buy_99 filled BEFORE grid_buy_98,
			// AND verify neither tp_from_98 nor counter_short_100 filled on sub-bar #2's pre-fill High=100.4!
			mu.Lock()
			snapEvents := append([]fillEvent(nil), events...)
			mu.Unlock()
			if len(snapEvents) != 2 {
				t.Fatalf("after sub-bar #2 expected exactly 2 fills (zero lookahead on counter 100.0), got %d: %+v", len(snapEvents), snapEvents)
			}
			if snapEvents[0].tag != "grid_buy_99" || snapEvents[1].tag != "grid_buy_98" {
				t.Fatalf("expected chronological intrabar order [grid_buy_99, grid_buy_98], got [%s, %s]", snapEvents[0].tag, snapEvents[1].tag)
			}
			if snapEvents[0].atMS > snapEvents[1].atMS {
				t.Fatalf("expected grid_buy_99 fill time (%d) <= grid_buy_98 fill time (%d)", snapEvents[0].atMS, snapEvents[1].atMS)
			}
		}
	}

	eventsBeforeCoarse := len(events)
	// Deliver the coarse 1h bar [hour1Start, hour1Start+1h) spanning the exact 12 sub-bars -> must NOT double-match!
	feedTestSeriesBar(t, &trader, exs, "1h", hour1Start, hourMS, 100.0, 102.2, 97.5, 102.0, false)
	if len(events) != eventsBeforeCoarse {
		t.Fatalf("coarse 1h bar double-matched orders already handled by 5m refine_tf: before=%d after=%d", eventsBeforeCoarse, len(events))
	}

	// Verify exact chronological sub-bar progression and strict causality across all 12 events!
	wantSeq := []string{
		"enter:grid_buy_99@99.0",
		"enter:grid_buy_98@98.0",
		"enter:counter_short_100@100.0",
		"exit:grid_buy_98/tp_from_98@100.0",
		"exit:grid_buy_99/tp_from_99@100.5",
		"enter:grid_sell_101@101.0",
		"exit:grid_sell_101/tp_from_101@99.0",
		"exit:counter_short_100/tp_counter_short_98_5@98.5",
		"enter:grid_re_buy_98@98.0",
		"exit:grid_re_buy_98/tp_from_re_buy_98@101.0",
		"enter:grid_sell_102@102.0",
	}
	if len(events) != len(wantSeq) {
		var gotDesc []string
		for _, ev := range events {
			gotDesc = append(gotDesc, fmt.Sprintf("%d:%s/%s@%.1f(t=%d)", ev.kind, ev.tag, ev.exitTag, ev.price, ev.atMS))
		}
		t.Fatalf("fill sequence count = %d, want %d:\ngot:  %v\nwant: %v", len(events), len(wantSeq), gotDesc, wantSeq)
	}

	entryFillTime := make(map[string]int64)
	var prevMS int64
	for i, ev := range events {
		var label string
		if ev.kind == strat.OdChgEnterFill {
			label = fmt.Sprintf("enter:%s@%.1f", ev.tag, ev.price)
			entryFillTime[ev.tag] = ev.atMS
			if ev.atMS < ev.createMS {
				t.Fatalf("event %d (%s) filled at %d earlier than its CreateAt %d!", i, label, ev.atMS, ev.createMS)
			}
		} else {
			label = fmt.Sprintf("exit:%s/%s@%.1f", ev.tag, ev.exitTag, ev.price)
			entMS, ok := entryFillTime[ev.tag]
			if !ok || ev.createMS < entMS || ev.atMS <= entMS {
				t.Fatalf("causality violation on %s: entFill=%d exitCreate=%d exitFill=%d", label, entMS, ev.createMS, ev.atMS)
			}
		}
		if label != wantSeq[i] {
			t.Fatalf("event[%d] = %q, want %q", i, label, wantSeq[i])
		}
		if ev.atMS < prevMS {
			t.Fatalf("non-monotonic sub-bar fill timestamps at event[%d] (%s): %d < %d", i, label, ev.atMS, prevMS)
		}
		prevMS = ev.atMS
	}

	// Verify counter_short_100 and grid_re_buy_98 created strictly >= the fill that spawned them.
	for _, ev := range events {
		if ev.kind == strat.OdChgEnterFill && ev.tag == "counter_short_100" && ev.createMS < entryFillTime["grid_buy_98"] {
			t.Fatalf("counter_short_100 created at %d before grid_buy_98 filled at %d", ev.createMS, entryFillTime["grid_buy_98"])
		}
	}

	// Close remaining open grid_sell_102 at 102.0 and reconcile realized grid profit:
	// Closed round-trips:
	//   grid_buy_99 -> 100.5: +1.5
	//   grid_buy_98 -> 100.0: +2.0
	//   counter_short_100 -> 98.5: +1.5
	//   grid_sell_101 -> 99.0: +2.0
	//   grid_re_buy_98 -> 101.0: +3.0
	//   grid_sell_102 -> 102.0 (CleanUp at 102.0): 0.0
	// Total net profit = +10.0 USDT -> wallet balance = 10,010.0 USDT.
	mgr := deps.Trading.OrderManager("default").(*LocalOrderMgr)
	if cleanErr := mgr.CleanUp(); cleanErr != nil {
		t.Fatalf("CleanUp: %v", cleanErr)
	}
	gotBal := wallets.Get("USDT").Available
	if math.Abs(gotBal-10_010.0) > 1e-6 {
		t.Fatalf("final USDT wallet balance = %.8f, want 10010.00000000", gotBal)
	}
}

// TestTradeStratRefineTFGracefulDegradationAndYAMLOverride verifies:
//  1. A strategy declaring RefineTF: "5m" still matches orders cleanly in a synthetic coarse-only
//     unit test where only "1h" bars are fed (graceful degradation when sub-TF bars are absent);
//  2. Explicit YAML RunPolicyConfig.RefineTF overrides TradeStrat.RefineTF.
func TestTradeStratRefineTFGracefulDegradationAndYAMLOverride(t *testing.T) {
	const (
		symbol   = "BTC/USDT:USDT"
		stratKey = "test_ts06:refine_fallback"
		hourMS   = int64(3600_000)
		baseMS   = int64(1_704_067_200_000)
	)
	strat.RegisterStrategy(stratKey, func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			WarmupNum:     1,
			RunTimeFrames: []string{"1h"},
			RefineTF:      "15m",
			OnData: strat.RouteData(strat.DataHandlers{
				Main: func(s *strat.StratJob, _ strat.DataEvent) {
					if !s.IsWarmUp && s.GetOrderNum(0) == 0 && len(s.Entrys) == 0 {
						_ = s.OpenOrder(&strat.EnterReq{Tag: "coarse_lim", Limit: 98.0, TakeProfit: 102.0, Amount: 1})
					}
				},
			}),
		}
	})
	t.Cleanup(func() { strat.UnregisterStrategy(stratKey) })

	t.Run("YAML refine_tf overrides TradeStrat.RefineTF", func(t *testing.T) {
		coreState, _ := core.NewState(nil)
		t.Cleanup(coreState.Close)
		cfg := config.NewSnapshotWithDirs(&config.Config{
			RunTimeframes: []string{"1h"},
			Pairs:         []string{symbol},
			Accounts:      map[string]*config.AccountConfig{"default": {}},
			RunPolicy:     []*config.RunPolicyConfig{{Name: stratKey, RefineTF: "5m"}},
		}, t.TempDir(), "")
		symbols := orm.NewSymbolStateWithAllocatorAndIdentity(orm.NewSIDAllocator(), "binance", banexg.MarketLinear)
		if cErr := symbols.CacheExSymbolChecked(&orm.ExSymbol{ID: 1, Exchange: "binance", Market: banexg.MarketLinear, Symbol: symbol}); cErr != nil {
			t.Fatal(cErr)
		}
		exgMock := newTSGridFundingMockExchange(banexg.MarketLinear, symbol)
		stState := strat.NewStateWithRuntime(coreState, btime.NewClockState(true, nil), cfg.View(), symbols, exgMock)
		stState.BindRuntimeAccounts(map[string]*config.AccountConfig{"default": {}})
		warms, _, err := strat.LoadStratJobsWithState(stState, coreState, symbols, []string{symbol}, map[string]map[string]float64{symbol: {"1h": 1}}, ormo.NewOrderState())
		if err != nil {
			t.Fatal(err)
		}
		if got := stState.RefineTimeFrame(stratKey, "1h"); got != "5m" {
			t.Fatalf("RefineTimeFrame = %q, want 5m from YAML override", got)
		}
		if _, ok := warms[symbol]["5m"]; !ok {
			t.Fatalf("expected 5m in warms, got %+v", warms)
		}
	})

	t.Run("coarse-only synthetic test gracefully degrades to main timeframe", func(t *testing.T) {
		coreState, _ := core.NewState(nil)
		t.Cleanup(coreState.Close)
		coreState.SetRunMode(core.RunModeBackTest)
		coreState.BackTestMode = true
		coreState.Market = banexg.MarketLinear
		coreState.IsContract = true
		coreState.ExgName = "binance"
		coreState.SetPairs([]string{symbol}, nil)

		clock := btime.NewClockState(true, nil)
		clock.SetTimeMS(baseMS)
		exs := &orm.ExSymbol{ID: 1, Exchange: "binance", Market: banexg.MarketLinear, Symbol: symbol}
		symbols := orm.NewSymbolStateWithAllocatorAndIdentity(orm.NewSIDAllocator(), "binance", banexg.MarketLinear)
		if cErr := symbols.CacheExSymbolChecked(exs); cErr != nil {
			t.Fatal(cErr)
		}
		cfg := config.NewSnapshotWithDirs(&config.Config{
			Name: "fallback-bt", MarketType: banexg.MarketLinear, StakeAmount: 100, Leverage: 5,
			WalletAmounts: map[string]float64{"USDT": 10_000},
			Accounts:      map[string]*config.AccountConfig{"default": {Leverage: 5}},
			RunTimeframes: []string{"1h"}, Pairs: []string{symbol},
			RunPolicy: []*config.RunPolicyConfig{{Name: stratKey}},
		}, t.TempDir(), "")
		exgMock := newTSGridFundingMockExchange(banexg.MarketLinear, symbol)
		deps := RuntimeDeps{
			Core: coreState, Clock: clock, Market: com.NewMarketStateWithExchange("binance", exgMock),
			Batch: strat.NewBatchState(), Strategies: strat.NewState(), Orders: ormo.NewOrderState(),
			Trading: NewTradingState(), Config: cfg, Accounts: map[string]*config.AccountConfig{"default": {Leverage: 5}},
			AccountsMu: &sync.RWMutex{}, Symbols: symbols, Exchange: exgMock, DefaultAccount: "default",
		}
		if err := BindRuntimeDeps(deps); err != nil {
			t.Fatal(err)
		}
		deps.Orders.SetTask("default", &ormo.BotTask{ID: 1})
		InitFakeWalletsWithRuntimeDeps(deps)
		InitLocalOrderMgrWithRuntimeDeps(deps, func(*ormo.InOutOrder, bool) {}, false)
		if _, _, err := strat.LoadStratJobsWithState(deps.Strategies, coreState, symbols, []string{symbol}, map[string]map[string]float64{symbol: {"1h": 1}}, deps.Orders); err != nil {
			t.Fatal(err)
		}
		trader, err := NewTraderWithRuntimeDeps(deps)
		if err != nil {
			t.Fatal(err)
		}

		// Feed ONLY 1h bars (zero 15m sub-bars): warmup, signal bar, then matching bar dipping to 97 and rallying to 103.
		feedTestSeriesBar(t, &trader, exs, "1h", baseMS, hourMS, 100, 101, 99, 100, true)
		feedTestSeriesBar(t, &trader, exs, "1h", baseMS+hourMS, hourMS, 100, 101, 99, 100, false)
		feedTestSeriesBar(t, &trader, exs, "1h", baseMS+2*hourMS, hourMS, 100, 103, 97, 102.5, false)

		hist := deps.Orders.HistoricalOrders()
		if len(hist) != 1 || hist[0].ExitTag != core.ExitTagTakeProfit || math.Abs(hist[0].Profit-4.0) > 1e-6 {
			t.Fatalf("expected coarse-only fallback to enter@98 and TP@102 (+4.0), got hist=%+v", hist)
		}
	})
}

// TestBilateralSymmetricApproxFundingAndDeltaNeutralHedge verifies:
//  1. BtFeatureLevel >= 15 enables bilateral symmetric approximate funding by default;
//  2. Under positive baseline BTFundingApr, linear contract longs pay (+cost) and shorts receive (-cost);
//  3. An equal-notional 1:1 long + short hedge held over the same interval has exact zero net funding drift
//     on both order profits and wallet settlement;
//  4. Inverse contracts skip approximate funding and tag funding_skipped=inverse;
//  5. BTLegacyApproxFunding=true preserves legacy unilateral long-only charging for regression comparisons.
func TestBilateralSymmetricApproxFundingAndDeltaNeutralHedge(t *testing.T) {
	if core.BtFeatureLevel < 15 {
		t.Fatalf("expected BtFeatureLevel >= 15, got %d", core.BtFeatureLevel)
	}

	const (
		symbol = "BTC/USDT:USDT"
		dayMS  = int64(86_400_000)
		baseMS = int64(1_704_067_200_000)
	)

	runHedgeScenario := func(t *testing.T, aprPct float64, legacyUnilateral bool) (longOd, shortOd *ormo.InOutOrder, finalWallet float64) {
		t.Helper()
		coreState, err := core.NewState(nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(coreState.Close)
		coreState.SetRunMode(core.RunModeBackTest)
		coreState.BackTestMode = true
		coreState.Market = banexg.MarketLinear
		coreState.IsContract = true
		coreState.ExgName = "binance"

		clock := btime.NewClockState(true, nil)
		clock.SetTimeMS(baseMS)
		exs := &orm.ExSymbol{ID: 1, Exchange: "binance", Market: banexg.MarketLinear, Symbol: symbol}
		symbols := orm.NewSymbolStateWithAllocatorAndIdentity(orm.NewSIDAllocator(), "binance", banexg.MarketLinear)
		if cErr := symbols.CacheExSymbolChecked(exs); cErr != nil {
			t.Fatal(cErr)
		}

		cfg := config.NewSnapshotWithDirs(&config.Config{
			Name:                  "ts-funding-sym",
			MarketType:            banexg.MarketLinear,
			StakeAmount:           1000,
			Leverage:              5,
			BTNetCost:             0,
			BTFundingApr:          aprPct,
			BTLegacyApproxFunding: legacyUnilateral,
			WalletAmounts:         map[string]float64{"USDT": 10_000},
			Accounts:              map[string]*config.AccountConfig{"default": {Leverage: 5}},
		}, t.TempDir(), "")
		exgMock := newTSGridFundingMockExchange(banexg.MarketLinear, symbol)
		deps := RuntimeDeps{
			Core: coreState, Clock: clock, Market: com.NewMarketStateWithExchange("binance", exgMock),
			Batch: strat.NewBatchState(), Strategies: strat.NewState(), Orders: ormo.NewOrderState(),
			Trading: NewTradingState(), Config: cfg, Accounts: map[string]*config.AccountConfig{"default": {Leverage: 5}},
			AccountsMu: &sync.RWMutex{}, Symbols: symbols, Exchange: exgMock, DefaultAccount: "default",
		}
		if bindErr := BindRuntimeDeps(deps); bindErr != nil {
			t.Fatal(bindErr)
		}
		deps.Orders.SetTask("default", &ormo.BotTask{ID: 1})
		wallets := InitFakeWalletsWithRuntimeDeps(deps)
		InitLocalOrderMgrWithRuntimeDeps(deps, func(*ormo.InOutOrder, bool) {}, false)
		deps.Market.Prices.SetBarPriceAt(baseMS, symbol, 100.0)
		mgr := deps.Trading.OrderManager("default").(*LocalOrderMgr)

		// Open equal-notional 1000 USDT (10 contracts @ 100) Long + Short at baseMS (makeLocalAfterEnter bumps +1ms; pin EnterAt=baseMS for exact 10d comparison).
		var enterErr *errs.Error
		longOd, enterErr = mgr.EnterOrder(exs, "1h", &strat.EnterReq{Tag: "hedge_long", StratName: "hedge", Limit: 100.0, Amount: 10})
		if enterErr != nil {
			t.Fatal(enterErr)
		}
		shortOd, enterErr = mgr.EnterOrder(exs, "1h", &strat.EnterReq{Tag: "hedge_short", StratName: "hedge", Short: true, Limit: 100.0, Amount: 10})
		if enterErr != nil {
			t.Fatal(enterErr)
		}

		// Fill both entries at baseMS @ 100.0
		clock.SetTimeMS(baseMS + 3600_000)
		deps.Market.Prices.SetBarPriceAt(baseMS+3600_000, symbol, 100.0)
		fillEvt := orm.NewDataSeriesFromKline(exs, "1h", &banexg.Kline{
			Time: baseMS, Open: 100, High: 100, Low: 100, Close: 100, Volume: 1000,
		}, nil, true, true)
		if err := mgr.UpdateByDataSeries([]*ormo.InOutOrder{longOd, shortOd}, fillEvt); err != nil {
			t.Fatal(err)
		}
		// Align both EnterAt stamps to baseMS so both legs span the exact 10-day holding interval.
		longOd.EnterAt = baseMS
		shortOd.EnterAt = baseMS

		// Exit both legs 10 days later at flat price 100.0.
		exitMS := baseMS + 10*dayMS
		clock.SetTimeMS(exitMS)
		deps.Market.Prices.SetBarPriceAt(exitMS, symbol, 100.0)
		if err := mgr.ExitAndFill([]*ormo.InOutOrder{longOd, shortOd}, &strat.ExitReq{Tag: "hedge_close", Dirt: core.OdDirtBoth, Force: true}); err != nil {
			t.Fatal(err)
		}
		return longOd, shortOd, wallets.Get("USDT").Available
	}

	t.Run("positive baseline rate: long pays, short receives, 1:1 hedge has zero net drift", func(t *testing.T) {
		// 1000 USDT * 36.5%/yr * (10 / 365) yr = 10.0 USDT
		longOd, shortOd, finalWallet := runHedgeScenario(t, 36.5, false)
		if math.Abs(longOd.GetInfoFloat64("funding")-10.0) > 1e-9 || math.Abs(longOd.Profit-(-10.0)) > 1e-9 {
			t.Fatalf("long funding=%v profit=%v, want +10.0 and -10.0", longOd.GetInfoFloat64("funding"), longOd.Profit)
		}
		if math.Abs(shortOd.GetInfoFloat64("funding")-(-10.0)) > 1e-9 || math.Abs(shortOd.Profit-10.0) > 1e-9 {
			t.Fatalf("short funding=%v profit=%v, want -10.0 and +10.0", shortOd.GetInfoFloat64("funding"), shortOd.Profit)
		}
		netFunding := longOd.GetInfoFloat64("funding") + shortOd.GetInfoFloat64("funding")
		netProfit := longOd.Profit + shortOd.Profit
		if math.Abs(netFunding) > 1e-12 || math.Abs(netProfit) > 1e-12 {
			t.Fatalf("1:1 hedge netFunding=%v netProfit=%v, want 0", netFunding, netProfit)
		}
		if math.Abs(finalWallet-10_000.0) > 1e-9 {
			t.Fatalf("1:1 hedge final wallet = %.8f, want 10000.00000000 (zero net funding drift)", finalWallet)
		}
	})

	t.Run("negative baseline rate: long receives, short pays, 1:1 hedge has zero net drift", func(t *testing.T) {
		longOd, shortOd, finalWallet := runHedgeScenario(t, -36.5, false)
		if math.Abs(longOd.GetInfoFloat64("funding")-(-10.0)) > 1e-9 || math.Abs(longOd.Profit-10.0) > 1e-9 {
			t.Fatalf("long negative-APR funding=%v profit=%v, want -10.0 and +10.0", longOd.GetInfoFloat64("funding"), longOd.Profit)
		}
		if math.Abs(shortOd.GetInfoFloat64("funding")-10.0) > 1e-9 || math.Abs(shortOd.Profit-(-10.0)) > 1e-9 {
			t.Fatalf("short negative-APR funding=%v profit=%v, want +10.0 and -10.0", shortOd.GetInfoFloat64("funding"), shortOd.Profit)
		}
		if math.Abs(finalWallet-10_000.0) > 1e-9 {
			t.Fatalf("negative-APR 1:1 hedge final wallet = %.8f, want 10000.00000000", finalWallet)
		}
	})

	t.Run("legacy unilateral flag preserves pre-level-15 long-only funding", func(t *testing.T) {
		longOd, shortOd, finalWallet := runHedgeScenario(t, 36.5, true)
		if math.Abs(longOd.GetInfoFloat64("funding")-10.0) > 1e-9 || math.Abs(longOd.Profit-(-10.0)) > 1e-9 {
			t.Fatalf("legacy long funding=%v profit=%v, want +10.0 and -10.0", longOd.GetInfoFloat64("funding"), longOd.Profit)
		}
		if shortOd.GetInfoFloat64("funding") != 0 || shortOd.Profit != 0 {
			t.Fatalf("legacy short funding=%v profit=%v, want 0 and 0", shortOd.GetInfoFloat64("funding"), shortOd.Profit)
		}
		if math.Abs(finalWallet-9_990.0) > 1e-9 {
			t.Fatalf("legacy unilateral hedge wallet = %.8f, want 9990.0", finalWallet)
		}
	})

	t.Run("inverse contract skips both long and short and tags funding_skipped", func(t *testing.T) {
		for _, isShort := range []bool{false, true} {
			od := &ormo.InOutOrder{
				IOrder: &ormo.IOrder{Short: isShort, EnterAt: baseMS},
				Enter:  &ormo.ExOrder{Average: 100, Filled: 10},
				Exit:   &ormo.ExOrder{UpdateAt: baseMS + 10*dayMS, Fee: 0.5, FeeQuote: 0.5},
			}
			addApproxFunding(od, 36.5, true, true, exitTimeForTest(baseMS, dayMS))
			if od.Exit.FeeQuote != 0.5 || od.GetInfoFloat64("funding") != 0 || !strings.EqualFold(od.GetInfoString("funding_skipped"), "inverse") {
				t.Fatalf("inverse short=%v must skip and tag inverse: fee=%v info=%q", isShort, od.Exit.FeeQuote, od.GetInfoString("funding_skipped"))
			}
		}
	})
}

func exitTimeForTest(baseMS, dayMS int64) int64 {
	return baseMS + 10*dayMS
}
