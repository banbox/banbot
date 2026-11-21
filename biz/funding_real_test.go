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
	"github.com/banbox/banexg/errs"
)

type stubBizFundingRepo struct {
	rows map[int32][]*orm.DataRecord
}

func (r *stubBizFundingRepo) EnsureSeriesTable(context.Context, *orm.SeriesInfo) *errs.Error {
	return nil
}
func (r *stubBizFundingRepo) InsertSeriesBatch(context.Context, *orm.SeriesInfo, []*orm.DataRecord) *errs.Error {
	return nil
}
func (r *stubBizFundingRepo) DeleteSeriesRange(context.Context, *orm.SeriesInfo, int32, int64, int64) *errs.Error {
	return nil
}
func (r *stubBizFundingRepo) UpdateSeriesRange(context.Context, *orm.SeriesInfo, int32, int64, int64) *errs.Error {
	return nil
}
func (r *stubBizFundingRepo) UpdateSeriesCoverage(context.Context, *orm.SeriesInfo, int32, int64, int64, []*orm.DataRecord) *errs.Error {
	return nil
}
func (r *stubBizFundingRepo) GetSeriesRange(context.Context, *orm.SeriesInfo, int32) (int64, int64, *errs.Error) {
	return 0, 0, nil
}
func (r *stubBizFundingRepo) QuerySeriesRange(_ context.Context, _ *orm.SeriesInfo, sid int32, startMS, endMS int64, _ int) ([]*orm.DataRecord, *errs.Error) {
	var out []*orm.DataRecord
	for _, row := range r.rows[sid] {
		if row != nil && row.TimeMS >= startMS && row.TimeMS < endMS {
			out = append(out, row)
		}
	}
	return out, nil
}

func TestTSBacktestRealFundingSettlementBilateralAndFallback(t *testing.T) {
	const symbol = "BTC/USDT:USDT"
	const fallbackSymbol = "ETH/USDT:USDT"
	const baseMS = int64(1704067200000)
	const stepMS = int64(8 * 3600 * 1000)

	coreState, _ := core.NewState(context.Background())
	t.Cleanup(coreState.Close)
	coreState.SetRunMode(core.RunModeBackTest)
	coreState.Market = banexg.MarketLinear
	coreState.IsContract = true
	coreState.ExgName = "binance"

	symbols := orm.NewSymbolStateWithAllocatorAndIdentity(orm.NewSIDAllocator(), "binance", banexg.MarketLinear)
	btc := &orm.ExSymbol{ID: 101, Symbol: symbol, Exchange: "binance", Market: banexg.MarketLinear}
	eth := &orm.ExSymbol{ID: 102, Symbol: fallbackSymbol, Exchange: "binance", Market: banexg.MarketLinear}
	if err := symbols.CacheExSymbolChecked(btc); err != nil {
		t.Fatal(err)
	}
	if err := symbols.CacheExSymbolChecked(eth); err != nil {
		t.Fatal(err)
	}

	repo := &stubBizFundingRepo{
		rows: map[int32][]*orm.DataRecord{
			btc.ID: {
				// Settlement at baseMS + 8h: +0.1%
				{Sid: btc.ID, TimeMS: baseMS, EndMS: baseMS + stepMS, Closed: true, Values: map[string]any{"rate": 0.001}},
				// Settlement at baseMS + 16h: -0.25%
				{Sid: btc.ID, TimeMS: baseMS + stepMS, EndMS: baseMS + 2*stepMS, Closed: true, Values: map[string]any{"rate": -0.0025}},
			},
		},
	}

	clock := btime.NewClockState(true, nil)
	mgr := &OrderMgr{
		runtimeDeps: true,
		runtimeCore: coreState,
		symbols:     symbols,
		clock:       clock,
		prices:      com.NewPriceState("binance"),
		fundingRepo: repo,
		runtimeCfg:  runtimeOrderConfig{fundingApr: 36.5},
	}

	makeOrder := func(sym string, short bool, enterMS, exitMS int64) *ormo.InOutOrder {
		// Notional = 100 * 10 = 1000 USDT; base exit fee = 1.0 USDT
		return &ormo.InOutOrder{
			IOrder: &ormo.IOrder{
				Symbol:  sym,
				Short:   short,
				EnterAt: enterMS,
				ExitAt:  exitMS,
				Status:  ormo.InOutStatusFullExit,
			},
			Enter: &ormo.ExOrder{
				Symbol:   sym,
				Enter:    true,
				Average:  100,
				Filled:   10,
				Fee:      0.5,
				FeeQuote: 0.5,
				UpdateAt: enterMS,
				Status:   ormo.OdStatusClosed,
			},
			Exit: &ormo.ExOrder{
				Symbol:   sym,
				Enter:    false,
				Average:  100,
				Filled:   10,
				Fee:      1.0,
				FeeQuote: 1.0,
				UpdateAt: exitMS,
				Status:   ormo.OdStatusClosed,
			},
			Info: map[string]interface{}{},
		}
	}

	// 1. Long crossing +0.001 settlement at baseMS+8h (entered AFTER baseMS, exited AT baseMS+8h): pays +1.0 USDT
	longPos := makeOrder(symbol, false, baseMS+1000, baseMS+stepMS)
	mgr.settleBacktestFunding(longPos, 36.5, true, false, baseMS+stepMS)
	longPos.UpdateProfits(100)
	if got := longPos.GetInfoFloat64("funding"); math.Abs(got-1.0) > 1e-9 {
		t.Fatalf("long +rate funding = %v, want +1.0", got)
	}
	if math.Abs(longPos.Exit.FeeQuote-2.0) > 1e-9 || math.Abs(longPos.Profit-(-2.5)) > 1e-9 {
		t.Fatalf("long +rate Exit.FeeQuote=%v Profit=%v, want 2.0 and -2.5", longPos.Exit.FeeQuote, longPos.Profit)
	}

	// 2. Short crossing +0.001 settlement at baseMS+8h: RECEIVES 1.0 USDT (cost = -1.0)
	shortPos := makeOrder(symbol, true, baseMS+1000, baseMS+stepMS)
	mgr.settleBacktestFunding(shortPos, 36.5, true, false, baseMS+stepMS)
	shortPos.UpdateProfits(100)
	if got := shortPos.GetInfoFloat64("funding"); math.Abs(got-(-1.0)) > 1e-9 {
		t.Fatalf("short +rate funding = %v, want -1.0", got)
	}
	if math.Abs(shortPos.Exit.FeeQuote-0.0) > 1e-9 || math.Abs(shortPos.Profit-(-0.5)) > 1e-9 {
		t.Fatalf("short +rate Exit.FeeQuote=%v Profit=%v, want 0.0 and -0.5", shortPos.Exit.FeeQuote, shortPos.Profit)
	}

	// 3. Long crossing -0.0025 settlement at baseMS+16h (entered AT baseMS+8h so +8h is NOT double-counted): RECEIVES 2.5 USDT (cost = -2.5)
	longNeg := makeOrder(symbol, false, baseMS+stepMS, baseMS+2*stepMS)
	mgr.settleBacktestFunding(longNeg, 36.5, true, false, baseMS+2*stepMS)
	longNeg.UpdateProfits(100)
	if got := longNeg.GetInfoFloat64("funding"); math.Abs(got-(-2.5)) > 1e-9 {
		t.Fatalf("long -rate funding = %v, want -2.5", got)
	}
	if math.Abs(longNeg.Exit.FeeQuote-(-1.5)) > 1e-9 || math.Abs(longNeg.Profit-1.0) > 1e-9 {
		t.Fatalf("long -rate Exit.FeeQuote=%v Profit=%v, want -1.5 and +1.0", longNeg.Exit.FeeQuote, longNeg.Profit)
	}

	// 4. Short crossing -0.0025 settlement at baseMS+16h: PAYS +2.5 USDT (cost = +2.5)
	shortNeg := makeOrder(symbol, true, baseMS+stepMS, baseMS+2*stepMS)
	mgr.settleBacktestFunding(shortNeg, 36.5, true, false, baseMS+2*stepMS)
	shortNeg.UpdateProfits(100)
	if got := shortNeg.GetInfoFloat64("funding"); math.Abs(got-2.5) > 1e-9 {
		t.Fatalf("short -rate funding = %v, want +2.5", got)
	}
	if math.Abs(shortNeg.Exit.FeeQuote-3.5) > 1e-9 || math.Abs(shortNeg.Profit-(-4.0)) > 1e-9 {
		t.Fatalf("short -rate Exit.FeeQuote=%v Profit=%v, want 3.5 and -4.0", shortNeg.Exit.FeeQuote, shortNeg.Profit)
	}

	// 5. Fallback: ETH has no stored funding_rate rows -> falls back to addApproxFunding (10 days @ 36.5% APR on 1000 = +10.0)
	const dayMS = int64(86400 * 1000)
	ethFallback := makeOrder(fallbackSymbol, false, baseMS, baseMS+10*dayMS)
	mgr.settleBacktestFunding(ethFallback, 36.5, true, false, baseMS+10*dayMS)
	if got := ethFallback.GetInfoFloat64("funding"); math.Abs(got-10.0) > 1e-9 {
		t.Fatalf("fallback funding = %v, want +10.0", got)
	}
	if ethFallback.GetInfoString("funding_mode") != "" {
		t.Fatalf("fallback order must not set real funding_mode, got %q", ethFallback.GetInfoString("funding_mode"))
	}
}

var (
	registerFundingSubStratOnce sync.Once
	fundingSubObservedMu        sync.Mutex
	fundingSubObservedRates     []float64
)

func TestTSDataSubFundingRateSignalAndStreamedRecording(t *testing.T) {
	const symbol = "BTC/USDT:USDT"
	const baseMS = int64(1704067200000)
	const stepMS = int64(8 * 3600 * 1000)

	registerFundingSubStratOnce.Do(func() {
		strat.AddStratGroup("test_funding_sub", map[string]strat.FuncMakeStrat{
			"v1": func(_ *config.RunPolicyConfig) *strat.TradeStrat {
				return &strat.TradeStrat{
					WarmupNum: 1,
					OnDataSubs: func(j *strat.StratJob) []*strat.DataSub {
						return []*strat.DataSub{{
							Source:    orm.SeriesSourceFundingRate,
							ExSymbol:  j.Symbol,
							TimeFrame: "8h",
						}}
					},
					OnData: func(j *strat.StratJob, evt strat.DataEvent) {
						if !j.IsWarmUp {
							if _, ok := evt.RawValue("rate"); ok {
								fundingSubObservedMu.Lock()
								fundingSubObservedRates = append(fundingSubObservedRates, evt.Float64("rate"))
								fundingSubObservedMu.Unlock()
							}
						}
					},
				}
			},
		})
	})
	fundingSubObservedMu.Lock()
	fundingSubObservedRates = nil
	fundingSubObservedMu.Unlock()

	spec, err := config.LoadRunSpec(&config.CmdArgs{
		NoDefault:  true,
		DataDir:    t.TempDir(),
		ConfigData: "config_version: 2\ntime_start: '20240101'\nexchange: {name: binance}\nmarket_type: linear\npairs: ['BTC/USDT:USDT']\nstake_currency: [USDT]\nrun_policy:\n  - name: test_funding_sub:v1\n    run_timeframes: [1h]\n",
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := spec.RuntimeSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	coreState, _ := core.NewState(context.Background())
	t.Cleanup(coreState.Close)
	coreState.SetRunMode(core.RunModeBackTest)
	coreState.Market = banexg.MarketLinear
	coreState.IsContract = true
	coreState.ExgName = "binance"
	coreState.SetPairs([]string{symbol}, nil)

	symbols := orm.NewSymbolStateWithAllocatorAndIdentity(orm.NewSIDAllocator(), "binance", banexg.MarketLinear)
	btc := &orm.ExSymbol{ID: 201, Symbol: symbol, Exchange: "binance", Market: banexg.MarketLinear}
	if err := symbols.CacheExSymbolChecked(btc); err != nil {
		t.Fatal(err)
	}

	strategies := strat.NewState()
	orders := ormo.NewOrderState()
	trading := NewTradingState()
	clock := btime.NewClockState(true, nil)
	mockExg := &struct{ *banexg.Exchange }{Exchange: &banexg.Exchange{
		ExgInfo: &banexg.ExgInfo{
			ID:         "binance",
			MarketType: banexg.MarketLinear,
			Markets: banexg.MarketMap{
				symbol: {ID: "BTCUSDT", Symbol: symbol, Type: banexg.MarketLinear, Linear: true, Swap: true, Contract: true},
			},
		},
	}}
	market := com.NewMarketStateWithExchange("binance", mockExg)
	accountsMu := sync.RWMutex{}
	deps := RuntimeDeps{
		Core:           coreState,
		Clock:          clock,
		Market:         market,
		Batch:          strat.NewBatchState(),
		Strategies:     strategies,
		Orders:         orders,
		Trading:        trading,
		Config:         snap,
		Accounts:       map[string]*config.AccountConfig{"default": {}},
		AccountsMu:     &accountsMu,
		Symbols:        symbols,
		Exchange:       mockExg,
		DefaultAccount: "default",
	}
	if err := BindRuntimeDeps(deps); err != nil {
		t.Fatal(err)
	}
	mgr := &LocalOrderMgr{OrderMgr: OrderMgr{Account: "default"}}
	mgr.bindRuntimeDeps(deps)
	trading.SetOrderManager("default", mgr)

	tfScores := map[string]map[string]float64{symbol: {"1h": 1.0}}
	if _, _, err := strat.LoadStratJobsWithState(strategies, coreState, symbols, []string{symbol}, tfScores, orders); err != nil {
		t.Fatal(err)
	}
	trader, err := NewTraderWithRuntimeDeps(deps)
	if err != nil {
		t.Fatal(err)
	}

	// Feed warmup event (should NOT record in fundingSubObservedRates or OrderMgr streamed funding).
	if err := trader.FeedDataSeries(&orm.DataSeries{
		Source: orm.SeriesSourceFundingRate, Sid: btc.ID, ExSymbol: btc, TimeFrame: "8h",
		TimeMS: baseMS - stepMS, EndMS: baseMS, Closed: true, IsWarmUp: true,
		Values: map[string]any{"rate": 0.0009},
	}); err != nil {
		t.Fatal(err)
	}
	// Feed live backtest event at baseMS + 8h.
	if err := trader.FeedDataSeries(&orm.DataSeries{
		Source: orm.SeriesSourceFundingRate, Sid: btc.ID, ExSymbol: btc, TimeFrame: "8h",
		TimeMS: baseMS, EndMS: baseMS + stepMS, Closed: true, IsWarmUp: false,
		Values: map[string]any{"rate": -0.0004},
	}); err != nil {
		t.Fatal(err)
	}

	fundingSubObservedMu.Lock()
	gotRates := append([]float64(nil), fundingSubObservedRates...)
	fundingSubObservedMu.Unlock()
	if len(gotRates) != 1 || math.Abs(gotRates[0]+0.0004) > 1e-12 {
		t.Fatalf("expected strategy OnData to observe [-0.0004], got %v", gotRates)
	}
	pts, hasReal := mgr.symbolFundingPoints(symbol)
	if !hasReal || len(pts) != 1 || pts[0].atMS != baseMS+stepMS || math.Abs(pts[0].rate+0.0004) > 1e-12 {
		t.Fatalf("expected OrderMgr to record streamed funding point, got %+v (hasReal=%v)", pts, hasReal)
	}
}
