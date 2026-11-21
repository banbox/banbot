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
)

type oiSignalCapture struct {
	oi            float64
	oiValue       float64
	topLSPosRatio float64
	oiSeriesPrev  float64
	takerHas      bool
	takerRaw      any
	takerSeries   float64
}

var (
	registerOISubStratOnce sync.Once
	oiSubCaptureMu         sync.Mutex
	oiSubCaptured          []oiSignalCapture
)

func TestTSDataSubOpenInterestSignalAndNullSemantics(t *testing.T) {
	const symbol = "BTC/USDT:USDT"
	const baseMS = int64(1704067200000)
	const hourMS = int64(3600 * 1000)

	registerOISubStratOnce.Do(func() {
		strat.AddStratGroup("test_oi_sub", map[string]strat.FuncMakeStrat{
			"v1": func(_ *config.RunPolicyConfig) *strat.TradeStrat {
				return &strat.TradeStrat{
					WarmupNum: 1,
					OnDataSubs: func(j *strat.StratJob) []*strat.DataSub {
						return []*strat.DataSub{{
							Source:    orm.SeriesSourceOpenInterest,
							ExSymbol:  j.Symbol,
							TimeFrame: "1h",
							WarmupNum: 1,
						}}
					},
					OnData: strat.RouteData(strat.DataHandlers{
						Custom: func(j *strat.StratJob, evt strat.DataEvent) {
							if j.IsWarmUp || evt.IsWarmUp {
								return
							}
							rawTaker, hasTaker := evt.RawValue(orm.FieldTakerLSVolRatio)
							var prevOI float64
							if s := evt.Series(orm.FieldOpenInterest); s != nil {
								prevOI = s.Get(1)
							}
							var takerS float64
							if s := evt.Series(orm.FieldTakerLSVolRatio); s != nil {
								takerS = s.Get(0)
							}
							oiSubCaptureMu.Lock()
							oiSubCaptured = append(oiSubCaptured, oiSignalCapture{
								oi:            evt.Float64(orm.FieldOpenInterest),
								oiValue:       evt.Float64(orm.FieldOpenInterestValue),
								topLSPosRatio: evt.Float64(orm.FieldTopLSPosRatio),
								oiSeriesPrev:  prevOI,
								takerHas:      hasTaker,
								takerRaw:      rawTaker,
								takerSeries:   takerS,
							})
							oiSubCaptureMu.Unlock()
						},
					}),
				}
			},
		})
	})

	oiSubCaptureMu.Lock()
	oiSubCaptured = nil
	oiSubCaptureMu.Unlock()

	spec, err := config.LoadRunSpec(&config.CmdArgs{
		NoDefault:  true,
		DataDir:    t.TempDir(),
		ConfigData: "config_version: 2\ntime_start: '20240101'\nexchange: {name: binance}\nmarket_type: linear\npairs: ['BTC/USDT:USDT']\nstake_currency: [USDT]\nrun_policy:\n  - name: test_oi_sub:v1\n    run_timeframes: [1h]\n",
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
	btc := &orm.ExSymbol{ID: 301, Symbol: symbol, Exchange: "binance", Market: banexg.MarketLinear}
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

	// 1) Warmup OI bar [baseMS-1h, baseMS): populates ta.Series history without triggering live signal capture.
	if err := trader.FeedDataSeries(&orm.DataSeries{
		Source: orm.SeriesSourceOpenInterest, Sid: btc.ID, ExSymbol: btc, TimeFrame: "1h",
		TimeMS: baseMS - hourMS, EndMS: baseMS, Closed: true, IsWarmUp: true,
		Values: map[string]any{
			orm.FieldOpenInterest:      10000.0,
			orm.FieldOpenInterestValue: 420_000_000.0,
			orm.FieldTopLSPosRatio:     1.20,
			orm.FieldTopLSAccRatio:     1.10,
			orm.FieldGlobalLSRatio:     0.95,
			orm.FieldTakerLSVolRatio:   1.02,
		},
	}); err != nil {
		t.Fatal(err)
	}

	// 2) Live OI bar [baseMS, baseMS+1h) with OI surge and explicit NULL on taker_ls_vol_ratio.
	if err := trader.FeedDataSeries(&orm.DataSeries{
		Source: orm.SeriesSourceOpenInterest, Sid: btc.ID, ExSymbol: btc, TimeFrame: "1h",
		TimeMS: baseMS, EndMS: baseMS + hourMS, Closed: true, IsWarmUp: false,
		Values: map[string]any{
			orm.FieldOpenInterest:      12500.0,
			orm.FieldOpenInterestValue: 550_000_000.0,
			orm.FieldTopLSPosRatio:     1.65,
			orm.FieldTopLSAccRatio:     1.30,
			orm.FieldGlobalLSRatio:     0.85,
			orm.FieldTakerLSVolRatio:   nil,
		},
	}); err != nil {
		t.Fatal(err)
	}

	oiSubCaptureMu.Lock()
	got := append([]oiSignalCapture(nil), oiSubCaptured...)
	oiSubCaptureMu.Unlock()
	if len(got) != 1 {
		t.Fatalf("expected 1 non-warmup Custom OI callback, got %d", len(got))
	}
	if math.Abs(got[0].oi-12500.0) > 1e-9 ||
		math.Abs(got[0].oiValue-550_000_000.0) > 1e-6 ||
		math.Abs(got[0].topLSPosRatio-1.65) > 1e-9 ||
		math.Abs(got[0].oiSeriesPrev-10000.0) > 1e-9 {
		t.Fatalf("unexpected OI signal values: %+v", got[0])
	}
	if !got[0].takerHas || got[0].takerRaw != nil || !math.IsNaN(got[0].takerSeries) {
		t.Fatalf("expected explicit NULL taker_ls_vol_ratio preserved as (nil, true) and Series NaN, got %+v", got[0])
	}
}
