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

type priceKlineSignalCapture struct {
	source    string
	open      float64
	high      float64
	low       float64
	close     float64
	prevClose float64
}

var (
	registerPriceKlineStratOnce sync.Once
	priceKlineCaptureMu         sync.Mutex
	priceKlineCaptured          []priceKlineSignalCapture
)

func TestTSDataSubPremiumAndIndexKlineZeroLookahead(t *testing.T) {
	const symbol = "BTC/USDT:USDT"
	const baseMS = int64(1704067200000)
	const hourMS = int64(3600 * 1000)

	registerPriceKlineStratOnce.Do(func() {
		strat.AddStratGroup("test_basis_sub", map[string]strat.FuncMakeStrat{
			"v1": func(_ *config.RunPolicyConfig) *strat.TradeStrat {
				return &strat.TradeStrat{
					WarmupNum: 1,
					OnDataSubs: func(j *strat.StratJob) []*strat.DataSub {
						return []*strat.DataSub{
							{
								Source:    orm.SeriesSourcePremiumKline,
								ExSymbol:  j.Symbol,
								TimeFrame: "1h",
								WarmupNum: 1,
							},
							{
								Source:    orm.SeriesSourceIndexKline,
								ExSymbol:  j.Symbol,
								TimeFrame: "1h",
								WarmupNum: 1,
							},
						}
					},
					OnData: strat.RouteData(strat.DataHandlers{
						Custom: func(j *strat.StratJob, evt strat.DataEvent) {
							if j.IsWarmUp || evt.IsWarmUp {
								return
							}
							var prevC float64
							if s := evt.Series("close"); s != nil {
								prevC = s.Get(1)
							}
							priceKlineCaptureMu.Lock()
							priceKlineCaptured = append(priceKlineCaptured, priceKlineSignalCapture{
								source:    evt.Source,
								open:      evt.Float64("open"),
								high:      evt.Float64("high"),
								low:       evt.Float64("low"),
								close:     evt.Float64("close"),
								prevClose: prevC,
							})
							priceKlineCaptureMu.Unlock()
						},
					}),
				}
			},
		})
	})

	priceKlineCaptureMu.Lock()
	priceKlineCaptured = nil
	priceKlineCaptureMu.Unlock()

	spec, err := config.LoadRunSpec(&config.CmdArgs{
		NoDefault:  true,
		DataDir:    t.TempDir(),
		ConfigData: "config_version: 2\ntime_start: '20240101'\nexchange: {name: binance}\nmarket_type: linear\npairs: ['BTC/USDT:USDT']\nstake_currency: [USDT]\nrun_policy:\n  - name: test_basis_sub:v1\n    run_timeframes: [1h]\n",
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
	btc := &orm.ExSymbol{ID: 401, Symbol: symbol, Exchange: "binance", Market: banexg.MarketLinear}
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

	// 1) Warmup bars [baseMS-1h, baseMS) for premium_kline (negative basis) & index_kline:
	if err := trader.FeedDataSeries(&orm.DataSeries{
		Source: orm.SeriesSourcePremiumKline, Sid: btc.ID, ExSymbol: btc, TimeFrame: "1h",
		TimeMS: baseMS - hourMS, EndMS: baseMS, Closed: true, IsWarmUp: true,
		Values: map[string]any{"open": -0.0010, "high": -0.0005, "low": -0.0018, "close": -0.0015},
	}); err != nil {
		t.Fatal(err)
	}
	if err := trader.FeedDataSeries(&orm.DataSeries{
		Source: orm.SeriesSourceIndexKline, Sid: btc.ID, ExSymbol: btc, TimeFrame: "1h",
		TimeMS: baseMS - hourMS, EndMS: baseMS, Closed: true, IsWarmUp: true,
		Values: map[string]any{"open": 42000.0, "high": 42100.0, "low": 41900.0, "close": 42050.0},
	}); err != nil {
		t.Fatal(err)
	}

	// 2) Live bars [baseMS, baseMS+1h):
	if err := trader.FeedDataSeries(&orm.DataSeries{
		Source: orm.SeriesSourcePremiumKline, Sid: btc.ID, ExSymbol: btc, TimeFrame: "1h",
		TimeMS: baseMS, EndMS: baseMS + hourMS, Closed: true, IsWarmUp: false,
		Values: map[string]any{"open": -0.0015, "high": -0.0002, "low": -0.0027, "close": -0.0008},
	}); err != nil {
		t.Fatal(err)
	}
	if err := trader.FeedDataSeries(&orm.DataSeries{
		Source: orm.SeriesSourceIndexKline, Sid: btc.ID, ExSymbol: btc, TimeFrame: "1h",
		TimeMS: baseMS, EndMS: baseMS + hourMS, Closed: true, IsWarmUp: false,
		Values: map[string]any{"open": 42050.0, "high": 42300.0, "low": 42010.0, "close": 42220.0},
	}); err != nil {
		t.Fatal(err)
	}

	priceKlineCaptureMu.Lock()
	got := append([]priceKlineSignalCapture(nil), priceKlineCaptured...)
	priceKlineCaptureMu.Unlock()
	if len(got) != 2 {
		t.Fatalf("expected 2 non-warmup Custom callbacks (premium_kline + index_kline), got %d", len(got))
	}
	if got[0].source != orm.SeriesSourcePremiumKline ||
		math.Abs(got[0].low - -0.0027) > 1e-9 ||
		math.Abs(got[0].close - -0.0008) > 1e-9 ||
		math.Abs(got[0].prevClose - -0.0015) > 1e-9 {
		t.Fatalf("unexpected premium_kline callback capture: %+v", got[0])
	}
	if got[1].source != orm.SeriesSourceIndexKline ||
		math.Abs(got[1].close-42220.0) > 1e-6 ||
		math.Abs(got[1].prevClose-42050.0) > 1e-6 {
		t.Fatalf("unexpected index_kline callback capture: %+v", got[1])
	}
}
