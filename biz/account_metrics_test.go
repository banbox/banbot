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
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	ta "github.com/banbox/banta"
	"github.com/shopspring/decimal"
)

type riskSizingMockExchange struct {
	banexg.BanExchange
	markets map[string]*banexg.Market
}

func newRiskSizingMockExchange(marketType string, symbols ...string) *riskSizingMockExchange {
	markets := make(map[string]*banexg.Market, len(symbols))
	for _, sym := range symbols {
		base, quote, settle, _ := core.SplitSymbol(sym)
		isContract := banexg.IsContract(marketType)
		markets[sym] = &banexg.Market{
			ID:           sym,
			Symbol:       sym,
			Base:         base,
			Quote:        quote,
			Settle:       settle,
			Type:         marketType,
			Spot:         !isContract,
			Swap:         isContract,
			Linear:       marketType == banexg.MarketLinear,
			Contract:     isContract,
			ContractSize: 1,
			Precision: &banexg.Precision{
				Amount:     0.001,
				Price:      0.01,
				ModeAmount: banexg.PrecModeTickSize,
				ModePrice:  banexg.PrecModeTickSize,
			},
			Limits: &banexg.MarketLimits{
				Amount: &banexg.LimitRange{Min: 0.001, Max: 1_000_000},
				Cost:   &banexg.LimitRange{Min: 5, Max: 10_000_000},
			},
		}
	}
	ex := &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "binance", MarketType: marketType, Markets: markets}}
	return &riskSizingMockExchange{BanExchange: ex, markets: markets}
}

func (e *riskSizingMockExchange) GetMarket(symbol string) (*banexg.Market, *errs.Error) {
	if m, ok := e.markets[symbol]; ok {
		return m, nil
	}
	return nil, errs.NewMsg(errs.CodeNoMarketForPair, "market not found: %s", symbol)
}

func (e *riskSizingMockExchange) PrecAmount(m *banexg.Market, amount float64) (float64, *errs.Error) {
	return math.Floor(amount*1000+1e-9) / 1000, nil
}

func (e *riskSizingMockExchange) PrecPrice(m *banexg.Market, price float64) (float64, *errs.Error) {
	return math.Round(price*100) / 100, nil
}

func (e *riskSizingMockExchange) PrecCost(m *banexg.Market, cost float64) (float64, *errs.Error) {
	return math.Round(cost*100) / 100, nil
}

func (e *riskSizingMockExchange) CalculateFee(symbol, odType, side string, amount, price float64, isMaker bool, params map[string]interface{}) (*banexg.Fee, *errs.Error) {
	_, quote, settle, _ := core.SplitSymbol(symbol)
	ccy := quote
	if settle != "" {
		ccy = settle
	}
	return &banexg.Fee{Currency: ccy, Cost: 0, QuoteCost: 0}, nil
}

func (e *riskSizingMockExchange) CalcMaintMargin(_ string, cost float64) (float64, *errs.Error) {
	return cost * 0.005, nil
}

func makeKlineEvent(exs *orm.ExSymbol, tf string, timeMS int64, o, h, l, c, v float64) *orm.DataSeries {
	return &orm.DataSeries{
		Source:    orm.SeriesSourceKline,
		Sid:       exs.ID,
		ExSymbol:  exs,
		TimeFrame: tf,
		TimeMS:    timeMS,
		EndMS:     timeMS + 3600_000,
		Values: map[string]any{
			"open":       o,
			"high":       h,
			"low":        l,
			"close":      c,
			"volume":     v,
			"quote":      c * v,
			"buy_volume": v * 0.5,
			"trade_num":  int64(100),
		},
	}
}

func TestStratJobAccountMetricsCompoundingAndSameStepMultiSymbol(t *testing.T) {
	const (
		symA = "BTC/USDT:USDT"
		symB = "ETH/USDT:USDT"
		tf   = "1h"
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
	baseMS := int64(1_700_000_000_000 / 3600_000 * 3600_000)
	clock.SetTimeMS(baseMS)

	ex := newRiskSizingMockExchange(banexg.MarketLinear, symA, symB)
	market := com.NewMarketState("binance")
	symbols := orm.NewSymbolStateWithIdentity("binance", banexg.MarketLinear)
	exsA := &orm.ExSymbol{ID: 101, Exchange: "binance", Market: banexg.MarketLinear, Symbol: symA}
	exsB := &orm.ExSymbol{ID: 102, Exchange: "binance", Market: banexg.MarketLinear, Symbol: symB}
	if setErr := symbols.SetExSymbols([]*orm.ExSymbol{exsA, exsB}); setErr != nil {
		t.Fatal(setErr)
	}

	const stratName = "ts02_test:compound_same_step"
	type obs struct {
		eq, ava, dd float64
		cost        float64
	}
	observedB := make(map[int64]obs)
	observedA := make(map[int64]obs)

	strat.RegisterStrategy(stratName, func(pol *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			WarmupNum:    0,
			EachMaxLong:  2,
			EachMaxShort: 2,
			OnData: strat.RouteData(strat.DataHandlers{
				Main: func(s *strat.StratJob, d strat.DataEvent) {
					barIdx := (d.TimeMS - baseMS) / 3600_000
					rec := obs{
						eq:  s.AccountEquity(),
						ava: s.AvailableBalance(),
						dd:  s.AccountDrawdown(),
					}
					if s.Symbol.Symbol == symA {
						if barIdx == 0 {
							// Open BTC long with RiskRate=0.02 (2% of 10000 = $200 risk), SL distance = 10% (100 on 1000) => LegalCost = 2000, TakeProfit = 2000 (+100%, +$2000 profit).
							req := &strat.EnterReq{Tag: "btc_long", RiskRate: 0.02, StopLossVal: 100, TakeProfit: 2000}
							if oErr := s.OpenOrder(req); oErr != nil {
								t.Errorf("BTC OpenOrder bar 0 fail: %v", oErr)
							}
							rec.cost = req.LegalCost
						}
						observedA[barIdx] = rec
					} else if s.Symbol.Symbol == symB {
						if barIdx == 0 {
							// Also open a small ETH long on bar 0 so multi-symbol open PnL coexists with BTC!
							req := &strat.EnterReq{Tag: "eth_hold", LegalCost: 1000, StopLossVal: 20}
							if oErr := s.OpenOrder(req); oErr != nil {
								t.Errorf("ETH OpenOrder bar 0 fail: %v", oErr)
							}
							rec.cost = req.LegalCost
						} else if barIdx == 3 {
							// At bar 3, BTC just hit TakeProfit (+2000 realized into wallet) in the SAME timestep!
							// ETH sizes a new order with RiskRate=0.02, SL distance=10% => LegalCost must reflect compounded equity without double-counting BTC's old cached UnrealizedPOL!
							req := &strat.EnterReq{Tag: "eth_compound", RiskRate: 0.02, StopLossVal: 10}
							if oErr := s.OpenOrder(req); oErr != nil {
								t.Errorf("ETH OpenOrder bar 3 fail: %v", oErr)
							}
							rec.cost = req.LegalCost
						}
						observedB[barIdx] = rec
					}
				},
			}),
		}
	})

	cfg := &config.Config{
		Name:          "ts02-bt",
		MarketType:    banexg.MarketLinear,
		Leverage:      5,
		StakeAmount:   100,
		OpenVolRate:   1,
		WalletAmounts: map[string]float64{"USDT": 10000},
		RunTimeframes: []string{tf},
		Accounts:      map[string]*config.AccountConfig{"default": {Leverage: 5}},
		RunPolicy:     []*config.RunPolicyConfig{{Name: stratName, RunTimeframes: []string{tf}}},
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
		t.Fatalf("BindRuntimeDeps fail: %v", bindErr)
	}
	deps.Orders.SetTask("default", &ormo.BotTask{ID: 1})
	wallets := InitFakeWalletsWithRuntimeDeps(deps)
	InitLocalOrderMgrWithRuntimeDeps(deps, func(*ormo.InOutOrder, bool) {}, false)

	envA, _ := ta.NewBarEnv("binance", banexg.MarketLinear, symA, tf)
	envB, _ := ta.NewBarEnv("binance", banexg.MarketLinear, symB, tf)
	deps.Strategies.SetEnv(symA+"_"+tf, envA)
	deps.Strategies.SetEnv(symB+"_"+tf, envB)

	stgy := deps.Strategies.NewStrategy(cfg.RunPolicy[0])
	jobA := &strat.StratJob{
		Strat: stgy, Env: envA, DataHub: strat.NewDataHub(), Symbol: exsA,
		TimeFrame: tf, Account: "default", TPMaxs: map[int64]float64{},
		CloseLong: true, CloseShort: true, ExgStopLoss: true, ExgTakeProfit: true,
	}
	jobB := &strat.StratJob{
		Strat: stgy, Env: envB, DataHub: strat.NewDataHub(), Symbol: exsB,
		TimeFrame: tf, Account: "default", TPMaxs: map[int64]float64{},
		CloseLong: true, CloseShort: true, ExgStopLoss: true, ExgTakeProfit: true,
	}
	jobA.BindRuntimeState(deps.Strategies, deps.Core, deps.Clock)
	jobB.BindRuntimeState(deps.Strategies, deps.Core, deps.Clock)
	deps.Strategies.SetJobMap("default", symA+"_"+tf, map[string]*strat.StratJob{stgy.Name: jobA})
	deps.Strategies.SetJobMap("default", symB+"_"+tf, map[string]*strat.StratJob{stgy.Name: jobB})
	coreState.SetOrderMatchEnabled(tf, true)

	trader, trErr := NewTraderWithRuntimeDeps(deps)
	if trErr != nil {
		t.Fatal(trErr)
	}

	feedStep := func(idx int64, oA, hA, lA, cA, oB, hB, lB, cB float64) {
		tMS := baseMS + idx*3600_000
		clock.SetTimeMS(tMS + 3600_000)
		if fErr := trader.FeedDataSeries(makeKlineEvent(exsA, tf, tMS, oA, hA, lA, cA, 10000)); fErr != nil {
			t.Fatalf("step %d BTC feed fail: %v", idx, fErr)
		}
		if fErr := trader.FeedDataSeries(makeKlineEvent(exsB, tf, tMS, oB, hB, lB, cB, 10000)); fErr != nil {
			t.Fatalf("step %d ETH feed fail: %v", idx, fErr)
		}
	}

	// Bar 0: initial equity=10000; BTC queues 2000 notional (2 BTC @ 1000), ETH queues 1000 notional (10 ETH @ 100).
	feedStep(0, 1000, 1000, 1000, 1000, 100, 100, 100, 100)
	if math.Abs(observedA[0].eq-10000) > 1e-9 || math.Abs(observedA[0].ava-10000) > 1e-9 || observedA[0].dd != 0 {
		t.Fatalf("bar 0 BTC metrics = %+v, want eq=10000 ava=10000 dd=0", observedA[0])
	}
	if math.Abs(observedA[0].cost-2000) > 1e-9 {
		t.Fatalf("bar 0 BTC RiskRate LegalCost = %v, want 2000", observedA[0].cost)
	}

	// Bar 1 (flat): BTC fills at exact 1000 (2 BTC), ETH fills at exact 100 (10 ETH). Margins frozen: 400 + 200 = 600.
	feedStep(1, 1000, 1000, 1000, 1000, 100, 100, 100, 100)
	if math.Abs(observedB[1].eq-10000) > 1e-6 || math.Abs(observedB[1].ava-9400) > 1e-6 {
		t.Fatalf("bar 1 ETH metrics after flat fill = %+v, want eq=10000 ava=9400", observedB[1])
	}

	// Bar 2: BTC rises to 1500 (+1000 unrealized PnL); ETH stays 100 (0 PnL).
	feedStep(2, 1000, 1500, 1000, 1500, 100, 100, 100, 100)
	if math.Abs(observedB[2].eq-11000) > 1e-6 {
		t.Fatalf("bar 2 ETH observed equity = %v, want 11000", observedB[2].eq)
	}
	if observedB[2].ava >= 11000 || observedB[2].ava <= 0 {
		t.Fatalf("bar 2 available balance = %v, expected positive free margin < equity", observedB[2].ava)
	}

	// Bar 3: BTC hits TakeProfit=2000 (+2000 realized PnL -> settled cash becomes 12000)!
	// Deliberately poison wallets.Items["USDT"].UnrealizedPOL before feeding bar 3 to prove stale cache is never double-counted!
	wallets.Items["USDT"].UnrealizedPOL = 9999
	feedStep(3, 1500, 2050, 1500, 2000, 100, 100, 100, 100)
	if math.Abs(observedB[3].eq-12000) > 1e-6 {
		t.Fatalf("bar 3 same-step ETH equity = %v, want 12000 (no double-counting)", observedB[3].eq)
	}
	// ETH RiskRate=0.02, SL=10 on 100 (10%) => LegalCost = (12000 * 0.02) / 0.10 = 2400 (compounded from 2000 -> 2400!).
	if math.Abs(observedB[3].cost-2400) > 1e-6 {
		t.Fatalf("bar 3 compounded ETH LegalCost = %v, want 2400", observedB[3].cost)
	}
	if observedB[3].dd != 0 {
		t.Fatalf("bar 3 drawdown at new peak = %v, want 0", observedB[3].dd)
	}

	// Bar 4 (flat): ETH fills the 2400 order at exact 100 (total 10 + 24 = 34 ETH @ 100).
	feedStep(4, 2000, 2000, 2000, 2000, 100, 100, 100, 100)

	// Bar 5: ETH rises to 110 => unrealized +340 => peak equity reaches 12340!
	feedStep(5, 2000, 2000, 2000, 2000, 100, 110, 100, 110)
	if math.Abs(observedB[5].eq-12340) > 1e-6 || observedB[5].dd != 0 {
		t.Fatalf("bar 5 peak metrics = %+v, want eq=12340 dd=0", observedB[5])
	}

	// Bar 6: ETH drops to 95 => 34 * (95 - 100) = -170 unrealized => equity = 12000 - 170 = 11830.
	// Drawdown from peak 12340 = (12340 - 11830) / 12340.
	feedStep(6, 2000, 2000, 2000, 2000, 110, 110, 95, 95)
	wantDD := (12340.0 - 11830.0) / 12340.0
	if math.Abs(observedB[6].eq-11830) > 1e-6 {
		t.Fatalf("bar 6 equity = %v, want 11830", observedB[6].eq)
	}
	if math.Abs(observedB[6].dd-wantDD) > 1e-9 {
		t.Fatalf("bar 6 drawdown = %.12f, want %.12f", observedB[6].dd, wantDD)
	}
}

func TestStratJobAccountMetricsLiveSpotAndSharedAccount(t *testing.T) {
	// 1) Live contract wallet via UpdateWalletByBalances.
	t.Run("LiveContractWallet", func(t *testing.T) {
		coreState, coreErr := core.NewState(nil)
		if coreErr != nil {
			t.Fatal(coreErr)
		}
		defer coreState.Close()
		coreState.SetRunMode(core.RunModeLive)
		coreState.SetRunEnv(core.RunEnvProd)
		coreState.Market = banexg.MarketLinear
		coreState.IsContract = true

		deps := completeTraderDepsForTest(RuntimeDeps{Core: coreState})
		wallet := deps.Trading.Wallet("default")
		wallet.bindRuntimeDeps(deps)

		UpdateWalletByBalancesWithRuntime(wallet, &banexg.Balances{
			Assets: map[string]*banexg.Asset{
				"USDT": {Code: "USDT", Free: 7000, Used: 3000, UPol: 500, Total: 10000},
			},
		})

		job := &strat.StratJob{Account: "default", Strat: &strat.TradeStrat{Name: "live"}}
		job.BindRuntimeState(deps.Strategies, deps.Core, deps.Clock)

		if eq, ava, dd := job.AccountEquity(), job.AvailableBalance(), job.AccountDrawdown(); math.Abs(eq-10500) > 1e-9 || math.Abs(ava-7000) > 1e-9 || dd != 0 {
			t.Fatalf("live initial metrics = (%v, %v, %v), want (10500, 7000, 0)", eq, ava, dd)
		}

		UpdateWalletByBalancesWithRuntime(wallet, &banexg.Balances{
			Assets: map[string]*banexg.Asset{
				"USDT": {Code: "USDT", Free: 6500, Used: 3000, UPol: -500, Total: 9500},
			},
		})
		wantDD := (10500.0 - 9000.0) / 10500.0
		if eq, ava, dd := job.AccountEquity(), job.AvailableBalance(), job.AccountDrawdown(); math.Abs(eq-9000) > 1e-9 || math.Abs(ava-6500) > 1e-9 || math.Abs(dd-wantDD) > 1e-9 {
			t.Fatalf("live drawdown metrics = (%v, %v, %v), want (9000, 6500, %v)", eq, ava, dd, wantDD)
		}
	})

	// 2) Spot wallet does not double-count base coin or treat held spot position as available cash.
	t.Run("SpotBacktestWallet", func(t *testing.T) {
		coreState, coreErr := core.NewState(nil)
		if coreErr != nil {
			t.Fatal(coreErr)
		}
		defer coreState.Close()
		coreState.SetRunMode(core.RunModeBackTest)
		coreState.Market = banexg.MarketSpot
		coreState.IsContract = false

		clock := btime.NewClockState(true, nil)
		clock.SetTimeMS(1_700_000_000_000)
		market := com.NewMarketState("binance")
		market.Prices.SetBarPriceAt(clock.TimeMS(), "BTC/USDT", 7000)
		ex := newRiskSizingMockExchange(banexg.MarketSpot, "BTC/USDT")

		deps := completeTraderDepsForTest(RuntimeDeps{
			Core:     coreState,
			Clock:    clock,
			Market:   market,
			Exchange: ex,
		})
		wallet := deps.Trading.Wallet("default")
		wallet.bindRuntimeDeps(deps)
		wallet.SetWallets(map[string]float64{"USDT": 4000, "BTC": 1})

		// Open spot long holding 1 BTC bought at 6000 (now worth 7000).
		deps.Orders.SetTask("default", &ormo.BotTask{ID: 1})
		od := &ormo.InOutOrder{
			IOrder: &ormo.IOrder{
				ID: 1, TaskID: 1, Symbol: "BTC/USDT", Status: ormo.InOutStatusFullEnter,
				Profit: 1000, EnterAt: clock.TimeMS() - 60_000,
			},
			Enter: &ormo.ExOrder{Enter: true, Amount: 1, Filled: 1, Average: 6000, Status: ormo.OdStatusClosed},
		}
		od.BindState(deps.Orders)
		if sErr := od.Save(); sErr != nil {
			t.Fatal(sErr)
		}

		job := &strat.StratJob{Account: "default", Strat: &strat.TradeStrat{Name: "spot"}}
		job.BindRuntimeState(deps.Strategies, deps.Core, deps.Clock)

		// Equity = 4000 USDT + 1 BTC * 7000 = 11000 (order Profit=+1000 is NOT double-counted).
		// AvailableBalance = 4000 USDT (the 1 BTC held by open spot order is excluded from free cash).
		if eq := job.AccountEquity(); math.Abs(eq-11000) > 1e-9 {
			t.Fatalf("spot AccountEquity() = %v, want 11000", eq)
		}
		if ava := job.AvailableBalance(); math.Abs(ava-4000) > 1e-9 {
			t.Fatalf("spot AvailableBalance() = %v, want 4000", ava)
		}
	})

	// 3) SharedAccount execution ledger.
	t.Run("SharedAccountLedger", func(t *testing.T) {
		key := execution.AccountKey{VenueSessionIdentity: "ts02-shared", Account: "default", SettlementDomain: "USDT"}
		registry := &execution.AccountRegistry{}
		owner, err := registry.Acquire(key)
		if err != nil {
			t.Fatal(err)
		}
		adapter, err := execution.NewPaperAdapter(decimal.NewFromInt(20000), decimal.Zero, decimal.Zero)
		if err != nil {
			t.Fatal(err)
		}
		service, err := execution.NewSharedAccount(owner, execution.SharedExecutionOptions{
			Memory:                true,
			Adapter:               adapter,
			AuthoritativeSnapshot: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		borrow := service.Borrow()
		t.Cleanup(func() {
			borrow.Release()
			_ = service.Close()
			registry.Close()
		})

		if err := borrow.BootstrapCapital(context.Background(), map[execution.StrategyID]decimal.Decimal{
			"s1": decimal.NewFromInt(20000),
		}, 1000); err != nil {
			t.Fatal(err)
		}

		coreState, coreErr := core.NewState(nil)
		if coreErr != nil {
			t.Fatal(coreErr)
		}
		defer coreState.Close()
		coreState.SetRunMode(core.RunModeBackTest)
		coreState.Market = banexg.MarketLinear
		coreState.IsContract = true

		deps := completeTraderDepsForTest(RuntimeDeps{
			Core:            coreState,
			SharedExecution: borrow,
		})
		job := &strat.StratJob{Account: "default", Strat: &strat.TradeStrat{Name: "shared"}}
		job.BindRuntimeState(deps.Strategies, deps.Core, deps.Clock)

		if eq, ava, dd := job.AccountEquity(), job.AvailableBalance(), job.AccountDrawdown(); math.Abs(eq-20000) > 1e-9 || math.Abs(ava-20000) > 1e-9 || dd != 0 {
			t.Fatalf("SharedAccount initial metrics = (%v, %v, %v), want (20000, 20000, 0)", eq, ava, dd)
		}

		// Apply a fee/funding debit of -2000 to verify real-time equity & HWM drawdown on SharedAccount.
		if err := borrow.CashEvent(execution.CashEvent{
			ID:           "funding-debit-1",
			Kind:         execution.Funding,
			AccountDelta: decimal.NewFromInt(-2000),
			Postings:     []execution.CashPosting{{Strategy: "s1", Amount: decimal.NewFromInt(-2000)}},
			AtMS:         2000,
		}); err != nil {
			t.Fatal(err)
		}
		if eq, dd := job.AccountEquity(), job.AccountDrawdown(); math.Abs(eq-18000) > 1e-9 || math.Abs(dd-0.10) > 1e-9 {
			t.Fatalf("SharedAccount post-debit metrics = (%v, %v), want (18000, 0.10)", eq, dd)
		}
	})
}
