package strat

import (
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
	ta "github.com/banbox/banta"
)

func TestBatchStateNeutralBarPriorityOrderAndSeed(t *testing.T) {
	symbols := []string{
		"ADA/USDT:USDT", "APT/USDT:USDT", "ARB/USDT:USDT", "ATOM/USDT:USDT",
		"AVAX/USDT:USDT", "BNB/USDT:USDT", "BTC/USDT:USDT", "DOGE/USDT:USDT",
		"DOT/USDT:USDT", "ETH/USDT:USDT", "LINK/USDT:USDT", "LTC/USDT:USDT",
		"MATIC/USDT:USDT", "NEAR/USDT:USDT", "OP/USDT:USDT", "PEPE/USDT:USDT",
		"SHIB/USDT:USDT", "SOL/USDT:USDT", "SUI/USDT:USDT", "TIA/USDT:USDT",
		"TON/USDT:USDT", "TRX/USDT:USDT", "UNI/USDT:USDT", "XRP/USDT:USDT",
	}

	makeBatchOrder := func(barMS int64, seed uint64, inputOrder []string) []string {
		cfg := &config.Config{BTBarOrderSeed: seed}
		strategy := &TradeStrat{Name: "batch_cta", runtimeConfig: cfg, runtimeExplicit: true}
		state := NewBatchState()
		for _, sym := range inputOrder {
			env := &ta.BarEnv{Symbol: sym, TimeFrame: "1h", TFMSecs: 3_600_000, TimeStart: barMS - 3_600_000, TimeStop: barMS}
			job := &StratJob{
				Strat:     strategy,
				Env:       env,
				Symbol:    &orm.ExSymbol{Symbol: sym},
				TimeFrame: "1h",
				Account:   "default",
			}
			state.AddTask("1h_default_batch_cta", sym+"_main", &JobEnv{Job: job, Symbol: sym}, 3_600_000, barMS+3000)
		}
		ready, wait := state.TakeReady(barMS+3001, true)
		if wait != 0 || len(ready) != 1 {
			t.Fatalf("TakeReady wait=%d len=%d, want 0 and 1", wait, len(ready))
		}
		out := make([]string, len(ready[0].MainJobs))
		for i, j := range ready[0].MainJobs {
			out[i] = j.Symbol.Symbol
		}
		return out
	}

	// 1) Bit-reproducible regardless of AddTask insertion order.
	bar0 := int64(1_700_000_000_000)
	baseline := makeBatchOrder(bar0, 0, symbols)
	if slices.Equal(baseline, symbols) {
		t.Fatalf("expected neutral hash order, got alphabetical order: %v", baseline)
	}
	reversed := slices.Clone(symbols)
	slices.Reverse(reversed)
	for run := 0; run < 20; run++ {
		if got := makeBatchOrder(bar0, 0, reversed); !slices.Equal(got, baseline) {
			t.Fatalf("run %d order mismatch:\ngot:  %v\nwant: %v", run, got, baseline)
		}
	}

	// 2) Uniformity across 1000 bars: every symbol leads roughly equally, no alphabetical bias toward 'A' names.
	const numBars = 1000
	firstCounts := make(map[string]int, len(symbols))
	aPrefixWins := 0
	for i := 0; i < numBars; i++ {
		barMS := bar0 + int64(i)*3_600_000
		order := makeBatchOrder(barMS, 0, symbols)
		lead := order[0]
		firstCounts[lead]++
		if lead[0] == 'A' {
			aPrefixWins++
		}
	}
	if len(firstCounts) != len(symbols) {
		t.Fatalf("only %d of %d symbols ever placed first: %v", len(firstCounts), len(symbols), firstCounts)
	}
	// 5 of 24 symbols start with 'A' (~20.8%); alphabetical ordering would produce 1000/1000 (100%) 'A' wins.
	if aPrefixWins > 300 {
		t.Fatalf("alphabetical bias persisted: A-prefix won %d/%d bars", aPrefixWins, numBars)
	}
	expected := float64(numBars) / float64(len(symbols))
	chi2 := 0.0
	for _, sym := range symbols {
		d := float64(firstCounts[sym]) - expected
		chi2 += d * d / expected
	}
	// df=23, 99.9% critical value = 49.7
	if chi2 > 49.7 {
		t.Fatalf("first-place distribution non-uniform: chi2=%.2f counts=%v", chi2, firstCounts)
	}

	// 3) Changing bt_bar_order_seed changes the order reproducibly, and BatchState.SetBarOrderSeed overrides it.
	seeded := makeBatchOrder(bar0, 42, symbols)
	if slices.Equal(seeded, baseline) {
		t.Fatalf("seed=42 produced identical order to seed=0: %v", seeded)
	}
	overrideState := NewBatchState()
	overrideState.SetBarOrderSeed(42)
	if gotSeed, ok := overrideState.BarOrderSeed(); !ok || gotSeed != 42 {
		t.Fatalf("BarOrderSeed() = (%d, %v), want (42, true)", gotSeed, ok)
	}
	cfgZero := &config.Config{BTBarOrderSeed: 0}
	stgZero := &TradeStrat{Name: "batch_override", runtimeConfig: cfgZero, runtimeExplicit: true}
	for _, sym := range reversed {
		env := &ta.BarEnv{Symbol: sym, TimeFrame: "1h", TFMSecs: 3_600_000, TimeStop: bar0}
		job := &StratJob{Strat: stgZero, Env: env, Symbol: &orm.ExSymbol{Symbol: sym}, TimeFrame: "1h", Account: "default"}
		overrideState.AddTask("1h_default_batch_override", sym+"_main", &JobEnv{Job: job, Symbol: sym}, 3_600_000, bar0+3000)
	}
	readyOverride, _ := overrideState.TakeReady(bar0+3001, true)
	gotOverride := make([]string, len(readyOverride[0].MainJobs))
	for i, j := range readyOverride[0].MainJobs {
		gotOverride[i] = j.Symbol.Symbol
	}
	if !slices.Equal(gotOverride, seeded) {
		t.Fatalf("SetBarOrderSeed(42) order = %v, want %v", gotOverride, seeded)
	}
	overrideState.Reset()
	if _, ok := overrideState.BarOrderSeed(); ok {
		t.Fatal("Reset() did not clear explicit BarOrderSeed")
	}
}

type rebalanceTestHarness struct {
	coreState *core.State
	clock     *btime.ClockState
	prices    *com.PriceState
	symbols   *orm.SymbolState
	state     *State
	strategy  *TradeStrat
	metrics   *AccountMetrics
	jobsBySym map[string]*StratJob
	jobs      []*StratJob
	nextOdID  int64
}

type symbolSpec struct {
	symbol string
	price  float64
	natr   float64 // approximate % range per bar when feeding synthetic bars
}

func newRebalanceTestHarness(t *testing.T, equity float64, specs []symbolSpec) *rebalanceTestHarness {
	t.Helper()
	coreState, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coreState.Close)
	coreState.SetRunMode(core.RunModeBackTest)
	coreState.Market = banexg.MarketLinear
	coreState.IsContract = true

	barStopMS := int64(1_700_003_600_000)
	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(barStopMS)

	cfg := &config.Config{
		StakeAmount:    100,
		Leverage:       5,
		OpenVolRate:    1,
		BTBarOrderSeed: 11,
		Accounts:       map[string]*config.AccountConfig{"default": {Leverage: 5}},
	}

	exSymbols := make([]*orm.ExSymbol, len(specs))
	for i, sp := range specs {
		exSymbols[i] = &orm.ExSymbol{
			ID:       int32(i + 1),
			Exchange: "binance",
			Market:   banexg.MarketLinear,
			Symbol:   sp.symbol,
		}
	}
	symState := orm.NewSymbolState()
	if setErr := symState.SetExSymbols(exSymbols); setErr != nil {
		t.Fatal(setErr)
	}

	prices := com.NewPriceState("binance")
	stratState := NewStateWithRuntime(coreState, clock, cfg, symState, nil)
	metrics := &AccountMetrics{Equity: equity, Available: equity, Valid: true}
	owner := new(int)
	if bindErr := stratState.BindAccountMetrics(owner, "default", func(string) AccountMetrics {
		return *metrics
	}); bindErr != nil {
		t.Fatal(bindErr)
	}

	stgy := &TradeStrat{
		Name:            "test:batch_rebalance",
		BatchInOut:      true,
		runtimeConfig:   cfg,
		runtimeCore:     coreState,
		runtimeClock:    clock,
		runtimeExplicit: true,
	}

	jobsBySym := make(map[string]*StratJob, len(specs))
	jobs := make([]*StratJob, 0, len(specs))
	const numBars = 30
	for i, sp := range specs {
		env, envErr := ta.NewBarEnv("binance", banexg.MarketLinear, sp.symbol, "1h")
		if envErr != nil {
			t.Fatal(envErr)
		}
		pct := sp.natr / 100.0
		if pct <= 0 {
			pct = 0.02
		}
		half := sp.price * pct * 0.5
		for b := 0; b < numBars; b++ {
			openMS := barStopMS - int64(numBars-b)*3_600_000
			// Constant High-Low range around Close=sp.price yields exact TR = sp.price*pct and NATR = sp.natr.
			if bErr := env.OnBar(openMS, sp.price, sp.price+half, sp.price-half, sp.price, 10_000, sp.price*10_000, 5_000, 500); bErr != nil {
				t.Fatal(bErr)
			}
		}
		prices.SetBarPriceAt(barStopMS, sp.symbol, sp.price)
		job := &StratJob{
			Strat:         stgy,
			Env:           env,
			DataHub:       NewDataHub(),
			Symbol:        exSymbols[i],
			TimeFrame:     "1h",
			Account:       "default",
			CloseLong:     true,
			CloseShort:    true,
			ExgStopLoss:   true,
			ExgTakeProfit: true,
			LastBarMS:     barStopMS,
			symbols:       symState,
		}
		job.BindRuntimeState(stratState, coreState, clock)
		job.BindRuntimeMarket(prices, clock)
		jobsBySym[sp.symbol] = job
		jobs = append(jobs, job)
	}

	return &rebalanceTestHarness{
		coreState: coreState,
		clock:     clock,
		prices:    prices,
		symbols:   symState,
		state:     stratState,
		strategy:  stgy,
		metrics:   metrics,
		jobsBySym: jobsBySym,
		jobs:      jobs,
		nextOdID:  1,
	}
}

func (h *rebalanceTestHarness) setPrice(symbol string, newPrice float64) {
	job := h.jobsBySym[symbol]
	nextOpenMS := job.Env.TimeStop
	_ = job.Env.OnBar(nextOpenMS, newPrice, newPrice*1.01, newPrice*0.99, newPrice, 10_000, newPrice*10_000, 5_000, 500)
	job.LastBarMS = job.Env.TimeStop
	h.clock.SetTimeMS(job.Env.TimeStop)
	h.prices.SetBarPriceAt(job.Env.TimeStop, symbol, newPrice)
}

// applySimFills simulates local backtest execution of queued Exits then Entrys across all jobs
// so multi-step rebalance lifecycles can be tested deterministically within package strat.
func (h *rebalanceTestHarness) applySimFills(t *testing.T) {
	t.Helper()
	for _, job := range h.jobs {
		enters, exits := job.DrainOrderRequests()
		px := job.Env.Close.Get(0)
		var curOrders []*ormo.InOutOrder
		curOrders = append(curOrders, job.GetOrders(core.OdDirtLong)...)
		curOrders = append(curOrders, job.GetOrders(core.OdDirtShort)...)

		for _, ex := range exits {
			rate := ex.ExitRate
			if rate <= 0 || rate >= 1-1e-9 {
				rate = 1
			}
			kept := make([]*ormo.InOutOrder, 0, len(curOrders))
			for _, od := range curOrders {
				matchSide := ex.Dirt == core.OdDirtBoth ||
					(ex.Dirt == core.OdDirtLong && !od.Short) ||
					(ex.Dirt == core.OdDirtShort && od.Short)
				if !matchSide {
					kept = append(kept, od)
					continue
				}
				if rate >= 1 {
					continue
				}
				remAmt := od.HoldAmount() * (1 - rate)
				if remAmt <= core.AmtDust {
					continue
				}
				exitAmt := od.HoldAmount() - remAmt
				if od.Exit == nil {
					od.Exit = &ormo.ExOrder{Average: od.Enter.Average, Price: od.Enter.Average}
				}
				od.Exit.Filled += exitAmt
				od.Status = ormo.InOutStatusPartExit
				kept = append(kept, od)
			}
			curOrders = kept
		}

		for _, en := range enters {
			amt := en.Amount
			if amt <= 0 {
				amt = en.LegalCost / px
			}
			od := &ormo.InOutOrder{
				IOrder: &ormo.IOrder{
					ID:        h.nextOdID,
					Symbol:    job.Symbol.Symbol,
					Timeframe: job.TimeFrame,
					Strategy:  job.Strat.Name,
					Short:     en.Short,
					EnterTag:  en.Tag,
					InitPrice: px,
					QuoteCost: amt * px,
					Leverage:  en.Leverage,
					EnterAt:   job.Env.TimeStop,
					Status:    ormo.InOutStatusFullEnter,
				},
				Enter: &ormo.ExOrder{
					Price:   px,
					Average: px,
					Amount:  amt,
					Filled:  amt,
					Status:  ormo.OdStatusClosed,
				},
			}
			h.nextOdID++
			curOrders = append(curOrders, od)
		}
		job.UpdateOrders(curOrders)
	}
}

func TestRebalanceBatchWeightsFullLifecycle(t *testing.T) {
	h := newRebalanceTestHarness(t, 10_000, []symbolSpec{
		{symbol: "BTC/USDT:USDT", price: 50_000, natr: 2.0},
		{symbol: "ETH/USDT:USDT", price: 2_500, natr: 3.0},
		{symbol: "SOL/USDT:USDT", price: 100, natr: 5.0},
		{symbol: "XRP/USDT:USDT", price: 0.5, natr: 4.0},
	})

	// Step 1: Empty -> Initial portfolio construction + sub-MinStakeAmount dust ignored + descending LegalCost order.
	// BTC: +0.40 ($4,000), ETH: -0.25 ($2,500), SOL: +0.15 ($1,500), XRP: +0.0004 ($4.00 < MinStakeAmount 5.5 -> dust skip).
	r1 := RebalanceBatchWeights(h.jobs, map[string]float64{
		"BTC/USDT:USDT": 0.40,
		"ETH/USDT:USDT": -0.25,
		"SOL/USDT:USDT": 0.15,
		"XRP/USDT:USDT": 0.0004,
	}, BatchRebalanceOpts{
		StopLossRate:   0.05,
		TakeProfitRate: 0.10,
		CallbackPct:    1.5,
		Leverage:       3,
	})
	if r1.OpenedCount != 3 || r1.SkippedDustCount != 1 || r1.ErrorCount != 0 {
		t.Fatalf("Step 1 counts: opened=%d dust=%d err=%d, want 3, 1, 0", r1.OpenedCount, r1.SkippedDustCount, r1.ErrorCount)
	}
	// Verify descending LegalCost order in Pass 2: BTC ($4000) -> ETH ($2500) -> SOL ($1500).
	if len(r1.Actions) != 3 ||
		r1.Actions[0].Symbol != "BTC/USDT:USDT" || math.Abs(r1.Actions[0].LegalCost-4000) > 1e-9 ||
		r1.Actions[1].Symbol != "ETH/USDT:USDT" || math.Abs(r1.Actions[1].LegalCost-2500) > 1e-9 || !r1.Actions[1].Short ||
		r1.Actions[2].Symbol != "SOL/USDT:USDT" || math.Abs(r1.Actions[2].LegalCost-1500) > 1e-9 {
		t.Fatalf("Step 1 action sequence mismatch: %+v", r1.Actions)
	}
	if len(h.jobsBySym["XRP/USDT:USDT"].Entrys) != 0 {
		t.Fatal("expected dust XRP order (< MinStakeAmount) to be skipped")
	}

	// Verify protective StopLoss / TakeProfit / CallbackPct / Leverage on queued EnterReq!
	btcEntry := h.jobsBySym["BTC/USDT:USDT"].Entrys[0]
	if btcEntry.Tag != DefaultBatchRebalanceTag ||
		math.Abs(btcEntry.StopLoss-47_500) > 1e-6 ||
		math.Abs(btcEntry.TakeProfit-55_000) > 1e-6 ||
		btcEntry.CallbackPct != 1.5 ||
		btcEntry.Leverage != 3 {
		t.Fatalf("BTC protective EnterReq mismatch: %+v", btcEntry)
	}
	ethEntry := h.jobsBySym["ETH/USDT:USDT"].Entrys[0]
	if !ethEntry.Short ||
		math.Abs(ethEntry.StopLoss-2_625) > 1e-6 ||
		math.Abs(ethEntry.TakeProfit-2_250) > 1e-6 {
		t.Fatalf("ETH short protective EnterReq mismatch: %+v", ethEntry)
	}
	h.applySimFills(t)

	// Step 2: Micro-drift inside DriftThreshold (default 0.02) -> zero orders placed!
	r2 := RebalanceBatchWeights(h.jobs, map[string]float64{
		"BTC/USDT:USDT": 0.415,  // +0.015 < 0.02
		"ETH/USDT:USDT": -0.235, // +0.015 < 0.02
		"SOL/USDT:USDT": 0.140,  // -0.010 < 0.02
	}, BatchRebalanceOpts{})
	if r2.SkippedDriftCount != 3 || len(r2.Actions) != 0 || r2.ErrorCount != 0 {
		t.Fatalf("Step 2 expected all 3 within drift buffer, got skippedDrift=%d actions=%+v", r2.SkippedDriftCount, r2.Actions)
	}

	// Step 3: Mixed rebalance verifying Pass 1 (closes & reductions) strictly precedes Pass 2 (opens & scale-ins):
	// - BTC (+0.40 -> +0.20): partial reduce by ExitRate = (0.40 - 0.20) / 0.40 = 0.50
	// - ETH (-0.25 -> +0.30): short-to-long reversal (Pass 1 full close short, Pass 2 open +$3,000 long)
	// - SOL (+0.15 -> 0.00): full close
	// - XRP (0.00 -> +0.35): new open +$3,500 long (larger than ETH's $3,000, so XRP opens before ETH!)
	r3 := RebalanceBatchWeights(h.jobs, map[string]float64{
		"BTC/USDT:USDT": 0.20,
		"ETH/USDT:USDT": 0.30,
		"SOL/USDT:USDT": 0.00,
		"XRP/USDT:USDT": 0.35,
	}, BatchRebalanceOpts{Tag: "cta_reb"})

	if r3.ClosedCount != 2 || r3.ReducedCount != 1 || r3.ReversedCount != 1 || r3.OpenedCount != 2 || r3.ErrorCount != 0 {
		t.Fatalf("Step 3 summary mismatch: %+v", r3)
	}
	if len(r3.Actions) != 5 {
		t.Fatalf("Step 3 expected 5 actions (3 exits in Pass 1 + 2 opens in Pass 2), got %+v", r3.Actions)
	}
	// First 3 actions MUST be Pass 1 exits (reduce/close), last 2 MUST be Pass 2 opens ordered XRP ($3500) -> ETH ($3000).
	for i := 0; i < 3; i++ {
		if r3.Actions[i].Kind != RebalanceActionClose && r3.Actions[i].Kind != RebalanceActionReduce {
			t.Fatalf("action[%d] = %+v, expected Pass 1 close/reduce before any open", i, r3.Actions[i])
		}
	}
	btcReduce := h.jobsBySym["BTC/USDT:USDT"].Exits[0]
	if btcReduce.Dirt != core.OdDirtLong || math.Abs(btcReduce.ExitRate-0.5) > 1e-9 || btcReduce.Tag != "cta_reb" {
		t.Fatalf("BTC partial reduce ExitReq = %+v, want Dirt=Long ExitRate=0.5", btcReduce)
	}
	ethClose := h.jobsBySym["ETH/USDT:USDT"].Exits[0]
	if ethClose.Dirt != core.OdDirtShort || ethClose.ExitRate != 1 {
		t.Fatalf("ETH reversal close ExitReq = %+v, want Dirt=Short ExitRate=1", ethClose)
	}
	if r3.Actions[3].Symbol != "XRP/USDT:USDT" || math.Abs(r3.Actions[3].LegalCost-3500) > 1e-9 ||
		r3.Actions[4].Symbol != "ETH/USDT:USDT" || math.Abs(r3.Actions[4].LegalCost-3000) > 1e-9 || r3.Actions[4].Short {
		t.Fatalf("Pass 2 open order mismatch: got [%+v, %+v]", r3.Actions[3], r3.Actions[4])
	}
	h.applySimFills(t)

	// Step 4: Same-direction scale-in (BTC +0.20 -> +0.45 => delta +0.25 = $2,500) & Long-to-Short reversal on XRP (+0.35 -> -0.20).
	r4 := RebalanceBatchWeights(h.jobs, map[string]float64{
		"BTC/USDT:USDT": 0.45,
		"ETH/USDT:USDT": 0.30, // unchanged -> drift skip
		"XRP/USDT:USDT": -0.20,
	}, BatchRebalanceOpts{})
	if r4.ClosedCount != 1 || r4.ReversedCount != 1 || r4.ScaledInCount != 1 || r4.OpenedCount != 1 || r4.SkippedDriftCount != 1 {
		t.Fatalf("Step 4 summary mismatch: %+v", r4)
	}
	if r4.Actions[0].Symbol != "XRP/USDT:USDT" || r4.Actions[0].Kind != RebalanceActionClose ||
		r4.Actions[1].Symbol != "BTC/USDT:USDT" || r4.Actions[1].Kind != RebalanceActionScaleIn || math.Abs(r4.Actions[1].LegalCost-2500) > 1e-9 ||
		r4.Actions[2].Symbol != "XRP/USDT:USDT" || r4.Actions[2].Kind != RebalanceActionOpen || !r4.Actions[2].Short || math.Abs(r4.Actions[2].LegalCost-2000) > 1e-9 {
		t.Fatalf("Step 4 actions mismatch: %+v", r4.Actions)
	}
	h.applySimFills(t)

	// Step 5: Flatten entire portfolio by passing empty map.
	r5 := RebalanceBatchWeights(h.jobs, nil, BatchRebalanceOpts{Tag: "flatten_all"})
	if r5.ClosedCount != 3 || r5.OpenedCount != 0 || r5.ErrorCount != 0 {
		t.Fatalf("Step 5 flatten mismatch: %+v", r5)
	}
	h.applySimFills(t)
	for _, job := range h.jobs {
		if job.GetOrderNum(0) != 0 {
			t.Fatalf("%s still has %d open orders after full flatten", job.Symbol.Symbol, job.GetOrderNum(0))
		}
	}
}

func TestRebalanceBatchWeightsMaxGrossExposureAndMarkToMarket(t *testing.T) {
	h := newRebalanceTestHarness(t, 10_000, []symbolSpec{
		{symbol: "BTC/USDT:USDT", price: 50_000, natr: 2.0},
		{symbol: "ETH/USDT:USDT", price: 2_500, natr: 3.0},
		{symbol: "SOL/USDT:USDT", price: 100, natr: 4.0},
	})

	// 1) Raw gross = 0.8 + 0.6 + 0.2 = 1.6 > MaxGrossExposure 0.8 => ScaleFactor = 0.5
	// Scaled weights: BTC +0.4 ($4,000), ETH -0.3 ($3,000), SOL +0.1 ($1,000); small portfolio leg below 0.02 still opens from flat!
	r1 := RebalanceBatchWeights(h.jobs, map[string]float64{
		"BTC/USDT:USDT": 0.80,
		"ETH/USDT:USDT": -0.60,
		"SOL/USDT:USDT": 0.02, // scaled to 0.01 ($100 >= $5.50), < DriftThreshold(0.02) yet opens cleanly from flat!
	}, BatchRebalanceOpts{MaxGrossExposure: 0.71})

	wantRaw := 1.42
	wantScale := 0.71 / wantRaw // 0.5
	if math.Abs(r1.RawGrossExposure-wantRaw) > 1e-9 ||
		math.Abs(r1.ScaleFactor-wantScale) > 1e-9 ||
		math.Abs(r1.EffectiveGrossExposure-0.71) > 1e-9 {
		t.Fatalf("exposure scaling mismatch: %+v", r1)
	}
	if r1.OpenedCount != 3 {
		t.Fatalf("expected all 3 scaled legs (including 1%% SOL leg from flat) to open, got %+v", r1)
	}
	if math.Abs(h.jobsBySym["BTC/USDT:USDT"].Entrys[0].LegalCost-4000) > 1e-6 ||
		math.Abs(h.jobsBySym["ETH/USDT:USDT"].Entrys[0].LegalCost-3000) > 1e-6 ||
		math.Abs(h.jobsBySym["SOL/USDT:USDT"].Entrys[0].LegalCost-100) > 1e-6 {
		t.Fatalf("scaled LegalCosts mismatch: BTC=%v ETH=%v SOL=%v",
			h.jobsBySym["BTC/USDT:USDT"].Entrys[0].LegalCost,
			h.jobsBySym["ETH/USDT:USDT"].Entrys[0].LegalCost,
			h.jobsBySym["SOL/USDT:USDT"].Entrys[0].LegalCost)
	}
	h.applySimFills(t)

	// 2) Mark-to-market drift: BTC doubles from 50,000 to 100,000 (0.08 BTC held => $8,000 notional).
	// Account equity rises from $10,000 to $14,000, so BTC's actual notional weight is 8,000 / 14,000 = 0.571428...
	// Rebalancing BTC back to 0.40 must REDUCE BTC by ExitRate = (0.571428 - 0.40) / 0.571428 = 0.30!
	h.setPrice("BTC/USDT:USDT", 100_000)
	h.metrics.Equity = 14_000
	r2 := RebalanceBatchWeights([]*StratJob{h.jobsBySym["BTC/USDT:USDT"]}, map[string]float64{
		"BTC/USDT:USDT": 0.40,
	}, BatchRebalanceOpts{})
	if r2.ReducedCount != 1 || len(h.jobsBySym["BTC/USDT:USDT"].Exits) != 1 {
		t.Fatalf("expected mark-to-market overweight BTC to be reduced, got %+v", r2)
	}
	gotExitRate := h.jobsBySym["BTC/USDT:USDT"].Exits[0].ExitRate
	if math.Abs(gotExitRate-0.30) > 1e-6 {
		t.Fatalf("mark-to-market BTC ExitRate = %.8f, want 0.30", gotExitRate)
	}
}

func TestInverseVolWeightsRiskParityAndCacheAlignment(t *testing.T) {
	// NATR setup: BTC=1.5%, ETH=3.0%, SOL=6.0% (1 : 2 : 4 volatility ratio).
	h := newRebalanceTestHarness(t, 20_000, []symbolSpec{
		{symbol: "BTC/USDT:USDT", price: 60_000, natr: 1.5},
		{symbol: "ETH/USDT:USDT", price: 3_000, natr: 3.0},
		{symbol: "SOL/USDT:USDT", price: 150, natr: 6.0},
	})

	// 1) Pre-warmed BarEnv (where ta.NATR was never called in OnData during warmup) works on the
	// very first OnBatchJobs call, and zero-signal SOL still advances SOL's NATR cache cleanly.
	wPartial := InverseVolWeights(h.jobs, map[string]float64{
		"BTC/USDT:USDT": 1,
		"ETH/USDT:USDT": -1,
		"SOL/USDT:USDT": 0,
	}, 14, 0.90)
	if _, hasSOL := wPartial["SOL/USDT:USDT"]; hasSOL {
		t.Fatalf("zero-signal SOL should be excluded, got %v", wPartial)
	}
	if math.Abs(wPartial["BTC/USDT:USDT"]-0.60) > 1e-6 || math.Abs(wPartial["ETH/USDT:USDT"]+0.30) > 1e-6 {
		t.Fatalf("2-asset risk parity weights = %v, want BTC=+0.60 ETH=-0.30", wPartial)
	}

	// 2) Advance 20 incremental bars (with SOL signal=0 for the first 19 bars) so stateful ta.NATR
	// takes over on all 3 symbols without any series cache misalignment!
	symbolRanges := map[string]struct{ price, natr float64 }{
		"BTC/USDT:USDT": {60_000, 1.5},
		"ETH/USDT:USDT": {3_000, 3.0},
		"SOL/USDT:USDT": {150, 6.0},
	}
	for step := 0; step < 20; step++ {
		for _, sym := range []string{"BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"} {
			job := h.jobsBySym[sym]
			sp := symbolRanges[sym]
			half := sp.price * (sp.natr / 100.0) * 0.5
			openMS := job.Env.TimeStop
			if bErr := job.Env.OnBar(openMS, sp.price, sp.price+half, sp.price-half, sp.price, 10_000, sp.price*10_000, 5_000, 500); bErr != nil {
				t.Fatal(bErr)
			}
			job.LastBarMS = job.Env.TimeStop
			h.clock.SetTimeMS(job.Env.TimeStop)
			h.prices.SetBarPriceAt(job.Env.TimeStop, sym, sp.price)
		}
		if step < 19 {
			_ = InverseVolWeights(h.jobs, map[string]float64{
				"BTC/USDT:USDT": 1,
				"ETH/USDT:USDT": -1,
				"SOL/USDT:USDT": 0,
			}, 14, 0.90)
		}
	}

	btcNATR := ta.NATR(h.jobsBySym["BTC/USDT:USDT"].Env.High, h.jobsBySym["BTC/USDT:USDT"].Env.Low, h.jobsBySym["BTC/USDT:USDT"].Env.Close, 14).Get(0)
	ethNATR := ta.NATR(h.jobsBySym["ETH/USDT:USDT"].Env.High, h.jobsBySym["ETH/USDT:USDT"].Env.Low, h.jobsBySym["ETH/USDT:USDT"].Env.Close, 14).Get(0)
	solNATR := ta.NATR(h.jobsBySym["SOL/USDT:USDT"].Env.High, h.jobsBySym["SOL/USDT:USDT"].Env.Low, h.jobsBySym["SOL/USDT:USDT"].Env.Close, 14).Get(0)
	if math.IsNaN(btcNATR) || math.IsNaN(ethNATR) || math.IsNaN(solNATR) || !(btcNATR < ethNATR && ethNATR < solNATR) {
		t.Fatalf("expected stateful NATR ordering BTC(%v) < ETH(%v) < SOL(%v)", btcNATR, ethNATR, solNATR)
	}

	const targetGross = 0.84
	weights := InverseVolWeights(h.jobs, map[string]float64{
		"BTC/USDT:USDT": 1,
		"ETH/USDT:USDT": -1,
		"SOL/USDT:USDT": 1,
	}, 14, targetGross)

	wBTC := weights["BTC/USDT:USDT"]
	wETH := weights["ETH/USDT:USDT"]
	wSOL := weights["SOL/USDT:USDT"]
	if !(wBTC > math.Abs(wETH) && math.Abs(wETH) > wSOL && wETH < 0 && wSOL > 0) {
		t.Fatalf("inverse-vol ordering/sign violated: BTC=%v ETH=%v SOL=%v", wBTC, wETH, wSOL)
	}
	gross := math.Abs(wBTC) + math.Abs(wETH) + math.Abs(wSOL)
	if math.Abs(gross-targetGross) > 1e-12 {
		t.Fatalf("gross exposure = %.15f, want %.15f", gross, targetGross)
	}
	// Equal risk contribution: |w_i| * NATR_i must be identical across all active symbols!
	rcBTC := math.Abs(wBTC) * btcNATR
	rcETH := math.Abs(wETH) * ethNATR
	rcSOL := math.Abs(wSOL) * solNATR
	if math.Abs(rcBTC-rcETH) > 1e-9 || math.Abs(rcETH-rcSOL) > 1e-9 {
		t.Fatalf("risk contributions not equal: BTC=%v ETH=%v SOL=%v", rcBTC, rcETH, rcSOL)
	}

	// Permuting input jobs slice yields bit-identical weights.
	revJobs := slices.Clone(h.jobs)
	slices.Reverse(revJobs)
	weightsRev := InverseVolWeights(revJobs, map[string]float64{
		"BTC/USDT:USDT": 1,
		"ETH/USDT:USDT": -1,
		"SOL/USDT:USDT": 1,
	}, 14, targetGross)
	for _, sym := range []string{"BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"} {
		if weights[sym] != weightsRev[sym] {
			t.Fatalf("weight for %s changed with job slice order: %v vs %v", sym, weights[sym], weightsRev[sym])
		}
	}

	// Edge cases: insufficient warmup (1 bar -> NATR NaN), non-positive targetGross, empty signals.
	shortEnv, _ := ta.NewBarEnv("binance", banexg.MarketLinear, "NEW/USDT:USDT", "1h")
	_ = shortEnv.OnBar(1_700_000_000_000, 10, 11, 9, 10, 100, 1000, 50, 10)
	unreadyJob := &StratJob{Env: shortEnv, Symbol: &orm.ExSymbol{Symbol: "NEW/USDT:USDT"}}
	if got := InverseVolWeights([]*StratJob{unreadyJob}, map[string]float64{"NEW/USDT:USDT": 1}, 14, 1.0); len(got) != 0 {
		t.Fatalf("expected empty weights when NATR is NaN, got %v", got)
	}
	if got := InverseVolWeights(h.jobs, map[string]float64{"BTC/USDT:USDT": 1}, 14, 0); len(got) != 0 {
		t.Fatalf("expected empty weights for targetGross=0, got %v", got)
	}
	if got := InverseVolWeights(h.jobs, nil, 14, 1.0); len(got) != 0 {
		t.Fatalf("expected empty weights for nil signals, got %v", got)
	}
	_ = fmt.Sprint(rcBTC)
}
