package strat

import (
	"math"
	"reflect"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	ta "github.com/banbox/banta"
	"go.uber.org/zap/zapcore"
)

func TestEnterReqCloneAndZapFieldsCompleteness(t *testing.T) {
	orig := &EnterReq{
		CommandID:       "cmd-1",
		Tag:             "atr_breakout",
		StratName:       "demo:atr",
		Short:           true,
		OrderType:       core.OrderTypeLimit,
		Limit:           1950.5,
		Stop:            1960.0,
		CostRate:        0.75,
		RiskRate:        0.015,
		LegalCost:       1200.0,
		Leverage:        5.0,
		Amount:          0.6,
		StopLossVal:     40.0,
		StopLoss:        2010.0,
		StopLossLimit:   2015.0,
		StopLossRate:    0.5,
		StopLossTag:     "atr_sl",
		ActivationPrice: 1900.0,
		CallbackPct:     1.2,
		TakeProfitVal:   80.0,
		TakeProfit:      1850.0,
		TakeProfitLimit: 1845.0,
		TakeProfitRate:  0.8,
		TakeProfitTag:   "atr_tp",
		StopBars:        6,
		ClientID:        "cli-9",
		Infos:           map[string]string{"regime": "trend"},
		Log:             true,
	}

	rv := reflect.ValueOf(orig).Elem()
	rt := rv.Type()
	for i := 0; i < rv.NumField(); i++ {
		if rv.Field(i).IsZero() {
			t.Fatalf("test fixture left EnterReq.%s at zero value; populate every field to verify Clone completeness", rt.Field(i).Name)
		}
	}

	cloned := orig.Clone()
	if !reflect.DeepEqual(orig, cloned) {
		t.Fatalf("Clone() mismatch:\ngot:  %+v\nwant: %+v", cloned, orig)
	}
	cloned.Infos["regime"] = "mutated"
	if orig.Infos["regime"] != "trend" {
		t.Fatal("Clone() shared Infos map with original")
	}

	fields := orig.GetZapFields(nil)
	hasRiskRate := false
	for _, f := range fields {
		if f.Key == "riskRate" && f.Type == zapcore.Float64Type && math.Float64frombits(uint64(f.Integer)) == 0.015 {
			hasRiskRate = true
		}
	}
	if !hasRiskRate {
		t.Fatalf("GetZapFields did not include riskRate=0.015: %+v", fields)
	}
}

func TestStratJobAccountAPIsFallbackAndDrawdown(t *testing.T) {
	var nilJob *StratJob
	if got := nilJob.AccountEquity(); got <= 0 || math.IsNaN(got) || math.IsInf(got, 0) {
		t.Fatalf("nil job AccountEquity() = %v, want finite > 0", got)
	}
	if got := nilJob.AvailableBalance(); got != 0 {
		t.Fatalf("nil job AvailableBalance() = %v, want 0", got)
	}
	if got := nilJob.AccountDrawdown(); got != 0 {
		t.Fatalf("nil job AccountDrawdown() = %v, want 0", got)
	}

	// Zero-config explicit job falls back to core.MinStakeAmount without touching legacy globals.
	emptyCore, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer emptyCore.Close()
	emptyCore.SetRunMode(core.RunModeBackTest)
	emptyClock := btime.NewClockState(true, nil)
	emptyState := NewStateWithRuntime(emptyCore, emptyClock, &config.Config{}, orm.NewSymbolState(), nil)
	zeroJob := &StratJob{Strat: &TradeStrat{Name: "zero", runtimeExplicit: true}, Account: "default"}
	zeroJob.BindRuntimeState(emptyState, emptyCore, emptyClock)
	if got := zeroJob.AccountEquity(); got != core.MinStakeAmount {
		t.Fatalf("zero-config AccountEquity() = %v, want %v", got, core.MinStakeAmount)
	}
	if got := zeroJob.AvailableBalance(); got != core.MinStakeAmount {
		t.Fatalf("zero-config AvailableBalance() = %v, want %v", got, core.MinStakeAmount)
	}
	if got := zeroJob.AccountDrawdown(); got != 0 {
		t.Fatalf("zero-config AccountDrawdown() = %v, want 0", got)
	}

	// WalletAmounts fallback before wallet initialization and during warmup.
	cfgWithWallet := &config.Config{
		StakeAmount:   200,
		WalletAmounts: map[string]float64{"USDT": 10000},
		Accounts:      map[string]*config.AccountConfig{"default": {}},
	}
	stateWithWallet := NewStateWithRuntime(emptyCore, emptyClock, cfgWithWallet, orm.NewSymbolState(), nil)
	job := &StratJob{
		Strat:   &TradeStrat{Name: "wallet_fb", runtimeConfig: cfgWithWallet, runtimeExplicit: true},
		Account: "default",
	}
	job.BindRuntimeState(stateWithWallet, emptyCore, emptyClock)
	if got := job.AccountEquity(); got != 10000 {
		t.Fatalf("uninitialized wallet AccountEquity() = %v, want 10000", got)
	}
	if got := job.AvailableBalance(); got != 10000 {
		t.Fatalf("uninitialized wallet AvailableBalance() = %v, want 10000", got)
	}

	// StakeAmount fallback when WalletAmounts is absent.
	cfgStakeOnly := &config.Config{
		StakeAmount: 350,
		Accounts:    map[string]*config.AccountConfig{"default": {}},
	}
	stateStake := NewStateWithRuntime(emptyCore, emptyClock, cfgStakeOnly, orm.NewSymbolState(), nil)
	stakeJob := &StratJob{
		Strat:   &TradeStrat{Name: "stake_fb", runtimeConfig: cfgStakeOnly, runtimeExplicit: true},
		Account: "default",
	}
	stakeJob.BindRuntimeState(stateStake, emptyCore, emptyClock)
	if got := stakeJob.AccountEquity(); got != 350 {
		t.Fatalf("stake fallback AccountEquity() = %v, want 350", got)
	}

	// Live/backtest metrics progression: compounding -> drawdown -> recovery -> depletion.
	current := AccountMetrics{Equity: 10000, Available: 8000, Valid: true}
	owner := new(int)
	if bindErr := stateWithWallet.BindAccountMetrics(owner, "default", func(string) AccountMetrics {
		return current
	}); bindErr != nil {
		t.Fatalf("BindAccountMetrics failed: %v", bindErr)
	}

	if eq, ava, dd := job.AccountEquity(), job.AvailableBalance(), job.AccountDrawdown(); eq != 10000 || ava != 8000 || dd != 0 {
		t.Fatalf("initial metrics = (%v, %v, %v), want (10000, 8000, 0)", eq, ava, dd)
	}

	// Warmup must not overwrite HWM or expose live drift; returns fallback equity and 0 drawdown.
	current = AccountMetrics{Equity: 50000, Available: 50000, Valid: true}
	job.SetWarmUp(true)
	if eq, dd := job.AccountEquity(), job.AccountDrawdown(); eq != 10000 || dd != 0 {
		t.Fatalf("warmup metrics = (%v, %v), want (10000, 0)", eq, dd)
	}
	job.SetWarmUp(false)

	// Peak rises to 12500.
	current = AccountMetrics{Equity: 12500, Available: 9500, Valid: true}
	if eq, ava, dd := job.AccountEquity(), job.AvailableBalance(), job.AccountDrawdown(); eq != 12500 || ava != 9500 || dd != 0 {
		t.Fatalf("peak metrics = (%v, %v, %v), want (12500, 9500, 0)", eq, ava, dd)
	}

	// Drawdown to 10000 -> (12500 - 10000) / 12500 = 0.20.
	current = AccountMetrics{Equity: 10000, Available: 7000, Valid: true}
	if dd := job.AccountDrawdown(); math.Abs(dd-0.20) > 1e-12 {
		t.Fatalf("AccountDrawdown() = %.12f, want 0.20", dd)
	}

	// Another job on the same runtime & account shares the same High-Water Mark.
	peerJob := &StratJob{
		Strat:   &TradeStrat{Name: "peer", runtimeConfig: cfgWithWallet, runtimeExplicit: true},
		Account: "default",
	}
	peerJob.BindRuntimeState(stateWithWallet, emptyCore, emptyClock)
	if dd := peerJob.AccountDrawdown(); math.Abs(dd-0.20) > 1e-12 {
		t.Fatalf("peer job AccountDrawdown() = %.12f, want 0.20", dd)
	}

	// Depleted account (equity <= 0 after positive peak): positive dust equity, 0 available, 1.0 drawdown.
	current = AccountMetrics{Equity: -25, Available: -10, Valid: true}
	if eq := job.AccountEquity(); eq != core.AmtDust {
		t.Fatalf("depleted AccountEquity() = %v, want %v (positive dust)", eq, core.AmtDust)
	}
	if ava := job.AvailableBalance(); ava != 0 {
		t.Fatalf("depleted AvailableBalance() = %v, want 0", ava)
	}
	if dd := job.AccountDrawdown(); dd != 1 {
		t.Fatalf("depleted AccountDrawdown() = %v, want 1", dd)
	}
}

func newRiskSizingTestJob(t *testing.T, market string, cfg *config.Config, equity float64, closePrice, volume float64) (*StratJob, *AccountMetrics) {
	t.Helper()
	coreState, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coreState.Close)
	coreState.SetRunMode(core.RunModeBackTest)
	coreState.Market = market
	coreState.IsContract = banexg.IsContract(market)

	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(1_700_000_060_000)

	symbolName := "ETH/USDT:USDT"
	if market == banexg.MarketSpot {
		symbolName = "ETH/USDT"
	}
	exs := &orm.ExSymbol{ID: 1, Exchange: "binance", Market: market, Symbol: symbolName}
	symbols := orm.NewSymbolState()
	if setErr := symbols.SetExSymbols([]*orm.ExSymbol{exs}); setErr != nil {
		t.Fatal(setErr)
	}

	prices := com.NewPriceState("binance")
	prices.SetBarPriceAt(clock.TimeMS(), symbolName, closePrice)

	env, envErr := ta.NewBarEnv("binance", market, symbolName, "1h")
	if envErr != nil {
		t.Fatal(envErr)
	}
	for i := 0; i < 5; i++ {
		ts := clock.TimeMS() - int64(5-i)*3600_000
		if bErr := env.OnBar(ts, closePrice, closePrice*1.01, closePrice*0.99, closePrice, volume, closePrice*volume, volume*0.5, 100); bErr != nil {
			t.Fatal(bErr)
		}
	}

	state := NewStateWithRuntime(coreState, clock, cfg, symbols, nil)
	metrics := &AccountMetrics{Equity: equity, Available: equity, Valid: true}
	owner := new(int)
	if bindErr := state.BindAccountMetrics(owner, "default", func(string) AccountMetrics {
		return *metrics
	}); bindErr != nil {
		t.Fatal(bindErr)
	}

	stgy := &TradeStrat{
		Name:            "test:risk",
		runtimeConfig:   cfg,
		runtimeCore:     coreState,
		runtimeClock:    clock,
		runtimeExplicit: true,
	}
	job := &StratJob{
		Strat:         stgy,
		Env:           env,
		DataHub:       NewDataHub(),
		Symbol:        exs,
		TimeFrame:     "1h",
		Account:       "default",
		CloseLong:     true,
		CloseShort:    true,
		ExgStopLoss:   true,
		ExgTakeProfit: true,
		symbols:       symbols,
	}
	job.BindRuntimeState(state, coreState, clock)
	job.BindRuntimeMarket(prices, clock)
	return job, metrics
}

func TestOpenOrderRiskRateSizingAndSafetyGates(t *testing.T) {
	baseCfg := &config.Config{
		StakeAmount: 100,
		Leverage:    5,
		OpenVolRate: 1,
		Accounts:    map[string]*config.AccountConfig{"default": {Leverage: 5}},
	}
	job, metrics := newRiskSizingTestJob(t, banexg.MarketLinear, baseCfg, 10000, 2000, 1000)

	// 1) Long ATR StopLossVal: equity=10000, risk=1% ($100), price=2000, SL distance=80 (4%) => LegalCost=2500, SL=1920.
	reqLong := &EnterReq{Tag: "long_atr", RiskRate: 0.01, StopLossVal: 80}
	if err := job.OpenOrder(reqLong); err != nil {
		t.Fatalf("OpenOrder long_atr failed: %v", err)
	}
	if math.Abs(reqLong.LegalCost-2500) > 1e-9 || math.Abs(reqLong.StopLoss-1920) > 1e-9 {
		t.Fatalf("long_atr LegalCost=%v StopLoss=%v, want 2500 and 1920", reqLong.LegalCost, reqLong.StopLoss)
	}

	// 2) Compounding: equity grows to 15000 => LegalCost scales to 3750.
	metrics.Equity = 15000
	reqCompound := &EnterReq{Tag: "long_compound", RiskRate: 0.01, StopLossVal: 80}
	if err := job.OpenOrder(reqCompound); err != nil {
		t.Fatalf("OpenOrder long_compound failed: %v", err)
	}
	if math.Abs(reqCompound.LegalCost-3750) > 1e-9 {
		t.Fatalf("compounded LegalCost=%v, want 3750", reqCompound.LegalCost)
	}
	metrics.Equity = 10000

	// 3) Short with StopLossVal=100 (5% of 2000), risk=1% ($100) => LegalCost=2000, SL=2100.
	reqShort := &EnterReq{Tag: "short_atr", Short: true, RiskRate: 0.01, StopLossVal: 100}
	if err := job.OpenOrder(reqShort); err != nil {
		t.Fatalf("OpenOrder short_atr failed: %v", err)
	}
	if math.Abs(reqShort.LegalCost-2000) > 1e-9 || math.Abs(reqShort.StopLoss-2100) > 1e-9 {
		t.Fatalf("short_atr LegalCost=%v StopLoss=%v, want 2000 and 2100", reqShort.LegalCost, reqShort.StopLoss)
	}

	// 4) Limit entry + explicit StopLoss price: Limit=1950 (<2000), StopLoss=1872 (4% of 1950) => LegalCost=2500.
	reqLimit := &EnterReq{Tag: "limit_sl", Limit: 1950, StopLoss: 1872, RiskRate: 0.01}
	if err := job.OpenOrder(reqLimit); err != nil {
		t.Fatalf("OpenOrder limit_sl failed: %v", err)
	}
	if math.Abs(reqLimit.LegalCost-2500) > 1e-9 || math.Abs(reqLimit.StopLoss-1872) > 1e-9 {
		t.Fatalf("limit_sl LegalCost=%v StopLoss=%v, want 2500 and 1872", reqLimit.LegalCost, reqLimit.StopLoss)
	}

	// 5) Strategy default StopLoss (2%) + RiskRate (1%) => LegalCost = 100 / 0.02 = 5000.
	job.Strat.StopLoss = 0.02
	reqStratSL := &EnterReq{Tag: "strat_sl", RiskRate: 0.01}
	if err := job.OpenOrder(reqStratSL); err != nil {
		t.Fatalf("OpenOrder strat_sl failed: %v", err)
	}
	if math.Abs(reqStratSL.LegalCost-5000) > 1e-9 {
		t.Fatalf("strat_sl LegalCost=%v, want 5000", reqStratSL.LegalCost)
	}
	job.Strat.StopLoss = 0

	// 6) Explicit Amount overrides RiskRate.
	reqAmt := &EnterReq{Tag: "explicit_amt", Amount: 0.75, RiskRate: 0.01, StopLossVal: 80}
	if err := job.OpenOrder(reqAmt); err != nil {
		t.Fatalf("OpenOrder explicit_amt failed: %v", err)
	}
	if reqAmt.Amount != 0.75 || reqAmt.LegalCost != 0 {
		t.Fatalf("explicit Amount altered: Amount=%v LegalCost=%v", reqAmt.Amount, reqAmt.LegalCost)
	}

	// 7) RiskRate without any stop-loss falls back to StakeAmount * CostRate (100 * 1 = 100).
	reqNoSL := &EnterReq{Tag: "no_sl_fallback", RiskRate: 0.01}
	if err := job.OpenOrder(reqNoSL); err != nil {
		t.Fatalf("OpenOrder no_sl_fallback failed: %v", err)
	}
	if math.Abs(reqNoSL.LegalCost-100) > 1e-9 {
		t.Fatalf("no_sl_fallback LegalCost=%v, want 100", reqNoSL.LegalCost)
	}

	// 8) Max leverage cap: StopLossVal=1 (0.05%) => raw LegalCost=200,000, capped at equity * leverage = 50,000 (or req.Leverage=3 => 30,000).
	reqLvgCap := &EnterReq{Tag: "lvg_cap", Leverage: 3, RiskRate: 0.01, StopLossVal: 1}
	if err := job.OpenOrder(reqLvgCap); err != nil {
		t.Fatalf("OpenOrder lvg_cap failed: %v", err)
	}
	if math.Abs(reqLvgCap.LegalCost-30000) > 1e-9 {
		t.Fatalf("lvg_cap LegalCost=%v, want 30000", reqLvgCap.LegalCost)
	}

	// Spot leverage cap is 1x equity (10000).
	spotJob, _ := newRiskSizingTestJob(t, banexg.MarketSpot, baseCfg, 10000, 2000, 1000)
	reqSpotCap := &EnterReq{Tag: "spot_cap", RiskRate: 0.01, StopLossVal: 1}
	if err := spotJob.OpenOrder(reqSpotCap); err != nil {
		t.Fatalf("OpenOrder spot_cap failed: %v", err)
	}
	if math.Abs(reqSpotCap.LegalCost-10000) > 1e-9 {
		t.Fatalf("spot_cap LegalCost=%v, want 10000", reqSpotCap.LegalCost)
	}

	// 9) max_stake_amt cap.
	cappedCfg := &config.Config{
		StakeAmount: 100,
		Leverage:    5,
		MaxStakeAmt: 1800,
		OpenVolRate: 1,
		Accounts:    map[string]*config.AccountConfig{"default": {Leverage: 5}},
	}
	maxStakeJob, _ := newRiskSizingTestJob(t, banexg.MarketLinear, cappedCfg, 10000, 2000, 1000)
	reqMaxStake := &EnterReq{Tag: "max_stake_cap", RiskRate: 0.01, StopLossVal: 80}
	if err := maxStakeJob.OpenOrder(reqMaxStake); err != nil {
		t.Fatalf("OpenOrder max_stake_cap failed: %v", err)
	}
	if math.Abs(reqMaxStake.LegalCost-1800) > 1e-9 {
		t.Fatalf("max_stake_cap LegalCost=%v, want 1800", reqMaxStake.LegalCost)
	}

	// 10) open_vol_rate cap: avgVol=0.4 ETH, openVolRate=1.0, price=2000 => max notional=800.
	lowVolJob, _ := newRiskSizingTestJob(t, banexg.MarketLinear, baseCfg, 10000, 2000, 0.4)
	reqVolCap := &EnterReq{Tag: "vol_cap", RiskRate: 0.01, StopLossVal: 80}
	if err := lowVolJob.OpenOrder(reqVolCap); err != nil {
		t.Fatalf("OpenOrder vol_cap failed: %v", err)
	}
	if math.Abs(reqVolCap.LegalCost-800) > 1e-9 {
		t.Fatalf("vol_cap LegalCost=%v, want 800", reqVolCap.LegalCost)
	}

	// 11) MinStakeAmount gate: tiny equity=100, RiskRate=0.001, SL=10% => LegalCost=1 < 5.5 => rejected.
	tinyJob, _ := newRiskSizingTestJob(t, banexg.MarketLinear, baseCfg, 100, 2000, 1000)
	if err := tinyJob.OpenOrder(&EnterReq{Tag: "too_small", RiskRate: 0.001, StopLossVal: 200}); err == nil {
		t.Fatal("expected MinStakeAmount rejection for tiny risk-sized cost")
	}

	// 12) Invalid RiskRate and invalid stop-loss direction rejected.
	for _, badRate := range []float64{-0.01, 1.01, math.NaN(), math.Inf(1)} {
		if err := job.OpenOrder(&EnterReq{Tag: "bad_rate", RiskRate: badRate, StopLossVal: 80}); err == nil {
			t.Fatalf("expected error for invalid RiskRate=%v", badRate)
		}
	}
	if err := job.OpenOrder(&EnterReq{Tag: "bad_sl_dir", RiskRate: 0.01, StopLoss: 2100}); err == nil {
		t.Fatal("expected error for long StopLoss above entry price")
	}
}
