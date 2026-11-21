package strat

import (
	"math"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
)

func TestOpenOrderStopLimitValidationAndPricing(t *testing.T) {
	cfg := &config.Config{
		StakeAmount: 100,
		Leverage:    5,
		OpenVolRate: 1,
		Accounts:    map[string]*config.AccountConfig{"default": {Leverage: 5}},
	}
	job, _ := newRiskSizingTestJob(t, banexg.MarketLinear, cfg, 10000, 2000, 1000)

	// Long breakout Stop-Limit: Stop=2050, Limit=2060, SL=2010 (< Stop=2050 even though > curPrice=2000).
	reqLong := &EnterReq{Tag: "sl_long", Stop: 2050, Limit: 2060, StopLoss: 2010}
	if err := job.OpenOrder(reqLong); err != nil {
		t.Fatalf("long Stop-Limit OpenOrder failed: %v", err)
	}
	if reqLong.Stop != 2050 || reqLong.Limit != 2060 || reqLong.OrderType != core.OrderTypeLimit || reqLong.StopLoss != 2010 {
		t.Fatalf("unexpected normalized long Stop-Limit: %+v", reqLong)
	}

	// Short breakout Stop-Limit: Stop=1950, Limit=1940, SL=1990 (> Stop=1950 even though < curPrice=2000).
	reqShort := &EnterReq{Tag: "sl_short", Short: true, Stop: 1950, Limit: 1940, StopLoss: 1990}
	if err := job.OpenOrder(reqShort); err != nil {
		t.Fatalf("short Stop-Limit OpenOrder failed: %v", err)
	}
	if reqShort.Stop != 1950 || reqShort.Limit != 1940 || reqShort.OrderType != core.OrderTypeLimit || reqShort.StopLoss != 1990 {
		t.Fatalf("unexpected normalized short Stop-Limit: %+v", reqShort)
	}

	// Explicit Market OrderType with Limit must be rejected.
	if err := job.OpenOrder(&EnterReq{Tag: "bad_mkt_lim", OrderType: core.OrderTypeMarket, Stop: 2050, Limit: 2060}); err == nil {
		t.Fatal("expected error when Limit > 0 on Market order")
	}
}

func TestOpenOrderCallbackPctWidenedRange(t *testing.T) {
	cfg := &config.Config{
		StakeAmount: 100,
		Leverage:    5,
		OpenVolRate: 1,
		Accounts:    map[string]*config.AccountConfig{"default": {Leverage: 5}},
	}
	job, _ := newRiskSizingTestJob(t, banexg.MarketLinear, cfg, 10000, 2000, 1000)

	for _, validPct := range []float64{0.05, 0.08, 1.5, 25.0, 50.0} {
		req := &EnterReq{Tag: "trail_ok", CallbackPct: validPct}
		if err := job.OpenOrder(req); err != nil {
			t.Fatalf("CallbackPct=%v rejected: %v", validPct, err)
		}
	}
	for _, invalidPct := range []float64{-0.1, 0.04, 50.01, math.NaN(), math.Inf(1)} {
		req := &EnterReq{Tag: "trail_bad", CallbackPct: invalidPct}
		if err := job.OpenOrder(req); err == nil {
			t.Fatalf("CallbackPct=%v should be rejected", invalidPct)
		}
	}
}

func TestOpenOrderMultiTakeProfitsValidation(t *testing.T) {
	cfg := &config.Config{
		StakeAmount: 100,
		Leverage:    5,
		OpenVolRate: 1,
		Accounts:    map[string]*config.AccountConfig{"default": {Leverage: 5}},
	}
	job, _ := newRiskSizingTestJob(t, banexg.MarketLinear, cfg, 10000, 2000, 1000)

	req := &EnterReq{
		Tag: "multi_tp",
		TakeProfits: []TakeProfitTarget{
			{Rate: 0.02, ExitRate: 0.5, MoveSLToBreakeven: true, Tag: "tp1"},
			{Val: 100, ExitRate: 0.5, Tag: "tp2"},
		},
	}
	if err := job.OpenOrder(req); err != nil {
		t.Fatalf("valid multi-TP rejected: %v", err)
	}

	// Sum of ExitRate > 1 rejected.
	if err := job.OpenOrder(&EnterReq{
		Tag: "bad_sum",
		TakeProfits: []TakeProfitTarget{
			{Rate: 0.02, ExitRate: 0.6},
			{Rate: 0.04, ExitRate: 0.5},
		},
	}); err == nil {
		t.Fatal("expected error when TakeProfits ExitRate sum > 1")
	}

	// Combining single TakeProfit and TakeProfits rejected.
	if err := job.OpenOrder(&EnterReq{
		Tag:        "both_tp",
		TakeProfit: 2100,
		TakeProfits: []TakeProfitTarget{
			{Price: 2200, ExitRate: 1},
		},
	}); err == nil {
		t.Fatal("expected error combining TakeProfit and TakeProfits")
	}

	// Wrong direction Price in TakeProfits rejected.
	if err := job.OpenOrder(&EnterReq{
		Tag: "bad_tp_dir",
		TakeProfits: []TakeProfitTarget{
			{Price: 1900, ExitRate: 1},
		},
	}); err == nil {
		t.Fatal("expected error for long TakeProfits price below entry")
	}
}

func TestOpenOrderLongShortTPPriceAndExgStopLossLocalFlag(t *testing.T) {
	cfg := &config.Config{
		StakeAmount: 100,
		Leverage:    5,
		OpenVolRate: 1,
		Accounts:    map[string]*config.AccountConfig{"default": {Leverage: 5}},
	}
	job, _ := newRiskSizingTestJob(t, banexg.MarketLinear, cfg, 10000, 2000, 1000)
	job.LongTPPrice = 2200
	job.ShortTPPrice = 1800

	reqLong := &EnterReq{Tag: "job_long_tp"}
	if err := job.OpenOrder(reqLong); err != nil {
		t.Fatalf("LongTPPrice OpenOrder failed: %v", err)
	}
	if reqLong.TakeProfit != 2200 {
		t.Fatalf("LongTPPrice not wired: got %v, want 2200", reqLong.TakeProfit)
	}

	reqShort := &EnterReq{Tag: "job_short_tp", Short: true}
	if err := job.OpenOrder(reqShort); err != nil {
		t.Fatalf("ShortTPPrice OpenOrder failed: %v", err)
	}
	if reqShort.TakeProfit != 1800 {
		t.Fatalf("ShortTPPrice not wired: got %v, want 1800", reqShort.TakeProfit)
	}

	// Explicit TakeProfitVal overrides LongTPPrice.
	reqOverride := &EnterReq{Tag: "override_tp", TakeProfitVal: 150}
	if err := job.OpenOrder(reqOverride); err != nil {
		t.Fatalf("TakeProfitVal override failed: %v", err)
	}
	if reqOverride.TakeProfit != 2150 {
		t.Fatalf("TakeProfitVal override got %v, want 2150", reqOverride.TakeProfit)
	}

	// ExgStopLoss=false preserves req.StopLoss and marks OdInfoLocalStopLoss="1", while StopLoss==0 leaves it unset.
	job.ExgStopLoss = false
	job.LongTPPrice = 0
	reqLocalSL := &EnterReq{Tag: "local_sl", StopLoss: 1900}
	if err := job.OpenOrder(reqLocalSL); err != nil {
		t.Fatalf("ExgStopLoss=false OpenOrder failed: %v", err)
	}
	if reqLocalSL.StopLoss != 1900 || reqLocalSL.Infos[ormo.OdInfoLocalStopLoss] != "1" {
		t.Fatalf("ExgStopLoss=false should keep StopLoss=1900 with LocalStopLoss=1, got SL=%v Infos=%+v", reqLocalSL.StopLoss, reqLocalSL.Infos)
	}

	reqNoSL := &EnterReq{Tag: "no_sl"}
	if err := job.OpenOrder(reqNoSL); err != nil {
		t.Fatalf("no_sl OpenOrder failed: %v", err)
	}
	if reqNoSL.StopLoss != 0 || reqNoSL.Infos[ormo.OdInfoLocalStopLoss] != "" {
		t.Fatalf("StopLoss==0 should remain unset, got SL=%v Infos=%+v", reqNoSL.StopLoss, reqNoSL.Infos)
	}
}

func TestInOutOrderClonePreservesOCOGroupAndTPLadder(t *testing.T) {
	od := &ormo.InOutOrder{
		IOrder:   &ormo.IOrder{ID: 7, Symbol: "BTC/USDT:USDT", Strategy: "s"},
		Enter:    &ormo.ExOrder{Enter: true, Amount: 2, Filled: 2, Average: 100},
		Info:     map[string]interface{}{},
		OCOGroup: "oco-alpha",
	}
	od.SetTakeProfits([]*ormo.TPLadderRung{
		{Rate: 0.05, ExitRate: 0.5, MoveSLToBreakeven: true, Tag: "tp1"},
		{Rate: 0.02, ExitRate: 0.5, Tag: "tp0"},
	})
	od.ResolveTakeProfits(100)

	cloned := od.Clone()
	if cloned.OCOGroup != "oco-alpha" {
		t.Fatalf("cloned OCOGroup = %q, want oco-alpha", cloned.OCOGroup)
	}
	ladder := cloned.GetTakeProfits()
	if ladder == nil || len(ladder.Rungs) != 2 || math.Abs(ladder.Rungs[0].Price-102) > 1e-9 || math.Abs(ladder.Rungs[1].Price-105) > 1e-9 {
		t.Fatalf("resolved & sorted ladder mismatch: %+v", ladder)
	}
	ladder.Rungs[0].Done = true
	if od.GetTakeProfits().Rungs[0].Done {
		t.Fatal("InOutOrder.Clone() aliased TPLadderState rungs")
	}
}
