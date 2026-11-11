package opt

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm/ormo"
)

func TestOnLiquidationRecordsAndKeepsContextAlive(t *testing.T) {
	originalChargeOnBomb := config.ChargeOnBomb
	t.Cleanup(func() { config.ChargeOnBomb = originalChargeOnBomb })
	config.ChargeOnBomb = false
	for _, isOpt := range []bool{false, true} {
		bt := &BackTestLite{isOpt: isOpt, dp: data.NewHistProvider(nil, nil, nil, false, nil)}
		bt.onLiquidation("BTC/USDT:USDT")
		first := bt.liquidation
		if first == nil || first.symbol != "BTC/USDT:USDT" || !strings.Contains(first.reason, "BOMB") {
			t.Fatalf("isOpt=%v liquidation = %+v", isOpt, first)
		}
		// later symbols of the same bar must not replace the first record
		bt.onLiquidation("ETH/USDT:USDT")
		if bt.liquidation != first {
			t.Fatalf("isOpt=%v: liquidation record was replaced", isOpt)
		}
		if bt.runErr != nil || bt.resolveLoopError(nil) != nil {
			t.Fatalf("isOpt=%v: liquidation must not be a run error: %v", isOpt, bt.runErr)
		}
	}
}

func TestRelabelLiquidationExits(t *testing.T) {
	mk := func(tag string) *ormo.InOutOrder {
		od := &ormo.InOutOrder{IOrder: &ormo.IOrder{}}
		od.ExitTag = tag
		return od
	}
	ods := []*ormo.InOutOrder{mk(core.ExitTagBotStop), mk(core.ExitTagStopLoss), mk("delist")}
	relabelLiquidationExits(ods)
	if ods[0].ExitTag != core.ExitTagLiquidation || ods[1].ExitTag != core.ExitTagStopLoss || ods[2].ExitTag != "delist" {
		t.Fatalf("tags = %v %v %v", ods[0].ExitTag, ods[1].ExitTag, ods[2].ExitTag)
	}
}

func TestApplyLiquidationZeroEquityMetrics(t *testing.T) {
	const day = int64(86400000)
	deps := legacyReplayReportDeps()
	start := int64(1700006400000)
	deps.Clock.SetTimeMS(start)
	deps.Market.Prices.SetPricesAt(start, map[string]float64{"USDT": 1}, "")
	wallet := biz.InitFakeWalletsWithRuntimeDeps(deps.bizRuntimeDeps())
	wallet.Items = map[string]*biz.ItemWallet{"USDT": {Coin: "USDT", Available: 1000}}
	r := NewBTResult()
	r.reportDeps = deps
	r.TotalInvest = 1000
	r.StartMS = start
	r.PlotEvery = 1
	for i := int64(0); i < 10; i++ {
		wallet.Items["USDT"].Available = 1000 + float64(i)*10
		r.TimeNum++
		r.logState(start, start+i*day, 0)
		r.logPlot(wallet, start+i*day, 0, -1)
	}
	liqMS := start + 9*day
	endMS := start + 40*day
	r.EndMS = endMS
	wallet.Items["USDT"].Available = 3
	r.applyLiquidation(wallet, liqMS, "BTC/USDT:USDT", "wallet BTC/USDT:USDT BOMB at x, exit")
	r.collectUlcer()
	sharpe, sortino, err := CalcMeasuresByReal(r.Plots.Real, (endMS-start)/1000, "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	r.SharpeRatio, r.SortinoRatio = sharpe, sortino
	if !r.Liquidated || r.LiquidatedAtMS != liqMS || r.LiquidatedSymbol != "BTC/USDT:USDT" {
		t.Fatalf("flags %+v", r)
	}
	if r.MaxDrawDownPct != 100 || r.MinReal != 0 {
		t.Fatalf("drawdown %v min %v", r.MaxDrawDownPct, r.MinReal)
	}
	n := len(r.Plots.Real)
	if r.Plots.Real[n-1] != 0 || r.Plots.Real[n-2] != 0 || r.Plots.Available[n-1] != 0 || r.lastPlotMS != endMS {
		t.Fatalf("tail %v last=%d", r.Plots.Real[n-3:], r.lastPlotMS)
	}
	if r.UlcerIndex == nil || r.UlcerPerformanceIndex == nil || *r.UlcerPerformanceIndex >= 0 {
		t.Fatalf("ulcer %v upi %v", r.UlcerIndex, r.UlcerPerformanceIndex)
	}
	last := r.DailyEquity[len(r.DailyEquity)-1]
	if last[0] != float64(endMS) && last[1] != 0 {
		t.Fatalf("daily equity tail %v", last)
	}
	for _, v := range []float64{*r.UlcerIndex, *r.UlcerPerformanceIndex, r.SharpeRatio, r.SortinoRatio} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			t.Fatalf("non finite metric: %v %v %v %v", *r.UlcerIndex, *r.UlcerPerformanceIndex, r.SharpeRatio, r.SortinoRatio)
		}
	}
	// JSON contract: snake_case keys, only when liquidated
	raw, _ := json.Marshal(r)
	for _, key := range []string{`"liquidated":true`, `"liquidated_at_ms":`, `"liquidated_symbol":"BTC/USDT:USDT"`, `"liquidated_reason":`} {
		if !strings.Contains(string(raw), key) {
			t.Fatalf("detail json lacks %s", key)
		}
	}
	plain, _ := json.Marshal(NewBTResult())
	if strings.Contains(string(plain), "liquidated") {
		t.Fatal("a normal result must not carry liquidation keys")
	}
}
