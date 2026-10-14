package opt

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/utils"
)

func TestReportUlcerDoesNotDoubleCountWithdrawals(t *testing.T) {
	deps := legacyReplayReportDeps()
	start := int64(1700006400000)
	deps.Clock.SetTimeMS(start)
	deps.Market.Prices.SetPricesAt(start, map[string]float64{"USDT": 1}, "")
	wallet := biz.InitFakeWalletsWithRuntimeDeps(deps.bizRuntimeDeps())
	wallet.Items = map[string]*biz.ItemWallet{"USDT": {Coin: "USDT", Available: 100}}
	r := NewBTResult()
	r.reportDeps = deps
	r.TotalInvest = 100
	r.StartMS = start
	r.EndMS = start + 86400000
	r.logPlot(wallet, start, 0, -1)
	wallet.Items["USDT"].Available = 60
	wallet.Items["USDT"].Withdraw = 40
	r.logPlot(wallet, start+86400000, 0, -1)
	r.collectUlcer()
	if r.UlcerIndex == nil || *r.UlcerIndex != 0 || r.UlcerPerformanceIndex != nil || r.ulcerPending != 100 {
		t.Fatalf("withdrawal changed metrics %+v", r)
	}
}

func TestReportUlcerDailySamplingAndBaseline(t *testing.T) {
	const day = int64(86400000)
	start := int64(1700006400000)
	r := NewBTResult()
	r.TotalInvest = 100
	r.StartMS = start
	r.EndMS = start + 3*day
	r.recordUlcer(start+1, 105)
	r.recordUlcer(start+2, 90)
	r.recordUlcer(start+day+1, 95)
	r.recordUlcer(start+2*day+1, 105)
	r.recordUlcer(start+3*day, 105)
	r.Plots.Real = []float64{100, 105} // chart decimation hides both drawdowns
	r.collectUlcer()
	wantUI, wantUPI, _ := utils.CalcUlcerPerformance([]float64{100, 90, 95, 105}, 3.0/utils.UlcerYearDays, 0)
	if r.UlcerIndex == nil || r.UlcerPerformanceIndex == nil || math.Abs(*r.UlcerIndex-*wantUI) > 1e-10 || math.Abs(*r.UlcerPerformanceIndex-*wantUPI) > 1e-8 || r.UlcerSource != "daily_equity" {
		t.Fatalf("metrics %+v", r)
	}
}

func TestReportUlcerCarriesMissingDays(t *testing.T) {
	const day = int64(86400000)
	start := int64(1700006400000)
	r := NewBTResult()
	r.TotalInvest = 100
	r.StartMS = start
	r.EndMS = start + 3*day
	r.recordUlcer(start+1, 90)
	r.recordUlcer(start+3*day, 100)
	r.collectUlcer()
	want, _, _ := utils.CalcUlcerPerformance([]float64{100, 90, 90, 100}, 3.0/utils.UlcerYearDays, 0)
	if r.UlcerIndex == nil || math.Abs(*r.UlcerIndex-*want) > 1e-10 {
		t.Fatalf("missing day UI=%v", r.UlcerIndex)
	}
}

func TestReportUlcerDoesNotEstimateHistoricalCharts(t *testing.T) {
	r := NewBTResult()
	r.TotalInvest = 100
	r.StartMS = 1700006400000
	r.EndMS = r.StartMS + 86400000
	r.Plots.Real = []float64{100, 90, 105}
	r.collectUlcer()
	if r.UlcerIndex != nil || r.UlcerPerformanceIndex != nil {
		t.Fatal("historical charts should not invent full-resolution metrics")
	}
}

func TestReportUlcerJSONAndOptimizationRoundTrip(t *testing.T) {
	ui, upi, _ := utils.CalcUlcerPerformance([]float64{100, 90, 95, 105}, 1, 0)
	r := &BTResult{UlcerIndex: ui, UlcerPerformanceIndex: upi}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"ulcerIndex":`) {
		t.Fatal(string(data))
	}
	var parsed BTResult
	if err = json.Unmarshal(data, &parsed); err != nil {
		t.Fatal(err)
	}
	opt := OptInfo{BTResult: &BTResult{}}
	parseOptMetrics(r.BriefLine(), &opt)
	if opt.UlcerIndex == nil || opt.UlcerPerformanceIndex == nil || math.Abs(*opt.UlcerIndex-*ui) > .001 || math.Abs(*opt.UlcerPerformanceIndex-*upi) > .001 {
		t.Fatalf("roundtrip %+v", opt)
	}
	old := OptInfo{BTResult: &BTResult{}}
	parseOptMetrics("sharpe: 0.4, odNum: 12", &old)
	if old.UlcerIndex != nil || old.UlcerPerformanceIndex != nil {
		t.Fatal("old metrics must remain missing")
	}
	undefined := OptInfo{BTResult: &BTResult{}}
	parseOptMetrics("ulcerIndex: 0.000%, upi: N/A", &undefined)
	if undefined.UlcerIndex == nil || undefined.UlcerPerformanceIndex != nil {
		t.Fatal("zero UI is defined, UPI is not")
	}
}
