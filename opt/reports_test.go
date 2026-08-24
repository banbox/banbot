package opt

import (
	"strconv"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/utils"
)

func TestLogStateUsesMonotonicEventTimeForPlots(t *testing.T) {
	const (
		startMS  = int64(1700000000000)
		firstMS  = int64(1700000003000)
		secondMS = int64(1700000004000)
	)
	result := NewBTResult()
	wallets := &biz.BanWallets{Items: map[string]*biz.ItemWallet{}}

	result.logState(startMS, firstMS, 2)
	result.TimeNum++
	result.logState(startMS-1000, secondMS, 1)

	if result.StartMS != startMS {
		t.Fatalf("StartMS = %d, want %d", result.StartMS, startMS)
	}
	if result.EndMS != secondMS {
		t.Fatalf("EndMS = %d, want %d", result.EndMS, secondMS)
	}
	wantLabels := []string{
		btime.ToDateStr(firstMS, ""),
		btime.ToDateStr(secondMS, ""),
	}
	if len(result.Plots.Labels) != len(wantLabels) {
		t.Fatalf("labels = %v, want %v", result.Plots.Labels, wantLabels)
	}
	for i, want := range wantLabels {
		if result.Plots.Labels[i] != want {
			t.Fatalf("label[%d] = %q, want %q", i, result.Plots.Labels[i], want)
		}
	}
	assertPlotLengths(t, result.Plots, 2)

	result.logPlot(wallets, secondMS, 0, 10)
	if len(result.Plots.Labels) != 2 {
		t.Fatalf("equal terminal timestamp appended a label: %v", result.Plots.Labels)
	}
	if result.Plots.Real[1] != 10 || result.Plots.OdNum[1] != 0 {
		t.Fatalf("terminal plot = real %v orders %v, want 10 and 0", result.Plots.Real[1], result.Plots.OdNum[1])
	}
	assertPlotLengths(t, result.Plots, 2)

	result.logPlot(wallets, firstMS+500, 9, 99)
	if result.Plots.Real[1] != 10 || result.Plots.OdNum[1] != 0 {
		t.Fatalf("backwards plot changed terminal state: real %v orders %v", result.Plots.Real[1], result.Plots.OdNum[1])
	}
	assertPlotLengths(t, result.Plots, 2)
}

func TestNormalizeBacktestResultRangeUsesConfiguredWindow(t *testing.T) {
	previous := config.TimeRange
	t.Cleanup(func() { config.TimeRange = previous })
	config.TimeRange = &config.TimeTuple{StartMS: 1_651_363_200_000, EndMS: 1_786_492_800_000}

	result := &BTResult{StartMS: config.TimeRange.EndMS, EndMS: config.TimeRange.EndMS}
	normalizeBacktestResultRange(result)

	if result.StartMS != config.TimeRange.StartMS || result.EndMS != config.TimeRange.EndMS {
		t.Fatalf("result range = %d-%d, want configured range %d-%d",
			result.StartMS, result.EndMS, config.TimeRange.StartMS, config.TimeRange.EndMS)
	}
}

func assertPlotLengths(t *testing.T, plots *PlotData, want int) {
	t.Helper()
	lengths := map[string]int{
		"labels": len(plots.Labels), "orders": len(plots.OdNum), "jobs": len(plots.JobNum),
		"real": len(plots.Real), "available": len(plots.Available), "profit": len(plots.Profit),
		"unrealized": len(plots.UnrealizedPOL), "withdraw": len(plots.WithDraw),
	}
	for name, got := range lengths {
		if got != want {
			t.Fatalf("%s length = %d, want %d", name, got, want)
		}
	}
}

func TestGroupByProfitsHandlesEmptyKMeansClusters(t *testing.T) {
	const orderCount = 10
	orders := make([]*ormo.InOutOrder, orderCount)
	rates := make([]float64, orderCount)
	for i := range orders {
		orders[i] = &ormo.InOutOrder{
			IOrder: &ormo.IOrder{
				ProfitRate: 0.01,
				Profit:     1,
				Leverage:   1,
			},
			Enter: &ormo.ExOrder{Average: 1, Filled: 1},
		}
		rates[i] = orders[i].ProfitRate
	}

	clusters := utils.KMeansVals(rates, 4)
	hasEmptyCluster := false
	for _, cluster := range clusters.Clusters {
		hasEmptyCluster = hasEmptyCluster || len(cluster.Items) == 0
	}
	if !hasEmptyCluster {
		t.Fatal("expected duplicate profit rates to produce an empty KMeans cluster")
	}

	result := &BTResult{}
	result.groupByProfits(orders)
	if len(result.ProfitGrps) != 1 {
		t.Fatalf("profit groups = %d, want 1", len(result.ProfitGrps))
	}
}

func TestCalcMeasuresByCurveEmpty(t *testing.T) {
	sharpe, sortino, err := calcMeasuresByCurve(nil, 86400)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sharpe != 0 || sortino != 0 {
		t.Fatalf("got sharpe=%v sortino=%v, want 0, 0", sharpe, sortino)
	}
}

func TestReportReplayOrderMakesConstrainedAdmissionDeterministic(t *testing.T) {
	orders := []*ormo.InOutOrder{
		{IOrder: &ormo.IOrder{ID: 1, EnterAt: 1000}},
		{IOrder: &ormo.IOrder{ID: 2, EnterAt: 1000}},
		{IOrder: &ormo.IOrder{ID: 3, EnterAt: 1000}},
	}
	costs := map[int64]float64{1: 60, 2: 50, 3: 40}
	for _, input := range [][]*ormo.InOutOrder{
		orders,
		{orders[2], orders[1], orders[0]},
		{orders[1], orders[2], orders[0]},
	} {
		wallets := &biz.BanWallets{Items: map[string]*biz.ItemWallet{
			"USDT": {Available: 100, Pendings: map[string]float64{}, Frozens: map[string]float64{}},
		}}
		inputIDs := []int64{input[0].ID, input[1].ID, input[2].ID}
		var admitted []int64
		ordered := reportReplayOrder(input)
		for _, od := range ordered {
			if _, err := wallets.CostAva(strconv.FormatInt(od.ID, 10), "USDT", costs[od.ID], false, 0.9); err == nil {
				admitted = append(admitted, od.ID)
			}
		}
		if len(admitted) != 2 || admitted[0] != 1 || admitted[1] != 3 {
			t.Fatalf("admitted orders = %v for input [%d,%d,%d]", admitted, input[0].ID, input[1].ID, input[2].ID)
		}
		if ordered[0].ID != 1 || ordered[1].ID != 2 || ordered[2].ID != 3 {
			t.Fatalf("replay order = [%d,%d,%d]", ordered[0].ID, ordered[1].ID, ordered[2].ID)
		}
		if input[0].ID != inputIDs[0] || input[1].ID != inputIDs[1] || input[2].ID != inputIDs[2] {
			t.Fatalf("reportReplayOrder modified input %v", inputIDs)
		}
	}
}
