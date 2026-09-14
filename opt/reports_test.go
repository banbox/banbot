package opt

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestBTResultLoggerIsScopedToReportRuntime(t *testing.T) {
	firstCore, firstLogs := observer.New(zap.ErrorLevel)
	secondCore, secondLogs := observer.New(zap.ErrorLevel)
	first := &BTResult{
		lastPlotMS: 2,
		reportDeps: &ReportDeps{Core: &core.State{Logger: zap.New(firstCore)}},
	}
	second := &BTResult{
		lastPlotMS: 3,
		reportDeps: &ReportDeps{Core: &core.State{Logger: zap.New(secondCore)}},
	}
	wallets := &biz.BanWallets{}

	first.logPlot(wallets, 1, 0, 0)
	if got := firstLogs.Len(); got != 1 {
		t.Fatalf("first report logger entries = %d, want 1", got)
	}
	if got := secondLogs.Len(); got != 0 {
		t.Fatalf("second report logger entries after first result = %d, want 0", got)
	}

	second.logPlot(wallets, 2, 0, 0)
	if got := firstLogs.Len(); got != 1 {
		t.Fatalf("first report logger entries after second result = %d, want 1", got)
	}
	if got := secondLogs.Len(); got != 1 {
		t.Fatalf("second report logger entries = %d, want 1", got)
	}
}

func TestBTResultGroupMetricsUseReportDeps(t *testing.T) {
	logCore, _ := observer.New(zap.WarnLevel)
	result := &BTResult{reportDeps: &ReportDeps{
		Core:   &core.State{Logger: zap.New(logCore)},
		Config: config.NewSnapshot(&config.Config{WalletAmounts: map[string]float64{"USDT": 100}}),
	}}
	err := result.groupByPairs([]*ormo.InOutOrder{{
		IOrder: &ormo.IOrder{Symbol: "BTC/USDT", Leverage: 1, Profit: 1},
		Enter:  &ormo.ExOrder{Average: 1, Filled: 1},
	}})
	if err == nil || !strings.Contains(err.Error(), "market prices") {
		t.Fatalf("runtime report metric error = %v, want report dependency error", err)
	}
}

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

func TestExplicitReportDepsDoNotUseLegacyStorageOrOrders(t *testing.T) {
	previous := ormo.HistODs
	legacyOrder := &ormo.InOutOrder{}
	ormo.HistODs = []*ormo.InOutOrder{legacyOrder}
	t.Cleanup(func() { ormo.HistODs = previous })

	result := &BTResult{reportDeps: &ReportDeps{}}
	if got := result.historyOrders(); got != nil {
		t.Fatalf("explicit report history orders = %v, want nil without OrderState", got)
	}

	deps := &ReportDeps{Symbols: orm.NewSymbolStateWithAllocator(
		orm.NewSIDAllocatorForStorage("explicit:"+t.Name(), t.TempDir()),
	)}
	_, _, err := deps.queries()
	if err == nil || !strings.Contains(err.Error(), "report storage is required") {
		t.Fatalf("explicit report query error = %v, want fail-closed storage error", err)
	}
}

func TestNormalizeBacktestResultRangeUsesRunWindow(t *testing.T) {
	runRange := &config.TimeTuple{StartMS: 1_651_363_200_000, EndMS: 1_786_492_800_000}

	result := &BTResult{StartMS: runRange.EndMS, EndMS: runRange.EndMS}
	normalizeBacktestResultRangeForTimeRange(result, false, runRange)

	if result.StartMS != runRange.StartMS || result.EndMS != runRange.EndMS {
		t.Fatalf("result range = %d-%d, want configured range %d-%d",
			result.StartMS, result.EndMS, runRange.StartMS, runRange.EndMS)
	}
}

func TestNormalizeBacktestResultRangePreservesEarlyStop(t *testing.T) {
	runRange := &config.TimeTuple{StartMS: 1_651_363_200_000, EndMS: 1_786_492_800_000}

	actualEnd := runRange.StartMS + 7*24*60*60*1000
	result := &BTResult{StartMS: runRange.StartMS, EndMS: actualEnd}
	normalizeBacktestResultRangeForTimeRange(result, true, runRange)

	if result.StartMS != runRange.StartMS || result.EndMS != actualEnd {
		t.Fatalf("early-stop result range = %d-%d, want %d-%d",
			result.StartMS, result.EndMS, config.TimeRange.StartMS, actualEnd)
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

func TestBTResultCollectPropagatesGroupMeasureError(t *testing.T) {
	previousMeasure := calcMeasureByOrdersFn
	previousOrders := ormo.HistODs
	t.Cleanup(func() {
		calcMeasureByOrdersFn = previousMeasure
		ormo.HistODs = previousOrders
	})

	want := errs.NewMsg(core.ErrRunTime, "historical coverage missing")
	calcMeasureByOrdersFn = func([]*ormo.InOutOrder, *ReportDeps) (float64, float64, *errs.Error) {
		return 0, 0, want
	}
	ormo.HistODs = []*ormo.InOutOrder{{
		IOrder: &ormo.IOrder{
			ID:        1,
			Symbol:    "BTC/USDT",
			Timeframe: "1h",
			EnterAt:   1_700_000_000_000,
			ExitAt:    1_700_003_600_000,
			Leverage:  1,
			Profit:    1,
		},
		Enter: &ormo.ExOrder{Filled: 1, Average: 1, FeeQuote: 0},
		Exit:  &ormo.ExOrder{Filled: 1, Average: 2, FeeQuote: 0},
	}}

	result := NewBTResult()
	result.TotalInvest = 1
	if got := result.Collect(); got != want {
		t.Fatalf("Collect error = %v, want %v", got, want)
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

func TestCalcGroupEndProfitsUsesExitTimeAndFinalOrderProfit(t *testing.T) {
	const startMS = int64(1700000000000)
	orders := []*ormo.InOutOrder{
		reportTestOrder(1, startMS, startMS+6000, "zone1", 10),
		reportTestOrder(3, startMS+2000, startMS+8000, "zone1", 4),
		reportTestOrder(2, startMS+1000, startMS+2000, "zone1", -3),
		reportTestOrder(5, startMS+700, startMS+4000, "zone2", -2),
		reportTestOrder(4, startMS+500, startMS+4000, "zone2", 5),
	}

	labels, datasets := CalcGroupEndProfits(orders, func(o *ormo.InOutOrder) string {
		return o.EnterTag
	}, 4)
	if len(labels) != 5 {
		t.Fatalf("labels = %d, want 5", len(labels))
	}
	if len(datasets) != 2 {
		t.Fatalf("datasets = %d, want 2", len(datasets))
	}
	assertReportCurve(t, datasets[0], "zone1", []float64{0, -3, -3, 7, 11})
	assertReportCurve(t, datasets[1], "zone2", []float64{0, 0, 3, 3, 3})

	if orders[0].ID != 1 || orders[1].ID != 3 || orders[2].ID != 2 {
		t.Fatalf("CalcGroupEndProfits modified input order sequence")
	}
}

func TestPairPickerRegistry(t *testing.T) {
	RegisterPairPicker("runtime-test", func(*BTResult) []string { return []string{"BTC/USDT"} })
	t.Cleanup(func() { UnregisterPairPicker("runtime-test") })

	if got := selectPairs(NewBTResult(), "runtime-test"); len(got) != 1 || got[0] != "BTC/USDT" {
		t.Fatalf("registered pair picker result = %v", got)
	}
	if _, ok := SnapshotPairPickers()["runtime-test"]; !ok {
		t.Fatal("pair picker snapshot omitted registered picker")
	}
}

func TestExplicitPairPickerDoesNotReadLegacyDirectMap(t *testing.T) {
	PairPickers["legacy-direct"] = func(*BTResult) []string { return []string{"legacy"} }
	t.Cleanup(func() { delete(PairPickers, "legacy-direct") })

	if got := selectPairs(&BTResult{reportDeps: &ReportDeps{}}, "legacy-direct"); got != nil {
		t.Fatalf("explicit report used legacy direct picker: %v", got)
	}
	if got := selectPairs(NewBTResult(), "legacy-direct"); len(got) != 1 || got[0] != "legacy" {
		t.Fatalf("legacy report lost direct picker compatibility: %v", got)
	}
}

func TestCalcGroupEndProfitsIncludesAllProfitsAtFinalLabel(t *testing.T) {
	const startMS = int64(1700000000000)
	orders := []*ormo.InOutOrder{
		reportTestOrder(2, startMS+1000, startMS+3000, "zone", -7),
		reportTestOrder(1, startMS, startMS+9000, "zone", 12),
	}

	labels, datasets := CalcGroupEndProfits(orders, func(o *ormo.InOutOrder) string {
		return o.EnterTag
	}, 3)
	if len(labels) != 4 {
		t.Fatalf("labels = %d, want 4", len(labels))
	}
	assertReportCurve(t, datasets[0], "zone", []float64{0, -7, -7, 5})
}

func TestCalcGroupEndProfitsWithRuntimeDepsUsesSnapshotLocation(t *testing.T) {
	const startMS = int64(1704069000000) // 2024-01-01 00:30:00 UTC
	deps := biz.RuntimeDeps{Config: config.NewSnapshotWithDirs(&config.Config{
		Exchange: &config.ExchangeConfig{Name: "china"},
	}, t.TempDir(), "")}

	labels, datasets := CalcGroupEndProfitsWithRuntimeDeps([]*ormo.InOutOrder{
		reportTestOrder(1, startMS, startMS+1000, "zone", 1),
	}, func(o *ormo.InOutOrder) string {
		return o.EnterTag
	}, 1, deps)
	if len(datasets) != 1 || len(labels) != 2 {
		t.Fatalf("runtime end-profit curve = labels %v datasets %v", labels, datasets)
	}
	want := btime.MSToTime(startMS).In(deps.Config.Location()).Format(core.DefaultDateFmt)
	if labels[0] != want {
		t.Fatalf("runtime end-profit label = %q, want snapshot location %q", labels[0], want)
	}
}

func TestCalcPairStatsLegacyFacadeKeepsEmptyInputUsable(t *testing.T) {
	stats, err := CalcPairStats(nil, 0, 1, "1m")
	if err != nil || len(stats) != 0 {
		t.Fatalf("legacy pair stats = %v, %v; want empty successful result", stats, err)
	}
}

func TestDumpEnterTagCumProfitsPreservesFourDigitValues(t *testing.T) {
	const startMS = int64(1700000000000)
	path := t.TempDir() + "/enters.html"
	if err := DumpEnterTagCumProfits(path, []*ormo.InOutOrder{
		reportTestOrder(1, startMS, startMS+6000, "zone3", -1028.988866),
	}, 1); err != nil {
		t.Fatalf("DumpEnterTagCumProfits failed: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read generated chart: %v", err)
	}
	const marker = "var chartData = "
	start := bytes.Index(data, []byte(marker))
	if start < 0 {
		t.Fatal("generated chart does not contain chartData")
	}
	data = data[start+len(marker):]
	end := bytes.IndexByte(data, '\n')
	if end < 0 {
		t.Fatal("generated chart chartData is not line terminated")
	}
	var chart Chart
	if err := json.Unmarshal(data[:end], &chart); err != nil {
		t.Fatalf("decode generated chart data: %v", err)
	}
	if len(chart.Datasets) != 1 || len(chart.Datasets[0].Data) != 2 {
		t.Fatalf("generated datasets = %#v, want one two-point dataset", chart.Datasets)
	}
	if math.Abs(chart.Datasets[0].Data[1]+1029) > 1e-9 {
		t.Fatalf("serialized final value = %v, want -1029 (not the old -100)", chart.Datasets[0].Data[1])
	}
}

func TestDumpEnterTagCumProfitsWithRuntimeDepsUsesSnapshotLocation(t *testing.T) {
	const startMS = int64(1704069000000) // 2024-01-01 00:30:00 UTC
	path := t.TempDir() + "/enters.html"
	deps := biz.RuntimeDeps{Config: config.NewSnapshotWithDirs(&config.Config{
		Exchange: &config.ExchangeConfig{Name: "china"},
	}, t.TempDir(), "")}
	if err := DumpEnterTagCumProfitsWithRuntimeDeps(path, []*ormo.InOutOrder{
		reportTestOrder(1, startMS, startMS+1000, "zone", 1),
	}, 1, deps); err != nil {
		t.Fatalf("DumpEnterTagCumProfitsWithRuntimeDeps failed: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read generated chart: %v", err)
	}
	const marker = "var chartData = "
	start := bytes.Index(data, []byte(marker))
	if start < 0 {
		t.Fatal("generated chart does not contain chartData")
	}
	data = data[start+len(marker):]
	end := bytes.IndexByte(data, '\n')
	if end < 0 {
		t.Fatal("generated chart chartData is not line terminated")
	}
	var chart Chart
	if err := json.Unmarshal(data[:end], &chart); err != nil {
		t.Fatalf("decode generated chart data: %v", err)
	}
	if got, want := chart.Labels[0], "2024-01-01 08:30:00"; got != want {
		t.Fatalf("runtime chart label = %q, want snapshot-local %q", got, want)
	}
}

func reportTestOrder(id int64, enterMS, exitMS int64, tag string, profit float64) *ormo.InOutOrder {
	return &ormo.InOutOrder{
		IOrder: &ormo.IOrder{
			ID:       id,
			EnterAt:  enterMS,
			ExitAt:   exitMS,
			EnterTag: tag,
			Profit:   profit,
		},
		Enter: &ormo.ExOrder{UpdateAt: enterMS},
		Exit:  &ormo.ExOrder{UpdateAt: exitMS},
	}
}

func assertReportCurve(t *testing.T, dataset *ChartDs, wantLabel string, want []float64) {
	t.Helper()
	if dataset.Label != wantLabel {
		t.Fatalf("dataset label = %q, want %q", dataset.Label, wantLabel)
	}
	if len(dataset.Data) != len(want) {
		t.Fatalf("%s data length = %d, want %d", dataset.Label, len(dataset.Data), len(want))
	}
	for i, wantVal := range want {
		if math.Abs(dataset.Data[i]-wantVal) > 1e-9 {
			t.Fatalf("%s data[%d] = %v, want %v", dataset.Label, i, dataset.Data[i], wantVal)
		}
	}
}
