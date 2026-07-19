package data

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
)

type stubDataFeeder struct {
	symbol  string
	warmLog *[]string
}

func (f *stubDataFeeder) getSymbol() string            { return f.symbol }
func (f *stubDataFeeder) getWaitData() *orm.DataSeries { return nil }
func (f *stubDataFeeder) setWaitData(*orm.DataSeries)  {}
func (f *stubDataFeeder) getStates() []*PairTFCache    { return nil }
func (f *stubDataFeeder) onNewData(int64, []*orm.DataSeries) (bool, *errs.Error) {
	return false, nil
}
func (f *stubDataFeeder) SubTfs(tfs []string, _ bool) []string {
	return tfs
}
func (f *stubDataFeeder) WarmTfs(_ int64, tfNums map[string]int, _ *utils.PrgBar) (int64, map[string][2]int, *errs.Error) {
	*f.warmLog = append(*f.warmLog, fmt.Sprintf("%s:%d", f.symbol, tfNums["1h"]))
	return 0, nil, nil
}

func TestSubWarmPairsUsesStablePairOrder(t *testing.T) {
	var created, warmed []string
	p := &Provider[IDataFeeder]{
		holders: make(map[string]IDataFeeder),
		newFeeder: func(pair string, _ []string) (IDataFeeder, *errs.Error) {
			created = append(created, pair)
			return &stubDataFeeder{symbol: pair, warmLog: &warmed}, nil
		},
	}
	items := map[string]map[string]int{
		"SOL/USDT": {"1h": 10},
		"BTC/USDT": {"1h": 30},
		"ETH/USDT": {"1h": 20},
	}

	_, _, _, err := p.SubWarmPairs(items, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantCreated := []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}
	wantWarmed := []string{"BTC/USDT:30", "ETH/USDT:20", "SOL/USDT:10"}
	if !reflect.DeepEqual(created, wantCreated) || !reflect.DeepEqual(warmed, wantWarmed) {
		t.Fatalf("unstable warmup order: created=%v warmed=%v", created, warmed)
	}
}

func TestHistProviderMakeFeedersUsesStableCategoryAndKeyOrder(t *testing.T) {
	symbol := &orm.ExSymbol{ID: 1, Symbol: "SAME/USDT"}
	holderA := &DBSeriesFeeder{SeriesFeeder: SeriesFeeder{Feeder: Feeder{ExSymbol: symbol}}}
	holderZ := &DBSeriesFeeder{SeriesFeeder: SeriesFeeder{Feeder: Feeder{ExSymbol: symbol}}}
	tradeA := &TradeFeeder{ExSymbol: symbol}
	tradeZ := &TradeFeeder{ExSymbol: symbol}
	seriesA := &HistSeriesFeeder{info: &orm.SeriesInfo{Name: "macro", TimeFrame: "1m"}, target: symbol}
	seriesZ := &HistSeriesFeeder{info: &orm.SeriesInfo{Name: "macro", TimeFrame: "1m"}, target: symbol}
	provider := &HistProvider{
		Provider: Provider[IHistDataFeeder]{holders: map[string]IHistDataFeeder{
			"z": holderZ,
			"a": holderA,
		}},
		trades: map[string]*TradeFeeder{"z": tradeZ, "a": tradeA},
		series: map[string]*HistSeriesFeeder{"z": seriesZ, "a": seriesA},
	}

	got := provider.makeFeeders()
	want := []IHistFeeder{holderA, holderZ, tradeA, tradeZ, seriesA, seriesZ}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unstable feeder order: got %v, want %v", got, want)
	}
}

type histFeederBatch struct {
	symbol string
	timeMS int64
}

func (b histFeederBatch) TimeMS() int64 {
	return b.timeMS
}

type stubHistFeeder struct {
	name   string
	symbol string
	times  []int64
	index  int
	runs   *[]histFeederBatch
	onRun  func()
}

func (f *stubHistFeeder) getNextMS() int64 {
	if f.index >= len(f.times) {
		return math.MaxInt64
	}
	return f.times[f.index]
}

func (f *stubHistFeeder) SetSeek(int64) {}

func (f *stubHistFeeder) SetEndMS(int64) {}

func (f *stubHistFeeder) GetBatch() Batch {
	if f.index >= len(f.times) {
		return nil
	}
	name := f.name
	if name == "" {
		name = f.symbol
	}
	return histFeederBatch{symbol: name, timeMS: f.times[f.index]}
}

func (f *stubHistFeeder) RunBatch(batch Batch) *errs.Error {
	*f.runs = append(*f.runs, batch.(histFeederBatch))
	if f.onRun != nil {
		f.onRun()
	}
	return nil
}

func (f *stubHistFeeder) CallNext() {
	f.index++
}

func (f *stubHistFeeder) Type() string {
	return "stub"
}

func (f *stubHistFeeder) getSymbol() string {
	return f.symbol
}

func TestRunHistFeedersRotatesExactTies(t *testing.T) {
	var runs []histFeederBatch
	feeders := []IHistFeeder{
		&stubHistFeeder{name: "first", symbol: "same", times: []int64{100, 100}, runs: &runs},
		&stubHistFeeder{name: "second", symbol: "same", times: []int64{100, 100}, runs: &runs},
	}

	err := RunHistFeeders(func() []IHistFeeder { return feeders }, make(chan int, 1), nil)
	if err != nil {
		t.Fatalf("RunHistFeeders returned error: %v", err)
	}

	want := []histFeederBatch{
		{symbol: "first", timeMS: 100},
		{symbol: "second", timeMS: 100},
		{symbol: "first", timeMS: 100},
		{symbol: "second", timeMS: 100},
	}
	if !reflect.DeepEqual(runs, want) {
		t.Fatalf("unexpected exact-tie order: got %v, want %v", runs, want)
	}
}

func TestRunHistFeedersOrdersByTimeAndSymbol(t *testing.T) {
	var runs []histFeederBatch
	feeders := []IHistFeeder{
		&stubHistFeeder{symbol: "z", times: []int64{100, 300}, runs: &runs},
		&stubHistFeeder{symbol: "a", times: []int64{100, 200}, runs: &runs},
	}

	err := RunHistFeeders(func() []IHistFeeder { return feeders }, make(chan int, 1), nil)
	if err != nil {
		t.Fatalf("RunHistFeeders returned error: %v", err)
	}

	want := []histFeederBatch{
		{symbol: "a", timeMS: 100},
		{symbol: "z", timeMS: 100},
		{symbol: "a", timeMS: 200},
		{symbol: "z", timeMS: 300},
	}
	if !reflect.DeepEqual(runs, want) {
		t.Fatalf("unexpected replay order: got %v, want %v", runs, want)
	}
}

func TestRunHistFeedersRebuildsHeapOnNewVersion(t *testing.T) {
	versions := make(chan int, 1)
	var runs []histFeederBatch
	base := &stubHistFeeder{symbol: "base", times: []int64{100, 300}, runs: &runs}
	added := &stubHistFeeder{symbol: "added", times: []int64{200}, runs: &runs}
	base.onRun = func() {
		base.onRun = nil
		versions <- 1
	}
	makeCalls := 0

	err := RunHistFeeders(func() []IHistFeeder {
		makeCalls++
		if makeCalls == 1 {
			return []IHistFeeder{base}
		}
		return []IHistFeeder{base, added}
	}, versions, nil)
	if err != nil {
		t.Fatalf("RunHistFeeders returned error: %v", err)
	}

	want := []histFeederBatch{
		{symbol: "base", timeMS: 100},
		{symbol: "added", timeMS: 200},
		{symbol: "base", timeMS: 300},
	}
	if makeCalls != 2 || !reflect.DeepEqual(runs, want) {
		t.Fatalf("unexpected version reload: calls=%d, got %v, want %v", makeCalls, runs, want)
	}
}

func TestRunHistFeedersHandlesEmptyFeeders(t *testing.T) {
	err := RunHistFeeders(func() []IHistFeeder { return nil }, make(chan int, 1), nil)
	if err != nil {
		t.Fatalf("RunHistFeeders returned error: %v", err)
	}
}
