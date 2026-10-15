package opt

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	runtimepkg "github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

type r1BacktestBar struct {
	timeMS int64
	price  float64
}

type r1BacktestResult struct {
	times    []int64
	prices   []float64
	orderIDs []int64
	wallet   float64
	pairHits int
}

type r1BacktestRunner struct {
	runtime *runtimepkg.Runtime
	lite    *BackTestLite
	symbol  *orm.ExSymbol

	result r1BacktestResult
	hook   func(*r1BacktestRunner)
}

func newR1BacktestRunner(t *testing.T, process *runtimepkg.Process, id string, startMS int64) *r1BacktestRunner {
	t.Helper()
	runtimeConfig := &config.Config{
		Name:          id,
		Accounts:      map[string]*config.AccountConfig{"default": {}},
		WalletAmounts: map[string]float64{"USDT": 1_000},
		StakeCurrency: []string{"USDT"},
	}
	exchange := newBacktestRuntimeExchangeStub("test", "spot")
	rt, err := process.NewRuntime(runtimepkg.Options{
		ID:           id,
		Mode:         core.RunModeBackTest,
		Env:          core.RunEnvDryRun,
		StartAt:      startMS,
		Config:       runtimeConfig,
		Exchange:     exchange,
		ExchangeName: "test",
		Market:       "spot",
	})
	if err != nil {
		t.Fatalf("create runtime %s: %v", id, err)
	}
	symbol := &orm.ExSymbol{ID: 1, Exchange: "test", Market: "spot", Symbol: "BTC/USDT"}
	rt.Symbols.CacheExSymbol(symbol)
	deps := rt.BizDeps()
	runner := &r1BacktestRunner{runtime: rt, symbol: symbol}
	strategy := &strat.TradeStrat{
		Name: "r1-" + id,
		OnData: func(_ *strat.StratJob, event strat.DataEvent) {
			if event.DataFields == nil {
				t.Fatalf("runtime %s callback received no data fields", id)
			}
			runner.result.times = append(runner.result.times, event.TimeMS)
			price := event.Float64("close")
			rt.Market.Prices.SetBarPriceAt(rt.Clock.TimeMS(), symbol.Symbol, price)
			runner.result.prices = append(runner.result.prices,
				rt.Market.Prices.GetLastBarPriceAt(symbol.Symbol))
			orderID := int64(len(runner.result.times))
			rt.Orders.AddHistoricalOrder(&ormo.InOutOrder{
				IOrder: &ormo.IOrder{ID: orderID, Symbol: symbol.Symbol},
				Enter:  &ormo.ExOrder{Filled: 1},
			})
			runner.result.orderIDs = append(runner.result.orderIDs, orderID)
			rt.Trading.Wallet("default").SetWallets(map[string]float64{
				"USDT": 1_000 + float64(orderID),
			})
			rt.Core.AddTfPairHits("1m", symbol.Symbol, 1)
			if runner.hook != nil {
				runner.hook(runner)
			}
		},
	}
	job := &strat.StratJob{
		Strat:     strategy,
		DataHub:   strat.NewDataHub(),
		Symbol:    symbol,
		TimeFrame: "1m",
		Account:   "default",
	}
	rt.Strategies.SetInfoJobMap("default", strat.DataSubKey("macro", symbol.ID, "1m"), map[string]*strat.StratJob{
		strategy.Name: job,
	})
	var liteErr *errs.Error
	runner.lite, liteErr = NewBackTestLiteWithRuntimeDeps(deps, true, nil, nil, nil)
	if liteErr != nil {
		t.Fatal(liteErr)
	}
	return runner
}

func (r *r1BacktestRunner) feed(bars []r1BacktestBar) bool {
	for _, bar := range bars {
		event := &orm.DataSeries{
			Source:    "macro",
			Sid:       r.symbol.ID,
			TimeMS:    bar.timeMS,
			EndMS:     bar.timeMS + 1,
			TimeFrame: "1m",
			Values:    map[string]any{"close": bar.price},
		}
		if !r.lite.FeedDataSeries(event) {
			return false
		}
	}
	return true
}

func (r *r1BacktestRunner) snapshot() r1BacktestResult {
	pairHits := r.runtime.Core.DrainTfPairHits()["1m"][r.symbol.Symbol]
	return r1BacktestResult{
		times:    append([]int64(nil), r.result.times...),
		prices:   append([]float64(nil), r.result.prices...),
		orderIDs: append([]int64(nil), r.result.orderIDs...),
		wallet:   r.runtime.Trading.Wallet("default").TotalLegal(nil, false),
		pairHits: pairHits,
	}
}

func runR1BacktestBaseline(t *testing.T, id string, startMS int64, bars []r1BacktestBar) r1BacktestResult {
	t.Helper()
	process := runtimepkg.NewProcess()
	runner := newR1BacktestRunner(t, process, id, startMS)
	if !runner.feed(bars) {
		t.Fatalf("baseline runtime %s stopped unexpectedly", id)
	}
	result := runner.snapshot()
	process.Close()
	return result
}

type r1BacktestFirstBarrier struct {
	arrived atomic.Int32
	ready   chan struct{}
	release chan struct{}
	once    sync.Once
}

func newR1BacktestFirstBarrier() *r1BacktestFirstBarrier {
	return &r1BacktestFirstBarrier{ready: make(chan struct{}), release: make(chan struct{})}
}

func (b *r1BacktestFirstBarrier) wait() {
	if b.arrived.Add(1) == 2 {
		b.once.Do(func() { close(b.ready) })
	}
	<-b.release
}

func waitR1BacktestSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func TestBacktestRuntimeRunnersConcurrentMatchSerialBaseline(t *testing.T) {
	barsA := []r1BacktestBar{{1_000, 11}, {2_000, 12}, {3_000, 13}, {4_000, 14}}
	barsB := []r1BacktestBar{{101_000, 21}, {102_000, 22}, {103_000, 23}, {104_000, 24}}
	process := runtimepkg.NewProcess()
	defer process.Close()
	a := newR1BacktestRunner(t, process, "r1-a", 10)
	b := newR1BacktestRunner(t, process, "r1-b", 20)
	barrier := newR1BacktestFirstBarrier()
	a.hook = func(r *r1BacktestRunner) {
		if len(r.result.times) == 1 {
			barrier.wait()
		}
	}
	b.hook = a.hook
	done := make(chan bool, 2)
	go func() { done <- a.feed(barsA) }()
	go func() { done <- b.feed(barsB) }()
	waitR1BacktestSignal(t, barrier.ready, "both backtest callbacks")
	close(barrier.release)
	if !<-done || !<-done {
		t.Fatal("concurrent backtest runner stopped unexpectedly")
	}

	gotA, gotB := a.snapshot(), b.snapshot()
	wantA := runR1BacktestBaseline(t, "r1-a-baseline", 10, barsA)
	wantB := runR1BacktestBaseline(t, "r1-b-baseline", 20, barsB)
	if !reflect.DeepEqual(gotA, wantA) || !reflect.DeepEqual(gotB, wantB) {
		t.Fatalf("concurrent results differ from serial baselines: got=%+v/%+v want=%+v/%+v", gotA, gotB, wantA, wantB)
	}
	if a.runtime.Symbols == b.runtime.Symbols || a.runtime.Market == b.runtime.Market ||
		a.runtime.Orders == b.runtime.Orders || a.runtime.Trading == b.runtime.Trading {
		t.Fatal("backtest runners share mutable runtime state")
	}
}

func TestBacktestRuntimeCancellationIsolatedFromSibling(t *testing.T) {
	barsA := []r1BacktestBar{{1_000, 31}, {2_000, 32}, {3_000, 33}}
	barsB := []r1BacktestBar{{101_000, 41}, {102_000, 42}, {103_000, 43}, {104_000, 44}}
	process := runtimepkg.NewProcess()
	defer process.Close()
	a := newR1BacktestRunner(t, process, "r1-cancel-a", 10)
	b := newR1BacktestRunner(t, process, "r1-cancel-b", 20)
	aFirst := make(chan struct{})
	aRelease := make(chan struct{})
	var aOnce sync.Once
	a.hook = func(r *r1BacktestRunner) {
		if len(r.result.times) == 1 {
			aOnce.Do(func() { close(aFirst) })
			<-aRelease
		}
	}
	bSecond := make(chan struct{})
	bRelease := make(chan struct{})
	var bOnce sync.Once
	b.hook = func(r *r1BacktestRunner) {
		if len(r.result.times) == 2 {
			bOnce.Do(func() { close(bSecond) })
			<-bRelease
		}
	}
	aDone := make(chan bool, 1)
	bDone := make(chan bool, 1)
	go func() { aDone <- a.feed(barsA) }()
	waitR1BacktestSignal(t, aFirst, "cancelled runner first callback")
	go func() { bDone <- b.feed(barsB) }()
	waitR1BacktestSignal(t, bSecond, "sibling runner second callback")
	a.runtime.Stop()
	select {
	case <-a.runtime.Done():
	case <-time.After(time.Second):
		t.Fatal("cancelled backtest runtime did not publish cancellation")
	}
	close(aRelease)
	if <-aDone {
		t.Fatal("cancelled backtest runner reported complete feed")
	}
	close(bRelease)
	if !<-bDone {
		t.Fatal("sibling backtest runner stopped with cancelled runtime")
	}

	gotA, gotB := a.snapshot(), b.snapshot()
	if len(gotA.times) != 1 || len(gotA.orderIDs) != 1 || gotA.pairHits != 1 {
		t.Fatalf("cancelled runner processed unexpected state: %+v", gotA)
	}
	if len(gotB.times) != len(barsB) || gotB.pairHits != len(barsB) {
		t.Fatalf("sibling runner did not finish after cancellation: %+v", gotB)
	}
	if gotB.wallet != 1_000+float64(len(barsB)) || gotB.prices[len(gotB.prices)-1] != barsB[len(barsB)-1].price {
		t.Fatalf("sibling runtime state changed unexpectedly: %+v", gotB)
	}
	if b.runtime.Core.BotRunning == false {
		t.Fatal("sibling runtime was stopped with cancelled runner")
	}
	if a.lite.FeedDataSeries(&orm.DataSeries{
		Source: "macro", Sid: a.symbol.ID, TimeMS: 9_000, EndMS: 9_001, TimeFrame: "1m",
		Values: map[string]any{"close": 39.0},
	}) {
		t.Fatal("cancelled backtest accepted a new event")
	}
	if len(a.snapshot().times) != 1 {
		t.Fatal("cancelled backtest processed an event after Stop")
	}
}
