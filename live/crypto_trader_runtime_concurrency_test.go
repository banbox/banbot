package live

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	runtimepkg "github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type r1LiveBar struct {
	timeMS int64
	price  float64
}

type r1LiveResult struct {
	times    []int64
	prices   []float64
	orderIDs []int64
	wallet   float64
}

type r1LiveRunner struct {
	runtime *runtimepkg.Runtime
	trader  *CryptoTrader
	symbol  *orm.ExSymbol
	nowMS   int64
	result  r1LiveResult
	hook    func(*r1LiveRunner)
}

func newR1LiveRunner(t *testing.T, process *runtimepkg.Process, id string, startMS int64) *r1LiveRunner {
	t.Helper()
	runtimeConfig := &config.Config{
		Name:          id,
		Accounts:      map[string]*config.AccountConfig{"default": {}},
		WalletAmounts: map[string]float64{"USDT": 1_000},
		StakeCurrency: []string{"USDT"},
	}
	exchange := &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "test", MarketType: "spot"}}
	rt, err := process.NewRuntime(runtimepkg.Options{
		ID:           id,
		Mode:         core.RunModeLive,
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
	runner := &r1LiveRunner{runtime: rt, symbol: symbol}
	strategy := &strat.TradeStrat{
		Name: "r1-" + id,
		OnData: func(_ *strat.StratJob, event strat.DataEvent) {
			if event.DataFields == nil {
				t.Fatalf("runtime %s callback received no data fields", id)
			}
			runner.result.times = append(runner.result.times, event.TimeMS)
			price := event.Float64("close")
			rt.Market.Prices.SetBarPriceAt(runner.nowMS, symbol.Symbol, price)
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
	trader, traderErr := newRuntimeCryptoTraderForTest(rt, deps, nil)
	if traderErr != nil {
		t.Fatal(traderErr)
	}
	runner.trader = trader
	trader.initFn = func() *errs.Error {
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.collectJobsFn = func() []*strat.StratJob { return nil }
	trader.startJobsFn = func() {}
	return runner
}

func (r *r1LiveRunner) feed(bars []r1LiveBar) *errs.Error {
	r.trader.loopMainFn = func() *errs.Error {
		for _, bar := range bars {
			if !r.trader.runtimeActive() {
				break
			}
			r.nowMS = bar.timeMS
			r.trader.FeedDataSeries(&orm.DataSeries{
				Source:    "macro",
				Sid:       r.symbol.ID,
				TimeMS:    bar.timeMS,
				EndMS:     bar.timeMS + 1,
				TimeFrame: "1m",
				Values:    map[string]any{"close": bar.price},
			})
		}
		return nil
	}
	return r.trader.runWithDeps()
}

func (r *r1LiveRunner) snapshot() r1LiveResult {
	return r1LiveResult{
		times:    append([]int64(nil), r.result.times...),
		prices:   append([]float64(nil), r.result.prices...),
		orderIDs: append([]int64(nil), r.result.orderIDs...),
		wallet:   r.runtime.Trading.Wallet("default").TotalLegal(nil, false),
	}
}

func runR1LiveBaseline(t *testing.T, id string, startMS int64, bars []r1LiveBar) r1LiveResult {
	t.Helper()
	process := runtimepkg.NewProcess()
	runner := newR1LiveRunner(t, process, id, startMS)
	if err := runner.feed(bars); err != nil {
		t.Fatalf("baseline live runtime %s failed: %v", id, err)
	}
	result := runner.snapshot()
	process.Close()
	return result
}

func waitR1LiveSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func TestLiveRuntimeRunnersConcurrentMatchSerialBaseline(t *testing.T) {
	barsA := []r1LiveBar{{1_000, 51}, {2_000, 52}, {3_000, 53}, {4_000, 54}}
	barsB := []r1LiveBar{{101_000, 61}, {102_000, 62}, {103_000, 63}, {104_000, 64}}
	process := runtimepkg.NewProcess()
	defer process.Close()
	a := newR1LiveRunner(t, process, "r1-live-a", 10)
	b := newR1LiveRunner(t, process, "r1-live-b", 20)
	firstA := make(chan struct{})
	firstB := make(chan struct{})
	var aOnce, bOnce sync.Once
	a.hook = func(_ *r1LiveRunner) { aOnce.Do(func() { close(firstA) }) }
	b.hook = func(_ *r1LiveRunner) { bOnce.Do(func() { close(firstB) }) }
	done := make(chan *errs.Error, 2)
	go func() { done <- a.feed(barsA) }()
	go func() { done <- b.feed(barsB) }()
	waitR1LiveSignal(t, firstA, "first live runner callback")
	waitR1LiveSignal(t, firstB, "second live runner callback")
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("concurrent live runner failed: %v", err)
		}
	}

	gotA, gotB := a.snapshot(), b.snapshot()
	wantA := runR1LiveBaseline(t, "r1-live-a-baseline", 10, barsA)
	wantB := runR1LiveBaseline(t, "r1-live-b-baseline", 20, barsB)
	if !reflect.DeepEqual(gotA, wantA) || !reflect.DeepEqual(gotB, wantB) {
		t.Fatalf("concurrent live results differ from serial baselines: got=%+v/%+v want=%+v/%+v", gotA, gotB, wantA, wantB)
	}
	if a.runtime.Strategies == b.runtime.Strategies || a.runtime.Orders == b.runtime.Orders ||
		a.runtime.Trading == b.runtime.Trading || a.trader.dataDeps == b.trader.dataDeps {
		t.Fatal("live runners share mutable runtime state")
	}
}

func TestLiveRuntimeCancellationIsolatedFromSibling(t *testing.T) {
	barsA := []r1LiveBar{{1_000, 71}, {2_000, 72}, {3_000, 73}}
	barsB := []r1LiveBar{{101_000, 81}, {102_000, 82}, {103_000, 83}, {104_000, 84}}
	process := runtimepkg.NewProcess()
	defer process.Close()
	a := newR1LiveRunner(t, process, "r1-live-cancel-a", 10)
	b := newR1LiveRunner(t, process, "r1-live-cancel-b", 20)
	aStopped := make(chan struct{})
	var stopOnce sync.Once
	a.hook = func(r *r1LiveRunner) {
		if len(r.result.times) != 1 {
			return
		}
		r.runtime.Stop()
		stopOnce.Do(func() { close(aStopped) })
	}
	var bOnce sync.Once
	b.hook = func(_ *r1LiveRunner) {
		bOnce.Do(func() { <-aStopped })
	}
	done := make(chan *errs.Error, 2)
	go func() { done <- a.feed(barsA) }()
	go func() { done <- b.feed(barsB) }()
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("cancelled/sibling live runner failed: %v", err)
		}
	}
	if len(a.snapshot().times) != 1 {
		t.Fatalf("cancelled live runner processed unexpected events: %+v", a.snapshot())
	}
	gotB := b.snapshot()
	if len(gotB.times) != len(barsB) || gotB.wallet != 1_000+float64(len(barsB)) ||
		gotB.prices[len(gotB.prices)-1] != barsB[len(barsB)-1].price {
		t.Fatalf("sibling live runtime did not finish independently: %+v", gotB)
	}
	select {
	case <-b.runtime.Done():
		t.Fatal("sibling live runtime was stopped with cancelled runner")
	default:
	}
}
