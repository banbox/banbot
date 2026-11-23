package runtime

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	ta "github.com/banbox/banta"
)

type liveHooksFixture struct {
	f      *sharedTriggerFixture
	symbol *orm.ExSymbol
	sub    *strat.DataSub
	job    *strat.StratJob
}

// newLiveHooksFixture binds one kline job ("1m", BTC) of strategy "legacy".
func newLiveHooksFixture(t *testing.T, setup func(*strat.TradeStrat), mods ...func(*Options)) *liveHooksFixture {
	t.Helper()
	f := newSharedTriggerFixture(t)
	opts := Options{Mode: core.RunModeBackTest, Config: &config.Config{Exchange: &config.ExchangeConfig{Name: "observer"}, MarketType: "linear"}, Exchange: &observerRuntimeExchange{}, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge}
	for _, mod := range mods {
		mod(&opts)
	}
	rt, err := f.process.NewRuntime(opts)
	if err != nil {
		t.Fatal(err)
	}
	f.rt = rt
	rt.Clock.SetTimeMS(100)
	f.rt.Market.Prices.SetBarPriceAt(100, "BTC", 100)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC", Exchange: "observer", Market: "linear"}
	if err := rt.Symbols.CacheExSymbolChecked(symbol); err != nil {
		t.Fatal(err)
	}
	sub := &strat.DataSub{Source: orm.SeriesSourceKline, ExSymbol: symbol, TimeFrame: "1m"}
	stg := &strat.TradeStrat{Name: "legacy"}
	setup(stg)
	job := &strat.StratJob{Account: "default", Symbol: symbol, TimeFrame: "1m", Strat: stg, Env: &ta.BarEnv{Exchange: "observer", MarketType: "linear", Symbol: "BTC", TimeFrame: "1m", TFMSecs: 60000}}
	if err := rt.BindFactorLegacyJobs([]*strat.StratJob{job}, []*strat.DataSub{sub}); err != nil {
		t.Fatal(err)
	}
	return &liveHooksFixture{f: f, symbol: symbol, sub: sub, job: job}
}

func (x *liveHooksFixture) bar(timeMS int64, warm bool) *orm.DataSeries {
	return &orm.DataSeries{Source: orm.SeriesSourceKline, Sid: 1, TimeFrame: "1m", TimeMS: timeMS, EndMS: timeMS + 60000, Closed: true, IsWarmUp: warm,
		Values: map[string]any{"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1.0}}
}

func (x *liveHooksFixture) feed(t *testing.T, s *orm.DataSeries) {
	t.Helper()
	if err := x.f.rt.feedFactorLegacy(s); err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, what string, limit time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for ", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestExecutionPathFiresBatchJobsOncePerBarAfterDelayAndNotInWarmup(t *testing.T) {
	var warmCalls, liveCalls atomic.Int32
	var x *liveHooksFixture
	x = newLiveHooksFixture(t, func(s *strat.TradeStrat) {
		s.BatchInOut = true
		s.OnBatchJobs = func(jobs []*strat.StratJob) {
			if len(jobs) != 1 {
				t.Error("batch must collect the bound job once", len(jobs))
				return
			}
			if jobs[0].IsWarmUp {
				warmCalls.Add(1)
				// Opening during warmup must stay without effect.
				_ = jobs[0].OpenOrder(&strat.EnterReq{Tag: "warm", Amount: 1, StopLoss: 95})
				return
			}
			liveCalls.Add(1)
			if err := jobs[0].OpenOrder(&strat.EnterReq{Tag: "batch", Amount: 1, StopLoss: 95}); err != nil {
				t.Error(err)
			}
		}
	})
	rt := x.f.rt
	rt.Clock.SetTimeMS(1000)
	// Warmup rows: the batch of a finished bar fires when the next bar arrives.
	x.feed(t, x.bar(0, true))
	x.feed(t, x.bar(60000, true))
	if got := warmCalls.Load(); got != 1 {
		t.Fatalf("warmup batch fired %d times, want 1 (one per bar time)", got)
	}
	if got := x.f.adapter.Metrics().Fills; got != 0 {
		t.Fatalf("warmup batch opened %d orders", got)
	}
	rt.Clock.SetTimeMS(1_000_000)
	// A live row only arms the delay timer.
	live := x.bar(120000, false)
	x.feed(t, live)
	if liveCalls.Load() != 0 {
		t.Fatal("live batch fired before the delay")
	}
	rt.Clock.SetTimeMS(1_000_000 + core.DelayBatchMS + 10)
	waitFor(t, "delayed batch", core.DelayBatchMS*time.Millisecond+5*time.Second, func() bool { return liveCalls.Load() == 1 })
	time.Sleep(core.DelayBatchMS*time.Millisecond/2 + 200*time.Millisecond)
	if got := liveCalls.Load(); got != 1 {
		t.Fatalf("live batch fired %d times for one bar", got)
	}
	if got := warmCalls.Load(); got != 1 {
		t.Fatalf("warmup batch fired again: %d", got)
	}
	if got := x.f.adapter.Metrics().Fills; got != 1 {
		t.Fatalf("batch order was not executed: fills=%d", got)
	}
}

func TestExecutionPathBatchTimerIsStoppedOnClose(t *testing.T) {
	var calls atomic.Int32
	x := newLiveHooksFixture(t, func(s *strat.TradeStrat) {
		s.BatchInOut = true
		s.OnBatchJobs = func([]*strat.StratJob) { calls.Add(1) }
	})
	rt := x.f.rt
	x.feed(t, x.bar(0, false))
	h := lookupLiveHooks(rt)
	h.mu.Lock()
	armed := len(h.timers)
	h.mu.Unlock()
	if armed != 1 {
		t.Fatal("delay timer was not armed", armed)
	}
	rt.Close()
	rt.Clock.SetTimeMS(rt.Clock.TimeMS() + 10*core.DelayBatchMS)
	time.Sleep(core.DelayBatchMS*time.Millisecond + 500*time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("batch fired after runtime close")
	}
	if lookupLiveHooks(rt) != nil {
		t.Fatal("hook state leaked after close")
	}
}

func TestExecutionPathWithoutBatchStrategiesArmsNoTimers(t *testing.T) {
	x := newLiveHooksFixture(t, func(s *strat.TradeStrat) { s.OnData = func(*strat.StratJob, strat.DataEvent) {} })
	x.feed(t, x.bar(0, false))
	h := lookupLiveHooks(x.f.rt)
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.timers) != 0 {
		t.Fatal("timers armed for a runtime without batch callbacks")
	}
}

func TestExecutionPathRunsStrategyShutdownHooksOnce(t *testing.T) {
	var shut, exit atomic.Int32
	x := newLiveHooksFixture(t, func(s *strat.TradeStrat) {
		s.OnShutDown = func(*strat.StratJob) { shut.Add(1) }
		s.OnStratExit = func() { exit.Add(1) }
	})
	if shut.Load() != 0 {
		t.Fatal("shutdown hook ran before close")
	}
	x.f.rt.Close()
	x.f.rt.Close()
	x.f.rt.Join()
	// OnStratExit runs for strategies cached by the job loader, which this
	// fixture bypasses; the per-job hook is what is bound here.
	if shut.Load() != 1 || exit.Load() != 0 {
		t.Fatalf("OnShutDown=%d OnStratExit=%d, want 1 and 0", shut.Load(), exit.Load())
	}
}

func TestExecutionPathShutdownHooksWaitForRunningFeedAndSurvivePanic(t *testing.T) {
	var shut atomic.Int32
	x := newLiveHooksFixture(t, func(s *strat.TradeStrat) {
		s.OnShutDown = func(*strat.StratJob) { shut.Add(1); panic("boom") }
	})
	x.f.rt.Close()
	x.f.rt.Join()
	if shut.Load() != 1 {
		t.Fatal("shutdown hook did not run")
	}
}

func TestSeriesObserverReceivesLiveRowsSkipsWarmupAndIsolatesFailures(t *testing.T) {
	x := newLiveHooksFixture(t, func(s *strat.TradeStrat) { s.OnData = func(*strat.StratJob, strat.DataEvent) {} })
	var mu sync.Mutex
	var got []*data.SeriesMsg
	release := make(chan struct{})
	var calls atomic.Int32
	x.f.rt.SetSeriesObserver(func(m *data.SeriesMsg) {
		n := calls.Add(1)
		if n == 1 {
			panic("observer failure")
		}
		if n == 2 {
			<-release // slow observer
		}
		mu.Lock()
		got = append(got, m)
		mu.Unlock()
	})
	x.feed(t, x.bar(0, true))
	row := x.bar(60000, false)
	start := time.Now()
	x.feed(t, row)
	row.Values["close"] = 555.0 // the observer must hold its own copy
	x.feed(t, x.bar(120000, false))
	for i := 0; i < 2*seriesObserverQueueSize; i++ {
		x.feed(t, x.bar(int64(180000+60000*i), false))
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("slow observer blocked the feed")
	}
	waitFor(t, "observer start", time.Second, func() bool { return calls.Load() >= 2 })
	close(release)
	waitFor(t, "observer rows", 2*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) > 0 })
	mu.Lock()
	defer mu.Unlock()
	first := got[0]
	if first.Pair != "BTC" || first.ExgName != "observer" || first.TFSecs != 60 || len(first.Rows) != 1 {
		t.Fatalf("unexpected message %+v", first)
	}
	for _, m := range got {
		if m.Rows[0].IsWarmUp {
			t.Fatal("warmup row published")
		}
		if m.Rows[0].TimeMS == 60000 && m.Rows[0].Values["close"] != 100.0 {
			t.Fatal("observer shares the feed's value map")
		}
	}
	// The feed keeps working with the observer removed.
	x.f.rt.SetSeriesObserver(nil)
	x.feed(t, x.bar(99999999, false))
}

func TestExecutionPathLoadsStrategyPerformanceOnlyWhenEnabled(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		dir := t.TempDir()
		body := "legacy:\n  perf:\n    BTC_1m: \"6|1.5|1.25\"\n"
		if err := os.WriteFile(filepath.Join(dir, "strat_perfs.yml"), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		x := newLiveHooksFixture(t, func(s *strat.TradeStrat) { s.OnData = func(*strat.StratJob, strat.DataEvent) {} }, func(o *Options) {
			o.DataDir = dir
			o.Config.StratPerf = &config.StratPerfConfig{Enable: enabled}
		})
		perf, ok := x.f.rt.Core.JobPerfValue(core.KeyStratPairTf("legacy", "BTC", "1m"))
		if enabled && (!ok || perf.Score != 1.25 || perf.Num != 6) {
			t.Fatalf("enabled: perf not loaded: %+v %v", perf, ok)
		}
		if !enabled && ok {
			t.Fatal("disabled strat_perf must stay inert")
		}
		x.f.rt.Close()
	}
}
