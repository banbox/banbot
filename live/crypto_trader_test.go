package live

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

type stubTraderDataSource struct {
	info           *orm.SeriesInfo
	subscribeCount int
	subscribedSubs [][]*strat.DataSub
	emissions      [][]*orm.DataRecord
	subscribeErr   error
}

func newStubTraderDataSource(name string) *stubTraderDataSource {
	return &stubTraderDataSource{
		info: &orm.SeriesInfo{
			Name:      name,
			TimeFrame: "1d",
			Binding: orm.SeriesBinding{
				Table:      name,
				TimeColumn: "ts",
				EndColumn:  "end_ms",
				SIDColumn:  "sid",
				Fields: []orm.SeriesField{
					{Name: "open", Type: "float", Role: "open"},
					{Name: "high", Type: "float", Role: "high"},
					{Name: "low", Type: "float", Role: "low"},
					{Name: "close", Type: "float", Role: "close"},
					{Name: "volume", Type: "float", Role: "volume"},
				},
			},
		},
	}
}

func traderTestSourceName(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("%s_%s", t.Name(), suffix)
}

func (s *stubTraderDataSource) Info() *orm.SeriesInfo {
	return s.info
}

func (s *stubTraderDataSource) FetchHistory(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error) {
	return nil, nil
}

func (s *stubTraderDataSource) SubscribeLive(ctx context.Context, subs []*strat.DataSub, sink data.DataSink) error {
	s.subscribeCount++
	cp := make([]*strat.DataSub, 0, len(subs))
	for _, sub := range subs {
		if sub == nil {
			cp = append(cp, nil)
			continue
		}
		dup := *sub
		cp = append(cp, &dup)
	}
	s.subscribedSubs = append(s.subscribedSubs, cp)
	if sink != nil && len(s.emissions) > 0 {
		for _, batch := range s.emissions {
			if err := sink.Emit(subs[0], batch); err != nil {
				return err
			}
		}
	}
	return s.subscribeErr
}

func TestCryptoTraderEmitRequiresStartupProvider(t *testing.T) {
	trader := &CryptoTrader{}
	err := trader.Emit(&strat.DataSub{ExSymbol: &orm.ExSymbol{ID: 1}, TimeFrame: "1d"}, []*orm.DataRecord{{TimeMS: 1, EndMS: 2}})
	if err == nil || err.Error() != "live provider is required" {
		t.Fatalf("expected missing live provider error, got %v", err)
	}
}

func TestCryptoTraderEmitRoutesThirdPartyRowsThroughOnData(t *testing.T) {
	oldAccounts := strat.AccInfoJobs
	strat.AccInfoJobs = map[string]map[string]map[string]*strat.StratJob{
		"default": {},
	}
	t.Cleanup(func() {
		strat.AccInfoJobs = oldAccounts
	})

	var got []*orm.DataSeries
	job := &strat.StratJob{
		Strat: &strat.TradeStrat{
			OnData: func(s *strat.StratJob, evt *orm.DataSeries) {
				got = append(got, evt)
			},
		},
		DataHub: strat.NewDataHub(),
		Symbol:  &orm.ExSymbol{ID: 77, Exchange: "macro", Market: "macro", Symbol: "CPI_US"},
		Account: "default",
	}
	key := strat.DataSubKey("macro", 77, "1d")
	strat.AccInfoJobs["default"][key] = map[string]*strat.StratJob{"job": job}

	trader := &CryptoTrader{dp: &data.LiveProvider{}}
	notified := 0
	trader.dp.OnDataSeries = func(msg *data.SeriesMsg, rows []*orm.DataSeries) *errs.Error {
		notified++
		if msg == nil || msg.Pair != "CPI_US" || msg.TFSecs != 86400 {
			t.Fatalf("unexpected series notification: %+v", msg)
		}
		if len(rows) != 2 || rows[0].Source != "macro" || rows[1].TimeMS != 200 {
			t.Fatalf("unexpected emitted rows: %+v", rows)
		}
		return nil
	}

	err := trader.Emit(&strat.DataSub{Source: "macro", ExSymbol: job.Symbol, TimeFrame: "1d"}, []*orm.DataRecord{
		{TimeMS: 100, EndMS: 200, Closed: true, Values: map[string]any{"value": 10.0}},
		{TimeMS: 200, EndMS: 300, Closed: true, Values: map[string]any{"value": 11.0}},
	})
	if err != nil {
		t.Fatalf("Emit returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected OnData called twice, got %d", len(got))
	}
	if got[0].Source != "macro" || got[0].Sid != 77 || got[0].TimeFrame != "1d" {
		t.Fatalf("unexpected first OnData event: %+v", got[0])
	}
	if job.IsWarmUp {
		t.Fatalf("expected emitted live rows to stay non-warmup")
	}
	latest := job.DataHub.Latest("macro", 77, "1d")
	if latest == nil || latest.TimeMS != 200 {
		t.Fatalf("expected latest macro row at 200, got %+v", latest)
	}
	window := job.DataHub.Window("macro", 77, "1d", 2)
	if len(window) != 2 || window[0].TimeMS != 100 || window[1].TimeMS != 200 {
		t.Fatalf("unexpected macro window after emit: %+v", window)
	}
	if notified != 1 {
		t.Fatalf("expected provider callback once, got %d", notified)
	}
}

func TestCryptoTraderRunSkipsStartupWhenCallbackNil(t *testing.T) {
	trader := NewCryptoTrader()
	initCalls := 0
	startCalls := 0
	loopCalls := 0
	trader.initFn = func() *errs.Error {
		initCalls++
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.collectJobsFn = func() []*strat.StratJob { return nil }
	trader.startJobsFn = func() {
		startCalls++
	}
	trader.loopMainFn = func() *errs.Error {
		loopCalls++
		return nil
	}

	if err := trader.runWithDeps(); err != nil {
		t.Fatalf("runWithDeps failed: %v", err)
	}
	if initCalls != 1 {
		t.Fatalf("expected Init once, got %d", initCalls)
	}
	if startCalls != 1 || loopCalls != 1 {
		t.Fatalf("expected startJobs and loopMain once, got start=%d loop=%d", startCalls, loopCalls)
	}
}

func TestCryptoTraderRunEnsuresThirdPartyBeforeActivateAndLoop(t *testing.T) {
	alpha := newStubTraderDataSource(traderTestSourceName(t, "alpha"))
	if err := data.RegisterDataSource(alpha); err != nil {
		t.Fatalf("RegisterDataSource alpha failed: %v", err)
	}
	job := &strat.StratJob{
		Symbol: &orm.ExSymbol{ID: 101, Symbol: "BTC/USDT"},
		Strat: &strat.TradeStrat{
			Name: "stg",
			OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{{Source: alpha.info.Name, ExSymbol: s.Symbol, TimeFrame: alpha.info.TimeFrame, WarmupNum: 4}}
			},
		},
	}
	collected := []*strat.DataSub{{Source: alpha.info.Name, ExSymbol: &orm.ExSymbol{ID: 101, Symbol: "BTC/USDT"}, TimeFrame: alpha.info.TimeFrame, WarmupNum: 4}}
	trader := NewCryptoTrader()
	steps := make([]string, 0, 4)
	trader.initFn = func() *errs.Error {
		trader.dp = &data.LiveProvider{}
		steps = append(steps, "init")
		return nil
	}
	trader.collectJobsFn = func() []*strat.StratJob {
		steps = append(steps, "collect")
		return []*strat.StratJob{job}
	}
	trader.nowMSFn = func() int64 { return 50_000 }
	trader.seriesRuntime = data.NewSeriesRuntime(trader)
	trader.seriesRuntime.EnsureFunc = func(ctx context.Context, plan *data.ThirdPartySeriesBootstrap) *errs.Error {
		steps = append(steps, fmt.Sprintf("ensure:%d:%d", plan.StartMS, plan.EndMS))
		if !reflect.DeepEqual(plan.Subs, collected) {
			t.Fatalf("unexpected subs passed to ensure: %+v", plan.Subs)
		}
		return nil
	}
	trader.seriesRuntime.ActivateFn = func(ctx context.Context, subs []*strat.DataSub, sink data.DataSink) ([]*strat.DataSub, error) {
		steps = append(steps, "activate")
		if sink != trader {
			t.Fatalf("expected trader sink")
		}
		if !reflect.DeepEqual(subs, collected) {
			t.Fatalf("unexpected subs passed to activate: %+v", subs)
		}
		return subs, nil
	}
	trader.startJobsFn = func() { steps = append(steps, "start") }
	trader.loopMainFn = func() *errs.Error {
		steps = append(steps, "loop")
		return nil
	}

	if err := trader.runWithDeps(); err != nil {
		t.Fatalf("runWithDeps failed: %v", err)
	}
	want := []string{"init", "collect", "ensure:-345550000:50000", "activate", "start", "loop"}
	if !reflect.DeepEqual(steps, want) {
		t.Fatalf("unexpected startup order\nwant: %+v\n got: %+v", want, steps)
	}
	if alpha.subscribeCount != 0 {
		t.Fatalf("expected injected activation seam to bypass real data source calls, got %d", alpha.subscribeCount)
	}
}

func TestCryptoTraderRunEnsureFailureStopsBeforeActivateAndLoop(t *testing.T) {
	trader := NewCryptoTrader()
	trader.initFn = func() *errs.Error {
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.collectJobsFn = func() []*strat.StratJob {
		return []*strat.StratJob{{
			Symbol: &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT"},
			Strat: &strat.TradeStrat{
				Name: "stg",
				OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
					return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 1}}
				},
			},
		}}
	}
	trader.nowMSFn = func() int64 { return 100_000 }
	trader.seriesRuntime = data.NewSeriesRuntime(trader)
	trader.seriesRuntime.EnsureFunc = func(ctx context.Context, plan *data.ThirdPartySeriesBootstrap) *errs.Error {
		return errs.NewMsg(1001, "bootstrap ensure source=macro sid=7 tf=1d phase=ensure: boom")
	}
	activateCalls := 0
	trader.seriesRuntime.ActivateFn = func(ctx context.Context, subs []*strat.DataSub, sink data.DataSink) ([]*strat.DataSub, error) {
		activateCalls++
		return subs, nil
	}
	startCalls := 0
	loopCalls := 0
	trader.startJobsFn = func() { startCalls++ }
	trader.loopMainFn = func() *errs.Error {
		loopCalls++
		return nil
	}

	err := trader.runWithDeps()
	if err == nil || !strings.Contains(err.Message(), "phase=ensure") {
		t.Fatalf("expected ensure-phase startup error, got %v", err)
	}
	if activateCalls != 0 || startCalls != 0 || loopCalls != 0 {
		t.Fatalf("expected ensure failure to stop before activate/start/loop, got activate=%d start=%d loop=%d", activateCalls, startCalls, loopCalls)
	}
}

func TestCryptoTraderRunActivateFailureStopsBeforeLoop(t *testing.T) {
	trader := NewCryptoTrader()
	trader.initFn = func() *errs.Error {
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.collectJobsFn = func() []*strat.StratJob {
		return []*strat.StratJob{{
			Symbol: &orm.ExSymbol{ID: 9, Symbol: "BTC/USDT"},
			Strat: &strat.TradeStrat{
				Name: "stg",
				OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
					return []*strat.DataSub{{Source: "macro", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 2}}
				},
			},
		}}
	}
	trader.nowMSFn = func() int64 { return 100_000 }
	trader.seriesRuntime = data.NewSeriesRuntime(trader)
	trader.seriesRuntime.EnsureFunc = func(ctx context.Context, plan *data.ThirdPartySeriesBootstrap) *errs.Error {
		return nil
	}
	trader.seriesRuntime.ActivateFn = func(ctx context.Context, subs []*strat.DataSub, sink data.DataSink) ([]*strat.DataSub, error) {
		return nil, fmt.Errorf("activate data source \"macro\": bad source")
	}
	startCalls := 0
	loopCalls := 0
	trader.startJobsFn = func() { startCalls++ }
	trader.loopMainFn = func() *errs.Error {
		loopCalls++
		return nil
	}

	err := trader.runWithDeps()
	if err == nil || !strings.Contains(err.Message(), "phase=activate") {
		t.Fatalf("expected activate-phase startup error, got %v", err)
	}
	if trader.seriesRuntime.Active(strat.DataSubKey("macro", 9, "1d")) {
		t.Fatalf("expected failed activation to remain retryable")
	}
	if startCalls != 0 || loopCalls != 0 {
		t.Fatalf("expected activation failure to stop before start/loop, got start=%d loop=%d", startCalls, loopCalls)
	}
}

func TestCryptoTraderRunMarksPartiallyActivatedSourcesRetryable(t *testing.T) {
	trader := NewCryptoTrader()
	trader.initFn = func() *errs.Error {
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.collectJobsFn = func() []*strat.StratJob {
		return []*strat.StratJob{{
			Symbol: &orm.ExSymbol{ID: 9, Symbol: "BTC/USDT"},
			Strat: &strat.TradeStrat{
				Name: "stg",
				OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
					return []*strat.DataSub{
						{Source: "alpha", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 2},
						{Source: "beta", ExSymbol: s.Symbol, TimeFrame: "1d", WarmupNum: 2},
					}
				},
			},
		}}
	}
	trader.nowMSFn = func() int64 { return 100_000 }
	trader.seriesRuntime = data.NewSeriesRuntime(trader)
	trader.seriesRuntime.EnsureFunc = func(ctx context.Context, plan *data.ThirdPartySeriesBootstrap) *errs.Error {
		return nil
	}
	trader.seriesRuntime.ActivateFn = func(ctx context.Context, subs []*strat.DataSub, sink data.DataSink) ([]*strat.DataSub, error) {
		if len(subs) != 2 {
			t.Fatalf("expected alpha and beta activation attempt, got %+v", subs)
		}
		return subs[:1], fmt.Errorf("activate data source \"beta\": bad source")
	}
	trader.startJobsFn = func() { t.Fatal("startJobs should not run after activation failure") }
	trader.loopMainFn = func() *errs.Error {
		t.Fatal("loopMain should not run after activation failure")
		return nil
	}

	err := trader.runWithDeps()
	if err == nil || !strings.Contains(err.Message(), "phase=activate") {
		t.Fatalf("expected activate-phase startup error, got %v", err)
	}
	if !trader.seriesRuntime.Active(strat.DataSubKey("alpha", 9, "1d")) {
		t.Fatalf("expected successfully activated alpha sub to be marked active")
	}
	if trader.seriesRuntime.Active(strat.DataSubKey("beta", 9, "1d")) {
		t.Fatalf("expected failed beta sub to remain retryable")
	}
}

func TestCryptoTraderRunStartupActivatesSelectedSourcesOnce(t *testing.T) {
	alpha := newStubTraderDataSource(traderTestSourceName(t, "alpha"))
	beta := newStubTraderDataSource(traderTestSourceName(t, "beta"))
	if err := data.RegisterDataSource(alpha); err != nil {
		t.Fatalf("RegisterDataSource alpha failed: %v", err)
	}
	if err := data.RegisterDataSource(beta); err != nil {
		t.Fatalf("RegisterDataSource beta failed: %v", err)
	}

	trader := NewCryptoTraderWith(func(ctx context.Context, trader *CryptoTrader) error {
		_, err := data.ActivateDataSources(ctx, []*strat.DataSub{
			{Source: alpha.info.Name, ExSymbol: &orm.ExSymbol{ID: 101, Symbol: "BTC/USDT"}, TimeFrame: alpha.info.TimeFrame},
			{Source: beta.info.Name, ExSymbol: &orm.ExSymbol{ID: 202, Symbol: "ETH/USDT"}, TimeFrame: beta.info.TimeFrame},
			{Source: alpha.info.Name, ExSymbol: &orm.ExSymbol{ID: 303, Symbol: "SOL/USDT"}, TimeFrame: alpha.info.TimeFrame},
		}, trader)
		return err
	})
	initCalls := 0
	startCalls := 0
	loopCalls := 0
	trader.initFn = func() *errs.Error {
		initCalls++
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.startJobsFn = func() {
		startCalls++
	}
	trader.loopMainFn = func() *errs.Error {
		loopCalls++
		return nil
	}

	if err := trader.runWithDeps(); err != nil {
		t.Fatalf("runWithDeps failed: %v", err)
	}
	if initCalls != 1 {
		t.Fatalf("expected Init once, got %d", initCalls)
	}
	if alpha.subscribeCount != 1 || beta.subscribeCount != 1 {
		t.Fatalf("expected grouped source activation once per source, got alpha=%d beta=%d", alpha.subscribeCount, beta.subscribeCount)
	}
	if len(alpha.subscribedSubs) != 1 || len(alpha.subscribedSubs[0]) != 2 {
		t.Fatalf("expected alpha activation group of 2 subs, got %+v", alpha.subscribedSubs)
	}
	if len(beta.subscribedSubs) != 1 || len(beta.subscribedSubs[0]) != 1 {
		t.Fatalf("expected beta activation group of 1 sub, got %+v", beta.subscribedSubs)
	}
	if startCalls != 1 || loopCalls != 1 {
		t.Fatalf("expected startup completion before loop, got start=%d loop=%d", startCalls, loopCalls)
	}
}

func TestCryptoTraderRunStartupReturnsUnknownSourceError(t *testing.T) {
	trader := NewCryptoTraderWith(func(ctx context.Context, trader *CryptoTrader) error {
		_, err := data.ActivateDataSources(ctx, []*strat.DataSub{{
			Source:    "bot_missing_source",
			ExSymbol:  &orm.ExSymbol{ID: 404, Symbol: "BTC/USDT"},
			TimeFrame: "1d",
		}}, trader)
		return err
	})
	startCalls := 0
	loopCalls := 0
	trader.initFn = func() *errs.Error {
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.startJobsFn = func() {
		startCalls++
	}
	trader.loopMainFn = func() *errs.Error {
		loopCalls++
		return nil
	}

	err := trader.runWithDeps()
	if err == nil {
		t.Fatalf("expected unknown source startup error")
	}
	if startCalls != 0 || loopCalls != 0 {
		t.Fatalf("expected startup error to stop before jobs/loop, got start=%d loop=%d", startCalls, loopCalls)
	}
}

func TestCryptoTraderRunStartupReturnsCallbackError(t *testing.T) {
	want := errors.New("startup boom")
	trader := NewCryptoTraderWith(func(ctx context.Context, trader *CryptoTrader) error {
		return want
	})
	startCalls := 0
	loopCalls := 0
	trader.initFn = func() *errs.Error {
		trader.dp = &data.LiveProvider{}
		return nil
	}
	trader.startJobsFn = func() {
		startCalls++
	}
	trader.loopMainFn = func() *errs.Error {
		loopCalls++
		return nil
	}

	err := trader.runWithDeps()
	if err == nil || err.Message() != want.Error() {
		t.Fatalf("expected startup callback error %q, got %v", want.Error(), err)
	}
	if startCalls != 0 || loopCalls != 0 {
		t.Fatalf("expected startup error to stop before jobs/loop, got start=%d loop=%d", startCalls, loopCalls)
	}
}
