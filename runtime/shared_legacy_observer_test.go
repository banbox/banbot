package runtime

import (
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"testing"
)

type observerRuntimeExchange struct{ banexg.BanExchange }

func (*observerRuntimeExchange) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: "observer", MarketType: "linear"}
}

func TestSharedLegacyTimeframeJobsBothObserveSharedReferenceOnce(t *testing.T) {
	f := newSharedTriggerFixture(t)
	rt, err := f.process.NewRuntime(Options{Mode: core.RunModeBackTest, Config: &config.Config{Exchange: &config.ExchangeConfig{Name: "observer"}, MarketType: "linear"}, Exchange: &observerRuntimeExchange{}, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
	if err != nil {
		t.Fatal(err)
	}
	f.rt = rt
	rt.Clock.SetTimeMS(100)
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC", Exchange: "observer", Market: "linear"}
	if err := f.rt.Symbols.CacheExSymbolChecked(symbol); err != nil {
		t.Fatal(err)
	}
	calls := map[string]int{}
	source := "shared-reference"
	reference := &strat.DataSub{Source: source, ExSymbol: symbol, TimeFrame: "1h", Fields: []string{"custom"}}
	var jobs []*strat.StratJob
	subs := []*strat.DataSub{reference}
	for _, tf := range []string{"1m", "5m"} {
		primary := &strat.DataSub{Source: source, ExSymbol: symbol, TimeFrame: tf, Fields: []string{"custom"}}
		subs = append(subs, primary)
		job := &strat.StratJob{Account: "default", Symbol: symbol, TimeFrame: tf, Strat: &strat.TradeStrat{Name: "legacy"}}
		job.Strat.OnDataSubs = func(*strat.StratJob) []*strat.DataSub { return []*strat.DataSub{primary, reference} }
		job.Strat.OnData = func(j *strat.StratJob, event strat.DataEvent) {
			if event.Raw("custom") != int64(7) {
				t.Error("shared arbitrary type changed")
			}
			calls[j.TimeFrame]++
		}
		jobs = append(jobs, job)
	}
	if err := f.rt.BindFactorLegacyJobs(jobs, subs); err != nil {
		t.Fatal(err)
	}
	if err := f.rt.feedFactorLegacy(&orm.DataSeries{Source: source, Sid: 1, TimeFrame: "1h", TimeMS: 100, EndMS: 101, Closed: true, Values: map[string]any{"custom": int64(7)}}); err != nil {
		t.Fatal(err)
	}
	if calls["1m"] != 1 || calls["5m"] != 1 {
		t.Fatal("shared reference lost or duplicated own-job callbacks", calls)
	}
	if err := f.rt.feedFactorLegacy(&orm.DataSeries{Source: source, Sid: 1, TimeFrame: "1m", TimeMS: 101, EndMS: 102, Closed: true, Values: map[string]any{"custom": int64(7)}}); err != nil {
		t.Fatal(err)
	}
	if calls["1m"] != 2 || calls["5m"] != 1 {
		t.Fatal("primary data sent to other timeframe", calls)
	}
}
