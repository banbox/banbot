package biz

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	ta "github.com/banbox/banta"
)

func TestFeedSeriesRoutesNonKlineDataSubs(t *testing.T) {
	oldAccounts := config.Accounts
	oldInfoJobs := strat.AccInfoJobs
	config.Accounts = map[string]*config.AccountConfig{
		config.DefAcc: {},
	}
	strat.AccInfoJobs = map[string]map[string]map[string]*strat.StratJob{
		config.DefAcc: {},
	}
	t.Cleanup(func() {
		config.Accounts = oldAccounts
		strat.AccInfoJobs = oldInfoJobs
	})

	var got []*orm.DataSeries
	job := &strat.StratJob{
		Strat: &strat.TradeStrat{
			OnData: func(s *strat.StratJob, evt *orm.DataSeries) {
				got = append(got, evt)
				if evt.Source != "macro" {
					t.Fatalf("unexpected source: %s", evt.Source)
				}
			},
		},
		DataHub: strat.NewDataHub(),
		Symbol:  &orm.ExSymbol{ID: 11, Exchange: "macro", Market: "macro", Symbol: "CPI_US"},
		Account: config.DefAcc,
	}
	key := strat.DataSubKey("macro", 11, "1d")
	strat.AccInfoJobs[config.DefAcc][key] = map[string]*strat.StratJob{"job": job}

	events := []*orm.DataSeries{
		{
			Source:    "macro",
			Sid:       11,
			TimeMS:    100,
			EndMS:     200,
			TimeFrame: "1d",
			Closed:    true,
			IsWarmUp:  true,
			Values: map[string]any{
				"value": 10.0,
			},
		},
		{
			Source:    "macro",
			Sid:       11,
			TimeMS:    200,
			EndMS:     300,
			TimeFrame: "1d",
			Closed:    true,
			Values: map[string]any{
				"value": 11.0,
			},
		},
		{
			Source:    "macro",
			Sid:       11,
			TimeMS:    300,
			EndMS:     400,
			TimeFrame: "1d",
			Closed:    true,
			Values: map[string]any{
				"value": 12.0,
			},
		},
	}
	trader := &Trader{}
	for idx, evt := range events {
		if err := trader.FeedSeries(evt); err != nil {
			t.Fatalf("FeedSeries returned error for source=%s sid=%d tf=%s: %v", evt.Source, evt.Sid, evt.TimeFrame, err)
		}
		if idx == 0 && !job.IsWarmUp {
			t.Fatalf("expected first warmup event to mark job warmup")
		}
	}
	if len(got) != len(events) {
		t.Fatalf("expected OnData called %d times, got %d", len(events), len(got))
	}
	if !got[0].IsWarmUp {
		t.Fatalf("expected warmup event to reach OnData, got %+v", got[0])
	}
	if job.IsWarmUp {
		t.Fatalf("expected latest non-warmup event to clear job warmup state")
	}
	latest := job.DataHub.Latest("macro", 11, "1d")
	if latest == nil || latest.TimeMS != 300 || latest.IsWarmUp {
		t.Fatalf("expected DataHub latest non-kline event at 300, got %+v", latest)
	}
	window := job.DataHub.Window("macro", 11, "1d", 3)
	if len(window) != 3 || window[0].TimeMS != 100 || !window[0].IsWarmUp || window[2].TimeMS != 300 {
		t.Fatalf("expected ordered non-kline window with warmup preserved, got %+v", window)
	}
}

func TestFeedSeriesFallsBackToOnInfoBarForLegacyKlineSubs(t *testing.T) {
	oldAccounts := config.Accounts
	oldEnvs := strat.Envs
	oldInfoJobs := strat.AccInfoJobs
	oldMatchTfs := core.OrderMatchTfs
	config.Accounts = map[string]*config.AccountConfig{
		config.DefAcc: {},
	}
	strat.Envs = map[string]*ta.BarEnv{}
	strat.AccInfoJobs = map[string]map[string]map[string]*strat.StratJob{
		config.DefAcc: {},
	}
	core.OrderMatchTfs = map[string]bool{}
	t.Cleanup(func() {
		config.Accounts = oldAccounts
		strat.Envs = oldEnvs
		strat.AccInfoJobs = oldInfoJobs
		core.OrderMatchTfs = oldMatchTfs
	})

	exs := &orm.ExSymbol{ID: 11, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"}
	env, err := ta.NewBarEnv("binance", "spot", exs.Symbol, "5m")
	if err != nil {
		t.Fatalf("NewBarEnv failed: %v", err)
	}
	strat.Envs[exs.Symbol+"_5m"] = env

	called := 0
	job := &strat.StratJob{
		Strat: &strat.TradeStrat{
			OnInfoBar: func(s *strat.StratJob, e *ta.BarEnv, pair, tf string) {
				called++
				if pair != exs.Symbol || tf != "5m" {
					t.Fatalf("unexpected info callback args: %s %s", pair, tf)
				}
			},
		},
		DataHub: strat.NewDataHub(),
		Symbol:  &orm.ExSymbol{ID: 99, Symbol: "BTC/USDT"},
		Account: config.DefAcc,
	}
	key := strat.DataSubKey("kline", exs.ID, "5m")
	strat.AccInfoJobs[config.DefAcc][key] = map[string]*strat.StratJob{"job": job}

	evt := orm.NewDataSeriesFromKline(exs, "5m", &banexg.Kline{
		Time:      100,
		Open:      1,
		High:      2,
		Low:       0.5,
		Close:     1.5,
		Volume:    10,
		Quote:     15,
		BuyVolume: 6,
		TradeNum:  7,
	}, nil, false, true)
	if evt == nil {
		t.Fatalf("expected kline series event")
	}

	trader := &Trader{}
	if err := trader.FeedSeries(evt); err != nil {
		t.Fatalf("FeedSeries returned error: %v", err)
	}
	if called != 1 {
		t.Fatalf("expected OnInfoBar called once, got %d", called)
	}
}

func TestFeedSeriesCoexistsForThirdPartyAndLegacyInfoSubs(t *testing.T) {
	oldAccounts := config.Accounts
	oldEnvs := strat.Envs
	oldInfoJobs := strat.AccInfoJobs
	oldMatchTfs := core.OrderMatchTfs
	config.Accounts = map[string]*config.AccountConfig{
		config.DefAcc: {},
	}
	strat.Envs = map[string]*ta.BarEnv{}
	strat.AccInfoJobs = map[string]map[string]map[string]*strat.StratJob{
		config.DefAcc: {},
	}
	core.OrderMatchTfs = map[string]bool{}
	t.Cleanup(func() {
		config.Accounts = oldAccounts
		strat.Envs = oldEnvs
		strat.AccInfoJobs = oldInfoJobs
		core.OrderMatchTfs = oldMatchTfs
	})

	macroEvents := 0
	macroJob := &strat.StratJob{
		Strat: &strat.TradeStrat{
			OnData: func(s *strat.StratJob, evt *orm.DataSeries) {
				macroEvents++
				if evt.Source != "macro" || evt.Sid != 11 || evt.TimeFrame != "1d" {
					t.Fatalf("unexpected macro routing identity: %+v", evt)
				}
			},
		},
		DataHub: strat.NewDataHub(),
		Symbol:  &orm.ExSymbol{ID: 11, Exchange: "macro", Market: "macro", Symbol: "CPI_US"},
		Account: config.DefAcc,
	}
	strat.AccInfoJobs[config.DefAcc][strat.DataSubKey("macro", 11, "1d")] = map[string]*strat.StratJob{"macro": macroJob}

	exs := &orm.ExSymbol{ID: 11, Exchange: "binance", Market: "spot", Symbol: "ETH/USDT"}
	env, err := ta.NewBarEnv("binance", "spot", exs.Symbol, "5m")
	if err != nil {
		t.Fatalf("NewBarEnv failed: %v", err)
	}
	strat.Envs[exs.Symbol+"_5m"] = env

	legacyCalls := 0
	legacyJob := &strat.StratJob{
		Strat: &strat.TradeStrat{
			OnInfoBar: func(s *strat.StratJob, e *ta.BarEnv, pair, tf string) {
				legacyCalls++
				if pair != exs.Symbol || tf != "5m" {
					t.Fatalf("unexpected legacy callback args: %s %s", pair, tf)
				}
			},
		},
		DataHub: strat.NewDataHub(),
		Symbol:  &orm.ExSymbol{ID: 99, Symbol: "BTC/USDT"},
		Account: config.DefAcc,
	}
	strat.AccInfoJobs[config.DefAcc][strat.DataSubKey("kline", exs.ID, "5m")] = map[string]*strat.StratJob{"legacy": legacyJob}

	trader := &Trader{}
	macroEvt := &orm.DataSeries{
		Source:    "macro",
		Sid:       11,
		TimeMS:    100,
		EndMS:     200,
		TimeFrame: "1d",
		Closed:    true,
		Values: map[string]any{
			"value": 10.0,
		},
	}
	if err := trader.FeedSeries(macroEvt); err != nil {
		t.Fatalf("FeedSeries returned error for macro event: %v", err)
	}
	legacyEvt := orm.NewDataSeriesFromKline(exs, "5m", &banexg.Kline{
		Time:      200,
		Open:      1,
		High:      2,
		Low:       0.5,
		Close:     1.5,
		Volume:    10,
		Quote:     15,
		BuyVolume: 6,
		TradeNum:  7,
	}, nil, false, true)
	if legacyEvt == nil {
		t.Fatalf("expected kline series event")
	}
	if err := trader.FeedSeries(legacyEvt); err != nil {
		t.Fatalf("FeedSeries returned error for legacy kline event: %v", err)
	}

	if macroEvents != 1 {
		t.Fatalf("expected macro OnData once, got %d", macroEvents)
	}
	if legacyCalls != 1 {
		t.Fatalf("expected legacy OnInfoBar once, got %d", legacyCalls)
	}
	macroLatest := macroJob.DataHub.Latest("macro", 11, "1d")
	if macroLatest == nil || macroLatest.TimeMS != 100 {
		t.Fatalf("expected macro DataHub latest at 100, got %+v", macroLatest)
	}
	legacyLatest := legacyJob.DataHub.Latest("kline", exs.ID, "5m")
	if legacyLatest == nil || legacyLatest.TimeMS != 200 {
		t.Fatalf("expected legacy DataHub latest at 200, got %+v", legacyLatest)
	}
	if macroJob.DataHub.Latest("kline", exs.ID, "5m") != nil {
		t.Fatalf("expected macro job hub to stay isolated from legacy kline key")
	}
	if legacyJob.DataHub.Latest("macro", 11, "1d") != nil {
		t.Fatalf("expected legacy job hub to stay isolated from macro key")
	}
}
