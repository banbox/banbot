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
	strat.AccInfoJobs = map[string]map[string]map[string]*strat.StratJob{
		config.DefAcc: {},
	}
	got := 0
	job := &strat.StratJob{
		Strat: &strat.TradeStrat{
			OnData: func(s *strat.StratJob, evt *orm.DataSeries) {
				got++
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

	evt := &orm.DataSeries{
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
	trader := &Trader{}
	if err := trader.FeedSeries(evt); err != nil {
		t.Fatalf("FeedSeries returned error: %v", err)
	}
	if got != 1 {
		t.Fatalf("expected OnData called once, got %d", got)
	}
	if latest := job.DataHub.Latest("macro", 11, "1d"); latest == nil {
		t.Fatalf("expected DataHub to store non-kline series")
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
