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

func TestOHLCVSeriesUsesOnDataWithMainRole(t *testing.T) {
	env, err := ta.NewBarEnv("binance", "spot", "BTC/USDT", "1m")
	if err != nil {
		t.Fatalf("NewBarEnv failed: %v", err)
	}
	dataCalls := 0
	job := &strat.StratJob{
		Strat: &strat.TradeStrat{
			OnBar: func(s *strat.StratJob) { t.Fatal("OnBar must not run when OnData is set") },
			OnData: func(s *strat.StratJob, data strat.DataEvent) {
				dataCalls++
				if !data.IsMain() || !data.IsKline() || data.Role != strat.DataRoleMain {
					t.Fatalf("unexpected main data role: %v", data.Role)
				}
				if data.Symbol == nil || data.Symbol.ID != 11 {
					t.Fatalf("unexpected main data symbol: %+v", data.Symbol)
				}
				if data.Float64("signal") != 2.5 {
					t.Fatalf("OnData did not receive the processed field: %+v", data.Raw("signal"))
				}
			},
		},
		Env:       env,
		DataHub:   strat.NewDataHub(),
		Symbol:    &orm.ExSymbol{ID: 11, Symbol: "BTC/USDT"},
		TimeFrame: "1m",
		Account:   config.DefAcc,
		IsWarmUp:  true,
	}
	bar := &orm.InfoKline{
		PairTFKline: &banexg.PairTFKline{
			Kline:     banexg.Kline{Time: 1_700_000_000_000},
			Symbol:    "BTC/USDT",
			TimeFrame: "1m",
		},
		Sid:      11,
		IsWarmUp: true,
	}
	evt := orm.KlineToDataSeries(bar)
	evt.Values["signal"] = 2.5

	trader := &Trader{}
	fields := job.SetData(evt)
	if err := trader.onAccountDataSeriesJob(nil, job, evt, fields, false); err != nil {
		t.Fatalf("onAccountDataSeriesJob returned error: %v", err)
	}
	if dataCalls != 1 {
		t.Fatalf("expected OnData once, got %d", dataCalls)
	}
}

func TestNonKlineOHLCVShapeTriggersOnlyOnData(t *testing.T) {
	oldAccounts := config.Accounts
	oldInfoJobs := strat.AccInfoJobs
	config.Accounts = map[string]*config.AccountConfig{config.DefAcc: {}}
	strat.AccInfoJobs = map[string]map[string]map[string]*strat.StratJob{config.DefAcc: {}}
	t.Cleanup(func() {
		config.Accounts = oldAccounts
		strat.AccInfoJobs = oldInfoJobs
	})

	dataCalls := 0
	job := &strat.StratJob{
		Strat: &strat.TradeStrat{
			OnData: func(_ *strat.StratJob, data strat.DataEvent) {
				dataCalls++
				if data.Role != strat.DataRoleCustom || data.IsKline() {
					t.Fatalf("unexpected custom data role: %v", data.Role)
				}
			},
			OnBar: func(_ *strat.StratJob) { t.Fatal("custom source must not enter OnBar") },
		},
		DataHub: strat.NewDataHub(),
		Symbol:  &orm.ExSymbol{ID: 11, Symbol: "CPI_US"},
	}
	strat.AccInfoJobs[config.DefAcc][strat.DataSubKey("macro", 11, "1d")] = map[string]*strat.StratJob{"job": job}
	evt := &orm.DataSeries{
		Source: "macro", Sid: 11, TimeFrame: "1d", TimeMS: 100, EndMS: 200, Closed: true,
		Values: map[string]any{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 3.0},
	}
	if err := (&Trader{}).FeedDataSeries(evt); err != nil {
		t.Fatalf("FeedDataSeries returned error: %v", err)
	}
	if dataCalls != 1 {
		t.Fatalf("expected one OnData call, got %d", dataCalls)
	}
}

func TestPrimaryAndSideSubscriptionDoesNotDuplicateOnData(t *testing.T) {
	oldJobs := strat.AccJobs
	oldInfoJobs := strat.AccInfoJobs
	oldParallel := core.ParallelOnBar
	strat.AccJobs = map[string]map[string]map[string]*strat.StratJob{config.DefAcc: {}}
	strat.AccInfoJobs = map[string]map[string]map[string]*strat.StratJob{config.DefAcc: {}}
	core.ParallelOnBar = false
	t.Cleanup(func() {
		strat.AccJobs = oldJobs
		strat.AccInfoJobs = oldInfoJobs
		core.ParallelOnBar = oldParallel
	})

	exs := &orm.ExSymbol{ID: 11, Symbol: "BTC/USDT"}
	env, err := ta.NewBarEnv("binance", "spot", exs.Symbol, "1m")
	if err != nil {
		t.Fatalf("NewBarEnv failed: %v", err)
	}
	dataCalls := 0
	barCalls := 0
	job := &strat.StratJob{
		Strat: &strat.TradeStrat{
			OnData: func(_ *strat.StratJob, data strat.DataEvent) {
				dataCalls++
				if data.Role != strat.DataRoleMain {
					t.Fatalf("unexpected duplicate subscription role: %v", data.Role)
				}
			},
			OnBar: func(_ *strat.StratJob) { barCalls++ },
		},
		Env: env, DataHub: strat.NewDataHub(), Symbol: exs, TimeFrame: "1m", Account: config.DefAcc,
	}
	strat.AccJobs[config.DefAcc][exs.Symbol+"_1m"] = map[string]*strat.StratJob{"job": job}
	strat.AccInfoJobs[config.DefAcc][strat.DataSubKey("kline", exs.ID, "1m")] = map[string]*strat.StratJob{"job": job}
	evt := orm.NewDataSeriesFromKline(exs, "1m", &banexg.Kline{Time: 60_000, Close: 1}, nil, true, true)
	if err := (&Trader{}).onAccountDataSeries(config.DefAcc, env, evt, nil, false); err != nil {
		t.Fatalf("onAccountDataSeries returned error: %v", err)
	}
	if dataCalls != 1 || barCalls != 0 {
		t.Fatalf("expected one OnData dispatch, got data=%d bar=%d", dataCalls, barCalls)
	}
}
