package biz

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	ta "github.com/banbox/banta"
)

func TestOnDataTakesPrecedenceOverOnBar(t *testing.T) {
	env, err := ta.NewBarEnv("binance", "spot", "BTC/USDT", "1m")
	if err != nil {
		t.Fatalf("NewBarEnv failed: %v", err)
	}
	called := 0
	job := &strat.StratJob{
		Strat: &strat.TradeStrat{
			OnBar: func(s *strat.StratJob) {
				t.Fatalf("OnBar should not be called when OnData is present")
			},
			OnData: func(s *strat.StratJob, evt *orm.DataSeries) {
				called++
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

	trader := &Trader{}
	if err := trader.onAccountDataSeriesJob(nil, job, evt, bar, false); err != nil {
		t.Fatalf("onAccountDataSeriesJob returned error: %v", err)
	}
	if called != 1 {
		t.Fatalf("expected OnData called once, got %d", called)
	}
}
