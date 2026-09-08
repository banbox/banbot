package data

import (
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
)

func TestWatchJobsExplicitStateFailsBeforeSavingOrSending(t *testing.T) {
	state := orm.NewSymbolStateWithIdentity("binance", "spot")
	if err := state.SetExSymbols([]*orm.ExSymbol{{
		ID: 1, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT",
	}}); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		exg    string
		market string
		jobs   []WatchJob
	}{
		{name: "missing symbol after valid symbol", exg: "binance", market: "spot", jobs: []WatchJob{
			{Symbol: "BTC/USDT", TimeFrame: "1m"},
			{Symbol: "ETH/USDT", TimeFrame: "1m"},
		}},
		{name: "foreign identity", exg: "okx", market: "spot", jobs: []WatchJob{
			{Symbol: "BTC/USDT", TimeFrame: "1m"},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			watcher := &SeriesWatcher{symbols: state, jobs: make(map[string]map[string]*PairTFCache)}
			err := watcher.WatchJobs(test.exg, test.market, "ohlcv", test.jobs...)
			if err == nil || err.Code != core.ErrInvalidSymbol {
				t.Fatalf("WatchJobs error = %v, want invalid symbol", err)
			}
			if len(watcher.jobs) != 0 {
				t.Fatalf("WatchJobs saved jobs before validation: %+v", watcher.jobs)
			}
			if len(watcher.initMsgs) != 0 {
				t.Fatalf("WatchJobs saved subscription messages before validation: %+v", watcher.initMsgs)
			}
		})
	}
}
