package strat

import (
	"testing"

	"github.com/banbox/banbot/orm"
)

func TestDataHubLatestAndWindow(t *testing.T) {
	hub := NewDataHub()
	hub.Set(&orm.DataSeries{Source: "kline", Sid: 1, TimeMS: 100, EndMS: 200, TimeFrame: "1m", Values: map[string]any{"close": 1.0}})
	hub.Set(&orm.DataSeries{Source: "kline", Sid: 1, TimeMS: 200, EndMS: 300, TimeFrame: "1m", Values: map[string]any{"close": 2.0}})

	latest := hub.Latest("kline", 1, "1m")
	if latest == nil || latest.TimeMS != 200 {
		t.Fatalf("unexpected latest event: %+v", latest)
	}
	window := hub.Window("kline", 1, "1m", 2)
	if len(window) != 2 || window[0].TimeMS != 100 || window[1].TimeMS != 200 {
		t.Fatalf("unexpected window: %+v", window)
	}
}

func TestCollectDataSubsBridgesLegacyPairInfos(t *testing.T) {
	job := &StratJob{
		Strat: &TradeStrat{
			OnPairInfos: func(s *StratJob) []*PairSub {
				return []*PairSub{{Pair: "_cur_", TimeFrame: "5m", WarmupNum: 20}}
			},
			OnDataSubs: func(s *StratJob) []*DataSub {
				return []*DataSub{{Source: "kline", ExSymbol: s.Symbol, TimeFrame: "1h", WarmupNum: 5}}
			},
		},
		Symbol: &orm.ExSymbol{ID: 7, Exchange: "binance", Market: "spot", Symbol: "BTC/USDT"},
	}

	subs := CollectDataSubs(job)
	if len(subs) != 2 {
		t.Fatalf("expected 2 data subs, got %d", len(subs))
	}
	if subs[0].Source != "kline" || subs[0].ExSymbol.Symbol != "BTC/USDT" || subs[0].TimeFrame != "5m" {
		t.Fatalf("unexpected legacy bridged sub: %+v", subs[0])
	}
	if subs[1].TimeFrame != "1h" {
		t.Fatalf("unexpected explicit data sub: %+v", subs[1])
	}
}
