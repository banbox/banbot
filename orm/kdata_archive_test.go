package orm

import (
	"context"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
)

type archiveTestCall struct {
	startMS int64
	endMS   int64
}

type archiveTestExchange struct {
	banexg.BanExchange
	info         *banexg.ExgInfo
	archiveCalls []archiveTestCall
	apiCalls     []archiveTestCall
}

func (e *archiveTestExchange) Info() *banexg.ExgInfo { return e.info }

func (e *archiveTestExchange) FetchOHLCVArchive(_ context.Context, _ string, _ string, startMS, endMS int64) ([]*banexg.Kline, bool, *errs.Error) {
	e.archiveCalls = append(e.archiveCalls, archiveTestCall{startMS: startMS, endMS: endMS})
	return []*banexg.Kline{{Time: startMS}}, true, nil
}

func (e *archiveTestExchange) FetchOHLCV(_ string, timeframe string, since int64, limit int, _ map[string]interface{}) ([]*banexg.Kline, *errs.Error) {
	e.apiCalls = append(e.apiCalls, archiveTestCall{startMS: since})
	tfMS := int64(utils2.TFToSecs(timeframe)) * 1000
	if limit <= 0 {
		limit = 1
	}
	return []*banexg.Kline{{Time: since + int64(limit-1)*tfMS}}, nil
}

func TestFetchApiOHLCVUsesVisionBeforeTwoDayCutover(t *testing.T) {
	oldBacktest, oldNoDownload, oldNetDisable := core.BackTestMode, config.Data.BTNoKlineDownload, core.NetDisable
	core.BackTestMode = false
	config.Data.BTNoKlineDownload = false
	core.NetDisable = false
	t.Cleanup(func() {
		core.BackTestMode, config.Data.BTNoKlineDownload, core.NetDisable = oldBacktest, oldNoDownload, oldNetDisable
	})

	now := btime.UTCStamp()
	startMS := now - int64(72*60*60*1000)
	endMS := now - int64(60*60*1000)
	exchange := &archiveTestExchange{info: &banexg.ExgInfo{ID: "test", MarketType: banexg.MarketSpot}}
	out := make(chan []*banexg.Kline, 8)
	if err := FetchApiOHLCV(context.Background(), exchange, "TEST/USDT", "1m", startMS, endMS, out); err != nil {
		t.Fatal(err)
	}
	if len(exchange.archiveCalls) != 1 {
		t.Fatalf("archive calls = %#v, want one", exchange.archiveCalls)
	}
	archiveCall := exchange.archiveCalls[0]
	if archiveCall.startMS != startMS || archiveCall.endMS <= startMS || archiveCall.endMS >= endMS {
		t.Fatalf("archive range = %#v, want [%d,%d) split before recent range", archiveCall, startMS, endMS)
	}
	if len(exchange.apiCalls) == 0 || exchange.apiCalls[0].startMS < archiveCall.endMS {
		t.Fatalf("api calls = %#v, want API to start at archive end", exchange.apiCalls)
	}
	if len(out) < 2 {
		t.Fatalf("received %d batches, want archive and API batches", len(out))
	}
}

func TestFetchApiOHLCVSkipsVisionForRecentRange(t *testing.T) {
	oldBacktest, oldNoDownload, oldNetDisable := core.BackTestMode, config.Data.BTNoKlineDownload, core.NetDisable
	core.BackTestMode = false
	config.Data.BTNoKlineDownload = false
	core.NetDisable = false
	t.Cleanup(func() {
		core.BackTestMode, config.Data.BTNoKlineDownload, core.NetDisable = oldBacktest, oldNoDownload, oldNetDisable
	})

	now := btime.UTCStamp()
	startMS := now - int64(60*60*1000)
	endMS := now
	exchange := &archiveTestExchange{info: &banexg.ExgInfo{ID: "test", MarketType: banexg.MarketSpot}}
	out := make(chan []*banexg.Kline, 8)
	if err := FetchApiOHLCV(context.Background(), exchange, "TEST/USDT", "1m", startMS, endMS, out); err != nil {
		t.Fatal(err)
	}
	if len(exchange.archiveCalls) != 0 || len(exchange.apiCalls) == 0 {
		t.Fatalf("archive calls = %#v, api calls = %#v; want API only", exchange.archiveCalls, exchange.apiCalls)
	}
}

func TestFetchApiOHLCVCanDisableArchive(t *testing.T) {
	oldBacktest, oldNoDownload, oldNetDisable := core.BackTestMode, config.Data.BTNoKlineDownload, core.NetDisable
	core.BackTestMode = false
	config.Data.BTNoKlineDownload = false
	core.NetDisable = false
	t.Cleanup(func() {
		core.BackTestMode, config.Data.BTNoKlineDownload, core.NetDisable = oldBacktest, oldNoDownload, oldNetDisable
	})

	now := btime.UTCStamp()
	startMS := now - int64(72*60*60*1000)
	endMS := now - int64(60*60*1000)
	exchange := &archiveTestExchange{info: &banexg.ExgInfo{ID: "test", MarketType: banexg.MarketSpot}}
	out := make(chan []*banexg.Kline, 8)
	if err := fetchApiOHLCV(context.Background(), exchange, "TEST/USDT", "1h", startMS, endMS, out, false); err != nil {
		t.Fatal(err)
	}
	if len(exchange.archiveCalls) != 0 || len(exchange.apiCalls) == 0 {
		t.Fatalf("archive calls = %#v, api calls = %#v; want API only", exchange.archiveCalls, exchange.apiCalls)
	}
}

func TestArchivePolicyUsesRequestedTimeframe(t *testing.T) {
	tests := []struct {
		timeframe string
		want      bool
	}{
		{timeframe: "1m", want: true},
		{timeframe: "1h", want: true},
		{timeframe: "3h", want: true},
		{timeframe: "4h", want: false},
		{timeframe: "1d", want: false},
	}
	for _, test := range tests {
		if got := shouldUseOHLCVArchive(test.timeframe); got != test.want {
			t.Errorf("shouldUseOHLCVArchive(%q) = %v, want %v", test.timeframe, got, test.want)
		}
	}
}

func TestSendKlineBatchesSplitsArchiveResults(t *testing.T) {
	data := []*banexg.Kline{{Time: 1}, {Time: 2}, {Time: 3}, {Time: 4}, {Time: 5}}
	out := make(chan []*banexg.Kline, 3)
	if !sendKlineBatches(context.Background(), out, data, 2) {
		t.Fatal("sendKlineBatches returned false")
	}
	var got []*banexg.Kline
	for i := 0; i < 3; i++ {
		got = append(got, (<-out)...)
	}
	if len(got) != len(data) {
		t.Fatalf("received %d klines, want %d", len(got), len(data))
	}
	for i, kline := range got {
		if kline != data[i] {
			t.Fatalf("kline %d = %#v, want %#v", i, kline, data[i])
		}
	}
}
