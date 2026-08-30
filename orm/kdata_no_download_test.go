package orm

import (
	"context"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type localReadExchange struct {
	banexg.BanExchange
	info   *banexg.ExgInfo
	market *banexg.Market
}

func (e *localReadExchange) Info() *banexg.ExgInfo { return e.info }

func (e *localReadExchange) GetMarket(string) (*banexg.Market, *errs.Error) {
	return e.market, nil
}

func (e *localReadExchange) HasApi(string, string) bool { return false }

func TestFastBulkOHLCVNoDownloadStillReadsLocalRows(t *testing.T) {
	initSeriesRepoTestApp(t, mustFindSeriesRepoConfig(t, "config.local.yml"))
	if IsQuestDB {
		t.Skip("postgres/timescale backend is not active")
	}

	const (
		symbol = "LOCAL/USDT"
		tf     = "1m"
	)
	sid := int32(time.Now().UnixNano()%1_000_000 + 4_000_000)
	startMS := int64(1_700_000_000_000)
	exs := &ExSymbol{ID: sid, Exchange: "local", Market: banexg.MarketSpot, Symbol: symbol, ListMs: 1}
	cacheExSymbol(exs)
	exchange := &localReadExchange{
		info:   &banexg.ExgInfo{ID: exs.Exchange, MarketType: exs.Market},
		market: &banexg.Market{Symbol: symbol, Type: exs.Market, Spot: true},
	}
	q, conn, err := Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	defer q.db.Exec(context.Background(), `DELETE FROM kline_1m WHERE sid = $1`, sid)
	if _, err := q.InsertKLines(tf, sid, []*banexg.Kline{{Time: startMS, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 3}}); err != nil {
		t.Fatal(err)
	}

	oldBacktest, oldData, oldCoverage := core.BackTestMode, config.Data, config.HistoricalCoverage
	core.BackTestMode = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true
	config.HistoricalCoverage = &config.HistoricalCoverageConfig{
		BaselineEndMS: startMS + 60_000,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			symbol: {tf: {{StartMS: startMS, StopMS: startMS + 60_000}}},
		},
	}
	defer func() {
		core.BackTestMode, config.Data, config.HistoricalCoverage = oldBacktest, oldData, oldCoverage
	}()
	var got []*banexg.Kline
	err = FastBulkOHLCV(exchange, []string{symbol}, tf, startMS, startMS+60_000, 0,
		func(_, _ string, rows []*banexg.Kline, _ []*AdjInfo) { got = rows })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Time != startMS || got[0].Close != 1.5 {
		t.Fatalf("expected prefetched local row, got %+v", got)
	}
}
