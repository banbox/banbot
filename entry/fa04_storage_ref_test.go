package entry

import (
	"context"
	"io"
	"math"
	"sync"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	runtimectx "github.com/banbox/banbot/runtime"
)

func TestStorageReplayPreloadsNonInvestableReferenceSymbolAndTrueMTFWarmup(t *testing.T) {
	const (
		hour = int64(3600 * 1000)
		day  = 24 * hour
	)

	// Configure pairs ONLY with altcoins (ETH, SOL); BTC/USDT:USDT is NOT in pairs/Investable,
	// and the expression combines mtf.asof("1d", ts.sma(close, 3)) + ts.beta(..., ref.field("BTC/USDT:USDT", "close"), 4).
	body := `config_version: 2
time_start: '20240105'
time_end: '20240107'
exchange: {name: binance}
market_type: linear
pairs: ['ETH/USDT:USDT', 'SOL/USDT:USDT']
stake_currency: [USDT]
execution: {funding_policy: explicit-zero}
data: {pit_policy: static-approximation, page_rows: 200, prefetch_rows: 2000, max_records: 5000}
run_policy:
  - name: MTFBetaFactor
    id: mtf_beta_v1
    engine: factor
    run_timeframes: [1h]
    params: {k: 1}
    decision: {latency_ms: 1, expiry_ms: 7200000}
    portfolio: {long_notional: 0.5, short_notional: 0.5, mode: full}
    research: {labels: []}
    expressions:
      schema_version: 1
      timeframe: 1h
      bindings:
        kline: {source: kline, timeframe: 1h}
      outputs:
        trend_1d: 'mtf.asof("1d", ts.sma(close, 3))'
        btc_beta: 'ts.beta(ts.ret(close, 1), ts.ret(ref.field("BTC/USDT:USDT", "close"), 1), 4)'
      combine:
        method: fixed
        weights: {trend_1d: 0.0, btc_beta: 1.0}
`
	spec, loadErr := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: body}, false)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	configs, err := buildFactorConfigs(spec, runner.Weights)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, snapshotErr := spec.RuntimeSnapshot()
	if snapshotErr != nil {
		t.Fatal(snapshotErr)
	}
	process := runtimectx.NewProcess()
	t.Cleanup(process.Close)
	task, err := process.NewRuntime(runtimectx.Options{
		Context: context.Background(), Config: snapshot.View(), Mode: core.RunModeBackTest,
		ExchangeName: "binance", Market: "linear", StartAt: snapshot.View().TimeRange.StartMS, NetDisable: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { task.Close(); task.Join() })

	// Register ETH (SID 1), SOL (SID 2), and non-investable reference BTC (SID 99).
	for _, sym := range []*orm.ExSymbol{
		{ID: 1, Symbol: "ETH/USDT:USDT", Exchange: "binance", Market: "linear"},
		{ID: 2, Symbol: "SOL/USDT:USDT", Exchange: "binance", Market: "linear"},
		{ID: 99, Symbol: "BTC/USDT:USDT", Exchange: "binance", Market: "linear"},
	} {
		if err := task.Symbols.CacheExSymbolChecked(sym); err != nil {
			t.Fatal(err)
		}
	}

	anchor, end := snapshot.View().TimeRange.StartMS, snapshot.View().TimeRange.EndMS
	var mu sync.Mutex
	seenStreams := map[string]int64{}

	query := func(ctx context.Context, sub orm.Subscription, start, stop int64, limit int) ([]*orm.DataSeries, error) {
		mu.Lock()
		key := sub.ExSymbol.Symbol + "@" + sub.TimeFrame
		if prev, exists := seenStreams[key]; !exists || start < prev {
			seenStreams[key] = start
		}
		mu.Unlock()

		step := hour
		if sub.TimeFrame == "1d" {
			step = day
		}
		firstOpen := (start / step) * step
		if firstOpen < start {
			firstOpen += step
		}
		var rows []*orm.DataSeries
		for openMS := firstOpen; openMS < stop && openMS <= end; openMS += step {
			endMS := openMS + step
			var closePrice float64
			if sub.TimeFrame == "1d" {
				dayIdx := float64((endMS - anchor) / day)
				closePrice = 1000*float64(sub.ExSymbol.ID) + 10*dayIdx
			} else {
				// BTC oscillates; ETH has 2x sensitivity, SOL has 0.5x sensitivity -> ETH always outranks SOL on btc_beta!
				hIdx := int((endMS - (anchor - 5*day)) / hour)
				btcBase := 40000.0
				ethBase := 2000.0
				solBase := 100.0
				for k := 1; k <= hIdx; k++ {
					r := 0.01 * math.Sin(float64(k)*0.9)
					btcBase *= 1 + r
					ethBase *= 1 + 2.0*r
					solBase *= 1 + 0.5*r
				}
				switch sub.ExSymbol.ID {
				case 99:
					closePrice = btcBase
				case 1:
					closePrice = ethBase
				case 2:
					closePrice = solBase
				}
			}
			rows = append(rows, &orm.DataSeries{
				Source: sub.Source, Sid: sub.ExSymbol.ID, TimeFrame: sub.TimeFrame,
				TimeMS: openMS, EndMS: endMS, Closed: true,
				Values: map[string]any{"close": closePrice},
			})
			if len(rows) == limit {
				break
			}
		}
		return rows, nil
	}

	prepared, err := assembleFactorStorageInputs(context.Background(), spec, task, configs, snapshot.View().Pairs, query)
	if err != nil {
		t.Fatal(err)
	}
	// BTC (SID 99) must be in SIDMap, NOT in Investable!
	if prepared[0].Snapshot.SIDMap[99] != "BTC/USDT:USDT" {
		t.Fatalf("expected BTC SID 99 in SIDMap, got %+v", prepared[0].Snapshot.SIDMap)
	}
	for _, sid := range prepared[0].Snapshot.Universe.Investable {
		if sid == 99 {
			t.Fatalf("reference-only BTC SID 99 leaked into Investable: %+v", prepared[0].Snapshot.Universe)
		}
	}

	results, err := runFactorConfigs(context.Background(), prepared, nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Decisions == 0 || results[0].Incomplete != 0 || results[0].Executions == 0 {
		t.Fatalf("unexpected storage replay result: %+v", results)
	}
	// Verify BTC was never traded in the book, while ETH/SOL were traded!
	if _, tradedBTC := results[0].Book.Quantities[99]; tradedBTC {
		t.Fatalf("reference-only BTC SID 99 was traded: %+v", results[0].Book.Quantities)
	}
	// Verify storage preloaded BTC@1h, ETH@1d (3d warmup = anchor - 3*day), SOL@1d, and did NOT need BTC@1d!
	mu.Lock()
	defer mu.Unlock()
	if _, ok := seenStreams["BTC/USDT:USDT@1h"]; !ok {
		t.Fatalf("BTC/USDT:USDT@1h was not preloaded: %+v", seenStreams)
	}
	if _, ok := seenStreams["BTC/USDT:USDT@1d"]; ok {
		t.Fatalf("BTC/USDT:USDT@1d should not be subscribed when BTC is only referenced on 1h: %+v", seenStreams)
	}
	if gotStart, ok := seenStreams["ETH/USDT:USDT@1d"]; !ok || gotStart != anchor-3*day {
		t.Fatalf("ETH/USDT:USDT@1d warmup start = %d (ok=%v), want %d (anchor - 3d)", gotStart, ok, anchor-3*day)
	}
}
