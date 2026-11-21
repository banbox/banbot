package data

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
)

type mockPriceKlineExchange struct {
	*banexg.Exchange
	netDisable   bool
	nowMS        int64
	archiveCalls int
	restCalls    int
	archiveData  map[string][]*banexg.Kline // key: priceType + "|" + timeframe
	restData     map[string][]*banexg.Kline // key: priceType + "|" + timeframe
}

func newMockPriceKlineExchange(symbol string, nowMS int64) *mockPriceKlineExchange {
	market := &banexg.Market{
		ID:       "BTCUSDT",
		Symbol:   symbol,
		Type:     banexg.MarketLinear,
		Linear:   true,
		Swap:     true,
		Contract: true,
	}
	return &mockPriceKlineExchange{
		Exchange: &banexg.Exchange{
			ExgInfo: &banexg.ExgInfo{
				ID:         "mockex",
				MarketType: banexg.MarketLinear,
				Markets:    banexg.MarketMap{symbol: market},
			},
		},
		nowMS:       nowMS,
		archiveData: make(map[string][]*banexg.Kline),
		restData:    make(map[string][]*banexg.Kline),
	}
}

func (m *mockPriceKlineExchange) GetNetDisable() bool  { return m.netDisable }
func (m *mockPriceKlineExchange) SetNetDisable(v bool) { m.netDisable = v }
func (m *mockPriceKlineExchange) MilliSeconds() int64  { return m.nowMS }

func (m *mockPriceKlineExchange) FetchPriceOHLCVArchive(_ context.Context, priceType, _ string, timeframe string, startMS, endMS int64) ([]*banexg.Kline, bool, *errs.Error) {
	m.archiveCalls++
	src, ok := m.archiveData[priceType+"|"+timeframe]
	if !ok {
		return nil, false, nil
	}
	var out []*banexg.Kline
	for _, item := range src {
		if item.Time >= startMS && item.Time < endMS {
			cp := *item
			out = append(out, &cp)
		}
	}
	return out, true, nil
}

func (m *mockPriceKlineExchange) FetchOHLCV(_ string, timeframe string, since int64, limit int, params map[string]interface{}) ([]*banexg.Kline, *errs.Error) {
	m.restCalls++
	priceType := utils2.GetMapVal(params, "price", "")
	until := utils2.GetMapVal(params, banexg.ParamUntil, int64(0))
	src := m.restData[priceType+"|"+timeframe]
	var out []*banexg.Kline
	for _, item := range src {
		if since > 0 && item.Time < since {
			continue
		}
		if until > 0 && item.Time > until {
			continue
		}
		cp := *item
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Time < out[j].Time })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func TestBuiltinPriceKlineSourcesRegisteredAndRuntimeIsolated(t *testing.T) {
	wantSources := []string{
		orm.SeriesSourceMarkKline,
		orm.SeriesSourceIndexKline,
		orm.SeriesSourcePremiumKline,
	}
	registered := ListDataSources()
	for _, want := range wantSources {
		found := false
		for _, got := range registered {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected built-in %q in ListDataSources(), got %v", want, registered)
		}
	}

	cat1, err := RuntimeCatalogFromRegisteredSources()
	if err != nil {
		t.Fatal(err)
	}
	cat2, err := RuntimeCatalogFromRegisteredSources()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range wantSources {
		s1 := cat1.GetDataSource(name)
		s2 := cat2.GetDataSource(name)
		if s1 == nil || s2 == nil || s1 == s2 {
			t.Fatalf("expected distinct runtime instances for %s, got %p vs %p", name, s1, s2)
		}
		tfSrc, ok := s1.(TimeFrameSeriesSource)
		if !ok {
			t.Fatalf("expected %s to implement TimeFrameSeriesSource", name)
		}
		for _, tf := range []string{"1m", "5m", "15m", "1h", "4h", "8h", "1d"} {
			info, supported := tfSrc.SeriesInfoForTimeFrame(tf)
			if !supported || info == nil || info.Binding.Table != name+"_"+tf {
				t.Fatalf("unexpected SeriesInfo for %s@%s: supported=%v info=%+v", name, tf, supported, info)
			}
			if len(info.Binding.Fields) != 4 {
				t.Fatalf("expected 4 OHLC fields (no volume), got %+v", info.Binding.Fields)
			}
		}
		if _, supported := tfSrc.SeriesInfoForTimeFrame("7m"); supported {
			t.Fatalf("expected unsupported timeframe 7m to return false for %s", name)
		}
	}
}

func TestPremiumKlineZeroVolumeNegativeBasisResample5mTo1h4h8h1d(t *testing.T) {
	const symbol = "BTC/USDT:USDT"
	const fiveMinMS = int64(5 * 60 * 1000)
	const hourMS = int64(3600 * 1000)
	const baseMS = int64(1704067200000) // 2024-01-01 00:00:00 UTC

	// Set mock clock 5 days ahead so [00:00, 01:00) is fetched from archive and recent tail from REST.
	ex := newMockPriceKlineExchange(symbol, baseMS+5*24*hourMS)

	// Populate 12 five-minute premiumIndex bars in [00:00, 01:00) where EVERY bar has
	// Volume == 0 AND all values are NEGATIVE basis rates (verifying neither zero-volume
	// filtering nor 0-initialized high/low clamping corrupts OHLC resampling!).
	// Explicitly shape the 12 bars:
	//   i=0  (00:00): open = -0.0015, high = -0.0012, low = -0.0018, close = -0.0016
	//   i=4  (00:20): global max high = -0.0003 (still negative!)
	//   i=8  (00:40): global min low  = -0.0034
	//   i=11 (00:55): final close     = -0.0021
	// Plus 2 unclosed bars in [01:00, 01:10) that MUST NOT form a premature 2nd hour bar.
	for i := 0; i < 14; i++ {
		ts := baseMS + int64(i)*fiveMinMS
		bar := &banexg.Kline{
			Time:   ts,
			Open:   -0.0015 - float64(i)*0.00005,
			High:   -0.0012 - float64(i)*0.00002,
			Low:    -0.0018 - float64(i)*0.00005,
			Close:  -0.0016 - float64(i)*0.00003,
			Volume: 0, // Price index klines naturally have zero volume!
		}
		switch i {
		case 0:
			bar.Open = -0.0015
		case 4:
			bar.High = -0.0003
		case 8:
			bar.Low = -0.0034
		case 11:
			bar.Close = -0.0021
		}
		ex.archiveData["premiumIndex|5m"] = append(ex.archiveData["premiumIndex|5m"], bar)
	}

	cat, err := RuntimeCatalogFromRegisteredSources()
	if err != nil {
		t.Fatal(err)
	}
	coreState, _ := core.NewState(context.Background())
	t.Cleanup(coreState.Close)
	rtDeps := &RuntimeDeps{
		Core:     coreState,
		Clock:    btime.NewClockState(true, nil),
		Catalog:  cat,
		Exchange: ex,
	}
	cat.BindRuntimeDeps(rtDeps)

	repo := newMemMultiTableSeriesRepo()
	exs := &orm.ExSymbol{ID: 51, Symbol: symbol, Exchange: "mockex", Market: "linear"}
	sub5m := &orm.Subscription{Source: orm.SeriesSourcePremiumKline, ExSymbol: exs, TimeFrame: "5m"}

	if err := cat.EnsureSeriesSubsRange(context.Background(), repo, []*orm.Subscription{sub5m}, baseMS, baseMS+hourMS+2*fiveMinMS); err != nil {
		t.Fatalf("EnsureSeriesSubsRange(premium_kline 5m) failed: %v", err)
	}
	if ex.archiveCalls == 0 {
		t.Fatalf("expected Vision archive fast-path call, got 0")
	}

	stored5m, qErr := repo.QuerySeriesRange(context.Background(), orm.NewPremiumKlineSeriesInfo("5m"), exs.ID, baseMS, baseMS+2*hourMS, 0)
	if qErr != nil || len(stored5m) != 14 {
		t.Fatalf("expected all 14 zero-volume 5m bars stored without loss, got len=%d err=%v", len(stored5m), qErr)
	}
	if stored5m[0].TimeMS != baseMS || stored5m[0].EndMS != baseMS+fiveMinMS {
		t.Fatalf("unexpected 5m zero-lookahead bounds: %+v", stored5m[0])
	}

	// Verify HistSeriesFeeder schedules first 5m bar at EndMS = baseMS + 5m (zero lookahead).
	feeder, fErr := NewHistSeriesFeederWithRuntimeDeps(rtDeps, repo, orm.NewPremiumKlineSeriesInfo("5m"), sub5m, nil, baseMS)
	if fErr != nil {
		t.Fatal(fErr)
	}
	feeder.SetEndMS(baseMS + hourMS)
	feeder.SetSeek(baseMS)
	if gotVis := feeder.getNextMS(); gotVis != baseMS+fiveMinMS {
		t.Fatalf("expected first 5m premium_kline visible at EndMS=%d, got %d", baseMS+fiveMinMS, gotVis)
	}

	// 1) Resample 5m -> 1h via ReadSubscriptionPage (when premium_kline_1h is empty):
	sub1h := orm.Subscription{Source: orm.SeriesSourcePremiumKline, ExSymbol: exs, TimeFrame: "1h"}
	page1h, rErr := cat.ReadSubscriptionPage(context.Background(), repo, nil, sub1h, baseMS, baseMS+2*hourMS, 10)
	if rErr != nil {
		t.Fatalf("ReadSubscriptionPage(premium_kline 1h from 5m) failed: %v", rErr)
	}
	if len(page1h) != 1 {
		t.Fatalf("expected 1 complete resampled 1h bar (partial 2nd hour excluded), got %d", len(page1h))
	}
	b1h := page1h[0]
	open1h, _ := b1h.OpenValue()
	high1h, _ := b1h.HighValue()
	low1h, _ := b1h.LowValue()
	close1h, _ := b1h.CloseValue()
	if math.Abs(open1h - -0.0015) > 1e-9 ||
		math.Abs(high1h - -0.0003) > 1e-9 ||
		math.Abs(low1h - -0.0034) > 1e-9 ||
		math.Abs(close1h - -0.0021) > 1e-9 {
		t.Fatalf("5m->1h negative OHLC mismatch: got O=%v H=%v L=%v C=%v, want O=-0.0015 H=-0.0003 L=-0.0034 C=-0.0021",
			open1h, high1h, low1h, close1h)
	}

	// 2) Also verify orm.ResampleDataSeries directly on 24 hourly zero-volume bars -> 4h, 8h, 1d!
	var hourlySeries []*orm.DataSeries
	var hourlyRecs []*orm.DataRecord
	for h := 0; h < 24; h++ {
		ts := baseMS + int64(h)*hourMS
		o := -0.0010 + float64(h)*0.0001
		hi := o + 0.0004
		lo := o - 0.0006
		cl := o + 0.0002
		if h == 0 {
			o = -0.0022
		}
		if h == 5 {
			lo = -0.0045
		}
		if h == 19 {
			hi = 0.0038
		}
		if h == 23 {
			cl = 0.0011
		}
		vals := map[string]any{"open": o, "high": hi, "low": lo, "close": cl}
		hourlySeries = append(hourlySeries, &orm.DataSeries{
			Source: orm.SeriesSourcePremiumKline, Sid: exs.ID, ExSymbol: exs,
			TimeFrame: "1h", TimeMS: ts, EndMS: ts + hourMS, Closed: true, Values: vals,
		})
		hourlyRecs = append(hourlyRecs, &orm.DataRecord{
			Sid: exs.ID, TimeMS: ts, EndMS: ts + hourMS, Closed: true, Values: vals,
		})
	}
	_ = repo.InsertSeriesBatch(context.Background(), orm.NewPremiumKlineSeriesInfo("1h"), hourlyRecs)

	for _, coarse := range []struct {
		tf      string
		wantLen int
	}{
		{tf: "4h", wantLen: 6},
		{tf: "8h", wantLen: 3},
		{tf: "1d", wantLen: 1},
	} {
		subCoarse := orm.Subscription{Source: orm.SeriesSourcePremiumKline, ExSymbol: exs, TimeFrame: coarse.tf}
		gotPage, cErr := cat.ReadSubscriptionPage(context.Background(), repo, nil, subCoarse, baseMS, baseMS+24*hourMS, 10)
		if cErr != nil || len(gotPage) != coarse.wantLen {
			t.Fatalf("resample 1h->%s failed: len=%d (want %d) err=%v", coarse.tf, len(gotPage), coarse.wantLen, cErr)
		}
		if coarse.tf == "1d" {
			o, _ := gotPage[0].OpenValue()
			h, _ := gotPage[0].HighValue()
			l, _ := gotPage[0].LowValue()
			c, _ := gotPage[0].CloseValue()
			if math.Abs(o - -0.0022) > 1e-9 || math.Abs(h-0.0038) > 1e-9 || math.Abs(l - -0.0045) > 1e-9 || math.Abs(c-0.0011) > 1e-9 {
				t.Fatalf("1h->1d ReadSubscriptionPage OHLC = (%v,%v,%v,%v), want (-0.0022, 0.0038, -0.0045, 0.0011)", o, h, l, c)
			}
		}
	}

	agg1d, finished, aggErr := orm.ResampleDataSeries(exs, "1d", hourlySeries, nil, 24*hourMS, 0, hourMS, 0, false)
	if aggErr != nil || !finished || len(agg1d) != 1 {
		t.Fatalf("orm.ResampleDataSeries(1h->1d) = len %d finished=%v err=%v", len(agg1d), finished, aggErr)
	}
	if o, _ := agg1d[0].OpenValue(); math.Abs(o - -0.0022) > 1e-9 {
		t.Fatalf("ResampleDataSeries Open = %v, want -0.0022", o)
	}
	if h, _ := agg1d[0].HighValue(); math.Abs(h-0.0038) > 1e-9 {
		t.Fatalf("ResampleDataSeries High = %v, want 0.0038", h)
	}
	if l, _ := agg1d[0].LowValue(); math.Abs(l - -0.0045) > 1e-9 {
		t.Fatalf("ResampleDataSeries Low = %v, want -0.0045", l)
	}
	if c, _ := agg1d[0].CloseValue(); math.Abs(c-0.0011) > 1e-9 {
		t.Fatalf("ResampleDataSeries Close = %v, want 0.0011", c)
	}
}

func TestPriceKlineArchiveAndRESTStitchingAndOfflineControls(t *testing.T) {
	const symbol = "BTC/USDT:USDT"
	const hourMS = int64(3600 * 1000)
	const dayMS = 24 * hourMS
	const baseMS = int64(1704067200000) // 2024-01-01 00:00:00 UTC

	// nowMS = 2024-01-04 06:00 UTC -> archiveCutover = 2024-01-02 00:00 UTC.
	// [Jan 1 00:00, Jan 2 00:00) comes from Archive; [Jan 2 00:00, Jan 2 03:00) comes from REST!
	nowMS := baseMS + 3*dayMS + 6*hourMS
	ex := newMockPriceKlineExchange(symbol, nowMS)
	for h := 0; h < 24; h++ {
		ts := baseMS + int64(h)*hourMS
		ex.archiveData["index|1h"] = append(ex.archiveData["index|1h"], &banexg.Kline{
			Time: ts, Open: 42000 + float64(h), High: 42050 + float64(h), Low: 41950 + float64(h), Close: 42010 + float64(h),
		})
	}
	for h := 24; h < 27; h++ {
		ts := baseMS + int64(h)*hourMS
		ex.restData["index|1h"] = append(ex.restData["index|1h"], &banexg.Kline{
			Time: ts, Open: 42000 + float64(h), High: 42050 + float64(h), Low: 41950 + float64(h), Close: 42010 + float64(h),
		})
	}

	cat, err := RuntimeCatalogFromRegisteredSources()
	if err != nil {
		t.Fatal(err)
	}
	coreState, _ := core.NewState(context.Background())
	t.Cleanup(coreState.Close)
	cat.BindRuntimeDeps(&RuntimeDeps{
		Core:     coreState,
		Clock:    btime.NewClockState(true, nil),
		Catalog:  cat,
		Exchange: ex,
	})

	repo := newMemMultiTableSeriesRepo()
	exs := &orm.ExSymbol{ID: 61, Symbol: symbol, Exchange: "mockex", Market: "linear"}
	sub := &orm.Subscription{Source: orm.SeriesSourceIndexKline, ExSymbol: exs, TimeFrame: "1h"}
	if err := cat.EnsureSeriesSubsRange(context.Background(), repo, []*orm.Subscription{sub}, baseMS, baseMS+27*hourMS); err != nil {
		t.Fatalf("stitching EnsureSeriesSubsRange failed: %v", err)
	}
	if ex.archiveCalls == 0 || ex.restCalls == 0 {
		t.Fatalf("expected both archive (%d) and REST (%d) calls", ex.archiveCalls, ex.restCalls)
	}
	rows, qErr := repo.QuerySeriesRange(context.Background(), orm.NewIndexKlineSeriesInfo("1h"), exs.ID, baseMS, baseMS+27*hourMS, 0)
	if qErr != nil || len(rows) != 27 {
		t.Fatalf("expected 27 stitched index_kline rows, got len=%d err=%v", len(rows), qErr)
	}

	// Verify NetDisable and bt_no_kline_download skip all remote fetches.
	for _, mode := range []string{"bt_no_kline_download", "net_disable"} {
		t.Run(mode, func(t *testing.T) {
			exOff := newMockPriceKlineExchange(symbol, nowMS)
			exOff.restData["mark|1h"] = []*banexg.Kline{{Time: baseMS, Open: 1, High: 2, Low: 0.5, Close: 1.5}}
			catOff, err := RuntimeCatalogFromRegisteredSources()
			if err != nil {
				t.Fatal(err)
			}
			cState, _ := core.NewState(context.Background())
			t.Cleanup(cState.Close)
			var snap *config.Snapshot
			if mode == "bt_no_kline_download" {
				spec, lErr := config.LoadRunSpec(&config.CmdArgs{
					NoDefault:  true,
					DataDir:    t.TempDir(),
					ConfigData: fmt.Sprintf("config_version: 2\ntime_start: '20240101'\nbt_no_kline_download: true\nexchange: {name: binance}\nmarket_type: linear\npairs: [%q]\nstake_currency: [USDT]\nrun_policy:\n  - name: dummy\n    run_timeframes: [1h]\n", symbol),
				}, false)
				if lErr != nil {
					t.Fatal(lErr)
				}
				snap, lErr = spec.RuntimeSnapshot()
				if lErr != nil {
					t.Fatal(lErr)
				}
			} else {
				cState.NetDisable = true
			}
			catOff.BindRuntimeDeps(&RuntimeDeps{
				Core: cState, Clock: btime.NewClockState(true, nil), Catalog: catOff, Config: snap, Exchange: exOff,
			})
			memRepo := newMemMultiTableSeriesRepo()
			subMark := &orm.Subscription{Source: orm.SeriesSourceMarkKline, ExSymbol: exs, TimeFrame: "1h"}
			if err := catOff.EnsureSeriesSubsRange(context.Background(), memRepo, []*orm.Subscription{subMark}, baseMS, baseMS+4*hourMS); err != nil {
				t.Fatalf("offline EnsureSeriesSubsRange failed: %v", err)
			}
			if exOff.archiveCalls != 0 || exOff.restCalls != 0 {
				t.Fatalf("expected 0 network calls in %s, got archive=%d rest=%d", mode, exOff.archiveCalls, exOff.restCalls)
			}
		})
	}
}
