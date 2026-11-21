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
)

type mockOpenInterestExchange struct {
	*banexg.Exchange
	netDisable  bool
	metricCalls int
	byTF        map[string][]*banexg.DerivativesMetric
}

func newMockOpenInterestExchange(symbol string) *mockOpenInterestExchange {
	market := &banexg.Market{
		ID:       "BTCUSDT",
		Symbol:   symbol,
		Type:     banexg.MarketLinear,
		Linear:   true,
		Swap:     true,
		Contract: true,
	}
	return &mockOpenInterestExchange{
		Exchange: &banexg.Exchange{
			ExgInfo: &banexg.ExgInfo{
				ID:         "mockex",
				MarketType: banexg.MarketLinear,
				Markets:    banexg.MarketMap{symbol: market},
			},
		},
		byTF: make(map[string][]*banexg.DerivativesMetric),
	}
}

func (m *mockOpenInterestExchange) GetNetDisable() bool  { return m.netDisable }
func (m *mockOpenInterestExchange) SetNetDisable(v bool) { m.netDisable = v }

func (m *mockOpenInterestExchange) FetchDerivativesMetrics(_ context.Context, symbol, timeframe string, startMS, endMS int64, limit int, _ map[string]interface{}) ([]*banexg.DerivativesMetric, bool, *errs.Error) {
	m.metricCalls++
	src := m.byTF[timeframe]
	var out []*banexg.DerivativesMetric
	for _, item := range src {
		if item.Symbol != symbol {
			continue
		}
		if startMS > 0 && item.Timestamp < startMS {
			continue
		}
		if endMS > 0 && item.Timestamp > endMS {
			continue
		}
		cp := *item
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, true, nil
}

type memMultiTableSeriesRepo struct {
	rows     map[string]map[int32]map[int64]*orm.DataRecord
	coverage map[string]map[int32][]orm.MSRange
}

func newMemMultiTableSeriesRepo() *memMultiTableSeriesRepo {
	return &memMultiTableSeriesRepo{
		rows:     make(map[string]map[int32]map[int64]*orm.DataRecord),
		coverage: make(map[string]map[int32][]orm.MSRange),
	}
}

func (r *memMultiTableSeriesRepo) EnsureSeriesTable(context.Context, *orm.SeriesInfo) *errs.Error {
	return nil
}

func (r *memMultiTableSeriesRepo) InsertSeriesBatch(_ context.Context, info *orm.SeriesInfo, batch []*orm.DataRecord) *errs.Error {
	tbl := info.Binding.Table
	if r.rows[tbl] == nil {
		r.rows[tbl] = make(map[int32]map[int64]*orm.DataRecord)
	}
	for _, row := range batch {
		if row == nil {
			continue
		}
		if r.rows[tbl][row.Sid] == nil {
			r.rows[tbl][row.Sid] = make(map[int64]*orm.DataRecord)
		}
		cp := *row
		vals := make(map[string]any, len(row.Values))
		for k, v := range row.Values {
			vals[k] = v
		}
		cp.Values = vals
		r.rows[tbl][row.Sid][row.TimeMS] = &cp
	}
	return nil
}

func (r *memMultiTableSeriesRepo) QuerySeriesRange(_ context.Context, info *orm.SeriesInfo, sid int32, startMS, endMS int64, limit int) ([]*orm.DataRecord, *errs.Error) {
	byTS := r.rows[info.Binding.Table][sid]
	if len(byTS) == 0 {
		return nil, nil
	}
	keys := make([]int64, 0, len(byTS))
	for ts := range byTS {
		if ts >= startMS && ts < endMS {
			keys = append(keys, ts)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]*orm.DataRecord, 0, len(keys))
	for _, ts := range keys {
		src := byTS[ts]
		cp := *src
		vals := make(map[string]any, len(info.Binding.Fields))
		for _, f := range info.Binding.Fields {
			vals[f.Name] = src.Values[f.Name]
		}
		cp.Values = vals
		out = append(out, &cp)
	}
	return out, nil
}

func (r *memMultiTableSeriesRepo) DeleteSeriesRange(_ context.Context, info *orm.SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	byTS := r.rows[info.Binding.Table][sid]
	for ts := range byTS {
		if ts >= startMS && ts < endMS {
			delete(byTS, ts)
		}
	}
	return nil
}

func (r *memMultiTableSeriesRepo) UpdateSeriesRange(_ context.Context, info *orm.SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	tbl := info.Binding.Table
	if r.coverage[tbl] == nil {
		r.coverage[tbl] = make(map[int32][]orm.MSRange)
	}
	r.coverage[tbl][sid] = mergeMemRanges(append(r.coverage[tbl][sid], orm.MSRange{Start: startMS, Stop: endMS}))
	return nil
}

func (r *memMultiTableSeriesRepo) UpdateSeriesCoverage(ctx context.Context, info *orm.SeriesInfo, sid int32, startMS, endMS int64, _ []*orm.DataRecord) *errs.Error {
	return r.UpdateSeriesRange(ctx, info, sid, startMS, endMS)
}

func (r *memMultiTableSeriesRepo) GetSeriesRange(_ context.Context, info *orm.SeriesInfo, sid int32) (int64, int64, *errs.Error) {
	cov := r.coverage[info.Binding.Table][sid]
	if len(cov) == 0 {
		return 0, 0, nil
	}
	return cov[0].Start, cov[len(cov)-1].Stop, nil
}

func (r *memMultiTableSeriesRepo) MissingSeriesRanges(_ context.Context, info *orm.SeriesInfo, sid int32, startMS, endMS int64) ([]orm.MSRange, *errs.Error) {
	missing := []orm.MSRange{{Start: startMS, Stop: endMS}}
	for _, cov := range r.coverage[info.Binding.Table][sid] {
		var next []orm.MSRange
		for _, m := range missing {
			if cov.Stop <= m.Start || cov.Start >= m.Stop {
				next = append(next, m)
				continue
			}
			if m.Start < cov.Start {
				next = append(next, orm.MSRange{Start: m.Start, Stop: cov.Start})
			}
			if cov.Stop < m.Stop {
				next = append(next, orm.MSRange{Start: cov.Stop, Stop: m.Stop})
			}
		}
		missing = next
	}
	return missing, nil
}

func TestBuiltinOpenInterestRegisteredMultiTimeFrameAndRuntimeIsolated(t *testing.T) {
	names := ListDataSources()
	found := false
	for _, name := range names {
		if name == orm.SeriesSourceOpenInterest {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected built-in %q in ListDataSources(), got %v", orm.SeriesSourceOpenInterest, names)
	}

	cat1, err := RuntimeCatalogFromRegisteredSources()
	if err != nil {
		t.Fatal(err)
	}
	cat2, err := RuntimeCatalogFromRegisteredSources()
	if err != nil {
		t.Fatal(err)
	}
	src1 := cat1.GetDataSource(orm.SeriesSourceOpenInterest)
	src2 := cat2.GetDataSource(orm.SeriesSourceOpenInterest)
	if src1 == nil || src2 == nil || src1 == src2 {
		t.Fatalf("expected distinct runtime open_interest instances, got %p vs %p", src1, src2)
	}

	tfSrc, ok := src1.(TimeFrameSeriesSource)
	if !ok {
		t.Fatalf("expected OpenInterestDataSource to implement TimeFrameSeriesSource")
	}
	for _, tf := range []string{"5m", "15m", "1h", "4h", "1d"} {
		info, supported := tfSrc.SeriesInfoForTimeFrame(tf)
		if !supported || info == nil || info.Binding.Table != "open_interest_"+tf {
			t.Fatalf("unexpected SeriesInfo for %s: supported=%v info=%+v", tf, supported, info)
		}
	}
	if _, supported := tfSrc.SeriesInfoForTimeFrame("7m"); supported {
		t.Fatalf("expected unsupported timeframe 7m to return false")
	}

	for _, field := range orm.DefaultOpenInterestFields() {
		rule, ok := orm.GetFieldAggRule(field)
		if !ok || rule != "last" {
			t.Fatalf("expected field %q agg rule 'last', got %q (ok=%v)", field, rule, ok)
		}
	}
}

func TestOpenInterestBackfillNullSemanticsZeroLookaheadAndLastResample(t *testing.T) {
	const symbol = "BTC/USDT:USDT"
	const fiveMinMS = int64(5 * 60 * 1000)
	const hourMS = int64(3600 * 1000)
	const baseMS = int64(1704067200000) // 2024-01-01 00:00:00 UTC

	ex := newMockOpenInterestExchange(symbol)
	// Populate 14 five-minute snapshots: 12 closing [00:00, 01:00), plus 2 unclosed [01:00, 01:10) for the second hour.
	for i := 1; i <= 14; i++ {
		ts := baseMS + int64(i)*fiveMinMS
		m := banexg.NewDerivativesMetric(symbol, ts)
		m.OpenInterest = 10000.0 + float64(i)
		m.OpenInterestValue = m.OpenInterest * 42000.0
		m.TopTraderLongShortPosRatio = 1.20 + float64(i)*0.01
		// Leave TopTraderLongShortAccountRatio, GlobalLongShortAccountRatio, TakerLongShortVolRatio as NaN on i==12
		// to verify explicit SQL NULL preservation through backfill and 5m->1h resampling!
		if i != 12 {
			m.TopTraderLongShortAccountRatio = 1.10
			m.GlobalLongShortAccountRatio = 0.95
			m.TakerLongShortVolRatio = 1.05
		}
		ex.byTF["5m"] = append(ex.byTF["5m"], m)
	}

	cat, err := RuntimeCatalogFromRegisteredSources()
	if err != nil {
		t.Fatal(err)
	}
	coreState, _ := core.NewState(context.Background())
	t.Cleanup(coreState.Close)
	clock := btime.NewClockState(true, nil)
	rtDeps := &RuntimeDeps{Core: coreState, Clock: clock, Catalog: cat, Exchange: ex}
	cat.BindRuntimeDeps(rtDeps)

	repo := newMemMultiTableSeriesRepo()
	exs := &orm.ExSymbol{ID: 31, Symbol: symbol, Exchange: "mockex", Market: "linear"}
	sub5m := &orm.Subscription{Source: orm.SeriesSourceOpenInterest, ExSymbol: exs, TimeFrame: "5m"}

	if err := cat.EnsureSeriesSubsRange(context.Background(), repo, []*orm.Subscription{sub5m}, baseMS, baseMS+hourMS+2*fiveMinMS); err != nil {
		t.Fatalf("EnsureSeriesSubsRange(5m) failed: %v", err)
	}
	if ex.metricCalls != 1 {
		t.Fatalf("expected 1 exchange call, got %d", ex.metricCalls)
	}

	rows5m, qErr := repo.QuerySeriesRange(context.Background(), orm.NewOpenInterestSeriesInfo("5m"), exs.ID, baseMS, baseMS+hourMS+2*fiveMinMS, 0)
	if qErr != nil || len(rows5m) != 14 {
		t.Fatalf("expected 14 stored 5m rows, got len=%d err=%v", len(rows5m), qErr)
	}
	// Row 0 covers [00:00, 00:05) -> EndMS = 00:05 (i=1, OI=10001).
	if rows5m[0].TimeMS != baseMS || rows5m[0].EndMS != baseMS+fiveMinMS || rows5m[0].Values["oi"].(float64) != 10001.0 {
		t.Fatalf("unexpected first 5m row: %+v", rows5m[0])
	}
	// Row 11 covers [00:55, 01:00) -> EndMS = 01:00 (i=12, OI=10012, missing fields stored as nil NULL!).
	if rows5m[11].Values["top_ls_acc_ratio"] != nil || rows5m[11].Values["taker_ls_vol_ratio"] != nil {
		t.Fatalf("expected NaN fields to be stored as explicit nil NULL, got %+v", rows5m[11].Values)
	}

	// Verify zero-lookahead visibility via HistSeriesFeeder: [00:00, 00:05) row is scheduled at EndMS = 00:05.
	feeder, fErr := NewHistSeriesFeederWithRuntimeDeps(rtDeps, repo, orm.NewOpenInterestSeriesInfo("5m"), sub5m, nil, baseMS)
	if fErr != nil {
		t.Fatal(fErr)
	}
	feeder.SetEndMS(baseMS + hourMS)
	feeder.SetSeek(baseMS)
	if nextVis := feeder.getNextMS(); nextVis != baseMS+fiveMinMS {
		t.Fatalf("expected first 5m OI row visible at EndMS=%d, got %d", baseMS+fiveMinMS, nextVis)
	}

	// Now read 1h from the stored 5m table (open_interest_1h is empty):
	// - [00:00, 01:00) is complete and MUST take the closing snapshot (i=12 -> OI=10012, NOT sum!), preserving NULL!
	// - [01:00, 02:00) only has [01:00, 01:10) and MUST NOT be emitted prematurely!
	sub1h := orm.Subscription{Source: orm.SeriesSourceOpenInterest, ExSymbol: exs, TimeFrame: "1h"}
	page1h, rErr := cat.ReadSubscriptionPage(context.Background(), repo, nil, sub1h, baseMS, baseMS+2*hourMS, 10)
	if rErr != nil {
		t.Fatalf("ReadSubscriptionPage(1h from 5m) failed: %v", rErr)
	}
	if len(page1h) != 1 {
		t.Fatalf("expected exactly 1 complete resampled 1h bar (no premature partial 2nd hour), got %d", len(page1h))
	}
	bar1h := page1h[0]
	if bar1h.TimeMS != baseMS || bar1h.EndMS != baseMS+hourMS || !bar1h.Closed {
		t.Fatalf("unexpected resampled 1h bar bounds: %+v", bar1h)
	}
	if oi, _ := bar1h.FloatValue("oi"); math.Abs(oi-10012.0) > 1e-9 {
		t.Fatalf("expected 'last' OI=10012.0 (not sum), got %v", oi)
	}
	if ratio, _ := bar1h.FloatValue("top_ls_pos_ratio"); math.Abs(ratio-1.32) > 1e-9 {
		t.Fatalf("expected 'last' top_ls_pos_ratio=1.32, got %v", ratio)
	}
	if rawVal, exists := bar1h.Values["taker_ls_vol_ratio"]; !exists || rawVal != nil {
		t.Fatalf("expected explicit nil NULL preserved after 5m->1h resample, got exists=%v val=%v", exists, rawVal)
	}
}

func TestOpenInterestOfflineAndBTNoKlineDownloadSkipNetwork(t *testing.T) {
	const symbol = "BTC/USDT:USDT"
	const hourMS = int64(3600 * 1000)
	const baseMS = int64(1704067200000)

	for _, mode := range []string{"bt_no_kline_download", "net_disable"} {
		t.Run(mode, func(t *testing.T) {
			ex := newMockOpenInterestExchange(symbol)
			m := banexg.NewDerivativesMetric(symbol, baseMS+hourMS)
			m.OpenInterest = 9999
			ex.byTF["1h"] = []*banexg.DerivativesMetric{m}

			cat, err := RuntimeCatalogFromRegisteredSources()
			if err != nil {
				t.Fatal(err)
			}
			coreState, _ := core.NewState(context.Background())
			t.Cleanup(coreState.Close)
			var snap *config.Snapshot
			if mode == "bt_no_kline_download" {
				spec, loadErr := config.LoadRunSpec(&config.CmdArgs{
					NoDefault:  true,
					DataDir:    t.TempDir(),
					ConfigData: fmt.Sprintf("config_version: 2\ntime_start: '20240101'\nbt_no_kline_download: true\nexchange: {name: binance}\nmarket_type: linear\npairs: [%q]\nstake_currency: [USDT]\nrun_policy:\n  - name: dummy\n    run_timeframes: [1h]\n", symbol),
				}, false)
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				snap, loadErr = spec.RuntimeSnapshot()
				if loadErr != nil {
					t.Fatal(loadErr)
				}
			} else {
				coreState.NetDisable = true
			}
			cat.BindRuntimeDeps(&RuntimeDeps{
				Core:     coreState,
				Clock:    btime.NewClockState(true, nil),
				Catalog:  cat,
				Config:   snap,
				Exchange: ex,
			})

			repo := newMemMultiTableSeriesRepo()
			exs := &orm.ExSymbol{ID: 41, Symbol: symbol, Exchange: "mockex", Market: "linear"}
			_ = repo.InsertSeriesBatch(context.Background(), orm.NewOpenInterestSeriesInfo("1h"), []*orm.DataRecord{{
				Sid:    exs.ID,
				TimeMS: baseMS,
				EndMS:  baseMS + hourMS,
				Closed: true,
				Values: map[string]any{
					"oi":                 12345.0,
					"oi_value":           500000000.0,
					"top_ls_pos_ratio":   1.45,
					"top_ls_acc_ratio":   nil,
					"global_ls_ratio":    nil,
					"taker_ls_vol_ratio": nil,
				},
			}})

			sub := &orm.Subscription{Source: orm.SeriesSourceOpenInterest, ExSymbol: exs, TimeFrame: "1h"}
			if err := cat.EnsureSeriesSubsRange(context.Background(), repo, []*orm.Subscription{sub}, baseMS, baseMS+4*hourMS); err != nil {
				t.Fatalf("offline EnsureSeriesSubsRange failed: %v", err)
			}
			if ex.metricCalls != 0 {
				t.Fatalf("expected 0 network calls in %s mode, got %d", mode, ex.metricCalls)
			}
			if len(repo.coverage["open_interest_1h"][exs.ID]) != 0 {
				t.Fatalf("offline ensure must not write synthetic coverage, got %+v", repo.coverage["open_interest_1h"][exs.ID])
			}
			page, readErr := cat.ReadSubscriptionPage(context.Background(), repo, nil, *sub, baseMS, baseMS+4*hourMS, 10)
			if readErr != nil || len(page) != 1 {
				t.Fatalf("expected 1 local row readable offline, got len=%d err=%v", len(page), readErr)
			}
			if oiVal, _ := page[0].FloatValue("oi_value"); math.Abs(oiVal-500000000.0) > 1e-6 {
				t.Fatalf("unexpected local oi_value %v", oiVal)
			}
		})
	}
}
