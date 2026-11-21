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

type mockFundingExchange struct {
	*banexg.Exchange
	netDisable   bool
	historyCalls int
	archiveCalls int
	archiveAvail bool
	archiveRates []*banexg.FundingRate
	apiRates     []*banexg.FundingRate
}

func newMockFundingExchange(symbol string) *mockFundingExchange {
	market := &banexg.Market{
		ID:       "BTCUSDT",
		Symbol:   symbol,
		Type:     banexg.MarketLinear,
		Linear:   true,
		Swap:     true,
		Contract: true,
	}
	return &mockFundingExchange{
		Exchange: &banexg.Exchange{
			ExgInfo: &banexg.ExgInfo{
				ID:         "mockex",
				MarketType: banexg.MarketLinear,
				Markets:    banexg.MarketMap{symbol: market},
			},
		},
	}
}

func (m *mockFundingExchange) GetNetDisable() bool  { return m.netDisable }
func (m *mockFundingExchange) SetNetDisable(v bool) { m.netDisable = v }

func (m *mockFundingExchange) FetchFundingRateArchive(_ context.Context, symbol string, startMS, endMS int64) ([]*banexg.FundingRate, bool, *errs.Error) {
	m.archiveCalls++
	if !m.archiveAvail {
		return nil, false, nil
	}
	var out []*banexg.FundingRate
	for _, item := range m.archiveRates {
		if item.Symbol == symbol && item.Timestamp >= startMS && item.Timestamp <= endMS {
			cp := *item
			out = append(out, &cp)
		}
	}
	return out, true, nil
}

func (m *mockFundingExchange) FetchFundingRateHistory(symbol string, since int64, limit int, params map[string]interface{}) ([]*banexg.FundingRate, *errs.Error) {
	m.historyCalls++
	var until int64
	if raw, ok := params[banexg.ParamUntil].(int64); ok {
		until = raw
	}
	var out []*banexg.FundingRate
	for _, item := range m.apiRates {
		if item.Symbol != symbol {
			continue
		}
		if since > 0 && item.Timestamp < since {
			continue
		}
		if until > 0 && item.Timestamp > until {
			continue
		}
		cp := *item
		out = append(out, &cp)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })
	return out, nil
}

type memFundingRepo struct {
	rows     map[int32]map[int64]*orm.DataRecord
	coverage map[int32][]orm.MSRange
}

func newMemFundingRepo() *memFundingRepo {
	return &memFundingRepo{
		rows:     make(map[int32]map[int64]*orm.DataRecord),
		coverage: make(map[int32][]orm.MSRange),
	}
}

func (r *memFundingRepo) EnsureSeriesTable(context.Context, *orm.SeriesInfo) *errs.Error { return nil }

func (r *memFundingRepo) InsertSeriesBatch(_ context.Context, _ *orm.SeriesInfo, batch []*orm.DataRecord) *errs.Error {
	for _, row := range batch {
		if row == nil {
			continue
		}
		if r.rows[row.Sid] == nil {
			r.rows[row.Sid] = make(map[int64]*orm.DataRecord)
		}
		cp := *row
		vals := make(map[string]any, len(row.Values))
		for k, v := range row.Values {
			vals[k] = v
		}
		cp.Values = vals
		r.rows[row.Sid][row.TimeMS] = &cp
	}
	return nil
}

func (r *memFundingRepo) QuerySeriesRange(_ context.Context, _ *orm.SeriesInfo, sid int32, startMS, endMS int64, limit int) ([]*orm.DataRecord, *errs.Error) {
	byTS := r.rows[sid]
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
		out = append(out, byTS[ts])
	}
	return out, nil
}

func (r *memFundingRepo) DeleteSeriesRange(_ context.Context, _ *orm.SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	for ts := range r.rows[sid] {
		if ts >= startMS && ts < endMS {
			delete(r.rows[sid], ts)
		}
	}
	return nil
}

func (r *memFundingRepo) UpdateSeriesRange(_ context.Context, _ *orm.SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	r.coverage[sid] = mergeMemRanges(append(r.coverage[sid], orm.MSRange{Start: startMS, Stop: endMS}))
	return nil
}

func (r *memFundingRepo) UpdateSeriesCoverage(_ context.Context, _ *orm.SeriesInfo, sid int32, startMS, endMS int64, _ []*orm.DataRecord) *errs.Error {
	r.coverage[sid] = mergeMemRanges(append(r.coverage[sid], orm.MSRange{Start: startMS, Stop: endMS}))
	return nil
}

func (r *memFundingRepo) GetSeriesRange(_ context.Context, _ *orm.SeriesInfo, sid int32) (int64, int64, *errs.Error) {
	cov := r.coverage[sid]
	if len(cov) == 0 {
		return 0, 0, nil
	}
	return cov[0].Start, cov[len(cov)-1].Stop, nil
}

func (r *memFundingRepo) MissingSeriesRanges(_ context.Context, _ *orm.SeriesInfo, sid int32, startMS, endMS int64) ([]orm.MSRange, *errs.Error) {
	missing := []orm.MSRange{{Start: startMS, Stop: endMS}}
	for _, cov := range r.coverage[sid] {
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

func mergeMemRanges(items []orm.MSRange) []orm.MSRange {
	if len(items) == 0 {
		return nil
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Start < items[j].Start })
	out := []orm.MSRange{items[0]}
	for _, cur := range items[1:] {
		last := &out[len(out)-1]
		if cur.Start <= last.Stop {
			if cur.Stop > last.Stop {
				last.Stop = cur.Stop
			}
			continue
		}
		out = append(out, cur)
	}
	return out
}

func TestBuiltinFundingRateRegisteredAndRuntimeIsolated(t *testing.T) {
	names := ListDataSources()
	found := false
	for _, name := range names {
		if name == orm.SeriesSourceFundingRate {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected built-in %q in ListDataSources(), got %v", orm.SeriesSourceFundingRate, names)
	}

	cat1, err := RuntimeCatalogFromRegisteredSources()
	if err != nil {
		t.Fatalf("RuntimeCatalogFromRegisteredSources failed: %v", err)
	}
	cat2, err := RuntimeCatalogFromRegisteredSources()
	if err != nil {
		t.Fatalf("second RuntimeCatalogFromRegisteredSources failed: %v", err)
	}
	src1 := cat1.GetDataSource(orm.SeriesSourceFundingRate)
	src2 := cat2.GetDataSource(orm.SeriesSourceFundingRate)
	if src1 == nil || src2 == nil || src1 == src2 {
		t.Fatalf("expected distinct runtime funding_rate instances, got %p vs %p", src1, src2)
	}
	info := src1.Info()
	if info.Name != "funding_rate" || info.TimeFrame != "8h" || info.Binding.Table != "funding_rate_8h" {
		t.Fatalf("unexpected funding_rate SeriesInfo: %+v", info)
	}
}

func TestFundingRateSourceBackfillGapFillAndZeroLookaheadVisibility(t *testing.T) {
	const symbol = "BTC/USDT:USDT"
	const hourMS = int64(3600 * 1000)
	const stepMS = 8 * hourMS
	// Historical UTC timestamps well before current UTC month so archive path is exercised first.
	const baseMS = int64(1704067200000) // 2024-01-01 00:00:00 UTC

	ex := newMockFundingExchange(symbol)
	ex.archiveAvail = true
	ex.archiveRates = []*banexg.FundingRate{
		{Symbol: symbol, Timestamp: baseMS, FundingRate: 0.00005},
		{Symbol: symbol, Timestamp: baseMS + stepMS, FundingRate: 0.0001},
		{Symbol: symbol, Timestamp: baseMS + 2*stepMS, FundingRate: -0.0002},
		{Symbol: symbol, Timestamp: baseMS + 3*stepMS, FundingRate: 0.0003},
		{Symbol: symbol, Timestamp: baseMS + 4*stepMS, FundingRate: -0.0004},
	}

	cat, err := RuntimeCatalogFromRegisteredSources()
	if err != nil {
		t.Fatal(err)
	}
	coreState, _ := core.NewState(context.Background())
	t.Cleanup(coreState.Close)
	clock := btime.NewClockState(true, nil)
	cat.BindRuntimeDeps(&RuntimeDeps{
		Core:     coreState,
		Clock:    clock,
		Exchange: ex,
	})

	repo := newMemFundingRepo()
	exs := &orm.ExSymbol{ID: 11, Symbol: symbol, Exchange: "mockex", Market: "linear"}
	sub := &orm.Subscription{Source: orm.SeriesSourceFundingRate, ExSymbol: exs, TimeFrame: "8h"}

	// First ensure [00:00, 16:00): should persist 08:00 and 16:00 settlements without including 00:00.
	if err := cat.EnsureSeriesSubsRange(context.Background(), repo, []*orm.Subscription{sub}, baseMS, baseMS+2*stepMS); err != nil {
		t.Fatalf("initial EnsureSeriesSubsRange failed: %v", err)
	}
	if ex.archiveCalls != 1 || ex.historyCalls != 0 {
		t.Fatalf("expected 1 archive call and 0 REST calls for old historical range, got archive=%d rest=%d", ex.archiveCalls, ex.historyCalls)
	}
	rows, qErr := repo.QuerySeriesRange(context.Background(), orm.NewFundingRateSeriesInfo("8h"), exs.ID, baseMS, baseMS+2*stepMS, 0)
	if qErr != nil || len(rows) != 2 {
		t.Fatalf("expected 2 stored rows, got len=%d err=%v", len(rows), qErr)
	}
	if rows[0].TimeMS != baseMS || rows[0].EndMS != baseMS+stepMS || math.Abs(rows[0].Values["rate"].(float64)-0.0001) > 1e-12 {
		t.Fatalf("unexpected first row: %+v", rows[0])
	}
	if rows[1].TimeMS != baseMS+stepMS || rows[1].EndMS != baseMS+2*stepMS || math.Abs(rows[1].Values["rate"].(float64)+0.0002) > 1e-12 {
		t.Fatalf("unexpected second row: %+v", rows[1])
	}

	// Switch archive off and extend range to [00:00, 32:00): only missing [16:00, 32:00) gap should hit REST!
	ex.archiveAvail = false
	ex.apiRates = ex.archiveRates
	var lastRequestedStart int64
	if err := cat.EnsureSeriesSubsRange(context.Background(), repo, []*orm.Subscription{sub}, baseMS, baseMS+4*stepMS); err != nil {
		t.Fatalf("gap-fill EnsureSeriesSubsRange failed: %v", err)
	}
	if ex.historyCalls != 1 {
		t.Fatalf("expected 1 REST gap-fill call, got %d", ex.historyCalls)
	}
	allRows, qErr := repo.QuerySeriesRange(context.Background(), orm.NewFundingRateSeriesInfo("8h"), exs.ID, baseMS, baseMS+4*stepMS, 0)
	if qErr != nil || len(allRows) != 4 {
		t.Fatalf("expected 4 total stored rows after gap fill, got %d (err=%v)", len(allRows), qErr)
	}
	_ = lastRequestedStart

	// Verify zero-lookahead visibility: row covering [00:00, 08:00) has EndMS = 08:00,
	// so HistSeriesFeeder schedules it at 08:00 (never visible at 07:59:59.999).
	var emitted []*orm.DataSeries
	rtDeps := &RuntimeDeps{Core: coreState, Clock: clock, Exchange: ex}
	feeder, feedErr := NewHistSeriesFeederWithRuntimeDeps(
		rtDeps,
		repo,
		cat.GetDataSource(orm.SeriesSourceFundingRate).Info(),
		sub,
		func(evt *orm.DataSeries) {
			emitted = append(emitted, evt)
		},
		baseMS,
	)
	if feedErr != nil {
		t.Fatalf("NewHistSeriesFeederWithRuntimeDeps failed: %v", feedErr)
	}
	feeder.SetEndMS(baseMS + 2*stepMS)
	feeder.SetSeek(baseMS)
	nextVis := feeder.getNextMS()
	if nextVis != baseMS+stepMS {
		t.Fatalf("expected first funding settlement visible at EndMS=%d (08:00), got %d", baseMS+stepMS, nextVis)
	}
	if err := feeder.RunBatch(feeder.GetBatch()); err != nil {
		t.Fatalf("RunBatch failed: %v", err)
	}
	if len(emitted) != 1 || emitted[0].EndMS != baseMS+stepMS || emitted[0].IsWarmUp {
		t.Fatalf("unexpected emitted event: %+v", emitted)
	}
}

func TestFundingRateSourceOfflineAndBTNoKlineDownloadSkipNetwork(t *testing.T) {
	const symbol = "BTC/USDT:USDT"
	const baseMS = int64(1704067200000)
	const stepMS = int64(8 * 3600 * 1000)

	for _, mode := range []string{"bt_no_kline_download", "net_disable"} {
		t.Run(mode, func(t *testing.T) {
			ex := newMockFundingExchange(symbol)
			ex.apiRates = []*banexg.FundingRate{
				{Symbol: symbol, Timestamp: baseMS + stepMS, FundingRate: 0.0001},
			}
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
				Config:   snap,
				Exchange: ex,
			})
			repo := newMemFundingRepo()
			exs := &orm.ExSymbol{ID: 21, Symbol: symbol, Exchange: "mockex", Market: "linear"}
			// Pre-populate 1 local row and verify offline ensure neither hits network nor overwrites coverage.
			_ = repo.InsertSeriesBatch(context.Background(), orm.NewFundingRateSeriesInfo("8h"), []*orm.DataRecord{{
				Sid: exs.ID, TimeMS: baseMS, EndMS: baseMS + stepMS, Closed: true, Values: map[string]any{"rate": -0.00025},
			}})
			sub := &orm.Subscription{Source: orm.SeriesSourceFundingRate, ExSymbol: exs, TimeFrame: "8h"}
			if err := cat.EnsureSeriesSubsRange(context.Background(), repo, []*orm.Subscription{sub}, baseMS, baseMS+3*stepMS); err != nil {
				t.Fatalf("offline EnsureSeriesSubsRange failed: %v", err)
			}
			if ex.historyCalls != 0 || ex.archiveCalls != 0 {
				t.Fatalf("expected zero network calls in %s mode, got history=%d archive=%d", mode, ex.historyCalls, ex.archiveCalls)
			}
			if len(repo.coverage[exs.ID]) != 0 {
				t.Fatalf("offline ensure must not write synthetic empty coverage, got %+v", repo.coverage[exs.ID])
			}
			page, readErr := cat.ReadSubscriptionPage(context.Background(), repo, nil, *sub, baseMS, baseMS+3*stepMS, 10)
			if readErr != nil || len(page) != 1 {
				t.Fatalf("expected pre-populated local row to remain readable, got len=%d err=%v", len(page), readErr)
			}
			if rate, _ := page[0].FloatValueDefault("rate"); math.Abs(rate+0.00025) > 1e-12 {
				t.Fatalf("unexpected local rate %v", rate)
			}
		})
	}
}
