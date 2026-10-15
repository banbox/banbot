package data

import (
	"context"
	"reflect"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type pageTrackingRepo struct {
	stubSeriesRepo
	limits []int
}

// The kline DB boundary is injected; provider subscription/warmup/seek remains real.
type subscriptionKlineStub struct {
	*DBSeriesFeeder
	warmups map[string]int
}

func (f *subscriptionKlineStub) WarmTfs(since int64, nums map[string]int, _ *utils.PrgBar) (int64, map[string][2]int, *errs.Error) {
	f.warmups = nums
	return since, nil, nil
}
func (f *subscriptionKlineStub) SetSeek(since int64) {
	f.nextMS = since
	f.caches = make([]*orm.DataSeries, f.BatchRows)
}
func (f *subscriptionKlineStub) DownIfNeed(*orm.Queries, banexg.BanExchange, *utils.PrgBar) *errs.Error {
	return nil
}

func (f *subscriptionKlineStub) SubTfs(tfs []string, _ bool) []string {
	for _, tf := range tfs {
		f.States = append(f.States, &PairTFCache{TimeFrame: tf, TFSecs: 3600})
	}
	return tfs
}

func (r *pageTrackingRepo) QuerySeriesRange(ctx context.Context, info *orm.SeriesInfo, sid int32, start, end int64, limit int) ([]*orm.DataRecord, *errs.Error) {
	r.limits = append(r.limits, limit)
	return r.stubSeriesRepo.QuerySeriesRange(ctx, info, sid, start, end, limit)
}

func TestHistoryDrainUsesVisibleTimeTiesAndFinalBatch(t *testing.T) {
	old := btime.CurTimeMS
	t.Cleanup(func() { btime.CurTimeMS = old })
	info := orm.NewSeriesInfo("macro", "1m", []orm.SeriesField{{Name: "value", Type: "float"}})
	var trace []int64
	var drains []int64
	var feeders []IHistFeeder
	for _, sid := range []int32{2, 1} {
		repo := &stubSeriesRepo{queryRows: []*orm.DataRecord{
			{TimeMS: 100, EndMS: 200, Values: map[string]any{"value": 1.0}},
			{TimeMS: 101, EndMS: 200, Values: map[string]any{"value": 2.0}},
			{TimeMS: 250, EndMS: 350, Values: map[string]any{"value": 3.0}},
		}}
		f, err := NewHistSeriesFeeder(repo, info, &strat.DataSub{ExSymbol: &orm.ExSymbol{ID: sid}, TimeFrame: "1m"}, func(row *orm.DataSeries) { trace = append(trace, row.EndMS) }, 0)
		if err != nil {
			t.Fatal(err)
		}
		f.BatchRows = 1
		f.SetEndMS(400)
		f.SetSeek(100)
		feeders = append(feeders, f)
	}
	err := RunHistFeedersWithDrainObserver(nil, func() []IHistFeeder { return feeders }, make(chan int, 1), nil, func(ms int64) *errs.Error {
		drains = append(drains, ms)
		if ms == 200 && len(trace) != 4 {
			t.Fatalf("drained before all ties: %v", trace)
		}
		if ms == 350 && len(trace) != 6 {
			t.Fatalf("final drain before events: %v", trace)
		}
		return nil
	})
	if err != nil || !reflect.DeepEqual(drains, []int64{200, 350}) {
		t.Fatalf("drains=%v err=%v", drains, err)
	}
}

func TestHistoryDrainDoesNotPublishReadFailureAndPropagatesObserverError(t *testing.T) {
	info := orm.NewSeriesInfo("macro", "1m", []orm.SeriesField{{Name: "value", Type: "float"}})
	f, err := NewHistSeriesFeeder(&stubSeriesRepo{queryErr: testErr("read failure")}, info, &strat.DataSub{ExSymbol: &orm.ExSymbol{ID: 1}}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.SetSeek(10)
	drains := 0
	err2 := RunHistFeedersWithDrainObserver(nil, func() []IHistFeeder { return []IHistFeeder{f} }, nil, nil, func(int64) *errs.Error { drains++; return nil })
	if err2 == nil || drains != 0 {
		t.Fatalf("failed read emitted drain: count=%d err=%v", drains, err2)
	}
	var runs []histFeederBatch
	stub := &stubHistFeeder{symbol: "one", times: []int64{100, 200}, runs: &runs}
	wantErr := testErr("observer failure")
	err2 = RunHistFeedersWithDrainObserver(nil, func() []IHistFeeder { return []IHistFeeder{stub} }, nil, nil, func(int64) *errs.Error { return wantErr })
	if err2 != wantErr || len(runs) != 1 {
		t.Fatalf("observer error did not stop: err=%v runs=%v", err2, runs)
	}
}

func TestHistSeriesPagesReleaseRowsAndPreserveValues(t *testing.T) {
	old := btime.CurTimeMS
	t.Cleanup(func() { btime.CurTimeMS = old })
	info := orm.NewSeriesInfo("macro", "1m", []orm.SeriesField{{Name: "value", Type: "float"}, {Name: "label", Type: "string"}, {Name: "flag", Type: "bool"}, {Name: "n", Type: "int"}})
	values := map[string]any{"value": nil, "label": "x", "flag": true, "n": int64(3)}
	repo := &pageTrackingRepo{stubSeriesRepo: stubSeriesRepo{queryRows: []*orm.DataRecord{
		{TimeMS: 100, EndMS: 200, Values: values}, {TimeMS: 200, EndMS: 300, Values: values}, {TimeMS: 300, EndMS: 400, Values: values},
	}}}
	count := 0
	f, err := NewHistSeriesFeeder(repo, info, &strat.DataSub{ExSymbol: &orm.ExSymbol{ID: 1}}, func(row *orm.DataSeries) {
		count++
		if !reflect.DeepEqual(row.Values, values) {
			t.Fatalf("value types/null changed: %v", row.Values)
		}
	}, 200)
	if err != nil {
		t.Fatal(err)
	}
	f.BatchRows = 2
	f.SetEndMS(400)
	f.SetSeek(100)
	if err := RunHistFeeders(func() []IHistFeeder { return []IHistFeeder{f} }, nil, nil); err != nil {
		t.Fatal(err)
	}
	if count != 3 || len(f.rows) != 0 || !reflect.DeepEqual(repo.limits, []int{2, 2, 2}) {
		t.Fatalf("page replay count=%d retained=%d queries=%v", count, len(f.rows), repo.limits)
	}
}

func TestSeriesPrefetchBudgetAcross500Streams(t *testing.T) {
	oldRange, oldTime, oldMode := config.TimeRange, btime.CurTimeMS, core.BackTestMode
	config.TimeRange = &config.TimeTuple{StartMS: 100, EndMS: 1000}
	core.BackTestMode = true
	btime.CurTimeMS = 100
	t.Cleanup(func() { config.TimeRange = oldRange; btime.CurTimeMS = oldTime; core.BackTestMode = oldMode })
	catalog := NewDataSourceCatalog()
	src := newStubRegistrySource("budget_macro")
	if err := catalog.RegisterDataSource(src); err != nil {
		t.Fatal(err)
	}
	p := NewHistProviderWithCatalog(catalog, nil, nil, nil, nil, false, nil)
	repo := &pageTrackingRepo{}
	for i := 0; i < 10; i++ {
		repo.queryRows = append(repo.queryRows, &orm.DataRecord{TimeMS: 100 + int64(i)*10, EndMS: 110 + int64(i)*10, Values: map[string]any{"value": float64(i)}})
	}
	p.seriesRepo = repo
	if err := p.SetSeriesPrefetch(20_000, 1000); err != nil {
		t.Fatal(err)
	}
	var subs []Subscription
	for sid := int32(1); sid <= 500; sid++ {
		subs = append(subs, Subscription{Source: src.info.Name, ExSymbol: &orm.ExSymbol{ID: sid}, TimeFrame: "1d"})
	}
	if err := p.SetSubscriptions(subs); err != nil {
		t.Fatal(err)
	}
	if err := p.SetSubscriptions(subs); err == nil {
		t.Fatal("generic plan replaced after initialization")
	}
	retained := 0
	for _, f := range p.series {
		retained += len(f.rows)
	}
	if len(p.series) != 500 || retained != 1000 {
		t.Fatalf("streams=%d retained=%d", len(p.series), retained)
	}
	for _, limit := range repo.limits {
		if limit != 2 {
			t.Fatalf("unbounded page: %d", limit)
		}
	}
	if err := p.SetSeriesPrefetch(2, 5); err == nil {
		t.Fatal("changed budget after subscription")
	}
	q := NewHistProviderWithCatalog(catalog, nil, nil, nil, nil, false, nil)
	if err := q.SetSeriesPrefetch(2, 499); err != nil {
		t.Fatal(err)
	}
	if err := q.SetSubscriptions(subs); err == nil {
		t.Fatal("insufficient budget admitted")
	}
}

func TestHistSeriesCancellationBeforeRead(t *testing.T) {
	state, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state.Stop()
	repo := &pageTrackingRepo{}
	f, createErr := NewHistSeriesFeederWithRuntimeDeps(&RuntimeDeps{Core: state}, repo, orm.NewSeriesInfo("macro", "1m", []orm.SeriesField{{Name: "value", Type: "float"}}), &strat.DataSub{ExSymbol: &orm.ExSymbol{ID: 1}}, nil, 0)
	if createErr != nil {
		t.Fatal(createErr)
	}
	f.SetSeek(100)
	if len(repo.limits) != 0 || f.loadErr == nil {
		t.Fatalf("cancelled feeder queried: limits=%v err=%v", repo.limits, f.loadErr)
	}
}

func TestHistoryDrainCancellationDuringCallback(t *testing.T) {
	state, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Stop()
	var runs []histFeederBatch
	f := &stubHistFeeder{symbol: "one", times: []int64{100}, runs: &runs, onRun: state.Stop}
	drains := 0
	if err := RunHistFeedersWithDrainObserver(&RuntimeDeps{Core: state}, func() []IHistFeeder { return []IHistFeeder{f} }, nil, nil, func(int64) *errs.Error { drains++; return nil }); err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || drains != 0 {
		t.Fatalf("cancelled callback published drain: runs=%v drains=%d", runs, drains)
	}
}

func TestGenericKlineAndSideSourcesSharePrefetchBudget(t *testing.T) {
	oldRange, oldTime, oldMode := config.TimeRange, btime.CurTimeMS, core.BackTestMode
	config.TimeRange = &config.TimeTuple{StartMS: 100, EndMS: 1000}
	core.BackTestMode = true
	btime.CurTimeMS = 100
	t.Cleanup(func() { config.TimeRange = oldRange; btime.CurTimeMS = oldTime; core.BackTestMode = oldMode })
	catalog := NewDataSourceCatalog()
	src := newStubRegistrySource("mixed_budget")
	if err := catalog.RegisterDataSource(src); err != nil {
		t.Fatal(err)
	}
	p := NewHistProviderWithCatalog(catalog, nil, nil, nil, nil, false, nil)
	p.SetAllowDownload(false)
	p.seriesRepo = &stubSeriesRepo{queryRows: []*orm.DataRecord{{TimeMS: 100, EndMS: 200, Values: map[string]any{"value": 1.0}}, {TimeMS: 200, EndMS: 300, Values: map[string]any{"value": 2.0}}}}
	if err := p.SetSeriesPrefetch(20_000, 3); err != nil {
		t.Fatal(err)
	}
	var kline *subscriptionKlineStub
	p.newFeeder = func(pair string, tfs []string) (IHistDataFeeder, *errs.Error) {
		kline = &subscriptionKlineStub{DBSeriesFeeder: &DBSeriesFeeder{SeriesFeeder: SeriesFeeder{Feeder: Feeder{ExSymbol: &orm.ExSymbol{ID: 1, Symbol: pair}}}, TfSeriesLoader: &TfSeriesLoader{}}}
		configureKlineSubscriptions(kline.DBSeriesFeeder, p.genericKlineSubs[pair], p.genericPageRows)
		kline.SubTfs(tfs, false)
		return kline, nil
	}
	subs := []Subscription{
		{Source: orm.SeriesSourceKline, ExSymbol: &orm.ExSymbol{ID: 1, Symbol: "BTC/USDT"}, TimeFrame: "1h", Fields: []string{"nullable"}, WarmupNum: 2},
		{Source: orm.SeriesSourceKline, ExSymbol: &orm.ExSymbol{ID: 1, Symbol: "BTC/USDT"}, TimeFrame: "1h", Fields: []string{"label"}, WarmupNum: 5},
		{Source: src.info.Name, ExSymbol: &orm.ExSymbol{ID: 2}, TimeFrame: "1d"},
		{Source: src.info.Name, ExSymbol: &orm.ExSymbol{ID: 3}, TimeFrame: "1d"},
	}
	if err := p.SetSubscriptions(subs); err != nil {
		t.Fatal(err)
	}
	retained := len(kline.caches)
	for _, f := range p.series {
		retained += len(f.rows)
	}
	if retained != 3 || kline.BatchRows != 1 || kline.warmups["1h"] != 5 {
		t.Fatalf("mixed streams budget/warmup: rows=%d page=%d warmup=%v", retained, kline.BatchRows, kline.warmups)
	}
	fields := kline.Feeder.subscriptionFields["1h"]
	for _, name := range []string{"nullable", "label"} {
		found := false
		for _, field := range fields {
			if field == name {
				found = true
			}
		}
		if !found {
			t.Fatalf("generic kline projection lost %s", name)
		}
	}
}

func TestGenericSubscriptionInitializationFailureCannotReplay(t *testing.T) {
	oldRange, oldTime, oldMode := config.TimeRange, btime.CurTimeMS, core.BackTestMode
	config.TimeRange = &config.TimeTuple{StartMS: 100, EndMS: 1000}
	core.BackTestMode = true
	btime.CurTimeMS = 100
	t.Cleanup(func() { config.TimeRange = oldRange; btime.CurTimeMS = oldTime; core.BackTestMode = oldMode })
	catalog := NewDataSourceCatalog()
	src := newStubRegistrySource("invalid_projection")
	if err := catalog.RegisterDataSource(src); err != nil {
		t.Fatal(err)
	}
	p := NewHistProviderWithCatalog(catalog, nil, nil, nil, nil, false, nil)
	p.seriesRepo = &stubSeriesRepo{queryErr: testErr("read failure")}
	subs := []Subscription{{Source: src.info.Name, ExSymbol: &orm.ExSymbol{ID: 1}, TimeFrame: "1d"}}
	if err := p.SetSubscriptions(subs); err == nil {
		t.Fatal("source failure not returned")
	}
	if err := p.LoopMain(); err == nil {
		t.Fatal("partial generic plan was replayed")
	}
	if err := p.SetSubscriptions(subs); err == nil {
		t.Fatal("failed provider was reused")
	}
}

func TestLegacySeriesSubsUnionRetainsDefaultProjection(t *testing.T) {
	oldRange, oldTime, oldMode := config.TimeRange, btime.CurTimeMS, core.BackTestMode
	config.TimeRange = &config.TimeTuple{StartMS: 100, EndMS: 1000}
	core.BackTestMode = true
	btime.CurTimeMS = 100
	t.Cleanup(func() { config.TimeRange = oldRange; btime.CurTimeMS = oldTime; core.BackTestMode = oldMode })
	catalog := NewDataSourceCatalog()
	src := newStubRegistrySource("legacy_union")
	src.info.Binding.Fields = append(src.info.Binding.Fields, orm.SeriesField{Name: "label", Type: "string"})
	if err := catalog.RegisterDataSource(src); err != nil {
		t.Fatal(err)
	}
	p := NewHistProviderWithCatalog(catalog, nil, nil, nil, nil, false, nil)
	p.seriesRepo = &stubSeriesRepo{}
	exs := &orm.ExSymbol{ID: 1}
	if err := p.SetSeriesSubs([]*strat.DataSub{{Source: src.info.Name, ExSymbol: exs, TimeFrame: "1d"}, {Source: src.info.Name, ExSymbol: exs, TimeFrame: "1d", Fields: []string{"value"}}}); err != nil {
		t.Fatal(err)
	}
	if len(p.series) != 1 {
		t.Fatalf("duplicate feeds: %d", len(p.series))
	}
	for _, f := range p.series {
		if len(f.info.Binding.Fields) != 2 {
			t.Fatalf("default projection narrowed: %+v", f.info.Binding.Fields)
		}
	}
}
