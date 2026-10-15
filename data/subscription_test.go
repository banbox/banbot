package data

import (
	"context"
	"reflect"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
)

func TestGenericSubscriptionsUnionAndCoverageWithoutJobs(t *testing.T) {
	catalog := NewDataSourceCatalog()
	src := newStubRegistrySource("generic_macro")
	src.info.Binding.Fields = []orm.SeriesField{{Name: "value", Type: "float"}, {Name: "label", Type: "string"}}
	src.rows = []*orm.DataRecord{{TimeMS: 100, EndMS: 200, Values: map[string]any{"value": nil, "label": "CPI"}}}
	if err := catalog.RegisterDataSource(src); err != nil {
		t.Fatal(err)
	}
	exs := &orm.ExSymbol{ID: 7, Symbol: "CPI"}
	subs := []Subscription{
		{Source: src.info.Name, ExSymbol: exs, TimeFrame: "1d", Fields: []string{"value"}, WarmupNum: 2},
		{Source: src.info.Name, ExSymbol: exs, TimeFrame: "1d", Fields: []string{"label"}, WarmupNum: 5},
	}
	got, err := catalog.NormalizeSubscriptions(subs)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].WarmupNum != 5 || !reflect.DeepEqual(got[0].Fields, []string{"value", "label"}) {
		t.Fatalf("merged subscriptions: %+v", got)
	}
	if !reflect.DeepEqual(subs[0].Fields, []string{"value"}) {
		t.Fatal("mutated input")
	}
	repo := &stubSeriesRepo{}
	for range 2 {
		if err := catalog.EnsureSubscriptionsRange(context.Background(), repo, subs, 100, 200); err != nil {
			t.Fatal(err)
		}
	}
	if src.fetchCount != 1 {
		t.Fatalf("coverage did not deduplicate fetches: %d", src.fetchCount)
	}
	// A known-empty range is covered as well; distinct SID/source keys aren't.
	if err := repo.UpdateSeriesCoverage(context.Background(), src.info, 8, 100, 200, nil); err != nil {
		t.Fatal(err)
	}
	missing, err2 := repo.MissingSeriesRanges(context.Background(), src.info, 8, 100, 200)
	if err2 != nil || len(missing) != 0 {
		t.Fatalf("known empty coverage: %v %v", missing, err2)
	}
	missing, _ = repo.MissingSeriesRanges(context.Background(), src.info, 9, 100, 200)
	if len(missing) != 1 {
		t.Fatal("coverage leaked across SID")
	}
	other := *src.info
	other.Name = "other"
	missing, _ = repo.MissingSeriesRanges(context.Background(), &other, 8, 100, 200)
	if len(missing) != 1 {
		t.Fatal("coverage leaked across sources")
	}
}

func TestGenericKlineFieldsWarmupTypesAndAdjustmentWithoutJobs(t *testing.T) {
	oldTime := btime.CurTimeMS
	t.Cleanup(func() { btime.CurTimeMS = oldTime })
	exs := &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT"}
	fields := []string{"integer", "label", "flag", "nullable"}
	var got *orm.DataSeries
	base := Feeder{ExSymbol: exs, deps: &RuntimeDeps{}, isWarmUp: true, CallBack: func(row *orm.DataSeries) { got = row }}
	feeder := &DBSeriesFeeder{SeriesFeeder: SeriesFeeder{Feeder: base}, TfSeriesLoader: &TfSeriesLoader{}}
	configureKlineSubscriptions(feeder, []Subscription{{Source: orm.SeriesSourceKline, ExSymbol: exs, TimeFrame: "1h", Fields: fields}}, 3)
	feeder.Feeder.readKlineFields = func(_ *orm.ExSymbol, _ string, projected []string, _, _ int64) ([]*orm.DataSeries, *errs.Error) {
		for _, name := range fields {
			found := false
			for _, f := range projected {
				if f == name {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing field %s in %v", name, projected)
			}
		}
		return []*orm.DataSeries{{TimeMS: 100, Values: map[string]any{"integer": int64(9), "label": "x", "flag": true, "nullable": nil}}}, nil
	}
	row := &orm.DataSeries{TimeMS: 100, EndMS: 3_600_100, Values: map[string]any{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 3.0}}
	adj := &orm.AdjInfo{Factor: 2}
	if err := feeder.Feeder.fireCallBacks("1h", 3_600_000, []*orm.DataSeries{row}, adj); err != nil {
		t.Fatal(err)
	}
	if got == nil || !got.Closed || !got.IsWarmUp || got.Adj != adj {
		t.Fatalf("warm/closed/adjustment lost: %+v", got)
	}
	want := map[string]any{"integer": int64(9), "label": "x", "flag": true, "nullable": nil}
	for name, val := range want {
		value, present := got.Values[name]
		if !present || !reflect.DeepEqual(value, val) {
			t.Fatalf("field %s lost type/null: %T %v", name, value, value)
		}
	}
	if _, present := got.Values["missing"]; present {
		t.Fatal("missing field became present")
	}
	if feeder.TfSeriesLoader.BatchRows != 3 {
		t.Fatal("kline page bound not configured")
	}
}
