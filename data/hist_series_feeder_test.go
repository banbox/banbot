package data

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

func TestProjectSeriesInfoUsesRequestedFields(t *testing.T) {
	info := orm.NewSeriesInfo("macro", "1d", []orm.SeriesField{
		{Name: "value", Type: "float"},
		{Name: "revision", Type: "int"},
	})
	got, err := projectSeriesInfo(info, []string{"revision"})
	if err != nil {
		t.Fatalf("projectSeriesInfo returned error: %v", err)
	}
	if len(got.Binding.Fields) != 1 || got.Binding.Fields[0].Name != "revision" {
		t.Fatalf("unexpected projected fields: %+v", got.Binding.Fields)
	}
	if len(info.Binding.Fields) != 2 {
		t.Fatalf("projection mutated source info: %+v", info.Binding.Fields)
	}
}

func TestHistSeriesFeederJoinsUnifiedTimeline(t *testing.T) {
	oldTime := btime.CurTimeMS
	t.Cleanup(func() { btime.CurTimeMS = oldTime })
	info := orm.NewSeriesInfo("macro", "1d", []orm.SeriesField{{Name: "value", Type: "float"}})
	repo := &stubSeriesRepo{queryRows: []*orm.DataRecord{
		{Sid: 7, TimeMS: 100, EndMS: 150, Closed: true, Values: map[string]any{"value": 1.0}},
		{Sid: 7, TimeMS: 200, EndMS: 250, Closed: true, Values: map[string]any{"value": 2.0}},
	}}
	var got []*orm.DataSeries
	feeder, err := NewHistSeriesFeeder(repo, info, &strat.DataSub{
		Source: "macro", ExSymbol: &orm.ExSymbol{ID: 7, Symbol: "CPI_US"}, TimeFrame: "1d", Fields: []string{"value"},
	}, func(evt *orm.DataSeries) {
		got = append(got, evt)
	}, 150)
	if err != nil {
		t.Fatalf("NewHistSeriesFeeder returned error: %v", err)
	}
	feeder.SetEndMS(251)
	feeder.SetSeek(100)
	if err := RunHistFeeders(func() []IHistFeeder { return []IHistFeeder{feeder} }, make(chan int, 1), nil); err != nil {
		t.Fatalf("RunHistFeeders returned error: %v", err)
	}
	if len(got) != 2 || !got[0].IsWarmUp || got[1].IsWarmUp {
		t.Fatalf("unexpected replay events: %+v", got)
	}
	if got[0].Source != "macro" || got[1].Values["value"] != 2.0 || btime.CurTimeMS != 250 {
		t.Fatalf("custom series did not use unified event path: events=%+v now=%d", got, btime.CurTimeMS)
	}
}

func TestHistSeriesFeederSurfacesReadErrors(t *testing.T) {
	info := orm.NewSeriesInfo("macro", "1d", []orm.SeriesField{{Name: "value", Type: "float"}})
	repo := &stubSeriesRepo{queryErr: testErr("read failed")}
	feeder, err := NewHistSeriesFeeder(repo, info, &strat.DataSub{
		Source: "macro", ExSymbol: &orm.ExSymbol{ID: 7}, TimeFrame: "1d",
	}, nil, 0)
	if err != nil {
		t.Fatalf("NewHistSeriesFeeder returned error: %v", err)
	}
	feeder.SetEndMS(200)
	feeder.SetSeek(100)
	batch := feeder.GetBatch()
	if batch == nil {
		t.Fatal("expected read error batch")
	}
	if err := feeder.RunBatch(batch); err == nil || !strings.Contains(err.Short(), "read failed") {
		t.Fatalf("expected read error, got %v", err)
	}
}

func TestHistProviderAddsCustomSeriesToReplay(t *testing.T) {
	resetDataSourcesForTest(t)
	oldRange := config.TimeRange
	oldTime := btime.CurTimeMS
	oldBackTest := core.BackTestMode
	config.TimeRange = &config.TimeTuple{StartMS: 100, EndMS: 400}
	btime.CurTimeMS = 100
	core.BackTestMode = true
	t.Cleanup(func() {
		config.TimeRange = oldRange
		btime.CurTimeMS = oldTime
		core.BackTestMode = oldBackTest
	})
	src := newStubRegistrySource("macro_replay")
	if err := RegisterDataSource(src); err != nil {
		t.Fatalf("RegisterDataSource failed: %v", err)
	}
	var got []*orm.DataSeries
	provider := NewHistProvider(func(evt *orm.DataSeries) { got = append(got, evt) }, nil, nil, false, nil)
	provider.seriesRepo = &stubSeriesRepo{queryRows: []*orm.DataRecord{
		{Sid: 7, TimeMS: 100, EndMS: 200, Closed: true, Values: map[string]any{"value": 3.0}},
	}}
	sub := &strat.DataSub{
		Source: src.info.Name, ExSymbol: &orm.ExSymbol{ID: 7, Symbol: "CPI_US"},
		TimeFrame: "1d", Fields: []string{"value"},
	}
	if err := provider.SetSeriesSubs([]*strat.DataSub{sub}); err != nil {
		t.Fatalf("SetSeriesSubs failed: %v", err)
	}
	if len(provider.series) != 1 {
		t.Fatalf("expected one custom replay feeder, got %d", len(provider.series))
	}
	if err := RunHistFeeders(func() []IHistFeeder {
		return []IHistFeeder{provider.series[strat.DataSubKey(src.info.Name, 7, "1d")]}
	}, make(chan int, 1), nil); err != nil {
		t.Fatalf("custom replay failed: %v", err)
	}
	if len(got) != 1 || got[0].Source != src.info.Name || got[0].Values["value"] != 3.0 {
		t.Fatalf("unexpected custom replay events: %+v", got)
	}
}

func TestMergeKlineFieldRowsAddsStoredExtras(t *testing.T) {
	base := &orm.DataSeries{TimeMS: 100, Values: map[string]any{"close": 3.0}}
	stored := &orm.DataSeries{TimeMS: 100, Values: map[string]any{"close": 2.0, "open_interest": 9.0}}
	got := mergeKlineFieldRows([]*orm.DataSeries{nil, base}, map[int64]*orm.DataSeries{100: stored})
	if len(got) != 2 || got[0] != nil || got[1] == base || got[1].Values["open_interest"] != 9.0 {
		t.Fatalf("unexpected enriched rows: %+v", got)
	}
	if _, ok := base.Values["open_interest"]; ok {
		t.Fatalf("enrichment mutated input row: %+v", base.Values)
	}
	if got[1].Values["close"] != 3.0 {
		t.Fatalf("stored defaults overwrote in-memory OHLCV: %+v", got[1].Values)
	}
	if hasExtraKlineFields(orm.DefaultKlineFields()) || !hasExtraKlineFields([]string{"close", "open_interest"}) {
		t.Fatal("extra kline field detection is incorrect")
	}
}

func testErr(msg string) *errs.Error {
	return errs.NewMsg(core.ErrDbReadFail, "%s", msg)
}
