package data

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
)

type observationSource struct {
	*stubSeriesSource
	start int64
	err   error
}

func (s *observationSource) WarmupStart(ctx context.Context, sub *orm.Subscription, anchor int64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return s.start, s.err
}

func TestEventHistoricalReplayObservationWarmupAndTypedValues(t *testing.T) {
	oldRange, oldTime := config.TimeRange, btime.CurTimeMS
	oldBacktest := core.BackTestMode
	core.BackTestMode = true
	t.Cleanup(func() { core.BackTestMode = oldBacktest })
	t.Cleanup(func() { config.TimeRange, btime.CurTimeMS = oldRange, oldTime })
	config.TimeRange = &config.TimeTuple{StartMS: 100, EndMS: 400}
	btime.CurTimeMS = 100
	base := newStubRegistrySource("event_replay")
	base.info.TimeFrame = "event"
	base.info.Binding.Fields = []orm.SeriesField{{Name: "value", Type: "int"}, {Name: "nullable", Type: "string"}}
	source := &observationSource{stubSeriesSource: base, start: 40}
	catalog := NewDataSourceCatalog()
	if err := catalog.RegisterDataSource(source); err != nil {
		t.Fatal(err)
	}
	rows := []*orm.DataRecord{
		{Sid: 7, TimeMS: 40, EndMS: 41, Values: map[string]any{"value": int64(9007199254740993), "nullable": nil}},
		{Sid: 7, TimeMS: 80, EndMS: 81, Values: map[string]any{"value": int64(2)}},
		{Sid: 7, TimeMS: 210, EndMS: 211, Values: map[string]any{"value": int64(3)}},
		{Sid: 7, TimeMS: 390, EndMS: 391, Values: map[string]any{"value": int64(4)}},
	}
	var received []*orm.DataSeries
	p := NewHistProvider(func(evt *orm.DataSeries) { received = append(received, evt) }, nil, nil, false, nil)
	p.catalog, p.seriesRepo = catalog, &stubSeriesRepo{queryRows: rows}
	p.seriesBatchRows = 1
	sub := Subscription{Source: source.info.Name, ExSymbol: &orm.ExSymbol{ID: 7}, TimeFrame: "event", WarmupNum: 2}
	if err := p.SetSubscriptions([]Subscription{sub}); err != nil {
		t.Fatal(err)
	}
	if len(p.series) != 1 {
		t.Fatal("event stream did not create one reader")
	}
	if err := RunHistFeeders(func() []IHistFeeder { return []IHistFeeder{p.series[sub.Key().String()]} }, make(chan int, 1), nil); err != nil {
		t.Fatal(err)
	}
	if len(received) != 4 || !received[0].IsWarmUp || !received[1].IsWarmUp || received[2].IsWarmUp || received[3].IsWarmUp {
		t.Fatalf("wrong event warmup/replay: %+v", received)
	}
	if !reflect.DeepEqual(received[0].Values, rows[0].Values) || received[3].EndMS != 391 {
		t.Fatal("types, NULL, pagination or final batch lost")
	}
}

func TestObservationWarmupFailureDoesNotInstallPartialPlan(t *testing.T) {
	base := newStubRegistrySource("event_failure")
	base.info.TimeFrame = "event"
	source := &observationSource{stubSeriesSource: base, start: 40, err: errors.New("visible history incomplete")}
	catalog := NewDataSourceCatalog()
	if err := catalog.RegisterDataSource(source); err != nil {
		t.Fatal(err)
	}
	sub := Subscription{Source: base.info.Name, ExSymbol: &orm.ExSymbol{ID: 7}, TimeFrame: "event", WarmupNum: 2}
	if _, err := catalog.SubscriptionWarmupStart(context.Background(), []*orm.Subscription{&sub}, 100); err == nil {
		t.Fatal("incomplete history accepted")
	}
	source.err = nil
	for _, start := range []int64{-1, 100, 200} {
		source.start = start
		if _, err := catalog.SubscriptionWarmupStart(context.Background(), []*orm.Subscription{&sub}, 100); err == nil {
			t.Fatalf("invalid start %d accepted", start)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := catalog.SubscriptionWarmupStart(ctx, []*orm.Subscription{&sub}, 100); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestEventRangeBootstrapRejectsFutureAndForeignRows(t *testing.T) {
	base := newStubRegistrySource("event_range")
	base.info.TimeFrame = "event"
	sub := &orm.Subscription{Source: base.info.Name, ExSymbol: &orm.ExSymbol{ID: 7}, TimeFrame: "event"}
	for _, row := range []*orm.DataRecord{
		{Sid: 7, TimeMS: 200, EndMS: 201}, {Sid: 8, TimeMS: 100, EndMS: 101},
	} {
		base.rows = []*orm.DataRecord{row}
		repo := &stubSeriesRepo{}
		if err := EnsureSeriesRangeWithRepo(context.Background(), repo, base, sub, 100, 200); err == nil || repo.insertCalls != 0 {
			t.Fatalf("invalid observation written: %v", err)
		}
	}
}
