package data

import (
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
	"reflect"
	"slices"
	"testing"
)

type projectedLiveFeeder struct {
	*SeriesFeeder
	warm map[string]int
}

func (f *projectedLiveFeeder) WarmTfs(_ int64, counts map[string]int, _ *utils.PrgBar) (int64, map[string][2]int, *errs.Error) {
	f.warm = counts
	var end int64
	for _, tf := range sortedTimeframes(counts) {
		var err *errs.Error
		end, err = f.warmTfWithErr(tf, []*orm.DataSeries{{Source: "kline", TimeMS: 100, EndMS: 100 + int64(utils2.TFToSecs(tf))*1000, Values: map[string]any{"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "volume": 1.0}}})
		if err != nil {
			return 0, nil, err
		}
	}
	return end, nil, nil
}

func TestLiveKlineSubscriptionsInstallDeclaredFieldsBeforeWarming(t *testing.T) {
	state := orm.NewSymbolStateWithIdentity("fixture", "linear")
	symbol := &orm.ExSymbol{ID: 1, Symbol: "asset", Exchange: "fixture", Market: "linear"}
	if err := state.CacheExSymbolChecked(symbol); err != nil {
		t.Fatal(err)
	}
	deps := &RuntimeDeps{Core: &core.State{LiveMode: true}, Clock: btime.NewClockState(false, nil), Symbols: state, Strategies: strat.NewState(), ExchangeName: "fixture", MarketType: "linear"}
	var callbacks []*orm.DataSeries
	feeder := &projectedLiveFeeder{SeriesFeeder: &SeriesFeeder{Feeder: Feeder{ExSymbol: symbol, deps: deps, symbols: state, tfBars: make(map[string][]*orm.DataSeries), subscriptionFields: map[string][]string{"1h": {"legacy"}}, CallBack: func(row *orm.DataSeries) { callbacks = append(callbacks, row) }}}}
	fields := []string{"integer", "label", "flag", "nullable"}
	feeder.readKlineFields = func(_ *orm.ExSymbol, tf string, projection []string, _, _ int64) ([]*orm.DataSeries, *errs.Error) {
		for _, field := range fields {
			if !slices.Contains(projection, field) {
				t.Fatalf("field %s absent before %s warming: %v", field, tf, projection)
			}
		}
		if tf == "1h" && !slices.Contains(projection, "legacy") {
			t.Fatal("legacy projection discarded")
		}
		return []*orm.DataSeries{{TimeMS: 100, Values: map[string]any{"integer": int64(9), "label": "x", "flag": true, "nullable": nil, "legacy": int64(7)}}}, nil
	}
	p := &LiveProvider{catalog: NewDataSourceCatalog(), deps: deps, symbols: state, Provider: Provider[IDataFeeder]{deps: deps, holders: make(map[string]IDataFeeder), wsSubs: strat.NewWsSubJobRegistryWithState(deps.Strategies, state)}}
	p.newFeeder = func(_ string, tfs []string) (IDataFeeder, *errs.Error) {
		for _, tf := range tfs {
			feeder.States = append(feeder.States, &PairTFCache{TimeFrame: tf, TFSecs: utils2.TFToSecs(tf)})
		}
		feeder.hour = &TfSeriesLoader{}
		return feeder, nil
	}
	subs := []Subscription{{Source: "kline", ExSymbol: symbol, TimeFrame: "1h", Fields: fields[:2], WarmupNum: 2}, {Source: "kline", ExSymbol: symbol, TimeFrame: "1h", Fields: fields[2:], WarmupNum: 3}, {Source: "kline", ExSymbol: symbol, TimeFrame: "2h", Fields: fields, WarmupNum: 2}}
	if err := p.SetKlineSubscriptions(subs); err != nil {
		t.Fatal(err)
	}
	if feeder.warm["1h"] != 3 || feeder.warm["2h"] != 2 || len(callbacks) != 2 || !callbacks[0].IsWarmUp || !callbacks[1].IsWarmUp {
		t.Fatal("field union or warmup count lost")
	}
	if feeder.hour == nil || !reflect.DeepEqual(feeder.hour.subscriptionFields, feeder.subscriptionFields) {
		t.Fatal("hour loader projection lost")
	}
	if err := feeder.fireCallBacks("1h", 3600000, []*orm.DataSeries{{Source: "kline", TimeMS: 100, EndMS: 3600100, Values: map[string]any{"close": 100.0}}}, nil); err != nil {
		t.Fatal(err)
	}
	if len(callbacks) != 3 || callbacks[2].IsWarmUp {
		t.Fatal("live callback did not retain subscription")
	}
	for _, row := range callbacks {
		for field, want := range map[string]any{"integer": int64(9), "label": "x", "flag": true, "nullable": nil, "legacy": int64(7)} {
			got, ok := row.Values[field]
			if !ok || !reflect.DeepEqual(got, want) {
				t.Fatalf("field %s type/NULL lost: %#v", field, row.Values)
			}
		}
	}
	if !reflect.DeepEqual(subs[0].Fields, fields[:2]) {
		t.Fatal("caller projection mutated")
	}
	if err := p.SetKlineSubscriptions(subs); err == nil {
		t.Fatal("second startup plan admitted")
	}
}

func TestLiveKlineSubscriptionsRejectMissingFactoryAndSideSource(t *testing.T) {
	p := &LiveProvider{catalog: NewDataSourceCatalog()}
	sub := Subscription{Source: "kline", ExSymbol: &orm.ExSymbol{ID: 1, Symbol: "asset"}, TimeFrame: "1h", Fields: []string{"custom"}, WarmupNum: 2}
	if err := p.SetKlineSubscriptions([]Subscription{sub}); err == nil {
		t.Fatal("missing feeder factory admitted")
	}
	sub.Source = "macro"
	if err := p.SetKlineSubscriptions([]Subscription{sub}); err == nil {
		t.Fatal("side source silently treated as kline")
	}
}

func TestLiveKlineSubscriptionsEmptyPlanPreservesMissingFactory(t *testing.T) {
	p := &LiveProvider{catalog: NewDataSourceCatalog()}
	if err := p.SetKlineSubscriptions(nil); err != nil {
		t.Fatal(err)
	}
	if p.newFeeder != nil {
		t.Fatal("empty plan installed a wrapper around missing feeder factory")
	}
	if err := p.SetKlineSubscriptions(nil); err == nil {
		t.Fatal("second startup plan admitted")
	}
}
