package data

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
)

func TestSubscriptionDefaultProjectionSurvivesUnion(t *testing.T) {
	catalog := NewDataSourceCatalog()
	src := newStubRegistrySource("contract_macro")
	src.info.Binding.Fields = []orm.SeriesField{{Name: "value", Type: "float"}, {Name: "label", Type: "string"}, {Name: "flag", Type: "bool"}}
	if err := catalog.RegisterDataSource(src); err != nil {
		t.Fatal(err)
	}
	exs := &orm.ExSymbol{ID: 7, Symbol: "CPI"}
	for _, subs := range [][]Subscription{
		{{Source: src.info.Name, ExSymbol: exs, TimeFrame: "1d", Fields: []string{"label"}}, {Source: src.info.Name, ExSymbol: exs, TimeFrame: "1d", WarmupNum: 9}},
		{{Source: src.info.Name, ExSymbol: exs, TimeFrame: "1d", WarmupNum: 9}, {Source: src.info.Name, ExSymbol: exs, TimeFrame: "1d", Fields: []string{"label"}}},
	} {
		got, err := catalog.NormalizeSubscriptions(subs)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].WarmupNum != 9 || !fieldsContain(got[0].Fields, []string{"value", "label", "flag"}) {
			t.Fatalf("default projection was narrowed: %+v", got)
		}
		if !reflect.DeepEqual(got[0].SeriesFields, []string{"value"}) {
			t.Fatalf("numeric view changed: %v", got[0].SeriesFields)
		}
	}
}

func TestSubscriptionRejectsInvalidFrequencyWithoutPanic(t *testing.T) {
	catalog := NewDataSourceCatalog()
	exs := &orm.ExSymbol{ID: 7}
	for _, tf := range []string{"", "nonsense", "0h", "-1h", "9999999999999999999999h", "9223372036854775807h"} {
		t.Run(tf, func(t *testing.T) {
			if _, err := catalog.NormalizeSubscriptions([]Subscription{{ExSymbol: exs, TimeFrame: tf}}); err == nil {
				t.Fatal("accepted invalid frequency")
			}
		})
	}
	for _, sub := range []Subscription{
		{ExSymbol: exs, TimeFrame: "1h", Frequency: "tick"},
		{ExSymbol: exs, TimeFrame: "1h", Frequency: orm.FrequencyEvent},
		{ExSymbol: exs, TimeFrame: "event"},
		{ExSymbol: exs, TimeFrame: "1h", WarmupNum: -1},
		{ExSymbol: exs, TimeFrame: "1h", WarmupNum: int(^uint(0) >> 1)},
		{ExSymbol: exs, TimeFrame: "1h", Projection: orm.ProjectionSelected},
		{ExSymbol: exs, TimeFrame: "1h", Projection: "unknown"},
	} {
		if _, err := catalog.NormalizeSubscriptions([]Subscription{sub}); err == nil {
			t.Fatalf("accepted invalid subscription: %+v", sub)
		}
	}
}

func TestSubscriptionEventCompileAndInstallationBoundaries(t *testing.T) {
	catalog := NewDataSourceCatalog()
	src := newStubRegistrySource("contract_event")
	src.info.TimeFrame = "event"
	if err := catalog.RegisterDataSource(src); err != nil {
		t.Fatal(err)
	}
	sub := Subscription{Source: src.info.Name, ExSymbol: &orm.ExSymbol{ID: 7}, TimeFrame: "event"}
	items, err := catalog.NormalizeSubscriptions([]Subscription{sub})
	if err != nil || len(items) != 1 || items[0].Frequency != orm.FrequencyEvent {
		t.Fatalf("event compile: %+v %v", items, err)
	}
	if _, err = catalog.ActivateDataSources(context.Background(), legacySubscriptions(items), &stubDataSink{}); err != nil {
		t.Fatal(err)
	}
	if src.subscribeCount != 1 {
		t.Fatal("event source adapter was not called")
	}
	if err := catalog.EnsureSubscriptionsRange(context.Background(), &stubSeriesRepo{}, items, 1, 2); err != nil {
		t.Fatalf("event range bootstrap failed: %v", err)
	}
	if src.fetchCount != 1 {
		t.Fatal("event source historical adapter was not invoked")
	}
	sub.WarmupNum = 1
	if _, err := catalog.NormalizeSubscriptions([]Subscription{sub}); err == nil || !strings.Contains(err.Error(), "observation") {
		t.Fatalf("event count warmup: %v", err)
	}
}

func TestSubscriptionExplicitProjectionAndCatalogFrequency(t *testing.T) {
	catalog := NewDataSourceCatalog()
	src := newStubRegistrySource("contract_all")
	src.info.Binding.Fields = []orm.SeriesField{{Name: "value", Type: "float"}, {Name: "label", Type: "string"}}
	if err := catalog.RegisterDataSource(src); err != nil {
		t.Fatal(err)
	}
	sub := Subscription{Source: src.info.Name, ExSymbol: &orm.ExSymbol{ID: 7}, TimeFrame: "1d", Projection: orm.ProjectionAll}
	got, err := catalog.NormalizeSubscriptions([]Subscription{sub})
	if err != nil || len(got) != 1 || !reflect.DeepEqual(got[0].Fields, []string{"value", "label"}) {
		t.Fatalf("all projection: %+v %v", got, err)
	}
	if _, err := catalog.NormalizeSubscriptions(got); err != nil {
		t.Fatalf("normalized plan is not reusable: %v", err)
	}
	sub.TimeFrame = "1h"
	if _, err := catalog.NormalizeSubscriptions([]Subscription{sub}); err == nil {
		t.Fatal("accepted source frequency mismatch")
	}
}

func TestSubscriptionLegacyStreamKeyCompatibility(t *testing.T) {
	key := strat.DataSubKey("", 7, "1h")
	if key != "kline:7:1h" {
		t.Fatal(key)
	}
	source, sid, tf, ok := strat.ParseDataSubKey(key)
	if !ok || source != "kline" || sid != 7 || tf != "1h" {
		t.Fatalf("key roundtrip: %s %d %s %v", source, sid, tf, ok)
	}
}
