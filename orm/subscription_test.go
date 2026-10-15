package orm

import (
	"math"
	"reflect"
	"testing"
)

func TestNeutralSubscriptionNormalizationAndWarmup(t *testing.T) {
	sub := Subscription{Source: " ", ExSymbol: &ExSymbol{ID: 7}, TimeFrame: " 1h ", Fields: []string{" label ", "label"}, WarmupNum: 2}
	got, err := NormalizeSubscription(sub)
	if err != nil || got.Source != SeriesSourceKline || got.Frequency != FrequencyBar || got.Projection != ProjectionSelected || !reflect.DeepEqual(got.Fields, []string{"label"}) {
		t.Fatalf("normalize: %+v %v", got, err)
	}
	if !reflect.DeepEqual(sub.Fields, []string{" label ", "label"}) {
		t.Fatal("mutated declaration")
	}
	start, err := SubscriptionWarmupStart(got, 10_000_000)
	if err != nil || start != 2_800_000 {
		t.Fatalf("warmup: %d %v", start, err)
	}
	if _, err := SubscriptionWarmupStart(got, math.MinInt64); err == nil {
		t.Fatal("accepted timestamp underflow")
	}
	event := Subscription{Source: "macro", ExSymbol: sub.ExSymbol, TimeFrame: "event"}
	start, err = SubscriptionWarmupStart(event, 10_000_000)
	if err != nil || start != 10_000_000 {
		t.Fatalf("zero event warmup: %d %v", start, err)
	}
	if event.Key().String() != "macro:7:event" {
		t.Fatal(event.Key())
	}
}
