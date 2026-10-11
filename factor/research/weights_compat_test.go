package research

import (
	"math"
	"reflect"
	"testing"
)

func compatibilityHistory(t *testing.T, values map[string][]float64) *ICHistory {
	t.Helper()
	h, err := NewICHistory(8, "forward", []string{"positive", "negative", "zero"})
	if err != nil {
		t.Fatal(err)
	}
	for column, rows := range values {
		for i, value := range rows {
			decision := int64(i + 1)
			if err := h.Add(10, ICSample{Column: column, Label: "forward", DecisionTime: decision, MatureAt: decision + 1, AvailableAt: decision + 1, IC: number(value), Samples: 2}); err != nil {
				t.Fatal(err)
			}
		}
	}
	return h
}

func TestLegacyWeightsCompatibility(t *testing.T) {
	cases := []struct {
		name    string
		values  map[string][]float64
		columns []string
		want    map[string]float64
	}{
		{name: "no history", columns: []string{"positive"}},
		{name: "empty columns", values: map[string][]float64{"positive": {.5}}},
		{name: "undeclared and empty", values: map[string][]float64{"positive": {.5}}, columns: []string{"", "undeclared"}},
		{name: "all zero", values: map[string][]float64{"zero": {0}}, columns: []string{"zero"}},
		{name: "signed and zero entry", values: map[string][]float64{"positive": {.5}, "negative": {-.25}, "zero": {0}}, columns: []string{"positive", "negative", "zero", "", "undeclared"}, want: map[string]float64{"positive": 2.0 / 3, "negative": -1.0 / 3, "zero": 0}},
		// Legacy duplicate columns count again in the denominator even though
		// the returned map contains one entry per column.
		{name: "duplicate columns", values: map[string][]float64{"positive": {.5}, "negative": {-.25}}, columns: []string{"positive", "positive", "negative"}, want: map[string]float64{"positive": .4, "negative": -.2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := compatibilityHistory(t, tc.values)
			got, ready, err := h.Weights(20, tc.columns)
			if err != nil || ready != (tc.want != nil) || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Weights = %v, %v, %v; want %v", got, ready, err, tc.want)
			}
			if h.lastAsOf != 20 {
				t.Fatalf("successful selection did not advance cursor: %d", h.lastAsOf)
			}
			if _, _, err := h.Weights(19, tc.columns); err == nil || err.Error() != "research: IC history time moved backwards; replay with a new history" || h.lastAsOf != 20 {
				t.Fatalf("backward selection = %v; cursor %d", err, h.lastAsOf)
			}
		})
	}
}

func TestLegacyWeightsMatchesDefaultQuality(t *testing.T) {
	h := compatibilityHistory(t, map[string][]float64{"positive": {.5, .25}, "negative": {-.25, -.5}, "zero": {0}})
	columns := []string{"positive", "negative", "zero", "undeclared"}
	legacy, legacyReady, err := h.Weights(20, columns)
	if err != nil {
		t.Fatal(err)
	}
	quality, qualityReady, err := h.QualityWeights(20, ComboSpec{Method: HistoryIC, Columns: columns})
	if err != nil || qualityReady != legacyReady || !reflect.DeepEqual(quality, legacy) {
		t.Fatalf("default quality = %v, %v, %v; legacy = %v, %v", quality, qualityReady, err, legacy, legacyReady)
	}
}

func TestLegacyAndQualityMeanCancellationDifference(t *testing.T) {
	// Both means are legitimate float64 calculations, but replacing the legacy
	// sum/count with running statistics changes readiness around zero.
	h := compatibilityHistory(t, map[string][]float64{"positive": {.1, .3, -.4}})
	legacy, legacyReady, err := h.Weights(20, []string{"positive"})
	if err != nil || legacyReady || legacy != nil {
		t.Fatalf("legacy cancellation = %v, %v, %v", legacy, legacyReady, err)
	}
	quality, qualityReady, err := h.QualityWeights(20, ComboSpec{Method: HistoryIC, Columns: []string{"positive"}})
	if err != nil || !qualityReady || math.Abs(quality["positive"]) != 1 {
		t.Fatalf("running mean cancellation = %v, %v, %v", quality, qualityReady, err)
	}
}

func TestLegacyWeightsSelectsOnlyVisibleMaturedIC(t *testing.T) {
	h := compatibilityHistory(t, nil)
	// Preloaded histories must apply visibility at selection time too.
	h.samples["positive"] = []ICSample{
		{DecisionTime: 1, MatureAt: 2, AvailableAt: 3, IC: number(.5), Samples: 2},
		{DecisionTime: 20, MatureAt: 21, AvailableAt: 21, IC: number(-1), Samples: 2},
		{DecisionTime: 2, MatureAt: 21, AvailableAt: 21, IC: number(-1), Samples: 2},
		{DecisionTime: 3, MatureAt: 4, AvailableAt: 21, IC: number(-1), Samples: 2},
		{DecisionTime: 4, MatureAt: 5, AvailableAt: 5, IC: number(math.NaN()), Samples: 2},
	}
	got, ready, err := h.Weights(20, []string{"positive"})
	if err != nil || !ready || got["positive"] != 1 {
		t.Fatalf("visible matured selection = %v, %v, %v", got, ready, err)
	}
}
