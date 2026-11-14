package config

import (
	"testing"

	"github.com/banbox/banbot/core"
)

func TestDefIntNil(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params map[string]float64
		want   int
	}{
		{name: "default", want: 7},
		{name: "override", params: map[string]float64{"period": 12.4}, want: 12},
		{name: "zero_override", params: map[string]float64{"period": 0}, want: 0},
		{name: "positive_half", params: map[string]float64{"period": 2.5}, want: 3},
		{name: "negative_fraction", params: map[string]float64{"period": -2.4}, want: -2},
		{name: "negative_half", params: map[string]float64{"period": -2.5}, want: -3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &RunPolicyConfig{Params: tc.params}
			if got := cfg.DefInt("period", 7, nil); got != tc.want {
				t.Fatalf("DefInt = %d, want %d", got, tc.want)
			}
			if cfg.defs != nil || len(cfg.HyperParams()) != 0 || cfg.IsInt("period") {
				t.Fatal("nil parameter registered a search definition")
			}
		})
	}
}

func TestDefIntNilPreservesDefinitions(t *testing.T) {
	existing := core.PUniform(1, 10)
	cfg := &RunPolicyConfig{
		Params: map[string]float64{"period": -2.5},
		defs:   map[string]*core.Param{"period": existing},
	}
	want := *existing
	if got := cfg.DefInt("period", 7, nil); got != -3 {
		t.Fatalf("DefInt = %d, want -3", got)
	}
	if got := cfg.DefInt("other", -7, nil); got != -7 {
		t.Fatalf("default DefInt = %d, want -7", got)
	}
	if len(cfg.defs) != 1 || cfg.defs["period"] != existing || *existing != want {
		t.Fatal("nil parameter changed existing search definitions")
	}
}

func TestDefIntRegistersDefinition(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    *core.Param
		mean float64
		rate float64
		key  string
	}{
		{name: "normal_defaults", p: core.PNorm(-2.5, 4.5), mean: 7, rate: 1, key: "period"},
		{name: "normal_explicit", p: &core.Param{VType: core.VTypeNorm, Min: -2.5, Max: 4.5, Mean: 3, Rate: 2, Name: "custom"}, mean: 3, rate: 2, key: "custom"},
		{name: "uniform", p: core.PUniform(-2.5, 4.5), mean: 7, rate: 0, key: "period"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &RunPolicyConfig{Params: map[string]float64{"period": 2.5}}
			if got := cfg.DefInt("period", 7, tc.p); got != 3 {
				t.Fatalf("DefInt = %d, want 3", got)
			}
			if len(cfg.defs) != 1 || cfg.defs["period"] != tc.p || !cfg.IsInt("period") {
				t.Fatal("non-nil parameter was not registered as an integer definition")
			}
			if tc.p.Min != -3.49 || tc.p.Max != 5.49 || tc.p.Mean != tc.mean || tc.p.Rate != tc.rate || tc.p.Name != tc.key {
				t.Fatalf("unexpected definition: %+v", tc.p)
			}
		})
	}
}
