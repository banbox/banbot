package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/banbox/banbot/core"
	"gopkg.in/yaml.v3"
)

func TestLoadConfig(t *testing.T) {
	args := CmdArgs{}
	err := LoadConfig(&args)
	if err != nil {
		fmt.Printf("load data error: %s", err)
		return
	}
	data, err2 := yaml.Marshal(Data)
	if err2 != nil {
		fmt.Printf("dump data error: %s", err2)
		return
	}
	fmt.Println("result: \n", string(data))
}

func TestParseConfigsTimerangeOverridesEarlierStartEnd(t *testing.T) {
	cfg := parseLayeredTimeConfig(t,
		"time_start: '20240101'\ntime_end: '20260101'\n",
		"timerange: 20240201-20240301\n",
	)

	wantStart, wantEnd, err := ParseTimeRange("20240201-20240301")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TimeStart != "" || cfg.TimeEnd != "" {
		t.Fatalf("legacy time fields survived timerange override: %q - %q", cfg.TimeStart, cfg.TimeEnd)
	}
	if cfg.TimeRange.StartMS != wantStart || cfg.TimeRange.EndMS != wantEnd {
		t.Fatalf("resolved range = %d-%d, want %d-%d", cfg.TimeRange.StartMS, cfg.TimeRange.EndMS, wantStart, wantEnd)
	}
}

func TestParseConfigsStartEndOverrideEarlierTimerange(t *testing.T) {
	cfg := parseLayeredTimeConfig(t,
		"timerange: 20240101-20260101\n",
		"time_start: '20240201'\ntime_end: '20240301'\n",
	)

	wantStart, wantEnd, err := ParseTimeRange("20240201-20240301")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TimeRangeRaw != "" {
		t.Fatalf("timerange survived time_start/time_end override: %q", cfg.TimeRangeRaw)
	}
	if cfg.TimeRange.StartMS != wantStart || cfg.TimeRange.EndMS != wantEnd {
		t.Fatalf("resolved range = %d-%d, want %d-%d", cfg.TimeRange.StartMS, cfg.TimeRange.EndMS, wantStart, wantEnd)
	}
}

func parseLayeredTimeConfig(t *testing.T, layers ...string) *Config {
	t.Helper()
	dir := t.TempDir()
	paths := make([]string, 0, len(layers))
	for i, layer := range layers {
		path := filepath.Join(dir, fmt.Sprintf("%d.yml", i))
		if err := os.WriteFile(path, []byte(layer), 0644); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	cfg, err := ParseConfigs(paths, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Apply(&CmdArgs{}); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestRunPolicyClonePreservesFieldsWithoutSharingMutableState(t *testing.T) {
	orig := &RunPolicyConfig{
		Index:         2,
		Name:          "clone_probe",
		Filters:       []*CommonPairFilter{{Name: "VolumePairFilter", Items: map[string]interface{}{"limit": 12}}},
		TimeFrames:    "1h,4h",
		RunTimeframes: []string{"1h", "4h"},
		RefineTF:      "15m",
		MaxPair:       8,
		MaxOpen:       7,
		MaxSimulOpen:  3,
		OrderBarMax:   500,
		StakeRate:     1.75,
		Dirt:          "long",
		StopLoss:      "2%",
		StratPerf:     &StratPerfConfig{Enable: true, MinOdNum: 6, MidWeight: 0.4},
		Pairs:         []string{"BTC/USDT:USDT", "ETH/USDT:USDT"},
		Params:        map[string]float64{"threshold": 0.003938123456789},
		PairParams:    map[string]map[string]float64{"BTC/USDT:USDT": {"threshold": 0.0042}},
		More:          map[string]interface{}{"custom": map[string]interface{}{"enabled": true}},
		defs:          map[string]*core.Param{"threshold": {Name: "threshold", Min: 0.001, Max: 0.01}},
		Score:         9.5,
	}

	clone := orig.Clone()
	if !reflect.DeepEqual(clone, orig) {
		t.Fatalf("clone lost fields:\n got: %#v\nwant: %#v", clone, orig)
	}

	clone.Filters[0].Items["limit"] = 99
	clone.RunTimeframes[0] = "5m"
	clone.StratPerf.MinOdNum = 99
	clone.Pairs[0] = "XRP/USDT:USDT"
	clone.Params["threshold"] = 1
	clone.PairParams["BTC/USDT:USDT"]["threshold"] = 1
	clone.More["custom"].(map[string]interface{})["enabled"] = false
	clone.defs["threshold"].Min = 1

	if got := orig.Filters[0].Items["limit"]; got != 12 {
		t.Fatalf("clone filter mutation changed original: %v", got)
	}
	if orig.RunTimeframes[0] != "1h" || orig.StratPerf.MinOdNum != 6 || orig.Pairs[0] != "BTC/USDT:USDT" {
		t.Fatal("clone slice or struct mutation changed original")
	}
	if orig.Params["threshold"] != 0.003938123456789 || orig.PairParams["BTC/USDT:USDT"]["threshold"] != 0.0042 {
		t.Fatal("clone parameter mutation changed original")
	}
	if got := orig.More["custom"].(map[string]interface{})["enabled"]; got != true {
		t.Fatalf("clone inline config mutation changed original: %v", got)
	}
	if orig.defs["threshold"].Min != 0.001 {
		t.Fatal("clone hyperparameter definition mutation changed original")
	}
}

func TestRunPolicyToYamlRoundTrip(t *testing.T) {
	want := &RunPolicyConfig{
		Name:          "yaml_probe",
		Filters:       []*CommonPairFilter{{Name: "VolumePairFilter", Items: map[string]interface{}{"limit": 12}}},
		TimeFrames:    "1h,4h",
		RunTimeframes: []string{"1h", "4h"},
		RefineTF:      "15m",
		MaxPair:       8,
		MaxOpen:       7,
		MaxSimulOpen:  3,
		OrderBarMax:   500,
		StakeRate:     1.75,
		Dirt:          "long",
		StopLoss:      "2%",
		StratPerf:     &StratPerfConfig{Enable: true, MinOdNum: 6, MidWeight: 0.4},
		Pairs:         []string{"BTC/USDT:USDT"},
		Params:        map[string]float64{"threshold": 0.003938123456789},
		PairParams:    map[string]map[string]float64{"BTC/USDT:USDT": {"threshold": 0.0042}},
		More:          map[string]interface{}{"custom_flag": true},
		Index:         4,
		Score:         9.5,
	}

	text := want.ToYaml()
	if text != want.ToYaml() {
		t.Fatal("policy YAML is not deterministic")
	}
	if strings.Contains(text, "index:") || strings.Contains(text, "score:") {
		t.Fatalf("runtime fields leaked into policy YAML:\n%s", text)
	}
	var wrapper struct {
		RunPolicy []*RunPolicyConfig `yaml:"run_policy"`
	}
	if err := yaml.Unmarshal([]byte("run_policy:\n"+text), &wrapper); err != nil {
		t.Fatal(err)
	}
	if len(wrapper.RunPolicy) != 1 {
		t.Fatalf("round trip policy count = %d", len(wrapper.RunPolicy))
	}
	got := wrapper.RunPolicy[0]
	got.Index, got.Score = want.Index, want.Score
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("policy YAML lost fields:\n got: %#v\nwant: %#v\nYAML:\n%s", got, want, text)
	}
}
