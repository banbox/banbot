package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

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
