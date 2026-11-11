package entry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/factor/runner"
)

func TestResolvedFactorConfigurationReportsActualDefaultsAndOrigins(t *testing.T) {
	dir, path := factorYAMLFixture(t)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "      archive: data.gob", "      archive: data.gob\n      decision: {max_pending: 7}", 1))
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	spec, err := loadFactorYAMLSpec([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	configs, err := buildFactorConfigs(spec, runner.Weights)
	if err != nil {
		t.Fatal(err)
	}
	configs[0].ComputationContext.DataNamespace = "storage-fixture"
	output := filepath.Join(dir, "resolved.json")
	if err := writeResolvedFactorConfig(output, spec, configs); err != nil {
		t.Fatal(err)
	}
	var artifact struct {
		Version    int
		Strategies []map[string]resolvedFactorValue
	}
	raw, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Version != 1 || len(artifact.Strategies) != 1 {
		t.Fatalf("bad resolved artifact: %s", raw)
	}
	values := artifact.Strategies[0]
	if values["max_pending"].Value != float64(7) || values["max_pending"].Origin.Source != path || values["max_pending"].Origin.Kind != "file" {
		t.Fatalf("overridden decision origin lost: %+v", values["max_pending"])
	}
	if values["max_records"].Value != float64(100000) || values["max_records"].Origin.Kind != "derived" || values["latency_ms"].Value != float64(1) {
		t.Fatalf("actual defaults missing: %s", raw)
	}
	if values["account_initial_nav"].Value != float64(10000) || values["data_namespace"].Value != "storage-fixture" {
		t.Fatalf("runtime-derived values missing: %s", raw)
	}
	if strings.Contains(string(raw), "database") || strings.Contains(string(raw), "api_key") || strings.Contains(string(raw), "secret") {
		t.Fatal("resolved allowlist exposed a credential-bearing object")
	}
}

func TestResolvedYAMLOverridesCommonDefaultsWithoutInventingOrigins(t *testing.T) {
	dir, fixture := factorYAMLFixture(t)
	raw, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	overlay := filepath.Join(dir, "override.yml")
	body := strings.Replace(string(raw), "      archive: data.gob", "      archive: data.gob\n      initial_nav: 12345\n      manifest: {currency: USDT}\n      decision: {interval_ms: 300000}", 1)
	if err := os.WriteFile(overlay, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	spec, err := loadFactorYAMLSpec([]string{fixture, overlay})
	if err != nil {
		t.Fatal(err)
	}
	configs, err := buildFactorConfigs(spec, runner.Weights)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "resolved-overrides.json")
	if err := writeResolvedFactorConfig(output, spec, configs); err != nil {
		t.Fatal(err)
	}
	var artifact struct {
		Strategies []map[string]resolvedFactorValue
	}
	raw, err = os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	values := artifact.Strategies[0]
	for key, want := range map[string]any{"initial_nav": float64(12345), "decision_interval_ms": float64(300000), "currency": "USDT"} {
		if values[key].Value != want || values[key].Origin.Source != overlay {
			t.Fatalf("YAML %s value/origin lost: %+v", key, values[key])
		}
	}
	if values["max_pending"].Value != float64(64) || values["max_pending"].Origin.Kind != "derived" {
		t.Fatalf("omitted YAML field fabricated explicit origin: %+v", values["max_pending"])
	}
}

func TestPITKeysFromYAMLReachRunnerConfig(t *testing.T) {
	dir, fixture := factorYAMLFixture(t)
	raw, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		extra      string
		ratio, bps float64
		wantErr    bool
	}{
		{"", 0, 0, false},
		{"      pit_min_universe_ratio: 0.8\n      pit_delist_penalty_bps: 250\n", 0.8, 250, false},
		{"      pit_min_universe_ratio: -1\n      pit_delist_penalty_bps: -1\n", -1, -1, false},
		{"      pit_min_universe_ratio: 1.5\n", 0, 0, true},
		{"      pit_delist_penalty_bps: abc\n", 0, 0, true},
	} {
		overlay := filepath.Join(dir, "pit.yml")
		body := strings.Replace(string(raw), "      archive: data.gob\n", "      archive: data.gob\n"+tc.extra, 1)
		if err := os.WriteFile(overlay, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		spec, err := loadFactorYAMLSpec([]string{overlay})
		var configs []runner.Config
		if err == nil {
			configs, err = buildFactorConfigs(spec, runner.Weights)
		}
		if tc.wantErr {
			if err == nil {
				t.Fatalf("%q must be rejected", tc.extra)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%q: %v", tc.extra, err)
		}
		if configs[0].PITMinUniverseRatio != tc.ratio || configs[0].PITDelistPenaltyBps != tc.bps {
			t.Fatalf("%q: ratio=%v bps=%v", tc.extra, configs[0].PITMinUniverseRatio, configs[0].PITDelistPenaltyBps)
		}
	}
}
