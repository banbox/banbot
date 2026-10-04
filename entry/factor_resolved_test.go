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
	spec, err := loadFactorRunSpec([]string{path}, "")
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

func TestResolvedImportedValuesOverrideCommonDefaultsWithoutInventingOrigins(t *testing.T) {
	dir, fixture := factorYAMLFixture(t)
	spec, err := loadFactorRunSpec([]string{fixture}, "")
	if err != nil {
		t.Fatal(err)
	}
	configs, err := buildFactorConfigs(spec, runner.Weights)
	if err != nil {
		t.Fatal(err)
	}
	legacy := configs[0]
	legacy.InitialNAV, legacy.DecisionInterval, legacy.Manifest.Currency = 12345, 300000, "USDT"
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	var imported map[string]any
	if err := json.Unmarshal(raw, &imported); err != nil {
		t.Fatal(err)
	}
	delete(imported, "MaxPending")
	raw, err = json.Marshal(imported)
	if err != nil {
		t.Fatal(err)
	}
	legacyPath := filepath.Join(dir, "legacy.json")
	if err := os.WriteFile(legacyPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	common := filepath.Join(dir, "common.yml")
	if err := os.WriteFile(common, []byte("config_version: 2\nstake_currency: [USD]\nwallet_amounts: {USD: 10000}\nrun_timeframes: [1h]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	spec, err = loadFactorRunSpec([]string{common}, legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	configs, err = buildFactorConfigs(spec, runner.Weights)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "imported-resolved.json")
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
		if values[key].Value != want || values[key].Origin.Source != legacyPath+".yml" {
			t.Fatalf("imported %s value/origin lost: %+v", key, values[key])
		}
	}
	if values["max_pending"].Value != float64(64) || values["max_pending"].Origin.Kind != "derived" {
		t.Fatalf("omitted JSON field fabricated explicit origin: %+v", values["max_pending"])
	}
}
