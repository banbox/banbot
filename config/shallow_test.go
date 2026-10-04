package config

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestV05ConfigTemplateLoadsWithoutSourceChanges(t *testing.T) {
	raw, err := os.ReadFile("testdata/v0.5.7.yml")
	if err != nil {
		t.Fatal(err)
	}
	path := migrationFixture(t, "config.yml", string(raw))
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(path, 0600) })
	spec, loadErr := LoadRunSpec(&CmdArgs{NoDefault: true, Configs: []string{path}}, false)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if spec.Config().RunPolicy[0].Engine != EngineTimeSeries || spec.Config().RunPolicy[0].CapitalWeight != nil {
		t.Fatal("legacy sizing changed")
	}
	actual, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(raw, actual) {
		t.Fatalf("load changed source: %v", err)
	}
	items, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(items) != 1 {
		t.Fatalf("read-only load created files: %v", err)
	}
}

func TestUnmarkedPoliciesRetainLegacyMoreThroughExport(t *testing.T) {
	raw := []byte(`time_start: '20240101'
run_policy:
  - name: Legacy
    id: {custom: [null, false]}
    account: strategy-parameter
    capital_weight: 'not-a-budget'
    factor: [custom, values]
    archive: '../user-value'
    engine: custom-engine
`)
	old, err := ParseYmlConfig(raw, "legacy.yml")
	if err != nil {
		t.Fatal(err)
	}
	spec, err := LoadRunSpec(&CmdArgs{NoDefault: true, ConfigData: string(raw)}, false)
	if err != nil {
		t.Fatal(err)
	}
	view, err := spec.Config().TimeSeriesConfig()
	if err != nil || !reflect.DeepEqual(old.RunPolicy, view.RunPolicy) {
		t.Fatalf("legacy More changed: %v", err)
	}
	exported, exportErr := spec.EffectiveYAML(false)
	if exportErr != nil {
		t.Fatal(exportErr)
	}
	if bytes.Contains(exported, []byte("config_version:")) || bytes.Contains(exported, []byte("engine: time_series")) {
		t.Fatalf("export reinterprets legacy More:\n%s", exported)
	}
	copy, err := ParseUnifiedYAML(exported, "copy.yml")
	if err != nil || !reflect.DeepEqual(copy.RunPolicy[0].More, old.RunPolicy[0].More) {
		t.Fatalf("round trip changed More: %v", err)
	}
}

func TestShallowFactorAliasesCanonicalOriginsAndAccounts(t *testing.T) {
	base := migrationFixture(t, "base.yml", "accounts: {a: {leverage: 3, binance: {prod: {api_key: key, api_secret: secret}}}}\nexecution: {history: global.sqlite, margin_rate: '0.1'}\n")
	overlay := migrationFixture(t, "overlay.yml", "execution: {accounts: {a: {history: cold/a.sqlite}}}\nrun_policy: [{name: CS, engine: factor, account: a, factor: {archive: input.gob, portfolio: {k: 3}}}]\n")
	spec, err := LoadRunSpec(&CmdArgs{NoDefault: true, Configs: []string{base, overlay}}, false)
	if err != nil {
		t.Fatal(err)
	}
	for field, relative := range map[string]string{"run_policy[0].archive": "input.gob", "accounts.a.history": "cold/a.sqlite"} {
		path, pathErr := spec.ResolvePath(field)
		origin, ok := spec.Origin(field)
		if pathErr != nil || path != filepath.Join(filepath.Dir(overlay), relative) || !ok || origin.Source != overlay {
			t.Fatalf("canonical origin %s: %s %v %v", field, path, origin, pathErr)
		}
	}
	cfg := spec.Config()
	if cfg.Root.Accounts["a"].Leverage != 3 || cfg.Root.Accounts["a"].Exchanges["binance"].Prod.APIKey != "key" || cfg.AccountExecution["a"]["history"] != "cold/a.sqlite" {
		t.Fatal("account execution displaced legacy account fields")
	}
	if _, exists := cfg.Execution["accounts"]; exists {
		t.Fatal("second account map survived normalization")
	}
	raw, marshalErr := yaml.Marshal(cfg)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	copy, err := ParseUnifiedYAML(raw, "copy.yml")
	if err != nil || !reflect.DeepEqual(cfg, copy) {
		t.Fatalf("shallow config round trip differs: %v\n%s", err, raw)
	}
	effective, exportErr := spec.EffectiveYAML(true)
	if exportErr != nil {
		t.Fatal(exportErr)
	}
	var fields map[string]any
	if err := yaml.Unmarshal(effective, &fields); err != nil {
		t.Fatal(err)
	}
	if _, exists := fields["config_version"]; exists {
		t.Fatal("export requires version marker")
	}
	policy := fields["run_policy"].([]any)[0].(map[string]any)
	if _, exists := policy["factor"]; exists {
		t.Fatal("export retains factor wrapper")
	}
	if bytes.Contains(effective, []byte("api_key: key")) || bytes.Contains(effective, []byte("api_secret: secret")) {
		t.Fatal("shallow export leaked secrets")
	}
	path := migrationFixture(t, "effective.yml", string(effective))
	reloaded, err := LoadRunSpec(&CmdArgs{NoDefault: true, Configs: []string{path}}, false)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := spec.ResolvePath("accounts.a.history")
	after, pathErr := reloaded.ResolvePath("accounts.a.history")
	if pathErr != nil || before != after {
		t.Fatal("exported relative path lost its source directory")
	}
}

func TestShallowConfigRejectsConflictsAndInvalidAdvancedValues(t *testing.T) {
	for _, raw := range []string{
		"run_policy: [{name: CS, engine: factor, archive: flat, factor: {archive: nested}}]",
		"accounts: {a: {history: flat}}\nexecution: {accounts: {a: {history: nested}}}",
		"execution: {accounts: {missing: {history: cold.sqlite}}}",
		"accounts: {a: {margin_rate: 'NaN'}}",
		"accounts: {a: {store: null}}",
		"run_policy: [{name: CS, engine: factor, expressions: {frequency: 1h}}]",
		"run_policy: [{name: CS, engine: factor, decision: {latency_ms: -1}}]",
	} {
		if _, err := ParseUnifiedYAML([]byte(raw), "invalid.yml"); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	for _, raw := range []string{
		"{run_policy: [{name: Demo, account: custom-value}]}",
		"template: &p {name: Demo, id: {legacy: true}}\nrun_policy: [*p]",
		"fields: &p {factor: [custom]}\nrun_policy: [{name: Demo, <<: *p}]",
	} {
		if _, err := ParseUnifiedYAML([]byte(raw), "old.yml"); err != nil {
			t.Fatalf("rejected old YAML syntax %s: %v", raw, err)
		}
	}
}

func TestExplicitTimeSeriesIdentitySurvivesMarkerlessRoundTrip(t *testing.T) {
	u, err := ParseUnifiedYAML([]byte("accounts: {a: {}}\nrun_policy: [{name: TS, engine: time_series, id: ts, account: a, capital_weight: 0.5}]"), "ts.yml")
	if err != nil {
		t.Fatal(err)
	}
	raw, marshalErr := yaml.Marshal(u)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if strings.Contains(string(raw), "config_version") || !strings.Contains(string(raw), "engine: time_series") {
		t.Fatalf("identity activation lost:\n%s", raw)
	}
	again, err := ParseUnifiedYAML(raw, "copy.yml")
	if err != nil || !reflect.DeepEqual(u, again) {
		t.Fatalf("TS identity round trip changed: %v", err)
	}
}

func TestLegacyParsersRejectUnsupportedMarkerlessExecution(t *testing.T) {
	for _, raw := range []string{
		"accounts: {a: {}}\nrun_policy: [{name: TS, engine: time_series, account: a, capital_weight: 0.5}]",
		"run_policy: [{name: CS, engine: factor, archive: input.gob}]",
		"accounts: {a: {history: cold.sqlite}}\nrun_policy: [{name: TS}]",
	} {
		if _, err := ParseYmlConfig([]byte(raw), "new.yml"); err == nil {
			t.Fatalf("legacy parser silently accepted new execution semantics: %s", raw)
		}
		path := migrationFixture(t, "new.yml", raw)
		if _, err := ParseConfigs([]string{path}, false); err == nil {
			t.Fatalf("legacy file parser silently accepted new execution semantics: %s", raw)
		}
	}
}

func TestFactorNestedListsUseTheirOwnFieldValidation(t *testing.T) {
	valid := "run_policy: [{name: CS, engine: factor, chunks: [{path: bars.gob, from: 0, to: 1000}], research: {labels: [{name: forward_return, kind: return, horizon: 1}]}}]"
	if _, err := ParseUnifiedYAML([]byte(valid), "factor.yml"); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{
		strings.Replace(valid, "horizon: 1", "horizon: 0", 1),
		strings.Replace(valid, "path: bars.gob", "unexpected: bars.gob", 1),
		strings.Replace(valid, "name: forward_return", "unexpected: forward_return", 1),
	} {
		if _, err := ParseUnifiedYAML([]byte(invalid), "factor.yml"); err == nil {
			t.Fatalf("accepted invalid nested list: %s", invalid)
		}
	}
}
