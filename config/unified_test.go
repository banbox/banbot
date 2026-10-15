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

func TestUnifiedLegacyImportPreservesSizingAndOpenMore(t *testing.T) {
	raw := []byte("stake_amount: 100\nstake_pct: 2\nrun_policy:\n  - name: One\n    stake_rate: 2\n    custom: {enabled: false, count: 0, values: []}\n  - name: Two\n    stake_rate: 3\n")
	old, err := ParseYmlConfig(raw, "legacy.yml")
	if err != nil {
		t.Fatal(err)
	}
	u, err := ParseUnifiedYAML(raw, "legacy.yml")
	if err != nil {
		t.Fatal(err)
	}
	projected, err := u.TimeSeriesConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(old, projected) {
		t.Fatalf("v1/v2 TS projection differs:\nold: %#v\nnew: %#v", old, projected)
	}
	for _, policy := range u.RunPolicy {
		if policy.Engine != EngineTimeSeries || policy.CapitalWeight != nil {
			t.Fatalf("import enabled a new budget: %#v", policy)
		}
	}
	projected.RunPolicy[0].More["custom"].(map[string]any)["count"] = 9
	if u.RunPolicy[0].More["custom"].(map[string]any)["count"] != 0 {
		t.Fatal("projection shares mutable More")
	}
}

func TestUnifiedVersionAndReservedFieldFailures(t *testing.T) {
	for _, raw := range []string{
		"config_version: 3\n", "config_version: 0\n", "config_version: -1\n",
		"config_version: '2'\n", "config_version: 2.0\n", "config_version: true\n",
		"config_version: null\n", "config_version: ${VERSION}\n",
		"config_version: 2\nconfig_version: 2\n", "config_version: 2\n---\nconfig_version: 2\n",
		"config_version: 2\nrun_policy: [{name: Demo, engine: unknown}]\n",
		"config_version: 2\nrun_policy: [{name: Demo, engine: null}]\n",
		"config_version: 2\nrun_policy: [{name: Demo, engine: ''}]\n",
		"config_version: 2\nrun_policy: [{name: Demo, capital_weight: null}]\n",
		"config_version: 2\nrun_policy: [{name: Demo, capital_weight: '0.5'}]\n",
		"config_version: 2\nrun_policy: [{name: Demo, capital_weight: true}]\n",
		"config_version: 2\nrun_policy: [{name: Demo, capital_weight: .nan}]\n",
		"config_version: 2\nrun_policy: [{name: Demo, capital_weight: .inf}]\n",
		"config_version: 2\nrun_policy: [{name: Demo, capital_weight: -0.1}]\n",
		"config_version: 2\nrun_policy: [{name: Demo, capital_weight: 1.1}]\n",
		"config_version: 2\nrun_policy: [{name: Demo, factor: {window: 4}}]\n",
		"config_version: 2\nrun_policy: [{name: Demo, engine: factor, factor: []}]\n",
		"config_version: 2\ndata: []\n", "config_version: 2\nexecution: false\n",
		"config_version: 2\nrun_policy: [null]\n",
		"config_version: 2\nrun_policy: [{name: One, id: same}, {name: Two, id: same}]\n",
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := ParseUnifiedYAML([]byte(raw), "invalid.yml"); err == nil {
				t.Fatal("accepted invalid v2 configuration")
			}
		})
	}
}

func TestUnifiedBudgetRules(t *testing.T) {
	for _, tc := range []struct {
		policies string
		valid    bool
	}{
		{"[{name: One}, {name: Two}]", true},
		{"[{name: One, engine: factor}]", true},
		{"[{name: One, engine: factor}, {name: Two, engine: factor}]", false},
		{"[{name: One}, {name: Two, engine: factor}]", false},
		{"[{name: One, capital_weight: 0.4}, {name: Two}]", false},
		{"[{name: One, capital_weight: 0.4}, {name: Two, engine: factor, capital_weight: 0.5}]", true},
		{"[{name: One, capital_weight: 0}, {name: Two, engine: factor, capital_weight: 1}]", true},
		{"[{name: One, capital_weight: 0.6}, {name: Two, engine: factor, capital_weight: 0.5}]", false},
		{"[{name: One, capital_weight: 0.1}, {name: Two, capital_weight: 0.2}, {name: Three, capital_weight: 0.7}]", true},
		{"[{name: One, capital_weight: 0.5000000000001}, {name: Two, capital_weight: 0.5}]", false},
	} {
		t.Run(tc.policies, func(t *testing.T) {
			u, err := ParseUnifiedYAML([]byte("config_version: 2\nstake_amount: 100\nrun_policy: "+tc.policies+"\n"), "budget.yml")
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v, error=%v", tc.valid, err)
			}
			if err == nil && u.Root.StakeAmount != 100 {
				t.Fatal("budget changed root stake sizing")
			}
		})
	}
}

func TestUnifiedBudgetsUseActualAccountBindings(t *testing.T) {
	root := "config_version: 2\nenv: prod\naccounts: {a: {}, b: {}, disabled: {no_trade: true}}\nrun_policy: "
	for _, tc := range []struct {
		policies string
		valid    bool
	}{
		{"[{name: TS, account: a}, {name: CS, engine: factor, account: b}]", true},
		{"[{name: TS, account: a}, {name: CS, engine: factor, account: a}]", false},
		{"[{name: TS}, {name: CS, engine: factor, account: b}]", false},
		{"[{name: TS, capital_weight: 0.4}, {name: CS, engine: factor, account: b, capital_weight: 0.6}]", true},
		{"[{name: CS, engine: factor, account: missing}]", false},
		{"[{name: CS, engine: factor, account: disabled}]", false},
	} {
		if _, err := ParseUnifiedYAML([]byte(root+tc.policies+"\n"), "accounts.yml"); (err == nil) != tc.valid {
			t.Fatalf("%s: valid=%v, error=%v", tc.policies, tc.valid, err)
		}
	}
	// Dry-run default selection uses the same sorted configured account as legacy initExgAccs.
	if _, err := ParseUnifiedYAML([]byte(strings.Replace(root, "env: prod", "env: dry_run", 1)+"[{name: TS}, {name: CS, engine: factor, account: a}]\n"), "paper.yml"); err == nil {
		t.Fatal("synthetic default hid a shared-account mixed budget")
	}
}

func TestUnifiedOverlayWholeReplacementAndNoSourceWrites(t *testing.T) {
	dir := t.TempDir()
	first, last := filepath.Join(dir, "base.yml"), filepath.Join(dir, "overlay.yml")
	base := []byte("time_start: '20240101'\ntime_end: '20240201'\nrun_policy: [{name: Old}]\nwallet_amounts: {USDT: 100}\nwatch_jobs: {BTC: [1h]}\n")
	for _, empty := range []string{"[]", "null"} {
		overlay := []byte("config_version: 2\ntimerange: 20240301-20240401\nrun_policy: " + empty + "\nwallet_amounts: {}\nwatch_jobs: {}\n")
		if err := os.WriteFile(first, base, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(last, overlay, 0600); err != nil {
			t.Fatal(err)
		}
		u, err := ParseUnifiedConfigs([]string{first, last}, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(u.RunPolicy) != 0 || len(u.Root.WalletAmounts) != 0 || len(u.Root.WatchJobs) != 0 || u.Root.TimeStart != "" || u.Root.TimeEnd != "" {
			t.Fatalf("replacement semantics changed: %#v", u)
		}
		if (u.RunPolicy == nil) != (empty == "null") {
			t.Fatal("empty policy list and null collapsed")
		}
		legacy, err := ParseConfigs([]string{first, last}, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(legacy.RunPolicy) != 0 || legacy.TimeRangeRaw != u.Root.TimeRangeRaw {
			t.Fatal("TS and unified overlay rules differ")
		}
		for path, expected := range map[string][]byte{first: base, last: overlay} {
			actual, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(actual, expected) {
				t.Fatal("in-memory import modified source")
			}
		}
	}
	if err := os.WriteFile(first, []byte("config_version: 99\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, parse := range []func() bool{
		func() bool { _, err := ParseUnifiedConfigs([]string{first, last}, false); return err != nil },
		func() bool { _, err := ParseConfigs([]string{first, last}, false); return err != nil },
	} {
		if !parse() {
			t.Fatal("later overlay hid an unsupported source version")
		}
	}
}

func TestUnifiedDataDirPathDoesNotInstallGlobalDirectory(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("BanDataDir", dir)
	previous := DataDir
	DataDir = ""
	t.Cleanup(func() { DataDir = previous })
	if err := os.WriteFile(filepath.Join(dir, "config.yml"), []byte("run_policy: [{name: Demo}]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"$/config.yml", "@/config.yml"} {
		if _, err := ParseUnifiedConfigs([]string{path}, false); err != nil {
			t.Fatal(err)
		}
		if DataDir != "" {
			t.Fatal("pure config load installed the global DataDir")
		}
	}
}

func TestLegacyEntrypointsRejectFactorBeforeMutation(t *testing.T) {
	for _, marker := range []string{"", "config_version: 2\n"} {
		if _, err := ParseYmlConfig([]byte(marker+"run_policy: [{name: Demo, engine: factor}]\n"), "factor.yml"); err == nil {
			t.Fatal("legacy parser accepted factor")
		}
	}
	policy := &RunPolicyConfig{Name: "Demo", More: map[string]any{"engine": EngineFactor}}
	oldLoaded, oldName, oldPolicies := Loaded, Name, RunPolicy
	t.Cleanup(func() { Loaded, Name, RunPolicy = oldLoaded, oldName, oldPolicies })
	Loaded, Name = false, "before"
	if err := ApplyConfig(&CmdArgs{}, &Config{Name: "after", RunPolicy: []*RunPolicyConfig{policy}}); err == nil {
		t.Fatal("legacy apply accepted factor")
	}
	if Loaded || Name != "before" {
		t.Fatal("legacy apply mutated globals before rejecting factor")
	}
	if err := SetRunPolicy(true, policy); err == nil {
		t.Fatal("legacy policy installer accepted factor")
	}
	if err := (&Config{RunPolicy: []*RunPolicyConfig{policy}}).NormalizeRunPolicies(); err == nil {
		t.Fatal("legacy normalizer accepted factor")
	}
}

func TestUnifiedYAMLRoundTripAndProjectionGuards(t *testing.T) {
	raw := []byte("config_version: 2\nstake_amount: 100\nrun_policy: [{name: Demo, engine: factor, capital_weight: 0, factor: {portfolio: {k: 10}}, custom: {enabled: false}}]\ndata: {archive: '../${ARCHIVE}/input'}\nexecution: {store: relative/store}\n")
	t.Setenv("ARCHIVE", "history")
	u, err := ParseUnifiedYAML(raw, "factor.yml")
	if err != nil {
		t.Fatal(err)
	}
	serialized, marshalErr := yaml.Marshal(u)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	again, err := ParseUnifiedYAML(serialized, "roundtrip.yml")
	if err != nil {
		t.Fatal(err)
	}
	serializedAgain, marshalErr := yaml.Marshal(again)
	if marshalErr != nil || !bytes.Equal(serialized, serializedAgain) {
		t.Fatalf("canonical YAML round trip changed configuration:\n%s", serialized)
	}
	if _, err := u.TimeSeriesConfig(); err == nil {
		t.Fatal("TS projection accepted unsupported engine/overrides")
	}
	for _, extra := range []string{"capital_weight: 0.5", "id: persistent", "account: a"} {
		cfg, err := ParseUnifiedYAML([]byte("config_version: 2\naccounts: {a: {}}\nrun_policy: [{name: TS, "+extra+"}]\n"), "new.yml")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cfg.TimeSeriesConfig(); err == nil {
			t.Fatalf("legacy projection ignored %s", extra)
		}
	}
}
