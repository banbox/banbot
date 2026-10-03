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

func TestRunSpecOriginsPathsAndWholePolicyReplacement(t *testing.T) {
	base := migrationFixture(t, "base.yml", "config_version: 2\ntime_start: '20240101'\ntime_end: '20240201'\nexecution: {store: 'base/store', margin_rate: '0.1'}\nrun_policy: [{name: Old, custom: {flag: false}}]\n")
	overlay := migrationFixture(t, "overlay.yml", "config_version: 2\nexecution: {sender_lease_dir: 'leases'}\nrun_policy: [{name: CS, engine: factor, factor: {archive: 'data/chunk.gob'}}]\n")
	args := &CmdArgs{NoDefault: true, DataDir: t.TempDir(), Configs: ArrString{base, overlay}}
	spec, err := LoadRunSpec(args, false)
	if err != nil {
		t.Fatal(err)
	}
	for field, want := range map[string]string{"execution.store": filepath.Join(filepath.Dir(base), "base/store"), "execution.sender_lease_dir": filepath.Join(filepath.Dir(overlay), "leases"), "run_policy[0].factor.archive": filepath.Join(filepath.Dir(overlay), "data/chunk.gob")} {
		got, err := spec.ResolvePath(field)
		if err != nil || got != want {
			t.Fatalf("%s resolved %q, want %q: %v", field, got, want, err)
		}
	}
	if _, ok := spec.Origin("run_policy[0].custom.flag"); ok {
		t.Fatal("whole list replacement retained earlier parameter origins")
	}
	if origin, ok := spec.Origin("execution.margin_rate"); !ok || origin.Source != base {
		t.Fatal("nested overlay discarded surviving field source")
	}
	if !reflect.DeepEqual(spec.Engines(), []string{EngineFactor}) {
		t.Fatal("wrong engine set")
	}
	if args.Inited {
		t.Fatal("load mutated caller args")
	}
}

func TestRunSpecCopiesExportsAndExplicitCLI(t *testing.T) {
	args := &CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: "time_start: '20240101'\ntime_end: '20240201'\nstake_amount: 100\npairs: [BTC]\nbt_strict: true\naccounts: {default: {binance: {prod: {api_key: secret-key, api_secret: secret-value}}}}\nrun_policy: [{name: Demo, custom: {flag: false, values: []}}]\n", ExplicitFlags: map[string]bool{"stake-amount": true, "pairs": true, "bt-strict": true}}
	oldDir, oldName := DataDir, Name
	spec, err := LoadRunSpec(args, false)
	if err != nil {
		t.Fatal(err)
	}
	view := spec.Config()
	if view.Root.StakeAmount != 0 || len(view.Root.Pairs) != 0 || view.Root.BTStrict {
		t.Fatal("explicit zero/empty/false CLI did not override")
	}
	view.RunPolicy[0].More["custom"].(map[string]any)["flag"] = true
	view.Root.Accounts["default"].Exchanges["binance"].Prod.APIKey = "modified"
	if spec.Config().RunPolicy[0].More["custom"].(map[string]any)["flag"] != false {
		t.Fatal("mutable config view leaked into immutable spec")
	}
	origins := spec.Origins()
	origins["stake_amount"] = FieldOrigin{Source: "changed"}
	if origin, _ := spec.Origin("stake_amount"); origin.Kind != "cli" {
		t.Fatal("origin view leaked")
	}
	if origin, _ := spec.Origin("run_policy[0].engine"); origin.Kind != "default" {
		t.Fatal("default engine has no derivation source")
	}
	public, err2 := spec.EffectiveYAML(true)
	if err2 != nil {
		t.Fatal(err2)
	}
	if bytes.Contains(public, []byte("secret-key")) || bytes.Contains(public, []byte("secret-value")) {
		t.Fatal("export revealed secrets")
	}
	raw, err2 := spec.EffectiveYAML(false)
	if err2 != nil {
		t.Fatal(err2)
	}
	var fields map[string]any
	if err := yaml.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["stake_amount"] != 0 || fields["bt_strict"] != false {
		t.Fatal("export omitted explicit zero values")
	}
	if list, ok := fields["pairs"].([]any); !ok || len(list) != 0 {
		t.Fatal("export collapsed explicit empty list")
	}
	first, _ := spec.Hash()
	second, _ := spec.Hash()
	if first == "" || first != second {
		t.Fatal("effective hash is unstable")
	}
	if DataDir != oldDir || Name != oldName {
		t.Fatal("unified loading installed process configuration")
	}
}

func TestRunSpecDataDirInlinePathsAndArchiveOnlyResearch(t *testing.T) {
	dir := t.TempDir()
	args := &CmdArgs{NoDefault: true, DataDir: dir, ConfigData: "config_version: 2\nrun_policy: [{name: Custom, engine: factor, factor: {archive: '@/archive.gob'}}]\nexecution: {store: ':memory:'}\n"}
	spec, err := LoadRunSpec(args, false)
	if err != nil {
		t.Fatal(err)
	}
	path, pathErr := spec.ResolvePath("run_policy[0].factor.archive")
	if pathErr != nil || path != filepath.Join(dir, "archive.gob") {
		t.Fatalf("DataDir path: %q, %v", path, pathErr)
	}
	if memory, err := spec.ResolvePath("execution.store"); err != nil || memory != ":memory:" {
		t.Fatal("memory backend treated as a disk path")
	}
	if spec.Config().Root.TimeRange != nil {
		t.Fatal("archive research invented a time range")
	}
	items, _ := os.ReadDir(dir)
	if len(items) != 0 {
		t.Fatal("inline conversion created disk inputs")
	}
	if _, err := spec.RuntimeSnapshot(); err == nil {
		t.Fatal("trading snapshot omitted required exchange validation")
	}
}

func TestRunSpecCLIAndInlineValidationHappensBeforeMigration(t *testing.T) {
	for _, inline := range []string{"run_policy: [{name: CS, engine: factor}, {name: Other, engine: factor}]\n", ""} {
		path := migrationFixture(t, "config.yml", "time_start: '20240101'\nrun_policy: [{name: Demo}]\n")
		args := &CmdArgs{NoDefault: true, DataDir: t.TempDir(), Configs: ArrString{path}, ConfigData: inline, TimeStart: "not-a-time"}
		if _, err := LoadRunSpec(args, false); err == nil {
			t.Fatal("invalid final chain accepted")
		}
		if len(configBackups(t, path)) != 0 {
			t.Fatal("failed final validation created backups")
		}
		raw, _ := os.ReadFile(path)
		if strings.Contains(string(raw), "config_version") {
			t.Fatal("failed final validation migrated source")
		}
	}
}

func TestAdvancedOverridesStrictTypesAndAccountReferences(t *testing.T) {
	for _, block := range []string{
		"data: {unknown: 1}", "data: {page_rows: 1.0}", "data: {page_rows: 0}", "data: {archive: false}",
		"execution: {mode: silent-paper}", "execution: {margin_rate: 'NaN'}", "execution: {margin_rate: -1}", "execution: {accounts: {missing: {store: x}}}",
		"run_policy: [{name: CS, engine: factor, factor: {decision: {interval_ms: '1h'}}}]",
		"run_policy: [{name: CS, engine: factor, factor: {decision: {latency_ms: -1}}}]",
		"run_policy: [{name: CS, engine: factor, factor: {portfolio: {mode: wrong}}}]",
		"run_policy: [{name: CS, engine: factor, factor: {chunks: [{path: x, unknown: 1}]}}]",
		"run_policy: [{name: CS, engine: factor, factor: {snapshot: {typo: 1}}}]",
	} {
		if _, err := ParseUnifiedYAML([]byte("config_version: 2\n"+block+"\n"), "bad.yml"); err == nil {
			t.Fatalf("accepted %s", block)
		}
	}
	for _, block := range []string{
		"data: {archive: x, page_rows: 100, prefetch_rows: 50}",
		"accounts: {a: {}}\nexecution: {accounts: {a: {margin_rate: '0.10000000000000000001'}}}",
		"run_policy: [{name: CS, engine: factor, factor: {config: {Definition: Custom, Chunks: [{Path: old-relative.gob}]}}}]",
		"run_policy: [{name: TS, custom_open_parameter: {any: [null, false]}}]",
	} {
		if _, err := ParseUnifiedYAML([]byte("config_version: 2\n"+block+"\n"), "valid.yml"); err != nil {
			t.Fatalf("rejected %s: %v", block, err)
		}
	}
	if _, err := ParseDurationOverride("bad"); err == nil {
		t.Fatal("invalid duration accepted")
	}
	if _, err := ParseDurationOverride("-1ms"); err == nil {
		t.Fatal("negative duration accepted")
	}
}
