package entry

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
)

func factorYAMLFixture(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	archive := filepath.Join(dir, "data.gob")
	store, _ := factor.NewVersionStore(100)
	const hour int64 = 3600000
	for bar := int64(1); bar <= 8; bar++ {
		for sid := int32(1); sid <= 4; sid++ {
			price := 100 + float64(sid)*float64(bar*bar)
			for _, source := range []string{"kline", "tick"} {
				at, frequency, field := bar*hour, "1h", "close"
				if source == "tick" {
					at++
					frequency, field = "event", "price"
				}
				if err := store.Put(factor.VersionRecord{Series: orm.DataSeries{Source: source, Sid: sid, TimeMS: at - 1, EndMS: at, TimeFrame: frequency, Closed: true, Values: map[string]any{field: price, "big": int64(9007199254740993), "nullable": nil}}, EventTime: at, AvailableAt: at, IngestedAt: at, Revision: 1, SourceVersion: "v1"}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if _, err := store.Export(archive); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "factor.yml")
	body := []byte("config_version: 2\nwallet_amounts: {USD: 10000}\nexecution: {mode: weights, funding_policy: explicit-zero}\nrun_policy:\n  - name: MomentumVol\n    engine: factor\n    run_timeframes: [1h]\n    params: {window: 2, k: 1}\n    factor:\n      archive: data.gob\n")
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	return dir, path
}

func TestUnifiedFactorYAMLCommandsAndOrdinaryBacktest(t *testing.T) {
	dir, path := factorYAMLFixture(t)
	command := NewRootCommand()
	var out bytes.Buffer
	command.SetOut(&out)
	command.SetArgs([]string{"factor", "research", "--config", path})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(out.Bytes()), []byte{'\n'})
	var result runner.Result
	if err := json.Unmarshal(lines[len(lines)-1], &result); err != nil {
		t.Fatal(err)
	}
	if result.Decisions != 8 || result.ManifestID == "" || !result.Manifest.StaticUniverse {
		t.Fatalf("simple YAML did not derive archive identity: %+v", result)
	}
	outPath := filepath.Join(dir, "result")
	if err := RunBackTest(&config.CmdArgs{Configs: config.ArrString{path}, NoDefault: true, OutPath: outPath}); err != nil {
		t.Fatal(err)
	}
	artifacts, err := filepath.Glob(filepath.Join(outPath, "run.json"))
	if err != nil || len(artifacts) != 1 {
		t.Fatalf("ordinary backtest artifact missing: %v %v", artifacts, err)
	}
	var artifact struct {
		Version int
		Status  string
		Results []runner.Result
	}
	raw, _ := os.ReadFile(artifacts[0])
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Version != 1 || len(artifact.Results) != 1 || artifact.Results[0].Decisions != 8 {
		t.Fatalf("wrong result artifact %s", raw)
	}
	for _, name := range []string{"config.yml", "resolved.json", "events.jsonl", "strategy-1.json"} {
		if _, err := os.Stat(filepath.Join(outPath, name)); err != nil {
			t.Fatal(err)
		}
	}
	if databases, _ := filepath.Glob(filepath.Join(dir, "*.db")); len(databases) > 0 {
		t.Fatalf("ordinary factor replay created SQL: %v", databases)
	}
}

func TestFactorArchiveSchemaPreservesLargeIntegerAndRejectsLoss(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "rows.jsonl")
	archive := filepath.Join(dir, "archive.gob")
	schema := filepath.Join(dir, "schema.yml")
	raw := []byte(`{"Series":{"Source":"event","Sid":1,"TimeMS":10,"EndMS":11,"TimeFrame":"event","Values":{"big":9007199254740993,"unsigned":18446744073709551615,"i32":42,"price":100,"label":"x","flag":true,"nullable":null,"nested":{"count":9007199254740993}}},"EventTime":11,"Revision":1,"AvailableAt":11,"IngestedAt":11,"SourceVersion":"v1"}`)
	if err := os.WriteFile(input, raw, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := NewRootCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetArgs([]string{"factor", "archive", "--input", input, "--out", archive})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("lossy JSON integer accepted: %v", err)
	}
	if _, err := os.Stat(archive); !os.IsNotExist(err) {
		t.Fatal("failed conversion published archive")
	}
	if err := os.WriteFile(schema, []byte("event:\n  big: int64\n  unsigned: uint64\n  i32: int32\n  price: float64\n  label: string\n  flag: bool\n  nullable: string\n  nested: json\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cmd = NewRootCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetArgs([]string{"factor", "archive", "--input", input, "--out", archive, "--schema", schema})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	store, err := factor.OpenVersionStore(archive, 1)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.Records()
	if err != nil {
		t.Fatal(err)
	}
	values := rows[0].Series.Values
	if values["big"] != int64(9007199254740993) || values["unsigned"] != uint64(18446744073709551615) || values["i32"] != int32(42) || values["price"] != float64(100) || values["nullable"] != nil || values["nested"].(map[string]any)["count"] != int64(9007199254740993) {
		t.Fatalf("raw concrete types changed: %+v", values)
	}
	if _, exists := values["missing"]; exists {
		t.Fatal("missing became NULL")
	}
}

func TestFactorJSONImporterRereadsYAMLAndPreservesSource(t *testing.T) {
	dir, path := factorYAMLFixture(t)
	spec, err := loadFactorRunSpec([]string{path}, "")
	if err != nil {
		t.Fatal(err)
	}
	configs, err := buildFactorConfigs(spec, runner.Weights)
	if err != nil {
		t.Fatal(err)
	}
	c := configs[0]
	c.Chunks[0].Path = "data.gob"
	legacy := filepath.Join(dir, "legacy.json")
	raw, _ := json.Marshal(c)
	if err := os.WriteFile(legacy, raw, 0600); err != nil {
		t.Fatal(err)
	}
	converted, err := importFactorJSON(legacy)
	if err != nil {
		t.Fatal(err)
	}
	again, err := importFactorJSON(legacy)
	if err != nil || again != converted {
		t.Fatal("non-idempotent importer", again, err)
	}
	source, _ := os.ReadFile(legacy)
	if !bytes.Equal(source, raw) {
		t.Fatal("JSON source overwritten")
	}
	spec, err = loadFactorRunSpec(nil, legacy)
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, err := buildFactorConfigs(spec, runner.Weights)
	if err != nil || rebuilt[0].Chunks[0].Path != filepath.Join(dir, "data.gob") {
		t.Fatalf("path semantics changed: %+v %v", rebuilt, err)
	}
	commonDir := filepath.Join(dir, "common")
	if err := os.Mkdir(commonDir, 0700); err != nil {
		t.Fatal(err)
	}
	common := filepath.Join(commonDir, "runtime.yml")
	if err := os.WriteFile(common, []byte("config_version: 2\nwallet_amounts: {USD: 20000}\nexecution: {funding_policy: explicit-zero}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	combined, err := loadFactorRunSpec([]string{common}, legacy)
	if err != nil {
		t.Fatal(err)
	}
	configs, err = buildFactorConfigs(combined, runner.Weights)
	if err != nil || configs[0].Chunks[0].Path != filepath.Join(dir, "data.gob") {
		t.Fatalf("common YAML overlay lost imported path base: %+v %v", configs, err)
	}
	if _, err := loadFactorRunSpec([]string{path}, legacy); err == nil {
		t.Fatal("competing YAML factor definition admitted")
	}
}

func TestFactorArchiveFundingIdentityAndAccountOverrides(t *testing.T) {
	dir, path := factorYAMLFixture(t)
	store, err := factor.OpenVersionStore(filepath.Join(dir, "data.gob"), 100)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.Records()
	if err != nil {
		t.Fatal(err)
	}
	augmented, _ := factor.NewVersionStore(100)
	for _, row := range rows {
		if err := augmented.Put(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := augmented.Put(factor.VersionRecord{Series: orm.DataSeries{Source: "funding", Sid: 9, TimeMS: 3600000, EndMS: 3600001, TimeFrame: "event", Values: map[string]any{"rate": 0.0}}, EventTime: 3600001, AvailableAt: 3600001, IngestedAt: 3600001, Revision: 1, SourceVersion: "v1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := augmented.Export(filepath.Join(dir, "extra.gob")); err != nil {
		t.Fatal(err)
	}
	body, _ := os.ReadFile(path)
	body = append([]byte("accounts: {default: {}}\n"), body...)
	body = []byte(strings.Replace(string(body), "archive: data.gob", "archive: extra.gob", 1))
	body = []byte(strings.Replace(string(body), "params: {window: 2, k: 1}", "params: {window: 2, k: 1, custom_parameter: 7}", 1))
	body = []byte(strings.Replace(string(body), "execution: {mode: weights, funding_policy: explicit-zero}", "execution:\n  mode: weights\n  funding_policy: explicit-zero\n  store: root.db\n  sender_lease_dir: root-leases\n  accounts:\n    default:\n      store: account.db\n      sender_lease_dir: account-leases\n      margin_rate: '0.25'", 1))
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
	c := configs[0]
	if slices.Contains(c.Snapshot.Universe.Investable, int32(9)) || !slices.Contains(c.Snapshot.Universe.Tracked, int32(9)) {
		t.Fatalf("funding identity changed investment universe: %+v", c.Snapshot.Universe)
	}
	if c.Execution.StorePath != filepath.Join(dir, "account.db") || c.Execution.SenderLeaseDir != filepath.Join(dir, "account-leases") || c.Execution.MarginRate.String() != "0.25" || c.Manifest.Parameters["custom_parameter"] != 7 {
		t.Fatalf("advanced settings lost: %+v %+v", c.Execution, c.Manifest.Parameters)
	}
}
