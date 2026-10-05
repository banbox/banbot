package entry

import (
	"bytes"
	"encoding/json"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"os"
	"path/filepath"
	"testing"
)

func TestFactorArchiveCLIAndFreshRegistration(t *testing.T) {
	dir := t.TempDir()
	input, path := filepath.Join(dir, "input.jsonl"), filepath.Join(dir, "archive.gob")
	r := factor.VersionRecord{Series: orm.DataSeries{Source: "custom", Sid: 1, TimeMS: 10, EndMS: 11, TimeFrame: "event", Values: map[string]any{"price": 100.0, "nullable": nil, "label": "x"}}, EventTime: 11, AvailableAt: 11, IngestedAt: 12, Revision: 1, SourceVersion: "v1"}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(input, append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	root := NewRootCommand()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"data", "archive", "--input", input, "--out", path, "--max-records", "1"})
	if err = root.Execute(); err != nil {
		t.Fatal(err)
	}
	store, err := factor.OpenVersionStore(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.Records()
	if err != nil || len(rows) != 1 || rows[0].Series.Values["price"] != 100.0 || rows[0].Series.Values["nullable"] != nil {
		t.Fatalf("typed/null archive lost: %+v %v", rows, err)
	}
	for _, name := range []string{"research", "backtest", "trade"} {
		fresh := NewRootCommand()
		fresh.SetArgs([]string{name, "--help"})
		fresh.SetOut(&bytes.Buffer{})
		if err = fresh.Execute(); err != nil {
			t.Fatal(err)
		}
	}
}
func TestFactorResearchCLIRealArchiveStreamsValidJSON(t *testing.T) {
	_, configPath := factorYAMLFixture(t)
	cmd := NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"research", "--no-default", "--config", configPath})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(out.Bytes()), []byte{'\n'})
	if len(lines) < 10 {
		t.Fatalf("no streaming panels/metrics: %s", out.String())
	}
	for _, line := range lines {
		if !json.Valid(line) {
			t.Fatalf("invalid JSON/NaN: %s", line)
		}
	}
	var result runner.Result
	if err := json.Unmarshal(lines[len(lines)-1], &result); err != nil {
		t.Fatal(err)
	}
	if result.Decisions != 8 || result.Executions == 0 || result.StrategyHash == "" || len(result.Manifest.Snapshots) != 1 {
		t.Fatalf("CLI failed to run real archive: %+v", result)
	}
}
