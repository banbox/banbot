package entry

import (
	"bytes"
	"encoding/json"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
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
	root.SetArgs([]string{"factor", "archive", "--input", input, "--out", path, "--max-records", "1"})
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
		fresh.SetArgs([]string{"factor", name, "--help"})
		fresh.SetOut(&bytes.Buffer{})
		if err = fresh.Execute(); err != nil {
			t.Fatal(err)
		}
	}
}
func TestFactorResearchCLIRealArchiveStreamsValidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.gob")
	store, _ := factor.NewVersionStore(100)
	const hour int64 = 3600000
	for bar := int64(1); bar <= 8; bar++ {
		for sid := int32(1); sid <= 3; sid++ {
			price := 100 + float64(sid)*float64(bar*bar)
			for _, source := range []string{"kline", "tick"} {
				at := bar * hour
				freq, field := "1h", "close"
				if source == "tick" {
					at++
					freq, field = "event", "price"
				}
				r := factor.VersionRecord{Series: orm.DataSeries{Source: source, Sid: sid, TimeMS: at - hour, EndMS: at, Closed: true, TimeFrame: freq, Values: map[string]any{field: price, "nullable": nil}}, EventTime: at, AvailableAt: at, IngestedAt: at, Revision: 1, SourceVersion: "v1"}
				if err := store.Put(r); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if _, err := store.Export(path); err != nil {
		t.Fatal(err)
	}
	c := runner.Config{Chunks: []runner.Chunk{{Path: "data.gob", From: hour, To: 8*hour + 1}}, MaxRecords: 100, MaxPending: 8, DecisionInterval: hour, LatencyMS: 1, ExpiryMS: 100, Snapshot: factor.SnapshotSpec{Universe: factor.Universe{Version: "u", Investable: []int32{1, 2, 3}, Reference: []int32{1, 2, 3}, Tradable: []int32{1, 2, 3}, Evaluation: []int32{1, 2, 3}, Tracked: []int32{1, 2, 3}}, SIDMap: map[int32]string{1: "a", 2: "b", 3: "c"}, Schemas: map[string]string{"kline": "s", "tick": "s"}, SourceVersions: map[string]string{"kline": "v1", "tick": "v1"}, VisibilityPolicy: "available-at"}, Factor: research.MomentumVolConfig{Source: "kline", Frequency: "1h", Field: "close", Window: 2, DDOF: 1}, Manifest: research.ManifestSpec{Currency: "USD", CodeRevision: "test", Portfolio: research.PortfolioDefinition{K: 1, LongNotional: .5, ShortNotional: .5, Mode: factor.Full}, Labels: []research.LabelSpec{{Name: "1h", Kind: research.ExecutableReturn, Horizon: hour, PeriodsPerYear: 8760}}, Costs: research.CostSpec{FundingPolicy: "explicit-zero"}}, StrategyID: "s", AccountID: "a", InitialNAV: 10000, Prices: runner.PriceStream{Source: "tick", Frequency: "event", Field: "price"}}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "run.json")
	if err = os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"factor", "research", "--factor-config", configPath})
	if err = cmd.Execute(); err != nil {
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
	if err = json.Unmarshal(lines[len(lines)-1], &result); err != nil {
		t.Fatal(err)
	}
	if result.Decisions != 8 || result.Executions == 0 || result.StrategyHash == "" || len(result.Manifest.Snapshots) != 1 {
		t.Fatalf("CLI failed to run real archive: %+v", result)
	}
}
