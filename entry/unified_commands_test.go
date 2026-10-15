package entry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor/runner"
)

func TestUnifiedCommandLocations(t *testing.T) {
	for _, path := range [][]string{{"backtest"}, {"trade"}, {"research"}, {"data", "archive"}, {"validate"}, {"explain"}} {
		command, remaining, err := NewRootCommand().Find(path)
		if err != nil || len(remaining) != 0 || command.Name() != path[len(path)-1] {
			t.Fatalf("missing command %v: %v %v", path, remaining, err)
		}
	}
	removed := NewRootCommand()
	removed.SetOut(&bytes.Buffer{})
	removed.SetErr(&bytes.Buffer{})
	removed.SetArgs([]string{"factor", "backtest"})
	if err := removed.Execute(); err == nil {
		t.Fatal("removed factor command is still callable")
	}
	for _, name := range []string{"backtest", "trade", "research"} {
		root := NewRootCommand()
		command, _, _ := root.Find([]string{name})
		if command.Flags().Lookup("factor-config") != nil || command.Flags().Lookup("no-default").DefValue != "false" {
			t.Fatalf("%s retained factor-specific configuration options", name)
		}
		root.SetArgs([]string{name, "--factor-config", "removed.json"})
		root.SetErr(&bytes.Buffer{})
		if err := root.Execute(); err == nil || !strings.Contains(err.Error(), "unknown flag: --factor-config") {
			t.Fatalf("%s accepted a removed import flag: %v", name, err)
		}
	}
}

func TestUnifiedConfigPathsAreLiteralAndOverlaysUseRepeatedFlags(t *testing.T) {
	dir, path := factorYAMLFixture(t)
	literal := filepath.Join(dir, "factor,one.yml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(literal, body, 0600); err != nil {
		t.Fatal(err)
	}
	overlay := filepath.Join(dir, "overlay.yml")
	if err := os.WriteFile(overlay, []byte("wallet_amounts: {USD: 20000}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"research", "backtest"} {
		root := NewRootCommand()
		var output bytes.Buffer
		root.SetOut(&output)
		args := []string{name, "--no-default", "--config", literal, "--config", overlay}
		if name == "backtest" {
			args = append(args, "--out", filepath.Join(dir, "literal-result"))
		}
		root.SetArgs(args)
		if err := root.Execute(); err != nil {
			t.Fatalf("%s split a literal filename: %v", name, err)
		}
		lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'})
		var result runner.Result
		if err := json.Unmarshal(lines[len(lines)-1], &result); err != nil || result.Decisions != 8 {
			t.Fatalf("%s output: %s (%v)", name, output.Bytes(), err)
		}
	}
}

func TestUnifiedResearchLoadsDefaultYAML(t *testing.T) {
	dir, path := factorYAMLFixture(t)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.yml"), body, 0600); err != nil {
		t.Fatal(err)
	}
	root := NewRootCommand()
	var output bytes.Buffer
	root.SetOut(&output)
	root.SetArgs([]string{"research", "--datadir", dir})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'})
	var result runner.Result
	if err := json.Unmarshal(lines[len(lines)-1], &result); err != nil || result.Decisions != 8 {
		t.Fatalf("research did not use default YAML: %s (%v)", output.Bytes(), err)
	}
}

func factorEventsYAMLFixture(t *testing.T) (string, string) {
	t.Helper()
	dir, path := factorYAMLFixture(t)
	spec, err := config.LoadRunSpec(&config.CmdArgs{Configs: []string{path}, NoDefault: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	configs, buildErr := buildFactorConfigs(spec, runner.Weights)
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	var execution strings.Builder
	execution.WriteString("execution:\n  mode: weights\n  funding_policy: explicit-zero\n  margin_rate: '0.1'\n  max_account_margin: '1000000'\n  max_virtual_gross: '2000000'\n  strategy_gross_limit: '1000000'\n  instruments:\n")
	for sid, symbol := range configs[0].Snapshot.SIDMap {
		fmt.Fprintf(&execution, "    %d: {id: %q, version: v1, valuation: linear_perpetual, settlement_currency: USD, quantity_step: '0.01', price_tick: '0.01', contract_size: '1', money_scale: 8}\n", sid, symbol)
	}
	body, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	body = bytes.Replace(body, []byte("execution: {mode: weights, funding_policy: explicit-zero}\n"), []byte(execution.String()), 1)
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	return dir, path
}

func TestUnifiedTradeDryRunReplaysYAMLArchive(t *testing.T) {
	dir, path := factorEventsYAMLFixture(t)
	root := NewRootCommand()
	var output bytes.Buffer
	root.SetOut(&output)
	outPath := filepath.Join(dir, "replay")
	root.SetArgs([]string{"trade", "--no-default", "--dry-run", "--config", path, "--out", outPath})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	var artifact struct {
		Status  string
		Results []runner.Result
	}
	raw, err := os.ReadFile(filepath.Join(outPath, "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &artifact); err != nil || artifact.Status == "" || len(artifact.Results) != 1 || artifact.Results[0].Decisions != 8 || artifact.Results[0].Executions == 0 {
		t.Fatalf("historical simulation failed: status=%s results=%d (%v)", artifact.Status, len(artifact.Results), err)
	}
}

func TestExecutionModeOverrideKeepsOriginalSpecAndFlagOrigin(t *testing.T) {
	_, path := factorYAMLFixture(t)
	spec, err := config.LoadRunSpec(&config.CmdArgs{Configs: []string{path}, NoDefault: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	override := spec.WithExecutionMode(string(runner.Events), "--dry-run")
	if spec.Config().Execution["mode"] != "weights" || override.Config().Execution["mode"] != "events" {
		t.Fatal("mode override mutated the original spec")
	}
	origin, ok := override.Origin("execution.mode")
	if !ok || origin != (config.FieldOrigin{Source: "--dry-run", Kind: "cli"}) {
		t.Fatal("mode override lost the actual flag origin", origin)
	}
}

func TestUnifiedBacktestPreservesModeAndArtifacts(t *testing.T) {
	dir, path := factorYAMLFixture(t)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body = bytes.Replace(body, []byte("mode: weights"), []byte("mode: events"), 1)
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	outPath := filepath.Join(dir, "result")
	root := NewRootCommand()
	var output bytes.Buffer
	root.SetOut(&output)
	root.SetArgs([]string{"backtest", "--no-default", "--config", path, "--mode", "weights", "--out", outPath})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"config.yml", "resolved.json", "events.jsonl", "run.json", "strategy-1.json"} {
		if _, err := os.Stat(filepath.Join(outPath, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'})
	var result runner.Result
	if err := json.Unmarshal(lines[len(lines)-1], &result); err != nil || result.Decisions != 8 {
		t.Fatalf("replay output: %s (%v)", output.Bytes(), err)
	}
	effective, err := os.ReadFile(filepath.Join(outPath, "config.yml"))
	if err != nil || !bytes.Contains(effective, []byte("mode: weights")) {
		t.Fatalf("mode override missing from effective config: %s (%v)", effective, err)
	}
}

func TestMixedBacktestRejectsWeightsBeforeResources(t *testing.T) {
	dir, path := factorYAMLFixture(t)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body = append([]byte("time_start: '19700101'\ntime_end: '19700102'\n"), body...)
	body = bytes.Replace(body, []byte("    engine: factor"), []byte("    engine: factor\n    capital_weight: 0.5"), 1)
	body = append(body, []byte("  - name: time-series-fixture\n    capital_weight: 0.5\n    run_timeframes: [1h]\n")...)
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	root := NewRootCommand()
	root.SetOut(&bytes.Buffer{})
	root.SetErr(&bytes.Buffer{})
	outPath := filepath.Join(dir, "rejected")
	root.SetArgs([]string{"backtest", "--no-default", "--config", path, "--out", outPath})
	if err := root.Execute(); err == nil || !strings.Contains(err.Error(), "mixed backtest requires") {
		t.Fatalf("mixed backtest silently skipped TS policies: %v", err)
	}
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Fatal("invalid mixed backtest allocated output")
	}
}
