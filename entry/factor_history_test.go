package entry

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor/runner"
)

func TestUnifiedHistoryPathResolutionAndPreflight(t *testing.T) {
	dir, path := factorYAMLFixture(t)
	body, _ := os.ReadFile(path)
	body = []byte(strings.Replace(string(body), "funding_policy: explicit-zero", "funding_policy: explicit-zero, history: cold/history.sqlite", 1))
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	spec, err := loadFactorRunSpec([]string{path}, "")
	if err != nil {
		t.Fatal(err)
	}
	configs, err := buildFactorConfigs(spec, runner.Events)
	if err != nil || configs[0].Execution.HistoryPath != filepath.Join(dir, "cold", "history.sqlite") {
		t.Fatalf("history path lost declaring origin: %v %v", configs, err)
	}
	resolved, err := spec.EffectiveYAML(true)
	if err != nil || !strings.Contains(filepath.ToSlash(string(resolved)), filepath.ToSlash(dir)+"/cold/history.sqlite") {
		t.Fatalf("relocated effective YAML changed history path: %s %v", resolved, err)
	}
	c := configs[0]
	c.Mode = runner.Trade
	if err := runner.ValidateReplayConfig(c, false); err == nil || !strings.Contains(err.Error(), "cold history requires simulated") {
		t.Fatal("real account admitted memory archive", err)
	}
	if _, err := factorLiveAccountConfig([]runner.Config{c}); err == nil || !strings.Contains(err.Error(), "simulated replay") {
		t.Fatal("real live composition silently ignored cold history", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "cold")); !os.IsNotExist(err) {
		t.Fatal("preflight created output directory", err)
	}
}

func TestPureTSRejectsColdHistoryBeforeResources(t *testing.T) {
	for _, execution := range []string{"execution: {history: cold.sqlite}", "execution: {accounts: {default: {history: cold.sqlite}}}"} {
		dir := t.TempDir()
		body := "config_version: 2\ntime_start: '20240101'\ntime_end: '20240102'\nexchange: {name: mixedfixture}\nmarket_type: linear\nstake_currency: [USD]\naccounts: {default: {}}\nrun_policy:\n  - name: history-ts\n    run_timeframes: [1h]\n" + execution + "\n"
		args := &config.CmdArgs{NoDefault: true, DataDir: dir, ConfigData: body}
		spec, err := config.LoadRunSpec(args, false)
		if err != nil {
			t.Fatal(err)
		}
		if err := ValidateBacktestRunSpec(spec); err == nil || !strings.Contains(err.Error(), "pure TS") {
			t.Fatal("Web preflight silently ignored cold history", err)
		}
		if err := runExplicitBackTest(args); err == nil || !strings.Contains(err.Error(), "pure TS") {
			t.Fatal("TS backtest ignored cold history", err)
		}
		if err := runExplicitTrade(args, nil); err == nil || !strings.Contains(err.Error(), "pure TS") {
			t.Fatal("TS live ignored cold history", err)
		}
		entries, readErr := os.ReadDir(dir)
		if readErr != nil || len(entries) != 0 {
			t.Fatal("invalid history created resources", entries, readErr)
		}
	}
}

func TestAccountHistoryPreflightRejectsCrossAccountFile(t *testing.T) {
	_, path := factorYAMLFixture(t)
	body, _ := os.ReadFile(path)
	body = []byte(strings.Replace(string(body), "funding_policy: explicit-zero", "funding_policy: explicit-zero, history: same.sqlite", 1))
	body = append([]byte("time_start: '19700101'\ntime_end: '19700102'\nexchange: {name: mixedfixture}\nmarket_type: linear\nstake_currency: [USD]\naccounts: {default: {}, other: {}}\n"), body...)
	body = append(body, []byte("  - name: unused-ts\n    account: other\n    run_timeframes: [1h]\n")...)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	spec, err := loadFactorRunSpec([]string{path}, "")
	if err != nil {
		t.Fatal(err)
	}
	configs, err := buildFactorConfigs(spec, runner.Events)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateAccountHistoryPaths(spec, configs); err == nil || !strings.Contains(err.Error(), "separate history path") {
		t.Fatal("two accounts can overwrite the same history file", err)
	}
}
