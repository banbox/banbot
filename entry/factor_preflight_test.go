package entry

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/factor/runner"
	"gopkg.in/yaml.v3"
)

func TestBacktestPreflightRejectsInvalidYAMLReplayBeforeResources(t *testing.T) {
	tests := []struct {
		name, reason string
		change       func(map[string]any, map[string]any)
	}{
		{"pending", "max_pending", func(root, policy map[string]any) { policy["decision"] = map[string]any{"max_pending": 0} }},
		{"latency", "bounded", func(root, policy map[string]any) { policy["decision"] = map[string]any{"latency_ms": 0} }},
		{"labels", "label", func(root, policy map[string]any) {
			policy["research"] = map[string]any{"labels": []any{map[string]any{"name": "bad", "kind": "executable-return", "horizon": 0}}}
		}},
		{"price", "observable", func(root, policy map[string]any) {
			policy["prices"] = map[string]any{"source": "tick", "timeframe": "1h", "field": "price"}
		}},
		{"funding", "funding stream", func(root, policy map[string]any) {
			root["execution"].(map[string]any)["funding_policy"] = "required-stream"
			policy["funding_source"] = ""
		}},
		{"builder", "unregistered portfolio", func(root, policy map[string]any) {
			policy["portfolio"] = map[string]any{"builder": "missing-entry-builder"}
		}},
		{"risk", "risk limits", func(root, policy map[string]any) { root["execution"].(map[string]any)["margin_rate"] = "0" }},
		{"units", "instrument", func(root, policy map[string]any) {
			units := root["execution"].(map[string]any)["instruments"].(map[interface{}]interface{})
			units[1].(map[string]any)["quantity_step"] = "0"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir, path := factorEventsYAMLFixture(t)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var root map[string]any
			if err = yaml.Unmarshal(raw, &root); err != nil {
				t.Fatal(err)
			}
			policy := root["run_policy"].([]interface{})[0].(map[string]any)["factor"].(map[string]any)
			settings := root["execution"].(map[string]any)
			settings["mode"], settings["store"], settings["sender_lease_dir"] = "events", "rejected.db", "rejected-leases"
			test.change(root, policy)
			raw, err = yaml.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			spec, err := loadFactorYAMLSpec([]string{path})
			preflightErr := err
			if preflightErr == nil {
				preflightErr = ValidateBacktestRunSpec(spec)
			}
			if preflightErr == nil || !strings.Contains(preflightErr.Error(), test.reason) {
				t.Fatalf("invalid YAML replay passed preflight: %v expected=%s", preflightErr, test.reason)
			}
			out := filepath.Join(dir, "rejected-output")
			if spec != nil {
				if err := unifiedFactorBacktestContext(context.Background(), &config.CmdArgs{OutPath: out}, spec); err == nil || err.Code != core.ErrBadConfig || !strings.Contains(err.Error(), preflightErr.Error()) {
					t.Fatalf("execution and preflight differ: %v; preflight=%v", err, preflightErr)
				}
			}
			if err := RunBackTest(&config.CmdArgs{Configs: config.ArrString{path}, NoDefault: true, OutPath: out}); err == nil || !strings.Contains(err.Error(), test.reason) {
				t.Fatalf("CLI bypassed preflight: %v expected=%s", err, test.reason)
			}
			for _, resource := range []string{"rejected.db", "rejected-leases", "rejected-output"} {
				if _, err := os.Stat(filepath.Join(dir, resource)); !os.IsNotExist(err) {
					t.Fatalf("invalid replay created %s: %v", resource, err)
				}
			}
		})
	}
}

func TestUnifiedBacktestCancellationPrecedesInvalidConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := unifiedFactorBacktestContext(ctx, &config.CmdArgs{}, nil); err == nil || err.Code != core.ErrRunTime || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("cancellation not returned first: %v", err)
	}
}
func TestBacktestPreflightAcceptsOrdinaryStorageWithoutMarketUnits(t *testing.T) {
	spec := storageEntrySpec(t, "static-approximation")
	if err := ValidateBacktestRunSpec(spec); err != nil {
		t.Fatal(err)
	}
	configs, err := buildFactorConfigs(spec, runner.Events)
	if err != nil || len(configs[0].Execution.Instruments) != 0 {
		t.Fatal("preflight created execution metadata", err)
	}
}
