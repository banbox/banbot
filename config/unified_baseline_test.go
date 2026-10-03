package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLegacyMultiPolicySizingAndOpenParameters(t *testing.T) {
	t.Setenv("CONFIG_PROBE", "relative/archive")
	cfg, err := ParseYmlConfig([]byte(`stake_amount: 120
stake_pct: 3
leverage: 5
run_policy:
  - name: One
    stake_rate: 1.5
    account: custom-parameter
    id: old-id
    archive: ${CONFIG_PROBE}
    custom: {enabled: false, count: 0, values: []}
  - name: Two
    stake_rate: 2
`), "fixture.yml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StakeAmount != 120 || cfg.StakePct != 3 || cfg.Leverage != 5 ||
		len(cfg.RunPolicy) != 2 || cfg.RunPolicy[0].StakeRate != 1.5 || cfg.RunPolicy[1].StakeRate != 2 {
		t.Fatalf("legacy sizing changed: %#v", cfg)
	}
	more := cfg.RunPolicy[0].More
	if more["account"] != "custom-parameter" || more["id"] != "old-id" || more["archive"] != "relative/archive" {
		t.Fatalf("legacy More changed: %#v", more)
	}
	custom := more["custom"].(map[string]any)
	if custom["enabled"] != false || custom["count"] != 0 || len(custom["values"].([]any)) != 0 {
		t.Fatalf("explicit values changed: %#v", custom)
	}
}

func TestLegacyOverlayReplacesPoliciesAndExplicitEmptyValues(t *testing.T) {
	for _, empty := range []string{"[]", "null"} {
		t.Run(empty, func(t *testing.T) {
			dir := t.TempDir()
			first := filepath.Join(dir, "base.yml")
			last := filepath.Join(dir, "overlay.yml")
			if err := os.WriteFile(first, []byte("run_policy: [{name: First}]\nwallet_amounts: {USDT: 100}\nwatch_jobs: {BTC: [1h]}\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(last, []byte("run_policy: "+empty+"\nwallet_amounts: {}\nwatch_jobs: {}\n"), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := ParseConfigs([]string{first, last}, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(cfg.RunPolicy) != 0 || len(cfg.WalletAmounts) != 0 || len(cfg.WatchJobs) != 0 {
				t.Fatalf("explicit empty override inherited values: %#v", cfg)
			}
		})
	}
}
