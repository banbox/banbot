package entry

import (
	"bytes"
	"context"
	"github.com/banbox/banbot/factor/runner"
	"os"
	"strings"
	"testing"
)

func TestUnifiedLifecycleConfigurationAndExecution(t *testing.T) {
	_, path := factorYAMLFixture(t)
	raw, _ := os.ReadFile(path)
	text := strings.Replace(string(raw), "      archive: data.gob", "      archive: data.gob\n      portfolio:\n        policy: lifecycle-v1\n        long_notional: 1\n        short_notional: 0\n        rebalance: {every_bars: 2}\n        holding: {min_bars: 2, max_bars: 6}\n        transition: {mode: linear-exit, exit_steps: 2, basis: quantity}", 1)
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	spec, err := loadFactorYAMLSpec([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	configs, err := buildFactorConfigs(spec, runner.Weights)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolvedFactorValues(spec, configs)
	if err != nil {
		t.Fatal(err)
	}
	if resolved[0]["portfolio"].Value == nil {
		t.Fatal("missing resolved policy defaults")
	}
	var output bytes.Buffer
	result, err := runner.Run(context.Background(), configs[0], nil, &runner.JSONOutput{Writer: &output, SIDs: configs[0].Snapshot.Universe.Evaluation})
	if err != nil {
		t.Fatal(err)
	}
	if result.Decisions != 8 || result.TargetsAccepted == 0 || !strings.Contains(output.String(), "allocation-decision") {
		t.Fatalf("lifecycle did not execute: %+v", result)
	}
}

func TestPolicyConfigRejectsSilentIntegerAndUnknownFields(t *testing.T) {
	for _, field := range []string{"rebalance: {every_bars: 2.5}", "rebalance: {every_bars: 0}", "holding: {min_bars: -1}", "holding: {by_asset: {asset1: {nonsense: 4}}}", "transition: {mode: direct, basis: quantity}", "transition: {mode: linear-exit, exit_steps: 8}"} {
		t.Run(field, func(t *testing.T) {
			_, path := factorYAMLFixture(t)
			raw, _ := os.ReadFile(path)
			text := strings.Replace(string(raw), "      archive: data.gob", "      archive: data.gob\n      portfolio:\n        policy: lifecycle-v1\n        "+field, 1)
			if err := os.WriteFile(path, []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			spec, err := loadFactorYAMLSpec([]string{path})
			if err == nil {
				_, err = buildFactorConfigs(spec, runner.Weights)
			}
			if err == nil {
				t.Fatal("invalid policy accepted")
			}
		})
	}
}
