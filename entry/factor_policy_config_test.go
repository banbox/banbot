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
	for _, field := range []string{"rebalance: {every_bars: 2.5}", "rebalance: {every_bars: 0}", "holding: {min_bars: -1}", "holding: {by_asset: {asset1: {nonsense: 4}}}", "transition: {mode: direct, basis: quantity}", "transition: {mode: linear-exit, exit_steps: 8}", "selection: {unfilled_slot_policy: bogus}"} {
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

func TestYAMLScoreGateZeroPointerAndBreadthExpressions(t *testing.T) {
	// 1. Default TopBottomK path with explicit integer `long_min_score: 0` in YAML must decode to non-nil *float64(0).
	_, path := factorYAMLFixture(t)
	raw, _ := os.ReadFile(path)
	topBottomYAML := strings.Replace(string(raw), "      archive: data.gob", "      archive: data.gob\n      portfolio:\n        long_notional: 1\n        short_notional: 0\n        long_min_score: 0\n        unfilled_slot_policy: cash", 1)
	if err := os.WriteFile(path, []byte(topBottomYAML), 0600); err != nil {
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
	if configs[0].Manifest.Portfolio.LongMinScore == nil || *configs[0].Manifest.Portfolio.LongMinScore != 0 {
		t.Fatalf("YAML long_min_score: 0 lost pointer distinction: %+v", configs[0].Manifest.Portfolio.LongMinScore)
	}
	if _, err := runner.Run(context.Background(), configs[0], nil, nil); err != nil {
		t.Fatal(err)
	}

	// 2. Lifecycle-v1 path with selection.long_min_score: 0, unfilled_slot_policy: rescale, and cs.mean breadth expression.
	_, path2 := factorYAMLFixture(t)
	raw2, _ := os.ReadFile(path2)
	lifecycleYAML := strings.Replace(string(raw2), "      archive: data.gob", `      archive: data.gob
      portfolio:
        policy: lifecycle-v1
        long_notional: 1
        short_notional: 0
        selection: {long_k: 2, long_min_score: 0, unfilled_slot_policy: rescale}
        allocation: {asset_cap: 0.6}
      expressions:
        schema_version: 1
        timeframe: 1h
        bindings:
          kline: {source: kline, timeframe: 1h}
        outputs:
          mom: 'ts.return(positive(kline.close), 1)'
          exposure_scale: 'cs.mean(kline.close > ts.sma(kline.close, 2))'`, 1)
	if err := os.WriteFile(path2, []byte(lifecycleYAML), 0600); err != nil {
		t.Fatal(err)
	}
	spec2, err := loadFactorYAMLSpec([]string{path2})
	if err != nil {
		t.Fatal(err)
	}
	configs2, err := buildFactorConfigs(spec2, runner.Weights)
	if err != nil {
		t.Fatal(err)
	}
	pc, err := configs2[0].Manifest.Portfolio.PolicyConfig()
	if err != nil || pc.Selection.LongMinScore == nil || *pc.Selection.LongMinScore != 0 || pc.Selection.UnfilledSlotPolicy != "rescale" {
		t.Fatalf("lifecycle YAML selection gate mismatch: %+v err=%v", pc.Selection, err)
	}
	if _, err := runner.Run(context.Background(), configs2[0], nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestYAMLRiskAllocationMethodsWithoutExplicitPolicy(t *testing.T) {
	for _, allocBlock := range []string{
		"allocation: {method: inverse-volatility, risk_window: 4}",
		"allocation: {method: erc, risk_window: 4}",
		"allocation: {method: risk-parity, risk_window: 4, shrinkage: 0.1}",
		"allocation: {method: hrp, risk_window: 4}",
		"allocation: {method: min-variance, risk_window: 4}",
		"allocation: {method: mean-variance, risk_window: 4, risk_aversion: 1.5}",
	} {
		t.Run(allocBlock, func(t *testing.T) {
			_, path := factorYAMLFixture(t)
			raw, _ := os.ReadFile(path)
			yamlText := strings.Replace(string(raw), "      archive: data.gob", "      archive: data.gob\n      portfolio:\n        long_notional: 1\n        short_notional: 0\n        "+allocBlock, 1)
			if err := os.WriteFile(path, []byte(yamlText), 0600); err != nil {
				t.Fatal(err)
			}
			spec, err := loadFactorYAMLSpec([]string{path})
			if err != nil {
				t.Fatalf("loadFactorYAMLSpec failed: %v", err)
			}
			configs, err := buildFactorConfigs(spec, runner.Weights)
			if err != nil {
				t.Fatalf("buildFactorConfigs failed: %v", err)
			}
			res, err := runner.Run(context.Background(), configs[0], nil, nil)
			if err != nil || res.TargetsAccepted == 0 {
				t.Fatalf("YAML risk allocation run failed: res=%+v err=%v", res, err)
			}
		})
	}

	for _, invalidAlloc := range []string{
		"allocation: {method: bogus}",
		"allocation: {method: hrp, risk_window: 1}",
		"allocation: {method: hrp, shrinkage: 1.5}",
		"allocation: {method: mean-variance, risk_aversion: -0.5}",
		"allocation: {method: hrp, risk_aversion: 2.0}",
	} {
		t.Run("invalid_"+invalidAlloc, func(t *testing.T) {
			_, path := factorYAMLFixture(t)
			raw, _ := os.ReadFile(path)
			yamlText := strings.Replace(string(raw), "      archive: data.gob", "      archive: data.gob\n      portfolio:\n        "+invalidAlloc, 1)
			if err := os.WriteFile(path, []byte(yamlText), 0600); err != nil {
				t.Fatal(err)
			}
			spec, err := loadFactorYAMLSpec([]string{path})
			if err == nil {
				_, err = buildFactorConfigs(spec, runner.Weights)
			}
			if err == nil {
				t.Fatalf("expected invalid YAML allocation %q to fail", invalidAlloc)
			}
		})
	}
}

func TestYAMLDriftBufferHybridStopsAndSectorQuantileExpressions(t *testing.T) {
	_, path := factorYAMLFixture(t)
	raw, _ := os.ReadFile(path)
	yamlText := strings.Replace(string(raw), "      archive: data.gob", `      archive: data.gob
      portfolio:
        long_notional: 1
        short_notional: 0
        drift_threshold: 0.02
        rebalance: {every_bars: 2}
        selection: {long_k: 2}
        holding: {stop_loss_pct: 0.08, trailing_stop_pct: 0.12, cooldown_bars: 2}
      expressions:
        schema_version: 1
        timeframe: 1h
        bindings:
          kline: {source: kline, timeframe: 1h}
        outputs:
          sec_mom: 'cs.sector_demean(ts.return(positive(kline.close), 1))'
          tier_mom: 'group.quantile_demean(ts.return(positive(kline.close), 1), ts.sma(kline.close, 2), 2)'`, 1)
	if err := os.WriteFile(path, []byte(yamlText), 0600); err != nil {
		t.Fatal(err)
	}
	spec, err := loadFactorYAMLSpec([]string{path})
	if err != nil {
		t.Fatalf("loadFactorYAMLSpec failed: %v", err)
	}
	configs, err := buildFactorConfigs(spec, runner.Weights)
	if err != nil {
		t.Fatalf("buildFactorConfigs failed: %v", err)
	}
	pc, err := configs[0].Manifest.Portfolio.PolicyConfig()
	if err != nil {
		t.Fatalf("PolicyConfig failed: %v", err)
	}
	if pc.Policy != "lifecycle-v1" || pc.EffectiveDriftThreshold() != 0.02 || pc.Holding.StopLossPct != 0.08 || pc.Holding.TrailingStopPct != 0.12 || pc.Holding.CooldownBars != 2 {
		t.Fatalf("unexpected decoded YAML policy config: %+v", pc)
	}
	res, err := runner.Run(context.Background(), configs[0], nil, nil)
	if err != nil || res.TargetsAccepted == 0 {
		t.Fatalf("YAML hybrid stop + sector/quantile run failed: res=%+v err=%v", res, err)
	}

	for _, invalid := range []string{
		"drift_threshold: 1.0",
		"drift_threshold: -0.01",
		"rebalance: {drift_threshold: 1.2}",
		"holding: {stop_loss_pct: 1.0}",
		"holding: {stop_loss_pct: -0.05}",
		"holding: {trailing_stop_pct: 1.5}",
	} {
		t.Run("invalid_"+invalid, func(t *testing.T) {
			_, p := factorYAMLFixture(t)
			r, _ := os.ReadFile(p)
			badYAML := strings.Replace(string(r), "      archive: data.gob", "      archive: data.gob\n      portfolio:\n        long_notional: 1\n        short_notional: 0\n        "+invalid, 1)
			if err := os.WriteFile(p, []byte(badYAML), 0600); err != nil {
				t.Fatal(err)
			}
			s, err := loadFactorYAMLSpec([]string{p})
			if err == nil {
				_, err = buildFactorConfigs(s, runner.Weights)
			}
			if err == nil {
				t.Fatalf("expected invalid YAML %q to fail", invalid)
			}
		})
	}
}
