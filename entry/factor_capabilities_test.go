package entry

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"gopkg.in/yaml.v3"
)

func capabilityYAML(t *testing.T, policy, executionFields string) string {
	t.Helper()
	_, path := factorYAMLFixture(t)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(string(raw), "      archive: data.gob", "      archive: data.gob\n"+policy, 1)
	if executionFields != "" {
		text = strings.Replace(text, "execution: {mode: weights, funding_policy: explicit-zero}", executionFields, 1)
	}
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestYAMLCapabilitiesReachConfigAndExports(t *testing.T) {
	path := capabilityYAML(t, "      decision: {source_event_decisions: true, source: kline, timeframe: 1h}\n      option_expiry_source: expiry\n      terminal_settlement_source: terminal", "execution: {mode: events, funding_policy: explicit-zero, paper_fill_model: midpoint}\naccounts:\n  default:\n    paper_fill_model: bid-ask")
	spec, err := loadFactorYAMLSpec([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	configs, err := buildFactorConfigs(spec, runner.Events)
	if err != nil {
		t.Fatal(err)
	}
	c := configs[0]
	if !c.SourceEventDecisions || c.Factor.Source != "kline" || c.Factor.TimeFrame != "1h" || c.OptionExpirySource != "expiry" || c.TerminalSettlementSource != "terminal" || c.Execution.PaperFillModel != execution.PaperBidAsk {
		t.Fatalf("capabilities lost: %+v", c)
	}
	resolved, err := resolvedFactorValues(spec, configs)
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]any{"source_event_decisions": true, "decision_source": "kline", "decision_timeframe": "1h", "option_expiry_source": "expiry", "terminal_settlement_source": "terminal", "paper_fill_model": execution.PaperBidAsk} {
		if got := resolved[0][key]; got.Value != want || got.Origin.Source != path {
			t.Fatalf("%s missing value/origin: %+v", key, got)
		}
	}
	raw, err := yaml.Marshal(spec.Config())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"source_event_decisions:", "option_expiry_source:", "terminal_settlement_source:", "paper_fill_model:"} {
		if !strings.Contains(string(raw), key) {
			t.Fatalf("export missing %s: %s", key, raw)
		}
	}
	// The configured source clock actually drives replay, without needing an account.
	c.Mode = runner.Weights
	c.Execution.PaperFillModel = ""
	c.OptionExpirySource = ""
	c.TerminalSettlementSource = ""
	result, err := runner.Run(context.Background(), c, nil, nil)
	if err != nil || result.Decisions != 8 {
		t.Fatalf("source clock not runnable: %+v %v", result, err)
	}
}

func TestYAMLCapabilitiesRejectInvalidAndUnsupported(t *testing.T) {
	for _, tc := range []struct {
		name, policy, exec string
		mode               runner.Mode
	}{
		{"boolean", "      decision: {source_event_decisions: yes}", "", runner.Weights},
		{"typo", "      decision: {source_event_decision: true}", "", runner.Weights},
		{"source-type", "      terminal_settlement_source: 12", "", runner.Weights},
		{"empty-source", "      option_expiry_source: ''", "", runner.Events},
		{"clock-missing", "      decision: {source_event_decisions: true}", "", runner.Weights},
		{"clock-partial", "      decision: {source_event_decisions: true, source: kline}", "", runner.Weights},
		{"clock-source-without-enable", "      decision: {source: kline, timeframe: event}", "", runner.Weights},
		{"clock-delay", "      decision: {source_event_decisions: true, source: kline, timeframe: 1h, delay_ms: 1}", "", runner.Weights},
		{"clock-live", "      decision: {source_event_decisions: true, source: kline, timeframe: 1h}", "", runner.Trade},
		{"option-weights", "      option_expiry_source: expiry", "", runner.Weights},
		{"terminal-live", "      terminal_settlement_source: terminal", "", runner.Trade},
		{"source-collision", "      terminal_settlement_source: tick", "", runner.Weights},
		{"model-invalid", "", "execution: {funding_policy: explicit-zero, paper_fill_model: best}", runner.Events},
		{"model-weights", "", "execution: {funding_policy: explicit-zero, paper_fill_model: midpoint}", runner.Weights},
		{"model-live", "", "execution: {funding_policy: explicit-zero, paper_fill_model: bid-ask}", runner.Trade},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := capabilityYAML(t, tc.policy, tc.exec)
			spec, err := loadFactorYAMLSpec([]string{path})
			if err == nil {
				_, err = buildFactorConfigs(spec, tc.mode)
			}
			if err == nil {
				t.Fatal("invalid capability accepted")
			}
		})
	}
}

func TestYAMLSourceEventClockUsesExplicitIrregularStream(t *testing.T) {
	path := capabilityYAML(t, "      decision: {source_event_decisions: true, source: information, timeframe: event}", "")
	archive := strings.Replace(path, "factor.yml", "data.gob", 1)
	store, err := factor.OpenVersionStore(archive, 100)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.Records()
	if err != nil {
		t.Fatal(err)
	}
	replacement, _ := factor.NewVersionStore(100)
	for _, row := range rows {
		if row.Series.Source == "kline" {
			row.Series.Source, row.Series.TimeFrame = "information", "event"
		}
		if err := replacement.Put(row); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := replacement.Export(strings.Replace(archive, "data.gob", "information.gob", 1)); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(raw), "data.gob", "information.gob", 1)), 0600); err != nil {
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
	c := configs[0]
	if c.DecisionInterval != 3600000 || c.Factor.TimeFrame != "event" {
		t.Fatalf("risk horizon and clock conflated: %+v", c)
	}
	result, err := runner.Run(context.Background(), c, nil, nil)
	if err != nil || result.Decisions != 8 {
		t.Fatalf("custom source clock unusable: %+v %v", result, err)
	}
}

func TestYAMLNestedOptionDecoderPreservesExplicitContract(t *testing.T) {
	var c runner.ExecutionConfig
	err := decodeFactorFields(map[string]any{"instruments": map[int]any{1: map[string]any{"option": map[string]any{"strike": "123.45"}}}}, &c)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(c.Instruments[1].Option)
	if err != nil || !strings.Contains(string(raw), "123.45") {
		t.Fatalf("option contract lost: %s %v", raw, err)
	}
}

func TestYAMLExplicitCashOptionContractAndModeBoundary(t *testing.T) {
	executionFields := `execution:
  funding_policy: explicit-zero
  margin_rate: '0.1'
  max_account_margin: '10000'
  max_virtual_gross: '20000'
  strategy_gross_limit: '10000'
  instruments:
    1:
      id: sid:1
      version: v1
      valuation: cash_european_option
      settlement_currency: USD
      quantity_step: '1'
      price_tick: '0.01'
      contract_size: '1'
      money_scale: 8
      option:
        underlying_id: underlying-1
        right: call
        strike: '123.45'
        expiry_ms: 90000000
        underlying_stress_max: '1000'
    2: {id: 'sid:2', version: v1, valuation: linear_perpetual, settlement_currency: USD, quantity_step: '1', price_tick: '0.01', contract_size: '1', money_scale: 8}
    3: {id: 'sid:3', version: v1, valuation: linear_perpetual, settlement_currency: USD, quantity_step: '1', price_tick: '0.01', contract_size: '1', money_scale: 8}
    4: {id: 'sid:4', version: v1, valuation: linear_perpetual, settlement_currency: USD, quantity_step: '1', price_tick: '0.01', contract_size: '1', money_scale: 8}`
	path := capabilityYAML(t, "      option_expiry_source: expiry", executionFields)
	spec, err := loadFactorYAMLSpec([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	configs, err := buildFactorConfigs(spec, runner.Events)
	if err != nil {
		t.Fatal(err)
	}
	i := configs[0].Execution.Instruments[1]
	if !i.IsOption() || i.Option.Strike.String() != "123.45" {
		t.Fatalf("explicit contract lost: %+v", i)
	}
	for _, mode := range []runner.Mode{runner.Weights, runner.Trade} {
		if _, err := buildFactorConfigs(spec, mode); err == nil {
			t.Fatalf("option accepted for %s", mode)
		}
	}
}

func TestYAMLLiveDraftPreservesNonArchivalLabels(t *testing.T) {
	path := capabilityYAML(t, "      research:\n        labels: [{name: live-close, kind: close-to-close, horizon: 3600000, periods_per_year: 8766}]", "")
	spec, err := loadFactorYAMLSpec([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	configs, err := buildFactorConfigs(spec, runner.Trade)
	if err != nil {
		t.Fatal(err)
	}
	if got := configs[0].Manifest.Labels[0].Kind; got != "close-to-close" {
		t.Fatalf("live label changed: %s", got)
	}
}
