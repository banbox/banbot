package entry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/shopspring/decimal"
)

func TestBacktestPreflightRejectsInvalidImportedReplayBeforeResources(t *testing.T) {
	tests := []struct {
		name, reason string
		change       func(*runner.Config)
	}{
		{"pending", "bounded", func(c *runner.Config) { c.MaxPending = 0 }},
		{"latency", "bounded", func(c *runner.Config) { c.LatencyMS = 0 }},
		{"labels", "label", func(c *runner.Config) { c.Manifest.Labels[0].Horizon = 0 }},
		{"price", "observable", func(c *runner.Config) { c.Prices.Frequency = "1h" }},
		{"funding", "funding stream", func(c *runner.Config) { c.Manifest.Costs.FundingPolicy = "required-stream"; c.FundingSource = "" }},
		{"builder", "unregistered portfolio", func(c *runner.Config) { c.Manifest.Portfolio.Builder = "missing-entry-builder" }},
		{"risk", "risk limits", func(c *runner.Config) { c.Execution.MarginRate = decimal.Zero }},
		{"units", "instrument", func(c *runner.Config) {
			unit := c.Execution.Instruments[1]
			unit.QuantityStep = decimal.Zero
			c.Execution.Instruments[1] = unit
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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
			c.Mode = runner.Events
			c.Chunks[0].Path = "data.gob"
			c.Execution = runner.ExecutionConfig{StorePath: "rejected.db", SenderLeaseDir: "rejected-leases", Instruments: map[int32]execution.Instrument{}, MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(10000), MaxVirtualGross: decimal.NewFromInt(20000), StrategyGrossLimit: decimal.NewFromInt(10000)}
			for sid, symbol := range c.Snapshot.SIDMap {
				c.Execution.Instruments[sid] = execution.Instrument{ID: symbol, Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.RequireFromString("0.01"), PriceTick: decimal.RequireFromString("0.01"), ContractSize: decimal.NewFromInt(1), MoneyScale: 8}
			}
			test.change(&c)
			body, err := json.Marshal(c)
			if err != nil {
				t.Fatal(err)
			}
			legacy := filepath.Join(dir, "invalid.json")
			if err := os.WriteFile(legacy, body, 0600); err != nil {
				t.Fatal(err)
			}
			spec, err = loadFactorRunSpec(nil, legacy)
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateBacktestRunSpec(spec); err == nil || !strings.Contains(err.Error(), test.reason) {
				t.Fatalf("invalid imported replay passed preflight: %v expected=%s", err, test.reason)
			}
			out := filepath.Join(dir, "rejected-output")
			if err := RunBackTest(&config.CmdArgs{Configs: config.ArrString{legacy + ".v2.yml"}, NoDefault: true, OutPath: out}); err == nil || !strings.Contains(err.Error(), test.reason) {
				t.Fatalf("CLI bypassed shared preflight: %v expected=%s", err, test.reason)
			}
			for _, resource := range []string{"rejected.db", "rejected-leases", "rejected-output"} {
				if _, err := os.Stat(filepath.Join(dir, resource)); !os.IsNotExist(err) {
					t.Fatalf("invalid replay created %s before rejection: %v", resource, err)
				}
			}
		})
	}
}

func TestBacktestPreflightAcceptsOrdinaryStorageWithoutMarketUnits(t *testing.T) {
	spec := storageEntrySpec(t, "static-approximation")
	if err := ValidateBacktestRunSpec(spec); err != nil {
		t.Fatalf("simple storage events configuration rejected before metadata assembly: %v", err)
	}
	configs, err := buildFactorConfigs(spec, runner.Events)
	if err != nil || len(configs[0].Execution.Instruments) != 0 {
		t.Fatalf("storage preflight created execution metadata: configs=%+v error=%v", configs, err)
	}
}
