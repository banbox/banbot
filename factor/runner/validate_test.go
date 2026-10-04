package runner

import (
	"context"
	"errors"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/shopspring/decimal"
)

func TestReplayPreflightRejectsInvalidChoicesBeforeExecutionResources(t *testing.T) {
	base := paperConfig(t, archiveConfig(t, false))
	base.Mode = Events
	tests := []struct {
		name, reason string
		change       func(*Config)
	}{
		{"pending", "bounded", func(c *Config) { c.MaxPending = 0 }},
		{"delay", "bounded", func(c *Config) { c.DecisionDelayMS = -1 }},
		{"latency", "bounded", func(c *Config) { c.LatencyMS = 0 }},
		{"expiry", "bounded", func(c *Config) { c.ExpiryMS = c.LatencyMS }},
		{"label-wait", "bounded", func(c *Config) { c.LabelWaitMS = -1 }},
		{"price", "observable", func(c *Config) { c.Prices.TimeFrame = "1h" }},
		{"label", "label", func(c *Config) { c.Manifest.Labels[0].Horizon = 0 }},
		{"label-rate", "label", func(c *Config) { c.Manifest.Labels[0].PeriodsPerYear = math.Inf(1) }},
		{"funding", "funding stream", func(c *Config) { c.FundingSource = "" }},
		{"builder", "unregistered portfolio", func(c *Config) { c.Manifest.Portfolio.Builder = "missing-preflight-builder" }},
		{"capital", "capital", func(c *Config) { c.InitialNAV = math.NaN() }},
		{"risk", "risk limits", func(c *Config) { c.Execution.MarginRate = decimal.NewFromInt(2) }},
		{"units", "instrument", func(c *Config) {
			unit := c.Execution.Instruments[1]
			unit.QuantityStep = decimal.Zero
			c.Execution.Instruments[1] = unit
		}},
		{"missing-unit", "missing execution", func(c *Config) { delete(c.Execution.Instruments, 1) }},
		{"ranges", "non-overlapping", func(c *Config) { c.Chunks = append(c.Chunks, c.Chunks[0]) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := base
			c.Manifest.Labels = append(c.Manifest.Labels[:0:0], base.Manifest.Labels...)
			c.Execution.Instruments = map[int32]execution.Instrument{}
			for sid, unit := range base.Execution.Instruments {
				c.Execution.Instruments[sid] = unit
			}
			test.change(&c)
			preflightErr := ValidateReplayConfig(c, true)
			_, runErr := Run(context.Background(), c, nil, nil)
			if preflightErr == nil || runErr == nil || preflightErr.Error() != runErr.Error() || !strings.Contains(preflightErr.Error(), test.reason) {
				t.Fatalf("preflight=%v runtime=%v expected=%s", preflightErr, runErr, test.reason)
			}
			for _, path := range []string{base.Execution.StorePath, base.Execution.SenderLeaseDir} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("invalid replay created execution resource %s: %v", path, err)
				}
			}
		})
	}
}

type validationInputFactory struct {
	ranges []Chunk
	opened bool
}

func (*validationInputFactory) Identity() string  { return "validation-input" }
func (f *validationInputFactory) Ranges() []Chunk { return f.ranges }
func (f *validationInputFactory) Open(context.Context, Config, Chunk) (HistoricalInput, error) {
	f.opened = true
	return nil, errors.New("validation must not open input")
}

func TestReplayPreflightDefersStorageMetadataAndChecksFactoryRanges(t *testing.T) {
	c := archiveConfig(t, false)
	c.Mode, c.Chunks, c.Execution = Events, nil, ExecutionConfig{}
	if err := ValidateReplayConfig(c, false); err != nil {
		t.Fatalf("ordinary storage preflight required unassembled metadata: %v", err)
	}
	if err := ValidateReplayConfig(c, true); err == nil {
		t.Fatal("execution accepted absent normalized metadata")
	}
	c.Mode = Weights
	for _, ranges := range [][]Chunk{nil, {{From: 10, To: 20}, {From: 20, To: 30}}, {{From: 10, To: 20}, {From: 21, To: 30}}} {
		factory := &validationInputFactory{ranges: ranges}
		c.HistoricalInput = factory
		err := ValidateReplayConfig(c, false)
		valid := len(ranges) == 2 && ranges[1].From == 21
		if (err == nil) != valid || factory.opened {
			t.Fatalf("ranges=%v error=%v opened=%v", ranges, err, factory.opened)
		}
	}
}
