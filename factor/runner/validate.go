package runner

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/shopspring/decimal"
)

// ValidateReplayConfig checks deterministic replay choices without opening an
// input, execution sink or runtime. Ordinary storage may defer its input and
// market-derived execution units until resource assembly. Explicit units are
// checked even when requireExecutionMetadata is false.
func ValidateReplayConfig(c Config, requireExecutionMetadata bool) error {
	return validateReplayConfig(c, requireExecutionMetadata, true)
}

// ValidateStaticConfig checks a draft without inspecting inputs. Archive-derived
// price identities and ranges remain unchecked until the ordinary preflight.
func ValidateStaticConfig(c Config) error {
	return validateReplayConfig(c, false, false)
}

func validateReplayConfig(c Config, requireExecutionMetadata, checkInputs bool) error {
	if c.Mode != Research && c.Mode != Weights && c.Mode != Events && c.Mode != Trade {
		return errors.New("runner: unsupported mode")
	}
	if c.MaxRecords <= 0 || c.MaxPending <= 0 || c.DecisionInterval <= 0 || c.DecisionDelayMS < 0 || c.LatencyMS <= 0 || c.ExpiryMS <= c.LatencyMS || c.LabelWaitMS < 0 || checkInputs && (c.Prices.Source == "" || c.Prices.TimeFrame == "" || c.Prices.Field == "") {
		return errors.New("runner: incomplete bounded replay/price configuration")
	}
	if (c.Mode == Events || c.Mode == Trade) && c.Prices.TimeFrame != "" && c.Prices.TimeFrame != "event" && c.Prices.TimeFrame != "1m" {
		return errors.New("runner: events/trade require tick or 1m observable prices")
	}
	archiveFundingPending := !checkInputs && len(c.Chunks) > 0 && c.Manifest.Costs.FundingPolicy == ""
	if !archiveFundingPending && c.Manifest.Costs.FundingPolicy != "explicit-zero" && c.Manifest.Costs.FundingPolicy != "required-stream" {
		return errors.New("runner: funding must be required-stream or explicit-zero")
	}
	if c.Manifest.Costs.FundingPolicy == "required-stream" && c.FundingSource == "" {
		return errors.New("runner: required funding stream absent")
	}
	if len(c.Manifest.Labels) > 0 && (len(c.Manifest.Labels) != 1 || c.Manifest.Labels[0].Kind != research.ExecutableReturn) {
		return errors.New("runner: archival pipeline currently requires one executable-return horizon")
	}
	if len(c.Manifest.Labels) > 0 {
		if _, err := research.NewLabelQueue(c.Manifest.Labels, 1, 1); err != nil {
			return err
		}
	}
	if c.InitialNAV <= 0 || math.IsNaN(c.InitialNAV) || math.IsInf(c.InitialNAV, 0) || c.AccountInitialNAV < 0 || math.IsNaN(c.AccountInitialNAV) || math.IsInf(c.AccountInitialNAV, 0) {
		return errors.New("runner: invalid replay capital")
	}
	if c.Manifest.Currency == "" || c.Manifest.CodeRevision == "" || c.Manifest.Costs.FeeRate < 0 || c.Manifest.Costs.SlippageRate < 0 || math.IsNaN(c.Manifest.Costs.FeeRate+c.Manifest.Costs.SlippageRate) || math.IsInf(c.Manifest.Costs.FeeRate+c.Manifest.Costs.SlippageRate, 0) {
		return errors.New("runner: invalid replay currency, revision or costs")
	}
	_, combo, err := compileDecision(c)
	if err != nil {
		return err
	}
	if len(c.Manifest.Labels) == 0 && (c.Mode == Research || combo.Method == research.HistoryIC) {
		return errors.New("runner: research and history-IC require executable-return labels")
	}
	if _, err := resolveDecisionPortfolio(c); err != nil {
		return err
	}
	ranges := c.Chunks
	if c.HistoricalInput != nil {
		if len(c.Chunks) != 0 || c.HistoricalInput.Identity() == "" {
			return errors.New("runner: historical input requires a stable identity and cannot also use archives")
		}
		ranges = c.HistoricalInput.Ranges()
		if len(ranges) == 0 {
			return errors.New("runner: historical input requires replay ranges")
		}
	}
	lastTo := int64(0)
	for _, chunk := range ranges {
		if !checkInputs && chunk.From == 0 && chunk.To == 0 {
			continue
		}
		if chunk.From <= lastTo || chunk.To < chunk.From {
			return errors.New("runner: chunks must have strictly ordered non-overlapping replay ranges")
		}
		lastTo = chunk.To
	}
	if c.Mode == Events || c.Mode == Trade {
		if c.Execution.HistoryPath != "" && (c.Mode == Trade || c.Execution.StorePath != "" || c.Execution.SenderLeaseDir != "" || !filepath.IsAbs(c.Execution.HistoryPath)) {
			return errors.New("runner: cold history requires simulated memory execution and an absolute path")
		}
		if (c.Execution.StorePath == "") != (c.Execution.SenderLeaseDir == "") {
			return errors.New("runner: durable paper replay needs both store and lease paths")
		}
		if requireExecutionMetadata || len(c.Execution.Instruments) > 0 {
			if c.AccountInitialNAV > 0 && c.AccountInitialNAV < c.InitialNAV {
				return errors.New("runner: account capital is smaller than strategy capital")
			}
			e := c.Execution
			if len(e.Instruments) == 0 || !e.MarginRate.IsPositive() || e.MarginRate.GreaterThan(decimal.NewFromInt(1)) || !e.MaxAccountMargin.IsPositive() || !e.MaxVirtualGross.IsPositive() || !e.StrategyGrossLimit.IsPositive() {
				return errors.New("runner: paper execution requires instrument units and risk limits")
			}
			seen := map[string]bool{}
			for sid, unit := range e.Instruments {
				if err := unit.Validate(); err != nil {
					return err
				}
				if sid <= 0 || seen[unit.ID] || unit.SettlementCurrency != c.Manifest.Currency || c.Snapshot.SIDMap[sid] != "" && c.Snapshot.SIDMap[sid] != unit.ID {
					return fmt.Errorf("runner: execution SID %d has conflicting instrument identity or currency", sid)
				}
				seen[unit.ID] = true
			}
			for _, sids := range [][]int32{c.Snapshot.Universe.Investable, c.Snapshot.Universe.Tracked} {
				for _, sid := range sids {
					if _, exists := e.Instruments[sid]; !exists {
						return fmt.Errorf("runner: missing execution instrument SID %d", sid)
					}
				}
			}
		}
	}
	return nil
}

func resolveDecisionPortfolio(c Config) (Config, error) {
	if c.PortfolioBuilder == nil && c.Manifest.Portfolio.Builder != "" && c.Manifest.Portfolio.Builder != "top-bottom-k-v1" {
		var ok bool
		c.PortfolioBuilder, ok = portfolioBuilder(c.Manifest.Portfolio.Builder)
		if !ok {
			return c, fmt.Errorf("runner: unregistered portfolio builder %q", c.Manifest.Portfolio.Builder)
		}
	}
	if c.PortfolioBuilder == nil && (c.Manifest.Portfolio.K <= 0 || c.Manifest.Portfolio.LongNotional+c.Manifest.Portfolio.ShortNotional <= 0) {
		return c, errors.New("runner: top/bottom builder needs positive K and notional")
	}
	if c.PortfolioBuilder != nil && c.Manifest.Portfolio.Builder == "" {
		return c, errors.New("runner: custom portfolio builder needs versioned manifest identity")
	}
	if c.PortfolioBuilder == nil && c.Manifest.Portfolio.Builder == "" {
		c.Manifest.Portfolio.Builder = "top-bottom-k-v1"
	}
	if c.PortfolioBuilder == nil && c.Manifest.Portfolio.Mode != factor.Full && c.Manifest.Portfolio.Mode != factor.Patch {
		return c, errors.New("runner: invalid portfolio mode")
	}
	if c.Manifest.Portfolio.LongNotional < 0 || c.Manifest.Portfolio.ShortNotional < 0 || math.IsNaN(c.Manifest.Portfolio.LongNotional+c.Manifest.Portfolio.ShortNotional) || math.IsInf(c.Manifest.Portfolio.LongNotional+c.Manifest.Portfolio.ShortNotional, 0) {
		return c, errors.New("runner: invalid portfolio notional fractions")
	}
	return c, nil
}
