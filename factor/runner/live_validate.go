package runner

import (
	"errors"
	"fmt"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
)

// ValidateLiveConfig checks live decisions without creating sessions, borrowing
// an account or consulting a venue. Capital comes from the reconciled sink;
// live definitions are not restricted to archival return-label pipelines.
func ValidateLiveConfig(c Config) error {
	plan, combo, err := compileLiveDecision(c)
	if err != nil {
		return err
	}
	c, err = resolveDecisionPortfolio(c)
	if err != nil {
		return err
	}
	c.Manifest.ExecutionMode = "trade"
	c.Manifest.LatencyAssumption = fmt.Sprintf("live completion clock; observable event after decision+%dms", c.LatencyMS)
	c.Manifest = decisionManifestSpec(c, plan, combo)
	_, err = research.BuildManifest(c.Manifest)
	return err
}

func compileLiveDecision(c Config) (*factor.Plan, research.ComboSpec, error) {
	var combo research.ComboSpec
	if c.Execution.HistoryPath != "" {
		return nil, combo, errors.New("runner: cold execution history is supported only by simulated replay")
	}
	if c.ComputationGroup != nil && (c.ComputationContext.DataNamespace == "" || c.ComputationContext.ClockDomain == "" || c.ComputationContext.SamplingIdentity == "") {
		return nil, combo, errors.New("runner: live shared computation needs data namespace, clock and sampling identity")
	}
	if c.DecisionInterval <= 0 || c.DecisionDelayMS < 0 || c.LatencyMS <= 0 || c.ExpiryMS <= c.LatencyMS {
		return nil, combo, errors.New("runner: live requires a valid decision cadence and execution window")
	}
	if c.Manifest.Costs.FundingPolicy == "required-stream" && c.FundingSource == "" {
		return nil, combo, errors.New("runner: live funding stream absent")
	}
	if c.Manifest.Costs.FundingPolicy != "required-stream" && c.Manifest.Costs.FundingPolicy != "explicit-zero" {
		return nil, combo, errors.New("runner: explicit live funding policy required")
	}
	method := c.Combo.Method
	if method == "" && c.Expressions != nil {
		method = c.Expressions.Combine.Method
	}
	if research.IsHistoryMethod(method) {
		return nil, combo, errors.New("runner: live history-IC requires an explicit matured history provider; use fixed/equal")
	}
	plan, combo, err := compileDecision(c)
	if err != nil {
		return nil, combo, err
	}
	if research.IsHistoryMethod(combo.Method) {
		return nil, combo, errors.New("runner: live history-IC requires an explicit matured history provider; use fixed/equal")
	}
	return plan, combo, nil
}
