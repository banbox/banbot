package entry

import (
	"errors"
	"fmt"
	"slices"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/web/dev"
)

// InspectBacktestRunSpec shares the ordinary defaults and definition compiler,
// but leaves archive inspection, storage and execution readiness to submission.
func InspectBacktestRunSpec(spec *config.RunSpec) (*dev.BacktestInspection, error) {
	if spec == nil {
		return nil, errors.New("backtest RunSpec is required")
	}
	u := spec.Config()
	if len(u.RunPolicy) == 0 {
		return nil, errors.New("run_policy is required")
	}
	report := &dev.BacktestInspection{Version: 1, Engines: spec.Engines(), Strategies: []dev.StrategyInspection{}, Warnings: []dev.PreflightIssue{{Code: "data_not_checked", Message: "Static validation does not check data coverage, archive identity, market metadata or live readiness."}}}
	var configs []runner.Config
	var resolved []map[string]resolvedFactorValue
	if slices.Contains(report.Engines, config.EngineFactor) {
		mode, err := factorBacktestMode(spec)
		if err != nil {
			return nil, err
		}
		report.ExecutionMode = string(mode)
		configs, err = buildFactorConfigsWithArchiveInspection(spec, mode, false)
		if err != nil {
			return nil, err
		}
		for _, c := range configs {
			if err := runner.ValidateStaticConfig(c); err != nil {
				return nil, err
			}
			if len(c.Chunks) > 0 {
				report.Warnings = append(report.Warnings, dev.PreflightIssue{Code: "archive_not_checked", Message: "Archive contents, inferred prices and execution units will be checked when submitting the backtest."})
			}
		}
		if err := validateAccountHistoryPaths(spec, configs); err != nil {
			return nil, err
		}
		resolved, err = resolvedFactorValues(spec, configs)
		if err != nil {
			return nil, err
		}
	} else if err := validateLegacyHistory(spec); err != nil {
		return nil, err
	}
	position := 0
	for _, policy := range u.RunPolicy {
		item := dev.StrategyInspection{Engine: policy.Engine, Name: policy.Name, ID: policy.ID, Account: policy.Account}
		if policy.Engine == config.EngineFactor {
			c := configs[position]
			item.ID, item.Account = c.StrategyID, c.AccountID
			item.TimeFrame = c.Factor.TimeFrame
			item.Resolved = resolved[position]
			// Unknown archive-derived evidence must not be presented as a default.
			for _, key := range []string{"price_source", "price_timeframe", "price_field", "visibility_policy", "funding_policy"} {
				if value := resolved[position][key]; value.Value == "" {
					delete(resolved[position], key)
				}
			}
			position++
		} else {
			// Legacy More fields named id/account stay custom parameters.
			frames := policy.RunTimeframes
			if len(frames) == 0 {
				frames = u.Root.RunTimeframes
			}
			item.TimeFrame = fmt.Sprint(frames)
			if item.ID == "" {
				item.ID = policy.Name
			}
		}
		report.Strategies = append(report.Strategies, item)
	}
	raw, err := spec.EffectiveYAML(true)
	if err != nil {
		return nil, err
	}
	report.EffectiveConfig = string(raw)
	return report, nil
}
