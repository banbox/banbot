package opt

import (
	"slices"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banexg/errs"
)

// BacktestFactory creates one isolated backtest runtime from an owned config
// snapshot. The cleanup function releases only state created for that run.
type BacktestFactory func(snapshot *config.Snapshot, isOpt bool, outDir string) (*BackTest, func(), *errs.Error)

func deriveBacktestSnapshot(source *config.Snapshot, startMS, endMS int64, pairs []string,
	policies []*config.RunPolicyConfig) *config.Snapshot {
	if source == nil {
		return nil
	}
	derived := source.Clone()
	cfg := derived.View()
	if cfg == nil {
		return derived
	}
	cfg.TimeRange = &config.TimeTuple{StartMS: startMS, EndMS: endMS}
	cfg.Pairs = slices.Clone(pairs)
	cfg.RunPolicy = make([]*config.RunPolicyConfig, len(policies))
	for i, policy := range policies {
		if policy != nil {
			cfg.RunPolicy[i] = policy.Clone()
		}
	}
	return derived
}

func deriveBacktestSnapshotForPolicies(source *config.Snapshot, policies []*config.RunPolicyConfig) *config.Snapshot {
	if source == nil || source.View() == nil || source.View().TimeRange == nil {
		return nil
	}
	cfg := source.View()
	return deriveBacktestSnapshot(source, cfg.TimeRange.StartMS, cfg.TimeRange.EndMS, cfg.Pairs, policies)
}
