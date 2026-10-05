package entry

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor/runner"
	"github.com/spf13/cobra"
)

func newStrategyBacktestCommand(factory FactorSinkFactory) *cobra.Command {
	var mode string
	var command *cobra.Command
	command = newConfigCommandContext("backtest", "backtest time-series and factor strategies from run_policy", func(ctx context.Context, args *config.CmdArgs) error {
		spec, loadErr := config.LoadRunSpec(args, false)
		if loadErr != nil {
			return loadErr
		}
		if mode != "" {
			if mode != string(runner.Weights) && mode != string(runner.Events) {
				return errors.New("backtest: --mode must be weights or events")
			}
			spec = spec.WithExecutionMode(mode, "--mode")
		}
		if slices.Contains(spec.Engines(), config.EngineFactor) {
			if args.Separate {
				return errors.New("backtest: --separate is only supported for time-series policies; use separate configurations for factor tasks")
			}
			if factory != nil {
				if slices.Contains(spec.Engines(), config.EngineTimeSeries) {
					return errors.New("backtest: a custom factor sink cannot replace the mixed engine account owner")
				}
				configs, err := validatedFactorBacktestConfigs(spec)
				if err != nil {
					return err
				}
				return runFactorReplayCommand(ctx, args, spec, configs, factory, command.OutOrStdout())
			}
			if err := unifiedFactorBacktestOutputContext(ctx, args, spec, command.OutOrStdout()); err != nil {
				return err
			}
			return nil
		}
		if mode == string(runner.Weights) {
			return errors.New("backtest: weights mode requires a factor policy")
		}
		if err := runExplicitBackTestSpecContext(ctx, args, spec); err != nil {
			return err
		}
		return nil
	}, true, bindOut, bindTimeRange, bindTimeStart, bindTimeEnd, bindStakeAmount, bindPairs, bindProgress, bindSeparate, bindBTStrict)
	command.Flags().StringVar(&mode, "mode", "", "override factor execution.mode: weights or events (default: YAML mode, then events)")
	return command
}

func newStrategyTradeCommand() *cobra.Command {
	var provider string
	var dryRun bool
	var command *cobra.Command
	command = newConfigCommandContext("trade", "trade time-series and factor strategies from run_policy", func(ctx context.Context, args *config.CmdArgs) error {
		spec, loadErr := config.LoadRunSpec(args, false)
		if loadErr != nil {
			return loadErr
		}
		if slices.Contains(spec.Engines(), config.EngineFactor) {
			if dryRun {
				spec = spec.WithExecutionMode(string(runner.Events), "--dry-run")
				if err := unifiedFactorBacktestOutputContext(ctx, args, spec, command.OutOrStdout()); err != nil {
					return err
				}
				return nil
			}
			configs, err := buildFactorConfigs(spec, runner.Trade)
			if err != nil {
				return err
			}
			return runFactorLiveSpecWithArgs(ctx, args, spec, configs, provider, command.OutOrStdout(), nil)
		}
		if dryRun || provider != "" {
			return errors.New("trade: --dry-run and --live-provider require a factor policy; use env: dry_run for time-series live simulation")
		}
		if err := runExplicitTradeSpecContext(ctx, args, spec, nil); err != nil {
			return err
		}
		return nil
	}, false, bindStakeAmount, bindPairs, bindSpider, bindOut)
	command.Flags().BoolVar(&dryRun, "dry-run", false, "replay factor and mixed policies with historical input and simulated execution")
	command.Flags().StringVar(&provider, "live-provider", "", "override the registered factor live binding")
	return command
}

func newResearchCommand() *cobra.Command {
	var command *cobra.Command
	command = newConfigCommandContext("research", "research factor policies with immutable historical input", func(ctx context.Context, args *config.CmdArgs) error {
		spec, loadErr := config.LoadRunSpec(args, false)
		if loadErr != nil {
			return loadErr
		}
		configs, err := buildFactorConfigs(spec, runner.Research)
		if err != nil {
			return err
		}
		if err := validateFactorReplayConfigs(configs); err != nil {
			return err
		}
		if err := validateAccountHistoryPaths(spec, configs); err != nil {
			return err
		}
		return runFactorReplayCommand(ctx, args, spec, configs, nil, command.OutOrStdout())
	}, true, bindTimeRange, bindTimeStart, bindTimeEnd, bindStakeAmount, bindPairs)
	return command
}

func runFactorReplayCommand(ctx context.Context, args *config.CmdArgs, spec *config.RunSpec, configs []runner.Config, factory FactorSinkFactory, out io.Writer) (resultErr error) {
	stopProfiles, profileErr := startProfilesFor(args.CPUProfile, args.MemProfile)
	if profileErr != nil {
		return profileErr
	}
	if stopProfiles != nil {
		defer stopProfiles()
	}
	configs, cleanup, prepareErr := prepareFactorStorageInputs(ctx, args, spec, configs)
	if prepareErr != nil {
		return prepareErr
	}
	defer func() { resultErr = errors.Join(resultErr, cleanup()) }()
	results, err := runFactorConfigs(ctx, configs, factory, out)
	if err != nil {
		return err
	}
	return writeFactorResults(out, results)
}

func writeFactorResults(out io.Writer, results []runner.Result) error {
	if len(results) == 1 {
		return json.NewEncoder(out).Encode(results[0])
	}
	return json.NewEncoder(out).Encode(results)
}
