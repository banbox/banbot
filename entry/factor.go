package entry

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/spf13/cobra"
	"io"
	"os"
)

// FactorSinkFactory lets embedded runtime owners attach reconciled account
// execution without giving archival research an exchange/global dependency.
type FactorSinkFactory func(context.Context, runner.Config, bool) (runner.Sink, func() error, error)

func newFactorCommand() *cobra.Command {
	return NewFactorCommandWithSink(nil)
}
func NewFactorCommandWithSink(factory FactorSinkFactory) *cobra.Command {
	root := &cobra.Command{Use: "factor", Short: "replay immutable factor archives", Args: cobra.NoArgs}
	for _, name := range []string{"research", "backtest", "trade"} {
		name := name
		var configPath, mode string
		var liveProvider string
		var factorConfigs []string
		var dryRun bool
		cmd := &cobra.Command{Use: name, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) (resultErr error) {
			spec, err := loadFactorRunSpec(factorConfigs, configPath)
			if err != nil {
				return err
			}
			runMode := runner.Mode(mode)
			if name == "research" {
				runMode = runner.Research
			}
			if name == "trade" {
				runMode = runner.Trade
			}
			if name == "trade" && dryRun {
				runMode = runner.Events
			}
			configs, err := buildFactorConfigs(spec, runMode)
			if err != nil {
				return err
			}
			if runMode != runner.Trade {
				if err := validateFactorReplayConfigs(configs); err != nil {
					return err
				}
				if err := validateAccountHistoryPaths(spec, configs); err != nil {
					return err
				}
			}
			cfg := configs[0]
			switch name {
			case "research":
				cfg.Mode = runner.Research
			case "trade":
				cfg.Mode = runMode
				if !dryRun {
					return runFactorLiveSpec(cmd.Context(), spec, configs, liveProvider, cmd.OutOrStdout())
				}
			default:
				cfg.Mode = runner.Mode(mode)
				if cfg.Mode != runner.Weights && cfg.Mode != runner.Events {
					return errors.New("factor backtest: --mode must be weights or events")
				}
			}
			configs, cleanup, err := prepareFactorStorageInputs(cmd.Context(), &config.CmdArgs{}, spec, configs)
			if err != nil {
				return err
			}
			defer func() { resultErr = errors.Join(resultErr, cleanup()) }()
			results, err := runFactorConfigs(cmd.Context(), configs, factory, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			if len(results) == 1 {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(results[0])
			}
			return json.NewEncoder(cmd.OutOrStdout()).Encode(results)
		}}
		cmd.Flags().StringVar(&configPath, "factor-config", "", "legacy JSON importer; writes v2 YAML and runs the unified configuration")
		cmd.Flags().StringSliceVar(&factorConfigs, "config", nil, "unified v2 YAML configuration and overlays")
		if name == "backtest" {
			cmd.Flags().StringVar(&mode, "mode", "weights", "weights or events")
		}
		if name == "trade" {
			cmd.Flags().BoolVar(&dryRun, "dry-run", false, "execute through local ledger and simulated adapter")
			cmd.Flags().StringVar(&liveProvider, "live-provider", "", "registered verified Banexg live binding")
		}
		root.AddCommand(cmd)
	}
	var input, path, schemaPath string
	var maxRows int
	archive := &cobra.Command{Use: "archive", Short: "freeze version-record JSON lines into an immutable typed archive", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		schema, err := readArchiveFieldTypes(schemaPath)
		if err != nil {
			return err
		}
		store, err := factor.NewVersionStore(maxRows)
		if err != nil {
			return err
		}
		f, err := os.Open(input)
		if err != nil {
			return err
		}
		defer f.Close()
		dec := json.NewDecoder(f)
		dec.DisallowUnknownFields()
		dec.UseNumber()
		for {
			if err = cmd.Context().Err(); err != nil {
				return err
			}
			var row factor.VersionRecord
			err = dec.Decode(&row)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			if err = restoreArchiveRecordTypes(&row, schema); err != nil {
				return err
			}
			if err = store.Put(row); err != nil {
				return err
			}
		}
		digest, err := store.Export(path)
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(map[string]string{"archive": path, "digest": digest})
	}}
	archive.Flags().StringVar(&input, "input", "", "version records JSON lines")
	archive.Flags().StringVar(&schemaPath, "schema", "", "YAML source-to-field type map; required for exact large integers")
	archive.Flags().StringVar(&path, "out", "", "new immutable gob archive path")
	archive.Flags().IntVar(&maxRows, "max-records", 100000, "hard archive record limit")
	_ = archive.MarkFlagRequired("input")
	_ = archive.MarkFlagRequired("out")
	root.AddCommand(archive)
	return root
}
