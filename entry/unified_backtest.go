package entry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banexg/errs"
)

func factorBacktestMode(spec *config.RunSpec) (runner.Mode, error) {
	u := spec.Config()
	mode := runner.Events
	if configured, ok := u.Execution["mode"].(string); ok {
		mode = runner.Mode(configured)
	}
	if mode != runner.Weights && mode != runner.Events {
		return "", errors.New("backtest execution.mode must be weights or events")
	}
	if mode != runner.Events && slices.Contains(spec.Engines(), config.EngineTimeSeries) {
		return "", errors.New("mixed backtest requires execution.mode: events")
	}
	return mode, nil
}

// ValidateBacktestRunSpec lets Web and CLI share factor preflight without
// creating an exchange, storage connection or account. Pure TS keeps its
// existing validation and execution profile at the caller.
func ValidateBacktestRunSpec(spec *config.RunSpec) error {
	if spec == nil {
		return errors.New("backtest RunSpec is required")
	}
	if !slices.Contains(spec.Engines(), config.EngineFactor) {
		return validateLegacyHistory(spec)
	}
	mode, err := factorBacktestMode(spec)
	if err != nil {
		return err
	}
	configs, err := buildFactorConfigs(spec, mode)
	if err != nil {
		return err
	}
	if err := validateFactorReplayConfigs(configs); err != nil {
		return err
	}
	return validateAccountHistoryPaths(spec, configs)
}

func validateFactorReplayConfigs(configs []runner.Config) error {
	for _, c := range configs {
		if err := runner.ValidateReplayConfig(c, len(c.Chunks) > 0); err != nil {
			return err
		}
	}
	return nil
}

func unifiedFactorBacktest(args *config.CmdArgs, spec *config.RunSpec) (resultErr *errs.Error) {
	return unifiedFactorBacktestContext(context.Background(), args, spec)
}

func unifiedFactorBacktestContext(ctx context.Context, args *config.CmdArgs, spec *config.RunSpec) (resultErr *errs.Error) {
	if err := ctx.Err(); err != nil {
		return errs.New(core.ErrRunTime, err)
	}
	mode, err := factorBacktestMode(spec)
	if err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	configs, err := buildFactorConfigs(spec, mode)
	if err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	if err := validateFactorReplayConfigs(configs); err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	if err := validateAccountHistoryPaths(spec, configs); err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	configs, cleanup, err := prepareFactorStorageInputs(ctx, args, spec, configs)
	if err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	joinError := func(cause error, code int) {
		if cause == nil {
			return
		}
		var primary error
		if resultErr != nil {
			primary = resultErr
		}
		// errs.New unwraps the first nested *errs.Error, discarding joined
		// siblings. Preserve the complete failure text at this API boundary.
		resultErr = errs.NewMsg(code, "%v", errors.Join(primary, cause))
	}
	storageClosed := false
	closeStorage := func() error {
		if storageClosed {
			return nil
		}
		storageClosed = true
		return cleanup()
	}
	defer func() { joinError(closeStorage(), core.ErrRunTime) }()
	base := args.OutPath
	if base == "" {
		dir := args.DataDir
		if dir == "" {
			dir = os.Getenv("BanDataDir")
		}
		if dir == "" {
			if len(configs[0].Chunks) > 0 {
				dir = filepath.Dir(configs[0].Chunks[0].Path)
			} else if snapshot, snapshotErr := spec.RuntimeSnapshot(); snapshotErr == nil {
				dir = snapshot.DataDir
			}
		}
		base = filepath.Join(dir, "backtest", "factor")
	}
	path, err := config.AllocateOutputDir(base)
	if err != nil {
		return errs.New(core.ErrIOWriteFail, err)
	}
	raw, err := spec.EffectiveYAML(true)
	if err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	if err := config.WriteConfigAtomic(filepath.Join(path, "config.yml"), nil, raw); err != nil {
		return errs.New(core.ErrIOWriteFail, err)
	}
	if err := writeResolvedFactorConfig(filepath.Join(path, "resolved.json"), spec, configs); err != nil {
		return errs.New(core.ErrIOWriteFail, err)
	}
	file, err := os.OpenFile(filepath.Join(path, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errs.New(core.ErrIOWriteFail, err)
	}
	var results []runner.Result
	defer func() {
		resultErr = finishUnifiedFactorBacktest(path, file, closeStorage, results, resultErr)
	}()
	for i := range configs {
		configs[i].ArtifactPath = filepath.Join(path, fmt.Sprintf("strategy-%d.json", i+1))
	}
	if slices.Contains(spec.Engines(), config.EngineTimeSeries) {
		results, err = runMixedFactorConfigs(ctx, spec, configs, file)
	} else {
		results, err = runFactorConfigs(ctx, configs, nil, file)
	}
	if err != nil {
		return errs.New(core.ErrRunTime, err)
	}
	return nil
}

// Completion includes closing the output and storage runtime. Publish the
// aggregate status only after both have joined, preserving every failure.
func finishUnifiedFactorBacktest(path string, file *os.File, cleanup func() error, results []runner.Result, resultErr *errs.Error) *errs.Error {
	joinError := func(cause error, code int) {
		if cause != nil {
			var primary error
			if resultErr != nil {
				primary = resultErr
			}
			resultErr = errs.NewMsg(code, "%v", errors.Join(primary, cause))
		}
	}
	joinError(errors.Join(file.Sync(), file.Close()), core.ErrIOWriteFail)
	joinError(cleanup(), core.ErrRunTime)
	status := "complete"
	var reasons []string
	if resultErr != nil {
		status, reasons = "incomplete", []string{resultErr.Error()}
	}
	for _, result := range results {
		if result.Unresolved > 0 {
			status = "incomplete"
		}
	}
	artifact := struct {
		Version int
		Status  string
		Errors  []string `json:",omitempty"`
		Results []runner.Result
	}{1, status, reasons, results}
	body, err := json.Marshal(artifact)
	if err == nil {
		if writeErr := config.WriteConfigAtomic(filepath.Join(path, "run.json"), nil, append(body, '\n')); writeErr != nil {
			err = writeErr
		}
	}
	joinError(err, core.ErrIOWriteFail)
	return resultErr
}
