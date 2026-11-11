package entry

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
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
	_, err := validatedFactorBacktestConfigs(spec)
	return err
}

// validatedFactorBacktestConfigs is the resource-free boundary shared by
// preflight and execution. Storage inputs are assembled only after it succeeds.
func validatedFactorBacktestConfigs(spec *config.RunSpec) ([]runner.Config, error) {
	mode, err := factorBacktestMode(spec)
	if err != nil {
		return nil, err
	}
	configs, err := buildFactorConfigs(spec, mode)
	if err != nil {
		return nil, err
	}
	if err := validateFactorReplayConfigs(configs); err != nil {
		return nil, err
	}
	if err := validateAccountHistoryPaths(spec, configs); err != nil {
		return nil, err
	}
	return configs, nil
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
	return unifiedFactorBacktestOutputContext(ctx, args, spec, nil)
}

func unifiedFactorBacktestOutputContext(ctx context.Context, args *config.CmdArgs, spec *config.RunSpec, out io.Writer) (resultErr *errs.Error) {
	if err := ctx.Err(); err != nil {
		return errs.New(core.ErrRunTime, err)
	}
	configs, err := validatedFactorBacktestConfigs(spec)
	if err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	stopProfiles, profileErr := startProfilesFor(args.CPUProfile, args.MemProfile)
	if profileErr != nil {
		return profileErr
	}
	if stopProfiles != nil {
		defer stopProfiles()
	}
	configs, cleanup, skippedPairs, err := prepareFactorStorageInputsReport(ctx, args, spec, configs)
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
	if strings.HasPrefix(base, "$") || strings.HasPrefix(base, "@") {
		dir := config.ResolveDataDir(args.DataDir)
		if dir == "" {
			return errs.NewMsg(core.ErrBadConfig, "DataDir is required for prefixed output paths")
		}
		base = config.NewSnapshotWithDirs(nil, dir, "").ParsePath(base)
	}
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
	// Only the file side is buffered; stdout (out) keeps per-event delivery because
	// bansite derives progress and the run start time from the decision events.
	// Every exit path (return, error, panic unwinding) reaches the deferred finish,
	// whose Sync flushes the buffer before the file is closed.
	events := newBufferedEventFile(file)
	var results []runner.Result
	defer func() {
		resultErr = finishUnifiedFactorBacktestSkipped(path, events, closeStorage, results, resultErr, skippedPairs)
	}()
	for i := range configs {
		configs[i].ArtifactPath = filepath.Join(path, fmt.Sprintf("strategy-%d.json", i+1))
	}
	var writer io.Writer = events
	if out != nil {
		writer = io.MultiWriter(events, out)
	}
	if slices.Contains(spec.Engines(), config.EngineTimeSeries) {
		results, err = runMixedFactorConfigs(ctx, spec, configs, writer)
	} else {
		results, err = runFactorConfigs(ctx, configs, nil, writer)
	}
	if err != nil {
		return errs.New(core.ErrRunTime, err)
	}
	if out != nil {
		if err := writeFactorResults(out, results); err != nil {
			return errs.New(core.ErrIOWriteFail, err)
		}
	}
	return nil
}

// eventsFile is the events.jsonl handle finishUnifiedFactorBacktest syncs and closes.
type eventsFile interface {
	Sync() error
	Close() error
}

// bufferedEventFile batches the many small JSON event lines into large file writes
// (one write syscall per line dominated statarb runs). Bytes and order are
// unchanged; Sync flushes first, so Sync+Close persists everything written.
type bufferedEventFile struct {
	*os.File
	buffer *bufio.Writer
}

func newBufferedEventFile(file *os.File) *bufferedEventFile {
	return &bufferedEventFile{File: file, buffer: bufio.NewWriterSize(file, 1<<20)}
}

func (f *bufferedEventFile) Write(p []byte) (int, error) { return f.buffer.Write(p) }

func (f *bufferedEventFile) Sync() error {
	return errors.Join(f.buffer.Flush(), f.File.Sync())
}

// Completion includes closing the output and storage runtime. Publish the
// aggregate status only after both have joined, preserving every failure.
func finishUnifiedFactorBacktest(path string, file eventsFile, cleanup func() error, results []runner.Result, resultErr *errs.Error) *errs.Error {
	return finishUnifiedFactorBacktestSkipped(path, file, cleanup, results, resultErr, nil)
}

// finishUnifiedFactorBacktestSkipped writes run.json; skipped (pool members without stored data inside the window, BtFeatureLevel 9)
// becomes the `skipped_pairs` field, present only when something was skipped so every other run.json stays byte-identical.
func finishUnifiedFactorBacktestSkipped(path string, file eventsFile, cleanup func() error, results []runner.Result, resultErr *errs.Error, skipped []orm.SkippedPair) *errs.Error {
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
		// SkippedPairs: pool members the engine left out for this window (no stored klines inside it); omitted when none
		SkippedPairs []orm.SkippedPair `json:"skipped_pairs,omitempty"`
	}{1, status, reasons, results, skipped}
	body, err := json.Marshal(artifact)
	if err == nil {
		if writeErr := config.WriteConfigAtomic(filepath.Join(path, "run.json"), nil, append(body, '\n')); writeErr != nil {
			err = writeErr
		}
	}
	joinError(err, core.ErrIOWriteFail)
	return resultErr
}
