package dev

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm/ormu"
)

// The aggregate is published by the ordinary backtest command after cleanup.
// Its strategy results are preserved without fabricating legacy TS metrics.
type unifiedBacktestReport struct {
	Version int
	Status  string
	Errors  []string `json:",omitempty"`
	Results []runner.Result
}

func webBacktestArgs(spec *config.RunSpec, taskPath string, separate bool) string {
	outPath := taskPath
	if slices.Contains(spec.Engines(), config.EngineFactor) {
		// The task root owns config/logs; unified entry reserves a fresh run
		// directory below it instead of allocating an unrelated sibling.
		outPath += "/run"
	}
	args := fmt.Sprintf("-out %s -prg uiPrg -no-default -config %s/config.yml", outPath, taskPath)
	if separate {
		args = "-separate " + args
	}
	return args
}

func readUnifiedBacktestReport(base string) (*unifiedBacktestReport, string, error) {
	for _, dir := range []string{base, filepath.Join(base, "run")} {
		raw, err := os.ReadFile(filepath.Join(dir, "run.json"))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		var report unifiedBacktestReport
		if err := json.Unmarshal(raw, &report); err != nil {
			return nil, "", fmt.Errorf("unified backtest artifact: %w", err)
		}
		if report.Version != 1 || (report.Status != "complete" && report.Status != "incomplete") {
			return nil, "", fmt.Errorf("unsupported unified backtest artifact version/status")
		}
		return &report, dir, nil
	}
	return nil, "", nil
}

func collectUnifiedBtTask(rootDir, relPath string) (*ormu.Task, error) {
	base, err := resolveReportRoot(rootDir, relPath)
	if err != nil {
		return nil, err
	}
	report, reportDir, err := readUnifiedBacktestReport(base)
	if err != nil || report == nil {
		return nil, err
	}
	spec, specErr := config.LoadRunSpec(&config.CmdArgs{Configs: []string{filepath.Join(reportDir, "config.yml")}, NoDefault: true}, false)
	if specErr != nil {
		return nil, specErr
	}
	cfg := backtestTaskConfig(spec)
	status := int64(ormu.BtStatusDone)
	if report.Status != "complete" || len(report.Errors) > 0 || len(report.Results) == 0 {
		status = ormu.BtStatusFail
	}
	for _, result := range report.Results {
		if result.Unresolved > 0 {
			status = ormu.BtStatusFail
		}
	}
	reportPath, err := filepath.Rel(rootDir, reportDir)
	if err != nil {
		return nil, err
	}
	info, err := json.Marshal(map[string]any{"unified": true, "reportPaths": []string{filepath.ToSlash(reportPath)}, "run": report})
	if err != nil {
		return nil, err
	}
	fileInfo, err := os.Stat(filepath.Join(reportDir, "run.json"))
	if err != nil {
		return nil, err
	}
	start, stop := int64(0), int64(0)
	if cfg.TimeRange != nil {
		start, stop = cfg.TimeRange.StartMS, cfg.TimeRange.EndMS
	}
	return &ormu.Task{Mode: "backtest", Path: relPath, Strats: strings.Join(cfg.Strats(), ","), Periods: strings.Join(cfg.RunTimeFrames(), ","), Pairs: cfg.ShowPairs(),
		CreateAt: fileInfo.ModTime().UnixMilli(), StartAt: start, StopAt: stop, Status: status, Progress: 1, Info: string(info)}, nil
}
