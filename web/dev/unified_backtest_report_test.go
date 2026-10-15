package dev

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm/ormu"
)

func TestWebUnifiedTaskOwnsOrdinaryOutputAndCollectedArtifact(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		name := "factor"
		if mixed {
			name = "mixed"
		}
		t.Run(name, func(t *testing.T) {
			server := newTestDevServer(t.TempDir())
			t.Cleanup(func() { server.Stop(); server.Join() })
			server.backtestPreflight = func(*config.RunSpec) error { return nil }
			raw := webFactorConfig
			if mixed {
				raw = strings.Replace(raw, "execution:\n", "  - name: trend\n    engine: time_series\n    id: trend-ts\n    capital_weight: 0.5\nexecution:\n", 1)
				raw += "exchange: {name: binance}\nmarket_type: linear\ntime_start: '2024-01-01'\ntime_end: '2024-01-02'\nwallet_amounts: {USDT: 10000}\nstake_currency: [USDT]\nstake_amount: 10\ndatabase: {url: postgres://local/test}\nrun_timeframes: [1m]\n"
			}
			requestDir, paths, err := server.prepareBacktestConfigFiles(map[string]string{"@config.yml": raw}, []string{"@config.yml"})
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(requestDir)
			spec, _, runnable, err := server.loadBacktestRunSpec(paths)
			if err != nil {
				t.Fatal(err)
			}
			base, err := config.AllocateOutputDir(filepath.Join(server.BacktestDir(), "task"))
			if err != nil {
				t.Fatal(err)
			}
			if err := config.WriteConfigAtomic(filepath.Join(base, "config.yml"), nil, runnable); err != nil {
				t.Fatal(err)
			}
			args := webBacktestArgs(spec, "$backtest/task", false)
			if !strings.Contains(args, "-out $backtest/task/run ") || !strings.Contains(args, "-config $backtest/task/config.yml") {
				t.Fatalf("ordinary command escaped reserved task subtree: %s", args)
			}
			// Ordinary entry reserves the requested output with this same allocator.
			runDir, err := config.AllocateOutputDir(filepath.Join(base, "run"))
			if err != nil || runDir != filepath.Join(base, "run") {
				t.Fatalf("ordinary output collision: %q error=%v", runDir, err)
			}
			if err := config.WriteConfigAtomic(filepath.Join(runDir, "config.yml"), nil, runnable); err != nil {
				t.Fatal(err)
			}
			results := []runner.Result{{Engine: config.EngineFactor, StrategyID: "cross-section", ManifestID: "actual-input-proof", Fills: 3}}
			if mixed {
				results = append(results, runner.Result{Engine: config.EngineTimeSeries, StrategyID: "trend-ts", Fills: 2})
			}
			for _, state := range []struct {
				name, status string
				errors       []string
				unresolved   int
				want         int64
			}{
				{"complete", "complete", nil, 0, ormu.BtStatusDone},
				{"incomplete", "incomplete", []string{"storage cleanup failed"}, 0, ormu.BtStatusFail},
				{"unresolved", "complete", nil, 1, ormu.BtStatusFail},
				{"error-with-complete-label", "complete", []string{"primary failed"}, 0, ormu.BtStatusFail},
			} {
				t.Run(state.name, func(t *testing.T) {
					results[0].Unresolved = state.unresolved
					report := unifiedBacktestReport{Version: 1, Status: state.status, Errors: state.errors, Results: results}
					body, err := json.Marshal(report)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(runDir, "run.json"), body, 0600); err != nil {
						t.Fatal(err)
					}
					collected, err := collectBtTaskResult(server.BacktestDir(), "task")
					if err != nil || collected == nil || collected.Status != state.want || collected.Path != "task" {
						t.Fatalf("collection=%+v error=%v, expected status=%d", collected, err, state.want)
					}
					if !strings.Contains(collected.Info, "actual-input-proof") || !strings.Contains(collected.Strats, "MomentumVol") || (mixed && !strings.Contains(collected.Strats, "trend")) {
						t.Fatalf("collector lost unified strategy lineage: %+v", collected)
					}
					dirs, err := server.taskReportDirs(collected)
					if err != nil || len(dirs) != 2 || dirs[0] != runDir || dirs[1] != base {
						t.Fatalf("unified report/log ownership = %v error=%v", dirs, err)
					}
				})
			}
		})
	}
}

func TestWebUnifiedCollectorRejectsUnsupportedAndMissingArtifacts(t *testing.T) {
	root := t.TempDir()
	base, err := config.AllocateOutputDir(filepath.Join(root, "task"))
	if err != nil {
		t.Fatal(err)
	}
	if task, err := collectBtTaskResult(root, "task"); err != nil || task != nil {
		t.Fatalf("unpublished artifact marked finished: %+v error=%v", task, err)
	}
	for _, body := range []string{`{"Version":2,"Status":"complete"}`, `{"Version":1,"Status":"running"}`, `{bad-json`} {
		if err := os.WriteFile(filepath.Join(base, "run.json"), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := collectBtTaskResult(root, "task"); err == nil {
			t.Fatalf("accepted unsupported/corrupt artifact: %s", body)
		}
	}
}
