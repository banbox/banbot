package dev

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm/ormu"
)

const separateTestConfig = `name: separate-test
exchange:
  name: binance
market_type: linear
time_start: "2024-01-01"
time_end: "2024-01-02"
stake_currency: [USDT]
stake_amount: 10
pairs: [BTC/USDT:USDT]
run_timeframes: [1m]
run_policy:
  - name: first
  - name: second
`

func writeSeparateReport(t *testing.T, dir string, orderNum int) {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.yml"), []byte(separateTestConfig), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "assets.html"), []byte("<html></html>"), 0644); err != nil {
		t.Fatal(err)
	}
	detail, err := json.Marshal(map[string]any{"orderNum": orderNum})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "detail.json"), detail, 0644); err != nil {
		t.Fatal(err)
	}
}

func TestCollectBtTaskResultAggregatesSeparateReports(t *testing.T) {
	root := t.TempDir()
	taskDir := filepath.Join(root, "abc")
	if err := os.MkdirAll(taskDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(taskDir, "config.yml"), []byte(separateTestConfig), 0644); err != nil {
		t.Fatal(err)
	}
	writeSeparateReport(t, filepath.Join(taskDir, "policy_1"), 2)
	writeSeparateReport(t, filepath.Join(taskDir, "policy_2"), 3)

	task, err := collectBtTaskResult(root, "abc")
	if err != nil {
		t.Fatal(err)
	}
	if task == nil || task.Status != ormu.BtStatusDone || task.OrderNum != 5 || task.Path != "abc" {
		t.Fatalf("aggregate = %#v, want done task with five orders", task)
	}
	var info map[string]any
	if err := json.Unmarshal([]byte(task.Info), &info); err != nil {
		t.Fatal(err)
	}
	if info["separate"] != true || info["policyCount"] != float64(2) {
		t.Fatalf("aggregate info = %#v", info)
	}
}

func TestCollectBtTaskResultWaitsForMissingSeparateReport(t *testing.T) {
	root := t.TempDir()
	taskDir := filepath.Join(root, "abc")
	if err := os.MkdirAll(taskDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(taskDir, "config.yml"), []byte(separateTestConfig), 0644); err != nil {
		t.Fatal(err)
	}
	writeSeparateReport(t, filepath.Join(taskDir, "policy_1"), 2)

	task, err := collectBtTaskResult(root, "abc")
	if err != nil {
		t.Fatal(err)
	}
	if task != nil {
		t.Fatalf("partial aggregate = %#v, want nil", task)
	}
}

func TestPrepareBacktestConfigFilesUsesRequestPrivateCopies(t *testing.T) {
	firstDir, firstPaths, err := prepareBacktestConfigFiles(
		map[string]string{"@config.yml": "name: first\n"}, []string{"@config.yml"})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(firstDir)
	secondDir, secondPaths, err := prepareBacktestConfigFiles(
		map[string]string{"@config.yml": "name: second\n"}, []string{"@config.yml"})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(secondDir)
	if firstDir == secondDir || len(firstPaths) != 1 || len(secondPaths) != 1 || firstPaths[0] == secondPaths[0] {
		t.Fatalf("temporary config paths are not isolated: %q/%q and %q/%q", firstDir, firstPaths, secondDir, secondPaths)
	}
	first, err := os.ReadFile(firstPaths[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(secondPaths[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != "name: first\n" || string(second) != "name: second\n" {
		t.Fatalf("private config contents = %q, %q", first, second)
	}
}

func TestBacktestConfigTempPathRejectsTraversal(t *testing.T) {
	for _, path := range []string{"@../config.yml", "../config.yml", "@"} {
		if _, err := backtestConfigTempPath(path); err == nil {
			t.Fatalf("backtestConfigTempPath(%q) accepted an unsafe path", path)
		}
	}
}

func TestPrepareBacktestConfigFilesRejectsBasenameCollision(t *testing.T) {
	firstDir := t.TempDir()
	secondDir := t.TempDir()
	firstPath := filepath.Join(firstDir, "config.yml")
	secondPath := filepath.Join(secondDir, "config.yml")
	if err := os.WriteFile(firstPath, []byte("name: first\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondPath, []byte("name: second\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if tempDir, paths, err := prepareBacktestConfigFiles(nil, []string{firstPath, secondPath}); err == nil {
		_ = os.RemoveAll(tempDir)
		t.Fatalf("basename collision was accepted with paths %q", paths)
	}
}

func TestTaskReportDirsResolveSeparateChildren(t *testing.T) {
	oldDataDir := config.DataDir
	config.DataDir = t.TempDir()
	t.Cleanup(func() { config.DataDir = oldDataDir })
	task := &ormu.Task{
		Path: "abc",
		Info: `{"separate":true,"reportPaths":["abc/policy_1","abc/policy_2"]}`,
	}
	dirs, err := taskReportDirs(task)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(config.DataDir, "backtest", "abc")
	want := []string{filepath.Join(root, "policy_1"), filepath.Join(root, "policy_2")}
	if len(dirs) != len(want) || dirs[0] != want[0] || dirs[1] != want[1] {
		t.Fatalf("separate report dirs = %q, want %q", dirs, want)
	}
}

func TestTaskReportDirsRejectTraversal(t *testing.T) {
	oldDataDir := config.DataDir
	config.DataDir = t.TempDir()
	t.Cleanup(func() { config.DataDir = oldDataDir })
	task := &ormu.Task{
		Path: "abc",
		Info: `{"separate":true,"reportPaths":["abc/../other"]}`,
	}
	if _, err := taskReportDirs(task); err == nil {
		t.Fatal("taskReportDirs accepted a report outside the task directory")
	}
}

func TestReportFilePathRejectsTraversal(t *testing.T) {
	dir := t.TempDir()
	if _, err := reportFilePath(dir, "../outside.txt"); err == nil {
		t.Fatal("reportFilePath accepted a path outside the report directory")
	}
}

func TestResolveReportRootRejectsTraversal(t *testing.T) {
	root := t.TempDir()
	for _, path := range []string{"../outside", "/tmp/outside", ""} {
		if _, err := resolveReportRoot(root, path); err == nil {
			t.Fatalf("resolveReportRoot accepted unsafe path %q", path)
		}
	}
}

func TestAppendTaskAssetsUsesValidatedSeparateReport(t *testing.T) {
	oldDataDir := config.DataDir
	config.DataDir = t.TempDir()
	t.Cleanup(func() { config.DataDir = oldDataDir })
	taskDir := filepath.Join(config.DataDir, "backtest", "abc")
	if err := os.MkdirAll(filepath.Join(taskDir, "policy_1"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(taskDir, "config.yml"), []byte(separateTestConfig), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(taskDir, "policy_1", "assets.html"), []byte("chartData = {\"labels\":[],\"datasets\":[{\"label\":\"Real\",\"data\":[1,2]}]}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	task := &ormu.Task{Path: "abc", Status: ormu.BtStatusDone, Info: `{"separate":true,"reportPaths":["abc/policy_1"]}`}
	result := make(map[string]interface{})
	appendTaskAssets(task, result)
	if len(result["reals"].([]float64)) != 2 {
		t.Fatalf("assets were not loaded from separate report: %#v", result)
	}
}
