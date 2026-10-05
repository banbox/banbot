package dev

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm/ormu"
	"github.com/gofiber/fiber/v2"
)

func TestStaticPreflightPreservesFilesOriginsAndDoesNotQueue(t *testing.T) {
	server := newTestDevServer(t.TempDir())
	t.Cleanup(func() { server.Stop(); server.Join() })
	raw := webFactorConfig
	path := filepath.Join(server.DataDir(), "config.yml")
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	server.inspectBacktest = func(spec *config.RunSpec) (*BacktestInspection, error) {
		archive, err := spec.ResolvePath("run_policy[0].archive")
		if err != nil || archive != filepath.Join(server.DataDir(), "fixtures", "history.jsonl") {
			return nil, fmt.Errorf("wrong archive base: %s %v", archive, err)
		}
		return &BacktestInspection{Version: 1, Engines: spec.Engines(), ExecutionMode: "events"}, nil
	}
	app := fiber.New()
	app.Post("/preflight", server.handleBacktestPreflight)
	status, result, err := editorRequest(app, http.MethodPost, "/preflight", map[string]any{"configs": map[string]string{"@config.yml": raw}, "paths": []string{"@config.yml"}})
	if err != nil || status != http.StatusOK {
		t.Fatalf("status=%d result=%v error=%v", status, result, err)
	}
	data := result["data"].(map[string]any)
	if data["data_checked"] != false || data["execution_mode"] != "events" {
		t.Fatalf("wrong static contract: %v", data)
	}
	origin := data["origins"].(map[string]any)["run_policy[0].engine"].(map[string]any)
	if origin["Source"] != "@config.yml" {
		t.Fatalf("temporary source leaked: %v", origin)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != raw {
		t.Fatal("static preflight modified source", err)
	}
	if len(server.notify) != 0 {
		t.Fatal("static preflight queued a task")
	}
	if _, err := os.Stat(server.BacktestDir()); !os.IsNotExist(err) {
		t.Fatalf("static preflight created outputs: %v", err)
	}
}

func TestStaticPreflightErrorsAreStructured(t *testing.T) {
	server := newTestDevServer(t.TempDir())
	t.Cleanup(func() { server.Stop(); server.Join() })
	server.inspectBacktest = func(*config.RunSpec) (*BacktestInspection, error) {
		return nil, fmt.Errorf("mixed backtest requires execution.mode: events")
	}
	app := fiber.New()
	app.Post("/preflight", server.handleBacktestPreflight)
	status, result, err := editorRequest(app, http.MethodPost, "/preflight", map[string]any{"configs": map[string]string{"@config.yml": webFactorConfig}})
	if err != nil || status != http.StatusBadRequest || !strings.Contains(fmt.Sprint(result["errors"]), "mixed backtest") {
		t.Fatalf("status=%d result=%v error=%v", status, result, err)
	}
	server.inspectBacktest = nil
	status, _, err = editorRequest(app, http.MethodPost, "/preflight", map[string]any{"configs": map[string]string{"@config.yml": webFactorConfig}})
	if err != nil || status != http.StatusServiceUnavailable {
		t.Fatalf("unavailable inspector status=%d error=%v", status, err)
	}
}

func TestStrategyCatalogAndPendingTaskMetadata(t *testing.T) {
	app := fiber.New()
	app.Get("/catalog", getStrategyCatalog)
	status, result, err := editorRequest(app, http.MethodGet, "/catalog", nil)
	if err != nil || status != http.StatusOK {
		t.Fatalf("status=%d result=%v error=%v", status, result, err)
	}
	data := result["data"].(map[string]any)
	if data["research_tasks"] != false || data["mixed_mode"] != "events" || !strings.Contains(fmt.Sprint(data["definitions"]), "momentum-vol") {
		t.Fatalf("false catalog capabilities: %v", data)
	}
	for _, test := range []struct {
		raw, mode string
		engines   []string
	}{
		{webFactorConfig, "events", []string{"factor"}},
		{strings.Replace(webFactorConfig, "mode: events", "mode: weights", 1), "weights", []string{"factor"}},
		{"run_policy: [{name: legacy, id: old-custom-id}]", "", []string{"time_series"}},
	} {
		task := &ormu.Task{Config: test.raw, Status: ormu.BtStatusInit}
		values := task.ToMap()
		appendBacktestMetadata(task, values)
		if !reflect.DeepEqual(values["engines"], test.engines) {
			t.Fatalf("bad task engine metadata: %v", values)
		}
		if mode, _ := values["executionMode"].(string); mode != test.mode {
			t.Fatalf("bad task mode metadata: %v", values)
		}
	}
}

func TestFailedFactorTaskRetainsStartupErrorWithoutReport(t *testing.T) {
	task := &ormu.Task{Config: webFactorConfig, Status: ormu.BtStatusFail, Info: "factor: archive file is missing"}
	values := task.ToMap()
	appendBacktestMetadata(task, values)
	if values["info"] != task.Info || !reflect.DeepEqual(values["engines"], []string{"factor"}) {
		t.Fatalf("startup error hidden: %v", values)
	}
}
