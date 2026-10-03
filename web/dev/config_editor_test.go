package dev

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/gofiber/fiber/v2"
	"gopkg.in/yaml.v3"
)

func editorTestApp(t *testing.T, raw []byte) (*fiber.App, *DevServer, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	server := newTestDevServer(dir)
	t.Cleanup(func() { server.Stop(); server.Join() })
	app := fiber.New()
	app.Get("/text", server.getText)
	app.Post("/save_text", server.saveText)
	return app, server, path
}

func editorRequest(app *fiber.App, method, path string, body any) (int, map[string]any, error) {
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	response, err := app.Test(req, -1)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	var result map[string]any
	err = json.NewDecoder(response.Body).Decode(&result)
	return response.StatusCode, result, err
}

func TestWebEditorVersionsAndAtomicSave(t *testing.T) {
	raw := []byte("# 中文\r\nname: original\r\n")
	app, _, path := editorTestApp(t, raw)
	status, read, err := editorRequest(app, http.MethodGet, "/text?path="+url.QueryEscape("$/config.yml"), nil)
	if err != nil || status != http.StatusOK || read["data"] != string(raw) || read["digest"] != textDigest(raw) {
		t.Fatalf("read status=%d result=%v error=%v", status, read, err)
	}
	request := map[string]any{"path": "$/config.yml", "content": "name: edited\n"}
	status, _, err = editorRequest(app, http.MethodPost, "/save_text", request)
	if err != nil || status != http.StatusPreconditionRequired {
		t.Fatalf("unversioned save status=%d error=%v", status, err)
	}
	request["digest"] = read["digest"]
	status, saved, err := editorRequest(app, http.MethodPost, "/save_text", request)
	if err != nil || status != http.StatusOK || saved["digest"] != textDigest([]byte(request["content"].(string))) {
		t.Fatalf("save status=%d result=%v error=%v", status, saved, err)
	}
	request["content"] = "name: stale\n"
	status, _, err = editorRequest(app, http.MethodPost, "/save_text", request)
	if err != nil || status != http.StatusConflict {
		t.Fatalf("stale save status=%d error=%v", status, err)
	}
	actual, err := os.ReadFile(path)
	if err != nil || string(actual) != "name: edited\n" {
		t.Fatalf("stale editor overwrote current file: %q error=%v", actual, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0200 == 0 {
		t.Fatalf("atomic save lost owner write permission: %v", info.Mode())
	}
}

func TestWebEditorConcurrentVersionedSavesKeepOneWinner(t *testing.T) {
	raw := []byte("name: original\n")
	app, _, path := editorTestApp(t, raw)
	type outcome struct {
		status int
		err    error
	}
	outcomes := make(chan outcome, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, name := range []string{"first", "second"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			<-start
			status, _, err := editorRequest(app, http.MethodPost, "/save_text", map[string]any{
				"path": "$/config.yml", "content": "name: " + name + "\n", "digest": textDigest(raw),
			})
			outcomes <- outcome{status, err}
		}(name)
	}
	close(start)
	wg.Wait()
	close(outcomes)
	counts := map[int]int{}
	for outcome := range outcomes {
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		counts[outcome.status]++
	}
	if counts[http.StatusOK] != 1 || counts[http.StatusConflict] != 1 {
		t.Fatalf("concurrent versions = %v, want one accepted and one conflict", counts)
	}
	actual, err := os.ReadFile(path)
	if err != nil || (string(actual) != "name: first\n" && string(actual) != "name: second\n") {
		t.Fatalf("partial save = %q error=%v", actual, err)
	}
}

func TestWebEditorRejectsVersionReadBeforeMigration(t *testing.T) {
	raw := []byte(separateTestConfig)
	app, server, path := editorTestApp(t, raw)
	_, read, err := editorRequest(app, http.MethodGet, "/text?path="+url.QueryEscape("$/config.yml"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := config.LoadRunSpec(&config.CmdArgs{Configs: []string{path}, NoDefault: true, DataDir: server.DataDir()}, false); err != nil {
		t.Fatal(err)
	}
	migrated, err := os.ReadFile(path)
	if err != nil || bytes.Equal(migrated, raw) {
		t.Fatalf("migration did not change the source: %v", err)
	}
	status, _, err := editorRequest(app, http.MethodPost, "/save_text", map[string]any{
		"path": "$/config.yml", "content": separateTestConfig + "# stale\n", "digest": read["digest"],
	})
	if err != nil || status != http.StatusConflict {
		t.Fatalf("post-migration stale save status=%d error=%v", status, err)
	}
	actual, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(actual, migrated) {
		t.Fatalf("migrated source overwritten: %v", err)
	}
}

const webFactorConfig = `config_version: 2
run_policy:
  - name: MomentumVol
    id: cross-section
    capital_weight: 0.5
    engine: factor
    factor:
      archive: fixtures/history.jsonl
execution:
  mode: events
data:
  namespace: research-input
`

func TestWebBacktestUsesUnifiedRunSpecWithoutTSProjection(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprintf("mixed=%v", mixed), func(t *testing.T) {
			server := newTestDevServer(t.TempDir())
			t.Cleanup(func() { server.Stop(); server.Join() })
			raw := webFactorConfig
			if mixed {
				raw = strings.Replace(raw, "execution:\n", "  - name: trend\n    engine: time_series\n    id: trend-ts\n    capital_weight: 0.5\nexecution:\n", 1)
				raw += "exchange:\n  name: binance\nmarket_type: linear\ntime_start: '2024-01-01'\ntime_end: '2024-01-02'\nwallet_amounts: {USDT: 10000}\nstake_currency: [USDT]\nstake_amount: 10\ndatabase:\n  url: postgres://local/test\nrun_timeframes: [1m]\n"
			}
			called := false
			server.backtestPreflight = func(spec *config.RunSpec) error {
				called = true
				if spec.Config().Data["namespace"] != "research-input" || spec.Config().Execution["mode"] != "events" {
					return fmt.Errorf("advanced fields were lost")
				}
				return nil
			}
			dir, paths, err := server.prepareBacktestConfigFiles(map[string]string{"@config.yml": raw}, []string{"@config.yml"})
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			spec, metadata, output, err := server.loadBacktestRunSpec(paths)
			if err != nil {
				t.Fatal(err)
			}
			if !called || len(metadata.RunPolicy) != len(spec.Config().RunPolicy) || !strings.Contains(string(output), "engine: factor") || !strings.Contains(string(output), "namespace: research-input") {
				t.Fatalf("unified Web config was projected to TS: metadata=%v output=%s", metadata.RunPolicy, output)
			}
			archive, err := spec.ResolvePath("run_policy[0].factor.archive")
			if err != nil || archive != filepath.Join(server.DataDir(), "fixtures", "history.jsonl") {
				t.Fatalf("archive lost original source base: %q error=%v", archive, err)
			}
			if err := os.WriteFile(filepath.Join(server.DataDir(), "replay.yml"), output, 0600); err != nil {
				t.Fatal(err)
			}
			os.RemoveAll(dir)
			reloaded, specErr := config.LoadRunSpec(&config.CmdArgs{Configs: []string{filepath.Join(server.DataDir(), "replay.yml")}, NoDefault: true, DataDir: server.DataDir()}, false)
			if specErr != nil {
				t.Fatal(specErr)
			}
			resolved, err := reloaded.ResolvePath("run_policy[0].factor.archive")
			if err != nil || archive != resolved {
				t.Fatalf("queued config depends on deleted request temp directory: %q error=%v", resolved, err)
			}
		})
	}
}

func TestWebBacktestRetainsTSProfileAndPreflightFailures(t *testing.T) {
	server := newTestDevServer(t.TempDir())
	t.Cleanup(func() { server.Stop(); server.Join() })
	for _, test := range []struct{ name, raw, want string }{
		{"legacy-ts-profile", separateTestConfig, "wallet_amounts is required"},
		{"factor-without-preflight", webFactorConfig, "unified backtest preflight is not configured"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, paths, err := server.prepareBacktestConfigFiles(map[string]string{"@config.yml": test.raw}, []string{"@config.yml"})
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			_, _, _, err = server.loadBacktestRunSpec(paths)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error=%v, want %s", err, test.want)
			}
		})
	}
	server.backtestPreflight = func(*config.RunSpec) error { return fmt.Errorf("mode/capability rejected before services") }
	dir, paths, err := server.prepareBacktestConfigFiles(map[string]string{"@config.yml": webFactorConfig}, []string{"@config.yml"})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	_, _, _, err = server.loadBacktestRunSpec(paths)
	if err == nil || !strings.Contains(err.Error(), "rejected before services") {
		t.Fatalf("preflight rejection lost: %v", err)
	}
}

func TestWebBacktestPrivateCopyPreservesAllAdvancedPathBases(t *testing.T) {
	server := newTestDevServer(t.TempDir())
	t.Cleanup(func() { server.Stop(); server.Join() })
	t.Setenv("WEB_TEST_ARCHIVE", "../env.jsonl")
	raw := []byte(`data:
  archive: "${WEB_TEST_ARCHIVE}"
execution:
  store: ":memory:"
  sender_lease_dir: locks
  accounts:
    shared:
      store: ../account.db
      sender_lease_dir: ../account-locks
run_policy:
  - name: MomentumVol
    archive: user-strategy-value
    factor:
      archive: ../history.jsonl
      chunks:
        - path: ../chunk.jsonl
      config:
        Chunks:
          - Path: ../imported.jsonl
        Execution:
          StorePath: state.db
          SenderLeaseDir: sender-locks
`)
	copy, err := server.backtestConfigContents("$/layers/overlay.yml", raw)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := yaml.Unmarshal(copy, &fields); err != nil {
		t.Fatal(err)
	}
	data := fields["data"].(map[string]any)
	execution := fields["execution"].(map[string]any)
	account := execution["accounts"].(map[string]any)["shared"].(map[string]any)
	policy := fields["run_policy"].([]any)[0].(map[string]any)
	factorFields := policy["factor"].(map[string]any)
	imported := factorFields["config"].(map[string]any)
	checks := []struct {
		value    any
		relative string
	}{
		{data["archive"], "env.jsonl"},
		{execution["sender_lease_dir"], "layers/locks"},
		{account["store"], "account.db"},
		{account["sender_lease_dir"], "account-locks"},
		{factorFields["archive"], "history.jsonl"},
		{factorFields["chunks"].([]any)[0].(map[string]any)["path"], "chunk.jsonl"},
		{imported["Chunks"].([]any)[0].(map[string]any)["Path"], "imported.jsonl"},
		{imported["Execution"].(map[string]any)["StorePath"], "layers/state.db"},
		{imported["Execution"].(map[string]any)["SenderLeaseDir"], "layers/sender-locks"},
	}
	for _, check := range checks {
		if check.value != filepath.Join(server.DataDir(), filepath.FromSlash(check.relative)) {
			t.Fatalf("path %v lost original source base, want %s", check.value, check.relative)
		}
	}
	if execution["store"] != ":memory:" || policy["archive"] != "user-strategy-value" || !bytes.Contains(raw, []byte("${WEB_TEST_ARCHIVE}")) {
		t.Fatal("private normalization changed special path, custom strategy field, or original input")
	}
}
