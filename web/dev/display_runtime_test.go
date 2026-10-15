package dev

import (
	"context"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/gofiber/fiber/v2"
)

func TestDevWebUsesRuntimeUIDirectoryAndRecoversPanic(t *testing.T) {
	previousDir, previousLang := config.DataDir, core.SysLang
	config.DataDir, core.SysLang = t.TempDir(), "legacy-language"
	t.Cleanup(func() { config.DataDir, core.SysLang = previousDir, previousLang })
	for _, content := range []string{"first runtime", "second runtime"} {
		directory := t.TempDir()
		t.Cleanup(func() {
			deadline := time.Now().Add(30 * time.Second)
			for {
				err := os.RemoveAll(directory)
				if err == nil {
					return
				}
				if time.Now().After(deadline) {
					t.Errorf("remove cached static files: %v", err)
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		})
		uiDirectory := filepath.Join(directory, "uidist")
		if err := os.MkdirAll(uiDirectory, 0755); err != nil {
			t.Fatal(err)
		}
		for name, text := range map[string]string{"index.html": content, "version.txt": core.UIVersion} {
			if err := os.WriteFile(filepath.Join(uiDirectory, name), []byte(text), 0644); err != nil {
				t.Fatal(err)
			}
		}
		state, stateErr := core.NewState(context.Background())
		if stateErr != nil {
			t.Fatal(stateErr)
		}
		state.SysLang = "en-US"
		t.Cleanup(state.Close)
		server := newDevServer(DevDeps{Data: &data.RuntimeDeps{
			Config: config.NewSnapshotWithDirs(nil, directory, ""),
			Core:   state,
		}})
		t.Cleanup(server.Stop)
		app := newWebApp()
		app.Get("/regression/panic", func(*fiber.Ctx) error { panic("regression") })
		if err := server.serveStatic(app); err != nil {
			t.Fatal(err)
		}
		for _, request := range []struct {
			path   string
			status int
		}{{"/regression/panic", 500}, {"/index.html", 200}} {
			response, err := app.Test(httptest.NewRequest("GET", request.path, nil))
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr != nil || response.StatusCode != request.status {
				t.Fatalf("%s status=%d body=%q error=%v", request.path, response.StatusCode, body, readErr)
			}
			if request.path == "/index.html" && string(body) != content {
				t.Fatalf("UI used another runtime's directory: %q", body)
			}
		}
		if _, err := os.Stat(filepath.Join(config.DataDir, "uidist")); !os.IsNotExist(err) {
			t.Fatal("runtime UI touched legacy data directory")
		}
	}
}
