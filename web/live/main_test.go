package live

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/gofiber/fiber/v2"
)

func TestAPIServerStopAndJoinAreIdempotent(t *testing.T) {
	app := fiber.New()
	listenRelease := make(chan struct{})
	shutdownStarted := make(chan struct{})
	shutdownRelease := make(chan struct{})
	var shutdownCalls atomic.Int32
	app.Hooks().OnShutdown(func() error {
		if shutdownCalls.Add(1) == 1 {
			close(shutdownStarted)
		}
		<-shutdownRelease
		return nil
	})

	server := newAPIServer(app, func() error {
		<-listenRelease
		return nil
	})

	stopReturned := make(chan struct{})
	go func() {
		server.Stop()
		close(stopReturned)
	}()
	select {
	case <-stopReturned:
	case <-time.After(time.Second):
		t.Fatal("Stop blocked")
	}
	server.Stop()

	select {
	case <-shutdownStarted:
	case <-time.After(time.Second):
		t.Fatal("Shutdown was not started")
	}
	if got := shutdownCalls.Load(); got != 1 {
		t.Fatalf("shutdown calls = %d, want 1", got)
	}

	joined := make(chan struct{})
	go func() {
		server.Join()
		close(joined)
	}()
	select {
	case <-joined:
		t.Fatal("Join returned before Listen finished")
	default:
	}

	close(listenRelease)
	select {
	case <-joined:
		t.Fatal("Join returned before Shutdown finished")
	case <-time.After(50 * time.Millisecond):
	}
	close(shutdownRelease)
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("Join blocked after Listen finished")
	}
	server.Join()
}

func TestStartAPIWithLifecycleFinishesAfterListenFailure(t *testing.T) {
	oldConfig, oldDataDir := config.APIServer, config.DataDir
	config.DataDir = t.TempDir()
	config.APIServer = &config.APIServerConfig{
		Enable: true, BindIPAddr: "127.0.0.1", Port: -1,
	}
	t.Cleanup(func() {
		config.APIServer, config.DataDir = oldConfig, oldDataDir
	})

	uiDir := filepath.Join(config.DataDir, "uidist")
	if err := os.MkdirAll(uiDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(uiDir, "index.html"), []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(uiDir, "version.txt"), []byte(core.UIVersion), 0o644); err != nil {
		t.Fatal(err)
	}

	server, err := startApiWithLifecycle(nil)
	if err != nil || server == nil {
		t.Fatalf("startApiWithLifecycle = %v, %v", server, err)
	}
	server.Join()
}

func TestStartApiWithLifecycleAllowsDisabledAPI(t *testing.T) {
	oldConfig := config.APIServer
	config.APIServer = nil
	t.Cleanup(func() { config.APIServer = oldConfig })

	server, err := startApiWithLifecycle(nil)
	if err != nil || server != nil {
		t.Fatalf("disabled API start = %v, %v; want nil, nil", server, err)
	}
}
