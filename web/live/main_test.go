package live

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/legacygate"
	"github.com/banbox/banexg/errs"
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

func TestAPIServerReleasesLegacyGateOnStop(t *testing.T) {
	unlocked := legacygate.Lock()
	listenRelease := make(chan struct{})
	server := newAPIServer(fiber.New(), func() error {
		<-listenRelease
		return nil
	}, unlocked)

	server.Stop()
	entered := make(chan struct{})
	done := make(chan struct{})
	go func() {
		legacygate.With(func() struct{} {
			close(entered)
			return struct{}{}
		})
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Stop did not release the legacy gate")
	}

	close(listenRelease)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("legacy gate callback did not finish")
	}
	server.Join()
}

func TestAPIServerReleasesLegacyGateOnJoin(t *testing.T) {
	unlocked := legacygate.Lock()
	server := newAPIServer(fiber.New(), func() error { return nil }, unlocked)
	server.Join()

	done := make(chan struct{})
	go func() {
		legacygate.With(func() struct{} { return struct{}{} })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Join did not release the legacy gate")
	}
}

func TestStartApiWithLifecycleInLegacySessionDoesNotReacquireGate(t *testing.T) {
	oldConfig := config.APIServer
	config.APIServer = nil
	t.Cleanup(func() { config.APIServer = oldConfig })

	unlock := legacygate.Lock()
	done := make(chan *errs.Error, 1)
	go func() {
		_, err := StartApiWithLifecycleInLegacySession(nil)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("legacy-session API start failed: %v", err)
		}
	case <-time.After(time.Second):
		unlock()
		t.Fatal("legacy-session API start reacquired the legacy gate")
	}
	unlock()
}
