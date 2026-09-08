package legacygate

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestWithSessionTokenExpiresAfterCallback(t *testing.T) {
	var session Session
	WithSession(func(current Session) struct{} {
		session = current
		current.Require()
		return struct{}{}
	})
	defer func() {
		if recover() == nil {
			t.Fatal("expired session was accepted")
		}
	}()
	session.Require()
}

func TestWithAndLockShareGate(t *testing.T) {
	unlock := Lock()
	done := make(chan struct{})
	var entered atomic.Bool
	go func() {
		With(func() struct{} {
			entered.Store(true)
			return struct{}{}
		})
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("With entered while Lock held")
	case <-time.After(20 * time.Millisecond):
	}
	if entered.Load() {
		t.Fatal("gate-protected callback overlapped the lock holder")
	}

	unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("With did not enter after Lock release")
	}
}
