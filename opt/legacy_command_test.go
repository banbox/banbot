package opt

import (
	"testing"
	"time"
)

func TestCommandLegacySessionAllowsNestedPublicSession(t *testing.T) {
	done := make(chan struct{})
	go func() {
		WithCommandLegacySession(func(session LegacySession) struct{} {
			WithLegacySessionIn(session, func(session LegacySession) struct{} {
				session.require()
				return struct{}{}
			})
			return struct{}{}
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("command legacy session deadlocked on nested public session")
	}
}
