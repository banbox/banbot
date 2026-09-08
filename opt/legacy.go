package opt

import (
	"github.com/banbox/banbot/legacygate"
)

// LegacySession proves that a caller is inside the process-wide compatibility
// boundary. It is only issued for the duration of WithLegacySession.
type LegacySession struct {
	state legacygate.Session
}

// WithLegacySession runs run while the legacy package facades are exclusively
// owned by the caller and passes a token to typed opt constructors that still
// need those facades during setup and execution.
func WithLegacySession[T any](run func(LegacySession) T) T {
	if run == nil {
		panic("opt: nil legacy session callback")
	}
	return legacygate.WithSession(func(state legacygate.Session) T {
		return run(LegacySession{state: state})
	})
}

// WithLegacySessionIn reuses an explicitly owned legacy session. It is the
// safe way for a command callback that already received a session to invoke a
// nested compatibility helper without consulting process-global state.
func WithLegacySessionIn[T any](session LegacySession, run func(LegacySession) T) T {
	if run == nil {
		panic("opt: nil legacy session callback")
	}
	session.require()
	return run(session)
}

// WithCommandLegacySession owns a normal process-wide legacy session for a
// command callback. Nested code must use WithLegacySessionIn with the session
// it received; no implicit current-session pointer is published, because that
// would let another goroutine enter the same legacy state concurrently.
func WithCommandLegacySession[T any](run func(LegacySession) T) T {
	if run == nil {
		panic("opt: nil command legacy session callback")
	}
	return legacygate.WithSession(func(state legacygate.Session) T { return run(LegacySession{state: state}) })
}

func (s LegacySession) require() {
	s.state.Require()
}
