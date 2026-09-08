// Package legacygate owns the process-wide compatibility gate without
// importing any domain or runtime-composition packages.
package legacygate

import (
	"sync"
	"sync/atomic"
)

const Annotation = "banbot/legacy-gate"

var stateMu sync.Mutex

type Session struct {
	state *sessionState
}

type sessionState struct {
	active atomic.Bool
}

func With[T any](run func() T) T {
	stateMu.Lock()
	defer stateMu.Unlock()
	return run()
}

func WithSession[T any](run func(Session) T) T {
	if run == nil {
		panic("legacygate: nil session callback")
	}
	return With(func() T {
		state := &sessionState{}
		state.active.Store(true)
		defer state.active.Store(false)
		return run(Session{state: state})
	})
}

func (s Session) Require() {
	if s.state == nil || !s.state.active.Load() {
		panic("legacygate: an active session is required")
	}
}

func Lock() func() {
	stateMu.Lock()
	return stateMu.Unlock
}
