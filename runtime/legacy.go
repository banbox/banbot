package runtime

import "github.com/banbox/banbot/legacygate"

// LegacyGateAnnotation keeps the legacy command marker available to old
// callers. The actual marker and gate live in the dependency-free legacygate
// package so domain packages do not depend on this composition root.
const LegacyGateAnnotation = legacygate.Annotation

// WithLegacy runs a compatibility operation under the one process-wide legacy
// state gate. The generic return keeps callers typed without exposing the
// mutex or adding any work to Runtime hot paths.
func WithLegacy[T any](run func() T) T {
	return legacygate.With(run)
}

// LockLegacy acquires the process-wide legacy state gate and returns its
// release function. It is useful for operations whose result has multiple
// return values and therefore cannot be represented by WithLegacy's single
// typed result.
func LockLegacy() func() {
	return legacygate.Lock()
}
