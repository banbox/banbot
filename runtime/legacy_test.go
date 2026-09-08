package runtime

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/legacygate"
)

func TestWithLegacySerializesConcurrentRunners(t *testing.T) {
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		WithLegacy(func() struct{} {
			close(firstEntered)
			<-releaseFirst
			return struct{}{}
		})
		close(firstDone)
	}()
	<-firstEntered

	var secondEntered atomic.Bool
	secondDone := make(chan struct{})
	go func() {
		WithLegacy(func() struct{} {
			secondEntered.Store(true)
			return struct{}{}
		})
		close(secondDone)
	}()
	t.Cleanup(func() {
		select {
		case <-releaseFirst:
		default:
			close(releaseFirst)
		}
	})

	select {
	case <-secondDone:
		t.Fatal("concurrent legacy runner entered before the active runner exited")
	case <-time.After(50 * time.Millisecond):
	}
	if secondEntered.Load() {
		t.Fatal("concurrent legacy runners overlapped")
	}

	close(releaseFirst)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first legacy runner did not exit")
	}
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("legacy gate was not released on runner exit")
	}
}

func TestWithLegacyReleasesGateAfterPanic(t *testing.T) {
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("WithLegacy callback did not panic")
			}
		}()
		WithLegacy(func() struct{} { panic("test panic") })
	}()

	done := make(chan struct{})
	go func() {
		WithLegacy(func() struct{} { return struct{}{} })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("legacy gate remained locked after callback panic")
	}
}

func TestExplicitRuntimeConstructionDoesNotWaitForLegacyGate(t *testing.T) {
	unlock := LockLegacy()
	defer unlock()

	done := make(chan error, 1)
	go func() {
		rt, err := NewProcess().NewRuntime(Options{})
		if rt != nil {
			rt.Close()
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("explicit runtime construction failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("explicit runtime construction unexpectedly waited for legacy gate")
	}
}

func TestLegacyCompatibilityWrapperSharesNarrowGate(t *testing.T) {
	unlock := LockLegacy()
	done := make(chan struct{})
	go func() {
		legacygate.With(func() struct{} { return struct{}{} })
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("legacygate callback bypassed runtime compatibility gate")
	case <-time.After(20 * time.Millisecond):
	}

	unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("legacygate callback did not run after runtime gate release")
	}
}
