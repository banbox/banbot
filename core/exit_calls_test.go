package core

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestRunExitCallsDrainsConcurrentRegistrationsOnce(t *testing.T) {
	exitCallsLock.Lock()
	previous := ExitCalls
	ExitCalls = nil
	exitCallsLock.Unlock()
	t.Cleanup(func() {
		exitCallsLock.Lock()
		ExitCalls = previous
		exitCallsLock.Unlock()
	})

	const count = 64
	var registered sync.WaitGroup
	registered.Add(count)
	for range count {
		go func() {
			AddExitCall(func() {})
			registered.Done()
		}()
	}
	registered.Wait()

	var runs atomic.Int32
	exitCallsLock.Lock()
	ExitCalls = append(ExitCalls, func() { runs.Add(1) })
	exitCallsLock.Unlock()
	var drains sync.WaitGroup
	drains.Add(2)
	go func() { defer drains.Done(); RunExitCalls() }()
	go func() { defer drains.Done(); RunExitCalls() }()
	drains.Wait()

	if got := runs.Load(); got != 1 {
		t.Fatalf("cleanup callbacks ran %d times, want once", got)
	}
	exitCallsLock.Lock()
	left := len(ExitCalls)
	exitCallsLock.Unlock()
	if left != 0 {
		t.Fatalf("exit callback list retains %d callbacks after drain", left)
	}
}
