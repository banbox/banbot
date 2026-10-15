package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/cron/v3"
)

type lifecycleScheduler struct {
	stopCalls atomic.Int32
	stopCtx   context.Context
	cancel    context.CancelFunc
	onStop    func()
	returnNil bool
}

func newLifecycleScheduler(returnNil bool) *lifecycleScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &lifecycleScheduler{stopCtx: ctx, cancel: cancel, returnNil: returnNil}
}

func (s *lifecycleScheduler) AddFunc(string, func()) (cron.EntryID, error) {
	return 0, nil
}

func (s *lifecycleScheduler) Start() {}

func (s *lifecycleScheduler) Stop() context.Context {
	s.stopCalls.Add(1)
	if s.onStop != nil {
		s.onStop()
	}
	if s.returnNil {
		return nil
	}
	return s.stopCtx
}

func waitForRuntimePhase(t *testing.T, rt *Runtime, want closePhase) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		rt.closeMu.Lock()
		got := rt.closePhase
		rt.closeMu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("runtime phase did not become %v", want)
}

func TestRuntimeCloseStopsSchedulerOnceWhenStopReturnsNil(t *testing.T) {
	scheduler := newLifecycleScheduler(true)
	rt, err := NewProcess().NewRuntime(Options{Scheduler: scheduler})
	if err != nil {
		t.Fatal(err)
	}

	rt.Close()
	rt.Stop()
	if got := scheduler.stopCalls.Load(); got != 1 {
		t.Fatalf("scheduler Stop calls = %d, want 1", got)
	}
}

func TestRuntimeRejectsSharedOwnedScheduler(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	scheduler := newLifecycleScheduler(true)
	first, err := process.NewRuntime(Options{Scheduler: scheduler})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := process.NewRuntime(Options{Scheduler: scheduler}); err == nil {
		t.Fatal("Process allowed two owner Runtimes to share one scheduler")
	}
	if got := scheduler.stopCalls.Load(); got != 0 {
		t.Fatalf("scheduler stopped while duplicate construction was rejected: %d", got)
	}
}

func TestRuntimeBorrowedSchedulerIsNeverStopped(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	scheduler := newLifecycleScheduler(true)
	first, err := process.NewRuntime(Options{Scheduler: scheduler, SchedulerBorrowed: true})
	if err != nil {
		t.Fatal(err)
	}
	second, err := process.NewRuntime(Options{Scheduler: scheduler, SchedulerBorrowed: true})
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	first.Close()
	if got := scheduler.stopCalls.Load(); got != 0 {
		t.Fatalf("borrowed scheduler stopped with one Runtime still active: %d", got)
	}
	second.Close()
	if got := scheduler.stopCalls.Load(); got != 0 {
		t.Fatalf("borrowed scheduler was stopped by Runtime.Close: %d", got)
	}
}

func TestRuntimeSchedulerStopAllowsOnCloseWaitRegistration(t *testing.T) {
	scheduler := newLifecycleScheduler(true)
	var rt *Runtime
	scheduler.onStop = func() {
		rt.OnCloseWait(func() {})
	}
	var err error
	rt, err = NewProcess().NewRuntime(Options{Scheduler: scheduler})
	if err != nil {
		t.Fatal(err)
	}

	closeDone := make(chan struct{})
	go func() {
		rt.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Runtime.Close deadlocked during scheduler Stop reentrancy")
	}
}

func TestRuntimeCloseWaitRegistrationDuringWaitingJoinsBeforeReset(t *testing.T) {
	scheduler := newLifecycleScheduler(false)
	rt, err := NewProcess().NewRuntime(Options{Scheduler: scheduler})
	if err != nil {
		t.Fatal(err)
	}
	rt.Batch.SetLastBatchMS(123)

	closeDone := make(chan struct{})
	go func() {
		rt.Close()
		close(closeDone)
	}()
	waitForRuntimePhase(t, rt, closeWaiting)

	hookEntered := make(chan struct{})
	releaseHook := make(chan struct{})
	hookValues := make(chan [2]int64, 1)
	registerDone := make(chan struct{})
	go func() {
		rt.OnCloseWait(func() {
			before := rt.Batch.LastBatchMS()
			close(hookEntered)
			<-releaseHook
			hookValues <- [2]int64{before, rt.Batch.LastBatchMS()}
		})
		close(registerDone)
	}()
	select {
	case <-registerDone:
	case <-time.After(time.Second):
		t.Fatal("OnCloseWait registration did not return during closeWaiting")
	}
	select {
	case <-hookEntered:
	case <-time.After(time.Second):
		t.Fatal("OnCloseWait did not run during closeWaiting")
	}

	scheduler.cancel()
	select {
	case <-closeDone:
		t.Fatal("Runtime.Close returned before the waiting hook completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseHook)
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Runtime.Close did not finish after the waiting hook completed")
	}
	values := <-hookValues
	if values[0] != 123 || values[1] != 123 {
		t.Fatalf("waiting hook observed batch state %v, want [123 123]", values)
	}
}

func TestRuntimeConcurrentStopAndCloseStopSchedulerOnce(t *testing.T) {
	scheduler := newLifecycleScheduler(true)
	var rt *Runtime
	stopEntered := make(chan struct{})
	releaseStop := make(chan struct{})
	var stopOnce sync.Once
	scheduler.onStop = func() {
		stopOnce.Do(func() { close(stopEntered) })
		rt.Stop()
		rt.Close()
		rt.stopScheduler()
		<-releaseStop
	}
	var err error
	rt, err = NewProcess().NewRuntime(Options{Scheduler: scheduler})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-releaseStop:
		default:
			close(releaseStop)
		}
		rt.Close()
		rt.Join()
	})

	done := make(chan struct{}, 5)
	go func() {
		rt.Stop()
		done <- struct{}{}
	}()
	select {
	case <-stopEntered:
	case <-time.After(time.Second):
		t.Fatal("scheduler Stop was not entered")
	}
	for i := 0; i < 4; i++ {
		go func(index int) {
			if index%2 == 0 {
				rt.Stop()
			} else {
				rt.Close()
			}
			done <- struct{}{}
		}(i)
	}
	close(releaseStop)
	for i := 0; i < 5; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("concurrent Stop/Close caller did not return")
		}
	}
	if got := scheduler.stopCalls.Load(); got != 1 {
		t.Fatalf("scheduler Stop calls = %d, want 1", got)
	}
	rt.Join()
}

func TestRuntimeSchedulerCallbackStopWithExternalCloseAndJoin(t *testing.T) {
	scheduler := newLifecycleScheduler(false)
	stopEntered := make(chan struct{})
	releaseSchedulerStop := make(chan struct{})
	scheduler.onStop = func() {
		close(stopEntered)
		<-releaseSchedulerStop
	}
	rt, err := NewProcess().NewRuntime(Options{Scheduler: scheduler})
	if err != nil {
		t.Fatal(err)
	}
	rt.Batch.SetLastBatchMS(123)

	callbackDone := make(chan struct{})
	stopReturned := make(chan struct{})
	releaseCallback := make(chan struct{})
	go func() {
		defer scheduler.cancel()
		rt.Stop()
		close(stopReturned)
		<-releaseCallback
		close(callbackDone)
	}()
	select {
	case <-stopEntered:
	case <-time.After(time.Second):
		t.Fatal("scheduler callback did not enter Runtime.Stop")
	}

	closeDone := make(chan struct{})
	go func() {
		rt.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("external Close did not record its request")
	}
	close(releaseSchedulerStop)
	select {
	case <-stopReturned:
	case <-time.After(time.Second):
		t.Fatal("Runtime.Stop did not return after scheduler Stop was released")
	}

	joinDone := make(chan struct{})
	go func() {
		rt.Join()
		close(joinDone)
	}()
	select {
	case <-joinDone:
		t.Fatal("Join returned before the scheduler callback completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseCallback)
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("Runtime.Stop deadlocked in the scheduler callback")
	}
	select {
	case <-joinDone:
	case <-time.After(time.Second):
		t.Fatal("Join did not return after callback and close completed")
	}
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("joined runtime retained batch timestamp %d, want reset", got)
	}
	if got := scheduler.stopCalls.Load(); got != 1 {
		t.Fatalf("scheduler Stop calls = %d, want 1", got)
	}

	rt.Close()
	rt.Join()
}

func TestRuntimeCallbackCloseDoesNotWaitForItsOwnLease(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	rt.Batch.SetLastBatchMS(123)

	callbackEntered := make(chan struct{})
	callClose := make(chan struct{})
	callbackReturned := make(chan struct{})
	releaseCallback := make(chan struct{})
	callbackDone := make(chan struct{})
	waitEntered := make(chan struct{})
	releaseWait := make(chan struct{})
	var releaseCallbackOnce sync.Once
	var releaseWaitOnce sync.Once
	releaseCallbackFn := func() { releaseCallbackOnce.Do(func() { close(releaseCallback) }) }
	releaseWaitFn := func() { releaseWaitOnce.Do(func() { close(releaseWait) }) }
	t.Cleanup(func() {
		releaseCallbackFn()
		releaseWaitFn()
		rt.Close()
		rt.Join()
	})
	rt.OnCloseWait(func() {
		close(waitEntered)
		<-releaseWait
	})
	go func() {
		if !rt.EnterCallback() {
			close(callbackDone)
			return
		}
		defer rt.LeaveCallback()
		defer close(callbackDone)
		close(callbackEntered)
		<-callClose
		rt.Close()
		close(callbackReturned)
		<-releaseCallback
	}()
	<-callbackEntered
	go rt.Close()
	select {
	case <-waitEntered:
	case <-time.After(time.Second):
		t.Fatal("runtime close did not reach its wait phase")
	}
	close(callClose)
	select {
	case <-callbackReturned:
	case <-time.After(time.Second):
		t.Fatal("callback Close waited for its own runtime lease")
	}
	if got := rt.Batch.LastBatchMS(); got != 123 {
		t.Fatalf("runtime state reset while callback was active: got %d", got)
	}
	releaseCallbackFn()
	releaseWaitFn()
	rt.Join()
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("callback did not leave its runtime lease")
	}
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("runtime state after callback close = %d, want 0", got)
	}
}

func TestRuntimeConcurrentCloseReturnsWhileCallbackLeaseIsActive(t *testing.T) {
	rt, err := NewProcess().NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	rt.Batch.SetLastBatchMS(123)
	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	callbackDone := make(chan struct{})
	waitEntered := make(chan struct{})
	releaseWait := make(chan struct{})
	var releaseCallbackOnce sync.Once
	var releaseWaitOnce sync.Once
	releaseCallbackFn := func() { releaseCallbackOnce.Do(func() { close(releaseCallback) }) }
	releaseWaitFn := func() { releaseWaitOnce.Do(func() { close(releaseWait) }) }
	t.Cleanup(func() {
		releaseCallbackFn()
		releaseWaitFn()
		rt.Close()
		rt.Join()
	})
	rt.OnCloseWait(func() {
		close(waitEntered)
		<-releaseWait
	})
	go func() {
		if !rt.EnterCallback() {
			close(callbackDone)
			return
		}
		defer rt.LeaveCallback()
		defer close(callbackDone)
		close(callbackEntered)
		<-releaseCallback
	}()
	<-callbackEntered

	const closeCount = 8
	closeDone := make(chan struct{}, closeCount)
	for range closeCount {
		go func() {
			rt.Close()
			closeDone <- struct{}{}
		}()
	}
	for range closeCount {
		select {
		case <-closeDone:
		case <-time.After(time.Second):
			t.Fatal("concurrent Runtime.Close waited on an active callback")
		}
	}
	select {
	case <-waitEntered:
	case <-time.After(time.Second):
		t.Fatal("concurrent close did not reach its wait phase")
	}
	if got := rt.Batch.LastBatchMS(); got != 123 {
		t.Fatalf("runtime state reset while callback was active: got %d", got)
	}
	releaseCallbackFn()
	releaseWaitFn()
	rt.Join()
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("callback did not leave its runtime lease")
	}
	if got := rt.Batch.LastBatchMS(); got != 0 {
		t.Fatalf("runtime state after concurrent close = %d, want 0", got)
	}
}
