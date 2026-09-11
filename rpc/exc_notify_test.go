package rpc

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestScopedExceptionStateAggregatesConcurrentWrites(t *testing.T) {
	const total = 64
	sent := make(chan map[string]interface{}, 2)
	state := &scopedExceptionState{
		sender: func(msg map[string]interface{}) {
			sent <- msg
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			state.add("caller", fmt.Sprintf("message-%d", i))
		}(i)
	}
	wg.Wait()

	select {
	case msg := <-sent:
		status, ok := msg["status"].(string)
		if !ok || !strings.HasPrefix(status, fmt.Sprintf("num:%d, caller\n", total)) {
			t.Fatalf("aggregated exception = %#v, want count %d", msg, total)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("scoped exception was not delivered")
	}

	// Exercise the post-interval cleanup without waiting for the production
	// 60-second interval. The entry must be removed only after its pending
	// content has been delivered, so one-off caller keys do not accumulate.
	state.mu.Lock()
	entry := state.entries["caller"]
	if entry == nil {
		state.mu.Unlock()
		t.Fatal("scoped exception entry disappeared before its interval elapsed")
	}
	if entry.timer != nil {
		entry.timer.Stop()
	}
	entry.nextMS = time.Now().UnixMilli() - 1
	entry.num = 1
	entry.content = "final"
	entry.timer = nil
	state.mu.Unlock()

	state.flush("caller")
	select {
	case msg := <-sent:
		if got := msg["status"]; got != "num:1, caller\nfinal" {
			t.Fatalf("final scoped exception = %#v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("final scoped exception was not delivered")
	}
	// The first flush starts the next throttle interval. Advance it and run
	// the cleanup timer's callback explicitly to avoid waiting 60 seconds.
	state.mu.Lock()
	if entry := state.entries["caller"]; entry != nil && entry.timer != nil {
		entry.timer.Stop()
	}
	state.entries["caller"].nextMS = time.Now().UnixMilli() - 1
	state.mu.Unlock()
	state.flush("caller")
	state.mu.Lock()
	_, ok := state.entries["caller"]
	state.mu.Unlock()
	if ok {
		t.Fatal("scoped exception entry was retained after its interval elapsed")
	}
}

func TestExcNotifyCloneSharesScopedState(t *testing.T) {
	notify := newExcNotifyWithSender(func(map[string]interface{}) {})
	clone := notify.clone()
	if clone.scoped != notify.scoped {
		t.Fatal("cloned exception core does not share scoped aggregation state")
	}
}
