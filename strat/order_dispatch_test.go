package strat

import (
	"fmt"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/orm/ormo"
)

func TestFireOdChangeWithStateIsolatesCallbacks(t *testing.T) {
	oldSubs := accOdSubs
	lockOdSub.Lock()
	accOdSubs = make(map[string][]FnOdChange)
	lockOdSub.Unlock()
	t.Cleanup(func() {
		lockOdSub.Lock()
		accOdSubs = oldSubs
		lockOdSub.Unlock()
	})

	var legacyCalls, firstCalls, firstWildcardCalls, secondCalls int
	AddOdSub("runtime", func(string, *ormo.InOutOrder, int) { legacyCalls++ })
	first := NewState()
	first.AddOdSub("runtime", func(string, *ormo.InOutOrder, int) { firstCalls++ })
	first.AddOdSub("*", func(string, *ormo.InOutOrder, int) { firstWildcardCalls++ })
	second := NewState()
	second.AddOdSub("runtime", func(string, *ormo.InOutOrder, int) { secondCalls++ })
	order := &ormo.InOutOrder{Enter: &ormo.ExOrder{}}

	FireOdChangeWithState(first, "runtime", order, OdChgEnter)
	if firstCalls != 1 || firstWildcardCalls != 1 || secondCalls != 0 || legacyCalls != 0 {
		t.Fatalf("first runtime dispatch leaked: first=%d wildcard=%d second=%d legacy=%d",
			firstCalls, firstWildcardCalls, secondCalls, legacyCalls)
	}

	FireOdChangeWithState(second, "runtime", order, OdChgEnter)
	if firstCalls != 1 || firstWildcardCalls != 1 || secondCalls != 1 || legacyCalls != 0 {
		t.Fatalf("second runtime dispatch leaked: first=%d wildcard=%d second=%d legacy=%d",
			firstCalls, firstWildcardCalls, secondCalls, legacyCalls)
	}

	FireOdChangeWithState(nil, "runtime", order, OdChgEnter)
	if legacyCalls != 1 {
		t.Fatalf("nil state did not preserve legacy dispatch: legacy=%d", legacyCalls)
	}
}

func newOrderCallbackState(timeMS int64) *State {
	state := NewState()
	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(timeMS)
	state.BindRuntime(nil, clock, nil, nil, nil)
	return state
}

func TestFireOdChangeWithStateKeepsClockDuringConcurrentCallbacks(t *testing.T) {
	state := newOrderCallbackState(1_000)
	firstEntered := make(chan struct{})
	allowFirstReturn := make(chan struct{})
	secondEntered := make(chan struct{})
	allowSecondReturn := make(chan struct{})
	firstDone := make(chan struct{})
	secondDone := make(chan struct{})
	first := &ormo.InOutOrder{Enter: &ormo.ExOrder{CreateAt: 100}}
	second := &ormo.InOutOrder{Enter: &ormo.ExOrder{CreateAt: 200}}

	state.AddOdSub("first", func(string, *ormo.InOutOrder, int) {
		close(firstEntered)
		<-allowFirstReturn
	})
	state.AddOdSub("second", func(string, *ormo.InOutOrder, int) {
		close(secondEntered)
		<-allowSecondReturn
	})
	go func() {
		FireOdChangeWithState(state, "first", first, OdChgEnter)
		close(firstDone)
	}()
	<-firstEntered
	if got := state.Clock.TimeMS(); got != 1_000 {
		t.Fatalf("first callback changed main clock: got %d want %d", got, 1_000)
	}
	go func() {
		FireOdChangeWithState(state, "second", second, OdChgEnter)
		close(secondDone)
	}()
	<-secondEntered
	if got := state.Clock.TimeMS(); got != 1_000 {
		t.Fatalf("concurrent callback changed main clock: got %d want %d", got, 1_000)
	}
	close(allowFirstReturn)
	<-firstDone
	close(allowSecondReturn)
	<-secondDone
	if got := state.Clock.TimeMS(); got != 1_000 {
		t.Fatalf("callbacks left main clock changed: got %d want %d", got, 1_000)
	}
}

func TestFireOdChangeWithStateKeepsClockDuringNestedCallback(t *testing.T) {
	state := newOrderCallbackState(1_000)
	outer := &ormo.InOutOrder{Enter: &ormo.ExOrder{CreateAt: 100}}
	inner := &ormo.InOutOrder{Enter: &ormo.ExOrder{CreateAt: 200}}
	state.AddOdSub("outer", func(string, *ormo.InOutOrder, int) {
		if got := state.Clock.TimeMS(); got != 1_000 {
			t.Fatalf("outer callback changed main clock: got %d want %d", got, 1_000)
		}
		FireOdChangeWithState(state, "inner", inner, OdChgEnter)
		if got := state.Clock.TimeMS(); got != 1_000 {
			t.Fatalf("nested callback changed main clock: got %d want %d", got, 1_000)
		}
	})
	state.AddOdSub("inner", func(string, *ormo.InOutOrder, int) {
		if got := state.Clock.TimeMS(); got != 1_000 {
			t.Fatalf("inner callback changed main clock: got %d want %d", got, 1_000)
		}
	})

	FireOdChangeWithState(state, "outer", outer, OdChgEnter)
}

func TestFireOdChangeWithStateKeepsClockAfterCallbackPanic(t *testing.T) {
	state := newOrderCallbackState(1_000)
	state.AddOdSub("runtime", func(string, *ormo.InOutOrder, int) { panic("callback failure") })
	order := &ormo.InOutOrder{Enter: &ormo.ExOrder{CreateAt: 100}}

	func() {
		defer func() {
			if got := recover(); fmt.Sprint(got) != "callback failure" {
				t.Fatalf("panic = %v, want callback failure", got)
			}
		}()
		FireOdChangeWithState(state, "runtime", order, OdChgEnter)
	}()
	if got := state.Clock.TimeMS(); got != 1_000 {
		t.Fatalf("panic callback changed main clock: got %d want %d", got, 1_000)
	}
}

func TestOrderEventTime(t *testing.T) {
	order := &ormo.InOutOrder{
		Enter: &ormo.ExOrder{CreateAt: 100, UpdateAt: 101},
		Exit:  &ormo.ExOrder{CreateAt: 200, UpdateAt: 201},
	}
	for _, test := range []struct {
		event int
		want  int64
	}{
		{OdChgNew, 0},
		{OdChgEnter, 100},
		{OdChgEnterFill, 101},
		{OdChgExit, 200},
		{OdChgExitFill, 201},
	} {
		if got := OrderEventTime(order, test.event); got != test.want {
			t.Errorf("OrderEventTime(event=%d) = %d, want %d", test.event, got, test.want)
		}
	}
}
