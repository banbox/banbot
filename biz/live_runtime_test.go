package biz

import (
	"testing"

	"github.com/banbox/banbot/btime"
)

func TestRuntimeFatalStopUsesRuntimeClockWhenNowIsNil(t *testing.T) {
	previous := btime.CurTimeMS
	btime.SetTimeMS(9_999)
	t.Cleanup(func() { btime.SetTimeMS(previous) })

	firstClock := btime.NewClockState(true, nil)
	firstClock.SetTimeMS(101)
	secondClock := btime.NewClockState(true, nil)
	secondClock.SetTimeMS(202)

	firstNow := runtimeFatalStopClock(RuntimeDeps{Clock: firstClock}, nil)
	secondNow := runtimeFatalStopClock(RuntimeDeps{Clock: secondClock}, nil)
	if firstNow == nil || secondNow == nil {
		t.Fatal("runtime clock was not selected")
	}
	if got := firstNow(); got != 101 {
		t.Fatalf("first runtime clock = %d, want 101", got)
	}
	if got := secondNow(); got != 202 {
		t.Fatalf("second runtime clock = %d, want 202", got)
	}
	if got := runtimeFatalStopClock(RuntimeDeps{}, nil); got != nil {
		t.Fatal("missing explicit runtime clock was not rejected")
	}

	explicit := func() int64 { return 303 }
	if got := runtimeFatalStopClock(RuntimeDeps{Clock: firstClock}, explicit)(); got != 303 {
		t.Fatalf("explicit now function = %d, want 303", got)
	}
}
