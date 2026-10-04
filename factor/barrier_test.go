package factor

import (
	"errors"
	"math"
	"reflect"
	"sync/atomic"
	"testing"
)

func beginTestRound(t *testing.T, barrier *RoundBarrier, plan *Plan, snapshot *Snapshot, deadline int64) RoundToken {
	t.Helper()
	requirements := make([]Requirement, 0, len(snapshot.status.Expected))
	for _, key := range snapshot.status.Expected {
		requirements = append(requirements, Requirement{SID: key.SID, Source: key.Source, TimeFrame: key.TimeFrame, EventTime: snapshot.spec.DecisionTime})
	}
	token, err := barrier.Begin(plan.Hash(), snapshot.Spec(), requirements, deadline)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range snapshot.status.Expected {
		row, _ := snapshot.Row(key.SID, key.Source, key.TimeFrame)
		if err := barrier.Observe(token, row, snapshot.spec.DecisionTime); err != nil {
			t.Fatal(err)
		}
	}
	return token
}

func TestBarrierCanceledLateComputationDoesNotPoisonRecursiveOwner(t *testing.T) {
	price := Field("prices", "close", "1h")
	ema := EMA(price, 3)
	std := StdDev(price, 3, 0)
	started, release := make(chan struct{}), make(chan struct{})
	var delay atomic.Bool
	slow := Custom("slow-1", []*Node{ema, std}, func(values []Numeric) Numeric {
		if delay.Swap(false) {
			close(started)
			<-release
		}
		return values[0]
	})
	plan, err := New().Add("ema", ema).Add("std", std).Add("slow", slow).Compile()
	if err != nil {
		t.Fatal(err)
	}
	owner, _ := NewSession(plan)
	reference, _ := NewSession(plan)
	barrier := &RoundBarrier{}
	for i := 1; i <= 5; i++ {
		snapshot := testSnapshot(t, int64(i)*1000, map[int32]map[string]any{1: {"close": float64(i)}})
		token := beginTestRound(t, barrier, plan, snapshot, int64(i)*1000+500)
		frame, err := barrier.Compute(token, owner, func() int64 { return int64(i) * 1000 })
		if err != nil {
			t.Fatal(err)
		}
		want, err := reference.Evaluate(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		compareNumeric(t, want.Values["ema"][1], frame.Values["ema"][1], "initial fork recursion")
	}
	before := owner.Updates()
	last, _ := barrier.Latest()
	snapshot := testSnapshot(t, 6000, map[int32]map[string]any{1: {"close": 999.0}})
	token := beginTestRound(t, barrier, plan, snapshot, 6500)
	ctx, err := barrier.Context(token)
	if err != nil {
		t.Fatal(err)
	}
	delay.Store(true)
	done := make(chan error, 1)
	go func() { _, err := barrier.Compute(token, owner, func() int64 { return 6000 }); done <- err }()
	<-started
	if err := barrier.Cancel(token); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	close(release)
	if err := <-done; !errors.Is(err, ErrRoundStale) {
		t.Fatalf("canceled completion admitted: %v", err)
	}
	if !reflect.DeepEqual(before, owner.Updates()) {
		t.Fatal("abandoned fork changed original counters")
	}
	after, _ := barrier.Latest()
	if after.SnapshotID != last.SnapshotID {
		t.Fatal("cancel published new target")
	}
	next := testSnapshot(t, 7000, map[int32]map[string]any{1: {"close": 7.0}})
	nextToken := beginTestRound(t, barrier, plan, next, 7500)
	got, err := barrier.Compute(nextToken, owner, func() int64 { return 7000 })
	if err != nil {
		t.Fatal(err)
	}
	want, err := reference.Evaluate(next)
	if err != nil {
		t.Fatal(err)
	}
	compareNumeric(t, want.Values["ema"][1], got.Values["ema"][1], "EMA after canceled huge value")
	compareNumeric(t, want.Values["std"][1], got.Values["std"][1], "StdDev More after cancellation")
}

func TestBarrierExpiryFreezeAndFailedComputationPreservePublication(t *testing.T) {
	plan, _ := New().Add("value", Field("prices", "close", "1h")).Compile()
	owner, _ := NewSession(plan)
	barrier := &RoundBarrier{}
	first := testSnapshot(t, 1000, map[int32]map[string]any{1: {"close": 1.0}})
	token := beginTestRound(t, barrier, plan, first, 1500)
	if _, err := barrier.Compute(token, owner, func() int64 { return 1000 }); err != nil {
		t.Fatal(err)
	}
	if _, err := barrier.Begin(plan.Hash(), first.Spec(), []Requirement{{SID: 1, Source: "prices", TimeFrame: "1h", EventTime: 1000}}, 1500); err == nil {
		t.Fatal("duplicate published decision admitted")
	}
	row, _ := first.Row(1, "prices", "1h")
	if err := barrier.Observe(token, row, 1100); !errors.Is(err, ErrRoundFrozen) {
		t.Fatalf("late observation: %v", err)
	}
	expired := testSnapshot(t, 2000, map[int32]map[string]any{1: {"close": 2.0}})
	expiredToken := beginTestRound(t, barrier, plan, expired, 2500)
	clock := atomic.Int64{}
	clock.Store(2000)
	if _, err := barrier.Freeze(expiredToken, 2000); err != nil {
		t.Fatal(err)
	}
	clock.Store(2500)
	if _, err := barrier.Compute(expiredToken, owner, clock.Load); !errors.Is(err, ErrRoundExpired) {
		t.Fatalf("deadline admitted: %v", err)
	}
	latest, _ := barrier.Latest()
	if latest.SnapshotID != first.ID() {
		t.Fatal("expired round replaced old publication")
	}
	third := testSnapshot(t, 3000, map[int32]map[string]any{1: {"close": 3.0}})
	spec := third.Spec()
	spec.SourceVersions["prices"] = "changed"
	row, _ = third.Row(1, "prices", "1h")
	row.SourceVersion = "changed"
	failed, err := Freeze(spec, []VersionRecord{row}, []Requirement{{SID: 1, Source: "prices", TimeFrame: "1h", EventTime: 3000}})
	if err != nil {
		t.Fatal(err)
	}
	failedToken := beginTestRound(t, barrier, plan, failed, 3500)
	before := owner.Updates()
	if _, err := barrier.Compute(failedToken, owner, func() int64 { return 3000 }); err == nil {
		t.Fatal("incompatible source computed")
	}
	if !reflect.DeepEqual(before, owner.Updates()) {
		t.Fatal("failed private computation changed live owner")
	}
	latest, _ = barrier.Latest()
	if latest.SnapshotID != first.ID() {
		t.Fatal("failed round replaced publication")
	}
}

func TestBarrierGenerationAndConcurrentOwnerAdvanceRejectOverwrite(t *testing.T) {
	field := Field("prices", "close", "1h")
	started, release := make(chan struct{}), make(chan struct{})
	var delay atomic.Bool
	slow := Custom("slow-1", []*Node{field}, func(values []Numeric) Numeric {
		if delay.Swap(false) {
			close(started)
			<-release
		}
		return values[0]
	})
	plan, _ := New().Add("value", slow).Compile()
	owner, _ := NewSession(plan)
	barrier := &RoundBarrier{}
	first := testSnapshot(t, 1000, map[int32]map[string]any{1: {"close": 1.0}})
	oldToken := beginTestRound(t, barrier, plan, first, 1500)
	second := testSnapshot(t, 2000, map[int32]map[string]any{1: {"close": 2.0}})
	token := beginTestRound(t, barrier, plan, second, 2500)
	if _, err := barrier.Freeze(oldToken, 1000); !errors.Is(err, ErrRoundStale) {
		t.Fatalf("superseded generation admitted: %v", err)
	}
	delay.Store(true)
	done := make(chan error, 1)
	go func() { _, err := barrier.Compute(token, owner, func() int64 { return 2000 }); done <- err }()
	<-started
	direct := testSnapshot(t, 3000, map[int32]map[string]any{1: {"close": 3.0}})
	if _, err := owner.Evaluate(direct); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("private round overwrote concurrently advanced owner")
	}
	if owner.latest.DecisionTime != 3000 || math.Abs(owner.latest.Values["value"][1].Value-3) > 1e-12 {
		t.Fatal("concurrent owner's accepted state corrupted")
	}
}

func TestBarrierIncompleteNeverProducesFrame(t *testing.T) {
	plan, _ := New().Add("value", Field("prices", "close", "1h")).Compile()
	owner, _ := NewSession(plan)
	base := testSnapshot(t, 1000, map[int32]map[string]any{1: {"close": 1.0}, 2: {"close": 2.0}})
	barrier := &RoundBarrier{}
	need := []Requirement{{SID: 1, Source: "prices", TimeFrame: "1h", EventTime: 1000}, {SID: 2, Source: "prices", TimeFrame: "1h", EventTime: 1000}}
	token, err := barrier.Begin(plan.Hash(), base.Spec(), need, 1500)
	if err != nil {
		t.Fatal(err)
	}
	row, _ := base.Row(1, "prices", "1h")
	if err := barrier.Observe(token, row, 1000); err != nil {
		t.Fatal(err)
	}
	if _, err := barrier.Compute(token, owner, func() int64 { return 1000 }); !errors.Is(err, ErrSnapshotIncomplete) {
		t.Fatalf("partial reference computed: %v", err)
	}
	if _, ok := barrier.Latest(); ok {
		t.Fatal("incomplete frame published")
	}
	if len(owner.Updates()) != 0 {
		t.Fatal("partial round advanced owner")
	}
}

func TestBarrierDeadlineAfterPrivateEvaluationAndStopJoin(t *testing.T) {
	field := Field("prices", "close", "1h")
	started, release := make(chan struct{}), make(chan struct{})
	var block atomic.Bool
	clock := atomic.Int64{}
	clock.Store(1000)
	custom := Custom("blocked", []*Node{field}, func(values []Numeric) Numeric {
		if block.Swap(false) {
			close(started)
			<-release
		}
		return values[0]
	})
	plan, _ := New().Add("value", custom).Compile()
	owner, _ := NewSession(plan)
	barrier := &RoundBarrier{}
	first := testSnapshot(t, 1000, map[int32]map[string]any{1: {"close": 1.0}})
	token := beginTestRound(t, barrier, plan, first, 1500)
	block.Store(true)
	done := make(chan error, 1)
	go func() { _, err := barrier.Compute(token, owner, clock.Load); done <- err }()
	<-started
	clock.Store(1500)
	close(release)
	if err := <-done; !errors.Is(err, ErrRoundExpired) {
		t.Fatalf("late computation published after deadline: %v", err)
	}
	if owner.revision != 0 || len(owner.Updates()) != 0 {
		t.Fatal("deadline advanced live recursive state")
	}
	barrier.Stop()
	barrier.Join()
	if _, err := barrier.Begin(plan.Hash(), first.Spec(), []Requirement{{SID: 1, Source: "prices", TimeFrame: "1h", EventTime: 1000}}, 2000); !errors.Is(err, ErrRoundStale) {
		t.Fatalf("stopped barrier admitted work: %v", err)
	}
}
