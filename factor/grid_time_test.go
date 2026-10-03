package factor

import "testing"

func TestDelayedPublicationUsesLogicalGridAndActualVisibility(t *testing.T) {
	plan, err := New().Add("close", Field("prices", "close", "1h")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	base := testSnapshot(t, 10, map[int32]map[string]any{1: {"close": float64(7)}})
	spec := base.Spec()
	row, _ := base.Row(1, "prices", "1h")
	row.AvailableAt, row.IngestedAt = 12, 13
	reqs := []Requirement{{SID: 1, Source: "prices", Frequency: "1h", EventTime: 10}}
	before, err := Freeze(spec, []VersionRecord{row}, reqs)
	if err != nil {
		t.Fatal(err)
	}
	if before.Status().Ready {
		t.Fatal("late publication visible at old bar close")
	}
	spec.GridTime, spec.DecisionTime, spec.ReplayTime = 10, 13, 13
	ready, err := Freeze(spec, []VersionRecord{row}, reqs)
	if err != nil {
		t.Fatal(err)
	}
	owner, _ := NewSession(plan)
	frame, err := owner.Evaluate(ready)
	if err != nil {
		t.Fatal(err)
	}
	if frame.GridTime != 10 || frame.DecisionTime != 13 || frame.Values["close"][1].Value != 7 {
		t.Fatalf("lost grid/visibility identity: %#v", frame)
	}
	batch, err := plan.Batch([]*Snapshot{ready}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if batch[0].GridTime != 10 || batch[0].DecisionTime != 13 || batch[0].Values["close"][1] != frame.Values["close"][1] {
		t.Fatal("batch/live grid parity lost")
	}
	spec.DecisionTime, spec.ReplayTime = 14, 14
	refrozen, err := Freeze(spec, []VersionRecord{row}, reqs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Evaluate(refrozen); err == nil {
		t.Fatal("same grid advanced indicators twice after visibility changed")
	}
	defaultGrid := base.Spec()
	defaultGrid.GridTime = 0
	old, err := Freeze(defaultGrid, []VersionRecord{testRecord(1, 10, map[string]any{"close": float64(7)})}, reqs)
	if err != nil {
		t.Fatal(err)
	}
	if old.ID() != base.ID() {
		t.Fatal("default grid changed logical snapshot hash")
	}
}

func TestBarrierCannotFreezeFutureVisibilityCutoff(t *testing.T) {
	base := testSnapshot(t, 10, map[int32]map[string]any{1: {"close": float64(7)}})
	spec := base.Spec()
	spec.GridTime, spec.DecisionTime, spec.ReplayTime = 10, 13, 13
	var barrier RoundBarrier
	needs := []Requirement{{SID: 1, Source: "prices", Frequency: "1h", EventTime: 10}}
	token, err := barrier.Begin("plan", spec, needs, 20)
	if err != nil {
		t.Fatal(err)
	}
	row, _ := base.Row(1, "prices", "1h")
	row.AvailableAt, row.IngestedAt = 12, 13
	if err := barrier.Observe(token, row, 11); err != nil {
		t.Fatal(err)
	}
	if _, err := barrier.Freeze(token, 11); err == nil {
		t.Fatal("future visibility cutoff was frozen")
	}
	if err := barrier.Observe(token, row, 13); err != nil {
		t.Fatal(err)
	}
	ready, err := barrier.Freeze(token, 13)
	if err != nil {
		t.Fatal(err)
	}
	if !ready.Status().Ready || ready.Spec().GridTime != 10 {
		t.Fatal("delayed visible record did not satisfy logical grid")
	}
}
