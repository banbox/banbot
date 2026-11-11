package factor

import (
	"encoding/json"
	"testing"
)

func TestLifecycleForgetAssets(t *testing.T) {
	p, err := NewLifecyclePolicy(baseLifecycle())
	if err != nil {
		t.Fatal(err)
	}
	forgetter, ok := p.(AssetForgetter)
	if !ok {
		t.Fatal("lifecycle policy must implement AssetForgetter")
	}
	state := LifecycleState{Schema: 1, Assets: map[int32]*AssetLifecycle{1: {Direction: 1, Last: Allocation{NAVFraction, "0.5"}}, 3: {Direction: 1, Last: Allocation{NAVFraction, "0.5"}}},
		AssetIdentities: map[int32]string{1: "A", 3: "C"},
		Cohorts: []PortfolioCohort{
			{ID: "a", Contributions: []CohortContribution{{SID: 1, Planned: "1", Filled: "1"}, {SID: 3, Planned: "1", Filled: "1"}}},
			{ID: "b", Contributions: []CohortContribution{{SID: 3, Planned: "1", Filled: "0"}}},
		}}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	next, err := forgetter.ForgetAssets(raw, []int32{3})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeLifecycleState(next)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Assets) != 1 || got.Assets[1] == nil || len(got.AssetIdentities) != 1 || len(got.Cohorts) != 1 || len(got.Cohorts[0].Contributions) != 1 || got.Cohorts[0].Contributions[0].SID != 1 {
		t.Fatalf("SID 3 not forgotten: %s", next)
	}
	again, err := forgetter.ForgetAssets(next, []int32{3})
	if err != nil || string(again) != string(next) {
		t.Fatalf("forgetting twice must be a no-op: %v %s", err, again)
	}
	if empty, err := forgetter.ForgetAssets(nil, []int32{3}); err != nil || len(empty) != 0 {
		t.Fatalf("empty checkpoint: %v %s", err, empty)
	}
}
