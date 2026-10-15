package research

import (
	"reflect"
	"testing"
)

func TestSelectParametersRequiresRealizedObservation(t *testing.T) {
	spec := ParameterSelectionSpec{TrainStart: 2, TrainEnd: 10, AvailableAt: 12, MinIndependentSamples: 2, ManifestID: "manifest", AlgorithmVersion: "v1"}
	rows := []ParameterObservation{
		{SID: 1, Candidate: "hold", BeginAt: 2, EndAt: 5, AvailableAt: 5, NetReturn: .1},
		{SID: 1, Candidate: "hold", BeginAt: 5, EndAt: 8, AvailableAt: 8, NetReturn: .2},
	}
	for _, publication := range []int64{1, 4} {
		bad := append([]ParameterObservation(nil), rows...)
		bad[0].AvailableAt = publication
		if _, err := SelectParameters(bad, spec); err == nil {
			t.Fatalf("observation published at %d before end 5 admitted", publication)
		}
	}
	for _, publication := range []int64{5, 12} {
		rows[0].AvailableAt = publication
		want, err := SelectParameters(rows, spec)
		if err != nil || want.Global.Samples != 2 || want.SampleCutoff != 8 {
			t.Fatalf("valid mature/delayed observations rejected: %+v %v", want, err)
		}
		withFuture := append([]ParameterObservation(nil), rows...)
		withFuture = append(withFuture,
			ParameterObservation{SID: 1, Candidate: "future", BeginAt: 8, EndAt: 10, AvailableAt: 13, NetReturn: 1e9},
			ParameterObservation{SID: 1, Candidate: "outside", BeginAt: 10, EndAt: 13, AvailableAt: 13, NetReturn: 1e9},
		)
		got, err := SelectParameters(withFuture, spec)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("future observations changed selection: %+v %v", got, err)
		}
	}
}

func TestICHistoryRejectsPublicationBeforeMaturity(t *testing.T) {
	h, err := NewICHistory(5, "forward", []string{"momentum"})
	if err != nil {
		t.Fatal(err)
	}
	sample := ICSample{Column: "momentum", Label: "forward", DecisionTime: 100, MatureAt: 110, AvailableAt: 109, IC: number(.5), Samples: 24}
	if err := h.Add(120, sample); err == nil {
		t.Fatal("IC published before maturity admitted")
	}
	if h.lastAsOf != 0 || len(h.samples[sample.Column]) != 0 {
		t.Fatal("rejected IC advanced history")
	}
	for _, publication := range []int64{110, 115} {
		h, _ := NewICHistory(5, "forward", []string{"momentum"})
		sample.AvailableAt = publication
		if err := h.Add(publication-1, sample); err == nil {
			t.Fatal("unseen IC admitted")
		}
		if err := h.Add(publication, sample); err != nil || len(h.samples[sample.Column]) != 1 {
			t.Fatalf("mature visible IC rejected: %v", err)
		}
	}
}

func TestEvaluateRequiresResolvedPublicationAfterMaturity(t *testing.T) {
	frame, universe := researchFixture()
	label := Label{Name: "forward", Kind: ExecutableReturn, SID: 1, DecisionTime: 100, BeginAt: 101, EndAt: 110, MatureAt: 115, AvailableAt: 114, Resolved: true, Value: number(.1)}
	spec := EvaluationSpec{AsOf: 120, PrimaryLabel: "forward"}
	if _, err := Evaluate(frame, universe, []Label{label}, spec); err == nil {
		t.Fatal("resolved label published before maturity admitted")
	}
	for _, publication := range []int64{115, 120, 121} {
		label.AvailableAt = publication
		r, err := Evaluate(frame, universe, []Label{label}, spec)
		if err != nil {
			t.Fatalf("valid resolved publication rejected: %v", err)
		}
		m, exists := r.Columns["momentum"].Labels["forward"]
		if publication <= spec.AsOf && (!exists || m.Pairs != 1) || publication > spec.AsOf && exists {
			t.Fatalf("incorrect visible label membership: %+v", r.Columns["momentum"])
		}
	}
	// Unresolved scheduled labels remain excluded, preserving their existing contract.
	label.Resolved, label.AvailableAt = false, 114
	r, err := Evaluate(frame, universe, []Label{label}, spec)
	if err != nil || len(r.Columns["momentum"].Labels) != 0 {
		t.Fatalf("unresolved label contract changed: %+v %v", r, err)
	}
}
