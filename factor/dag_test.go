package factor

import "testing"

func TestDAGCanonicalSharingParameterVersionAndDefensiveCopy(t *testing.T) {
	a, b := Field("prices", "close", "1h"), Field("prices", "close", "1h")
	plan, err := New().Add("first", Return(a, 24)).Add("second", Return(b, 24)).Add("different", Return(b, 12)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if plan.NodeCount() != 3 || plan.WarmupLength() != 24 || plan.StateRetention() != 25 {
		t.Fatalf("canonical/retention contract = %d/%d/%d", plan.NodeCount(), plan.WarmupLength(), plan.StateRetention())
	}
	hash := plan.Hash()
	a.Spec.Field = "other"
	if plan.Hash() != hash {
		t.Fatal("builder mutation altered compiled identity")
	}
	newPlan, err := New().Add("first", Return(Field("prices", "close", "1h"), 24)).Add("second", Return(Field("prices", "close", "1h"), 24)).Add("different", Return(Field("prices", "close", "1h"), 12)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if newPlan.Hash() != hash {
		t.Fatal("equivalent graph identity unstable")
	}
	b.Spec.Version = "changed"
	changed, err := New().Add("first", Return(b, 24)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	unchanged, err := New().Add("first", Return(Field("prices", "close", "1h"), 24)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if changed.Hash() == unchanged.Hash() {
		t.Fatal("operator version omitted from cache identity")
	}
}

func TestDAGRejectsCycleLabelsMissingCustomContractAndMixedTimeFrame(t *testing.T) {
	root := Field("prices", "close", "1h")
	cycle := EMA(root, 3)
	cycle.Inputs[0] = cycle
	label := Field("labels", "future", "1h")
	label.Spec.Operator = "label"
	custom := node("custom", TS, root)
	custom.Spec.Version = ""
	custom.Evaluate = func(values []Numeric) Numeric { return values[0] }
	for name, n := range map[string]*Node{"cycle": cycle, "label": Return(label, 1), "custom": custom, "period": EMA(root, 0), "ddof": StdDev(root, 3, 3), "timeframe": Linear([]*Node{root, Field("prices", "close", "1m")}, []float64{1, 1})} {
		if _, err := New().Add(name, n).Compile(); err == nil {
			t.Fatalf("accepted invalid %s graph", name)
		}
	}
}
