package factor

import (
	"fmt"
	"math"
	"testing"
)

func TestPointwiseSessionBatchHandCalculated(t *testing.T) {
	x, y := Field("prices", "close", "1h"), Field("prices", "other", "1h")
	one := Constant(1, "1h")
	outputs := map[string]*Node{
		"constant": one, "add": Add(x, y), "sub": Sub(x, y),
		"mul": Mul(x, y), "div": Div(x, y), "pow": Pow(x, y),
		"min": Min(x, y), "max": Max(x, y), "neg": Neg(x),
		"abs": Abs(Neg(x)), "log": Log(x), "sqrt": Sqrt(x), "positive": Positive(x),
		"cs-score": Sub(Rank(x), Mul(Constant(0.5, "1h"), Rank(y))),
		"std":      StdDev(Add(x, one), 2, 0),
	}
	plan, err := Compile(outputs)
	if err != nil {
		t.Fatal(err)
	}
	snapshots := []*Snapshot{
		testSnapshot(t, 1000, map[int32]map[string]any{1: {"close": 9.0, "other": 2.0}, 2: {"close": 4.0, "other": 3.0}}),
		testSnapshot(t, 2000, map[int32]map[string]any{1: {"close": 13.0, "other": 2.0}, 2: {"close": 8.0, "other": 3.0}}),
	}
	batch, err := plan.Batch(snapshots, len(snapshots))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"constant": 1, "add": 11, "sub": 7, "mul": 18, "div": 4.5, "pow": 81, "min": 2, "max": 9, "neg": -9, "abs": 9, "log": math.Log(9), "sqrt": 3, "positive": 9}
	for name, value := range want {
		compareNumeric(t, Numeric{value, Valid}, batch[0].Values[name][1], name)
	}
	compareNumeric(t, Numeric{1, Valid}, batch[0].Values["cs-score"][1], "cross-section score SID 1")
	compareNumeric(t, Numeric{-0.5, Valid}, batch[0].Values["cs-score"][2], "cross-section score SID 2")
	compareNumeric(t, Numeric{math.NaN(), Warmup}, batch[0].Values["std"][1], "std warmup")
	compareNumeric(t, Numeric{2, Valid}, batch[1].Values["std"][1], "std window")
	session, _ := NewSession(plan)
	for time, snapshot := range snapshots {
		frame, err := session.Evaluate(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		for name, values := range frame.Values {
			for sid, value := range values {
				compareNumeric(t, batch[time].Values[name][sid], value, fmt.Sprintf("%s/%d/%d", name, time, sid))
			}
		}
	}
}

func TestPointwiseValidityAndDomainSessionBatch(t *testing.T) {
	x, y := Field("prices", "close", "1h"), Field("prices", "other", "1h")
	plan, err := Compile(map[string]*Node{
		"add": Add(x, y), "mul-zero": Mul(x, Constant(0, "1h")),
		"div": Div(x, y), "log": Log(x), "sqrt": Sqrt(x),
		"pow": Pow(x, y), "min": Min(x, y), "max": Max(x, y), "positive": Positive(x),
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		fields map[string]any
		want   map[string]Validity
	}{
		{map[string]any{"other": nil}, map[string]Validity{"add": Missing, "min": Missing, "max": Missing, "mul-zero": Missing}},
		{map[string]any{"close": nil, "other": "bad"}, map[string]Validity{"add": Null, "div": Null, "min": Null, "positive": Null}},
		{map[string]any{"close": "bad", "other": 1}, map[string]Validity{"add": NotNumeric, "sqrt": NotNumeric}},
		{map[string]any{"close": math.NaN(), "other": 1}, map[string]Validity{"add": NonFinite, "mul-zero": NonFinite}},
		{map[string]any{"close": 1.0, "other": 0.0}, map[string]Validity{"div": NonFinite, "log": Valid}},
		{map[string]any{"close": 0.0, "other": 0.0}, map[string]Validity{"div": NonFinite, "log": NonFinite, "sqrt": Valid, "positive": NonFinite}},
		{map[string]any{"close": -4.0, "other": 0.5}, map[string]Validity{"log": NonFinite, "sqrt": NonFinite, "pow": NonFinite, "positive": NonFinite}},
		{map[string]any{"close": math.MaxFloat64, "other": math.MaxFloat64}, map[string]Validity{"add": NonFinite, "pow": NonFinite}},
		{map[string]any{"close": 1.0, "other": 1e-300}, map[string]Validity{"div": Valid}},
	}
	snapshots := make([]*Snapshot, len(cases))
	for i, test := range cases {
		snapshots[i] = testSnapshot(t, int64(i+1)*1000, map[int32]map[string]any{1: test.fields})
	}
	batch, err := plan.Batch(snapshots, len(snapshots))
	if err != nil {
		t.Fatal(err)
	}
	session, _ := NewSession(plan)
	for i, snapshot := range snapshots {
		frame, err := session.Evaluate(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		for name, validity := range cases[i].want {
			value := batch[i].Values[name][1]
			if value.Validity != validity {
				t.Fatalf("case %d/%s: want %s, got %v", i, name, validity, value)
			}
			if validity != Valid && !math.IsNaN(value.Value) {
				t.Fatalf("case %d/%s: invalid result should be NaN", i, name)
			}
		}
		for name, values := range frame.Values {
			compareNumeric(t, batch[i].Values[name][1], values[1], fmt.Sprintf("case %d/%s", i, name))
		}
	}
}

func TestConstantIdentityBroadcastAndPointwiseValidation(t *testing.T) {
	plan, err := New().Add("first", Constant(3, "1h")).Add("second", Constant(3, "1h")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if plan.NodeCount() != 1 || len(plan.Inputs()) != 0 || plan.WarmupLength() != 0 {
		t.Fatal("constant identity, subscriptions or warmup incorrect")
	}
	// A trading snapshot still carries the execution-price barrier, even when
	// the factor itself has no source subscriptions.
	snapshot := testSnapshot(t, 1000, map[int32]map[string]any{1: {}, 2: {}})
	session, _ := NewSession(plan)
	frame, err := session.Evaluate(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := plan.Batch([]*Snapshot{snapshot}, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, sid := range []int32{1, 2} {
		compareNumeric(t, Numeric{3, Valid}, frame.Values["first"][sid], "constant session broadcast")
		compareNumeric(t, Numeric{3, Valid}, batch[0].Values["first"][sid], "constant batch broadcast")
	}
	changed, _ := New().Add("first", Constant(4, "1h")).Add("second", Constant(3, "1h")).Compile()
	if plan.Hash() == changed.Hash() {
		t.Fatal("constant value omitted from identity")
	}
	x := Field("prices", "close", "1h")
	badArity := Add(x, x)
	badArity.Inputs = []*Node{x}
	badConstant := Constant(1, "1h")
	delete(badConstant.Spec.Parameters, "value")
	for name, root := range map[string]*Node{
		"arity": badArity, "constant-value": badConstant,
		"nan": Constant(math.NaN(), "1h"), "inf": Constant(math.Inf(1), "1h"),
		"frequency": Add(x, Constant(1, "1d")), "nil": Neg(nil),
		"period": StdDev(x, 0, 0), "ddof-negative": StdDev(x, 3, -1), "ddof-period": StdDev(x, 3, 3),
	} {
		if _, err := New().Add(name, root).Compile(); err == nil {
			t.Fatalf("accepted invalid %s", name)
		}
	}
	for parameter, value := range map[string]float64{"period": 1.5, "ddof": 0.5} {
		root := StdDev(x, 3, 0)
		root.Spec.Parameters[parameter] = value
		if _, err := New().Add("std", root).Compile(); err == nil {
			t.Fatalf("accepted fractional %s", parameter)
		}
	}
}
