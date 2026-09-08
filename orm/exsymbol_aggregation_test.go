package orm

import "testing"

func TestNumericAggregatesPreserveExplicitNullSemantics(t *testing.T) {
	field := SeriesField{Name: "value", Type: "float"}
	rules := []struct {
		name string
		fn   AggRuleFunc
		want float64
	}{
		{name: "min", fn: aggMin, want: 2},
		{name: "max", fn: aggMax, want: 6},
		{name: "sum", fn: aggSum, want: 8},
		{name: "avg", fn: aggAvg, want: 4},
		{name: "mid", fn: aggMid, want: 4},
	}
	allNull := []*DataRecord{
		{Values: map[string]any{"value": nil}},
		{Values: map[string]any{"value": nil}},
	}
	mixedNull := []*DataRecord{
		{Values: map[string]any{"value": nil}},
		{Values: map[string]any{"value": 2.0}},
		{Values: map[string]any{"value": nil}},
		{Values: map[string]any{"value": 6.0}},
	}

	for _, rule := range rules {
		t.Run(rule.name+"/all-null", func(t *testing.T) {
			got, err := rule.fn(allNull, field)
			if err != nil {
				t.Fatalf("all-NULL aggregate returned error: %v", err)
			}
			if got != nil {
				t.Fatalf("all-NULL aggregate = %#v, want nil", got)
			}
		})
		t.Run(rule.name+"/mixed-null", func(t *testing.T) {
			got, err := rule.fn(mixedNull, field)
			if err != nil {
				t.Fatalf("mixed-NULL aggregate returned error: %v", err)
			}
			if got != rule.want {
				t.Fatalf("mixed-NULL aggregate = %#v, want %#v", got, rule.want)
			}
		})
		t.Run(rule.name+"/missing", func(t *testing.T) {
			_, err := rule.fn([]*DataRecord{{Values: map[string]any{}}}, field)
			if err == nil {
				t.Fatal("missing field unexpectedly aggregated as NULL")
			}
		})
	}
}

func TestEdgeAggregatesPreserveExplicitNulls(t *testing.T) {
	field := SeriesField{Name: "value", Type: "float"}
	first, err := aggFirst([]*DataRecord{
		{Values: map[string]any{"value": nil}},
		{Values: map[string]any{"value": 2.0}},
	}, field)
	if err != nil || first != nil {
		t.Fatalf("first explicit NULL = (%#v, %v), want (nil, nil)", first, err)
	}
	last, err := aggLast([]*DataRecord{
		{Values: map[string]any{"value": 2.0}},
		{Values: map[string]any{"value": nil}},
	}, field)
	if err != nil || last != nil {
		t.Fatalf("last explicit NULL = (%#v, %v), want (nil, nil)", last, err)
	}
}

func TestAvgAndMidUseIntegerFieldResultType(t *testing.T) {
	field := SeriesField{Name: "value", Type: "int"}
	rows := []*DataRecord{
		{Values: map[string]any{"value": int64(2)}},
		{Values: map[string]any{"value": int64(4)}},
	}
	for _, rule := range []struct {
		name string
		fn   AggRuleFunc
	}{
		{name: "avg", fn: aggAvg},
		{name: "mid", fn: aggMid},
	} {
		t.Run(rule.name, func(t *testing.T) {
			got, err := rule.fn(rows, field)
			if err != nil {
				t.Fatalf("%s returned error: %v", rule.name, err)
			}
			value, ok := got.(int64)
			if !ok || value != 3 {
				t.Fatalf("%s = (%#v, %T), want int64(3)", rule.name, got, got)
			}
		})
	}
}
