package execution

import (
	"math"
	"reflect"
	"testing"
)

func TestQuantityStepsShrinkAndPrecision(t *testing.T) {
	for _, tc := range []struct {
		quantity, step string
		want           int64
	}{{"0.3", "0.1", 3}, {"0.299999999999999999", "0.1", 2}, {"-0.299999999999999999", "0.1", -2}, {"0.009", "0.01", 0}, {"1", "0.03", 33}, {"9223372036854775807", "1", math.MaxInt64}, {"-9223372036854775808", "1", math.MinInt64}} {
		got, err := QuantitySteps(intentPrice(tc.quantity), intentPrice(tc.step))
		if err != nil || got != tc.want {
			t.Fatalf("%s/%s: %d %v", tc.quantity, tc.step, got, err)
		}
	}
	for _, tc := range [][2]string{{"1", "0"}, {"1", "-1"}, {"9223372036854775808", "1"}, {"-9223372036854775809", "1"}} {
		if _, err := QuantitySteps(intentPrice(tc[0]), intentPrice(tc[1])); err == nil {
			t.Fatal("invalid precision/overflow accepted")
		}
	}
}

func TestAllocateStepsDeterministicAndConserved(t *testing.T) {
	for n := 0; n < 50; n++ {
		got, err := AllocateSteps(4, map[string]int64{"c": 3, "b": 3, "a": 3})
		if err != nil || !reflect.DeepEqual(got, map[string]int64{"a": 2, "b": 1, "c": 1}) {
			t.Fatal(got, err)
		}
	}
	for total := int64(0); total <= 13; total++ {
		demands := map[string]int64{"a": 1, "b": 5, "c": 7}
		got, err := AllocateSteps(total, demands)
		if err != nil {
			t.Fatal(err)
		}
		var sum int64
		for id, n := range got {
			if n < 0 || n > demands[id] {
				t.Fatal("demand exceeded")
			}
			sum += n
		}
		if sum != total {
			t.Fatalf("allocation %d != fill %d", sum, total)
		}
	}
	got, err := AllocateSteps(math.MaxInt64, map[string]int64{"a": math.MaxInt64, "b": math.MaxInt64})
	if err != nil || got["a"] != 4611686018427387904 || got["b"] != 4611686018427387903 {
		t.Fatal("overflow allocation", got, err)
	}
	if got, err := AllocateSteps(0, nil); err != nil || len(got) != 0 {
		t.Fatal(got, err)
	}
	for _, demands := range []map[string]int64{{"": 1}, {"a": 0}, {"a": -1}, nil, {"a": 1}} {
		if _, err := AllocateSteps(2, demands); err == nil {
			t.Fatal("invalid allocation accepted", demands)
		}
	}
	if _, err := AllocateSteps(-1, map[string]int64{"a": 1}); err == nil {
		t.Fatal("negative fill accepted")
	}
}
