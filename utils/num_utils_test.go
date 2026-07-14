package utils

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  float64
	}{
		{name: "int", value: int(-1), want: -1},
		{name: "int8", value: int8(-2), want: -2},
		{name: "int16", value: int16(-3), want: -3},
		{name: "int32", value: int32(-4), want: -4},
		{name: "int64", value: int64(-5), want: -5},
		{name: "uint", value: uint(1), want: 1},
		{name: "uint8", value: uint8(2), want: 2},
		{name: "uint16", value: uint16(3), want: 3},
		{name: "uint32", value: uint32(4), want: 4},
		{name: "uint64", value: uint64(5), want: 5},
		{name: "float32", value: float32(1.25), want: 1.25},
		{name: "float64", value: 2.5, want: 2.5},
		{name: "string", value: "3.75", want: 3.75},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ToFloat64(test.value)
			if err != nil {
				t.Fatalf("ToFloat64(%T) returned error: %v", test.value, err)
			}
			if got != test.want {
				t.Fatalf("ToFloat64(%T) = %v, want %v", test.value, got, test.want)
			}
		})
	}
}

func TestToFloat64RejectsInvalidValues(t *testing.T) {
	if _, err := ToFloat64("not-a-number"); err == nil {
		t.Fatal("expected invalid numeric string to return an error")
	}
	if _, err := ToFloat64(true); err == nil || !strings.Contains(err.Error(), "unsupported float type bool") {
		t.Fatalf("expected unsupported type error, got %v", err)
	}
}

func TestConvertFloat64KeepsLegacyUnsupportedDefaults(t *testing.T) {
	if got := ConvertFloat64(uint(7)); got != 0 {
		t.Fatalf("ConvertFloat64(uint(7)) = %v, want legacy default 0", got)
	}
	if got := ConvertFloat64("7"); got != 0 {
		t.Fatalf("ConvertFloat64(\"7\") = %v, want legacy default 0", got)
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  int64
	}{
		{name: "int", value: int(-1), want: -1},
		{name: "int8", value: int8(-2), want: -2},
		{name: "int16", value: int16(-3), want: -3},
		{name: "int32", value: int32(-4), want: -4},
		{name: "int64", value: int64(-5), want: -5},
		{name: "uint", value: uint(1), want: 1},
		{name: "uint8", value: uint8(2), want: 2},
		{name: "uint16", value: uint16(3), want: 3},
		{name: "uint32", value: uint32(4), want: 4},
		{name: "uint64", value: uint64(5), want: 5},
		{name: "float32", value: float32(1.75), want: 1},
		{name: "float64", value: 2.75, want: 2},
		{name: "string", value: "3", want: 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ToInt64(test.value)
			if err != nil {
				t.Fatalf("ToInt64(%T) returned error: %v", test.value, err)
			}
			if got != test.want {
				t.Fatalf("ToInt64(%T) = %v, want %v", test.value, got, test.want)
			}
		})
	}
}

func TestToInt64RejectsInvalidValues(t *testing.T) {
	if _, err := ToInt64("1.5"); err == nil {
		t.Fatal("expected non-integer string to return an error")
	}
	if _, err := ToInt64(true); err == nil || !strings.Contains(err.Error(), "unsupported int type bool") {
		t.Fatalf("expected unsupported type error, got %v", err)
	}
}

func TestConvertInt64KeepsLegacyUnsupportedDefaults(t *testing.T) {
	if got := ConvertInt64(uint(7)); got != 0 {
		t.Fatalf("ConvertInt64(uint(7)) = %v, want legacy default 0", got)
	}
	if got := ConvertInt64("7"); got != 0 {
		t.Fatalf("ConvertInt64(\"7\") = %v, want legacy default 0", got)
	}
}

func TestNumSign(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  int
	}{
		{name: "negative int", value: -1, want: -1},
		{name: "zero int", value: 0, want: 0},
		{name: "positive int", value: 1, want: 1},
		{name: "negative float32", value: float32(-1.5), want: -1},
		{name: "positive float64", value: 1.5, want: 1},
		{name: "nan", value: math.NaN(), want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := NumSign(test.value); got != test.want {
				t.Fatalf("NumSign(%v) = %d, want %d", test.value, got, test.want)
			}
		})
	}
}

func TestNumSignRejectsUnsupportedTypes(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected unsupported type to panic")
		}
	}()
	NumSign("1")
}

func TestArgSortDesc(t *testing.T) {
	arr := []float64{5, 3, 6, 8, 1, 9}
	res := ArgSortDesc(arr)
	fmt.Printf("%v", res)
}

func TestKMeansValsIsDeterministic(t *testing.T) {
	vals := []float64{
		-0.1583, -0.1440, -0.1012, -0.0954, -0.0784, -0.0728,
		-0.0596, -0.0460, -0.0352, -0.0221, -0.0109, -0.0002,
		0.0044, 0.0134, 0.0235, 0.0349, 0.0453, 0.0634,
		0.1025, 0.1452, 0.1900, 0.3652, 3.5202,
	}
	first := KMeansVals(vals, 6)
	if first == nil {
		t.Fatalf("expected clustering result")
	}
	for i := 0; i < 8; i++ {
		cur := KMeansVals(vals, 6)
		if cur == nil {
			t.Fatalf("expected clustering result on run %d", i)
		}
		if !reflect.DeepEqual(first, cur) {
			t.Fatalf("expected deterministic clustering, run %d differs\nfirst=%+v\ncur=%+v", i, first, cur)
		}
	}
}

func TestKMeansValsRetainsAllItems(t *testing.T) {
	vals := []float64{-3, -2, -1, 0, 1, 2, 3}
	res := KMeansVals(vals, 3)
	if res == nil {
		t.Fatalf("expected clustering result")
	}
	if len(res.RowGIds) != len(vals) {
		t.Fatalf("expected %d row ids, got %d", len(vals), len(res.RowGIds))
	}
	total := 0
	for _, cluster := range res.Clusters {
		total += len(cluster.Items)
	}
	if total != len(vals) {
		t.Fatalf("expected all values to be assigned, got %d/%d", total, len(vals))
	}
}
