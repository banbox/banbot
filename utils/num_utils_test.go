package utils

import (
	"fmt"
	"reflect"
	"testing"
)

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
