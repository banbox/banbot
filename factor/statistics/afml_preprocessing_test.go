package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestExpandingFracdiffSourceSkipMissingAndDrift(t *testing.T) {
	x := []float64{100, 100, 100, 100}
	got, skip, err := ExpandingFractionalDifference(x, .5, 1, 10)
	if err != nil || skip != 0 || !reflect.DeepEqual(got, []float64{100, 50, 37.5, 31.25}) {
		t.Fatalf("source expanding drift: %v skip%d %v", got, skip, err)
	}
	got, skip, err = ExpandingFractionalDifference(x, 1, .5, 10)
	if err != nil || skip != 1 || !math.IsNaN(got[0]) || got[1] != 0 || got[3] != 0 {
		t.Fatalf("strict source warmup: %v skip%d %v", got, skip, err)
	}
	got, skip, err = ExpandingFractionalDifference(x, 1, .01, 10)
	if err != nil || skip != 2 || !math.IsNaN(got[1]) || got[2] != 0 {
		t.Fatal("source default skip convention changed")
	}
	got, _, err = ExpandingFractionalDifference([]float64{math.NaN(), 10, math.NaN(), 13}, 1, 1, 10)
	if err != nil || !math.IsNaN(got[0]) || got[1] != 10 || !math.IsNaN(got[2]) || got[3] != 3 {
		t.Fatalf("missing semantics: %v %v", got, err)
	}
	if _, _, err = ExpandingFractionalDifference(x, .5, 1, 9); err == nil {
		t.Fatal("quadratic work budget ignored")
	}
	if _, _, err = ExpandingFractionalDifference([]float64{1, math.Inf(1)}, .5, 1, 3); err == nil {
		t.Fatal("infinite input accepted")
	}
	if _, _, err = ExpandingFractionalDifference(x, math.MaxFloat64, 1, 10); err == nil {
		t.Fatal("weight overflow accepted")
	}
}

func TestDropRareLabelsRecursiveAndTwoClassStop(t *testing.T) {
	labels := []int{3, 4, 4, 2, 2, 2, 1, 1, 1, 1}
	before := append([]int(nil), labels...)
	kept, dropped, err := DropRareLabels(labels, .25)
	if err != nil || !reflect.DeepEqual(kept, []int{3, 4, 5, 6, 7, 8, 9}) || !reflect.DeepEqual(dropped, []int{3, 4}) || !reflect.DeepEqual(labels, before) {
		t.Fatalf("recursive source dropping: %v %v %v", kept, dropped, err)
	}
	kept, dropped, err = DropRareLabels([]int{-1, 0, 1, 1}, .25)
	if err != nil || !reflect.DeepEqual(kept, []int{1, 2, 3}) || !reflect.DeepEqual(dropped, []int{-1}) {
		t.Fatal("threshold equality or first-appearance tie changed")
	}
	kept, dropped, err = DropRareLabels([]int{0, 1, 1, 1, 1}, .5)
	if err != nil || len(kept) != 5 || len(dropped) != 0 {
		t.Fatal("two-class termination ignored")
	}
}
