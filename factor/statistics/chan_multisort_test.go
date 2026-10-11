package statistics

import (
	"reflect"
	"testing"
)

func TestChanImpliedMomentsAndConditionalMultisort(t *testing.T) {
	v, s, k, err := ImpliedMoments(.2, .4, .5, .3)
	if err != nil || v != .30000000000000004 || s != .2 || k < .19 || k > .21 {
		t.Fatal("moment equations")
	}
	rows := [][]float64{{0, 100}, {1, -100}, {2, 20}, {3, 10}, {4, 0}, {5, 30}, {6, 40}, {7, 50}}
	long, short, err := SequentialQuantileTails(rows, .5)
	if err != nil || !reflect.DeepEqual(long, []int{6, 7}) || !reflect.DeepEqual(short, []int{1, 3}) {
		t.Fatalf("sequential versus independent ranks %v %v %v", long, short, err)
	}
}
