package ormo

import "testing"

// An order that never entered (all enter timestamps 0) must not pin the range start to 0,
// otherwise the report fails with "order equity metrics require valid millisecond timestamps".
func TestCalcTimeRangeIgnoresUnenteredOrder(t *testing.T) {
	odList := []*InOutOrder{
		{IOrder: &IOrder{EnterAt: 0, ExitAt: 0}},
		{IOrder: &IOrder{EnterAt: 1700000000000, ExitAt: 1700003600000}},
		{IOrder: &IOrder{EnterAt: 1690000000000, ExitAt: 1690003600000}},
	}
	start, end := CalcTimeRange(odList)
	if start != 1690000000000 || end != 1700003600000 {
		t.Fatalf("got %d..%d", start, end)
	}
}

// Enter.UpdateAt stamped after the exit (late entry-order update) must not make start > end.
func TestCalcTimeRangeEnterUpdatedAfterExit(t *testing.T) {
	od := &InOutOrder{IOrder: &IOrder{EnterAt: 1690819200000, ExitAt: 1690855648680},
		Enter: &ExOrder{CreateAt: 1690819200000, UpdateAt: 1690857683000},
		Exit:  &ExOrder{CreateAt: 1690855648680, UpdateAt: 1690855648680}}
	start, end := CalcTimeRange([]*InOutOrder{od})
	if start > end || end != 1690855648680 {
		t.Fatalf("got %d..%d", start, end)
	}
}
