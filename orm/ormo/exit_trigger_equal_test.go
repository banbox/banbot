package ormo

import "testing"

func TestExitTriggerEqualComparesRateWithRate(t *testing.T) {
	a := &ExitTrigger{Price: 1, Limit: 0, Rate: 0.5}
	if !a.Equal(&ExitTrigger{Price: 1, Limit: 0, Rate: 0.5}) {
		t.Fatal("identical triggers must be equal")
	}
	if a.Equal(&ExitTrigger{Price: 1, Limit: 0, Rate: 0.4}) {
		t.Fatal("different rates must not be equal")
	}
	b := &ExitTrigger{Price: 1, Limit: 0.5, Rate: 0}
	if b.Equal(&ExitTrigger{Price: 1, Limit: 0.5, Rate: 0.5}) {
		t.Fatal("rate must not be compared with limit")
	}
	var n *ExitTrigger
	if !n.Equal(nil) || n.Equal(a) {
		t.Fatal("nil semantics")
	}
}
