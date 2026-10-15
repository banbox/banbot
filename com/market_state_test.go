package com

import "testing"

func TestPairCopiedStateDeleteRefreshesLastBarMs(t *testing.T) {
	state := NewPairCopiedState()
	state.SetPairMsAt(100, "older", 100, 60)
	state.SetPairMsAt(200, "newer", 200, 60)

	state.DelPairCopieds("older")
	if got := state.LastBarMs(); got != 200 {
		t.Fatalf("last bar ms after deleting non-maximum = %d, want 200", got)
	}

	state.DelPairCopieds("newer")
	if got := state.LastBarMs(); got != 0 {
		t.Fatalf("last bar ms after deleting maximum = %d, want 0", got)
	}
}
