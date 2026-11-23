package com

import "testing"

func TestPairCopiedStateTouch(t *testing.T) {
	var nilState *PairCopiedState
	nilState.Touch(5) // must not panic
	if nilState.LastCopiedMs() != 0 {
		t.Fatal("nil state must read 0")
	}

	s := NewPairCopiedState()
	if s.LastCopiedMs() != 0 {
		t.Fatal("new state must start at 0")
	}
	s.Touch(1000)
	if got := s.LastCopiedMs(); got != 1000 {
		t.Fatalf("after Touch(1000) = %d", got)
	}
	s.Touch(500) // monotonic: never moves backwards
	if got := s.LastCopiedMs(); got != 1000 {
		t.Fatalf("Touch(500) lowered LastCopiedMs to %d", got)
	}
	s.Touch(2000)
	if got := s.LastCopiedMs(); got != 2000 {
		t.Fatalf("after Touch(2000) = %d", got)
	}
	if len(s.GetPairCopieds()) != 0 || s.LastBarMs() != 0 {
		t.Fatalf("Touch must not add a pair or bar: %v bar=%d", s.GetPairCopieds(), s.LastBarMs())
	}
	// Real data still advances and is not undone by an older Touch.
	s.SetPairMsAt(3000, "BTC/USDT", 2500, 60000)
	s.Touch(2999)
	if got := s.LastCopiedMs(); got != 3000 {
		t.Fatalf("LastCopiedMs = %d, want 3000", got)
	}
}
