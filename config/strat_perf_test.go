package config

import "testing"

func TestStratPerfDefaultsConsistentAcrossPaths(t *testing.T) {
	zero := &StratPerfConfig{}
	zero.Validate()
	want := StratPerfConfig{MinOdNum: 5, MaxOdNum: 30, MinJobNum: 10, MidWeight: 0.3, BadWeight: 0.15}
	if *zero != want {
		t.Fatalf("zero defaults = %+v, want %+v", *zero, want)
	}
	low := &StratPerfConfig{MinOdNum: 1, MaxOdNum: 2, MinJobNum: 3, MidWeight: 0.2, BadWeight: 0.1}
	low.Validate()
	if low.MinOdNum != 5 || low.MaxOdNum != 8 || low.MinJobNum != 7 || low.MidWeight != 0.2 || low.BadWeight != 0.1 {
		t.Fatalf("floors not applied: %+v", *low)
	}
}

func TestParsePairsUnsupportedMarketMessage(t *testing.T) {
	_, err := parsePairs("binance", "option", []string{"USDT"}, "BTC")
	if err == nil {
		t.Fatal("expected error for short pair on option market")
	}
	if msg := err.Short(); !containsAll(msg, "BTC", "option") {
		t.Fatalf("error message should name the pair and market, got %q", msg)
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		found := false
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
