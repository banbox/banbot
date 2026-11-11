package runner

import (
	"context"
	"testing"

	"github.com/banbox/banbot/factor"
)

func policyDelistConfig(t *testing.T, present func(int32, int64) bool, bars int64, same bool) Config {
	t.Helper()
	c := pitConfigFn(t, present, bars, 0)
	if same {
		c = samePools(c)
	}
	c.Manifest.Portfolio.Policy = "lifecycle-v1"
	c.Manifest.Portfolio.LongNotional = 1
	c.Manifest.Portfolio.ShortNotional = 0
	c.Manifest.Portfolio.Rebalance = &factor.RebalanceConfig{EveryBars: 2}
	c.Manifest.Portfolio.Transition = &factor.TransitionConfig{Mode: "direct"}
	c.Manifest.Portfolio.Holding = &factor.HoldingConfig{MinBars: 2, MaxBars: 8}
	return c
}

// A held instrument (top score, long) delists for good under lifecycle-v1: it is closed at its last mark, the
// policy state forgets it, and decisions continue on the reduced universe (the next-best name is bought).
func TestPolicyPITHeldInstrumentDelists(t *testing.T) {
	const bars = 30
	for _, same := range []bool{false, true} {
		c := policyDelistConfig(t, presence(nil, map[int32]int64{3: 12}, nil), bars, same)
		result, err := Run(context.Background(), c, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if result.Incomplete != 0 || result.DelistExits != 1 || result.UniverseDrops == 0 || result.DroppedBySID[3] != result.UniverseDrops {
			t.Fatalf("same=%v incomplete=%d exits=%d drops=%d by=%v", same, result.Incomplete, result.DelistExits, result.UniverseDrops, result.DroppedBySID)
		}
		if result.Book.Quantities[3] != 0 || result.Book.Quantities[1] <= 0 {
			t.Fatalf("same=%v after the delist the next-best name must be held: %v", same, result.Book.Quantities)
		}
		if result.Decisions < 25 {
			t.Fatalf("same=%v decisions=%d", same, result.Decisions)
		}
	}
}

// Delisted instrument comes back: dropped during the hole, eligible again afterwards, nothing double held.
func TestPolicyPITHeldInstrumentRecovers(t *testing.T) {
	const bars = 40
	c := policyDelistConfig(t, presence(nil, nil, map[int32][2]int64{3: {13, 18}}), bars, true)
	result, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Incomplete != 0 || result.DelistExits != 1 || result.UniverseDrops == 0 {
		t.Fatalf("incomplete=%d exits=%d drops=%d", result.Incomplete, result.DelistExits, result.UniverseDrops)
	}
	if result.Book.Quantities[3] <= 0 || result.Book.Quantities[1] != 0 {
		t.Fatalf("top name must be held again after recovery: %v", result.Book.Quantities)
	}
}

// One-bar hole.
func TestPolicyPITShortGap(t *testing.T) {
	const bars = 30
	c := policyDelistConfig(t, presence(nil, nil, map[int32][2]int64{3: {12, 12}}), bars, true)
	result, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Incomplete != 0 || result.Book.Quantities[3] <= 0 {
		t.Fatalf("incomplete=%d q=%v", result.Incomplete, result.Book.Quantities)
	}
}
