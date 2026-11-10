package runner

import (
	"context"
	"testing"
)

// 070: a single pair (2 legs) whose second leg's data ends: with the pairs-mode PIT the pair is closed and paused,
// the period is NOT incomplete (the old behaviour skipped every period and froze the position), and the pause
// is counted in PausedPeriods.
func TestPairsModeSinglePairLegDiesPausesInsteadOfIncomplete(t *testing.T) {
	const bars = 14
	c := ritaHoleConfig(t, []int32{1, 3}, nil, map[int32]int64{3: 6}, bars)
	c.PITPairs = [][2]int32{{1, 3}}
	res, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("incomplete=%d paused=%d drops=%d decisions=%d exits=%d", res.Incomplete, res.PausedPeriods, res.UniverseDrops, res.Decisions, res.DelistExits)
	if res.Incomplete != 0 {
		t.Fatalf("incomplete=%d, want 0 (pair-level PIT)", res.Incomplete)
	}
	if res.PausedPeriods == 0 || res.UniverseDrops == 0 {
		t.Fatalf("paused=%d drops=%d: the pair must be paused and the dead leg dropped", res.PausedPeriods, res.UniverseDrops)
	}
}

// 070: both legs of the only pair end: nothing left to decide, every later period is paused (not incomplete) and
// positions are closed once.
func TestPairsModeBothLegsDeadIsPausedNotIncomplete(t *testing.T) {
	const bars = 14
	c := ritaHoleConfig(t, []int32{1, 3}, nil, map[int32]int64{1: 6, 3: 6}, bars)
	c.PITPairs = [][2]int32{{1, 3}}
	res, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("incomplete=%d paused=%d drops=%d decisions=%d", res.Incomplete, res.PausedPeriods, res.UniverseDrops, res.Decisions)
	if res.Incomplete != 0 || res.PausedPeriods == 0 {
		t.Fatalf("incomplete=%d paused=%d", res.Incomplete, res.PausedPeriods)
	}
}
