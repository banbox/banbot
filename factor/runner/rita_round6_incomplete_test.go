package runner

import (
	"context"
	"testing"
)

// rita round6 (3): an instrument that has market data and then loses 1/3 of the run (data ends at bar 16 of 24).
//   - lifecycle policy: NOT dropped (081 NeverSeenAll=false) -> the decisions stay skipped -> Incomplete counts them,
//     so bansite skipped_ratio = Incomplete/(Decisions+Incomplete) flags it;
//   - weights mode: PIT (067) drops it (UniverseDrops), Incomplete stays 0 (by design, reported via universe_drops /
//     universe_avg_ratio / delist_exits).
func TestRitaMidRunMissingInstrument(t *testing.T) {
	const bars = 24
	pol := policyPITConfig(t, 0)
	pol = pitConfigFrom(t, nil, map[int32]int64{1: bars, 2: bars, 3: 16}, bars, 0)
	pol.Manifest.Portfolio.Policy = policyPITConfig(t, 0).Manifest.Portfolio.Policy
	pol.Manifest.Portfolio.LongNotional = 1
	pol.Manifest.Portfolio.ShortNotional = 0
	pol.Manifest.Portfolio.Rebalance = policyPITConfig(t, 0).Manifest.Portfolio.Rebalance
	pol.Manifest.Portfolio.Transition = policyPITConfig(t, 0).Manifest.Portfolio.Transition
	pol.Manifest.Portfolio.Holding = policyPITConfig(t, 0).Manifest.Portfolio.Holding
	r, err := Run(context.Background(), pol, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ratio := float64(r.Incomplete) / float64(max(r.Decisions+r.Incomplete, 1))
	t.Logf("policy: decisions=%d incomplete=%d drops=%d exits=%d skipped_ratio=%.2f", r.Decisions, r.Incomplete, r.UniverseDrops, r.DelistExits, ratio)
	if r.Incomplete == 0 || ratio <= 0.2 {
		t.Fatalf("policy mode must flag a 1/3 missing instrument: %.2f", ratio)
	}
	w := pitConfigFrom(t, nil, map[int32]int64{1: bars, 2: bars, 3: 16}, bars, 0)
	rw, err := Run(context.Background(), w, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("weights: decisions=%d incomplete=%d drops=%d exits=%d", rw.Decisions, rw.Incomplete, rw.UniverseDrops, rw.DelistExits)
}
