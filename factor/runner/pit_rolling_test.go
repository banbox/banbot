package runner

import (
	"context"
	"testing"

	"github.com/banbox/banbot/factor"
)

func keyOf(sid int32) factor.StreamKey {
	return factor.StreamKey{SID: sid, Source: "kline", TimeFrame: "1h"}
}

// The ratio gate compares survivors with the survivors of the last accepted decision (alive), not the nominal pool.
func TestPITReduceUniverseRollingDenominator(t *testing.T) {
	ids := []int32{1, 2, 3, 4, 5, 6}
	u := factor.Universe{Version: "v", Investable: ids, Reference: ids, Tradable: ids, Evaluation: ids, Tracked: ids}
	status := func(live ...int32) factor.SnapshotStatus {
		st := factor.SnapshotStatus{}
		for _, sid := range ids {
			st.Expected = append(st.Expected, keyOf(sid))
		}
		for _, sid := range live {
			st.Arrived = append(st.Arrived, keyOf(sid))
		}
		return st
	}
	// 2 of 6 left: refused against the nominal pool (2 < 3) ...
	if _, _, ok := pitReduceUniverse(u, status(1, 2), 0, 0); ok {
		t.Fatal("2 of 6 must be refused when alive is the nominal pool")
	}
	// ... accepted when the last accepted decision had 3 survivors (2 >= 1.5)
	if reduced, dropped, ok := pitReduceUniverse(u, status(1, 2), 0, 3); !ok || len(reduced.Investable) != 2 || len(dropped) != 4 {
		t.Fatalf("gradual shrink refused: %v %v %v", reduced.Investable, dropped, ok)
	}
	// a one-round mass loss against alive=6 stays refused; alive out of range falls back to the nominal count
	if _, _, ok := pitReduceUniverse(u, status(1, 2, 3), 0, 99); !ok {
		t.Fatal("3 of 6 meets 0.5 x 6")
	}
	if _, _, ok := pitReduceUniverse(u, status(1, 2), 0.5, 6); ok {
		t.Fatal("mass loss accepted")
	}
}

// Names retire one after another until fewer than half of the nominal pool is left: every decision is still taken
// and the dead positions are closed (before: Incomplete for the rest of the run, dead holdings kept in NAV).
func TestPITGradualDelistingBelowHalfKeepsDeciding(t *testing.T) {
	const bars = 30
	last := map[int32]int64{6: 6, 5: 8, 4: 10, 3: 12} // sid 3 (top score, held) dies last; sids 1, 2 survive
	c := samePools(ritaHoleConfig(t, []int32{1, 2, 3, 4, 5, 6}, nil, last, bars))
	res, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Incomplete != 0 || res.Decisions < 25 || res.DelistExits == 0 {
		t.Fatalf("incomplete=%d decisions=%d exits=%d", res.Incomplete, res.Decisions, res.DelistExits)
	}
	for sid := range last {
		if res.Book.Quantities[sid] != 0 {
			t.Fatalf("dead instrument %d still held: %v", sid, res.Book.Quantities)
		}
	}
	if res.Book.Quantities[1] <= 0 {
		t.Fatalf("surviving top name not held: %v", res.Book.Quantities)
	}
}

// One round loses 4 of 6 names (a data outage, then recovery): refused and counted, no drops; normal afterwards.
func TestPITMassLossWithinOneRoundIsRefusedThenRecovers(t *testing.T) {
	const bars = 24
	holes := map[int32][2]int64{3: {8, 9}, 4: {8, 9}, 5: {8, 9}, 6: {8, 9}}
	c := samePools(ritaHoleConfig(t, []int32{1, 2, 3, 4, 5, 6}, holes, nil, bars))
	res, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Incomplete == 0 || res.Incomplete > 4 || res.UniverseDrops != 0 || res.Decisions < 15 {
		t.Fatalf("incomplete=%d drops=%d decisions=%d", res.Incomplete, res.UniverseDrops, res.Decisions)
	}
	if res.Book.Quantities[3] <= 0 {
		t.Fatalf("not trading normally after the outage: %v", res.Book.Quantities)
	}
}

// Audit scenario: a 3-name pool loses two names one after another. The survivor count falls below 2: the dead
// positions are closed and the periods are paused (not Incomplete).
func TestPITThreeNamePoolTwoDelistingsClosesDeadPositions(t *testing.T) {
	const bars = 40
	c := samePools(pitConfigFn(t, presence(nil, map[int32]int64{3: 12, 2: 20}, nil), bars, 0))
	res, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Incomplete != 0 || res.PausedPeriods == 0 || res.Book.Quantities[3] != 0 || res.Book.Quantities[2] != 0 {
		t.Fatalf("incomplete=%d paused=%d q=%v", res.Incomplete, res.PausedPeriods, res.Book.Quantities)
	}
	// the same under the lifecycle policy
	p := policyDelistConfig(t, presence(nil, map[int32]int64{3: 12, 2: 20}, nil), bars, true)
	res, err = Run(context.Background(), p, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Incomplete != 0 || res.Book.Quantities[3] != 0 || res.Book.Quantities[2] != 0 {
		t.Fatalf("policy: incomplete=%d q=%v", res.Incomplete, res.Book.Quantities)
	}
}
