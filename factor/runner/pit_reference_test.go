package runner

import (
	"context"
	"testing"

	"github.com/banbox/banbot/factor"
)

// presence builds a presence function: instrument sid has bars in [first, last] except the bars listed in holes.
func presence(first, last map[int32]int64, holes map[int32][2]int64) func(int32, int64) bool {
	return func(sid int32, bar int64) bool {
		if f := first[sid]; f > 0 && bar < f {
			return false
		}
		if l, ok := last[sid]; ok && bar > l {
			return false
		}
		if h, ok := holes[sid]; ok && bar >= h[0] && bar <= h[1] {
			return false
		}
		return true
	}
}

// production shape: Reference == Investable == Tradable == Evaluation == Tracked.
func samePools(c Config) Config {
	u := c.Snapshot.Universe
	u.Reference = append([]int32(nil), u.Investable...)
	c.Snapshot.Universe = u
	return c
}

func TestPITReduceReferenceEqualsInvestable(t *testing.T) {
	const bars = 14
	u := factor.Universe{Version: "v", Investable: []int32{1, 2, 3}, Reference: []int32{1, 2, 3}, Tradable: []int32{1, 2, 3}, Evaluation: []int32{1, 2, 3}, Tracked: []int32{1, 2, 3}}
	status := factor.SnapshotStatus{
		Expected: []factor.StreamKey{{SID: 1, Source: "kline", TimeFrame: "1h"}, {SID: 3, Source: "kline", TimeFrame: "1h"}},
		Arrived:  []factor.StreamKey{{SID: 1, Source: "kline", TimeFrame: "1h"}},
	}
	reduced, dropped, ok := pitReduceUniverse(u, status, 0, 0)
	if !ok || len(dropped) != 1 || dropped[0] != 3 {
		t.Fatalf("not reduced: ok=%v dropped=%v", ok, dropped)
	}
	for name, pool := range map[string][]int32{"investable": reduced.Investable, "reference": reduced.Reference, "tradable": reduced.Tradable, "evaluation": reduced.Evaluation} {
		if len(pool) != 2 {
			t.Fatalf("%s not reduced: %v", name, pool)
		}
	}
	// a Reference-only instrument (not investable) is never dropped: reduction refused
	u.Reference = []int32{1, 2, 3, 9}
	status.Expected = append(status.Expected, factor.StreamKey{SID: 9, Source: "kline", TimeFrame: "1h"})
	if _, _, ok = pitReduceUniverse(u, status, 0, 0); ok {
		t.Fatal("reference-only instrument must not be dropped")
	}
	if _, _, _, ok = pitReducePairs(u, status, [][2]int32{{1, 3}}); ok {
		t.Fatal("pairs: reference-only instrument must not be dropped")
	}
	_ = bars
}

// Delisted (data ends) with Reference == Investable: dropped, position closed, decisions continue.
func TestPITSamePoolsDelistedInstrument(t *testing.T) {
	const bars = 14
	c := samePools(pitConfigFn(t, presence(nil, map[int32]int64{3: 6}, nil), bars, 0))
	result, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Incomplete != 0 || result.Decisions < 10 || result.UniverseDrops < 5 || result.DelistExits != 1 || result.DroppedBySID[3] != result.UniverseDrops || result.Book.Quantities[3] != 0 {
		t.Fatalf("incomplete=%d decisions=%d drops=%d exits=%d by=%v q=%v", result.Incomplete, result.Decisions, result.UniverseDrops, result.DelistExits, result.DroppedBySID, result.Book.Quantities)
	}
}

// Not yet listed with Reference == Investable.
func TestPITSamePoolsNotYetListed(t *testing.T) {
	const bars = 14
	c := samePools(pitConfigFn(t, presence(map[int32]int64{3: 6}, nil, nil), bars, 0))
	result, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Incomplete != 0 || result.UniverseDrops == 0 || result.DelistExits != 0 || result.DroppedBySID[3] != result.UniverseDrops {
		t.Fatalf("incomplete=%d drops=%d exits=%d by=%v", result.Incomplete, result.UniverseDrops, result.DelistExits, result.DroppedBySID)
	}
}

// A hole that recovers: dropped during the hole, traded again afterwards.
func TestPITSamePoolsGapRecovers(t *testing.T) {
	const bars = 20
	c := samePools(pitConfigFn(t, presence(nil, nil, map[int32][2]int64{3: {6, 9}}), bars, 0))
	result, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Incomplete != 0 || result.UniverseDrops == 0 || result.DelistExits != 1 || result.UniverseDropDecisions >= result.Decisions {
		t.Fatalf("incomplete=%d drops=%d exits=%d dd=%d decisions=%d", result.Incomplete, result.UniverseDrops, result.DelistExits, result.UniverseDropDecisions, result.Decisions)
	}
	if result.Book.Quantities[3] == 0 {
		t.Fatalf("instrument not traded again after recovery: %v", result.Book.Quantities)
	}
}

// PITMinUniverseRatio still gates the reduction with same pools: max(2, ratio x N) names must remain.
func TestPITSamePoolsMinRatio(t *testing.T) {
	const bars = 14
	for _, tc := range []struct {
		ratio   float64
		last    map[int32]int64
		reduced bool
	}{
		{0, map[int32]int64{2: 6, 3: 6}, false}, // 1 of 3 left < floor of 2
		{0.5, map[int32]int64{3: 6}, true},      // 2 of 3 >= 1.5
		{0.8, map[int32]int64{3: 6}, false},     // 2 of 3 < 2.4
		{-1, map[int32]int64{3: 6}, false},      // PIT off
	} {
		c := samePools(pitConfigFn(t, presence(nil, tc.last, nil), bars, tc.ratio))
		result, err := Run(context.Background(), c, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if (result.UniverseDrops > 0) != tc.reduced || (result.Incomplete == 0) != tc.reduced {
			t.Fatalf("ratio %v: incomplete=%d drops=%d", tc.ratio, result.Incomplete, result.UniverseDrops)
		}
	}
}

// Pairs mode with Reference == Investable: a dead leg pauses only its pair, the other pair keeps trading; when
// both pairs lose a leg the period counts as paused, never as incomplete.
func TestPITSamePoolsPairsOneLegDies(t *testing.T) {
	const bars = 14
	c := samePools(ritaHoleConfig(t, []int32{1, 2, 3, 4}, nil, map[int32]int64{3: 6}, bars))
	c.PITPairs = [][2]int32{{1, 2}, {3, 4}}
	res, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Incomplete != 0 || res.UniverseDrops == 0 || res.DroppedBySID[3] != res.UniverseDrops || res.DelistExits != 1 {
		t.Fatalf("incomplete=%d drops=%d by=%v exits=%d", res.Incomplete, res.UniverseDrops, res.DroppedBySID, res.DelistExits)
	}
	if res.PausedPeriods != 0 {
		t.Fatalf("pair {1,2} is alive, no period may be fully paused: %d", res.PausedPeriods)
	}
	if res.Decisions < 10 || res.Book.Quantities[3] != 0 || res.Book.Quantities[1] == 0 {
		t.Fatalf("surviving pair stopped trading: decisions=%d q=%v", res.Decisions, res.Book.Quantities)
	}
}

func TestPITSamePoolsPairsAllPaired(t *testing.T) {
	const bars = 14
	c := samePools(ritaHoleConfig(t, []int32{1, 2, 3, 4}, nil, map[int32]int64{2: 6, 4: 6}, bars))
	c.PITPairs = [][2]int32{{1, 2}, {3, 4}}
	res, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Incomplete != 0 || res.PausedPeriods == 0 || res.UniverseDrops == 0 {
		t.Fatalf("incomplete=%d paused=%d drops=%d", res.Incomplete, res.PausedPeriods, res.UniverseDrops)
	}
}
