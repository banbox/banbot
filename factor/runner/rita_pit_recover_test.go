package runner

import (
	"context"
	"math"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
)

// rita round4: like pitConfig but each sid may have a hole [holeFrom, holeTo] (inclusive bars) with no data at all.
func ritaHoleConfig(t *testing.T, sids []int32, holes map[int32][2]int64, last map[int32]int64, bars int64) Config {
	t.Helper()
	c := pitConfig(t, map[int32]int64{1: 0, 2: 0, 3: 0}, 1, 0) // reuse base wiring (store replaced below)
	c.Snapshot.Universe = factor.Universe{Version: "rita", Static: true, Investable: sids, Tradable: sids, Evaluation: sids, Tracked: sids}
	m := map[int32]string{}
	for _, s := range sids {
		m[s] = string(rune('A' + s - 1))
	}
	c.Snapshot.SIDMap = m
	store, err := factor.NewVersionStore(400)
	if err != nil {
		t.Fatal(err)
	}
	const hour int64 = 3600000
	price := map[int32]float64{1: 200, 2: 100, 3: 300, 4: 150}
	for _, sid := range sids {
		for bar := int64(1); bar <= bars; bar++ {
			if l, ok := last[sid]; ok && bar > l {
				break
			}
			if h, ok := holes[sid]; ok && bar >= h[0] && bar <= h[1] {
				continue
			}
			at := bar * hour
			if err = store.Put(factor.VersionRecord{Series: orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, TimeMS: at - hour, EndMS: at, Closed: true, Values: map[string]any{"close": price[sid]}}, EventTime: at, AvailableAt: at, IngestedAt: at, Revision: 1, SourceVersion: "v1"}); err != nil {
				t.Fatal(err)
			}
			if err = store.Put(factor.VersionRecord{Series: orm.DataSeries{Source: "tick", TimeFrame: "event", Sid: sid, TimeMS: at + 1, EndMS: at + 1, Closed: true, Values: map[string]any{"price": price[sid]}}, EventTime: at + 1, AvailableAt: at + 1, IngestedAt: at + 1, Revision: 1, SourceVersion: "v1"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	path := filepath.Join(t.TempDir(), "rita.gob")
	if _, err = store.Export(path); err != nil {
		t.Fatal(err)
	}
	c.Chunks = []Chunk{{Path: path, From: hour, To: bars*hour + 2}}
	_ = research.ComboSpec{}
	return c
}

// A held instrument vanishes for bars 6..9 and then comes back: it is closed once (cost only), re-enters after the
// hole, never double-held, no NAV jump, no Incomplete.
func TestRitaPITVanishThenRecover(t *testing.T) {
	const bars = 16
	c := ritaHoleConfig(t, []int32{1, 2, 3}, map[int32][2]int64{3: {6, 9}}, nil, bars)
	out := &navCapture{capture: capture{targets: map[int64]map[int32]float64{}}}
	res, err := Run(context.Background(), c, nil, out)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("decisions=%d incomplete=%d drops=%d dropDecisions=%d exits=%d nav=%v qty=%v", res.Decisions, res.Incomplete, res.UniverseDrops, res.UniverseDropDecisions, res.DelistExits, res.Book.NAV, res.Book.Quantities)
	for at, w := range out.targets {
		t.Logf("t=%d w=%v", at/3600000, w)
	}
	if res.Incomplete != 0 {
		t.Fatalf("incomplete=%d", res.Incomplete)
	}
	if res.DelistExits != 1 {
		t.Fatalf("exits=%d (want exactly one close at the hole)", res.DelistExits)
	}
	if q := res.Book.Quantities[3]; q <= 0 {
		t.Fatalf("instrument 3 did not re-enter after recovery: qty=%v", q)
	}
	for i := 1; i < len(out.navs); i++ {
		if math.Abs(out.navs[i]/out.navs[i-1]-1) > 0.02 {
			t.Fatalf("NAV jump %v -> %v", out.navs[i-1], out.navs[i])
		}
	}
	// gross exposure sanity: sum |qty x price| must not exceed NAV (no double opening)
	gross := 0.0
	for sid, q := range res.Book.Quantities {
		gross += math.Abs(q) * map[int32]float64{1: 200, 2: 100, 3: 300}[sid]
	}
	if gross > res.Book.NAV*1.05 {
		t.Fatalf("gross %v > NAV %v: duplicate positions?", gross, res.Book.NAV)
	}
}

// 2-name universe: one dies -> PIT refuses (need >= 2 names), decisions are skipped, Incomplete counted
// (documents that a single-pair statarb never benefits from the 067 reduction).
func TestRitaPITTwoNameUniverseNeverReduced(t *testing.T) {
	const bars = 14
	c := ritaHoleConfig(t, []int32{1, 3}, nil, map[int32]int64{3: 6}, bars)
	res, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("incomplete=%d drops=%d decisions=%d", res.Incomplete, res.UniverseDrops, res.Decisions)
	if res.UniverseDrops != 0 || res.Incomplete == 0 {
		t.Fatalf("unexpected: incomplete=%d drops=%d", res.Incomplete, res.UniverseDrops)
	}
}
