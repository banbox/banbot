package runner

import (
	"context"
	"math"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
)

type navCapture struct {
	capture
	navs []float64
}

func (o *navCapture) Nav(s backtest.State, _ int64) error { o.navs = append(o.navs, s.NAV); return nil }

func pitConfig(t *testing.T, lastBar map[int32]int64, bars int64, minRatio float64) Config {
	t.Helper()
	return pitConfigFrom(t, nil, lastBar, bars, minRatio)
}

// pitConfigFrom is pitConfig with a per-instrument first bar (late listing).
func pitConfigFrom(t *testing.T, firstBar, lastBar map[int32]int64, bars int64, minRatio float64) Config {
	t.Helper()
	c := archiveConfig(t, false)
	c.Mode = Weights
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Portfolio.K = 1
	c.Manifest.Labels = nil
	c.FundingSource = ""
	c.Prices = PriceStream{"tick", "event", "price"}
	c.PITMinUniverseRatio = minRatio
	sids := []int32{1, 2, 3}
	c.Snapshot.Universe = factor.Universe{Version: "three", Static: true, Investable: sids, Tradable: sids, Evaluation: sids, Tracked: sids}
	c.Snapshot.SIDMap = map[int32]string{1: "A", 2: "B", 3: "C"}
	c.Snapshot.Schemas = map[string]string{"kline": "s1", "tick": "s1"}
	c.Snapshot.SourceVersions = map[string]string{"kline": "v1", "tick": "v1"}
	var err error
	c.Plan, err = factor.New().Add("close", factor.Field("kline", "close", "1h")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	store, err := factor.NewVersionStore(200)
	if err != nil {
		t.Fatal(err)
	}
	const hour int64 = 3600000
	price := map[int32]float64{1: 200, 2: 100, 3: 300} // 3 is the top score -> long; 2 the bottom -> short
	for _, sid := range sids {
		for bar := int64(1); bar <= bars; bar++ {
			if bar < firstBar[sid] {
				continue
			}
			if bar > lastBar[sid] {
				break
			}
			at := bar * hour
			err = store.Put(factor.VersionRecord{Series: orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, TimeMS: at - hour, EndMS: at, Closed: true, Values: map[string]any{"close": price[sid]}}, EventTime: at, AvailableAt: at, IngestedAt: at, Revision: 1, SourceVersion: "v1"})
			if err != nil {
				t.Fatal(err)
			}
			err = store.Put(factor.VersionRecord{Series: orm.DataSeries{Source: "tick", TimeFrame: "event", Sid: sid, TimeMS: at + 1, EndMS: at + 1, Closed: true, Values: map[string]any{"price": price[sid]}}, EventTime: at + 1, AvailableAt: at + 1, IngestedAt: at + 1, Revision: 1, SourceVersion: "v1"})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	path := filepath.Join(t.TempDir(), "pit.gob")
	if _, err = store.Export(path); err != nil {
		t.Fatal(err)
	}
	c.Chunks = []Chunk{{Path: path, From: hour, To: bars*hour + 2}}
	return c
}

// 067: SID 3 (held long) stops delivering bars after bar 6. The decisions go on with the remaining universe, the
// NAV stays continuous, the dead position is closed at its last mark (cost only) and the drops are counted.
func TestPITUniverseDropsDelistedInstrumentAndContinues(t *testing.T) {
	const bars = 14
	c := pitConfig(t, map[int32]int64{1: bars, 2: bars, 3: 6}, bars, 0)
	out := &navCapture{capture: capture{targets: map[int64]map[int32]float64{}}}
	result, err := Run(context.Background(), c, nil, out)
	if err != nil {
		t.Fatal(err)
	}
	if result.Incomplete != 0 || result.Decisions < 10 {
		t.Fatalf("decisions interrupted: incomplete=%d decisions=%d", result.Incomplete, result.Decisions)
	}
	if result.DelistExits != 1 || result.UniverseDrops < 5 || result.DroppedBySID[3] != result.UniverseDrops {
		t.Fatalf("drops: exits=%d drops=%d by=%v", result.DelistExits, result.UniverseDrops, result.DroppedBySID)
	}
	if q := result.Book.Quantities[3]; q != 0 {
		t.Fatalf("dead instrument still held: %v", q)
	}
	if len(result.UniverseDropLog) == 0 || len(result.UniverseDropLog[0].Exited) != 1 || result.UniverseDropLog[0].Exited[0] != 3 {
		t.Fatalf("drop log: %+v", result.UniverseDropLog)
	}
	// weights of SID 3 are zero from the first dropped decision on
	for at, w := range out.targets {
		if at > 8*3600000 {
			if _, ok := w[3]; ok {
				t.Fatalf("dropped instrument still targeted at %d: %v", at, w)
			}
		}
	}
	// NAV continuity: prices are constant, so NAV can only move by costs (fee+slippage 0.1% each side)
	for i := 1; i < len(out.navs); i++ {
		if math.Abs(out.navs[i]/out.navs[i-1]-1) > 0.02 {
			t.Fatalf("NAV jump at %d: %v -> %v (%v)", i, out.navs[i-1], out.navs[i], out.navs)
		}
	}
	if len(out.navs) < 10 {
		t.Fatalf("NAV curve frozen: %d points", len(out.navs))
	}
	// closed at the last mark 300 x (1-slip): the realised loss is bounded by costs (< 1% of NAV), never a price jump
	if result.Book.NAV < 9700 || result.Book.NAV > 10000 {
		t.Fatalf("final NAV %v", result.Book.NAV)
	}
}

// With PIT disabled (ratio < 0) the old behaviour stays: every decision after the data end is skipped.
func TestPITUniverseDisabledKeepsSkippingDecisions(t *testing.T) {
	const bars = 14
	c := pitConfig(t, map[int32]int64{1: bars, 2: bars, 3: 6}, bars, -1)
	result, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Incomplete == 0 || result.UniverseDrops != 0 {
		t.Fatalf("incomplete=%d drops=%d", result.Incomplete, result.UniverseDrops)
	}
}

// Too few remaining names (here 1 of 3 < 50%): the decision is still skipped and counted as incomplete.
func TestPITUniverseRefusesTooSmallRemainder(t *testing.T) {
	const bars = 14
	c := pitConfig(t, map[int32]int64{1: bars, 2: 6, 3: 6}, bars, 0)
	result, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Incomplete == 0 || result.UniverseDrops != 0 {
		t.Fatalf("incomplete=%d drops=%d", result.Incomplete, result.UniverseDrops)
	}
}
