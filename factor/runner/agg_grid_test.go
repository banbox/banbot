package runner

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
)

// Decision grids must land on the bar close times of the timeframe: 3d/1w buckets close on the engine's own
// origin (1970-01-02 / 1970-01-05), not on epoch multiples.
func aggArchiveConfig(t testing.TB, step, firstEnd int64) Config {
	t.Helper()
	store, _ := factor.NewVersionStore(20000)
	sids := make([]int32, 24)
	symbols := map[int32]string{}
	hour := step
	put := func(source, freq string, sid int32, at, available int64, values map[string]any) {
		t.Helper()
		err := store.Put(factor.VersionRecord{Series: orm.DataSeries{Source: source, TimeFrame: freq, Sid: sid, TimeMS: at, EndMS: at, Closed: true, Values: values}, EventTime: at, AvailableAt: available, IngestedAt: available, Revision: 1, SourceVersion: "v1"})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := range sids {
		sid := int32(i + 1)
		sids[i] = sid
		symbols[sid] = fmt.Sprint("asset", sid)
		for bar := int64(1); bar <= 28; bar++ {
			at := firstEnd + (bar-1)*hour
			price := 100 + float64(i+1)*float64(bar) + float64((bar*bar+int64(i))%7)
			values := map[string]any{"close": price, "custom": int64(bar), "nullable": nil}
			if sid == 24 && bar == 25 {
				values["close"] = nil
			}
			put("kline", "1h", sid, at, at, values)
			p := price
			if !(sid == 23 && bar == 27) {
				put("tick", "event", sid, at+1, at+1, map[string]any{"price": p})
			}
			put("funding", "event", sid, at+2, at+2, map[string]any{"rate": .0001})
		}
	}
	if step >= 3*86400000 {
		// like the 1m price stream of a storage run: a source batch exists at every day boundary, so the decision grid
		// is decided by the origin rule and not by the run start (the archive otherwise only has batches at From+k*step)
		const day = int64(86400000)
		for _, sid := range sids {
			for at := firstEnd - day; at <= firstEnd+27*step; at += day {
				put("tick", "event", sid, at, at, map[string]any{"price": 100 + float64(sid)})
			}
		}
	}
	path := filepath.Join(t.TempDir(), "raw.gob")
	if _, err := store.Export(path); err != nil {
		t.Fatal(err)
	}
	return Config{Mode: Weights, Chunks: []Chunk{{path, firstEnd, firstEnd + 27*hour + 2}}, MaxRecords: 20000, MaxPending: 10, DecisionInterval: hour, LatencyMS: 1, ExpiryMS: 60000, Snapshot: factor.SnapshotSpec{Universe: factor.Universe{Version: "u", Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Tracked: sids, Static: true}, SIDMap: symbols, Schemas: map[string]string{"kline": "s1", "tick": "s1", "funding": "s1"}, SourceVersions: map[string]string{"kline": "v1", "tick": "v1", "funding": "v1"}, VisibilityPolicy: "available-at", AdjustmentVersion: "raw"}, Factor: research.DefaultMomentumVolConfig(), Manifest: research.ManifestSpec{CodeRevision: "test", Currency: "USD", Portfolio: research.PortfolioDefinition{K: 5, LongNotional: .5, ShortNotional: .5, Mode: factor.Full}, Labels: []research.LabelSpec{{Name: "agg", Kind: research.ExecutableReturn, Horizon: hour, PeriodsPerYear: 365.25 * 86400000 / float64(step), Overlapping: true}}, Costs: research.CostSpec{FeeRate: .001, SlippageRate: .001, FundingPolicy: "required-stream"}}, StrategyID: "s", AccountID: "a", InitialNAV: 10000, Prices: PriceStream{"tick", "event", "price"}, FundingSource: "funding"}
}

func TestDecisionGridFollowsAggregatedBarOrigin(t *testing.T) {
	const hour, day = int64(3600000), int64(86400000)
	cases := []struct {
		name           string
		step, firstEnd int64
	}{
		{"4h", 4 * hour, 1704067200000 + 4*hour},   // 2024-01-01 04:00
		{"8h", 8 * hour, 1704067200000 + 8*hour},   // ends are epoch multiples
		{"3d", 3 * day, 1704067200000 + 3*day},     // 2024-01-04: 1d + k*3d, NOT an epoch multiple of 3d
		{"1w", 7 * day, 1704067200000 + 7*day - 0}, // 2024-01-08: 4d + k*7d
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := aggArchiveConfig(t, c.step, c.firstEnd)
			cfg.DecisionInterval = c.step
			if c.name == "3d" || c.name == "1w" {
				// a run window that starts on a date which is not a bar close (the usual case): without the origin the
				// grid is anchored at the run start and never coincides with the bars
				cfg.Chunks[0].From = c.firstEnd - day
			}
			out := &capture{targets: map[int64]map[int32]float64{}}
			r, err := Run(context.Background(), cfg, nil, out)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%s result: decisions=%d incomplete=%d skipped=%d executions=%d", c.name, r.Decisions, r.Incomplete, r.Skipped, r.Executions)
			if r.Decisions != 28 || r.Incomplete != 0 {
				t.Fatalf("%s: decisions=%d incomplete=%d, want 28/0 (grid must coincide with the bar close times)", c.name, r.Decisions, r.Incomplete)
			}
			for at := range out.targets {
				if (at-c.firstEnd)%c.step != 0 {
					t.Fatalf("%s: decision at %d is off the bar grid", c.name, at)
				}
			}
		})
	}
}

func TestBarGridOrigin(t *testing.T) {
	const hour, day = int64(3600000), int64(86400000)
	for interval, want := range map[int64]int64{hour: 0, 4 * hour: 0, 8 * hour: 0, day: 0, 2 * day: 0, 3 * day: day, 6 * day: day, 7 * day: 4 * day, 500: 0} {
		if got := barGridOrigin(interval); got != want {
			t.Fatalf("barGridOrigin(%d)=%d want %d", interval, got, want)
		}
	}
}
