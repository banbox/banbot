package runner

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
)

func TestArchivePatchReadinessOnlyRequiresExplicitTargets(t *testing.T) {
	for _, scenario := range []struct {
		name           string
		mode           factor.PortfolioMode
		freshA, freshB bool
		executions     int
		remainingB     float64
	}{
		{"patch-omitted-B", factor.Patch, true, false, 2, 50},
		{"patch-stale-explicit-A", factor.Patch, false, false, 1, 50},
		{"full-stale-closing-B", factor.Full, true, false, 1, 50},
		{"full-fresh-closing-B", factor.Full, true, true, 2, 0},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			const hour int64 = 3600000
			c := archiveConfig(t, false)
			u := &c.Snapshot.Universe
			u.Investable = []int32{1, 2, 3}
			u.Reference = u.Investable
			u.Tradable = u.Investable
			u.Evaluation = u.Investable
			u.Tracked = u.Investable
			c.Mode = Weights
			c.Manifest.Costs.FundingPolicy = "explicit-zero"
			c.Manifest.Costs.FeeRate = 0
			c.Manifest.Costs.SlippageRate = 0
			c.Manifest.Portfolio.Mode = scenario.mode
			c.Manifest.Portfolio.K = 1
			var err error
			c.Plan, err = factor.New().Add("signal", factor.Field("kline", "signal", "1h")).Compile()
			if err != nil {
				t.Fatal(err)
			}
			c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"signal"}, Weights: map[string]float64{"signal": 1}}
			store, _ := factor.NewVersionStore(30)
			put := func(sid int32, source, freq string, at int64, values map[string]any) {
				t.Helper()
				if err := store.Put(factor.VersionRecord{Series: orm.DataSeries{Source: source, Sid: sid, TimeMS: at, EndMS: at, TimeFrame: freq, Closed: true, Values: values}, EventTime: at, AvailableAt: at, IngestedAt: at, Revision: 1, SourceVersion: "v1"}); err != nil {
					t.Fatal(err)
				}
			}
			for _, sid := range u.Investable {
				put(sid, "kline", "1h", hour, map[string]any{"signal": float64(sid)})
				signal := any(float64(sid))
				if sid == 3 {
					signal = nil
				}
				put(sid, "kline", "1h", 2*hour, map[string]any{"signal": signal})
			}
			for _, sid := range []int32{1, 3} {
				put(sid, "tick", "event", hour+1, map[string]any{"price": 100.0})
			}
			put(2, "tick", "event", 2*hour+1, map[string]any{"price": 100.0})
			if scenario.freshA {
				put(1, "tick", "event", 2*hour+1, map[string]any{"price": 100.0})
			}
			if scenario.freshB {
				put(3, "tick", "event", 2*hour+2, map[string]any{"price": 100.0})
			}
			path := filepath.Join(t.TempDir(), "patch.gob")
			if _, err = store.Export(path); err != nil {
				t.Fatal(err)
			}
			c.Chunks = []Chunk{{Path: path, From: hour, To: 2*hour + 3}}
			c.MaxRecords = 30
			result, err := Run(context.Background(), c, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if result.Executions != scenario.executions || result.Book.Quantities[3] != scenario.remainingB {
				t.Fatalf("readiness/retained quantity mismatch: executions=%d state=%+v", result.Executions, result.Book)
			}
		})
	}
}
