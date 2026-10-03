package runner

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
)

func TestArchiveLiveRecursiveDecisionParityAcrossChunks(t *testing.T) {
	c := archiveConfig(t, false)
	c.Mode = Research
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Portfolio.K = 1
	c.MaxPending = 10
	c.Snapshot.ReplayTime = 1
	c.Snapshot.Universe = factor.Universe{Version: "recursive", Static: true, Investable: []int32{1, 2, 3}, Reference: []int32{1, 2, 3}, Tradable: []int32{1, 2, 3}, Tracked: []int32{1, 2, 3}, Evaluation: []int32{1, 2, 3}}
	var err error
	c.Plan, err = factor.New().Add("recursive", factor.EMA(factor.Return(factor.Field("kline", "close", "1h"), 1), 3)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"recursive"}, Weights: map[string]float64{"recursive": 1}}
	c.Chunks = nil
	var rows []factor.VersionRecord
	for chunk := 0; chunk < 3; chunk++ {
		store, _ := factor.NewVersionStore(6)
		for bar := chunk*2 + 1; bar <= chunk*2+2; bar++ {
			at := int64(bar) * c.DecisionInterval
			for _, sid := range c.Snapshot.Universe.Investable {
				row := factor.VersionRecord{Series: orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, EndMS: at, Closed: true, Values: map[string]any{"close": 100 + float64(bar*bar)*float64(sid), "custom": int64(1<<53 + bar), "nullable": nil}}, EventTime: at, AvailableAt: at, IngestedAt: at, Revision: 1, SourceVersion: "v1"}
				if err := store.Put(row); err != nil {
					t.Fatal(err)
				}
				rows = append(rows, row)
			}
		}
		path := filepath.Join(t.TempDir(), "chunk.gob")
		if _, err := store.Export(path); err != nil {
			t.Fatal(err)
		}
		c.Chunks = append(c.Chunks, Chunk{Path: path, From: int64(chunk*2+1) * c.DecisionInterval, To: int64(chunk*2+2) * c.DecisionInterval})
	}
	archive := &parityOutput{capture: capture{targets: map[int64]map[int32]float64{}}}
	result, err := Run(context.Background(), c, nil, archive)
	if err != nil {
		t.Fatal(err)
	}
	clock := int64(0)
	liveOutput := &parityOutput{capture: capture{targets: map[int64]map[int32]float64{}}}
	live, err := NewLive(c, &liveSink{}, func() int64 { return clock }, liveOutput)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Stop()
	for i, row := range rows {
		clock = row.EventTime
		if err := live.Observe(context.Background(), row); err != nil {
			t.Fatal(err)
		}
		if (i+1)%3 == 0 {
			if err := live.Flush(context.Background(), clock); err != nil {
				t.Fatal(err)
			}
		}
	}
	if result.Decisions != 6 || len(archive.targets) == 0 || len(archive.frames) != len(liveOutput.frames) || !reflect.DeepEqual(archive.targets, liveOutput.targets) {
		t.Fatal("recursive state or decisions differ between chunks and live publication")
	}
	for i, expected := range archive.frames {
		actual := liveOutput.frames[i]
		if expected.GridTime != actual.GridTime || expected.DecisionTime != actual.DecisionTime || expected.PlanHash != actual.PlanHash || expected.SnapshotID != actual.SnapshotID {
			t.Fatalf("frame identity differs at grid %d", expected.GridTime)
		}
		for column, values := range expected.Values {
			for sid, value := range values {
				got, ok := actual.Values[column][sid]
				if !ok || got.Validity != value.Validity || got.Value != value.Value && !(math.IsNaN(got.Value) && math.IsNaN(value.Value)) {
					t.Fatalf("grid %d column %s SID %d: archive=%+v live=%+v", expected.GridTime, column, sid, value, got)
				}
			}
		}
	}
}

func TestLiveRecordRetentionOwnsTypedValues(t *testing.T) {
	for _, warmup := range []bool{false, true} {
		t.Run(map[bool]string{false: "observe", true: "warmup"}[warmup], func(t *testing.T) {
			c := archiveConfig(t, false)
			c.Manifest.Costs.FundingPolicy = "explicit-zero"
			live, err := NewLive(c, &liveSink{}, func() int64 { return 3600001 }, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer live.Stop()
			var typedNull *int64
			nested := map[string]any{"items": []int64{1<<53 + 7}, "name": "asset", "enabled": true}
			row := factor.VersionRecord{Series: orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: 1, EndMS: 3600000, Closed: true, Values: map[string]any{"close": 100.0, "large": int64(1<<53 + 7), "nested": nested, "null": nil, "typedNull": typedNull}}, EventTime: 3600000, AvailableAt: 3600000, IngestedAt: 3600000, Revision: 1, SourceVersion: "v1"}
			if warmup {
				err = live.Warmup(context.Background(), row)
			} else {
				err = live.Observe(context.Background(), row)
			}
			if err != nil {
				t.Fatal(err)
			}
			nested["items"].([]int64)[0] = 0
			row.Series.Values["large"] = float64(0)
			delete(row.Series.Values, "null")
			key := factor.StreamKey{SID: 1, Source: "kline", Frequency: "1h"}
			stored := live.rows[key]
			if warmup {
				stored = live.warmRows[key][0]
			}
			values := stored.Series.Values
			if values["large"] != int64(1<<53+7) || values["nested"].(map[string]any)["items"].([]int64)[0] != 1<<53+7 || values["typedNull"] != typedNull {
				t.Fatal("retained record lost type or shares callback values")
			}
			if null, ok := values["null"]; !ok || null != nil {
				t.Fatal("NULL became missing")
			}
			if _, ok := values["missing"]; ok {
				t.Fatal("missing field was invented")
			}
		})
	}
}

func TestLiveDecisionCompletionAndExpiryKeepPublicationSemantics(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "expired"}[expired], func(t *testing.T) {
			c := archiveConfig(t, false)
			c.Manifest.Costs.FundingPolicy = "explicit-zero"
			c.Manifest.Portfolio.K = 1
			c.Snapshot.Universe = factor.Universe{Version: "completion", Static: true, Investable: []int32{1, 2, 3}, Reference: []int32{1, 2, 3}, Tradable: []int32{1, 2, 3}}
			const grid int64 = 3600000
			clock := grid + 1
			completed := grid + 4
			if expired {
				completed = grid + c.ExpiryMS
			}
			// The callback emulates computation consuming real decision-clock time.
			node := factor.Custom("completion-test", []*factor.Node{factor.Field("kline", "close", "1h")}, func(values []factor.Numeric) factor.Numeric {
				clock = completed
				return values[0]
			})
			var err error
			c.Plan, err = factor.New().Add("close", node).Compile()
			if err != nil {
				t.Fatal(err)
			}
			c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
			live, err := NewLive(c, &liveSink{}, func() int64 { return clock }, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer live.Stop()
			for _, sid := range c.Snapshot.Universe.Investable {
				row := factor.VersionRecord{Series: orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, EndMS: grid, Closed: true, Values: map[string]any{"close": float64(sid)}}, EventTime: grid, AvailableAt: grid, IngestedAt: grid, Revision: 1, SourceVersion: "v1"}
				if err := live.Observe(context.Background(), row); err != nil {
					t.Fatal(err)
				}
			}
			err = live.Flush(context.Background(), grid)
			if expired {
				if !errors.Is(err, factor.ErrRoundExpired) || live.pending != nil || live.lastDecision != 0 || len(live.engine.session.Updates()) != 0 {
					t.Fatal("expired private computation published or advanced owner")
				}
				return
			}
			if err != nil || live.pending == nil {
				t.Fatalf("completed decision missing: %v", err)
			}
			p := live.pending.Spec()
			if p.DecisionTime != grid+1 || p.ExecutableAt != completed+c.LatencyMS || p.ExpireAt != grid+c.ExpiryMS || p.Budget.NAV != 10000 {
				t.Fatalf("cutoff, completion clock or frozen budget changed: %+v", p)
			}
		})
	}
}
