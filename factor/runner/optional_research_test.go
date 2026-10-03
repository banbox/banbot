package runner

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
)

func TestReplayOptionalResearchPreservesTrading(t *testing.T) {
	for _, mode := range []Mode{Weights, Events, Trade} {
		t.Run(string(mode), func(t *testing.T) {
			c := archiveConfig(t, false)
			c.Mode = mode
			var baseline *capture
			for _, enabled := range []bool{true, false} {
				cfg := c
				if !enabled {
					cfg.Manifest.Labels = nil
					cfg.MaxPending = 1
				}
				out := &capture{targets: map[int64]map[int32]float64{}}
				var sink Sink
				if mode != Weights {
					cfg = paperConfig(t, cfg)
					paper, close, err := NewPaperSink(context.Background(), cfg)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := close(); err != nil {
							t.Error(err)
						}
					})
					sink = paper
				}
				result, err := Run(context.Background(), cfg, sink, out)
				if err != nil {
					t.Fatalf("research=%v: %v", enabled, err)
				}
				if result.TargetsAccepted == 0 {
					t.Fatal("no targets accepted")
				}
				if enabled {
					baseline = out
				} else {
					if result.MaxPendingEvaluations != 0 || result.Unresolved != 0 || len(result.Summary) != 0 || len(out.reports) != 0 {
						t.Fatalf("disabled research retained evaluations: %+v", result)
					}
					if !reflect.DeepEqual(baseline.targets, out.targets) || !reflect.DeepEqual(baseline.executed, out.executed) {
						t.Fatal("disabling research changed decisions or acceptance times")
					}
				}
			}
		})
	}
}

func TestOptionalResearchKeepsResearchAndHistoryICStrict(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.Labels = nil
	c.Mode = Research
	if err := ValidateReplayConfig(c, false); err == nil {
		t.Fatal("research accepted absent labels")
	}
	c.Mode = Weights
	c.Combo = research.ComboSpec{Method: research.HistoryIC, Columns: []string{"momentum"}}
	if err := ValidateReplayConfig(c, false); err == nil {
		t.Fatal("history-IC accepted absent labels")
	}
	if err := ValidateLiveConfig(c); err == nil || !strings.Contains(err.Error(), "matured history provider") {
		t.Fatalf("live history-IC bypassed mature provider: %v", err)
	}
}

func TestLiveOptionalResearchBuildsAndPublishesTargets(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.Labels = nil
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Portfolio.K = 1
	c.Plan, _ = factor.New().Add("close", factor.Field("kline", "close", "1h")).Compile()
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	if err := ValidateLiveConfig(c); err != nil {
		t.Fatal(err)
	}
	clock := int64(3600002)
	sink := &liveSink{}
	live, err := NewLive(c, sink, func() int64 { return clock }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		live.Stop()
		if err := live.Join(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	for _, sid := range c.Snapshot.Universe.Investable {
		row := factor.VersionRecord{Series: orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, EndMS: 3600000, Closed: true, Values: map[string]any{"close": float64(sid) * 100}}, EventTime: 3600000, AvailableAt: clock, IngestedAt: clock, Revision: 1, SourceVersion: "v1"}
		if err := live.Observe(context.Background(), row); err != nil {
			t.Fatal(err)
		}
	}
	if err := live.Flush(context.Background(), 3600000); err != nil {
		t.Fatal(err)
	}
	clock += 2
	for _, sid := range c.Snapshot.Universe.Investable {
		row := factor.VersionRecord{Series: orm.DataSeries{Source: "tick", TimeFrame: "event", Sid: sid, TimeMS: clock, EndMS: clock, Closed: true, Values: map[string]any{"price": float64(sid) * 100}}, EventTime: clock, AvailableAt: clock, IngestedAt: clock, Revision: 1, SourceVersion: "v1"}
		if err := live.Observe(context.Background(), row); err != nil {
			t.Fatal(err)
		}
	}
	if len(sink.targets) != 1 {
		t.Fatalf("targets=%d", len(sink.targets))
	}
}

func BenchmarkReplayOptionalResearch(b *testing.B) {
	c := archiveConfig(b, false)
	for _, enabled := range []bool{true, false} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		b.Run(name, func(b *testing.B) {
			cfg := c
			if !enabled {
				cfg.Manifest.Labels = nil
				cfg.MaxPending = 1
			}
			b.ReportAllocs()
			for range b.N {
				result, err := Run(context.Background(), cfg, nil, nil)
				if err != nil {
					b.Fatal(err)
				}
				if result.Decisions != 28 || result.TargetsAccepted == 0 {
					b.Fatal("incomplete replay")
				}
				b.ReportMetric(float64(result.MaxPendingEvaluations), "pending-frames/op")
			}
		})
	}
}
