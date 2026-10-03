package runner

import (
	"context"
	"strings"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
)

func TestLiveRejectsColdExecutionHistory(t *testing.T) {
	c := archiveConfig(t, false)
	c.Execution.HistoryPath = "cold-history"
	if live, err := NewLive(c, &liveSink{}, func() int64 { return 100 }, nil); live != nil || err == nil || !strings.Contains(err.Error(), "only by simulated replay") {
		t.Fatalf("live cold history accepted: %v %v", live, err)
	}
}

func TestLiveWarmupBoundsSIDMajorHistoryAndNeverAdmitsHistoricalTargets(t *testing.T) {
	c := archiveConfig(t, false)
	c.Factor.Window = 2
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	clock := int64(1000000000)
	live, err := NewLive(c, &liveSink{}, func() int64 { return clock }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Stop()
	input := live.Inputs()[0]
	limit := max(1, input.WarmupLength) + 1
	for bar := 1; bar <= limit; bar++ {
		at := int64(bar) * c.DecisionInterval
		err = live.Warmup(context.Background(), factor.VersionRecord{Series: orm.DataSeries{Source: input.Source, TimeFrame: input.Frequency, Sid: 1, EndMS: at, Closed: true, IsWarmUp: true, Values: map[string]any{"close": float64(bar), "nullable": nil}}, EventTime: at, AvailableAt: at, IngestedAt: clock, Revision: 1, SourceVersion: "v1"})
		if err != nil {
			t.Fatal(err)
		}
	}
	key := factor.StreamKey{SID: 1, Source: input.Source, Frequency: input.Frequency}
	if len(live.warmRows[key]) != limit || live.pending != nil || live.sequence != 0 {
		t.Fatal("warmup buffer/admission changed")
	}
	at := int64(limit+1) * c.DecisionInterval
	if err := live.Warmup(context.Background(), factor.VersionRecord{Series: orm.DataSeries{Source: input.Source, TimeFrame: input.Frequency, Sid: 1, EndMS: at, Closed: true, Values: map[string]any{"close": 100.0}}, EventTime: at, AvailableAt: at, IngestedAt: clock, Revision: 1, SourceVersion: "v1"}); err == nil {
		t.Fatal("unbounded incomplete warmup accepted")
	}
}

func TestLiveWarmupReadinessRequiresCompleteDecisionSnapshots(t *testing.T) {
	for _, scenario := range []string{"complete", "missing-sid", "unaligned-events", "old-boundary", "too-few-grids"} {
		t.Run(scenario, func(t *testing.T) {
			c := archiveConfig(t, false)
			c.Factor.Window = 2
			c.Manifest.Costs.FundingPolicy = "explicit-zero"
			step := c.DecisionInterval
			clock := 3*step + 2
			sink := &liveSink{}
			live, err := NewLive(c, sink, func() int64 { return clock }, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer live.Stop()
			input := live.Inputs()[0]
			if err := live.ValidateWarmup(clock); err == nil {
				t.Fatal("empty startup history reported ready")
			}
			for _, sid := range live.DataSIDs() {
				if scenario == "missing-sid" && sid == live.DataSIDs()[len(live.DataSIDs())-1] {
					continue
				}
				for bar := int64(1); bar <= 3; bar++ {
					if scenario == "old-boundary" && bar == 3 || scenario == "too-few-grids" && bar < 3 {
						continue
					}
					at := bar * step
					if scenario == "unaligned-events" {
						at++
					}
					if err := live.Warmup(context.Background(), factor.VersionRecord{Series: orm.DataSeries{Source: input.Source, TimeFrame: input.Frequency, Sid: sid, EndMS: at, Closed: true, IsWarmUp: true, Values: map[string]any{"close": float64(bar) + float64(sid)}}, EventTime: at, AvailableAt: at, IngestedAt: clock, Revision: 1, SourceVersion: "v1"}); err != nil {
						t.Fatal(err)
					}
				}
			}
			err = live.ValidateWarmup(clock)
			if scenario == "complete" && err != nil {
				t.Fatal(err)
			}
			if scenario != "complete" && err == nil {
				t.Fatal("incomplete decision-grid history reported ready")
			}
			if live.pending != nil || live.sequence != 0 || len(sink.targets) != 0 {
				t.Fatal("readiness/warmup admitted historical targets")
			}
		})
	}
}
