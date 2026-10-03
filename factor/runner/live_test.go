package runner

import (
	"context"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
	"reflect"
	"slices"
	"testing"
)

type liveSink struct {
	targets []*factor.TargetPortfolio
	at      []int64
}

type scopedQuoteSink struct {
	liveSink
	quoted   []int32
	navCalls int
}

func (s *scopedQuoteSink) StrategyNAV(context.Context, int64) (float64, error) {
	s.navCalls++
	return 10000, nil
}

func (s *scopedQuoteSink) ObserveQuote(_ context.Context, sid int32, _ backtest.Quote, _ int64) error {
	s.quoted = append(s.quoted, sid)
	return nil
}

func TestLiveReferencePriceRowsFeedDAGWithoutExecutionQuote(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Snapshot.Universe = factor.Universe{Version: "scoped", Static: true, Investable: []int32{2}, Tradable: []int32{2}, Tracked: []int32{1}, Reference: []int32{3}, Evaluation: []int32{4}}
	c.Snapshot.SIDMap[4] = "asset-4"
	plan, err := factor.New().Add("close", factor.Field("kline", "close", "1h")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Plan = plan
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	c.Prices = PriceStream{Source: "kline", Frequency: "1h", Field: "close"}
	sink := &scopedQuoteSink{}
	live, err := NewLive(c, sink, func() int64 { return 3600002 }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Stop()
	for _, sid := range []int32{1, 2, 3, 4} {
		r := factor.VersionRecord{Series: orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, Closed: true, TimeMS: 0, EndMS: 3600000, Values: map[string]any{"close": 100.0}}, EventTime: 3600000, AvailableAt: 3600001, IngestedAt: 3600002, Revision: 1, SourceVersion: "v1"}
		if err := live.Observe(context.Background(), r); err != nil {
			t.Fatal(err)
		}
		if err := live.Flush(context.Background(), 3600000); err != nil {
			t.Fatal(err)
		}
		if sid < 3 && sink.navCalls != 0 {
			t.Fatal("barrier completed before required trading inputs")
		}
	}
	if !slices.Equal(sink.quoted, []int32{1, 2}) {
		t.Fatalf("execution quote pool: %v", sink.quoted)
	}
	if len(live.rows) != 4 || len(live.quotes) != 2 {
		t.Fatalf("DAG rows=%d execution quotes=%d", len(live.rows), len(live.quotes))
	}
	if sink.navCalls != 1 || live.lastDecision != 3600000 {
		t.Fatalf("all-pool decision incomplete: nav=%d grid=%d", sink.navCalls, live.lastDecision)
	}
}

func TestLiveEvaluationOnlyRowsDoNotChangeTradingReadinessOrTargets(t *testing.T) {
	var baseline map[int32]float64
	for _, mode := range []string{"absent", "current", "future"} {
		t.Run(mode, func(t *testing.T) {
			c := archiveConfig(t, false)
			c.Manifest.Costs.FundingPolicy = "explicit-zero"
			c.Manifest.Portfolio.K = 1
			c.Snapshot.Universe = factor.Universe{Version: "scoped", Static: true, Investable: []int32{1, 2}, Tradable: []int32{1, 2}, Tracked: []int32{1, 2}, Reference: []int32{3}, Evaluation: []int32{4}}
			c.Snapshot.SIDMap[4] = "asset-4"
			plan, err := factor.New().Add("close", factor.Field("kline", "close", "1h")).Compile()
			if err != nil {
				t.Fatal(err)
			}
			c.Plan = plan
			c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
			clock := int64(3600002)
			sink := &scopedQuoteSink{}
			live, err := NewLive(c, sink, func() int64 { return clock }, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer live.Stop()
			row := func(sid int32, source, frequency, field string, event int64, value float64) factor.VersionRecord {
				return factor.VersionRecord{Series: orm.DataSeries{Source: source, TimeFrame: frequency, Sid: sid, Closed: true, TimeMS: event - 1, EndMS: event, Values: map[string]any{field: value}}, EventTime: event, AvailableAt: event, IngestedAt: clock, Revision: 1, SourceVersion: "v1"}
			}
			if mode != "absent" {
				event := int64(3600000)
				if mode == "future" {
					event++
				}
				if err := live.Observe(context.Background(), row(4, "kline", "1h", "close", event, 999999)); err != nil {
					t.Fatal(err)
				}
				if err := live.Flush(context.Background(), 3600000); err != nil {
					t.Fatal(err)
				}
				if sink.navCalls != 0 {
					t.Fatal("evaluation-only row completed trading barrier")
				}
			}
			for _, sid := range []int32{1, 2, 3} {
				if err := live.Observe(context.Background(), row(sid, "kline", "1h", "close", 3600000, float64(sid)*10)); err != nil {
					t.Fatal(err)
				}
			}
			if err := live.Flush(context.Background(), 3600000); err != nil {
				t.Fatal(err)
			}
			if sink.navCalls != 1 || live.lastDecision != 3600000 || live.pending == nil {
				t.Fatal("required trading pool did not complete without evaluation data")
			}
			clock = 3600004
			for _, sid := range []int32{1, 2} {
				if err := live.Observe(context.Background(), row(sid, c.Prices.Source, c.Prices.Frequency, c.Prices.Field, 3600003, 100)); err != nil {
					t.Fatal(err)
				}
			}
			if len(sink.targets) != 1 {
				t.Fatalf("completed executions=%d", len(sink.targets))
			}
			targets := sink.targets[0].Targets()
			if len(targets) != 2 {
				t.Fatalf("execution targets: %v", targets)
			}
			if mode == "absent" {
				baseline = targets
			} else if !reflect.DeepEqual(targets, baseline) {
				t.Fatalf("evaluation perturbed targets: %v vs %v", targets, baseline)
			}
			if _, ok := targets[4]; ok {
				t.Fatal("evaluation-only SID entered target")
			}
		})
	}
}

func (s *liveSink) StrategyNAV(context.Context, int64) (float64, error) { return 10000, nil }
func (s *liveSink) ProcessSnapshot(_ context.Context, p *factor.TargetPortfolio, _ map[int32]backtest.Quote, at int64) error {
	s.targets = append(s.targets, p)
	s.at = append(s.at, at)
	return nil
}
func TestLiveDelayedBarrierAndStrictAfterCompletion(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Portfolio.K = 1
	u := &c.Snapshot.Universe
	u.Investable = u.Investable[:3]
	u.Reference = u.Reference[:3]
	u.Tradable = u.Tradable[:3]
	u.Tracked = u.Tracked[:3]
	u.Evaluation = u.Evaluation[:3]
	plan, err := factor.New().Add("close", factor.Field("kline", "close", "1h")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Plan = plan
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	clock := int64(3600002)
	sink := &liveSink{}
	live, err := NewLive(c, sink, func() int64 { return clock }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Stop()
	ctx := context.Background()
	row := func(sid int32, source, freq, field string, at, available int64, value float64) factor.VersionRecord {
		return factor.VersionRecord{Series: orm.DataSeries{Source: source, TimeFrame: freq, Sid: sid, Closed: true, TimeMS: at - 3600000, EndMS: at, Values: map[string]any{field: value}}, EventTime: at, AvailableAt: available, IngestedAt: available, Revision: 1, SourceVersion: "v1"}
	}
	for _, sid := range []int32{1, 2} {
		if err = live.Observe(ctx, row(sid, "kline", "1h", "close", 3600000, clock, float64(sid)*100)); err != nil {
			t.Fatal(err)
		}
	}
	if err = live.Flush(ctx, 3600000); err != nil {
		t.Fatal(err)
	}
	if len(sink.targets) != 0 {
		t.Fatal("incomplete barrier produced target")
	}
	if err = live.Observe(ctx, row(3, "kline", "1h", "close", 3600000, clock, 300)); err != nil {
		t.Fatal(err)
	}
	if err = live.Flush(ctx, 3600000); err != nil {
		t.Fatal(err)
	}
	clock = 3600004
	for _, sid := range []int32{1, 3} {
		if err = live.Observe(ctx, row(sid, "tick", "event", "price", 3600001, clock, float64(sid)*100)); err != nil {
			t.Fatal(err)
		}
	}
	if len(sink.targets) != 0 {
		t.Fatal("delayed earlier event backfilled live decision")
	}
	clock = 3600005
	for _, sid := range []int32{1, 3} {
		if err = live.Observe(ctx, row(sid, "tick", "event", "price", 3600004, clock, float64(sid)*100)); err != nil {
			t.Fatal(err)
		}
	}
	if len(sink.targets) != 1 || sink.at[0] != clock {
		t.Fatal("eligible event missing")
	}
	spec := sink.targets[0].Spec()
	if spec.DecisionTime != 3600002 || spec.ExecutableAt != 3600003 {
		t.Fatalf("actual cutoff/completion not frozen: %+v", spec)
	}
	if err = live.Flush(ctx, 3600000); err != nil {
		t.Fatal(err)
	}
	if len(sink.targets) != 1 {
		t.Fatal("duplicate grid replayed")
	}
	live.Stop()
	if err = live.Observe(ctx, row(1, "tick", "event", "price", 3600005, clock, 100)); err == nil {
		t.Fatal("stopped live accepted record")
	}
}
