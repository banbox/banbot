package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
)

func TestFactorsLiveProviderConsumerSharesComputationAcrossTenStrategies(t *testing.T) {
	f := newSharedTriggerFixture(t)
	raw, err := os.ReadFile("../factor/runner/example.json")
	if err != nil {
		t.Fatal(err)
	}
	var cfg runner.Config
	if err = json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Manifest.Costs.FundingPolicy = "explicit-zero"
	cfg.Snapshot.Universe = factor.Universe{Version: "ten", Static: true, Investable: []int32{1, 2}, Tradable: []int32{1, 2}, Reference: []int32{1, 2}, Evaluation: []int32{1, 2}, Tracked: []int32{3}}
	var calls atomic.Int64
	cfg.Plan, err = factor.New().Add("close", factor.Custom("live-provider-shared-v1", []*factor.Node{factor.Field("kline", "close", "1h")}, func(values []factor.Numeric) factor.Numeric { calls.Add(1); return values[0] })).Compile()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	cfg.ComputationGroup = runner.NewComputationGroup()
	cfg.ComputationContext = runner.ComputationContext{DataNamespace: "provider", ClockDomain: "runtime", SamplingIdentity: "received-batch"}
	const grid int64 = 3600000
	f.rt.Clock.SetTimeMS(grid + 2)
	engines := make([]*runner.Live, 10)
	sinks := make([]*closedGridSink, 10)
	intervals := make([]int64, 10)
	for i := range engines {
		cfg.StrategyID = fmt.Sprintf("provider-%d", i)
		sinks[i] = &closedGridSink{}
		// Real work clocks can advance between consumers. FlushAt keeps one
		// receipt cutoff without replacing their actual deadline clocks.
		engines[i], err = runner.NewLive(cfg, sinks[i], func() int64 { return f.rt.Clock.TimeMS() + int64(i) }, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer engines[i].Stop()
		intervals[i] = cfg.DecisionInterval
	}
	var mapped int
	consume := f.rt.factorsLiveConsumer(engines, func(series *orm.DataSeries, received int64) (factor.VersionRecord, error) {
		mapped++
		if series.Values["custom"] != int64(7) || series.Values["nullable"] != nil {
			t.Fatal("arbitrary fields changed")
		}
		return factor.VersionRecord{Series: *series, EventTime: series.EndMS, AvailableAt: series.EndMS, IngestedAt: received, Revision: 1, SourceVersion: "v1"}, nil
	}, intervals, func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	})
	for _, sid := range []int32{1, 2} {
		consume(&orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, TimeMS: grid - 3600000, EndMS: grid, Closed: true, Values: map[string]any{"close": float64(sid) * 100, "custom": int64(7), "nullable": nil}})
	}
	if calls.Load() != 2 || mapped != 2 {
		t.Fatalf("one batch repeated DAG/mapping: nodes=%d mapped=%d", calls.Load(), mapped)
	}
	// Tracked-only SID 3 requires quotes, never a factor input on the grid.
	f.rt.Clock.SetTimeMS(grid + 30)
	for _, sid := range []int32{1, 2, 3} {
		consume(&orm.DataSeries{Source: "tick", TimeFrame: "event", Sid: sid, TimeMS: grid + 25, EndMS: grid + 25, Values: map[string]any{"price": 100.0, "custom": int64(7), "nullable": nil}})
	}
	for _, sink := range sinks {
		if len(sink.targets) != 1 {
			t.Fatal("independent strategy target missing", len(sink.targets))
		}
	}
	for _, engine := range engines {
		engine.Stop()
		if err := engine.Join(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
