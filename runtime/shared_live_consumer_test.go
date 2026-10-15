package runtime

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
)

type closedGridSink struct {
	targets  []*factor.TargetPortfolio
	navCalls int
}

func (s *closedGridSink) StrategyNAV(context.Context, int64) (float64, error) {
	s.navCalls++
	return 10000, nil
}
func (s *closedGridSink) ProcessSnapshot(_ context.Context, p *factor.TargetPortfolio, _ map[int32]backtest.Quote, _ int64) error {
	s.targets = append(s.targets, p)
	return nil
}

func TestFactorLiveProductionConsumerMergesHistoricalWarmupWithoutExpiredAdmission(t *testing.T) {
	f := newSharedTriggerFixture(t)
	raw, err := os.ReadFile("../factor/runner/example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c runner.Config
	if err = json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	sink := &closedGridSink{}
	const hour int64 = 3600000
	f.rt.Clock.SetTimeMS(25*hour + 2)
	engine, err := runner.NewLive(c, sink, f.rt.Clock.TimeMS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()
	consume := f.rt.factorLiveConsumer(engine, func(s *orm.DataSeries, received int64) (factor.VersionRecord, error) {
		if _, ok := s.Values["nullable"]; !ok || s.Values["nullable"] != nil || s.Values["custom_int"] != int64(17) {
			t.Fatal("warmup fields/types lost")
		}
		return factor.VersionRecord{Series: *s, EventTime: s.EndMS, AvailableAt: s.EndMS, IngestedAt: received, Revision: 1, SourceVersion: "v1"}, nil
	}, c.DecisionInterval, func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	})
	row := func(sid int32, bar int64, warm bool) *orm.DataSeries {
		price := 100 + float64(sid)*float64(bar) + float64((bar*bar+int64(sid))%7)
		return &orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, TimeMS: (bar - 1) * hour, EndMS: bar * hour, Closed: true, IsWarmUp: warm, Values: map[string]any{"close": price, "nullable": nil, "custom_int": int64(17)}}
	}
	// Actual provider startup warms an entire pair before the next pair.
	for _, sid := range []int32{1, 2, 3} {
		for bar := int64(1); bar <= 24; bar++ {
			consume(row(sid, bar, true))
		}
	}
	if len(sink.targets) != 0 || sink.navCalls != 0 {
		t.Fatal("historical warmup reached trading/budget admission")
	}
	for _, sid := range []int32{1, 2, 3} {
		consume(row(sid, 25, false))
	}
	if sink.navCalls != 1 || len(sink.targets) != 0 {
		t.Fatalf("live barrier did not use warmup exactly once: budget=%d targets=%d", sink.navCalls, len(sink.targets))
	}
	f.rt.Clock.SetTimeMS(25*hour + 5)
	for _, sid := range []int32{1, 2, 3} {
		consume(&orm.DataSeries{Source: "tick", TimeFrame: "event", Sid: sid, TimeMS: 25*hour + 4, EndMS: 25*hour + 4, Values: map[string]any{"price": 100.0, "nullable": nil, "custom_int": int64(17)}})
	}
	if len(sink.targets) != 1 || sink.targets[0].Spec().DecisionTime != 25*hour+2 {
		t.Fatal("first live target missing or logical grid replaced publication cutoff")
	}
}

func TestFactorLiveProductionConsumerFlushesClosedGridWithoutEnvEnd(t *testing.T) {
	f := newSharedTriggerFixture(t)
	raw, err := os.ReadFile("../factor/runner/example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c runner.Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Snapshot.SIDMap[1] = "BTC"
	for sid, symbol := range c.Snapshot.SIDMap {
		if err := f.rt.Symbols.CacheExSymbolChecked(&orm.ExSymbol{ID: sid, Symbol: symbol, Exchange: "<runtime-unconfigured>", Market: "<runtime-unconfigured>"}); err != nil {
			t.Fatal(err)
		}
	}
	c.Plan, err = factor.New().Add("close", factor.Field("kline", "close", "1h")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	sink := &closedGridSink{}
	f.rt.Clock.SetTimeMS(3600002)
	engine, err := runner.NewLive(c, sink, f.rt.Clock.TimeMS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()
	mapper := func(s *orm.DataSeries, received int64) (factor.VersionRecord, error) {
		if s.Values["nullable"] != nil || s.Values["custom_int"] != int64(17) {
			t.Fatal("arbitrary field type/NULL lost")
		}
		return factor.VersionRecord{Series: *s, EventTime: s.EndMS, AvailableAt: received, IngestedAt: received, Revision: 1, SourceVersion: "v1"}, nil
	}
	if _, err := f.rt.SubscribeFactorLive(&data.LiveProvider{}, engine, c, mapper); err == nil || !strings.Contains(err.Error(), "tick is not registered") {
		t.Fatalf("missing execution price source accepted although factor DAG only requests kline: %v", err)
	}
	consume := f.rt.factorLiveConsumer(engine, mapper, c.DecisionInterval, func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	})
	row := func(sid int32, source, freq, field string, at int64) *orm.DataSeries {
		return &orm.DataSeries{Source: source, TimeFrame: freq, Sid: sid, TimeMS: at - 1, EndMS: at, Closed: true, Values: map[string]any{field: float64(sid) * 100, "nullable": nil, "custom_int": int64(17)}}
	}
	for _, sid := range []int32{1, 2} {
		consume(row(sid, "kline", "1h", "close", 3600000))
	}
	if len(sink.targets) != 0 {
		t.Fatal("incomplete grid executed")
	}
	consume(row(3, "kline", "1h", "close", 3600000))
	f.rt.Clock.SetTimeMS(3600004)
	for _, sid := range []int32{1, 3} {
		consume(row(sid, "tick", "event", "price", 3600001))
	}
	if len(sink.targets) != 0 {
		t.Fatal("pre-completion quote executed")
	}
	f.rt.Clock.SetTimeMS(3600005)
	for _, sid := range []int32{1, 3} {
		consume(row(sid, "tick", "event", "price", 3600004))
	}
	if len(sink.targets) != 1 {
		t.Fatal("closed production callback failed to Flush", len(sink.targets))
	}
	if sink.targets[0].Spec().DecisionTime != 3600002 {
		t.Fatal("decision used event rather than completion clock")
	}
}
