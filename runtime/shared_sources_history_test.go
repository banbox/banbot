package runtime

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
)

type liveHistoryHandle struct{ stopped, joined bool }

func (h *liveHistoryHandle) Stop()       { h.stopped = true }
func (h *liveHistoryHandle) Join() error { h.joined = true; return nil }

type liveHistorySource struct {
	name, frequency, field string
	subscriptions          []*orm.Subscription
	sink                   data.DataSink
	handle                 liveHistoryHandle
	fetches                int
}

func (s *liveHistorySource) Info() *orm.SeriesInfo {
	return orm.NewSeriesInfo(s.name, s.frequency, []orm.SeriesField{{Name: s.field, Type: "float"}})
}
func (s *liveHistorySource) FetchHistory(_ context.Context, sub *orm.Subscription, from, to int64) ([]*orm.DataRecord, error) {
	s.fetches++
	const hour int64 = 3600000
	var rows []*orm.DataRecord
	for bar := int64(1); bar <= 24; bar++ {
		if (bar-1)*hour < from || (bar-1)*hour >= to {
			continue
		}
		price := 100 + float64(sub.ExSymbol.ID)*float64(bar) + float64((bar*bar+int64(sub.ExSymbol.ID))%7)
		rows = append(rows, &orm.DataRecord{Sid: sub.ExSymbol.ID, TimeMS: (bar - 1) * hour, EndMS: bar * hour, Closed: true, Values: map[string]any{s.field: price, "custom_int": int64(9007199254740993), "nullable": nil}})
	}
	return rows, nil
}
func (*liveHistorySource) SubscribeLive(context.Context, []*orm.Subscription, data.DataSink) error {
	panic("managed source required")
}
func (s *liveHistorySource) SubscribeManaged(_ context.Context, subs []*orm.Subscription, sink data.DataSink) (data.LiveSourceSubscription, error) {
	s.subscriptions, s.sink = subs, sink
	if s.name == "side" {
		// Providers may queue their latest closed observation while history
		// is being fetched. Replaying it must not admit a historical target.
		for _, sub := range subs {
			if err := sink.Emit(sub, []*orm.DataRecord{{Sid: sub.ExSymbol.ID, TimeMS: 23 * 3600000, EndMS: 24 * 3600000, Closed: true, Values: map[string]any{"close": 100.0}}}); err != nil {
				return &s.handle, err
			}
		}
	}
	return &s.handle, nil
}

func TestFactorLiveSideHistoryWarmsBeforeFirstAdmission(t *testing.T) {
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
	c.Factor.Source = "side"
	c.Snapshot.Schemas["side"], c.Snapshot.SourceVersions["side"] = "schema-v1", "v1"
	for sid, symbol := range c.Snapshot.SIDMap {
		if err := f.rt.Symbols.CacheExSymbolChecked(&orm.ExSymbol{ID: sid, Symbol: symbol, Exchange: "<runtime-unconfigured>", Market: "<runtime-unconfigured>"}); err != nil {
			t.Fatal(err)
		}
	}
	const hour int64 = 3600000
	f.rt.Clock.SetTimeMS(24*hour + 2)
	targets := &closedGridSink{}
	engine, err := runner.NewLive(c, targets, f.rt.Clock.TimeMS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()
	side := &liveHistorySource{name: "side", frequency: "1h", field: "close"}
	tick := &liveHistorySource{name: "tick", frequency: "event", field: "price"}
	for _, source := range []data.DataSource{side, tick} {
		if err := f.rt.Catalog.RegisterDataSource(source); err != nil {
			t.Fatal(err)
		}
	}
	warmups := 0
	mapper := func(s *orm.DataSeries, received int64) (factor.VersionRecord, error) {
		if s.IsWarmUp {
			warmups++
			if s.Values["custom_int"] != int64(9007199254740993) {
				t.Fatal("warmup integer type or precision lost")
			}
			if value, ok := s.Values["nullable"]; !ok || value != nil {
				t.Fatal("warmup NULL lost")
			}
		}
		return factor.VersionRecord{Series: *s, EventTime: s.EndMS, AvailableAt: s.EndMS, IngestedAt: received, Revision: 1, SourceVersion: "v1"}, nil
	}
	failures, err := f.rt.SubscribeFactorLive(data.NewLiveSourceProvider(f.rt.Catalog), engine, c, mapper)
	if err != nil {
		t.Fatal(err)
	}
	if warmups != 72 || side.fetches != 3 || tick.fetches != 0 || targets.navCalls != 0 || len(targets.targets) != 0 {
		t.Fatalf("side history reached live admission or missing warmup: rows=%d fetches=%d budget=%d", warmups, side.fetches, targets.navCalls)
	}
	f.rt.Clock.SetTimeMS(25*hour + 2)
	for _, sub := range side.subscriptions {
		if err := side.sink.Emit(sub, []*orm.DataRecord{{Sid: sub.ExSymbol.ID, TimeMS: 24 * hour, EndMS: 25 * hour, Closed: true, Values: map[string]any{"close": 100 + float64(sub.ExSymbol.ID)*25 + float64((625+int64(sub.ExSymbol.ID))%7)}}}); err != nil {
			t.Fatal(err)
		}
	}
	if targets.navCalls != 1 || len(targets.targets) != 0 {
		t.Fatal("first live grid did not use preheated state exactly once")
	}
	f.rt.Clock.SetTimeMS(25*hour + 5)
	for _, sub := range tick.subscriptions {
		if err := tick.sink.Emit(sub, []*orm.DataRecord{{Sid: sub.ExSymbol.ID, TimeMS: 25*hour + 4, EndMS: 25*hour + 4, Values: map[string]any{"price": 100.0}}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(targets.targets) != 1 || targets.targets[0].Spec().DecisionTime != 25*hour+2 {
		t.Fatal("first valid live target missing after side history warmup")
	}
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	f.rt.Close()
	if !side.handle.stopped || !side.handle.joined || !tick.handle.joined {
		t.Fatal("side history installation not stopped and joined")
	}
}

func TestFactorLiveSideObservationCountCannotSubstituteForReadyGrids(t *testing.T) {
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
	c.Factor.Source, c.Factor.Window = "side", 2
	c.Snapshot.Schemas["side"], c.Snapshot.SourceVersions["side"] = "schema-v1", "v1"
	for sid, symbol := range c.Snapshot.SIDMap {
		if err := f.rt.Symbols.CacheExSymbolChecked(&orm.ExSymbol{ID: sid, Symbol: symbol, Exchange: "<runtime-unconfigured>", Market: "<runtime-unconfigured>"}); err != nil {
			t.Fatal(err)
		}
	}
	f.rt.Clock.SetTimeMS(3*3600000 + 2)
	targets := &closedGridSink{}
	engine, err := runner.NewLive(c, targets, f.rt.Clock.TimeMS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()
	side := &liveHistorySource{name: "side", frequency: "1h", field: "close"}
	tick := &liveHistorySource{name: "tick", frequency: "event", field: "price"}
	for _, source := range []data.DataSource{side, tick} {
		if err := f.rt.Catalog.RegisterDataSource(source); err != nil {
			t.Fatal(err)
		}
	}
	mapper := func(s *orm.DataSeries, received int64) (factor.VersionRecord, error) {
		// Every fetched source observation is complete and visible, but this
		// publication mapping cannot complete the declared hourly grid.
		return factor.VersionRecord{Series: *s, EventTime: s.EndMS + 1, AvailableAt: s.EndMS + 1, IngestedAt: received, Revision: 1, SourceVersion: "v1"}, nil
	}
	_, err = f.rt.SubscribeFactorLive(data.NewLiveSourceProvider(f.rt.Catalog), engine, c, mapper)
	if err == nil || !strings.Contains(err.Error(), "startup history incomplete") {
		t.Fatalf("source count accepted without complete grids: %v", err)
	}
	if side.fetches != 3 || targets.navCalls != 0 || len(targets.targets) != 0 || !side.handle.stopped || !side.handle.joined || !tick.handle.joined {
		t.Fatal("failed history readiness admitted live data or leaked sources")
	}
}
