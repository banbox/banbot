package runner

import (
	"context"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
)

func eventClockConfig(t *testing.T, rows []factor.VersionRecord) Config {
	t.Helper()
	c := archiveConfig(t, false)
	c.Mode = Weights
	c.SourceEventDecisions = true
	c.DecisionInterval = 50
	c.DecisionDelayMS = 0
	c.LatencyMS = 1
	c.ExpiryMS = 40
	c.MaxRecords = 100
	c.MaxPending = 100
	c.Factor.Source = "signal"
	c.Factor.TimeFrame = "event"
	c.Factor.Field = "close"
	c.Snapshot.Universe = factor.Universe{Version: "events", Investable: []int32{1, 2}, Reference: []int32{1, 2}, Tradable: []int32{1, 2}, Tracked: []int32{1, 2}, Evaluation: []int32{1, 2}, Static: true}
	c.Snapshot.Schemas["signal"] = "s1"
	c.Snapshot.SourceVersions["signal"] = "v1"
	var err error
	c.Plan, err = factor.New().Add("close", factor.Field("signal", "close", "event")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	c.Manifest.Portfolio.K = 1
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Labels = nil
	store, err := factor.NewVersionStore(100)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := store.Put(r); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "events.gob")
	if _, err := store.Export(path); err != nil {
		t.Fatal(err)
	}
	c.Chunks = []Chunk{{Path: path, From: 90, To: 330}}
	return c
}

func clockRow(source string, sid int32, event, published, received int64, closed bool) factor.VersionRecord {
	values := map[string]any{"close": float64(sid), "price": 100 + float64(sid), "custom": int64(1<<53 + 1), "null": nil}
	return factor.VersionRecord{Series: orm.DataSeries{Source: source, TimeFrame: "event", Sid: sid, TimeMS: event, EndMS: event, Closed: closed, Values: values}, EventTime: event, AvailableAt: published, IngestedAt: received, Revision: 1, SourceVersion: "v1"}
}

func TestSourceEventClockIrregularPITAndPendingExecution(t *testing.T) {
	var rows []factor.VersionRecord
	for _, sid := range []int32{1, 2} {
		for _, clocks := range [][3]int64{{100, 102, 110}, {175, 180, 185}, {275, 280, 289}} {
			rows = append(rows, clockRow("signal", sid, clocks[0], clocks[1], clocks[2], true))
		}
		// Prices process pending positions independently of source decisions.
		for _, at := range []int64{99, 112, 122, 170, 187, 197, 270, 291, 301} {
			rows = append(rows, clockRow("tick", sid, at, at, at, true))
		}
		rows = append(rows, clockRow("signal", sid, 225, 230, 230, false))
	}
	for _, mode := range []Mode{Weights, Events, Research} {
		t.Run(string(mode), func(t *testing.T) {
			c := eventClockConfig(t, rows)
			c.Mode = mode
			if mode == Events {
				c = paperConfig(t, c)
			}
			if mode == Research {
				c.Manifest.Labels = []research.LabelSpec{{Name: "10ms", Kind: research.ExecutableReturn, Horizon: 10, PeriodsPerYear: 100, Overlapping: true}}
			}
			out := &parityOutput{capture: capture{targets: map[int64]map[int32]float64{}}}
			r, err := Run(context.Background(), c, nil, out)
			if err != nil {
				t.Fatal(err)
			}
			if r.Decisions != 3 || r.Incomplete != 0 || len(out.frames) != 3 {
				t.Fatal(r, len(out.frames))
			}
			var grids, cutoffs []int64
			for _, frame := range out.frames {
				grids = append(grids, frame.GridTime)
				cutoffs = append(cutoffs, frame.DecisionTime)
			}
			if !reflect.DeepEqual(grids, []int64{100, 175, 275}) || !reflect.DeepEqual(cutoffs, []int64{110, 185, 289}) {
				t.Fatal(grids, cutoffs)
			}
			if mode != Research && (r.Executions != 3 || !reflect.DeepEqual(out.executed, []int64{112, 187, 291})) {
				t.Fatal("quote-only batches did not process pending execution", r.Executions, out.executed)
			}
			if !strings.Contains(r.Manifest.LatencyAssumption, "no synthetic periodic decisions") {
				t.Fatal(r.Manifest.LatencyAssumption)
			}
		})
	}
}

func TestSourceEventClockIncompleteSIDBarrierRetriesSameGrid(t *testing.T) {
	rows := []factor.VersionRecord{clockRow("signal", 1, 100, 110, 110, true), clockRow("signal", 2, 100, 113, 113, true), clockRow("signal", 1, 175, 180, 180, true), clockRow("signal", 2, 275, 280, 280, true)}
	c := eventClockConfig(t, rows)
	out := &parityOutput{capture: capture{targets: map[int64]map[int32]float64{}}}
	r, err := Run(context.Background(), c, nil, out)
	if err != nil {
		t.Fatal(err)
	}
	if r.Decisions != 1 || r.Incomplete != 3 || len(out.frames) != 1 || out.frames[0].GridTime != 100 || out.frames[0].DecisionTime != 113 {
		t.Fatal(r, out.frames)
	}
	if out.frames[0].Values["close"][2].Value != 2 {
		t.Fatal("late receipt lost from completed barrier", out.frames)
	}
}

func TestSourceEventClockRejectsAmbiguousAndBackdatedCompletedGrid(t *testing.T) {
	for _, test := range []struct {
		name    string
		rows    []factor.VersionRecord
		message string
	}{
		{"ambiguous", []factor.VersionRecord{clockRow("signal", 1, 100, 190, 190, true), clockRow("signal", 2, 175, 190, 190, true)}, "ambiguous source decision batch"},
		{"backwards", []factor.VersionRecord{clockRow("signal", 1, 175, 180, 180, true), clockRow("signal", 2, 175, 180, 180, true), clockRow("signal", 1, 100, 190, 190, true)}, "moved backwards"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := eventClockConfig(t, test.rows)
			if _, err := Run(context.Background(), c, nil, nil); err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatal(err)
			}
		})
	}
}

func TestSourceEventClockConfigurationAndIdentitySeparation(t *testing.T) {
	c := eventClockConfig(t, []factor.VersionRecord{clockRow("signal", 1, 100, 100, 100, true)})
	if err := ValidateReplayConfig(c, true); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Config){func(c *Config) { c.Mode = Trade }, func(c *Config) { c.DecisionDelayMS = 1 }, func(c *Config) { c.Factor.Source = "" }, func(c *Config) { c.Factor.TimeFrame = "" }} {
		copy := c
		mutate(&copy)
		if err := ValidateReplayConfig(copy, true); err == nil {
			t.Fatal("invalid source clock accepted", copy)
		}
	}
	if err := ValidateLiveConfig(c); err == nil {
		t.Fatal("historical clock accepted by live driver")
	}
	baseline := c
	baseline.SourceEventDecisions = false
	a, err := runAssumptions(c)
	if err != nil {
		t.Fatal(err)
	}
	b, err := runAssumptions(baseline)
	if err != nil || a == b {
		t.Fatal("run assumption collision", a, b, err)
	}
	a, err = computationKey(c, c.Plan)
	if err != nil {
		t.Fatal(err)
	}
	b, err = computationKey(baseline, c.Plan)
	if err != nil || a == b {
		t.Fatal("computation clock collision", err)
	}
	if sameReplayTimeline(c, baseline) {
		t.Fatal("shared timeline mixed source and periodic clocks")
	}
	different := c
	different.Factor.Source = "other"
	if sameReplayTimeline(c, different) {
		t.Fatal("different source clocks shared a timeline")
	}
	input, err := openArchiveInput(context.Background(), c, c.Chunks[0])
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	driver := newReplayInput(input, c, c.Chunks[0])
	batch, err := driver.Next(context.Background())
	if err != nil || batch.AtMS != 100 {
		t.Fatal(batch, err)
	}
	if _, err := driver.Next(context.Background()); err != io.EOF {
		t.Fatal("synthetic batch after last source event", err)
	}
}

func TestSourceDecisionGridIgnoresNonDecisionBatchesAndRejectsUnreceivedRecord(t *testing.T) {
	c := Config{}
	c.Factor.Source = "signal"
	c.Factor.TimeFrame = "event"
	rows := []factor.VersionRecord{clockRow("tick", 1, 100, 100, 100, true), clockRow("funding", 1, 100, 100, 100, true), clockRow("option-expiry", 1, 100, 100, 100, true), clockRow("signal", 1, 100, 100, 100, false)}
	otherTimeFrame := clockRow("signal", 1, 100, 100, 100, true)
	otherTimeFrame.Series.TimeFrame = "1h"
	rows = append(rows, otherTimeFrame)
	if grid, err := sourceDecisionGrid(c, HistoricalBatch{AtMS: 100, Records: rows}); err != nil || grid != 0 {
		t.Fatal(grid, err)
	}
	rows = append(rows, clockRow("signal", 1, 90, 91, 100, true))
	if grid, err := sourceDecisionGrid(c, HistoricalBatch{AtMS: 100, Records: rows}); err != nil || grid != 90 {
		t.Fatal(grid, err)
	}
	if _, err := sourceDecisionGrid(c, HistoricalBatch{AtMS: 99, Records: rows}); err == nil {
		t.Fatal("future local receipt used for source decision")
	}
}

type sourceClockQuoteWitness struct {
	*AccountSink
	observed []int64
}

func (s *sourceClockQuoteWitness) ObserveQuote(ctx context.Context, sid int32, q backtest.Quote, now int64) error {
	s.observed = append(s.observed, q.AtMS)
	return s.AccountSink.ObserveQuote(ctx, sid, q, now)
}

func TestSourceEventClockLateWarmupDoesNotSuppressCurrentQuotes(t *testing.T) {
	var rows []factor.VersionRecord
	for _, sid := range []int32{1, 2} {
		rows = append(rows, clockRow("signal", sid, 100, 160, 160, true), clockRow("tick", sid, 160, 160, 160, true), clockRow("signal", sid, 175, 185, 185, true), clockRow("tick", sid, 187, 187, 187, true))
	}
	c := eventClockConfig(t, rows)
	c.Mode = Events
	c.Chunks[0].From = 150
	c = paperConfig(t, c)
	base, done, err := NewPaperSink(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	witness := &sourceClockQuoteWitness{AccountSink: base}
	r, err := Run(context.Background(), c, witness, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Decisions != 1 || r.Executions != 1 || !reflect.DeepEqual(witness.observed, []int64{160, 160, 187, 187}) {
		t.Fatal(r.Decisions, r.Executions, witness.observed)
	}
}
