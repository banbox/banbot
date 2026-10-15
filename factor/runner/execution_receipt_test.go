package runner

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/orm"
)

type executionReceiptPages struct {
	rows  []factor.VersionRecord
	flags map[string]bool
}

func (f executionReceiptPages) Identity() string { return "execution-receipt-fixture" }
func (f executionReceiptPages) Open(_ context.Context, stream StorageStream, _, _ int64, receipt bool) (VersionPageReader, error) {
	f.flags[stream.Subscription.Key().String()] = receipt
	var rows []factor.VersionRecord
	for _, r := range f.rows {
		if r.Series.Source == stream.Subscription.Source && r.Series.Sid == stream.Subscription.ExSymbol.ID {
			rows = append(rows, r)
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].IngestedAt < rows[j].IngestedAt })
	return &strictFixtureReader{rows: rows}, nil
}

func receiptPagedConfig(t *testing.T, c Config, rows []factor.VersionRecord, flags map[string]bool) Config {
	t.Helper()
	streams := map[string]StorageStream{}
	for _, r := range rows {
		sub := orm.Subscription{Source: r.Series.Source, TimeFrame: r.Series.TimeFrame, ExSymbol: &orm.ExSymbol{ID: r.Series.Sid}}
		streams[sub.Key().String()] = StorageStream{Subscription: sub, SourceVersion: "v1", SchemaHash: "s1"}
	}
	var list []StorageStream
	for _, stream := range streams {
		list = append(list, stream)
	}
	factory, err := NewStorageInputFactory(StorageInputOptions{Namespace: "receipt", PITPolicy: "strict", FromMS: c.Chunks[0].From, ToMS: c.Chunks[0].To, PageRows: 1, Streams: list, VersionPages: executionReceiptPages{rows, flags}})
	if err != nil {
		t.Fatal(err)
	}
	c.HistoricalInput, c.Chunks = factory, nil
	c.Snapshot.VisibilityPolicy = "strict"
	return c
}

func TestExecutionReceiptPagedStreamsAndRawPublication(t *testing.T) {
	var rows []factor.VersionRecord
	for _, source := range []string{"tick", "funding", "signal"} {
		r := clockRow(source, 1, 100, 100, 200, true)
		r.Series.Values["rate"] = .01
		rows = append(rows, r)
	}
	c := eventClockConfig(t, rows)
	c.SourceEventDecisions = false
	c.Manifest.Costs.FundingPolicy = "required-stream"
	flags := map[string]bool{}
	c = receiptPagedConfig(t, c, rows, flags)
	input, err := c.HistoricalInput.Open(context.Background(), c, c.HistoricalInput.Ranges()[0])
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	first, err := input.Next(context.Background())
	if err != nil || first.AtMS != 100 || len(first.Records) != 1 || first.Records[0].Series.Source != "signal" {
		t.Fatal(first, err)
	}
	visible, err := input.Visible(context.Background(), 100, 100, 0)
	if err != nil || len(visible) != 0 {
		t.Fatal("unreceived raw row became decision-visible", visible, err)
	}
	second, err := input.Next(context.Background())
	if err != nil || second.AtMS != 200 || len(second.Records) != 2 {
		t.Fatal(second, err)
	}
	for _, r := range rows {
		sub := orm.Subscription{Source: r.Series.Source, TimeFrame: r.Series.TimeFrame, ExSymbol: &orm.ExSymbol{ID: 1}}
		want := r.Series.Source != "signal"
		if flags[sub.Key().String()] != want {
			t.Fatal("wrong per-stream receipt flag", r.Series.Source, flags)
		}
	}
}

func TestExecutionReceiptRawVisibilityRetainsPendingReceipt(t *testing.T) {
	rows := []factor.VersionRecord{clockRow("signal", 1, 100, 100, 100, true), clockRow("signal", 1, 150, 150, 250, true)}
	c := eventClockConfig(t, rows)
	c.SourceEventDecisions = false
	c = receiptPagedConfig(t, c, rows, map[string]bool{})
	ctx := context.Background()
	input, err := c.HistoricalInput.Open(ctx, c, c.HistoricalInput.Ranges()[0])
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	for _, at := range []int64{100, 150} {
		batch, err := input.Next(ctx)
		if err != nil || batch.AtMS != at {
			t.Fatal(batch, err)
		}
		visible, err := input.Visible(ctx, at, at, 0)
		if err != nil || len(visible) != 1 || visible[0].EventTime != 100 {
			t.Fatal("pending receipt hid received predecessor", visible, err)
		}
	}
	visible, err := input.Visible(ctx, 150, 250, 0)
	if err != nil || len(visible) != 1 || visible[0].EventTime != 150 {
		t.Fatal("pending receipt was pruned", visible, err)
	}
}

type executionReceiptWitness struct {
	base     *AccountSink
	quotes   []backtest.Quote
	quoteNow []int64
	funding  int
}

func (w *executionReceiptWitness) ObserveQuote(ctx context.Context, sid int32, q backtest.Quote, now int64) error {
	w.quotes = append(w.quotes, q)
	w.quoteNow = append(w.quoteNow, now)
	if w.base != nil {
		return w.base.ObserveQuote(ctx, sid, q, now)
	}
	return nil
}
func (w *executionReceiptWitness) ObserveFunding(ctx context.Context, f backtest.Funding, now int64) error {
	w.funding++
	if w.base != nil {
		return w.base.ObserveFunding(ctx, f, now)
	}
	return nil
}
func (w *executionReceiptWitness) ProcessSnapshot(ctx context.Context, p *factor.TargetPortfolio, q map[int32]backtest.Quote, now int64) error {
	if w.base != nil {
		return w.base.ProcessSnapshot(ctx, p, q, now)
	}
	return nil
}
func (w *executionReceiptWitness) StrategyNAV(ctx context.Context, now int64) (float64, error) {
	if w.base != nil {
		return w.base.StrategyNAV(ctx, now)
	}
	return 10000, nil
}
func (w *executionReceiptWitness) StrategyState(ctx context.Context, now int64) (backtest.State, error) {
	if w.base != nil {
		return w.base.StrategyState(ctx, now)
	}
	return backtest.State{NAV: 10000}, nil
}

func TestOrdinaryRunnerDelayedExecutionQuoteReceipt(t *testing.T) {
	for _, mode := range []Mode{Weights, Events} {
		for _, paged := range []bool{false, true} {
			t.Run(string(mode)+map[bool]string{false: "/archive", true: "/paged"}[paged], func(t *testing.T) {
				var rows []factor.VersionRecord
				for _, sid := range []int32{1, 2} {
					for _, event := range []int64{100, 150, 200} {
						rows = append(rows, clockRow("signal", sid, event, event, event, true), clockRow("tick", sid, event+1, event+1, event+25, true))
					}
				}
				c := eventClockConfig(t, rows)
				c.SourceEventDecisions = false
				c.Chunks[0].From = 100
				c.Chunks[0].To = 250
				c.Mode = mode
				if paged {
					c = receiptPagedConfig(t, c, rows, map[string]bool{})
				}
				w := &executionReceiptWitness{}
				if mode == Events {
					c = paperConfig(t, c)
					base, done, err := NewPaperSink(context.Background(), c)
					if err != nil {
						t.Fatal(err)
					}
					defer done()
					w.base = base
				}
				c.ObserveBatch = func(ctx context.Context, b HistoricalBatch) error {
					if b.AtMS < 125 && len(w.quotes) != 0 {
						t.Fatal("quote marked before receipt", b.AtMS, w.quotes)
					}
					if w.base != nil && b.AtMS < 125 && (len(w.base.marks) != 0 || w.base.Paper.Metrics().Fills != 0) {
						t.Fatal("account changed before receipt")
					}
					if w.base != nil && b.AtMS == 150 {
						q, err := w.base.Account.VisibleQuote(ctx, c.Execution.Instruments[1].ID, 150)
						if err != nil || q.AtMS != 101 || q.ReceivedMS != 125 {
							t.Fatal("stored receipt rewritten", q, err)
						}
					}
					return nil
				}
				out := &capture{targets: map[int64]map[int32]float64{}}
				r, err := Run(context.Background(), c, w, out)
				if err != nil {
					t.Fatal(err)
				}
				if r.Executions != 3 || !reflect.DeepEqual(out.executed, []int64{125, 175, 225}) {
					t.Fatal("execution chronology", r.Executions, out.executed)
				}
				if !reflect.DeepEqual(w.quoteNow, []int64{125, 125, 175, 175, 225, 225}) {
					t.Fatal("quote observed early or re-observed", w.quoteNow)
				}
				for i, q := range w.quotes {
					if q.AtMS != 101+int64(i/2)*50 || q.AvailableAt != w.quoteNow[i] {
						t.Fatal("quote clocks lost", q)
					}
				}
			})
		}
	}
}

func TestOrdinaryRunnerDelayedFundingRejectsBeforeMutation(t *testing.T) {
	for _, mode := range []Mode{Weights, Events} {
		for _, paged := range []bool{false, true} {
			t.Run(string(mode)+map[bool]string{false: "/archive", true: "/paged"}[paged], func(t *testing.T) {
				var rows []factor.VersionRecord
				for _, sid := range []int32{1, 2} {
					rows = append(rows, clockRow("signal", sid, 100, 100, 100, true), clockRow("tick", sid, 101, 101, 125, true))
					f := clockRow("funding", sid, 130, 130, 200, true)
					f.Series.Values["rate"] = .01
					rows = append(rows, f)
				}
				c := eventClockConfig(t, rows)
				c.SourceEventDecisions = false
				c.Chunks[0].From = 100
				c.Chunks[0].To = 250
				c.Mode = mode
				c.Manifest.Costs.FundingPolicy = "required-stream"
				if paged {
					c = receiptPagedConfig(t, c, rows, map[string]bool{})
				}
				w := &executionReceiptWitness{}
				var before backtest.State
				var lastObserved int64
				if mode == Events {
					c = paperConfig(t, c)
					base, done, err := NewPaperSink(context.Background(), c)
					if err != nil {
						t.Fatal(err)
					}
					defer done()
					w.base = base
				}
				c.ObserveBatch = func(ctx context.Context, b HistoricalBatch) error {
					lastObserved = b.AtMS
					if b.AtMS < 200 && w.funding != 0 {
						t.Fatal("premature funding")
					}
					if w.base != nil && b.AtMS == 150 {
						var err error
						before, err = w.base.StrategyState(ctx, 150)
						return err
					}
					return nil
				}
				_, err := Run(context.Background(), c, w, nil)
				if err == nil || !strings.Contains(err.Error(), "funding settlement unavailable or delivered late") || w.funding != 0 || lastObserved != 150 {
					t.Fatal("late funding consumed or rejected before receipt", err, w.funding, lastObserved)
				}
				if w.base != nil {
					after, err := w.base.Account.Snapshot(context.Background())
					cash := after.SyntheticStrategyCash[execution.StrategyID(c.StrategyID)].InexactFloat64()
					if err != nil || cash != before.Cash {
						t.Fatal("late funding mutated account cash", before, after, err)
					}
				}
			})
		}
	}
}
