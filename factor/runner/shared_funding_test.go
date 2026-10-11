package runner

import (
	"context"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/shopspring/decimal"
	"path/filepath"
	"testing"
)

func TestSharedFundingSameTickPendingReplay(t *testing.T) {
	for _, backend := range []string{"memory", "history", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			c := paperConfig(t, archiveConfig(t, false))
			c.Mode = Events
			// Put funding on the quote which executes each pending target. Runner one
			// trades before runner two observes that same funding identity.
			original, err := factor.OpenVersionStore(c.Chunks[0].Path, c.MaxRecords)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := original.Records()
			if err != nil {
				t.Fatal(err)
			}
			archive, _ := factor.NewVersionStore(c.MaxRecords)
			for _, r := range rows {
				if r.Series.Source == "funding" {
					r.EventTime--
					r.AvailableAt--
					r.IngestedAt--
					r.Series.TimeMS--
					r.Series.EndMS--
				}
				if err := archive.Put(r); err != nil {
					t.Fatal(err)
				}
			}
			c.Chunks[0].Path = filepath.Join(t.TempDir(), "same-tick.gob")
			if _, err := archive.Export(c.Chunks[0].Path); err != nil {
				t.Fatal(err)
			}
			if backend != "sqlite" {
				c.Execution.StorePath = ""
				c.Execution.SenderLeaseDir = ""
			}
			if backend == "history" {
				c.Execution.HistoryPath = filepath.Join(t.TempDir(), "history.sqlite")
			}
			c.InitialNAV = 4000
			c.AccountInitialNAV = 10000
			second := c
			second.StrategyID = "second"
			configs := []Config{c, second}
			ctx := context.Background()
			sinks, close, err := NewPaperSinks(ctx, configs)
			if err != nil {
				t.Fatal(err)
			}
			defer close()
			results, err := RunMany(ctx, configs, sinks, make([]Output, 2))
			if err != nil {
				t.Fatal(err)
			}
			if results[0].Executions == 0 || results[1].Executions == 0 {
				t.Fatal("pending execution was not replayed")
			}
		})
	}
}

func TestSharedFundingRequiresVisibleMark(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	c.Mode = Events
	c.Execution.StorePath = ""
	c.Execution.SenderLeaseDir = ""
	ctx := context.Background()
	sink, close, err := NewPaperSink(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer close()
	f := backtest.Funding{ID: "funding-visibility", SID: 1, AtMS: 12, AvailableAt: 12, Rate: .01}
	before, _ := sink.Account.Snapshot(ctx)
	if err := sink.ObserveFunding(ctx, f, 12); err == nil {
		t.Fatal("funding before first quote accepted")
	}
	i := sink.Instruments[1]
	sink.marks = map[string]decimal.Decimal{i.ID: decimal.NewFromInt(100)}
	for _, quote := range []execution.VisibleQuote{{AtMS: 13, ReceivedMS: 13}, {AtMS: 11, ReceivedMS: 13}, {AtMS: 11, ReceivedMS: 10}} {
		sink.observed = map[string]execution.VisibleQuote{i.ID: quote}
		if err := sink.ObserveFunding(ctx, f, 12); err == nil {
			t.Fatal("unavailable mark accepted", quote)
		}
	}
	after, _ := sink.Account.Snapshot(ctx)
	if before.Checkpoint != after.Checkpoint || !before.AccountSettledCash.Equal(after.AccountSettledCash) {
		t.Fatal("invalid mark mutated funding")
	}
	sink.observed = map[string]execution.VisibleQuote{i.ID: {AtMS: 11, ReceivedMS: 11}}
	if err := sink.ObserveFunding(ctx, f, 12); err != nil {
		t.Fatal("visible preceding quote rejected", err)
	}
	if err := sink.ObserveFunding(ctx, f, 13); err == nil {
		t.Fatal("late funding accepted")
	}
}
