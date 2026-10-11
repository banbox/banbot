package runner

import (
	"context"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/backtest"
	"math"
	"testing"
)

func TestNativePaperFillModelNeverFabricatesMissingSpread(t *testing.T) {
	for _, model := range []execution.PaperFillModel{execution.PaperBidAsk, execution.PaperMidEntryMarketExit} {
		c := paperConfig(t, archiveConfig(t, false))
		c.Execution.PaperFillModel = model
		sink, done, err := NewPaperSink(context.Background(), c)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range []backtest.Quote{{Price: 100, AtMS: 10, AvailableAt: 10}, {Price: 100, Bid: 101, Ask: 100, AtMS: 10, AvailableAt: 10}, {Price: 100, Bid: 99, Ask: math.Inf(1), AtMS: 10, AvailableAt: 10}} {
			if err = sink.ObserveQuote(context.Background(), 1, q, 10); err == nil {
				t.Fatal("runner manufactured bid/ask", model, q)
			}
			if len(sink.marks) != 0 || len(sink.observed) != 0 {
				t.Fatal("invalid spread published mark")
			}
		}
		q := backtest.Quote{Price: 100, Bid: 99, Ask: 101, AtMS: 10, AvailableAt: 10}
		if err = sink.ObserveQuote(context.Background(), 1, q, 10); err != nil {
			t.Fatal(err)
		}
		if err = done(); err != nil {
			t.Fatal(err)
		}
	}
}
