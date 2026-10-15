package execution

import (
	"context"
	"fmt"
	"testing"

	"github.com/shopspring/decimal"
)

func TestPaperSourceMidEntryMarketExitChargesBothOptionSpreads(t *testing.T) {
	for _, side := range []OrderSide{Buy, Sell} {
		paper, err := NewPaperAdapterWithFillModel(intentPrice("2000"), decimal.Zero, decimal.Zero, PaperMidEntryMarketExit)
		if err != nil {
			t.Fatal(err)
		}
		for n, right := range []string{"call", "put"} {
			i := cashOption(right)
			i.ID = right
			i.ContractSize = intentPrice("100")
			i.PriceTick = intentPrice("0.1")
			entry := OrderIntent{ID: fmt.Sprintf("entry-%d", n), Instrument: i, Side: side, Steps: 1, SubmitAtMS: 10, Observation: ExecutionObservation{Price: intentPrice("5"), Bid: intentPrice("4.9"), Ask: intentPrice("5.1"), AtMS: 10, ValidUntilMS: 30}}
			if _, err = paper.Submit(context.Background(), entry, entry.ID); err != nil {
				t.Fatal(err)
			}
			if !paper.Metrics().LastFill.Price.Equal(intentPrice("5")) {
				t.Fatal("source midpoint entry", paper.Metrics())
			}
			exit := entry
			exit.ID = fmt.Sprintf("exit-%d", n)
			exit.ReduceOnly = true
			exit.SubmitAtMS = 20
			exit.Observation.AtMS = 20
			exit.Side = Buy
			if side == Buy {
				exit.Side = Sell
			}
			if _, err = paper.Submit(context.Background(), exit, exit.ID); err != nil {
				t.Fatal(err)
			}
			want := "5.1"
			if side == Buy {
				want = "4.9"
			}
			if !paper.Metrics().LastFill.Price.Equal(intentPrice(want)) {
				t.Fatal("source market exit", paper.Metrics())
			}
		}
		if !paper.cash.Equal(intentPrice("1980")) {
			t.Fatal("unchanged two-leg premium must lose20 from spreads", side, paper.cash)
		}
		for _, position := range paper.positions {
			if position.steps != 0 {
				t.Fatal("roundtrip option left open")
			}
		}
	}
}

func TestPaperBidAskModelAndUnknownModel(t *testing.T) {
	if _, err := NewPaperAdapterWithFillModel(intentPrice("1000"), decimal.Zero, decimal.Zero, "magic"); err == nil {
		t.Fatal("unknown paper assumption")
	}
	paper, _ := NewPaperAdapterWithFillModel(intentPrice("1000"), decimal.Zero, decimal.Zero, PaperBidAsk)
	i := cashOption("call")
	i.PriceTick = intentPrice("0.1")
	o := OrderIntent{ID: "market", Instrument: i, Side: Buy, Steps: 1, SubmitAtMS: 10, Observation: ExecutionObservation{Price: intentPrice("5"), AtMS: 10, ValidUntilMS: 20}}
	if _, err := paper.Submit(context.Background(), o, o.ID); err == nil {
		t.Fatal("bidask fill inferred without spread")
	}
	o.Observation.Bid = intentPrice("4.9")
	o.Observation.Ask = intentPrice("5.1")
	if _, err := paper.Submit(context.Background(), o, o.ID); err != nil {
		t.Fatal(err)
	}
	if !paper.Metrics().LastFill.Price.Equal(intentPrice("5.1")) {
		t.Fatal(paper.Metrics())
	}
}
