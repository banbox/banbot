package execution

import (
	"context"
	"testing"
)

func TestPaperStrategyFillCountDeduplicatesLots(t *testing.T) {
	venue, err := NewPaperAdapter(intentPrice("1000"), intentPrice("0"), intentPrice("0"))
	if err != nil {
		t.Fatal(err)
	}
	order := OrderIntent{ID: "net", Instrument: ledgerInstrument(), Side: Buy, Steps: 10, Observation: ExecutionObservation{Price: intentPrice("100"), AtMS: 10}, SubmitAtMS: 11, Allocations: []FillAllocation{{Strategy: "a", Steps: 3}, {Strategy: "a", Steps: 3}, {Strategy: "b", Steps: 4}, {Strategy: "zero", Steps: 0}}}
	if _, err := venue.Submit(context.Background(), order, "client"); err != nil {
		t.Fatal(err)
	}
	if venue.Metrics().Fills != 1 || venue.StrategyFillCount("a") != 1 || venue.StrategyFillCount("b") != 1 || venue.StrategyFillCount("zero") != 0 {
		t.Fatal("strategy fills double counted", venue.Metrics())
	}
}
