package runtime

import (
	"context"
	"github.com/banbox/banbot/execution"
	"github.com/shopspring/decimal"
	"testing"
)

func TestSharedFactorUpdatePreservesPreparedLegacyConditions(t *testing.T) {
	f := newSharedTriggerFixture(t)
	a := f.rt.SharedExecution()
	i := f.bridge.Instruments["BTC"]
	if err := a.CashEvent(execution.CashEvent{ID: "split", Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Strategy: "ts", Amount: decimal.NewFromInt(-500)}, {Strategy: "cs", Amount: decimal.NewFromInt(500)}}, AtMS: 101}); err != nil {
		t.Fatal(err)
	}
	risk := f.bridge.Risk
	risk.StrategyGrossLimits["cs"] = decimal.NewFromInt(1000)
	if err := a.RegisterRiskPolicy("shared-risk-v1", risk); err != nil {
		t.Fatal(err)
	}
	risk.Marks = map[string]decimal.Decimal{"BTC": decimal.NewFromInt(90)}
	quote := execution.VisibleQuote{Bid: decimal.NewFromInt(90), Ask: decimal.NewFromInt(90), AtMS: 101, ReceivedMS: 101, ValidUntilMS: 301, Bar: 1}
	source := execution.EligibleIntent{ID: "original-limit", Account: f.key, Strategy: "ts", Lot: "pending", Instrument: "BTC", Side: execution.Buy, Kind: execution.EntryIntent, QuantitySteps: 10, Conditions: execution.IntentConditions{Limit: decimal.NewFromInt(90), CreatedBar: 1, StopBars: 5, ExpiresAtMS: 150}}
	old, err := a.PrepareRebalance(execution.CombinedRebalance{PlanID: "legacy-prepared", DecisionMS: 101, ExpiresMS: 150, Requests: []execution.InstrumentRebalance{{Instrument: i, Quote: quote, Targets: []execution.ExecutableTarget{{Strategy: "ts", Lot: "pending", SignedSteps: 10}}, IntentConstraints: []execution.EligibleIntent{source}}}, Risk: risk})
	if err != nil {
		t.Fatal(err)
	}
	request := execution.CombinedRebalance{PlanID: "factor-next", DecisionMS: 102, ExpiresMS: 200, Requests: []execution.InstrumentRebalance{{Instrument: i, Quote: quote, Targets: []execution.ExecutableTarget{{Strategy: "ts", Lot: "pending", SignedSteps: 10}, {Strategy: "cs", Lot: "cs", SignedSteps: 2}}}}, Risk: risk}
	request.Requests[0].Quote.Bid = decimal.NewFromInt(91)
	request.Requests[0].Quote.Ask = decimal.NewFromInt(91)
	if _, err := a.PrepareRebalance(request); err == nil {
		t.Fatal("factor update dropped otherstrategy limit")
	}
	latest, _ := a.LatestPlan(context.Background())
	if latest.ID != old.Plan.ID {
		t.Fatal("invalid carry partially superseded old plan")
	}
	request.Requests[0].Quote = quote
	prepared, err := a.PrepareRebalance(request)
	if err != nil {
		t.Fatal(err)
	}
	preserved := false
	for _, intent := range prepared.Plan.Intents {
		if intent.Strategy == "ts" && intent.Lot == "pending" && intent.ID != source.ID {
			if !intent.Conditions.Limit.Equal(source.Conditions.Limit) || intent.Conditions.StopBars != 5 || intent.Conditions.CreatedBar != 1 || intent.Conditions.ExpiresAtMS != 150 {
				t.Fatal("carried definition changed", intent)
			}
			preserved = true
		}
	}
	if !preserved {
		t.Fatal("new TS contributor absent")
	}
	if _, err := a.PrepareRebalance(request); err != nil {
		t.Fatal("frozen carried request retry changed", err)
	}
	for _, id := range prepared.OrderIDs {
		if err := a.Send(id, 151); err == nil {
			t.Fatal("factor update renewed expired TS contributor")
		}
	}
	if f.adapter.Metrics().Fills != 0 {
		t.Fatal("failed source carry emitted real order")
	}
}

func TestSharedSendUsesCurrentAccountClockAfterQuoteCollection(t *testing.T) {
	f := newSharedTriggerFixture(t)
	a := f.rt.SharedExecution()
	i := f.bridge.Instruments["BTC"]
	f.rt.Clock.SetTimeMS(101)
	q := execution.VisibleQuote{Bid: decimal.NewFromInt(100), Ask: decimal.NewFromInt(100), AtMS: 101, ReceivedMS: 101, ValidUntilMS: 150, Bar: 1}
	prepared, err := a.PrepareRebalance(execution.CombinedRebalance{PlanID: "clock-expiry", DecisionMS: 101, ExpiresMS: 150, Requests: []execution.InstrumentRebalance{{Instrument: i, Quote: q, Targets: []execution.ExecutableTarget{{Strategy: "ts", Lot: "clock-lot", SignedSteps: 10}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.OrderIDs) != 1 {
		t.Fatal("missing prepared order", prepared.OrderIDs)
	}
	// IO between quote collection and send advances the live clock, while the
	// caller still holds its original decision timestamp.
	f.rt.Clock.SetTimeMS(151)
	if err := a.Send(prepared.OrderIDs[0], 101); err == nil {
		t.Fatal("stale caller timestamp bypassed send expiry")
	}
	if f.adapter.Metrics().Fills != 0 {
		t.Fatal("expired order reached transport")
	}
}
