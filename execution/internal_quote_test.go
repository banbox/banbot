package execution

import (
	"context"
	"testing"
)

func TestInternalQuoteVisibilityTickAndBothIntentConditions(t *testing.T) {
	for _, scenario := range []string{"midpoint", "expired", "received-late", "timestamp-after-receipt", "tick-outside", "buy-limit", "sell-limit", "buy-stop", "sell-stop"} {
		t.Run(scenario, func(t *testing.T) {
			s, _, _, _ := testStore(t)
			fundStrategies(t, s)
			buy, sell := testIntent(Buy), testIntent(Sell)
			buy.ID, sell.ID = "buy", "sell"
			buy.Account, sell.Account = s.key, s.key
			buy.Strategy, sell.Strategy = "a", "b"
			buy.Lot, sell.Lot = "buy-lot", "sell-lot"
			buy.Conditions = IntentConditions{Limit: intentPrice("101"), Stop: intentPrice("99")}
			sell.Conditions = IntentConditions{Limit: intentPrice("99"), Stop: intentPrice("101")}
			instrument := ledgerInstrument()
			quote := VisibleQuote{Bid: intentPrice("100.01"), Ask: intentPrice("100.04"), AtMS: 10, ReceivedMS: 11, ValidUntilMS: 30, Bar: 1}
			switch scenario {
			case "expired":
				quote.ValidUntilMS = 20
			case "received-late":
				quote.ReceivedMS = 21
			case "timestamp-after-receipt":
				quote.AtMS = 12
			case "tick-outside":
				instrument.PriceTick = intentPrice("1")
			case "buy-limit":
				buy.Conditions.Limit = intentPrice("99")
			case "sell-limit":
				sell.Conditions.Limit = intentPrice("101")
			case "buy-stop":
				buy.Conditions.Stop = intentPrice("101")
			case "sell-stop":
				sell.Conditions.Stop = intentPrice("99")
			}
			ctx := context.Background()
			if err := s.SavePlan(ctx, Plan{ID: "quote-plan", Sequence: 1, DecisionMS: 10, ExpiresMS: 100, Intents: []EligibleIntent{buy, sell}}); err != nil {
				t.Fatal(err)
			}
			before, err := s.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			applied, err := s.ApplyInternalMatch(ctx, InternalMatch{ID: "quote-match", PlanID: "quote-plan", Instrument: instrument, BuyIntent: buy.ID, SellIntent: sell.ID, Steps: 1, Quote: quote, AtMS: 20})
			if scenario != "midpoint" {
				if err == nil || applied {
					t.Fatal("invalid quote/intent internally filled", scenario)
				}
				after, err := s.Snapshot(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if len(after.Lots) != 0 || after.Checkpoint != before.Checkpoint {
					t.Fatal("rejected crossing mutated books")
				}
				return
			}
			if err != nil || !applied {
				t.Fatal(applied, err)
			}
			snapshot, err := s.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Lots) != 2 || len(snapshot.ActualPositions) != 0 || !snapshot.AccountSettledCash.Equal(before.AccountSettledCash) {
				t.Fatal("internal crossing changed actual account", snapshot)
			}
			for _, lot := range snapshot.Lots {
				if !lot.CostBasis.Equal(intentPrice("10.002")) {
					t.Fatal("midpoint was not rounded down to tick", lot)
				}
			}
		})
	}
}
