package execution

import (
	"context"
	"testing"
)

func contributor(s *Store, strategy StrategyID, lot VirtualLotID, side OrderSide, steps int64, limit, stop string) EligibleIntent {
	return EligibleIntent{ID: VirtualIntentID("original-" + string(strategy)), Account: s.key, Strategy: strategy, Lot: lot, Instrument: "BTC", Side: side, Kind: EntryIntent, QuantitySteps: steps, State: PendingCondition, Conditions: IntentConditions{Limit: intentPrice(limit), Stop: intentPrice(stop)}}
}

func TestContributorConditionsPreservedThroughCrossingAndResidualCaps(t *testing.T) {
	for _, scenario := range []string{"buy-cap", "sell-cap", "crossing"} {
		t.Run(scenario, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			fundStrategies(t, s)
			executor := executorFor(s, h, &fakeExecutionAdapter{})
			request := domainRequest("contributors", 1, 10, "100", 3, 4)
			constraints := []EligibleIntent{contributor(s, "a", "lot-a", Buy, 3, "100", "99"), contributor(s, "b", "lot-b", Buy, 4, "101", "98")}
			wantLimit := "100"
			if scenario == "sell-cap" {
				request = domainRequest("contributors", 1, 10, "102", -3, -4)
				constraints = []EligibleIntent{contributor(s, "a", "lot-a", Sell, 3, "100", "103"), contributor(s, "b", "lot-b", Sell, 4, "101", "104")}
				wantLimit = "101"
			}
			if scenario == "crossing" {
				request = domainRequest("contributors", 1, 10, "100", 4, -2)
				constraints = []EligibleIntent{contributor(s, "a", "lot-a", Buy, 4, "100", "99"), contributor(s, "b", "lot-b", Sell, 2, "99", "101")}
			}
			request.Requests[0].IntentConstraints = constraints
			ready, err := executor.PrepareRebalance(request)
			if err != nil {
				t.Fatal(err)
			}
			if len(ready.OrderIDs) != 1 {
				t.Fatal(ready)
			}
			order, err := s.Order(context.Background(), ready.OrderIDs[0])
			if err != nil || !order.Intent.Limit.Equal(intentPrice(wantLimit)) {
				t.Fatal("contributor price cap became market", order, err)
			}
			for _, intent := range ready.Plan.Intents {
				var source EligibleIntent
				for _, candidate := range constraints {
					if candidate.Strategy == intent.Strategy {
						source = candidate
					}
				}
				if !intent.Conditions.Limit.Equal(source.Conditions.Limit) || !intent.Conditions.Stop.Equal(source.Conditions.Stop) || !intent.Triggered {
					t.Fatal("distinct contributor conditions lost", intent, source)
				}
			}
			if scenario == "crossing" && len(ready.InternalMatchIDs) != 1 {
				t.Fatal("eligible conditioned pair did not internally cross", ready)
			}
			if err := executor.Send(ready.OrderIDs[0], 11); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestContributorIneligibleAndExpiredSendNeverSubmit(t *testing.T) {
	for _, scenario := range []string{"limit", "stop", "duplicate", "foreign", "expiry"} {
		t.Run(scenario, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			fundStrategies(t, s)
			adapter := &fakeExecutionAdapter{}
			executor := executorFor(s, h, adapter)
			request := domainRequest("contributor-reject", 1, 10, "100", 3, 0)
			constraint := contributor(s, "a", "lot-a", Buy, 3, "100", "99")
			switch scenario {
			case "limit":
				constraint.Conditions.Limit = intentPrice("99")
			case "stop":
				constraint.Conditions.Stop = intentPrice("101")
			case "foreign":
				constraint.Account.Account = "wrong"
			case "expiry":
				constraint.Conditions.ExpiresAtMS = 15
			}
			request.Requests[0].IntentConstraints = []EligibleIntent{constraint}
			if scenario == "duplicate" {
				request.Requests[0].IntentConstraints = append(request.Requests[0].IntentConstraints, constraint)
			}
			ready, err := executor.PrepareRebalance(request)
			if scenario == "expiry" {
				if err != nil {
					t.Fatal(err)
				}
				if err := executor.Send(ready.OrderIDs[0], 15); err == nil {
					t.Fatal("expired original contributor emitted residual")
				}
				order, err := s.Order(context.Background(), ready.OrderIDs[0])
				if err != nil || order.State != OrderPrepared {
					t.Fatal("expiry emitted transport attempt", order, err)
				}
			} else {
				if err == nil {
					t.Fatal("ineligible contributor prepared market order", scenario)
				}
				sequence, err := s.LatestPlanSequence(context.Background())
				if err != nil || sequence != -1 {
					t.Fatal("condition rejection partially persisted", sequence, err)
				}
			}
			if len(adapter.calls()) != 0 {
				t.Fatal("rejected contributor touched transport", adapter.calls())
			}
		})
	}
}
