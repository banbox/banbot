package execution

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/shopspring/decimal"
)

func TestPendingProtectionCheckpointRetainsTrailingAnchor(t *testing.T) {
	s, _, _, _ := testStore(t)
	pending := pendingTestOrder(s, "a")
	specs := []PositionProtection{{Kind: TrailingProtection, TrailingPercent: intentPrice("5")}}
	kind, steps, err := pending.AdvanceProtection(s.Account(), ledgerInstrument(), specs, 10, intentPrice("100"), intentPrice("110"), 12, 5)
	if err != nil || kind != "" || steps != 0 {
		t.Fatal(kind, steps, err)
	}
	raw, err := json.Marshal(pending)
	if err != nil {
		t.Fatal(err)
	}
	var restored PendingOrder
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	kind, steps, err = restored.AdvanceProtection(s.Account(), ledgerInstrument(), nil, 10, intentPrice("100"), intentPrice("104"), 13, 6)
	if err != nil || kind != TrailingProtection || steps != 10 {
		t.Fatal("trailing anchor lost at checkpoint", kind, steps, err)
	}
	if err := restored.CommitProtection(kind, steps, 10, 12); err != nil {
		t.Fatal(err)
	}
	if restored.Exit.QuantitySteps != 12 || restored.Desired != 0 {
		t.Fatal("full trailing reduction lost late fills", restored)
	}
}

func TestPendingProtectionSelectsStopBeforeTakeProfit(t *testing.T) {
	s, _, _, _ := testStore(t)
	pending := pendingTestOrder(s, "a")
	specs := []PositionProtection{{Kind: TakeProfitProtection, Level: intentPrice("90")}, {Kind: StopLossProtection, Level: intentPrice("110")}}
	kind, steps, err := pending.AdvanceProtection(s.Account(), ledgerInstrument(), specs, 10, intentPrice("100"), intentPrice("100"), 12, 5)
	if err != nil || kind != StopLossProtection || steps != 10 {
		t.Fatal("nondeterministic protection precedence", kind, steps, err)
	}
}

func TestPendingExpiredExplicitExitDoesNotDisableProtection(t *testing.T) {
	store, _, _, _ := testStore(t)
	pending := pendingTestOrder(store, "a")
	pending.Exit = &EligibleIntent{State: Expired}
	kind, steps, err := pending.AdvanceProtection(store.Account(), ledgerInstrument(), []PositionProtection{{Kind: StopLossProtection, Level: intentPrice("95")}}, 10, intentPrice("100"), intentPrice("94"), 12, 5)
	if err != nil || kind != StopLossProtection || steps != 10 {
		t.Fatal("expired exit disabled stop loss", kind, steps, err)
	}
	if err := pending.CommitProtection(kind, steps, 10, 10); err != nil {
		t.Fatal(err)
	}
	if pending.Desired != 0 || pending.Exit.State == Expired {
		t.Fatal("stop loss did not replace expired exit", pending)
	}
}

func pendingTestQuote(price string) map[string]VisibleQuote {
	return map[string]VisibleQuote{"BTC": {Bid: intentPrice(price), Ask: intentPrice(price), AtMS: 12, ReceivedMS: 12, ValidUntilMS: 1000, Bar: 5}}
}

func pendingTestOrder(s *Store, strategy string) *PendingOrder {
	i := testIntent(Buy)
	i.ID, i.Account, i.Strategy, i.Lot, i.Instrument = VirtualIntentID("pending-"+strategy), s.Account(), StrategyID(strategy), VirtualLotID("lot-"+strategy), "BTC"
	i.Conditions.CreatedBar = 5
	return &PendingOrder{Strategy: i.Strategy, Lot: i.Lot, Entry: i}
}

func TestPendingProtectionSettlesFinalEntryFill(t *testing.T) {
	for _, rate := range []string{"0", "0.5"} {
		t.Run(rate, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			o := planOrder(t, s, "pending-protection", 1, Buy, EntryIntent, map[string]int64{"a": 10})
			a := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
			e := executorFor(s, h, a)
			service := &SharedAccount{store: s, owner: h, executor: e}
			if err := e.Send(o.ID, 11); err != nil {
				t.Fatal(err)
			}
			if err := e.ApplyTrade(FillReport{EventID: "initial", OrderID: o.ID, Steps: 6, Price: intentPrice("100"), AtMS: 12}); err != nil {
				t.Fatal(err)
			}
			a.query = func(context.Context, string, string) (QueryResult, error) {
				return QueryResult{Found: true, Complete: true, Authoritative: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: "exchange-" + o.ID, Fills: []FillReport{{EventID: "final", OrderID: o.ID, Steps: 8, Price: intentPrice("100"), Cost: intentPrice("80"), Cumulative: true, AtMS: 13}}}}, nil
			}
			pending := pendingTestOrder(s, "a")
			triggers, err := service.AdvancePendingProtections(context.Background(), []PendingProtectionRequest{{Order: pending, Instrument: ledgerInstrument(), Specs: []PositionProtection{{Kind: StopLossProtection, Distance: intentPrice("5"), Rate: intentPrice(rate)}}}}, pendingTestQuote("94"), 13)
			if err != nil {
				t.Fatal(err)
			}
			want := int64(8)
			if rate != "0" {
				want = 3
			}
			if len(triggers) != 1 || triggers[0].Kind != StopLossProtection || pending.Exit == nil || pending.Exit.QuantitySteps != want || pending.Desired != 8-want || pending.Entry.State != Canceled {
				t.Fatalf("final fill not reflected in protection: triggers=%+v pending=%+v", triggers, pending)
			}
		})
	}
}

func TestPendingProtectionFailedCancelDoesNotCommit(t *testing.T) {
	s, _, h, _ := testStore(t)
	o := planOrder(t, s, "pending-failed-cancel", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	failure := errors.New("cancel offline")
	a := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true}, cancel: func(context.Context, string) (bool, error) { return false, failure }}
	e := executorFor(s, h, a)
	if err := e.Send(o.ID, 11); err != nil {
		t.Fatal(err)
	}
	if err := e.ApplyTrade(FillReport{EventID: "initial", OrderID: o.ID, Steps: 6, Price: intentPrice("100"), AtMS: 12}); err != nil {
		t.Fatal(err)
	}
	pending := pendingTestOrder(s, "a")
	service := &SharedAccount{store: s, owner: h, executor: e}
	_, err := service.AdvancePendingProtections(context.Background(), []PendingProtectionRequest{{Order: pending, Instrument: ledgerInstrument(), Specs: []PositionProtection{{Kind: StopLossProtection, Level: intentPrice("95")}}}}, pendingTestQuote("94"), 13)
	if err == nil || pending.Exit != nil || pending.Canceled || pending.ProtectionDone[string(StopLossProtection)] {
		t.Fatalf("failed cancel committed protection: %+v, %v", pending, err)
	}
}

func TestPendingEntryAdmissionIncludesAllPendingAndFactorTargets(t *testing.T) {
	for _, contributor := range []string{"none", "pending", "factor"} {
		t.Run(contributor, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			service := &SharedAccount{store: s, owner: h}
			pending := pendingTestOrder(s, "a")
			pending.Entry.Conditions.Limit = intentPrice("90")
			requests := []PendingEntryRequest{{Order: pending, Instrument: ledgerInstrument(), Policy: OrderPolicy{Mode: ExchangeFirst}}}
			if contributor == "pending" {
				other := pendingTestOrder(s, "b")
				other.Entry.Conditions.Stop = intentPrice("200")
				requests = append(requests, PendingEntryRequest{Order: other, Instrument: ledgerInstrument()})
			}
			if contributor == "factor" {
				if err := s.SavePlan(context.Background(), Plan{ID: "factor", Sequence: 1, DecisionMS: 10, ExpiresMS: 1000, Targets: []ExecutableTarget{{Strategy: "factor", Lot: "factor-lot", Instrument: "BTC", SignedSteps: 5}}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := service.AdvancePendingEntries(context.Background(), requests, pendingTestQuote("100"), 13); err != nil {
				t.Fatal(err)
			}
			if (pending.Entry.VenueLimit != nil) != (contributor == "none") {
				t.Fatalf("wrong delegation for %s: %+v", contributor, pending)
			}
			if contributor != "none" && pending.Desired != 0 {
				t.Fatal("unmarketable shared limit became a target", pending)
			}
		})
	}
}

func TestPendingEntryExpiryDropsUnfilledTarget(t *testing.T) {
	s, _, _, _ := testStore(t)
	pending := pendingTestOrder(s, "a")
	pending.Desired = 10
	pending.Entry.Conditions.ExpiresAtMS = 13
	if err := pending.AdvanceEntry(intentPrice("100"), 13, 5, 3, OrderPolicy{}, true); err != nil {
		t.Fatal(err)
	}
	if pending.Entry.State != Expired || pending.Desired != 3 {
		t.Fatal("expired entry retained unfilled target", pending)
	}
}

func TestPendingEntryConflictCancelsRestingLimitBeforeAdvancing(t *testing.T) {
	s, _, h, _ := testStore(t)
	pending := pendingTestOrder(s, "a")
	pending.Entry.Conditions.Limit = intentPrice("90")
	pending.Entry.VenueLimit = &OrderPolicy{Mode: ExchangeFirst}
	pending.Desired = 10
	plan := Plan{ID: "resting-plan", Sequence: 1, DecisionMS: 10, ExpiresMS: 1000, Intents: []EligibleIntent{pending.Entry}}
	order := OrderIntent{ID: "resting", PlanID: plan.ID, Instrument: ledgerInstrument(), Side: Buy, Steps: 10, Limit: intentPrice("90"), Observation: ExecutionObservation{Price: intentPrice("100"), Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: 10, ValidUntilMS: 1000, Bar: 5}, Allocations: []FillAllocation{{ID: "a", IntentID: pending.Entry.ID, Strategy: "a", Lot: "lot-a", Kind: EntryIntent, Side: Buy, Steps: 10}}}
	if err := s.SavePlan(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if err := s.PrepareOrder(context.Background(), order, 10); err != nil {
		t.Fatal(err)
	}
	a := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
	e := executorFor(s, h, a)
	if err := e.Send(order.ID, 11); err != nil {
		t.Fatal(err)
	}
	other := pendingTestOrder(s, "b")
	other.Entry.Conditions.Stop = intentPrice("200")
	a.query = func(context.Context, string, string) (QueryResult, error) {
		if pending.Desired != 10 || pending.Entry.VenueLimit == nil || other.Desired != 0 {
			t.Fatal("pending states advanced before cancellation settled")
		}
		return QueryResult{Found: true, Complete: true, Authoritative: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: "exchange-resting", Fills: []FillReport{{EventID: "race", OrderID: order.ID, Steps: 4, Price: intentPrice("90"), Cost: intentPrice("36"), Cumulative: true, AtMS: 13}}}}, nil
	}
	service := &SharedAccount{store: s, owner: h, executor: e}
	err := service.AdvancePendingEntries(context.Background(), []PendingEntryRequest{{Order: pending, Instrument: ledgerInstrument(), Policy: OrderPolicy{Mode: ExchangeFirst}}, {Order: other, Instrument: ledgerInstrument()}}, pendingTestQuote("100"), 13)
	if err != nil {
		t.Fatal(err)
	}
	if pending.Entry.VenueLimit != nil || pending.Entry.FilledSteps != 4 || pending.Desired != 4 {
		t.Fatal("resting limit did not settle before returning local", pending)
	}
	stored, err := s.Order(context.Background(), order.ID)
	if err != nil || stored.State != OrderCanceled || stored.FilledSteps != 4 {
		t.Fatal("resting reservation retained", stored, err)
	}
}

func TestPendingProtectionUnconfirmedCancelDoesNotCommit(t *testing.T) {
	s, _, h, _ := testStore(t)
	o := planOrder(t, s, "pending-unconfirmed", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	a := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true}, cancel: func(context.Context, string) (bool, error) { return false, nil }}
	e := executorFor(s, h, a)
	if err := e.Send(o.ID, 11); err != nil {
		t.Fatal(err)
	}
	if err := e.ApplyTrade(FillReport{EventID: "initial", OrderID: o.ID, Steps: 6, Price: intentPrice("100"), AtMS: 12}); err != nil {
		t.Fatal(err)
	}
	pending := pendingTestOrder(s, "a")
	service := &SharedAccount{store: s, owner: h, executor: e}
	_, err := service.AdvancePendingProtections(context.Background(), []PendingProtectionRequest{{Order: pending, Instrument: ledgerInstrument(), Specs: []PositionProtection{{Kind: StopLossProtection, Level: intentPrice("95")}}}}, pendingTestQuote("94"), 13)
	if err == nil || pending.Exit != nil || pending.Canceled {
		t.Fatalf("unconfirmed cancellation committed: %+v %v", pending, err)
	}
	stored, err := s.Order(context.Background(), o.ID)
	if err != nil || stored.State != OrderCancelPending {
		t.Fatal(stored, err)
	}
}

func TestPendingRestingExitRevocationSettlesFinalFill(t *testing.T) {
	for _, mode := range []string{"conflict", "timestamp", "bar", "retreat", "unconfirmed", "imported_timestamp", "imported_bar", "imported_market_timestamp", "imported_market_bar", "imported_market_full_timestamp", "imported_market_full_bar"} {
		t.Run(mode, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			entry := planOrder(t, s, "exit-seed", 1, Buy, EntryIntent, map[string]int64{"a": 10})
			a := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
			e := executorFor(s, h, a)
			if err := e.Send(entry.ID, 11); err != nil {
				t.Fatal(err)
			}
			if err := e.ApplyTrade(FillReport{EventID: "seed", OrderID: entry.ID, Steps: 10, Price: intentPrice("100"), AtMS: 12}); err != nil {
				t.Fatal(err)
			}
			pending := pendingTestOrder(s, "a")
			pending.Canceled, pending.Desired, pending.Entry.State = true, 5, Filled
			exit := pending.Entry
			exit.ID, exit.Kind, exit.Side, exit.State, exit.QuantitySteps = "software-exit", ExitIntent, Sell, PendingCondition, 5
			exit.Conditions.Limit = intentPrice("110")
			exit.VenueLimit = &OrderPolicy{Mode: ExchangeFirst}
			if strings.HasSuffix(mode, "timestamp") {
				exit.Conditions.ExpiresAtMS = 13
			}
			if strings.HasSuffix(mode, "bar") {
				exit.Conditions.StopBars = 1
			}
			if strings.HasPrefix(mode, "imported_market_") {
				exit.Conditions.Limit = intentPrice("0")
				exit.VenueLimit = nil
			}
			pending.Exit = &exit
			pending.NativeExitOrders = []string{"resting-exit"}
			plan := Plan{ID: "exit-plan", Sequence: 2, DecisionMS: 12, ExpiresMS: 1000, Intents: []EligibleIntent{exit}}
			order := OrderIntent{ID: "resting-exit", PlanID: plan.ID, Instrument: ledgerInstrument(), Side: Sell, Steps: 5, Limit: intentPrice("110"), ReduceOnly: true, Observation: ExecutionObservation{Price: intentPrice("100"), Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: 12, ValidUntilMS: 1000, Bar: 5}, Allocations: []FillAllocation{{ID: "exit-a", IntentID: exit.ID, Strategy: "a", Lot: "lot-a", Kind: ExitIntent, Side: Sell, Steps: 5}}}
			if strings.HasPrefix(mode, "imported_market_") {
				order.Limit = intentPrice("0")
			}
			if err := s.SavePlan(context.Background(), plan); err != nil {
				t.Fatal(err)
			}
			if err := s.PrepareOrder(context.Background(), order, 12); err != nil {
				t.Fatal(err)
			}
			if err := e.Send(order.ID, 12); err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(mode, "imported_") {
				pending.Exit.VenueLimit = nil
			}
			finalSteps := int64(2)
			if strings.Contains(mode, "_full_") {
				finalSteps = 5
			}
			a.query = func(context.Context, string, string) (QueryResult, error) {
				return QueryResult{Found: true, Complete: true, Authoritative: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: "exchange-resting-exit", Fills: []FillReport{{EventID: "exit-race", OrderID: order.ID, Steps: finalSteps, Price: intentPrice("110"), Cost: intentPrice("11").Mul(decimal.NewFromInt(finalSteps)), Cumulative: true, AtMS: 13}}}}, nil
			}
			policy := OrderPolicy{Mode: ExchangeFirst}
			requests := []PendingEntryRequest{{Order: pending, Instrument: ledgerInstrument(), Policy: policy}}
			if mode == "conflict" || mode == "unconfirmed" {
				other := pendingTestOrder(s, "b")
				other.Entry.Conditions.Stop = intentPrice("200")
				requests = append(requests, PendingEntryRequest{Order: other, Instrument: ledgerInstrument()})
			}
			if mode == "retreat" {
				requests[0].Policy = OrderPolicy{}
			}
			if mode == "unconfirmed" {
				a.cancel = func(context.Context, string) (bool, error) { return false, nil }
			}
			quotes := pendingTestQuote("100")
			if strings.HasSuffix(mode, "bar") {
				q := quotes["BTC"]
				q.Bar = 6
				quotes["BTC"] = q
			}
			service := &SharedAccount{store: s, owner: h, executor: e}
			err := service.AdvancePendingEntries(context.Background(), requests, quotes, 13)
			if mode == "unconfirmed" {
				if err == nil || pending.Exit.VenueLimit == nil || pending.Desired != 5 || len(pending.NativeExitOrders) != 1 {
					t.Fatal("unconfirmed exit changed", pending, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if pending.Exit.VenueLimit != nil || len(pending.NativeExitOrders) != 0 {
				t.Fatal("exit delegation retained", pending)
			}
			if mode == "timestamp" || mode == "bar" || strings.HasPrefix(mode, "imported_") {
				if pending.Exit.State != Expired || pending.Desired != 10-finalSteps {
					t.Fatal("expired exit resubmits reduction", pending)
				}
			} else if pending.Desired != 5 || pending.Exit.FilledSteps != 2 {
				t.Fatal("exit final fill doubled reduction", pending)
			}
		})
	}
}
