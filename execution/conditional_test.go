package execution

import (
	"context"
	"strings"
	"testing"
)

type coreConditionalAdapter struct {
	*fakeExecutionAdapter
	proof ConditionalCapabilities
}

func (a *coreConditionalAdapter) NativeConditionalCapabilities() ConditionalCapabilities {
	return a.proof
}

func coreConditionalProof() ConditionalCapabilities {
	return ConditionalCapabilities{EvidenceID: "verified-native-test", ContextBound: true, StableClientID: true, QueryClientID: true, CompleteSnapshots: true, CompleteCumulativeReports: true, ReduceOnly: true, Limit: true,
		Kinds: map[ProtectionKind]bool{StopLossProtection: true, TakeProfitProtection: true}, References: []TriggerReference{MidpointTrigger}}
}

func coreConditionalFixture(t *testing.T) (*Store, *OwnerExecutor, *coreConditionalAdapter, PendingDefinition, ConditionalReservation) {
	t.Helper()
	store, _, owner, _ := testStore(t)
	fundStrategies(t, store)
	base := &fakeExecutionAdapter{}
	executor := executorFor(store, owner, base)
	entry := planOrder(t, store, "conditional-seed", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	if err := executor.Send(entry.ID, 11); err != nil {
		t.Fatal(err)
	}
	if err := executor.ApplyTrade(FillReport{EventID: "conditional-seed-fill", OrderID: entry.ID, Steps: 10, Price: intentPrice("100"), AtMS: 11}); err != nil {
		t.Fatal(err)
	}
	pending := pendingTestOrder(store, "a")
	pending.Entry.State, pending.Entry.FilledSteps, pending.Desired = Filled, 10, 10
	spec := PositionProtection{Kind: StopLossProtection, Level: intentPrice("95")}
	if _, _, err := pending.AdvanceProtection(store.Account(), ledgerInstrument(), []PositionProtection{spec}, 10, intentPrice("100"), intentPrice("100"), 12, 5); err != nil {
		t.Fatal(err)
	}
	definition := PendingDefinition{Order: *pending, Instrument: ledgerInstrument(), Specs: []PositionProtection{spec}, Policy: OrderPolicy{Mode: ExchangeFirst}}
	saveTestPendingBook(t, store, []PendingDefinition{definition})
	hash, err := ConditionalSourceHash(definition, StopLossProtection)
	if err != nil {
		t.Fatal(err)
	}
	reservation := ConditionalReservation{Intent: EligibleIntent{ID: "conditional-source", Account: store.Account(), Strategy: "a", Lot: "lot-a", Instrument: "BTC", Kind: ExitIntent, Side: Sell, QuantitySteps: 10, State: Eligible},
		Condition: ConditionalOrder{Kind: StopLossProtection, TriggerPrice: intentPrice("95"), SourceHash: hash, Reference: MidpointTrigger}}
	adapter := &coreConditionalAdapter{fakeExecutionAdapter: base, proof: coreConditionalProof()}
	executor.Adapter = adapter
	return store, executor, adapter, definition, reservation
}

func prepareCoreConditional(t *testing.T, executor *OwnerExecutor, reservation ConditionalReservation) PreparedRebalance {
	t.Helper()
	request := domainRequest("conditional-plan", 2, 12, "100", 10, 0)
	request.Requests[0].ConditionalReservations = []ConditionalReservation{reservation}
	prepared, err := executor.PrepareRebalance(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.OrderIDs) != 1 || len(prepared.InternalMatchIDs) != 0 || len(prepared.Plan.Intents) != 0 || len(prepared.Plan.ConditionalIntents) != 1 {
		t.Fatal("dormant reservation leaked into executable coordination", prepared)
	}
	return prepared
}

func TestConditionalCoreDormantExposureAndImmutablePlan(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			store, executor, _, _, reservation := coreConditionalFixture(t)
			prepared := prepareCoreConditional(t, executor, reservation)
			order, err := store.Order(context.Background(), prepared.OrderIDs[0])
			if err != nil || order.Intent.Conditional == nil {
				t.Fatal(order, err)
			}
			if err := executor.Send(order.Intent.ID, 13); err != nil {
				t.Fatal(err)
			}
			snapshot, err := store.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			lots, positions, err := confirmedExposure(snapshot)
			if err != nil || len(lots) != 1 || lots[0].SignedSteps != 10 || len(positions) != 1 || positions[0].SignedSteps != 10 {
				t.Fatal("untriggered exit freed held exposure", lots, positions, err)
			}
			coordination, err := Coordinate(snapshot, prepared.Plan.Targets, ledgerInstrument(), order.Intent.Observation, domainRisk())
			if err != nil || len(coordination.Legs) != 0 || coordination.ProjectedSteps != 10 {
				t.Fatal("dormant exit produced a replacement execution delta", coordination, err)
			}
			reserved, actual, err := reservedCarriedExposure(snapshot, prepared.Plan.Targets, prepared.Plan.Instruments)
			var owner *exposureReservation
			for n := range reserved {
				if reserved[n].Strategy == "a" && reserved[n].ID == "lot-a" {
					owner = &reserved[n]
				}
			}
			if err != nil || len(reserved) != 2 || owner == nil || owner.SignedSteps != 10 || len(actual) != 1 || actual[0].SignedSteps != 10 {
				t.Fatal("dormant exit freed carried risk", reserved, actual, err)
			}
			for _, steps := range owner.alternatives {
				if steps != 10 {
					t.Fatal("dormant exit projected an executable risk scenario", owner)
				}
			}
			changed := prepared.Plan
			changed.ConditionalIntents = append([]ConditionalReservation(nil), prepared.Plan.ConditionalIntents...)
			changed.ConditionalIntents[0].Condition.TriggerPrice = intentPrice("94")
			if err := store.SavePlan(context.Background(), changed); err == nil {
				t.Fatal("immutable conditional definition changed")
			}
		})
	}
}

func TestConditionalCoreRefusesOrdinaryOrChangedOrders(t *testing.T) {
	store, executor, _, _, reservation := coreConditionalFixture(t)
	prepared := prepareCoreConditional(t, executor, reservation)
	stored, err := store.Order(context.Background(), prepared.OrderIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, ordinary := range []bool{true, false} {
		order := stored.Intent
		order.ID = "tampered"
		condition := *order.Conditional
		order.Conditional = &condition
		if ordinary {
			order.Conditional = nil
		} else {
			order.Conditional.TriggerPrice = intentPrice("94")
		}
		if err := store.PrepareOrder(context.Background(), order, 13); err == nil {
			t.Fatal("dormant accounting intent allowed an unfrozen order", ordinary)
		}
	}
}

func TestConditionalCoreSendRechecksProofAndSource(t *testing.T) {
	for _, mode := range []string{"proof", "changed-source", "local-policy", "new-contributor"} {
		t.Run(mode, func(t *testing.T) {
			store, executor, adapter, definition, reservation := coreConditionalFixture(t)
			prepared := prepareCoreConditional(t, executor, reservation)
			definitions := []PendingDefinition{definition}
			switch mode {
			case "proof":
				adapter.proof.CompleteCumulativeReports = false
			case "changed-source":
				definitions[0].Specs[0].Level = intentPrice("94")
			case "local-policy":
				definitions[0].Policy = OrderPolicy{}
			case "new-contributor":
				other := *pendingTestOrder(store, "b")
				other.Entry.Conditions.Stop = intentPrice("110")
				definitions = append(definitions, PendingDefinition{Order: other, Instrument: ledgerInstrument()})
			}
			saveTestPendingBook(t, store, definitions)
			calls := len(adapter.calls())
			if err := executor.Send(prepared.OrderIDs[0], 13); err == nil {
				t.Fatal("unsafe native send admitted", mode)
			}
			order, err := store.Order(context.Background(), prepared.OrderIDs[0])
			if err != nil || order.State != OrderPrepared || order.Attempt != 0 || len(adapter.calls()) != calls {
				t.Fatal("failed admission mutated or submitted native order", order, err)
			}
		})
	}
}

func TestConditionalCoreCannotCrossLocally(t *testing.T) {
	store, _, _, _, reservation := coreConditionalFixture(t)
	buy := reservation.Intent
	buy.ID, buy.Strategy, buy.Lot, buy.Kind, buy.Side = "ordinary-buy", "b", "lot-b", EntryIntent, Buy
	plan := Plan{ID: "no-native-cross", Sequence: 2, DecisionMS: 12, ExpiresMS: 1000, Intents: []EligibleIntent{buy}, ConditionalIntents: []ConditionalReservation{reservation}}
	if err := store.SavePlan(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	quote := VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: 12, ReceivedMS: 12, ValidUntilMS: 1000, Bar: 5}
	if applied, err := store.ApplyInternalMatch(context.Background(), InternalMatch{ID: "invalid-native-cross", PlanID: plan.ID, Instrument: ledgerInstrument(), BuyIntent: buy.ID, SellIntent: reservation.Intent.ID, Steps: 1, Quote: quote, AtMS: 13}); err == nil || applied || !strings.Contains(err.Error(), "dormant") {
		t.Fatal("native reservation crossed before trigger", applied, err)
	}
}

func TestConditionalCoreTrailingNeedsRecoverableState(t *testing.T) {
	condition := ConditionalOrder{Kind: TrailingProtection, TrailingPercent: intentPrice("1"), TrailingAnchor: intentPrice("100"), Reference: MidpointTrigger, SourceHash: "source"}
	proof := coreConditionalProof()
	proof.Kinds[TrailingProtection], proof.SeedTrailingAnchor = true, true
	if err := proof.Supports(condition); err == nil || !strings.Contains(err.Error(), "state recovery") {
		t.Fatal("anchor seeding was mistaken for recoverable trailing state", err)
	}
}

func TestConditionalCoreRequiresTickAlignedTrigger(t *testing.T) {
	_, executor, _, _, reservation := coreConditionalFixture(t)
	reservation.Condition.TriggerPrice = intentPrice("95.001")
	request := domainRequest("off-tick-conditional", 2, 12, "100", 10, 0)
	request.Requests[0].ConditionalReservations = []ConditionalReservation{reservation}
	if _, err := executor.PrepareRebalance(request); err == nil || !strings.Contains(err.Error(), "tick") {
		t.Fatal("off-tick native trigger admitted", err)
	}
}

func TestConditionalCoreRequiresFrozenOwnerTarget(t *testing.T) {
	store, _, _, _, reservation := coreConditionalFixture(t)
	plan := Plan{ID: "missing-owner-target", Sequence: 2, DecisionMS: 12, ExpiresMS: 1000, ConditionalIntents: []ConditionalReservation{reservation}}
	if err := store.SavePlan(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	condition := reservation.Condition
	intent := reservation.Intent
	order := OrderIntent{ID: "unowned-native", PlanID: plan.ID, Instrument: ledgerInstrument(), Side: Sell, Steps: 10, ReduceOnly: true, Conditional: &condition,
		Observation: ExecutionObservation{Price: intentPrice("100"), AtMS: 12, ValidUntilMS: 1000, Bar: 5},
		Allocations: []FillAllocation{{ID: "native", IntentID: intent.ID, Strategy: intent.Strategy, Lot: intent.Lot, Side: Sell, Kind: ExitIntent, Steps: 10}}}
	if err := store.PrepareOrder(context.Background(), order, 12); err == nil || !strings.Contains(err.Error(), "owner target") {
		t.Fatal("native reservation without explicit unchanged owner target admitted", err)
	}
}

func TestConditionalSourceQuantityIsImmutableIdentity(t *testing.T) {
	_, _, _, definition, _ := coreConditionalFixture(t)
	before, err := ConditionalSourceHash(definition, StopLossProtection)
	if err != nil {
		t.Fatal(err)
	}
	trigger := definition.Order.Protections[string(StopLossProtection)]
	trigger.QuantitySteps--
	definition.Order.Protections[string(StopLossProtection)] = trigger
	after, err := ConditionalSourceHash(definition, StopLossProtection)
	if err != nil || before == after {
		t.Fatal("changed protection quantity reused immutable identity", err)
	}
}
