package execution

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

func pendingNativeFixture(t *testing.T) (*SharedAccount, *coreConditionalAdapter, []PendingDefinition) {
	t.Helper()
	store, executor, adapter, definition, _ := coreConditionalFixture(t)
	adapter.capabilities = AdapterCapabilities{QueryClientID: true, CumulativeReports: true}
	return &SharedAccount{ctx: context.Background(), store: store, owner: executor.Handle, executor: executor, ready: true, opts: SharedExecutionOptions{Adapter: adapter}}, adapter, []PendingDefinition{definition}
}

func coordinateNative(t *testing.T, service *SharedAccount, definitions []PendingDefinition) StoredOrder {
	t.Helper()
	requests, err := service.CoordinatePending(context.Background(), definitions, pendingTestQuote("100"), 12)
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || len(requests[0].ConditionalReservations) != 1 {
		t.Fatal("native protection not reserved", requests)
	}
	saveTestPendingBook(t, service.store, definitions)
	request := domainRequest("pending-native-plan", 2, 12, "100", 10, 0)
	request.Requests[0].ConditionalReservations = requests[0].ConditionalReservations
	prepared, err := service.executor.PrepareRebalance(request)
	if err != nil || len(prepared.OrderIDs) != 1 {
		t.Fatal(prepared, err)
	}
	if err := service.executor.Send(prepared.OrderIDs[0], 13); err != nil {
		t.Fatal(err)
	}
	order, err := service.store.Order(context.Background(), prepared.OrderIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if order.Intent.Conditional == nil || !order.Intent.ReduceOnly || order.State != OrderAcknowledged {
		t.Fatal("not a tracked dormant reduce-only order", order)
	}
	return order
}

func TestPendingNativePlacementAndVisibleFallback(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			for _, scenario := range []string{"native", "local", "proof", "shared", "trailing"} {
				t.Run(scenario, func(t *testing.T) {
					service, adapter, definitions := pendingNativeFixture(t)
					kind, reason := StopLossProtection, "verified_conditional"
					switch scenario {
					case "local":
						definitions[0].Policy = OrderPolicy{}
						reason = "local_policy"
					case "proof":
						adapter.proof.CompleteSnapshots = false
						reason = "conditional_recovery_or_reference_unproven"
					case "shared":
						other := pendingTestOrder(service.store, "b")
						other.Entry.Conditions.Stop = intentPrice("110")
						definitions = append(definitions, PendingDefinition{Order: *other, Instrument: ledgerInstrument()})
						reason = "shared_instrument"
					case "trailing":
						kind, reason = TrailingProtection, "native_trailing_state_recovery_unproven"
						definitions[0].Specs = []PositionProtection{{Kind: TrailingProtection, TrailingPercent: intentPrice("5")}}
						definitions[0].Order.Protections = nil
						definitions[0].Order.ProtectionLevels = nil
						if _, _, err := definitions[0].Order.AdvanceProtection(service.AccountKey(), ledgerInstrument(), definitions[0].Specs, 10, intentPrice("100"), intentPrice("100"), 12, 5); err != nil {
							t.Fatal(err)
						}
					}
					requests, err := service.CoordinatePending(context.Background(), definitions, pendingTestQuote("100"), 12)
					if err != nil {
						t.Fatal(err)
					}
					status := definitions[0].Order.ProtectionPlacement[string(kind)]
					if status.Reason != reason {
						t.Fatal("missing visible fallback", status, reason)
					}
					count := len(requests[0].ConditionalReservations)
					if scenario == "native" {
						if status.Effective != "native" || count != 1 {
							t.Fatal(status, requests)
						}
					} else if status.Effective != "local" || count != 0 {
						t.Fatal("unsafe native fallback", status, requests)
					}
				})
			}
		})
	}
}

func TestPendingNativePartialFillPersistsAndRetainedTargetDoesNotReenter(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			service, adapter, definitions := pendingNativeFixture(t)
			order := coordinateNative(t, service, definitions)
			adapter.query = func(context.Context, string, string) (QueryResult, error) {
				return QueryResult{Found: true, Complete: true, Authoritative: true, Receipt: SubmitReceipt{ExchangeID: order.ExchangeID, Fills: []FillReport{{EventID: "native-partial", OrderID: order.Intent.ID, Steps: 4, Price: intentPrice("95"), Cost: intentPrice("38"), Cumulative: true, AtMS: 14}}}}, nil
			}
			requests := []PendingProtectionRequest{{Order: &definitions[0].Order, Instrument: ledgerInstrument(), Specs: definitions[0].Specs, Policy: definitions[0].Policy}}
			triggers, err := service.AdvancePendingProtections(context.Background(), requests, pendingTestQuote("100"), 14)
			if err != nil || len(triggers) != 0 {
				t.Fatal(triggers, err)
			}
			book, err := service.store.LoadPendingBook(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			pending := book[0].Order
			if pending.Desired != 6 || !pending.Canceled || pending.Entry.State != Canceled || pending.NativeProtectionID != order.Intent.ID || pending.ProtectionDone[string(StopLossProtection)] || pending.Protections[string(StopLossProtection)].FilledSteps != 4 {
				t.Fatal("partial native economics not persisted", pending)
			}
			if definitions[0].Order.Desired != 6 {
				t.Fatal("caller retained pre-fill target", definitions[0].Order)
			}
			snapshot, err := service.store.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.Lots[0].SignedSteps != 6 {
				t.Fatal("native fill not applied to lot", snapshot.Lots)
			}
			latest, err := service.store.LatestPlan(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			targets, err := service.nativeFillTargets(context.Background(), latest, latest.Targets)
			if err != nil {
				t.Fatal(err)
			}
			for _, target := range targets {
				if target.Strategy == "a" && target.Lot == "lot-a" && target.SignedSteps != 6 {
					t.Fatal("retained target would reopen native reduction", targets)
				}
			}
			// Exact query replay cannot repeat a reduction or cancel its remainder.
			if _, err := service.AdvancePendingProtections(context.Background(), requests, pendingTestQuote("100"), 15); err != nil {
				t.Fatal(err)
			}
			current, err := service.store.Order(context.Background(), order.Intent.ID)
			if err != nil || current.State != OrderPartial || current.FilledSteps != 4 {
				t.Fatal(current, err)
			}
			for _, call := range adapter.calls() {
				if strings.HasPrefix(call, "Cancel:") {
					t.Fatal("partial native remainder was canceled", adapter.calls())
				}
			}
			borrow := service.Borrow()
			defer borrow.Release()
			if err := borrow.RegisterRiskPolicy("native-test", domainRisk()); err != nil {
				t.Fatal(err)
			}
			if err := borrow.RegisterAccountQuotes([]Instrument{ledgerInstrument()}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
				return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
			}, nil); err != nil {
				t.Fatal(err)
			}
			prepared, err := borrow.PrepareStrategy(strategyRequest("b", "unrelated-after-native-fill", 16, StrategyTargetsFull, ExecutableTarget{Lot: "lot-b", SignedSteps: 0}))
			if err != nil || len(prepared.OrderIDs) != 0 {
				t.Fatal("unrelated strategy reopened a native reduction", prepared, err)
			}
			for _, target := range prepared.Plan.Targets {
				if target.Strategy == "a" && target.Lot == "lot-a" && target.SignedSteps != 6 {
					t.Fatal("accepted unrelated revision retained stale target", target)
				}
			}
			adapter.query = func(context.Context, string, string) (QueryResult, error) {
				return QueryResult{Found: true, Complete: true, Authoritative: true, Receipt: SubmitReceipt{ExchangeID: order.ExchangeID, Fills: []FillReport{{EventID: "native-complete", OrderID: order.Intent.ID, Steps: 10, Price: intentPrice("95"), Cost: intentPrice("95"), Cumulative: true, AtMS: 18}}}}, nil
			}
			triggers, err = service.AdvancePendingProtections(context.Background(), requests, pendingTestQuote("100"), 18)
			if err != nil || len(triggers) != 0 {
				t.Fatal("native completion also fired a local sibling", triggers, err)
			}
			book, err = service.store.LoadPendingBook(context.Background())
			if err != nil || book[0].Order.Desired != 0 || book[0].Order.NativeProtectionID != "" || !book[0].Order.ProtectionDone[string(StopLossProtection)] {
				t.Fatal("native final fill not reflected in pending book", book, err)
			}
		})
	}
}

func TestPendingNativeLocalSiblingCancelsBeforeResidualExit(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			service, adapter, definitions := pendingNativeFixture(t)
			definitions[0].Specs = append(definitions[0].Specs, PositionProtection{Kind: TakeProfitProtection, Level: intentPrice("110"), Limit: intentPrice("109")})
			if _, _, err := definitions[0].Order.AdvanceProtection(service.AccountKey(), ledgerInstrument(), definitions[0].Specs, 10, intentPrice("100"), intentPrice("100"), 12, 5); err != nil {
				t.Fatal(err)
			}
			saveTestPendingBook(t, service.store, definitions)
			order := coordinateNative(t, service, definitions)
			queries := 0
			adapter.query = func(context.Context, string, string) (QueryResult, error) {
				queries++
				result := QueryResult{Found: true, Complete: true, Authoritative: true, Receipt: SubmitReceipt{ExchangeID: order.ExchangeID}}
				if queries > 1 {
					result.Canceled = true
					result.Receipt.Fills = []FillReport{{EventID: "native-final-on-cancel", OrderID: order.Intent.ID, Steps: 4, Price: intentPrice("95"), Cost: intentPrice("38"), Cumulative: true, AtMS: 14}}
				}
				return result, nil
			}
			triggers, err := service.AdvancePendingProtections(context.Background(), []PendingProtectionRequest{{Order: &definitions[0].Order, Instrument: ledgerInstrument(), Specs: definitions[0].Specs, Policy: definitions[0].Policy}}, pendingTestQuote("111"), 14)
			if err != nil || len(triggers) != 1 || triggers[0].Kind != TakeProfitProtection {
				t.Fatal(triggers, err)
			}
			pending := definitions[0].Order
			if pending.Desired != 0 || pending.Exit == nil || pending.Exit.QuantitySteps != 6 || pending.NativeProtectionID != "" {
				t.Fatal("sibling exit ignored final native fill", pending)
			}
			if !pending.Exit.Triggered || pending.Exit.State != Eligible || !pending.Exit.Conditions.Limit.Equal(intentPrice("109")) {
				t.Fatal("native final-fill refresh reset the selected local latch", pending.Exit)
			}
			available, err := pending.Exit.AvailableForSubmission(intentPrice("109"), 15, 6)
			if err != nil || available != 6 {
				t.Fatal("latched local sibling cannot execute below its original activation", available, err)
			}
			final, err := service.store.Order(context.Background(), order.Intent.ID)
			if err != nil || final.State != OrderCanceled || final.FilledSteps != 4 {
				t.Fatal(final, err)
			}
		})
	}
}

func TestPendingNativeFinalFillPreservesExplicitExitGoal(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			for _, goal := range []int64{0, 3, 8} {
				t.Run(strconv.FormatInt(goal, 10), func(t *testing.T) {
					service, adapter, definitions := pendingNativeFixture(t)
					order := coordinateNative(t, service, definitions)
					pending := &definitions[0].Order
					pending.Desired = goal
					pending.Exit = &EligibleIntent{ID: "manual-exit", Account: service.AccountKey(), Strategy: "a", Lot: "lot-a", Instrument: "BTC", Kind: ExitIntent, Side: Sell, QuantitySteps: 10 - goal, State: Eligible}
					queries := 0
					adapter.query = func(context.Context, string, string) (QueryResult, error) {
						queries++
						result := QueryResult{Found: true, Complete: true, Authoritative: true, Receipt: SubmitReceipt{ExchangeID: order.ExchangeID}}
						if queries > 1 {
							result.Canceled = true
							result.Receipt.Fills = []FillReport{{EventID: "manual-exit-native-final", OrderID: order.Intent.ID, Steps: 4, Price: intentPrice("95"), Cost: intentPrice("38"), Cumulative: true, AtMS: 14}}
						}
						return result, nil
					}
					requests, err := service.CoordinatePending(context.Background(), definitions, pendingTestQuote("100"), 14)
					if err != nil {
						t.Fatal(err)
					}
					want := min(goal, int64(6))
					if pending.Desired != want || pending.Exit == nil || pending.Exit.ID != "manual-exit" || pending.NativeProtectionID != "" || len(requests) != 1 || requests[0].Targets[0].SignedSteps != want {
						t.Fatal("native final-fill sync replaced explicit bounded exit goal", goal, pending, requests)
					}
					final, err := service.store.Order(context.Background(), order.Intent.ID)
					if err != nil || final.State != OrderCanceled || final.FilledSteps != 4 {
						t.Fatal(final, err)
					}
				})
			}
		})
	}
}

func TestPendingNativeUncertainCancellationDoesNotBecomeLocalFallback(t *testing.T) {
	service, adapter, definitions := pendingNativeFixture(t)
	definitions[0].Specs = append(definitions[0].Specs, PositionProtection{Kind: TakeProfitProtection, Level: intentPrice("110")})
	if _, _, err := definitions[0].Order.AdvanceProtection(service.AccountKey(), ledgerInstrument(), definitions[0].Specs, 10, intentPrice("100"), intentPrice("100"), 12, 5); err != nil {
		t.Fatal(err)
	}
	saveTestPendingBook(t, service.store, definitions)
	order := coordinateNative(t, service, definitions)
	adapter.query = func(context.Context, string, string) (QueryResult, error) {
		return QueryResult{Found: true, Complete: true, Authoritative: true, Receipt: SubmitReceipt{ExchangeID: order.ExchangeID}}, nil
	}
	adapter.cancel = func(context.Context, string) (bool, error) { return false, context.DeadlineExceeded }
	_, err := service.AdvancePendingProtections(context.Background(), []PendingProtectionRequest{{Order: &definitions[0].Order, Instrument: ledgerInstrument(), Specs: definitions[0].Specs, Policy: definitions[0].Policy}}, pendingTestQuote("111"), 14)
	pending := definitions[0].Order
	if err == nil || pending.Desired != 10 || pending.Exit != nil || pending.ProtectionDone[string(TakeProfitProtection)] {
		t.Fatal("uncertain native cancellation committed local replacement", pending, err)
	}
	if pending.ProtectionPlacement[string(StopLossProtection)].Effective != "native" {
		t.Fatal("uncertain attempt mislabeled as local fallback", pending.ProtectionPlacement)
	}
}

func TestPendingNativeRecoveryFailureStopsProtectionRound(t *testing.T) {
	service, adapter, definitions := pendingNativeFixture(t)
	order := coordinateNative(t, service, definitions)
	adapter.query = func(context.Context, string, string) (QueryResult, error) {
		return QueryResult{}, context.DeadlineExceeded
	}
	_, err := service.AdvancePendingProtections(context.Background(), []PendingProtectionRequest{{Order: &definitions[0].Order, Instrument: ledgerInstrument(), Specs: definitions[0].Specs, Policy: definitions[0].Policy}}, pendingTestQuote("94"), 14)
	if err == nil || definitions[0].Order.Desired != 10 || definitions[0].Order.Exit != nil || definitions[0].Order.ProtectionDone[string(StopLossProtection)] {
		t.Fatal("failed authoritative recovery allowed a local stop", definitions[0].Order, err)
	}
	current, readErr := service.store.Order(context.Background(), order.Intent.ID)
	if readErr != nil || terminalOrder(string(current.State)) {
		t.Fatal("recovery failure invented terminal state", current, readErr)
	}
	for _, call := range adapter.calls() {
		if strings.HasPrefix(call, "Cancel:") {
			t.Fatal("recovery failure continued into cancellation", adapter.calls())
		}
	}
}
