package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

// Reuse the existing behavioral contracts against both stores, including
// cancellation races, late fees, contributor limits and trailing checkpoints.
func TestStoreBackendParity(t *testing.T) {
	cases := []struct {
		name string
		test func(*testing.T)
	}{
		{"CumulativeRecovery", TestCumulativeRecoveryNormalizesLaterTrades},
		{"OwnerStop", TestOwnerStopPersistsUnknownAndJoinsSend},
		{"CancelLoss", TestCancelLossAndAuthoritativeAbsence},
		{"LateFillAndFees", TestTerminalFeeAndCanceledLateFillReconciliation},
		{"PlanReplacement", TestSupersededUnsentReservationReleased},
		{"UnknownRecovery", TestIncompleteQueryRetainsUnknown},
		{"TrailingCheckpoint", TestPendingIntentSurvivesCombinedPlansAndTrailingCheckpoint},
		{"ExternalIsolation", TestExternalIsolationRequiresFullReconciliation},
		{"CancelIncomplete", TestCancelIncompleteFinalQueryRetainsPendingUntilRecovery},
		{"ConcurrentFinalFill", TestCancelSettlesConcurrentFinalFillBeforeRelease},
		{"BudgetReduction", TestCombinedStrategyBudgetsAndOverLimitReduction},
		{"UntouchedReservation", TestUntouchedConfirmedOrdersReserveCombinedBudgets},
		{"CarriedMembership", TestCarriedTargetsPreserveMembershipWithoutExecution},
		{"CarriedRisk", TestCarriedUnfilledTargetsReserveRiskAndReductionsDoNotReleaseIt},
		{"ContributorLimits", TestContributorConditionsPreservedThroughCrossingAndResidualCaps},
		{"ContributorExpiry", TestContributorIneligibleAndExpiredSendNeverSubmit},
		{"ImmutablePolicy", TestAccountPolicyCannotBeOverwrittenByStrategyCheckpoint},
		{"InternalQuote", TestInternalQuoteVisibilityTickAndBothIntentConditions},
	}
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			for _, c := range cases {
				t.Run(c.name, c.test)
			}
		})
	}
}

func TestMemoryStoreNoDatabaseLeaseAndAtomicRollback(t *testing.T) {
	key := testIntent(Buy).Account
	s, err := NewMemoryStore(key)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	other, err := NewMemoryStore(key)
	if err != nil {
		t.Fatal("memory acquired filesystem sender lease", err)
	}
	defer other.Close()
	if s.Durability() != MemoryOnly || s.db != nil || s.releaseLease != nil {
		t.Fatal("memory allocated durable resources")
	}
	ctx := context.Background()
	fundStrategies(t, s)
	before, _ := s.Snapshot(ctx)
	want := errors.New("commit fault")
	err = s.atomically(ctx, func(ctx context.Context) error {
		if err := s.SaveStrategyCheckpoint(ctx, "a", "accepted", []byte(`{"accepted":true}`)); err != nil {
			return err
		}
		if _, err := s.ApplyCashEvent(ctx, CashEvent{ID: "rollback", Kind: CapitalTransfer, Postings: []CashPosting{{Strategy: "a", Amount: intentPrice("-10")}, {Strategy: "b", Amount: intentPrice("10")}}}); err != nil {
			return err
		}
		return want
	})
	if !errors.Is(err, want) {
		t.Fatal(err)
	}
	after, _ := s.Snapshot(ctx)
	a, _ := payload(before)
	b, _ := payload(after)
	if a != b {
		t.Fatal("partial memory commit", a, b)
	}
	if _, err := s.StrategyCheckpoint(ctx, "a", "accepted"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("checkpoint escaped rollback", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	err = s.atomically(canceled, func(ctx context.Context) error {
		if err := s.SaveStrategyCheckpoint(ctx, "a", "accepted", []byte(`{}`)); err != nil {
			return err
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("canceled transaction committed", err)
	}
	if _, err := s.StrategyCheckpoint(ctx, "a", "accepted"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("canceled checkpoint escaped", err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic hidden")
			}
		}()
		_ = s.atomically(ctx, func(ctx context.Context) error {
			_ = s.SaveStrategyCheckpoint(ctx, "a", "accepted", []byte(`{}`))
			panic("abort")
		})
	}()
	if _, err := s.StrategyCheckpoint(ctx, "a", "accepted"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("panic checkpoint escaped", err)
	}
	if err := s.atomically(ctx, func(ctx context.Context) error {
		return other.SaveStrategyCheckpoint(ctx, "a", "foreign", []byte(`{}`))
	}); err == nil {
		t.Fatal("foreign scoped transaction accepted")
	}
}

func TestMemoryStoreSnapshotAndEventParity(t *testing.T) {
	var expectedSnapshot, expectedEvents string
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			e, _ := openMixed(t, s, h)
			if _, err := s.ApplyFunding(context.Background(), FundingSettlement{ID: "funding", Instrument: ledgerInstrument(), Mark: intentPrice("110"), Rate: intentPrice("0.01"), AccountAmount: intentPrice("-0.44"), AtMS: 12}); err != nil {
				t.Fatal(err)
			}
			request := domainRequest("close", 2, 20, "110", 0, 0)
			ready, err := e.PrepareRebalance(request)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range ready.OrderIDs {
				o, err := s.Order(context.Background(), id)
				if err != nil {
					t.Fatal(err)
				}
				if err := e.Send(id, 21); err != nil {
					t.Fatal(err)
				}
				if err := e.ApplyFill(FillReport{EventID: "close:" + id, OrderID: id, Steps: o.Intent.Steps, Price: intentPrice("110"), Fee: intentPrice("0.03"), AtMS: 21}); err != nil {
					t.Fatal(err)
				}
			}
			snapshot, err := s.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			assertNAVConservation(t, snapshot, "110")
			ss, _ := payload(snapshot)
			events, err := s.EventsAfter(context.Background(), 0, 1000)
			if err != nil {
				t.Fatal(err)
			}
			// Owner generations are process-wide and intentionally differ for two
			// independently acquired handles; compare all other event facts.
			for n := range events {
				if events[n].Kind == "OrderAttempt" {
					var attempt OrderAttempt
					if err := json.Unmarshal(events[n].Payload, &attempt); err != nil {
						t.Fatal(err)
					}
					attempt.Generation = 0
					events[n].Payload, _ = json.Marshal(attempt)
					events[n].ID = fmt.Sprint("attempt-", events[n].Checkpoint)
				}
				if events[n].Kind == "OrderState" {
					var state OrderStateEvent
					if err := json.Unmarshal(events[n].Payload, &state); err != nil {
						t.Fatal(err)
					}
					state.Generation = 0
					events[n].Payload, _ = json.Marshal(state)
					events[n].ID = fmt.Sprint("state-", events[n].Checkpoint)
				}
			}
			es, _ := payload(events)
			if expectedSnapshot == "" {
				expectedSnapshot, expectedEvents = ss, es
			} else if expectedSnapshot != ss || expectedEvents != es {
				t.Fatalf("backend changed snapshot/events\n%s\n%s\n%s\n%s", expectedSnapshot, ss, expectedEvents, es)
			}
			if err := s.AdvanceProjection(context.Background(), "result", snapshot.Checkpoint); err != nil {
				t.Fatal(err)
			}
			cursor, err := s.ProjectionCursor(context.Background(), "result")
			if err != nil || cursor != snapshot.Checkpoint {
				t.Fatal(cursor, err)
			}
			if err := s.AdvanceProjection(context.Background(), "result", snapshot.Checkpoint+1); err == nil {
				t.Fatal("cursor passed committed checkpoint")
			}
		})
	}
}
