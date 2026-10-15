package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestTerminalFeeAndCanceledLateFillReconciliation(t *testing.T) {
	for _, terminal := range []string{"filled", "canceled"} {
		t.Run(terminal, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			order := planOrder(t, s, "order", 1, Buy, EntryIntent, map[string]int64{"a": 10})
			adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
			e := executorFor(s, h, adapter)
			if err := e.Send(order.ID, 11); err != nil {
				t.Fatal(err)
			}
			units := int64(10)
			if terminal == "canceled" {
				units = 2
			}
			if err := e.ApplyTrade(FillReport{EventID: "trade", OrderID: order.ID, Steps: units, Price: intentPrice("100"), Fee: intentPrice("0.02"), AtMS: 12}); err != nil {
				t.Fatal(err)
			}
			if terminal == "canceled" {
				adapter.query = func(context.Context, string, string) (QueryResult, error) {
					return QueryResult{Found: true, Authoritative: true, Complete: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: "exchange-order", Fills: []FillReport{{EventID: "cancel-final", OrderID: order.ID, Steps: 2, Price: intentPrice("100"), Cost: intentPrice("20"), Fee: intentPrice("0.02"), Cumulative: true, AtMS: 13}}}}, nil
				}
				if err := e.Cancel(order.ID, 13); err != nil {
					t.Fatal(err)
				}
			}
			cumulative := FillReport{EventID: "late-query", OrderID: order.ID, Steps: 10, Price: intentPrice("100"), Cost: intentPrice("100"), Fee: intentPrice("0.05"), Cumulative: true, AtMS: 14}
			if terminal == "canceled" {
				cumulative.Steps = 4
				cumulative.Price = intentPrice("105")
				cumulative.Cost = intentPrice("42")
			}
			adapter.query = func(context.Context, string, string) (QueryResult, error) {
				return QueryResult{Found: true, Authoritative: true, Complete: true, Canceled: terminal == "canceled", Receipt: SubmitReceipt{ExchangeID: "exchange-order", Fills: []FillReport{cumulative}}}, nil
			}
			if err := e.Recover(order.ID); err != nil {
				t.Fatal("terminal query ignored late reports", err)
			}
			if err := e.Recover(order.ID); err != nil {
				t.Fatal(err)
			}
			stored, _ := s.Order(context.Background(), order.ID)
			if stored.FilledSteps != cumulative.Steps || !stored.ReportedFee.Equal(cumulative.Fee) {
				t.Fatal(stored)
			}
			totals, err := s.StrategyTotals(context.Background(), "a")
			if err != nil || !totals.Fees.Equal(cumulative.Fee) {
				t.Fatal("late fees not accumulated", totals, err)
			}
			snapshot, _ := s.Snapshot(context.Background())
			assertNAVConservation(t, snapshot, "105")
			if !snapshot.AccountSettledCash.Equal(intentPrice("-0.05")) {
				t.Fatal(snapshot)
			}
			if terminal == "canceled" {
				if stored.State != OrderCanceled {
					t.Fatal(stored)
				}
				reissue := order
				reissue.ID = "reissue"
				reissue.Steps = 6
				reissue.Allocations = append([]FillAllocation(nil), order.Allocations...)
				reissue.Allocations[0].Steps = 6
				if err := s.PrepareOrder(context.Background(), reissue, 15); err != nil {
					t.Fatal("known canceled unfilled remainder never released", err)
				}
				if err := e.Send(reissue.ID, 16); err != nil {
					t.Fatal(err)
				}
				if err := e.ApplyFill(FillReport{EventID: "reissue-fill", OrderID: reissue.ID, Steps: 6, Price: intentPrice("110"), AtMS: 17}); err != nil {
					t.Fatal(err)
				}
				intent, err := s.Intent(context.Background(), order.Allocations[0].IntentID)
				if err != nil || intent.FilledSteps != 10 || intent.State != Filled {
					t.Fatal("reissue duplicated attribution", intent, err)
				}
			}
		})
	}
}

func TestSupersededUnsentReservationReleased(t *testing.T) {
	s, _, h, _ := testStore(t)
	order := planOrder(t, s, "old", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	original, err := s.LatestPlan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	plan := original
	plan.ID = "replacement"
	plan.Sequence = 2
	if err := s.SavePlan(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	old, _ := s.Order(context.Background(), order.ID)
	if old.State != OrderCanceled || old.Attempt != 0 {
		t.Fatal("superseded prepared not retired", old)
	}
	order.ID = "new"
	order.PlanID = plan.ID
	if err := s.PrepareOrder(context.Background(), order, 11); err != nil {
		t.Fatal("old unsent reservation stuck", err)
	}
	adapter := &fakeExecutionAdapter{}
	e := executorFor(s, h, adapter)
	if err := e.Send("old", 12); err == nil {
		t.Fatal("superseded order emitted")
	}
	if err := e.Send("new", 12); err != nil {
		t.Fatal(err)
	}
	if len(adapter.calls()) != 1 {
		t.Fatal(adapter.calls())
	}
}

func TestIncompleteQueryRetainsUnknown(t *testing.T) {
	s, _, h, _ := testStore(t)
	order := planOrder(t, s, "order", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true}}
	adapter.submit = func(context.Context, OrderIntent, string) (SubmitReceipt, error) {
		return SubmitReceipt{}, errors.New("lost ack")
	}
	adapter.query = func(context.Context, string, string) (QueryResult, error) {
		return QueryResult{Found: true, Authoritative: true, Receipt: SubmitReceipt{ExchangeID: "found"}}, nil
	}
	e := executorFor(s, h, adapter)
	if err := e.Send(order.ID, 11); err == nil {
		t.Fatal("lost ack hidden")
	}
	if err := e.Recover(order.ID); err == nil {
		t.Fatal("query identity alone considered complete")
	}
	stored, _ := s.Order(context.Background(), order.ID)
	if stored.State != OrderUnknown {
		t.Fatal(stored)
	}
}

func TestProjectionReplayAndBoundedActiveSnapshot(t *testing.T) {
	s, _, h, path := testStore(t)
	e := executorFor(s, h, &fakeExecutionAdapter{})
	for n, side := range []OrderSide{Buy, Sell} {
		kind := EntryIntent
		if n == 1 {
			kind = ExitIntent
		}
		id := []string{"open", "close"}[n]
		order := planOrder(t, s, id, int64(n+1), side, kind, map[string]int64{"a": 10})
		if err := e.Send(id, 11); err != nil {
			t.Fatal(err)
		}
		if err := e.ApplyFill(FillReport{EventID: id + "-trade", OrderID: order.ID, Steps: 10, Price: intentPrice("100"), AtMS: 12}); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, _ := s.Snapshot(context.Background())
	if len(snapshot.Orders) != 0 || len(snapshot.Lots) != 0 {
		t.Fatal("historical objects leaked into runtime snapshot", snapshot)
	}
	lot, err := s.Lot(context.Background(), "a", "lot-a")
	if err != nil || lot.SignedSteps != 0 {
		t.Fatal("closed history lost", lot, err)
	}
	order, err := s.Order(context.Background(), "open")
	if err != nil || order.State != OrderFilled {
		t.Fatal(order, err)
	}
	var cursor int64
	seen := make(map[string]bool)
	stateEvents := 0
	networkBefore := len(e.Adapter.(*fakeExecutionAdapter).calls())
	for {
		events, err := s.EventsAfter(context.Background(), cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) == 0 {
			break
		}
		for _, event := range events {
			if seen[event.ID] || event.Checkpoint <= cursor {
				t.Fatal("projection event identity/order unstable")
			}
			seen[event.ID] = true
			cursor = event.Checkpoint
			if event.Kind == "OrderState" {
				var state OrderStateEvent
				if err := json.Unmarshal(event.Payload, &state); err != nil || state.OrderID == "" || len(state.Allocations) != 1 {
					t.Fatal(state, err)
				}
				stateEvents++
			}
		}
		if err := s.AdvanceProjection(context.Background(), "legacy", cursor); err != nil {
			t.Fatal(err)
		}
	}
	if cursor != snapshot.Checkpoint || stateEvents < 6 || len(e.Adapter.(*fakeExecutionAdapter).calls()) != networkBefore {
		t.Fatal("projection replay caused network or skipped states")
	}
	if err := s.AdvanceProjection(context.Background(), "legacy", cursor-1); err == nil {
		t.Fatal("cursor regressed")
	}
	if err := s.AdvanceProjection(context.Background(), "legacy", cursor+1); err == nil {
		t.Fatal("cursor skipped uncommitted event")
	}
	s.Close()
	reopened, err := OpenStore(path, s.key)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := reopened.ProjectionCursor(context.Background(), "legacy")
	if err != nil || after != cursor {
		t.Fatal(after, err)
	}
	events, err := reopened.EventsAfter(context.Background(), after, 2)
	if err != nil || len(events) != 0 {
		t.Fatal(events, err)
	}
}

func TestPendingIntentSurvivesCombinedPlansAndTrailingCheckpoint(t *testing.T) {
	s, _, _, _ := testStore(t)
	intent := testIntent(Sell)
	intent.Kind = ExitIntent
	intent.Conditions = IntentConditions{ActivationPrice: intentPrice("100"), TrailingPercent: intentPrice("10"), CreatedBar: 1, StopBars: 4}
	intent.State = PendingCondition
	plan := Plan{ID: "ts", Sequence: 1, DecisionMS: 10, ExpiresMS: 1000, Intents: []EligibleIntent{intent}}
	if err := s.SavePlan(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	observation := ExecutionObservation{Price: intentPrice("110"), AtMS: 11, ValidUntilMS: 1000, Bar: 2}
	if _, units, err := s.EvaluateIntent(context.Background(), intent.ID, observation, 11); err != nil || units != 0 {
		t.Fatal(units, err)
	}
	checkpointed, err := s.Intent(context.Background(), intent.ID)
	if err != nil || !checkpointed.TrailingActive || !checkpointed.TrailingAnchor.Equal(intentPrice("110")) {
		t.Fatal(checkpointed, err)
	}
	plan.ID = "cs"
	plan.Sequence = 2
	plan.Intents = []EligibleIntent{checkpointed}
	if err := s.SavePlan(context.Background(), plan); err != nil {
		t.Fatal("carried runtime state treated as new identity", err)
	}
	observation.Price = intentPrice("99")
	observation.AtMS = 12
	observation.Bar = 3
	if _, units, err := s.EvaluateIntent(context.Background(), intent.ID, observation, 12); err != nil || units != 10 {
		t.Fatal("cross-strategy plan reset trailing anchor", units, err)
	}
	observation.Bar = 5
	if expired, units, err := s.EvaluateIntent(context.Background(), intent.ID, observation, 13); err != nil || units != 0 || expired.State != Expired {
		t.Fatal(expired, units, err)
	}
	metadata := json.RawMessage(`{"legacyOrder":42,"stopLoss":80,"clientID":"stable"}`)
	if err := s.SaveStrategyCheckpoint(context.Background(), intent.Strategy, "legacy", metadata); err != nil {
		t.Fatal(err)
	}
	saved, err := s.StrategyCheckpoint(context.Background(), intent.Strategy, "legacy")
	if err != nil || string(saved) != string(metadata) {
		t.Fatal(string(saved), err)
	}
}

func TestExplicitHostLeaseAcrossDifferentPathsAndTMP(t *testing.T) {
	key := testIntent(Buy).Account
	leaseDir := filepath.Join(t.TempDir(), "shared-leases")
	s, err := OpenStoreWithLeaseDir(filepath.Join(t.TempDir(), "a.db"), key, leaseDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := OpenStoreWithLeaseDir(filepath.Join(t.TempDir(), "b.db"), key, leaseDir); err == nil {
		t.Fatal("independent registry/database admitted duplicate sender")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExecutionLeaseChild$")
	command.Env = append(os.Environ(), "BAN_EXEC_LEASE_CHILD=1", "BAN_EXEC_LEASE_DIR="+leaseDir, "BAN_EXEC_LEASE_DB="+filepath.Join(t.TempDir(), "child.db"), "TMP="+t.TempDir())
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("cross-process lease: %v %s", err, output)
	}
	s.Close()
	reopened, err := OpenStoreWithLeaseDir(filepath.Join(t.TempDir(), "c.db"), key, leaseDir)
	if err != nil {
		t.Fatal("joined lease never released", err)
	}
	reopened.Close()
}
func TestExecutionLeaseChild(t *testing.T) {
	if os.Getenv("BAN_EXEC_LEASE_CHILD") != "1" {
		return
	}
	if _, err := OpenStoreWithLeaseDir(os.Getenv("BAN_EXEC_LEASE_DB"), testIntent(Buy).Account, os.Getenv("BAN_EXEC_LEASE_DIR")); err == nil {
		t.Fatal("child admitted while host sender lease held")
	}
}

func TestExternalIsolationRequiresFullReconciliation(t *testing.T) {
	s, _, h, _ := testStore(t)
	fundStrategies(t, s)
	external := ExternalPositionEvent{ID: "manual", Kind: ExternalCashChange, Instrument: ledgerInstrument(), Side: Buy, Steps: 5, Price: intentPrice("90"), AtMS: 10}
	if _, err := s.ApplyExternalPosition(context.Background(), external); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := s.Snapshot(context.Background())
	assertNAVConservation(t, snapshot, "100")
	if !snapshot.RiskFrozen || len(snapshot.ExternalPositions) != 1 || len(snapshot.Lots) != 0 {
		t.Fatal(snapshot)
	}
	if _, err := executorFor(s, h, &fakeExecutionAdapter{}).PrepareRebalance(domainRequest("blocked", 1, 11, "100", 10, -6)); err == nil {
		t.Fatal("unassigned external position did not freeze risk")
	}
	if _, err := s.Reconcile(context.Background(), AccountReconciliation{ID: "premature", AccountCash: intentPrice("1000"), Positions: map[string]int64{"BTC": 5}}); err == nil {
		t.Fatal("cash/position equality alone attributed external ownership")
	}
	external.ID = "liquidation"
	external.Kind = Liquidation
	external.Side = Sell
	external.Price = intentPrice("100")
	if _, err := s.ApplyExternalPosition(context.Background(), external); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Reconcile(context.Background(), AccountReconciliation{ID: "wrong", AccountCash: intentPrice("1000"), Positions: map[string]int64{"BTC": 0}}); err == nil {
		t.Fatal("mismatched snapshot unfroze account")
	}
	if _, err := s.Reconcile(context.Background(), AccountReconciliation{ID: "known", AccountCash: intentPrice("1005"), Positions: map[string]int64{"BTC": 0}}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ = s.Snapshot(context.Background())
	assertNAVConservation(t, snapshot, "100")
	if snapshot.RiskFrozen {
		t.Fatal("exact reconciliation remained frozen")
	}
	if _, err := s.Plan(context.Background(), "blocked"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("failed frozen preparation left plan", err)
	}
}
