package execution

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/shopspring/decimal"
)

func fundStrategies(t *testing.T, s *Store) {
	t.Helper()
	for _, event := range []CashEvent{{ID: "initial", Kind: Reconciliation, AccountDelta: intentPrice("1000"), Postings: []CashPosting{{Amount: intentPrice("1000")}}}, {ID: "capital", Kind: CapitalTransfer, Postings: []CashPosting{{Strategy: "a", Amount: intentPrice("500")}, {Strategy: "b", Amount: intentPrice("500")}, {Amount: intentPrice("-1000")}}}} {
		if _, err := s.ApplyCashEvent(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
}
func domainRisk() PortfolioRisk {
	return PortfolioRisk{MarginRate: intentPrice("1"), MaxAccountMargin: intentPrice("1000"), MaxVirtualGross: intentPrice("1000"), StrategyGrossLimits: map[StrategyID]decimal.Decimal{"a": intentPrice("500"), "b": intentPrice("500")}, Marks: map[string]decimal.Decimal{"BTC": intentPrice("100")}}
}
func domainRequest(id string, seq, at int64, price string, a, b int64) CombinedRebalance {
	return CombinedRebalance{PlanID: id, Sequence: seq, DecisionMS: at, ExpiresMS: 1000, Risk: domainRisk(), Requests: []InstrumentRebalance{{Instrument: ledgerInstrument(), Targets: []ExecutableTarget{{Strategy: "a", Lot: "lot-a", SignedSteps: a}, {Strategy: "b", Lot: "lot-b", SignedSteps: b}}, Quote: VisibleQuote{Bid: intentPrice(price), Ask: intentPrice(price), AtMS: at, ReceivedMS: at, ValidUntilMS: 1000, Bar: at}}}}
}
func assertNAVConservation(t *testing.T, snapshot AccountSnapshot, mark string) {
	t.Helper()
	marks := map[string]decimal.Decimal{"BTC": intentPrice(mark)}
	real, err := snapshot.Equity(marks)
	if err != nil {
		t.Fatal(err)
	}
	virtual := snapshot.UnassignedCash
	synthetic := snapshot.UnassignedCash
	for _, cash := range snapshot.SyntheticStrategyCash {
		virtual = virtual.Add(cash)
		synthetic = synthetic.Add(cash)
	}
	var net int64
	for _, lot := range snapshot.Lots {
		virtual = virtual.Add(lot.Unrealized(marks[lot.Instrument.ID]))
		net += lot.SignedSteps
	}
	for _, lot := range snapshot.ExternalPositions {
		virtual = virtual.Add(lot.Unrealized(marks[lot.Instrument.ID]))
		net += lot.SignedSteps
	}
	if !virtual.Equal(real) {
		t.Fatalf("virtual NAV %s != actual equity %s: %+v", virtual, real, snapshot)
	}
	if !synthetic.Sub(snapshot.AccountSettledCash).Equal(snapshot.PnLReclassification) {
		t.Fatal("unexplained synthetic/settled cash bridge", snapshot)
	}
	var actualNet int64
	for _, position := range snapshot.ActualPositions {
		actualNet += position.SignedSteps
	}
	if actualNet != net {
		t.Fatal("actual net != lots + declared external", actualNet, net)
	}
}
func openMixed(t *testing.T, s *Store, h *AccountHandle) (*OwnerExecutor, *fakeExecutionAdapter) {
	t.Helper()
	fundStrategies(t, s)
	adapter := &fakeExecutionAdapter{}
	executor := executorFor(s, h, adapter)
	prepared, err := executor.PrepareRebalance(domainRequest("initial-plan", 1, 10, "100", 10, -6))
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.InternalMatchIDs) != 1 || len(prepared.OrderIDs) != 1 {
		t.Fatal(prepared)
	}
	snapshot, err := s.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertNAVConservation(t, snapshot, "100")
	if !snapshot.AccountSettledCash.Equal(intentPrice("1000")) || len(snapshot.ActualPositions) != 0 {
		t.Fatal("internal cross altered actual book")
	}
	if err := executor.Send(prepared.OrderIDs[0], 11); err != nil {
		t.Fatal(err)
	}
	if err := executor.ApplyFill(FillReport{EventID: "initial-external", OrderID: prepared.OrderIDs[0], Steps: 4, Price: intentPrice("100"), AtMS: 11}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ = s.Snapshot(context.Background())
	assertNAVConservation(t, snapshot, "100")
	if snapshot.ActualPositions[0].SignedSteps != 4 || !snapshot.ActualPositions[0].CostBasis.Equal(intentPrice("40")) {
		t.Fatal(snapshot)
	}
	return executor, adapter
}

func TestMixedRealVirtualReversalFundingAndRestart(t *testing.T) {
	s, _, h, path := testStore(t)
	e, adapter := openMixed(t, s, h)
	prepared, err := e.PrepareRebalance(domainRequest("exit-a", 2, 20, "110", 0, -6))
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.OrderIDs) != 2 {
		t.Fatal(prepared)
	}
	decrease, increase := prepared.OrderIDs[0], prepared.OrderIDs[1]
	if err := e.Send(increase, 21); err == nil {
		t.Fatal("increase emitted before required decrease")
	}
	if err := e.Send(decrease, 21); err != nil {
		t.Fatal(err)
	}
	if err := e.ApplyFill(FillReport{EventID: "reduce-part", OrderID: decrease, Steps: 2, Price: intentPrice("110"), AtMS: 21}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := s.Snapshot(context.Background())
	assertNAVConservation(t, snapshot, "110")
	if !snapshot.AccountSettledCash.Equal(intentPrice("1002")) || snapshot.ActualPositions[0].SignedSteps != 2 {
		t.Fatal(snapshot)
	}
	if err := e.Send(increase, 22); err == nil {
		t.Fatal("partial reduce ACK bypassed durable dependency")
	}
	s.Close()
	reopened, err := OpenStore(path, s.key)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	e.Store = reopened
	if err := e.Send(increase, 22); err == nil {
		t.Fatal("restart lost dependency")
	}
	if err := e.ApplyFill(FillReport{EventID: "reduce-rest", OrderID: decrease, Steps: 2, Price: intentPrice("110"), AtMS: 22}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ = reopened.Snapshot(context.Background())
	assertNAVConservation(t, snapshot, "110")
	if snapshot.ActualPositions[0].SignedSteps != 0 || !snapshot.AccountSettledCash.Equal(intentPrice("1004")) {
		t.Fatal(snapshot)
	}
	if err := e.Send(increase, 23); err != nil {
		t.Fatal(err)
	}
	fill := FillReport{EventID: "reverse-open", OrderID: increase, Steps: 6, Price: intentPrice("110"), AtMS: 23}
	if err := e.ApplyFill(fill); err != nil {
		t.Fatal(err)
	}
	if err := e.ApplyFill(fill); err != nil {
		t.Fatal(err)
	}
	snapshot, _ = reopened.Snapshot(context.Background())
	assertNAVConservation(t, snapshot, "110")
	if snapshot.ActualPositions[0].SignedSteps != -6 || !snapshot.ActualPositions[0].CostBasis.Equal(intentPrice("66")) || !snapshot.PnLReclassification.Equal(intentPrice("6")) || !snapshot.SyntheticStrategyCash["a"].Equal(intentPrice("510")) || len(snapshot.Lots) != 1 || snapshot.Lots[0].SignedSteps != -6 {
		t.Fatal("real/virtual reversal bridge incorrect", snapshot)
	}
	funding := FundingSettlement{ID: "funding-positive", Instrument: ledgerInstrument(), Mark: intentPrice("110"), Rate: intentPrice("0.01"), AccountAmount: intentPrice("0.66"), AtMS: 24}
	if _, err := reopened.ApplyFunding(context.Background(), funding); err != nil {
		t.Fatal(err)
	}
	if applied, err := reopened.ApplyFunding(context.Background(), funding); err != nil || applied {
		t.Fatal(applied, err)
	}
	snapshot, _ = reopened.Snapshot(context.Background())
	assertNAVConservation(t, snapshot, "110")
	if !snapshot.AccountSettledCash.Equal(intentPrice("1004.66")) || !snapshot.SyntheticStrategyCash["b"].Equal(intentPrice("500.66")) {
		t.Fatal(snapshot)
	}
	funding.ID = "funding-negative"
	funding.Rate = intentPrice("-0.01")
	funding.AccountAmount = intentPrice("-0.66")
	if _, err := reopened.ApplyFunding(context.Background(), funding); err != nil {
		t.Fatal(err)
	}
	snapshot, _ = reopened.Snapshot(context.Background())
	assertNAVConservation(t, snapshot, "110")
	if len(adapter.calls()) != 3 {
		t.Fatal("unexpected duplicate network sends", adapter.calls())
	}
}

func TestCombinedPrepareFaultsAreAtomicAndRetryable(t *testing.T) {
	for _, stage := range []string{"plan", "internal", "order"} {
		t.Run(stage, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			fundStrategies(t, s)
			e := executorFor(s, h, &fakeExecutionAdapter{})
			request := domainRequest("fault", 1, 10, "100", 10, -6)
			checkpoint := StrategyCheckpoint{Strategy: "a", Name: "requests", Payload: []byte(`{"accepted":true}`)}
			previous := []byte(`{"accepted":false}`)
			if err := s.SaveStrategyCheckpoint(context.Background(), checkpoint.Strategy, checkpoint.Name, previous); err != nil {
				t.Fatal(err)
			}
			trigger := "CREATE TRIGGER prepare_fault BEFORE INSERT ON exec_plan WHEN NEW.id='fault' BEGIN SELECT RAISE(ABORT,'after planning'); END"
			if stage == "internal" {
				trigger = "CREATE TRIGGER prepare_fault BEFORE INSERT ON exec_internal_allocation BEGIN SELECT RAISE(ABORT,'after internal lot update'); END"
			}
			if stage == "order" {
				trigger = "CREATE TRIGGER prepare_fault BEFORE INSERT ON exec_event WHEN NEW.kind='OrderState' BEGIN SELECT RAISE(ABORT,'after order/allocation insert'); END"
			}
			if _, err := s.db.Exec(trigger); err != nil {
				t.Fatal(err)
			}
			if _, err := e.PrepareRebalanceWithCheckpoint(request, checkpoint); err == nil {
				t.Fatal("fault did not abort")
			}
			if body, err := s.StrategyCheckpoint(context.Background(), checkpoint.Strategy, checkpoint.Name); err != nil || string(body) != string(previous) {
				t.Fatal("rejected checkpoint escaped transaction", string(body), err)
			}
			if _, err := s.Plan(context.Background(), "fault"); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("partial plan escaped transaction", err)
			}
			snapshot, _ := s.Snapshot(context.Background())
			if len(snapshot.Lots) != 0 || len(snapshot.Orders) != 0 || snapshot.Checkpoint != 2 {
				t.Fatal("partial attribution/outbox escaped", snapshot)
			}
			if _, err := s.db.Exec("DROP TRIGGER prepare_fault"); err != nil {
				t.Fatal(err)
			}
			ready, err := e.PrepareRebalanceWithCheckpoint(request, checkpoint)
			if err != nil || len(ready.OrderIDs) != 1 {
				t.Fatal(ready, err)
			}
			if body, err := s.StrategyCheckpoint(context.Background(), checkpoint.Strategy, checkpoint.Name); err != nil || string(body) != string(checkpoint.Payload) {
				t.Fatal("accepted checkpoint missing", string(body), err)
			}
			again, err := e.PrepareRebalanceWithCheckpoint(request, checkpoint)
			if err != nil || len(again.OrderIDs) != 1 || again.OrderIDs[0] != ready.OrderIDs[0] {
				t.Fatal("retry changed frozen order", again, err)
			}
			rows, err := s.db.Query("PRAGMA foreign_key_check")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			if rows.Next() {
				t.Fatal("broken persistent relationship")
			}
		})
	}
}

func TestCombinedStrategyBudgetsAndOverLimitReduction(t *testing.T) {
	s, _, h, _ := testStore(t)
	fundStrategies(t, s)
	e := executorFor(s, h, &fakeExecutionAdapter{})
	request := domainRequest("two", 1, 10, "100", 6, 0)
	request.Risk.StrategyGrossLimits["a"] = intentPrice("100")
	second := request.Requests[0]
	second.Instrument.ID = "ETH"
	second.Targets = []ExecutableTarget{{Strategy: "a", Lot: "eth-a", SignedSteps: 6}}
	request.Requests = append(request.Requests, second)
	if _, err := e.PrepareRebalance(request); err == nil {
		t.Fatal("two instruments consumed separate copies of strategy gross cap")
	}
	request.Risk.StrategyGrossLimits["a"] = intentPrice("500")
	request.Risk.MarginRate = intentPrice("1")
	if _, err := s.ApplyCashEvent(context.Background(), CashEvent{ID: "reduce-budget", Kind: CapitalTransfer, Postings: []CashPosting{{Strategy: "a", Amount: intentPrice("-400")}, {Amount: intentPrice("400")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.PrepareRebalance(request); err == nil {
		t.Fatal("two instruments consumed separate copies of strategy NAV")
	}
	// Restore capital, establish a real position, then allow decreasing it even
	// when current account/virtual gross exceeds newly lowered caps.
	if _, err := s.ApplyCashEvent(context.Background(), CashEvent{ID: "restore-budget", Kind: CapitalTransfer, Postings: []CashPosting{{Strategy: "a", Amount: intentPrice("400")}, {Amount: intentPrice("-400")}}}); err != nil {
		t.Fatal(err)
	}
	open := domainRequest("open", 1, 10, "100", 10, 0)
	ready, err := e.PrepareRebalance(open)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Send(ready.OrderIDs[0], 11); err != nil {
		t.Fatal(err)
	}
	if err := e.ApplyFill(FillReport{EventID: "openfill", OrderID: ready.OrderIDs[0], Steps: 10, Price: intentPrice("100"), AtMS: 11}); err != nil {
		t.Fatal(err)
	}
	close := domainRequest("reduce", 2, 20, "100", 5, 0)
	close.Risk.MaxAccountMargin = intentPrice("1")
	close.Risk.MaxVirtualGross = intentPrice("1")
	close.Risk.StrategyGrossLimits["a"] = intentPrice("1")
	if _, err := e.PrepareRebalance(close); err != nil {
		t.Fatal("pure decrease blocked by pre-existing over-limit exposure", err)
	}
}

func TestUntouchedConfirmedOrdersReserveCombinedBudgets(t *testing.T) {
	for _, budget := range []string{"strategy", "account", "virtual"} {
		t.Run(budget, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			fundStrategies(t, s)
			e := executorFor(s, h, &fakeExecutionAdapter{})
			open := domainRequest("pending", 1, 10, "100", 6, 0)
			ready, err := e.PrepareRebalance(open)
			if err != nil {
				t.Fatal(err)
			}
			if err := e.Send(ready.OrderIDs[0], 11); err != nil {
				t.Fatal(err)
			}
			next := domainRequest("other", 2, 20, "100", 6, 0)
			firstID := next.Requests[0].Instrument.ID
			next.Risk.Marks[firstID] = intentPrice("100")
			next.Requests[0].Instrument.ID = "ETH"
			next.Requests[0].Targets[0].Lot = "eth-a"
			next.Requests[0].Targets[1].Lot = "eth-b"
			switch budget {
			case "strategy":
				next.Risk.StrategyGrossLimits["a"] = intentPrice("100")
			case "account":
				next.Risk.MaxAccountMargin = intentPrice("10")
			case "virtual":
				next.Risk.MaxVirtualGross = intentPrice("100")
			}
			if _, err := e.PrepareRebalance(next); err == nil {
				t.Fatal("confirmed untouched order did not reserve", budget, "budget")
			}
			// First-send revalidation must enforce the same untouched reservation.
			snapshot, err := s.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			instrument := next.Requests[0].Instrument
			next.Risk.Marks[instrument.ID] = intentPrice("100")
			plan := Plan{Instruments: []Instrument{instrument}, Targets: []ExecutableTarget{{Strategy: "a", Lot: "eth-a", Instrument: instrument.ID, SignedSteps: 6}}, RiskValidUntilMS: 1000}
			if err := validatePlanReservation(snapshot, plan, next.Risk, 21); err == nil {
				t.Fatal("send-time check omitted untouched", budget, "reservation")
			}
		})
	}
}

func TestCarriedTargetsPreserveMembershipWithoutExecution(t *testing.T) {
	s, _, h, _ := testStore(t)
	fundStrategies(t, s)
	e := executorFor(s, h, &fakeExecutionAdapter{})
	request := domainRequest("carry-target", 1, 10, "100", 0, 0)
	carried := ExecutableTarget{Strategy: "a", Lot: "old-lot", Instrument: "ETH", SignedSteps: 10}
	request.CarryTargets = []ExecutableTarget{carried}
	instrument := ledgerInstrument()
	instrument.ID = "ETH"
	request.CarryInstruments = []Instrument{instrument}
	request.Risk.Marks["ETH"] = intentPrice("100")
	ready, err := e.PrepareRebalance(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready.OrderIDs) != 0 || len(ready.Plan.Intents) != 0 || len(ready.Plan.Targets) != 3 || len(ready.Plan.CarriedTargets) != 1 || ready.Plan.CarriedTargets[0] != carried {
		t.Fatal("carried declaration initiated execution or lost membership", ready)
	}
	stored, err := s.Plan(context.Background(), ready.Plan.ID)
	if err != nil || len(stored.CarriedTargets) != 1 || stored.CarriedTargets[0] != carried {
		t.Fatal("carry freeze did not persist", stored, err)
	}
	for _, invalid := range [][]ExecutableTarget{{carried, carried}, {{Strategy: "a", Lot: "lot-a", Instrument: "BTC", SignedSteps: 1}}} {
		request.PlanID, request.Sequence, request.CarryTargets = "invalid-carry", 2, invalid
		if _, err := e.PrepareRebalance(request); err == nil {
			t.Fatal("conflicting carried targets accepted")
		}
	}
}

func TestCarriedUnfilledTargetsReserveRiskAndReductionsDoNotReleaseIt(t *testing.T) {
	for _, scenario := range []string{"unfilled-increase", "unexecuted-reduction", "pure-reduction"} {
		t.Run(scenario, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			fundStrategies(t, s)
			e := executorFor(s, h, &fakeExecutionAdapter{})
			open := domainRequest("prior", 1, 10, "100", 10, 0)
			ready, err := e.PrepareRebalance(open)
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "unfilled-increase" {
				if err := e.Send(ready.OrderIDs[0], 11); err != nil {
					t.Fatal(err)
				}
				if err := e.ApplyFill(FillReport{EventID: "prior-fill", OrderID: ready.OrderIDs[0], Steps: 10, Price: intentPrice("100"), AtMS: 11}); err != nil {
					t.Fatal(err)
				}
			}
			next := domainRequest("next", 2, 20, "100", 6, 0)
			eth := ledgerInstrument()
			eth.ID = "ETH"
			next.Risk.Marks["ETH"] = intentPrice("100")
			next.Risk.StrategyGrossLimits["a"] = intentPrice("150")
			if scenario == "pure-reduction" {
				next.Requests[0].Targets[0].SignedSteps = 5
				next.CarryTargets = []ExecutableTarget{{Strategy: "a", Lot: "eth-unfilled", Instrument: "ETH", SignedSteps: 10}}
				next.CarryInstruments = []Instrument{eth}
				next.Risk.StrategyGrossLimits["a"], next.Risk.MaxVirtualGross, next.Risk.MaxAccountMargin = intentPrice("1"), intentPrice("1"), intentPrice("1")
			} else {
				next.Requests[0].Instrument = eth
				next.Requests[0].Targets[0].Lot = "eth-a"
				next.Requests[0].Targets[1].Lot = "eth-b"
				carriedSteps := int64(10)
				if scenario == "unexecuted-reduction" {
					carriedSteps = 0
				}
				next.CarryTargets = []ExecutableTarget{{Strategy: "a", Lot: "lot-a", Instrument: "BTC", SignedSteps: carriedSteps}}
			}
			result, err := e.PrepareRebalance(next)
			if scenario == "pure-reduction" {
				if err != nil {
					t.Fatal("pure reduction rejected because carried risk already exceeds lowered limits", err)
				}
				if len(result.OrderIDs) != 1 {
					t.Fatal("untouched target initiated extra execution", result)
				}
				if err := e.Send(result.OrderIDs[0], 21); err != nil {
					t.Fatal("first-send pure reduction rejected", err)
				}
			} else if err == nil {
				t.Fatal("carried target failed to preserve reservation", scenario)
			}
		})
	}
}
