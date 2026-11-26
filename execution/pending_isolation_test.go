package execution

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

// isolationFixture extends sequenceFixture with helpers for stop entries.
func (f *sequenceFixture) createStop(t *testing.T, strategy StrategyID, lot string, steps int64, stop string, now int64) {
	t.Helper()
	command := PendingCommand{ID: lot, Strategy: strategy, Lot: VirtualLotID(lot), Action: PendingCreate,
		Create: &PendingCreateSpec{Instrument: "BTC", Side: Buy, QuantitySteps: steps, Conditions: IntentConditions{Stop: intentPrice(stop)}}}
	if _, err := f.b.ApplyPendingCommand(context.Background(), command, now); err != nil {
		t.Fatal(err)
	}
}

func (f *sequenceFixture) book(t *testing.T, lot string) PendingDefinition {
	t.Helper()
	book, err := f.b.service.store.LoadPendingBook(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range book {
		if string(d.Order.Lot) == lot {
			return d
		}
	}
	t.Fatal("lot missing from pending book", lot)
	return PendingDefinition{}
}

func (f *sequenceFixture) requireIsolated(t *testing.T, strategy StrategyID, lot, code string) {
	t.Helper()
	d := f.book(t, lot)
	if !d.Order.Canceled || d.Order.Entry.State != Canceled || d.Order.Desired != 0 {
		t.Fatalf("excluded entry is not terminal: %+v", d.Order)
	}
	body, err := f.b.service.store.StrategyCheckpoint(context.Background(), strategy, "pending-isolated:"+lot)
	var record isolationRecord
	if err != nil || json.Unmarshal(body, &record) != nil || record.Code != code {
		t.Fatal("audit record missing or wrong", string(body), err)
	}
	events, err := f.b.service.store.EventsAfter(context.Background(), 0, 10000)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		var accepted StrategyAcceptedEvent
		if event.Kind == "StrategyAccepted" && json.Unmarshal(event.Payload, &accepted) == nil && accepted.Lot == VirtualLotID(lot) && accepted.Rejected == code && accepted.Kind == EntryIntent {
			return
		}
	}
	t.Fatal("audit event with the rejection code missing")
}

func requireNotFrozen(t *testing.T, f *sequenceFixture) {
	t.Helper()
	snapshot, err := f.b.Snapshot(context.Background())
	if err != nil || snapshot.RiskFrozen {
		t.Fatal("account frozen", err)
	}
}

// An entry that becomes executable while breaching its strategy budget is
// excluded; the other strategy's entry in the same tick fills; nothing freezes.
func TestIsolationRiskLimitExcludesEntryOthersProceed(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		f.createStop(t, "a", "lot-a", 45, "105", 12)
		f.createStop(t, "b", "lot-b", 10, "105", 13)
		f.tick(t, "120", 14)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"b/lot-b": 10}) {
			t.Fatal("only the compliant entry may fill", got)
		}
		f.requireIsolated(t, "a", "lot-a", RejectRiskLimit)
		fills := f.venue.Metrics().Fills
		f.tick(t, "120", 15)
		f.tick(t, "120", 16)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"b/lot-b": 10}) || f.venue.Metrics().Fills != fills {
			t.Fatal("excluded entry came back or later ticks traded", got)
		}
		requireNotFrozen(t, f)
	})
}

func TestIsolationVenueMinimumExcludesAutonomousEntry(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		market := f.b.service.markets["BTC"]
		market.instrument.MinNotional = intentPrice("100")
		f.b.service.markets["BTC"] = market
		*f.price = "120"
		f.create(t, "a", "lot-a", Buy, "100", "", 12)
		f.tick(t, "80", 14) // 1 BTC at 80 is below the 100 minimum
		if got := f.lots(t); len(got) != 0 {
			t.Fatal("sub-minimum entry traded", got)
		}
		f.requireIsolated(t, "a", "lot-a", "venue_minimum")
		f.tick(t, "80", 15)
		requireNotFrozen(t, f)
	})
}

func (f *sequenceFixture) createProtected(t *testing.T, strategy StrategyID, lot string, side OrderSide, kind ProtectionKind, level string, now int64) {
	t.Helper()
	command := PendingCommand{ID: lot, Strategy: strategy, Lot: VirtualLotID(lot), Action: PendingCreate,
		Create: &PendingCreateSpec{Instrument: "BTC", Side: side, QuantitySteps: 10, Protections: []PositionProtection{{Kind: kind, Level: intentPrice(level)}}}}
	if _, err := f.b.ApplyPendingCommand(context.Background(), command, now); err != nil {
		t.Fatal(err)
	}
}

// A long lot's take-profit and a short lot's stop of one strategy fire in the
// same tick. The two exits cross internally: both lots close in that tick at
// the same price, with no venue order and nothing held for a later tick.
func TestIsolationOppositeReductionsSequenced(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		f.createProtected(t, "a", "lot-1", Buy, TakeProfitProtection, "110", 12)
		f.createProtected(t, "a", "lot-2", Sell, StopLossProtection, "110", 13)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"a/lot-1": 10, "a/lot-2": -10}) {
			t.Fatal("setup", got)
		}
		fills := f.venue.Metrics().Fills
		f.tick(t, "110", 14)
		if got := f.lots(t); len(got) != 0 {
			t.Fatal("opposite exits of one strategy must cross internally in one tick", got)
		}
		if f.venue.Metrics().Fills != fills {
			t.Fatal("internally crossed exits must not reach the venue", f.venue.Metrics())
		}
		requireNotFrozen(t, f)
	})
}

func TestSequenceOppositeReductionsRule(t *testing.T) {
	instrument := ledgerInstrument()
	build := func(held, target []int64) (AccountSnapshot, []InstrumentRebalance) {
		var snapshot AccountSnapshot
		var targets []ExecutableTarget
		for n := range target {
			lot := VirtualLotID("lot-" + string(rune('1'+n)))
			if held[n] != 0 {
				snapshot.Lots = append(snapshot.Lots, VirtualLot{Strategy: "a", ID: lot, Instrument: instrument, SignedSteps: held[n]})
			}
			targets = append(targets, ExecutableTarget{Strategy: "a", Lot: lot, SignedSteps: target[n]})
		}
		return snapshot, []InstrumentRebalance{{Instrument: instrument, Targets: targets}}
	}
	snapshot, requests := build([]int64{10, -10}, []int64{0, 0})
	// Entry-only form keeps rejecting.
	if _, n := sequenceOppositeLots(snapshot, requests); n != 0 {
		t.Fatal("entry-only rule delayed a reduction")
	}
	out, outcome := sequenceOpposite(snapshot, requests, true, nil)
	if out[0].Targets[0].SignedSteps != 0 || out[0].Targets[1].SignedSteps != -10 || !reflect.DeepEqual(outcome.HeldReductions, []lotRef{{"a", "lot-2"}}) || !reflect.DeepEqual(outcome.Executed, []lotRef{{"a", "lot-1"}}) {
		t.Fatal("lowest lot must win without history", out[0].Targets, outcome)
	}
	// A reduction that was already held outranks a lower lot ID.
	out, outcome = sequenceOpposite(snapshot, requests, true, func(r lotRef) int {
		if r.Lot == "lot-2" {
			return 1
		}
		return 0
	})
	if out[0].Targets[0].SignedSteps != 10 || out[0].Targets[1].SignedSteps != 0 || !reflect.DeepEqual(outcome.HeldReductions, []lotRef{{"a", "lot-1"}}) {
		t.Fatal("previously held reduction must win", out[0].Targets, outcome)
	}
	if requests[0].Targets[0].SignedSteps != 0 {
		t.Fatal("input mutated")
	}
}

// Only a reduction can trip the refusal: nothing is excludable, so the tick
// still fails exactly as before and nothing is written. The refusal is the
// minimum quantity, which applies to reductions (the minimum notional does not):
// a long's take-profit and another strategy's short stop fire together and net
// to a 4-step reduce-only sell, below the 5-step minimum.
func TestIsolationNeverExcludesReduction(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		market := f.b.service.markets["BTC"]
		market.instrument.MinSteps = 5
		f.b.service.markets["BTC"] = market
		f.createProtected(t, "a", "lot-1", Buy, TakeProfitProtection, "110", 12)
		short := PendingCommand{ID: "lot-2", Strategy: "b", Lot: "lot-2", Action: PendingCreate,
			Create: &PendingCreateSpec{Instrument: "BTC", Side: Sell, QuantitySteps: 6, Protections: []PositionProtection{{Kind: StopLossProtection, Level: intentPrice("110")}}}}
		if _, err := f.b.ApplyPendingCommand(context.Background(), short, 13); err != nil {
			t.Fatal(err)
		}
		want := map[string]int64{"a/lot-1": 10, "b/lot-2": -6}
		if got := f.lots(t); !reflect.DeepEqual(got, want) {
			t.Fatal("setup", got)
		}
		*f.price = "110"
		err := f.b.AdvancePendingExecution(context.Background(), 14)
		if !errors.Is(err, ErrVenueMinimum) {
			t.Fatal("reduction refusal must still fail the tick", err)
		}
		if got := f.lots(t); !reflect.DeepEqual(got, want) {
			t.Fatal("reduction state changed", got)
		}
		for _, id := range []string{"lot-1", "lot-2"} {
			if d := f.book(t, id); d.Order.Canceled {
				t.Fatal("durable book changed", id)
			}
		}
	})
}

// A stop whose closing sell is worth less than the minimum notional is a
// reduce-only exit: it is prepared and sent, the venue is the judge.
func TestReduceOnlyStopBelowMinNotionalExecutes(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		market := f.b.service.markets["BTC"]
		market.instrument.MinNotional = intentPrice("95")
		f.b.service.markets["BTC"] = market
		f.createProtected(t, "a", "lot-1", Buy, StopLossProtection, "95", 12)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"a/lot-1": 10}) {
			t.Fatal("setup", got)
		}
		fills := f.venue.Metrics().Fills
		f.tick(t, "90", 14) // 1 BTC worth 90 < 95
		if got := f.lots(t); len(got) != 0 {
			t.Fatal("stop below the minimum notional did not close", got)
		}
		if f.venue.Metrics().Fills == fills {
			t.Fatal("exit never reached the venue")
		}
		requireNotFrozen(t, f)
	})
}

// PrepareOrder keeps the minimum quantity for every order and the minimum
// notional for every order that is not reduce-only.
func TestPrepareOrderMinimums(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reduceOnly bool
		steps      int64
		minSteps   int64
		ok         bool
	}{
		{"increasing below notional refused", false, 5, 0, false}, // 0.5 BTC at 100 = 50 < 100
		{"reduce-only below notional prepared", true, 5, 0, true},
		{"reduce-only below min steps refused", true, 5, 6, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _, _ := testStore(t)
			instrument := ledgerInstrument()
			instrument.MinNotional = intentPrice("100")
			instrument.MinSteps = tc.minSteps
			kind := EntryIntent
			if tc.reduceOnly {
				kind = ExitIntent
			}
			plan := Plan{ID: "plan-min", Sequence: 1, DecisionMS: 10, ExpiresMS: 1000}
			intent := EligibleIntent{ID: "min-a", Account: s.key, Strategy: "a", Lot: "lot-a", Instrument: "BTC", Kind: kind, Side: Sell, QuantitySteps: tc.steps, State: Eligible, Conditions: IntentConditions{CreatedBar: 5}}
			plan.Intents = append(plan.Intents, intent)
			order := OrderIntent{ID: "min", PlanID: plan.ID, Instrument: instrument, Side: Sell, Steps: tc.steps, ReduceOnly: tc.reduceOnly,
				Observation: ExecutionObservation{Price: intentPrice("100"), AtMS: 10, ValidUntilMS: 1000, Bar: 5},
				Allocations: []FillAllocation{{ID: "a", IntentID: intent.ID, Strategy: "a", Lot: "lot-a", Side: Sell, Kind: kind, Steps: tc.steps}}}
			if err := s.SavePlan(context.Background(), plan); err != nil {
				t.Fatal(err)
			}
			err := s.PrepareOrder(context.Background(), order, 10)
			if tc.ok && err != nil {
				t.Fatal("must be prepared", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("must be refused")
			}
		})
	}
}

func isolationUnit() (AccountSnapshot, []PendingDefinition, []InstrumentRebalance) {
	instrument := ledgerInstrument()
	var definitions []PendingDefinition
	var targets []ExecutableTarget
	for _, lot := range []VirtualLotID{"lot-1", "lot-2"} {
		definitions = append(definitions, PendingDefinition{Instrument: instrument, CommandRevision: 1, Order: PendingOrder{Strategy: "a", Lot: lot, Entry: EligibleIntent{Strategy: "a", Lot: lot, Instrument: "BTC", Kind: EntryIntent, Side: Buy, QuantitySteps: 5, State: PendingCondition}}})
		targets = append(targets, ExecutableTarget{Strategy: "a", Lot: lot, SignedSteps: 5})
	}
	return AccountSnapshot{}, definitions, []InstrumentRebalance{{Instrument: instrument, Targets: targets}}
}

func TestIsolationUnclassifiedAndBoundedFallBackToFreeze(t *testing.T) {
	snapshot, definitions, requests := isolationUnit()
	calls := 0
	prepare := func([]InstrumentRebalance, []StrategyCheckpoint) (PreparedRebalance, error) {
		calls++
		return PreparedRebalance{}, rejected(RejectRiskLimit, "always")
	}
	plain := errors.New("execution: something else")
	if _, err := isolateRejections(snapshot, definitions, requests, 10, plain, prepare); err != plain || calls != 0 {
		t.Fatal("an unclassified error must be returned untouched", err, calls)
	}
	joined := errors.Join(rejected(RejectRiskLimit, "x"), plain)
	if _, err := isolateRejections(snapshot, definitions, requests, 10, joined, prepare); err != joined || calls != 0 {
		t.Fatal("a joined (uncertain) error must freeze", err, calls)
	}
	if _, err := isolateRejections(snapshot, definitions, requests, 10, rejected(RejectSelfCross, "x"), prepare); calls != 0 || err == nil {
		t.Fatal("self_cross is sequenced, never isolated", err, calls)
	}
	_, definitions, requests = isolationUnit()
	_, err := isolateRejections(snapshot, definitions, requests, 10, rejected(RejectRiskLimit, "first"), prepare)
	if err == nil || calls != 2 {
		t.Fatal("exclusion must stop once every candidate is gone", err, calls)
	}
	// An exit-attributed refusal never excludes an entry.
	_, definitions, requests = isolationUnit()
	calls = 0
	exit := rejectedAt(RejectLimitOffTick, "x", rejectionScope{Strategy: "a", Lot: "lot-1", Instrument: "BTC", Kind: ExitIntent})
	if _, err := isolateRejections(snapshot, definitions, requests, 10, exit, prepare); err != exit || calls != 0 {
		t.Fatal("exit refusal must freeze", err, calls)
	}
	// Site-attributed refusals exclude exactly the named lot.
	_, definitions, requests = isolationUnit()
	var seen []int64
	prepare = func(r []InstrumentRebalance, records []StrategyCheckpoint) (PreparedRebalance, error) {
		for _, target := range r[0].Targets {
			seen = append(seen, target.SignedSteps)
		}
		return PreparedRebalance{}, nil
	}
	named := rejectedAt(RejectSpotShort, "x", rejectionScope{Strategy: "a", Lot: "lot-1", Instrument: "BTC"})
	if _, err := isolateRejections(snapshot, definitions, requests, 10, named, prepare); err != nil || !reflect.DeepEqual(seen, []int64{0, 5}) || !definitions[0].Order.Canceled || definitions[1].Order.Canceled {
		t.Fatal("attributed lot not excluded alone", err, seen)
	}
	// An entry the owner never admitted stays fail-closed.
	_, definitions, requests = isolationUnit()
	definitions[0].CommandRevision, definitions[1].CommandRevision = 0, 0
	if _, err := isolateRejections(snapshot, definitions, requests, 10, named, prepare); err != named {
		t.Fatal("unadmitted entry isolated", err)
	}
}

// The terminal entry survives a restart and is not resent.
func TestIsolationRestartKeepsEntryTerminal(t *testing.T) {
	f := newSequenceFixture(t, false)
	f.createStop(t, "a", "lot-a", 45, "105", 12)
	f.tick(t, "120", 14)
	f.requireIsolated(t, "a", "lot-a", RejectRiskLimit)
	opts := f.b.service.opts
	f.b.Release()
	if err := f.b.service.Close(); err != nil {
		t.Fatal(err)
	}
	registry := &AccountRegistry{}
	owner, err := registry.Acquire(testIntent(Buy).Account)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewSharedAccount(owner, opts)
	if err != nil {
		t.Fatal(err)
	}
	account := service.Borrow()
	t.Cleanup(func() { account.Release(); registry.Close(); service.Close() })
	if err := account.Reconcile("restart", 15); err != nil {
		t.Fatal(err)
	}
	if err := account.RegisterAccountQuotes([]Instrument{ledgerInstrument()}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		return VisibleQuote{Bid: intentPrice("120"), Ask: intentPrice("120"), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	for now := int64(16); now < 19; now++ {
		if err := account.AdvancePendingExecution(context.Background(), now); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := account.Snapshot(context.Background())
	if err != nil || len(snapshot.Lots) != 0 || snapshot.RiskFrozen {
		t.Fatal("excluded entry traded after restart", snapshot.Lots, err)
	}
	book, err := service.store.LoadPendingBook(context.Background())
	if err != nil || len(book) != 1 || !book[0].Order.Canceled || book[0].Order.Entry.State != Canceled {
		t.Fatal("terminal state lost", book, err)
	}
	body, err := service.store.StrategyCheckpoint(context.Background(), "a", "pending-isolated:lot-a")
	var record isolationRecord
	if err != nil || json.Unmarshal(body, &record) != nil || record.Code != RejectRiskLimit {
		t.Fatal("audit record lost", string(body), err)
	}
}

// Command admission has no isolation: the caller gets the rejection and
// nothing is persisted.
func TestIsolationNotAppliedAtCommandAdmission(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		command := PendingCommand{ID: "lot-a", Strategy: "a", Lot: "lot-a", Action: PendingCreate,
			Create: &PendingCreateSpec{Instrument: "BTC", Side: Buy, QuantitySteps: 600}}
		_, err := f.b.ApplyPendingCommand(context.Background(), command, 12)
		if code, ok := RejectionCode(err); !ok || code != RejectRiskLimit {
			t.Fatal("admission must return the rejection", err)
		}
		book, err := f.b.service.store.LoadPendingBook(context.Background())
		if err != nil || len(book) != 0 {
			t.Fatal("rejected admission left a pending lot", book, err)
		}
	})
}
