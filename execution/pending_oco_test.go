package execution

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func (f *sequenceFixture) createOCO(t *testing.T, strategy StrategyID, lot, group string, side OrderSide, limit, stop string, now int64) {
	t.Helper()
	spec := &PendingCreateSpec{Instrument: "BTC", Side: side, QuantitySteps: 10, OCOGroup: group}
	if limit != "" {
		spec.Conditions.Limit = intentPrice(limit)
	}
	if stop != "" {
		spec.Conditions.Stop = intentPrice(stop)
	}
	command := PendingCommand{ID: lot, Strategy: strategy, Lot: VirtualLotID(lot), Action: PendingCreate, Create: spec}
	if _, err := f.b.ApplyPendingCommand(context.Background(), command, now); err != nil {
		t.Fatal(err)
	}
}

func (f *sequenceFixture) requireOCOCancelled(t *testing.T, strategy StrategyID, lot string) {
	t.Helper()
	f.requireOCOCancelledWith(t, strategy, lot, OCOSimultaneousReason)
}

func (f *sequenceFixture) requireOCOCancelledWith(t *testing.T, strategy StrategyID, lot, reason string) {
	t.Helper()
	d := f.book(t, lot)
	if !d.Order.Canceled || d.Order.Entry.State != Canceled || d.Order.Desired != 0 {
		t.Fatalf("OCO loser is not terminal: %+v", d.Order)
	}
	body, err := f.b.service.store.StrategyCheckpoint(context.Background(), strategy, "pending-oco:"+lot)
	var record ocoRecord
	if err != nil || json.Unmarshal(body, &record) != nil || record.Reason != reason {
		t.Fatal("OCO audit record missing", string(body), err)
	}
	events, err := f.b.service.store.EventsAfter(context.Background(), 0, 10000)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		var accepted StrategyAcceptedEvent
		if event.Kind == "StrategyAccepted" && json.Unmarshal(event.Payload, &accepted) == nil && accepted.Lot == VirtualLotID(lot) && accepted.Rejected == reason {
			return
		}
	}
	t.Fatal("OCO audit event missing")
}

func (f *sequenceFixture) requireNoOCOAudit(t *testing.T, strategy StrategyID, lot string) {
	t.Helper()
	if _, err := f.b.service.store.StrategyCheckpoint(context.Background(), strategy, "pending-oco:"+lot); err == nil {
		t.Fatal("unexpected OCO audit for", lot)
	}
}

// Two members of one group cross in one tick: the earlier-created one fills
// (regardless of lot ID order), the other is cancelled in the same commit.
func TestOCOSimultaneousMembersOneFillsOtherCancelled(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		f.createOCO(t, "a", "lot-z", "g", Buy, "", "105", 12)
		f.createOCO(t, "a", "lot-a", "g", Buy, "", "110", 13)
		fills := f.venue.Metrics().Fills
		f.tick(t, "120", 14)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"a/lot-z": 10}) {
			t.Fatal("exactly the first-created member must fill", got)
		}
		if f.venue.Metrics().Fills != fills+1 {
			t.Fatal("expected a single real fill", f.venue.Metrics())
		}
		f.requireOCOCancelled(t, "a", "lot-a")
		f.requireNoOCOAudit(t, "a", "lot-z")
		f.tick(t, "125", 15)
		f.tick(t, "90", 16)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"a/lot-z": 10}) {
			t.Fatal("cancelled sibling came back", got)
		}
		requireNotFrozen(t, f)
	})
}

// A breakout bracket (buy-stop above, sell-stop below) gapping through the
// buy leg fills one leg.
func TestOCOBreakoutBracketGap(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		f.createOCO(t, "a", "up", "g", Buy, "", "105", 12)
		f.createOCO(t, "a", "down", "g", Sell, "", "95", 13)
		f.tick(t, "120", 14)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"a/up": 10}) {
			t.Fatal(got)
		}
		// The sell leg was not executable in that tick; the next owner tick
		// cancels it because "up" has a fill (see the filled-sibling tests).
		f.requireHeldEntry(t, "down")
	})
}

// Opposite-side members (a crossed bracket of limits) both executable at once.
func TestOCOOppositeSidesSimultaneous(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		*f.price = "105"
		f.createOCO(t, "a", "buy", "g", Buy, "100", "", 12)
		*f.price = "100"
		f.createOCO(t, "a", "sell", "g", Sell, "100", "", 13)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"a/buy": 10}) {
			t.Fatal(got)
		}
		f.requireOCOCancelled(t, "a", "sell")
	})
}

// A create that is executable at admission together with an already executable
// older sibling is resolved in the command's own commit.
func TestOCOResolvedAtCommandAdmission(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		f.createOCO(t, "a", "first", "g", Buy, "", "105", 12)
		*f.price = "120" // no tick: the first stop is crossed but not yet executed
		f.createOCO(t, "a", "second", "g", Buy, "", "110", 13)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"a/first": 10}) {
			t.Fatal(got)
		}
		f.requireOCOCancelled(t, "a", "second")
	})
}

func TestOCOUnrelatedMembersUnaffected(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		f.createOCO(t, "a", "g1-1", "g1", Buy, "", "105", 12)
		f.createOCO(t, "a", "g2-1", "g2", Buy, "", "105", 13) // other group
		f.createOCO(t, "b", "b-1", "g1", Buy, "", "105", 14)  // other strategy, same key
		f.createOCO(t, "a", "none", "", Buy, "", "105", 15)   // no group
		f.tick(t, "120", 16)
		want := map[string]int64{"a/g1-1": 10, "a/g2-1": 10, "b/b-1": 10, "a/none": 10}
		if got := f.lots(t); !reflect.DeepEqual(got, want) {
			t.Fatal("unrelated members must all fill", got)
		}
		requireNotFrozen(t, f)
	})
}

// Only one executable member in the group: nothing is cancelled and the
// resting member stays pending.
func TestOCOSingleExecutableMemberAsToday(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		f.createOCO(t, "a", "near", "g", Buy, "", "105", 12)
		f.createOCO(t, "a", "far", "g", Buy, "", "130", 13)
		f.tick(t, "120", 14)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"a/near": 10}) {
			t.Fatal(got)
		}
		f.requireHeldEntry(t, "far")
		f.requireNoOCOAudit(t, "a", "far")
	})
}

// A member that already has fills is never cancelled by the rule, even when
// it was created later than an unfilled executable sibling.
func TestOCOMemberWithFillNeverCancelled(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		f.createOCO(t, "a", "old", "g", Buy, "", "105", 12)
		f.createOCO(t, "a", "new", "g", Buy, "", "110", 13)
		*f.price = "120"
		err := f.b.use(func(s *SharedAccount) error {
			ctx := context.Background()
			book, err := s.store.LoadPendingBook(ctx)
			if err != nil {
				return err
			}
			for n := range book {
				if book[n].Order.Lot == "new" {
					book[n].Order.Entry.FilledSteps = 3 // partial fill history
				}
			}
			q, err := s.ReadQuote(ctx, "BTC", 14)
			if err != nil {
				return err
			}
			records, err := s.resolvePendingOCO(ctx, book, map[string]VisibleQuote{"BTC": q}, 14)
			if err != nil {
				return err
			}
			if len(records) != 1 || records[0].Name != "pending-oco:old" {
				t.Fatal("unfilled older member must lose to the member with fills", records)
			}
			for _, d := range book {
				if d.Order.Lot == "new" && (d.Order.Canceled || d.Order.Entry.State == Canceled) {
					t.Fatal("member with a fill was cancelled")
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestOCOAdmissionValidationAndCompatibility(t *testing.T) {
	f := newSequenceFixture(t, true)
	for _, bad := range []string{" g", "g ", strings.Repeat("x", maxOCOGroupLen+1), "a\nb"} {
		command := PendingCommand{ID: "bad", Strategy: "a", Lot: "bad", Action: PendingCreate,
			Create: &PendingCreateSpec{Instrument: "BTC", Side: Buy, QuantitySteps: 10, Conditions: IntentConditions{Stop: intentPrice("105")}, OCOGroup: bad}}
		if _, err := f.b.ApplyPendingCommand(context.Background(), command, 12); err == nil {
			t.Fatalf("invalid group %q admitted", bad)
		}
	}
	// A group-less command and book keep their serialized form.
	body, err := json.Marshal(PendingCreateSpec{Instrument: "BTC"})
	if err != nil || strings.Contains(string(body), "OCO") {
		t.Fatal("OCOGroup must be omitted when empty", string(body))
	}
	body, err = json.Marshal(PendingOrder{})
	if err != nil || strings.Contains(string(body), "OCO") {
		t.Fatal("OCO fields must be omitted when empty", string(body))
	}
	if validatePendingBook([]PendingDefinition{{Order: PendingOrder{OCOSeq: 1}}}) == nil {
		t.Fatal("sequence without a group accepted")
	}
}

// A cancelled loser stays cancelled across a restart and nothing re-trades.
func TestOCORestartKeepsLoserCancelled(t *testing.T) {
	f := newSequenceFixture(t, false)
	f.createOCO(t, "a", "w", "g", Buy, "", "105", 12)
	f.createOCO(t, "a", "l", "g", Buy, "", "110", 13)
	f.tick(t, "120", 14)
	f.requireOCOCancelled(t, "a", "l")
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
	if err != nil || snapshot.RiskFrozen || len(snapshot.Lots) != 1 || snapshot.Lots[0].ID != "w" {
		t.Fatal("loser traded or account frozen after restart", snapshot.Lots, err)
	}
	book, err := service.store.LoadPendingBook(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range book {
		if d.Order.Lot == "l" && (!d.Order.Canceled || d.Order.Entry.State != Canceled || d.Order.OCOGroup != "g") {
			t.Fatal("terminal state lost", d.Order)
		}
	}
}

// A filled member cancels its unfilled sibling on the next owner tick, with no
// robot cancel_entry; the sibling can never fill afterwards.
func TestOCOFilledMemberCancelsSiblingWithoutRobot(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		f.createOCO(t, "a", "up", "g", Buy, "", "105", 12)
		f.createOCO(t, "a", "down", "g", Sell, "", "95", 13)
		f.tick(t, "120", 14)
		f.requireHeldEntry(t, "down")
		f.tick(t, "121", 15)
		f.requireOCOCancelledWith(t, "a", "down", OCOFilledSiblingReason)
		f.tick(t, "90", 16)
		f.tick(t, "90", 17)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"a/up": 10}) {
			t.Fatal("sibling filled after the owner cancelled it", got)
		}
		requireNotFrozen(t, f)
	})
}

// Members that both have fills are both kept.
func TestOCOBothFilledKeepsBoth(t *testing.T) {
	f := newSequenceFixture(t, true)
	f.createOCO(t, "a", "x", "g", Buy, "", "105", 12)
	f.createOCO(t, "a", "y", "g", Buy, "", "130", 13)
	*f.price = "120"
	err := f.b.use(func(s *SharedAccount) error {
		ctx := context.Background()
		book, err := s.store.LoadPendingBook(ctx)
		if err != nil {
			return err
		}
		for n := range book {
			book[n].Order.Entry.FilledSteps = 2
		}
		q, err := s.ReadQuote(ctx, "BTC", 14)
		if err != nil {
			return err
		}
		records, err := s.resolvePendingOCO(ctx, book, map[string]VisibleQuote{"BTC": q}, 14)
		if err != nil || len(records) != 0 {
			t.Fatal("members with fills must both be kept", records, err)
		}
		for _, d := range book {
			if d.Order.Canceled {
				t.Fatal("filled member cancelled")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Resolution is idempotent: a second tick adds no audit and a restart keeps it.
func TestOCOFilledSiblingIdempotent(t *testing.T) {
	f := newSequenceFixture(t, false)
	f.createOCO(t, "a", "up", "g", Buy, "", "105", 12)
	f.createOCO(t, "a", "down", "g", Sell, "", "95", 13)
	f.tick(t, "120", 14)
	f.tick(t, "120", 15)
	before := f.book(t, "down")
	f.tick(t, "120", 16)
	f.requireOCOCancelledWith(t, "a", "down", OCOFilledSiblingReason)
	body, _ := f.b.service.store.StrategyCheckpoint(context.Background(), "a", "pending-oco:down")
	var rec ocoRecord
	if json.Unmarshal(body, &rec) != nil || rec.AtMS != 15 || !before.Order.Canceled {
		t.Fatal("cancellation must be recorded once", rec, before.Order.Canceled)
	}
}
