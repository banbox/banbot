package execution

import (
	"context"
	"reflect"
	"strconv"
	"testing"
)

type sequenceFixture struct {
	b     *SharedAccountBorrow
	venue *PaperAdapter
	price *string
}

func newSequenceFixture(t *testing.T, memory bool) *sequenceFixture {
	t.Helper()
	price := "100"
	b, venue := strategyService(t, memory)
	market := b.service.markets["BTC"]
	market.quote = func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		return VisibleQuote{Bid: intentPrice(price), Ask: intentPrice(price), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
	}
	b.service.markets["BTC"] = market
	return &sequenceFixture{b: b, venue: venue, price: &price}
}

func (f *sequenceFixture) create(t *testing.T, strategy StrategyID, lot string, side OrderSide, limit, stop string, now int64) {
	t.Helper()
	spec := &PendingCreateSpec{Instrument: "BTC", Side: side, QuantitySteps: 10}
	if limit != "" {
		spec.Conditions.Limit = intentPrice(limit)
	}
	if stop != "" {
		spec.Protections = []PositionProtection{{Kind: StopLossProtection, Level: intentPrice(stop)}}
	}
	command := PendingCommand{ID: lot, Strategy: strategy, Lot: VirtualLotID(lot), Action: PendingCreate, Create: spec}
	if _, err := f.b.ApplyPendingCommand(context.Background(), command, now); err != nil {
		t.Fatal(err)
	}
}

func (f *sequenceFixture) lots(t *testing.T) map[string]int64 {
	t.Helper()
	snapshot, err := f.b.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]int64)
	for _, lot := range snapshot.Lots {
		out[string(lot.Strategy)+"/"+string(lot.ID)] = lot.SignedSteps
	}
	return out
}

func (f *sequenceFixture) tick(t *testing.T, price string, now int64) {
	t.Helper()
	*f.price = price
	if err := f.b.AdvancePendingExecution(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.b.Snapshot(context.Background())
	if err != nil || snapshot.RiskFrozen {
		t.Fatal("account frozen or unreadable after tick", err)
	}
	assertNAVConservation(t, snapshot, price)
}

// planIntents describes the virtual intents of the latest plan as
// "strategy/lot/kind/side/steps".
func (f *sequenceFixture) planIntents(t *testing.T) []string {
	t.Helper()
	plan, err := f.b.service.store.LatestPlan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, intent := range plan.Intents {
		out = append(out, string(intent.Strategy)+"/"+string(intent.Lot)+"/"+string(intent.Kind)+"/"+string(intent.Side)+"/"+strconv.FormatInt(intent.QuantitySteps, 10))
	}
	return out
}

func (f *sequenceFixture) requireHeldEntry(t *testing.T, lot string) {
	t.Helper()
	book, err := f.b.service.store.LoadPendingBook(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range book {
		if string(d.Order.Lot) != lot {
			continue
		}
		e := d.Order.Entry
		if e.State == Filled || e.State == Canceled || e.State == Expired || e.FilledSteps != 0 || e.ReservedSteps != 0 || d.Order.Canceled {
			t.Fatalf("held entry lost its pending state: %+v canceled=%v", e, d.Order.Canceled)
		}
		return
	}
	t.Fatal("held entry missing from the durable pending book")
}

func eachStore(t *testing.T, run func(t *testing.T, memory bool)) {
	for _, memory := range []bool{true, false} {
		t.Run(map[bool]string{true: "memory", false: "sqlite"}[memory], func(t *testing.T) { run(t, memory) })
	}
}

func TestSelfCrossSequencedLongStopAndBuyLimit(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		f.create(t, "a", "lot-1", Buy, "", "95", 12)
		f.create(t, "a", "lot-2", Buy, "90", "", 13)
		fills := f.venue.Metrics().Fills
		f.tick(t, "89", 14)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{}) {
			t.Fatal("stop did not reduce lot-1 alone", got)
		}
		if got := f.planIntents(t); !reflect.DeepEqual(got, []string{"a/lot-1/exit/sell/10"}) {
			t.Fatal("tick must carry the reduction only", got)
		}
		if m := f.venue.Metrics(); m.Fills != fills+1 || m.LastFill.Steps != 10 {
			t.Fatal("expected exactly one real sell fill of 10", m)
		}
		f.requireHeldEntry(t, "lot-2")
		// The entry fills on the next tick; later ticks do not double count it.
		f.tick(t, "89", 15)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"a/lot-2": 10}) {
			t.Fatal("held entry did not fill on the next tick", got)
		}
		if got := f.planIntents(t); !reflect.DeepEqual(got, []string{"a/lot-2/entry/buy/10"}) {
			t.Fatal("next tick must carry the entry only", got)
		}
		f.tick(t, "89", 16)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"a/lot-2": 10}) {
			t.Fatal("entry double counted", got)
		}
		if f.venue.Metrics().Fills != fills+2 {
			t.Fatal("unexpected extra fills", f.venue.Metrics())
		}
	})
}

func TestSelfCrossSequencedEntryStaysPendingWhenPriceMovesAway(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		f.create(t, "a", "lot-1", Buy, "", "95", 12)
		f.create(t, "a", "lot-2", Buy, "90", "", 13)
		f.tick(t, "89", 14)
		fills := f.venue.Metrics().Fills
		f.tick(t, "96", 15)
		if got := f.lots(t); len(got) != 0 || f.venue.Metrics().Fills != fills {
			t.Fatal("entry executed away from its limit", got)
		}
		f.requireHeldEntry(t, "lot-2")
		f.tick(t, "90", 16)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"a/lot-2": 10}) {
			t.Fatal("entry did not fill once executable again", got)
		}
	})
}

func TestSelfCrossSequencedShortMirror(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		f.create(t, "a", "lot-1", Sell, "", "105", 12)
		f.create(t, "a", "lot-2", Sell, "110", "", 13)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"a/lot-1": -10}) {
			t.Fatal("short lot-1 did not fill", got)
		}
		f.tick(t, "111", 14)
		if got := f.lots(t); len(got) != 0 {
			t.Fatal("stop did not cover lot-1 alone", got)
		}
		if got := f.planIntents(t); !reflect.DeepEqual(got, []string{"a/lot-1/exit/buy/10"}) {
			t.Fatal("tick must carry the cover only", got)
		}
		f.requireHeldEntry(t, "lot-2")
		f.tick(t, "111", 15)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"a/lot-2": -10}) {
			t.Fatal("held short entry did not fill on the next tick", got)
		}
	})
}

// Two strategies keep crossing internally with no real order for the crossed
// quantity: the stop of a and the limit of b net to zero at the venue.
func TestSelfCrossTwoStrategiesStillCrossInternally(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		f.create(t, "a", "lot-a", Buy, "", "95", 12)
		f.create(t, "b", "lot-b", Buy, "90", "", 13)
		fills := f.venue.Metrics().Fills
		f.tick(t, "89", 14)
		if got := f.lots(t); !reflect.DeepEqual(got, map[string]int64{"b/lot-b": 10}) {
			t.Fatal("internal cross allocation changed", got)
		}
		if f.venue.Metrics().Fills != fills {
			t.Fatal("internally crossed quantity reached the venue", f.venue.Metrics())
		}
		if got := f.planIntents(t); !reflect.DeepEqual(got, []string{"a/lot-a/exit/sell/10", "b/lot-b/entry/buy/10"}) {
			t.Fatal("two-strategy plan changed", got)
		}
		snapshot, _ := f.b.Snapshot(context.Background())
		if len(snapshot.ActualPositions) != 1 || snapshot.ActualPositions[0].SignedSteps != 10 {
			t.Fatal("net venue position must stay 10", snapshot.ActualPositions)
		}
	})
}

// Two lots of one strategy that exit on the same side are one real order.
func TestSelfCrossSameSideSingleStrategyUnaffected(t *testing.T) {
	eachStore(t, func(t *testing.T, memory bool) {
		f := newSequenceFixture(t, memory)
		f.create(t, "a", "lot-1", Buy, "", "95", 12)
		f.create(t, "a", "lot-2", Buy, "", "95", 13)
		fills := f.venue.Metrics().Fills
		f.tick(t, "89", 14)
		if got := f.lots(t); len(got) != 0 {
			t.Fatal("both stops must close", got)
		}
		if m := f.venue.Metrics(); m.Fills != fills+1 || m.LastFill.Steps != 20 {
			t.Fatal("same-side exits must be one real order of 20", m)
		}
	})
}

func TestSequenceOppositeLotsRule(t *testing.T) {
	instrument := ledgerInstrument()
	cases := []struct {
		name         string
		held, target []int64
		want         []int64
		n            int
	}{
		{"reduction kept, opposite entry held", []int64{10, 0}, []int64{0, 10}, []int64{0, 0}, 1},
		{"short cover kept, short entry held", []int64{-10, 0}, []int64{0, -10}, []int64{0, 0}, 1},
		{"reduction and same-side entry untouched", []int64{10, 0}, []int64{0, -5}, []int64{0, -5}, 0},
		{"two entries keep the lowest lot", []int64{0, 0}, []int64{7, -7}, []int64{7, 0}, 1},
		{"two entries, lowest lot sells", []int64{0, 0}, []int64{-7, 7}, []int64{-7, 0}, 1},
		{"opposite reductions are left alone", []int64{10, -10}, []int64{0, 0}, []int64{0, 0}, 0},
		{"flip counts as a reduction", []int64{10, 0}, []int64{-5, 4}, []int64{-5, 0}, 1},
		{"unchanged lots are ignored", []int64{10, 5}, []int64{10, 5}, []int64{10, 5}, 0},
		{"growth of a held long is an increase", []int64{10, 10}, []int64{0, 15}, []int64{0, 10}, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var snapshot AccountSnapshot
			var targets []ExecutableTarget
			for n := range c.target {
				lot := VirtualLotID("lot-" + strconv.Itoa(n+1))
				if c.held[n] != 0 {
					snapshot.Lots = append(snapshot.Lots, VirtualLot{Strategy: "a", ID: lot, Instrument: instrument, SignedSteps: c.held[n]})
				}
				targets = append(targets, ExecutableTarget{Strategy: "a", Lot: lot, SignedSteps: c.target[n]})
			}
			entry := EligibleIntent{Strategy: "a", Lot: "lot-2", Kind: EntryIntent}
			request := InstrumentRebalance{Instrument: instrument, Targets: targets, IntentConstraints: []EligibleIntent{entry}}
			out, n := sequenceOppositeLots(snapshot, []InstrumentRebalance{request})
			var got []int64
			for _, target := range out[0].Targets {
				got = append(got, target.SignedSteps)
			}
			if n != c.n || !reflect.DeepEqual(got, c.want) {
				t.Fatalf("held %d targets %v, want %d %v", n, got, c.n, c.want)
			}
			if (c.n == 1) != (len(out[0].IntentConstraints) == 0) {
				t.Fatal("held lot kept its entry constraint", out[0].IntentConstraints)
			}
			if len(out[0].Targets) != len(targets) || targets[0].SignedSteps != c.target[0] || targets[1].SignedSteps != c.target[1] {
				t.Fatal("input mutated or lot removed")
			}
		})
	}
}
