package execution

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/shopspring/decimal"
)

func cashOption(right string) Instrument {
	i := ledgerInstrument()
	i.ID = "OPT"
	i.Valuation = "cash_european_option"
	i.QuantityStep = decimal.NewFromInt(1)
	i.Option = &EuropeanOption{UnderlyingID: "BTC", Right: right, Strike: intentPrice("100"), ExpiryMS: 1000, UnderlyingStressMax: intentPrice("200")}
	return i
}

func optionConservation(t *testing.T, s *Store, marks map[string]decimal.Decimal) AccountSnapshot {
	t.Helper()
	snapshot, err := s.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	real, err := snapshot.Equity(marks)
	if err != nil {
		t.Fatal(err)
	}
	virtual := snapshot.UnassignedCash
	cash := virtual
	for _, n := range snapshot.SyntheticStrategyCash {
		virtual = virtual.Add(n)
		cash = cash.Add(n)
	}
	for _, lot := range append(append([]VirtualLot(nil), snapshot.Lots...), snapshot.ExternalPositions...) {
		virtual = virtual.Add(lot.EquityValue(marks[lot.Instrument.ID]))
	}
	if !real.Equal(virtual) || !cash.Sub(snapshot.AccountSettledCash).Equal(snapshot.PnLReclassification) {
		t.Fatalf("NAV/cash conservation failed: real=%s virtual=%s snapshot=%+v", real, virtual, snapshot)
	}
	return snapshot
}

func optionOrder(t *testing.T, s *Store, h *AccountHandle, i Instrument, seq int64, side OrderSide, n int64, price string) OrderIntent {
	t.Helper()
	id := fmt.Sprintf("option-%d", seq)
	at := seq * 10
	intent := EligibleIntent{ID: VirtualIntentID(id), Account: s.key, Strategy: "a", Lot: "oa", Instrument: i.ID, Kind: EntryIntent, Side: side, QuantitySteps: n, State: Eligible, Conditions: IntentConditions{CreatedBar: at}}
	p := Plan{ID: "p" + id, Sequence: seq, DecisionMS: at, ExpiresMS: 2000, Intents: []EligibleIntent{intent}, Instruments: []Instrument{i}}
	o := OrderIntent{ID: id, PlanID: p.ID, Instrument: i, Side: side, Steps: n, Observation: ExecutionObservation{Price: intentPrice(price), AtMS: at, ValidUntilMS: 2000, Bar: at}, Allocations: []FillAllocation{{ID: id, IntentID: intent.ID, Strategy: "a", Lot: "oa", Side: side, Kind: EntryIntent, Steps: n}}}
	if err := s.SavePlan(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if err := s.PrepareOrder(context.Background(), o, at); err != nil {
		t.Fatal(err)
	}
	if err := executorFor(s, h, &fakeExecutionAdapter{}).Send(o.ID, at); err != nil {
		t.Fatal(err)
	}
	return o
}

func TestOptionPremiumPartialReversalAndPaperParity(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			fundStrategies(t, s)
			ctx := context.Background()
			i := cashOption("call")
			paper, _ := NewPaperAdapter(intentPrice("1000"), decimal.Zero, decimal.Zero)
			trades := []struct {
				side        OrderSide
				n           int64
				price, cash string
				remaining   int64
			}{{Buy, 2, "5", "990", 2}, {Sell, 1, "7", "997", 1}, {Sell, 3, "4", "1009", -2}, {Buy, 2, "3", "1003", 0}}
			for k, tr := range trades {
				o := optionOrder(t, s, h, i, int64(k+1), tr.side, tr.n, tr.price)
				// First order proves incremental partial premium cash, not just a full fill.
				for j := int64(0); j < tr.n; j++ {
					if _, err := s.ApplyFill(ctx, FillReport{EventID: fmt.Sprintf("fill-%d-%d", k, j), OrderID: o.ID, Steps: 1, Price: intentPrice(tr.price), AtMS: int64((k + 1) * 10)}); err != nil {
						t.Fatal(err)
					}
				}
				o.SubmitAtMS = int64((k + 1) * 10)
				if _, err := paper.Submit(ctx, o, o.ID); err != nil {
					t.Fatal(err)
				}
				snap := optionConservation(t, s, map[string]decimal.Decimal{i.ID: intentPrice("6")})
				venue, _ := paper.Snapshot(ctx)
				if snap.AccountSettledCash.String() != tr.cash || venue.Cash != tr.cash || venue.Positions[i.ID] != tr.remaining {
					t.Fatalf("trade%d ledger=%s paper=%+v", k, snap.AccountSettledCash, venue)
				}
			}
			lots, err := s.Lot(ctx, "a", "oa")
			if err != nil || !lots.RealizedPnL.Equal(intentPrice("3")) || !lots.CostBasis.IsZero() {
				t.Fatal(lots, err)
			}
		})
	}
}

func TestOptionInternalCrossExpiryAndDedup(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			s, _, h, path := testStore(t)
			fundStrategies(t, s)
			i := cashOption("call")
			ctx := context.Background()
			paper, _ := NewPaperAdapter(intentPrice("1000"), decimal.Zero, decimal.Zero)
			e := &OwnerExecutor{Handle: h, Token: h.Token(), Store: s, Adapter: paper}
			r := domainRequest("options", 1, 10, "5", 3, -2)
			r.Requests[0].Instrument = i
			r.Risk.Marks = map[string]decimal.Decimal{i.ID: intentPrice("5")}
			p, err := e.PrepareRebalance(r)
			if err != nil {
				t.Fatal(err)
			}
			if len(p.InternalMatchIDs) != 1 || len(p.OrderIDs) != 1 {
				t.Fatal(p)
			}
			before := optionConservation(t, s, r.Risk.Marks)
			if before.SyntheticStrategyCash["a"].String() != "490" || before.SyntheticStrategyCash["b"].String() != "510" || before.AccountSettledCash.String() != "1000" {
				t.Fatal(before)
			}
			if err := e.Send(p.OrderIDs[0], 11); err != nil {
				t.Fatal(err)
			}
			snap := optionConservation(t, s, r.Risk.Marks)
			if snap.AccountSettledCash.String() != "995" || snap.SyntheticStrategyCash["a"].String() != "485" {
				t.Fatal(snap)
			}
			// Also carry a real perpetual; option settlement must leave it untouched.
			if _, err := s.ApplyExternalPosition(ctx, ExternalPositionEvent{ID: "manual-perp", Kind: ExternalCashChange, Instrument: ledgerInstrument(), Side: Buy, Steps: 1, Price: intentPrice("100"), AtMS: 12}); err != nil {
				t.Fatal(err)
			}
			event := OptionExpiry{Instrument: i, Fixing: intentPrice("110"), FixingAtMS: 1000, PublishedMS: 1002, AtMS: 1005}
			if ok, err := s.ApplyOptionExpiry(ctx, event); err != nil || !ok {
				t.Fatal(ok, err)
			}
			if _, err := paper.ApplyOptionExpiry(ctx, event); err != nil {
				t.Fatal(err)
			}
			snap = optionConservation(t, s, map[string]decimal.Decimal{"BTC": intentPrice("110")})
			if snap.AccountSettledCash.String() != "1005" || snap.SyntheticStrategyCash["a"].String() != "515" || snap.SyntheticStrategyCash["b"].String() != "490" {
				t.Fatal(snap)
			}
			venue, _ := paper.Snapshot(ctx)
			if venue.Cash != "1005" || venue.Positions[i.ID] != 0 {
				t.Fatal(venue)
			}
			if backend == "SQLite" {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s, err = OpenStore(path, s.key)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
			}
			event.AtMS++
			if ok, err := s.ApplyOptionExpiry(ctx, event); err != nil || ok {
				t.Fatal("retry", ok, err)
			}
			if done, err := s.OptionSettled(ctx, i); err != nil || !done {
				t.Fatal(done, err)
			}
			event.Fixing = intentPrice("111")
			if _, err := s.ApplyOptionExpiry(ctx, event); err == nil {
				t.Fatal("conflicting fixing accepted")
			}
		})
	}
}

func TestOptionExpiryZeroPayoffAndExternalShort(t *testing.T) {
	for _, right := range []string{"call", "put"} {
		for _, side := range []OrderSide{Buy, Sell} {
			t.Run(string(side)+right, func(t *testing.T) {
				s, _, h, _ := testStore(t)
				fundStrategies(t, s)
				ctx := context.Background()
				i := cashOption(right)
				o := optionOrder(t, s, h, i, 1, side, 2, "5")
				if _, err := s.ApplyFill(ctx, FillReport{EventID: "fill", OrderID: o.ID, Steps: 2, Price: intentPrice("5"), AtMS: 10}); err != nil {
					t.Fatal(err)
				}
				if _, err := s.ApplyExternalPosition(ctx, ExternalPositionEvent{ID: "manual-option", Kind: ExternalCashChange, Instrument: i, Side: Sell, Steps: 1, Price: intentPrice("5"), AtMS: 11}); err != nil {
					t.Fatal(err)
				}
				event := OptionExpiry{Instrument: i, Fixing: intentPrice("100"), FixingAtMS: 1000, PublishedMS: 1000, AtMS: 1000}
				if ok, err := s.ApplyOptionExpiry(ctx, event); err != nil || !ok {
					t.Fatal(ok, err)
				}
				snap := optionConservation(t, s, nil)
				want := "995"
				if side == Sell {
					want = "1015"
				}
				if snap.AccountSettledCash.String() != want || len(snap.Lots) != 0 || len(snap.ExternalPositions) != 1 || snap.ExternalPositions[0].SignedSteps != 0 {
					t.Fatal(snap)
				}
			})
		}
	}
}

func TestOptionExpiryVisibilityOrderGuardsAndStress(t *testing.T) {
	s, _, h, _ := testStore(t)
	fundStrategies(t, s)
	ctx := context.Background()
	i := cashOption("call")
	o := optionOrder(t, s, h, i, 1, Sell, 1, "0.01")
	event := OptionExpiry{Instrument: i, Fixing: intentPrice("110"), FixingAtMS: 1000, PublishedMS: 1001, AtMS: 1000}
	if _, err := s.ApplyOptionExpiry(ctx, event); err == nil {
		t.Fatal("unpublished fixing accepted")
	}
	event.AtMS = 1002
	if _, err := s.ApplyOptionExpiry(ctx, event); err == nil {
		t.Fatal("pending order accepted")
	}
	if _, err := s.ApplyFill(ctx, FillReport{EventID: "late", OrderID: o.ID, Steps: 1, Price: intentPrice("0.01"), AtMS: 1000}); err == nil {
		t.Fatal("postexpiry fill accepted")
	}
	if err := s.PrepareOrder(ctx, o, 1000); err == nil {
		t.Fatal("postexpiry prepare accepted")
	}
	if _, err := s.ApplyFill(ctx, FillReport{EventID: "valid", OrderID: o.ID, Steps: 1, Price: intentPrice("0.01"), AtMS: 10}); err != nil {
		t.Fatal(err)
	}
	if i.Collateral(-1, intentPrice("0.01"), intentPrice("0.01")).String() != "100" {
		t.Fatal("short liability depends on premium")
	}
	risk := domainRisk()
	risk.MarginRate = intentPrice("0.01")
	risk.MaxAccountMargin = intentPrice("50")
	risk.Marks = map[string]decimal.Decimal{i.ID: intentPrice("0.01")}
	snap := optionConservation(t, s, risk.Marks)
	if _, err := Coordinate(snap, []ExecutableTarget{{Strategy: "a", Lot: "oa", SignedSteps: -2, Instrument: i.ID}}, i, ExecutionObservation{Price: intentPrice("0.01"), AtMS: 20, ValidUntilMS: 30}, risk); err == nil {
		t.Fatal("premium-based short margin admitted")
	}
	event.Fixing = intentPrice("201")
	if _, err := s.ApplyOptionExpiry(ctx, event); err == nil {
		t.Fatal("outside stress scenario accepted")
	}
}

func TestOptionSharedExpiryRetiresTargetsAndPreservesPaperCash(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			borrow, paper := strategyService(t, backend != "SQLite")
			ctx := context.Background()
			i := cashOption("put")
			if err := borrow.RegisterAccountQuotes([]Instrument{i}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
				return VisibleQuote{Bid: intentPrice("5"), Ask: intentPrice("5"), AtMS: now, ReceivedMS: now, ValidUntilMS: 4000, Bar: now}, nil
			}, nil); err != nil {
				t.Fatal(err)
			}
			if err := borrow.RegisterAccountQuotes([]Instrument{ledgerInstrument()}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
				return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: 4000, Bar: now}, nil
			}, nil); err != nil {
				t.Fatal(err)
			}
			request := strategyRequest("a", "open-option", 10, StrategyTargetsFull, ExecutableTarget{Lot: "put", SignedSteps: 2})
			request.Requests[0].Instrument = i
			request.Requests[0].Quote.Bid = intentPrice("5")
			request.Requests[0].Quote.Ask = intentPrice("5")
			if err := borrow.RebalanceStrategy(request, 11); err != nil {
				t.Fatal(err)
			}
			event := OptionExpiry{Instrument: i, Fixing: intentPrice("90"), FixingAtMS: 1000, PublishedMS: 1002, AtMS: 1005}
			if applied, err := borrow.ApplyOptionExpiry(event); err != nil || !applied {
				t.Fatal(applied, err)
			}
			if applied, err := borrow.ApplyOptionExpiry(event); err != nil || applied {
				t.Fatal(applied, err)
			}
			snap, err := borrow.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			venue, _ := paper.Snapshot(ctx)
			if snap.AccountSettledCash.String() != "1010" || venue.Cash != "1010" {
				t.Fatal(snap, venue)
			}
			borrow.service.markets["BTC"] = accountMarket{instrument: ledgerInstrument(), quote: func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
				return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: 4000, Bar: now}, nil
			}}
			request = strategyRequest("b", "after-option", 1010, StrategyTargetsFull, ExecutableTarget{Lot: "perp", SignedSteps: 1})
			request.ExpiresMS = 2000
			request.Requests[0].Quote.ValidUntilMS = 2000
			if err := borrow.RebalanceStrategy(request, 1011); err != nil {
				t.Fatal("settled inherited target blocked later instrument", err)
			}
			latest, err := borrow.LatestPlan(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, target := range latest.Targets {
				if target.Instrument == i.ID {
					t.Fatal("option target resurrected", latest)
				}
			}
			if err := borrow.Reconcile("after-expiry", 1012); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOptionRejectsBackdatedFillAfterSettlementAndLiveAdapter(t *testing.T) {
	s, _, h, _ := testStore(t)
	fundStrategies(t, s)
	ctx := context.Background()
	i := cashOption("call")
	o := optionOrder(t, s, h, i, 1, Buy, 2, "5")
	if _, err := s.ApplyFill(ctx, FillReport{EventID: "partial", OrderID: o.ID, Steps: 1, Price: intentPrice("5"), AtMS: 10}); err != nil {
		t.Fatal(err)
	}
	event := OptionExpiry{Instrument: i, Fixing: intentPrice("110"), FixingAtMS: 1000, PublishedMS: 1000, AtMS: 1000}
	for _, state := range []RealOrderState{OrderSending, OrderUnknown, OrderCancelPending} {
		if err := s.commit(ctx, func(tx *storeTxn) error { return s.setOrderState(tx, o.ID, state) }); err != nil {
			t.Fatal(err)
		}
		if applied, err := s.ApplyOptionExpiry(ctx, event); err == nil || applied {
			t.Fatal("uncertain order settled", state, applied, err)
		}
	}
	if err := s.commit(ctx, func(tx *storeTxn) error { return s.setOrderState(tx, o.ID, OrderCanceled) }); err != nil {
		t.Fatal(err)
	}
	if applied, err := s.ApplyOptionExpiry(ctx, event); err != nil || !applied {
		t.Fatal(applied, err)
	}
	before, _ := s.Snapshot(ctx)
	if _, err := s.ApplyFill(ctx, FillReport{EventID: "late-backdated", OrderID: o.ID, Steps: 1, Price: intentPrice("5"), AtMS: 999}); err == nil {
		t.Fatal("backdated fill resurrected option")
	}
	after, _ := s.Snapshot(ctx)
	if after.Checkpoint != before.Checkpoint || !after.AccountSettledCash.Equal(before.AccountSettledCash) {
		t.Fatal("rejected fill changed ledger")
	}
	if _, err := NewBanexgAdapter(ctx, nil, BanexgAdapterConfig{Instruments: []BanexgInstrument{{Instrument: i}}}); err == nil || !strings.Contains(err.Error(), "research/paper") {
		t.Fatal("live adapter option boundary", err)
	}
	i.Option = nil
	if err := i.Validate(); err == nil {
		t.Fatal("option descriptor absent")
	}
}

func TestOptionContractUnitsFeesAndShortPutExpiry(t *testing.T) {
	s, _, h, _ := testStore(t)
	fundStrategies(t, s)
	ctx := context.Background()
	i := cashOption("put")
	i.ContractSize = intentPrice("10")
	o := optionOrder(t, s, h, i, 1, Sell, 1, "5")
	if _, err := s.ApplyFill(ctx, FillReport{EventID: "put-fill", OrderID: o.ID, Steps: 1, Price: intentPrice("5"), Fee: intentPrice("0.5"), AtMS: 10}); err != nil {
		t.Fatal(err)
	}
	paper, _ := NewPaperAdapter(intentPrice("1000"), intentPrice("0.01"), decimal.Zero)
	o.SubmitAtMS = 10
	if _, err := paper.Submit(ctx, o, o.ID); err != nil {
		t.Fatal(err)
	}
	snap := optionConservation(t, s, map[string]decimal.Decimal{i.ID: intentPrice("5")})
	equity, err := snap.Equity(map[string]decimal.Decimal{i.ID: intentPrice("5")})
	if err != nil || equity.String() != "999.5" || snap.AccountSettledCash.String() != "1049.5" {
		t.Fatal(snap, equity, err)
	}
	event := OptionExpiry{Instrument: i, Fixing: intentPrice("90"), FixingAtMS: 1000, PublishedMS: 1000, AtMS: 1000}
	if _, err := s.ApplyOptionExpiry(ctx, event); err != nil {
		t.Fatal(err)
	}
	if _, err := paper.ApplyOptionExpiry(ctx, event); err != nil {
		t.Fatal(err)
	}
	snap = optionConservation(t, s, nil)
	venue, _ := paper.Snapshot(ctx)
	if snap.AccountSettledCash.String() != "949.5" || venue.Cash != "949.5" {
		t.Fatal(snap, venue)
	}
	lot, err := s.Lot(ctx, "a", "oa")
	if err != nil || lot.RealizedPnL.String() != "-50" || lot.Fees.String() != "0.5" {
		t.Fatal(lot, err)
	}
}
