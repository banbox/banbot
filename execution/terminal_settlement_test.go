package execution

import (
	"context"
	"errors"
	"github.com/shopspring/decimal"
	"testing"
)

func TestTerminalSettlementAccountCashQuantitiesAndDedup(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		for _, value := range []string{"0", "60"} {
			for _, short := range []bool{false, true} {
				t.Run(backend+value+map[bool]string{false: "long", true: "short"}[short], func(t *testing.T) {
					s, _, h, _ := testStore(t)
					fundStrategies(t, s)
					ctx := context.Background()
					paper, _ := NewPaperAdapter(intentPrice("1000"), intentPrice("0.001"), decimal.Zero)
					e := &OwnerExecutor{Handle: h, Token: h.Token(), Store: s, Adapter: paper}
					a, b := int64(3), int64(-2)
					if short {
						a, b = 2, -3
					}
					r := domainRequest("terminal-open", 1, 10, "100", a, b)
					plan, err := e.PrepareRebalance(r)
					if err != nil {
						t.Fatal(err)
					}
					for _, id := range plan.OrderIDs {
						if err := e.Send(id, 11); err != nil {
							t.Fatal(err)
						}
					}
					other := ledgerInstrument()
					other.ID = "OTHER"
					if _, err := s.ApplyExternalPosition(ctx, ExternalPositionEvent{ID: "other", Kind: ExternalCashChange, Instrument: other, Side: Buy, Steps: 2, Price: intentPrice("50"), AtMS: 12}); err != nil {
						t.Fatal(err)
					}
					event := TerminalSettlement{Source: "terminal", SourceVersion: "v1", Instrument: ledgerInstrument(), CashValue: intentPrice(value), EventMS: 20, PublishedMS: 21, ReceivedMS: 25, AtMS: 25}
					if applied, err := s.ApplyTerminalSettlement(ctx, event); err != nil || !applied {
						t.Fatal(applied, err)
					}
					if applied, err := paper.ApplyTerminalSettlement(ctx, event); err != nil || !applied {
						t.Fatal(applied, err)
					}
					snap, err := s.Snapshot(ctx)
					if err != nil {
						t.Fatal(err)
					}
					net := a + b
					expected := intentPrice("1000").Sub(event.Instrument.Notional(absSteps(net), intentPrice("100")).Mul(intentPrice("0.001"))).Add(event.Instrument.Notional(net, intentPrice(value).Sub(intentPrice("100"))))
					venue, _ := paper.Snapshot(ctx)
					if !snap.AccountSettledCash.Equal(expected) || venue.Cash != expected.String() || venue.Positions[event.Instrument.ID] != 0 {
						t.Fatal(expected, snap.AccountSettledCash, venue)
					}
					sum := snap.UnassignedCash
					for _, cash := range snap.SyntheticStrategyCash {
						sum = sum.Add(cash)
					}
					if !sum.Sub(snap.PnLReclassification).Equal(snap.AccountSettledCash) {
						t.Fatal("cash reconciliation", snap)
					}
					for _, lot := range snap.Lots {
						if lot.Instrument.ID == event.Instrument.ID {
							t.Fatal("terminal lot remained", lot)
						}
					}
					if len(snap.ExternalPositions) != 1 || snap.ExternalPositions[0].Instrument.ID != "OTHER" || snap.ExternalPositions[0].SignedSteps != 2 {
						t.Fatal("other position changed", snap.ExternalPositions)
					}
					event.ReceivedMS = 26
					event.AtMS = 27
					if applied, err := s.ApplyTerminalSettlement(ctx, event); err != nil || applied {
						t.Fatal("retry", applied, err)
					}
					if applied, err := paper.ApplyTerminalSettlement(ctx, event); err != nil || applied {
						t.Fatal("paper retry", applied, err)
					}
					event.CashValue = event.CashValue.Add(decimal.NewFromInt(1))
					if _, err := s.ApplyTerminalSettlement(ctx, event); err == nil {
						t.Fatal("conflict accepted")
					}
					if _, err := paper.ApplyTerminalSettlement(ctx, event); err == nil {
						t.Fatal("paper conflict accepted")
					}
					if _, err := s.ApplyExternalPosition(ctx, ExternalPositionEvent{ID: "resurrect", Kind: ExternalCashChange, Instrument: ledgerInstrument(), Side: Buy, Steps: 1, Price: intentPrice("100"), AtMS: 30}); err == nil {
						t.Fatal("terminal resurrected")
					}
				})
			}
		}
	}
}
func TestTerminalSettlementPendingAndChronology(t *testing.T) {
	s, _, h, _ := testStore(t)
	fundStrategies(t, s)
	ctx := context.Background()
	paper, _ := NewPaperAdapter(intentPrice("1000"), decimal.Zero, decimal.Zero)
	e := &OwnerExecutor{Handle: h, Token: h.Token(), Store: s, Adapter: paper}
	if _, err := e.PrepareRebalance(domainRequest("pending-terminal", 1, 10, "100", 3, -2)); err != nil {
		t.Fatal(err)
	}
	event := TerminalSettlement{Instrument: ledgerInstrument(), CashValue: decimal.Zero, EventMS: 20, PublishedMS: 21, ReceivedMS: 25, AtMS: 24}
	if _, err := s.ApplyTerminalSettlement(ctx, event); err == nil {
		t.Fatal("future receipt accepted")
	}
	event.AtMS = 25
	if _, err := s.ApplyTerminalSettlement(ctx, event); err == nil {
		t.Fatal("pending order accepted")
	}
	if done, err := s.TerminalSettled(ctx, event.Instrument); err != nil || done {
		t.Fatal("rejected settlement persisted", done, err)
	}
	event.CashValue = intentPrice("-1")
	if _, err := s.ApplyTerminalSettlement(ctx, event); err == nil {
		t.Fatal("negative recovery accepted")
	}
}

func TestTerminalSettlementUncertainOrderBlocksAfterIntentExpiry(t *testing.T) {
	s, _, h, _ := testStore(t)
	fundStrategies(t, s)
	ctx := context.Background()
	order := planOrder(t, s, "unknown-terminal", 1, Buy, EntryIntent, map[string]int64{"a": 1})
	adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true}}
	adapter.submit = func(context.Context, OrderIntent, string) (SubmitReceipt, error) {
		return SubmitReceipt{}, errors.New("acknowledgement lost")
	}
	e := executorFor(s, h, adapter)
	if err := e.Send(order.ID, 11); err == nil {
		t.Fatal("expected uncertain transport")
	}
	event := TerminalSettlement{Source: "terminal", SourceVersion: "v1", Instrument: ledgerInstrument(), CashValue: decimal.Zero, EventMS: 1000, PublishedMS: 1001, ReceivedMS: 1002, AtMS: 1002}
	if _, err := s.ApplyTerminalSettlement(ctx, event); err == nil {
		t.Fatal("unknown order bypassed terminal settlement guard")
	}
	if done, err := s.TerminalSettled(ctx, event.Instrument); err != nil || done {
		t.Fatal("rejected terminal event committed", done, err)
	}
}
