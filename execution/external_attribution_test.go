package execution

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/shopspring/decimal"
)

func attributionPolicy(t *testing.T, s *Store) {
	t.Helper()
	if err := s.RegisterAccountPolicy(context.Background(), AccountPolicy{Version: "v1", Currency: s.Account().SettlementDomain, MarginRate: intentPrice("0.1"), MaxAccountMargin: intentPrice("10000"), MaxVirtualGross: intentPrice("10000"), StrategyGrossLimits: map[StrategyID]decimal.Decimal{"a": intentPrice("10000")}}); err != nil {
		t.Fatal(err)
	}
}

func assertAdoptionEntry(t *testing.T, s *Store, request ExternalAttribution, side OrderSide) {
	t.Helper()
	// Attribution records local adoption provenance, not an inferred original
	// venue entry timestamp. Only the adopted remaining inventory is an entry.
	atMS, err := s.LotEntryTime(context.Background(), request.Strategy, request.Lot, side)
	if err != nil || atMS != request.AtMS {
		t.Fatal("incorrect local adoption timestamp", atMS, err)
	}
	steps, err := s.LotEntrySteps(context.Background(), request.Strategy, request.Lot, side)
	if err != nil || steps != absSteps(request.ExpectedSignedSteps) {
		t.Fatal("incorrect adopted entry quantity", steps, err)
	}
}

func TestExternalAttributionConservation(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			for _, option := range []bool{false, true} {
				for _, side := range []OrderSide{Buy, Sell} {
					t.Run(fmt.Sprintf("option=%v/%s", option, side), func(t *testing.T) {
						ctx := context.Background()
						s, _, _, _ := testStore(t)
						attributionPolicy(t, s)
						instrument := ledgerInstrument()
						if option {
							instrument = cashOption("call")
						}
						if _, err := s.ApplyCashEvent(ctx, CashEvent{ID: "capital", Kind: ExternalCashChange, AccountDelta: intentPrice("1000"), Postings: []CashPosting{{Amount: intentPrice("1000")}}, AtMS: 1}); err != nil {
							t.Fatal(err)
						}
						opposite := Sell
						if side == Sell {
							opposite = Buy
						}
						events := []ExternalPositionEvent{
							{ID: "open", Kind: ExternalCashChange, Instrument: instrument, Side: side, Steps: 5, Price: intentPrice("100"), Fee: intentPrice("2"), AtMS: 10},
							{ID: "reduce", Kind: ExternalCashChange, Instrument: instrument, Side: opposite, Steps: 2, Price: intentPrice("120"), Fee: intentPrice("1"), AtMS: 11},
						}
						for _, event := range events {
							if _, err := s.ApplyExternalPosition(ctx, event); err != nil {
								t.Fatal(err)
							}
						}
						before, err := s.Snapshot(ctx)
						if err != nil {
							t.Fatal(err)
						}
						external := before.ExternalPositions[0]
						request := ExternalAttribution{ID: "attribute", Strategy: "a", Lot: "adopted", Instrument: instrument, ExpectedSignedSteps: external.SignedSteps, AtMS: 12}
						if applied, err := s.AttributeExternal(ctx, request); err != nil || !applied {
							t.Fatal(applied, err)
						}
						assertAdoptionEntry(t, s, request, side)
						after, err := s.Snapshot(ctx)
						if err != nil {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(before.ActualPositions, after.ActualPositions) || !before.AccountSettledCash.Equal(after.AccountSettledCash) || !before.PnLReclassification.Equal(after.PnLReclassification) || !after.RiskFrozen || (len(after.ExternalPositions) != 1 || after.ExternalPositions[0].SignedSteps != 0) || len(after.Lots) != 1 {
							t.Fatalf("non-attribution change: before=%+v after=%+v", before, after)
						}
						expected := external
						expected.Strategy = "a"
						expected.ID = "adopted"
						if !reflect.DeepEqual(expected, after.Lots[0]) {
							t.Fatalf("economics changed: want=%+v got=%+v", expected, after.Lots[0])
						}
						if !before.UnassignedCash.Equal(after.UnassignedCash.Add(after.SyntheticStrategyCash["a"])) {
							t.Fatal("cash not conserved")
						}
						cash := external.RealizedPnL.Sub(external.Fees)
						if option {
							if side == Buy {
								cash = cash.Sub(external.CostBasis)
							} else {
								cash = cash.Add(external.CostBasis)
							}
						}
						if !cash.Equal(after.SyntheticStrategyCash["a"]) {
							t.Fatal("incorrect cash attribution", cash, after.SyntheticStrategyCash)
						}
						totals, err := s.StrategyTotals(ctx, "a")
						if err != nil {
							t.Fatal(err)
						}
						if !totals.Fees.Equal(external.Fees) {
							t.Fatal("fees not transferred", totals)
						}
						if applied, err := s.AttributeExternal(ctx, request); err != nil || applied {
							t.Fatal("retry", applied, err)
						}
						unchanged, _ := s.Snapshot(ctx)
						if !reflect.DeepEqual(after, unchanged) {
							t.Fatal("retry changed state")
						}
						request.Lot = "different"
						if applied, err := s.AttributeExternal(ctx, request); err == nil || applied {
							t.Fatal("conflicting event accepted")
						}
						// New fills must not inherit already-transferred realized PnL/fees.
						if _, err := s.ApplyExternalPosition(ctx, ExternalPositionEvent{ID: "new-open", Kind: ExternalCashChange, Instrument: instrument, Side: side, Steps: 1, Price: intentPrice("110"), AtMS: 13}); err != nil {
							t.Fatal(err)
						}
						next, _ := s.Snapshot(ctx)
						if len(next.ExternalPositions) != 1 || !next.ExternalPositions[0].Fees.IsZero() || !next.ExternalPositions[0].RealizedPnL.IsZero() {
							t.Fatal("external history transferred twice", next.ExternalPositions)
						}
					})
				}
			}
		})
	}
}

func TestExternalAttributionRollback(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			s, _, _, _ := testStore(t)
			if _, err := s.ApplyExternalPosition(ctx, ExternalPositionEvent{ID: "external", Kind: ExternalCashChange, Instrument: ledgerInstrument(), Side: Buy, Steps: 3, Price: intentPrice("100"), AtMS: 10}); err != nil {
				t.Fatal(err)
			}
			request := ExternalAttribution{ID: "attribute", Strategy: "a", Lot: "adopted", Instrument: ledgerInstrument(), ExpectedSignedSteps: 3, AtMS: 11}
			before, _ := s.Snapshot(ctx)
			if _, err := s.AttributeExternal(ctx, request); err == nil {
				t.Fatal("unregistered strategy accepted")
			}
			after, _ := s.Snapshot(ctx)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("missing policy changed state")
			}
			attributionPolicy(t, s)
			before, _ = s.Snapshot(ctx)
			wrong := request
			wrong.ExpectedSignedSteps = -3
			if _, err := s.AttributeExternal(ctx, wrong); err == nil {
				t.Fatal("wrong inventory accepted")
			}
			wrong = request
			wrong.Instrument.Version = "v2"
			if _, err := s.AttributeExternal(ctx, wrong); err == nil {
				t.Fatal("wrong instrument accepted")
			}
			after, _ = s.Snapshot(ctx)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("failed validation changed state")
			}
			if applied, err := s.AttributeExternal(ctx, request); err != nil || !applied {
				t.Fatal("rollback retained event identity", applied, err)
			}
		})
	}
}

func TestExternalAttributionConflicts(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			for _, conflict := range []string{"existing-lot", "active-order", "unknown-order"} {
				t.Run(conflict, func(t *testing.T) {
					ctx := context.Background()
					s, _, _, _ := testStore(t)
					attributionPolicy(t, s)
					// Prepare before external activity freezes admission.
					order := planOrder(t, s, "pending", 1, Buy, EntryIntent, map[string]int64{"a": 1})
					if conflict != "active-order" {
						state := OrderCanceled
						if conflict == "unknown-order" {
							state = OrderUnknown
						}
						if err := s.commit(ctx, func(tx *storeTxn) error { return s.setOrderState(tx, order.ID, state) }); err != nil {
							t.Fatal(err)
						}
					}
					if conflict == "existing-lot" {
						if err := s.commit(ctx, func(tx *storeTxn) error {
							return s.writeLot(tx, VirtualLot{Strategy: "a", ID: "adopted", Instrument: ledgerInstrument()})
						}); err != nil {
							t.Fatal(err)
						}
					}
					if _, err := s.ApplyExternalPosition(ctx, ExternalPositionEvent{ID: "external", Kind: ExternalCashChange, Instrument: ledgerInstrument(), Side: Buy, Steps: 3, Price: intentPrice("100"), AtMS: 10}); err != nil {
						t.Fatal(err)
					}
					before, _ := s.Snapshot(ctx)
					request := ExternalAttribution{ID: "attribute", Strategy: "a", Lot: "adopted", Instrument: ledgerInstrument(), ExpectedSignedSteps: 3, AtMS: 11}
					if applied, err := s.AttributeExternal(ctx, request); err == nil || applied {
						t.Fatal("conflict accepted", applied, err)
					}
					after, _ := s.Snapshot(ctx)
					if !reflect.DeepEqual(before, after) {
						t.Fatal("conflict changed state")
					}
				})
			}
		})
	}
}

func TestExternalAttributionDurableRetry(t *testing.T) {
	ctx := context.Background()
	s, _, _, path := testStore(t)
	attributionPolicy(t, s)
	if _, err := s.ApplyExternalPosition(ctx, ExternalPositionEvent{ID: "external", Kind: ExternalCashChange, Instrument: ledgerInstrument(), Side: Sell, Steps: 3, Price: intentPrice("100"), Fee: intentPrice("1"), AtMS: 10}); err != nil {
		t.Fatal(err)
	}
	funding := FundingSettlement{ID: "funding", Instrument: ledgerInstrument(), Mark: intentPrice("100"), Rate: intentPrice("0.01"), AccountAmount: intentPrice("0.3"), AtMS: 10}
	if applied, err := s.ApplyFunding(ctx, funding); err != nil || !applied {
		t.Fatal(applied, err)
	}
	request := ExternalAttribution{ID: "attribute", Strategy: "a", Lot: "adopted", Instrument: ledgerInstrument(), ExpectedSignedSteps: -3, AtMS: 11}
	if applied, err := s.AttributeExternal(ctx, request); err != nil || !applied {
		t.Fatal(applied, err)
	}
	assertAdoptionEntry(t, s, request, Sell)
	before, _ := s.Snapshot(ctx)
	s.Close()
	reopened, err := OpenStore(path, s.key)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if applied, err := reopened.AttributeExternal(ctx, request); err != nil || applied {
		t.Fatal("restart retry", applied, err)
	}
	assertAdoptionEntry(t, reopened, request, Sell)
	if applied, err := reopened.ApplyFunding(ctx, funding); err != nil || applied {
		t.Fatal("restart funding retry", applied, err)
	}
	totals, err := reopened.StrategyTotals(ctx, request.Strategy)
	if err != nil || !totals.Funding.Equal(funding.AccountAmount) {
		t.Fatal("restart lost attributed funding", totals, err)
	}
	after, _ := reopened.Snapshot(ctx)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("restart changed attribution")
	}
	request.ExpectedSignedSteps = -2
	if _, err := reopened.AttributeExternal(ctx, request); err == nil {
		t.Fatal("restart lost identity conflict")
	}
}

func TestExternalFundingAttribution(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			for _, side := range []OrderSide{Buy, Sell} {
				t.Run(string(side), func(t *testing.T) {
					ctx := context.Background()
					s, _, _, _ := testStore(t)
					attributionPolicy(t, s)
					instrument := ledgerInstrument()
					if _, err := s.ApplyExternalPosition(ctx, ExternalPositionEvent{ID: "external", Kind: ExternalCashChange, Instrument: instrument, Side: side, Steps: 3, Price: intentPrice("100"), Fee: intentPrice("1"), AtMS: 10}); err != nil {
						t.Fatal(err)
					}
					steps := int64(3)
					if side == Sell {
						steps = -steps
					}
					funding := instrument.Notional(steps, intentPrice("120")).Mul(intentPrice("0.01")).Neg()
					// The unrelated venue residual must remain unassigned on adoption.
					residual := intentPrice("7")
					event := FundingSettlement{ID: "funding", Instrument: instrument, Mark: intentPrice("120"), Rate: intentPrice("0.01"), AccountAmount: funding.Add(residual), AtMS: 11}
					beforeInvalid, err := s.Snapshot(ctx)
					if err != nil {
						t.Fatal(err)
					}
					invalid := event
					invalid.Instrument.Version = "v2"
					if _, err := s.ApplyFunding(ctx, invalid); err == nil {
						t.Fatal("external funding instrument revision mismatch accepted")
					}
					afterInvalid, err := s.Snapshot(ctx)
					if err != nil || !reflect.DeepEqual(beforeInvalid, afterInvalid) {
						t.Fatal("invalid external funding changed state", err)
					}
					if applied, err := s.ApplyFunding(ctx, event); err != nil || !applied {
						t.Fatal(applied, err)
					}
					before, err := s.Snapshot(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if !before.ExternalPositions[0].Funding.Equal(funding) || !before.UnassignedCash.Equal(event.AccountAmount.Sub(intentPrice("1"))) || !before.AccountSettledCash.Equal(before.UnassignedCash) {
						t.Fatal("external funding/cash not booked", before)
					}
					if applied, err := s.ApplyFunding(ctx, event); err != nil || applied {
						t.Fatal("funding retry", applied, err)
					}
					conflict := event
					conflict.AccountAmount = decimal.Zero
					if _, err := s.ApplyFunding(ctx, conflict); err == nil {
						t.Fatal("conflicting funding event accepted")
					}
					unchanged, _ := s.Snapshot(ctx)
					if !reflect.DeepEqual(before, unchanged) {
						t.Fatal("funding retry changed state")
					}
					request := ExternalAttribution{ID: "attribute", Strategy: "a", Lot: "adopted", Instrument: instrument, ExpectedSignedSteps: steps, AtMS: 12}
					if applied, err := s.AttributeExternal(ctx, request); err != nil || !applied {
						t.Fatal(applied, err)
					}
					after, err := s.Snapshot(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if !after.UnassignedCash.Equal(residual) || !after.SyntheticStrategyCash["a"].Equal(funding.Sub(intentPrice("1"))) || !after.Lots[0].Funding.Equal(funding) || !after.ExternalPositions[0].Funding.IsZero() || !before.AccountSettledCash.Equal(after.AccountSettledCash) || !before.PnLReclassification.Equal(after.PnLReclassification) || !reflect.DeepEqual(before.ActualPositions, after.ActualPositions) {
						t.Fatal("funding attribution changed unrelated economics", before, after)
					}
					totals, err := s.StrategyTotals(ctx, "a")
					if err != nil || !totals.Funding.Equal(funding) {
						t.Fatal("funding total not attributed", totals, err)
					}
					if applied, err := s.ApplyFunding(ctx, event); err != nil || applied {
						t.Fatal("funding replay after adoption", applied, err)
					}
					if applied, err := s.AttributeExternal(ctx, request); err != nil || applied {
						t.Fatal("attribution replay", applied, err)
					}
					unchanged, _ = s.Snapshot(ctx)
					if !reflect.DeepEqual(after, unchanged) {
						t.Fatal("post-adoption retry changed state")
					}
				})
			}
		})
	}
}
