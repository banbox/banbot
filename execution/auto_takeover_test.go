package execution

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func TestAutoTakeoverExternalPreservesEconomicsAndFreshLots(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			s, _, _, _ := testStore(t)
			attributionPolicy(t, s)
			for _, side := range []OrderSide{Buy, Sell} {
				// Independent descriptors keep the two opening directions separate.
				instrument := ledgerInstrument()
				instrument.ID += string(side)
				event := ExternalPositionEvent{ID: "external/" + string(side), Kind: ExternalCashChange, Instrument: instrument, Side: side, Steps: 3, Price: intentPrice("100"), Fee: intentPrice("0.2"), AtMS: 10}
				if _, err := s.ApplyExternalPosition(ctx, event); err != nil {
					t.Fatal(err)
				}
				fundingAmount := instrument.Notional(3, event.Price).Mul(intentPrice("0.01")).Neg()
				if side == Sell {
					fundingAmount = fundingAmount.Neg()
				}
				if _, err := s.ApplyFunding(ctx, FundingSettlement{ID: "funding/" + string(side), Instrument: instrument, Mark: event.Price, Rate: intentPrice("0.01"), AccountAmount: fundingAmount, AtMS: 11}); err != nil {
					t.Fatal(err)
				}
				before, _ := s.Snapshot(ctx)
				adopted, err := s.AutoTakeoverExternal(ctx, "a", 11)
				if err != nil || len(adopted) != 1 {
					t.Fatal(adopted, err)
				}
				after, _ := s.Snapshot(ctx)
				if !reflect.DeepEqual(before.ActualPositions, after.ActualPositions) || !before.AccountSettledCash.Equal(after.AccountSettledCash) || !after.RiskFrozen {
					t.Fatal("adoption changed real economics or unfroze admission", before, after)
				}
				lot, err := s.Lot(ctx, "a", adopted[0].Attribution.Lot)
				if err != nil || !lot.Fees.Equal(event.Fee) || !lot.Funding.Equal(fundingAmount) || !lot.CostBasis.Equal(instrument.Notional(3, event.Price)) {
					t.Fatal("adopted economics missing", lot, err)
				}
				book, err := s.LoadPendingBook(ctx)
				if err != nil || len(book) != len(after.Lots) {
					t.Fatal("adoption lacks account-owned pending state", book, err)
				}
				for _, definition := range book {
					if definition.Order.Entry.State != Filled || definition.Order.Entry.FilledSteps != 3 || definition.Order.Desired == 0 {
						t.Fatal("adoption could resubmit an entry", definition)
					}
				}
				if again, err := s.AutoTakeoverExternal(ctx, "a", 12); err != nil || len(again) != 0 {
					t.Fatal("retry created another lot", again, err)
				}
				event.ID += "/more"
				event.AtMS = 13
				if _, err := s.ApplyExternalPosition(ctx, event); err != nil {
					t.Fatal(err)
				}
				more, err := s.AutoTakeoverExternal(ctx, "a", 14)
				if err != nil || len(more) != 1 || more[0].Attribution.Lot == adopted[0].Attribution.Lot {
					t.Fatal("another external source reused an existing lot", more, err)
				}
			}
		})
	}
}

func TestAutoTakeoverExternalQuarantinesAmbiguousAndActiveBooks(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		for _, scenario := range []string{"external-reduction", "liquidation", "active-order", "owned-reduction", "missing-source"} {
			t.Run(backend+"/"+scenario, func(t *testing.T) {
				ctx := context.Background()
				s, _, h, _ := testStore(t)
				attributionPolicy(t, s)
				instrument := ledgerInstrument()
				if scenario == "active-order" || scenario == "owned-reduction" {
					order := planOrder(t, s, "strategy-entry", 1, Buy, EntryIntent, map[string]int64{"a": 5})
					if scenario == "owned-reduction" {
						a := &fakeExecutionAdapter{}
						e := executorFor(s, h, a)
						if err := e.Send(order.ID, 11); err != nil {
							t.Fatal(err)
						}
						if err := e.ApplyTrade(FillReport{EventID: "owned", OrderID: order.ID, Steps: 5, Price: intentPrice("100"), AtMS: 12}); err != nil {
							t.Fatal(err)
						}
					}
				}
				event := ExternalPositionEvent{ID: "external", Kind: ExternalCashChange, Instrument: instrument, Side: Buy, Steps: 3, Price: intentPrice("100"), AtMS: 13}
				if scenario == "liquidation" {
					event.Kind = Liquidation
				}
				if scenario == "owned-reduction" {
					event.Side = Sell
				}
				if _, err := s.ApplyExternalPosition(ctx, event); err != nil {
					t.Fatal(err)
				}
				if scenario == "external-reduction" {
					event.ID, event.Side, event.Steps, event.AtMS = "reduction", Sell, 1, 14
					if _, err := s.ApplyExternalPosition(ctx, event); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "missing-source" {
					if err := s.commit(ctx, func(tx *storeTxn) error {
						return s.recordOnlyTakeoverTestEvent(tx)
					}); err != nil {
						t.Fatal(err)
					}
				}
				before, _ := s.Snapshot(ctx)
				if records, err := s.AutoTakeoverExternal(ctx, "a", 15); err != nil || len(records) != 0 {
					t.Fatal("ambiguous external activity adopted", records, err)
				}
				after, _ := s.Snapshot(ctx)
				if !reflect.DeepEqual(before, after) {
					t.Fatal("quarantined book changed", before, after)
				}
			})
		}
	}
}

func (s *Store) recordOnlyTakeoverTestEvent(tx *storeTxn) error {
	// A raw external descriptor without its source trades is not sufficient.
	var body string
	if err := tx.QueryRow(opReadExternalPosition, s.accountID, "BTC").Scan(&body); err != nil {
		return err
	}
	var lot VirtualLot
	if err := json.Unmarshal([]byte(body), &lot); err != nil {
		return err
	}
	lot.CostBasis = lot.CostBasis.Add(intentPrice("1"))
	updated, _ := payload(lot)
	_, err := tx.Exec(opPutExternalPosition, s.accountID, "BTC", lot.SignedSteps, updated)
	return err
}

func TestAutoTakeoverExternalRollbackAndRestart(t *testing.T) {
	ctx := context.Background()
	s, _, _, path := testStore(t)
	attributionPolicy(t, s)
	event := ExternalPositionEvent{ID: "external", Kind: ExternalCashChange, Instrument: ledgerInstrument(), Side: Buy, Steps: 2, Price: intentPrice("100"), Fee: intentPrice("0.2"), AtMS: 10}
	if _, err := s.ApplyExternalPosition(ctx, event); err != nil {
		t.Fatal(err)
	}
	before, _ := s.Snapshot(ctx)
	if _, err := s.AutoTakeoverExternal(ctx, "undeclared", 11); err == nil {
		t.Fatal("undeclared takeover owner admitted")
	}
	after, _ := s.Snapshot(ctx)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("failed takeover partially committed")
	}
	records, err := s.AutoTakeoverExternal(ctx, "a", 11)
	if err != nil || len(records) != 1 {
		t.Fatal(records, err)
	}
	key := s.Account()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restored, err := OpenStore(path, key)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if again, err := restored.AutoTakeoverExternal(ctx, "a", 12); err != nil || len(again) != 0 {
		t.Fatal("restart replay created another lot", again, err)
	}
	persisted, err := restored.AutoTakeoverRecords(ctx)
	if err != nil || !reflect.DeepEqual(records, persisted) {
		t.Fatal("adoption projection evidence lost on restart", persisted, err)
	}
}

type takeoverSnapshotAdapter struct {
	*fakeExecutionAdapter
	snapshot VenueSnapshot
}

func (a *takeoverSnapshotAdapter) Snapshot(context.Context) (VenueSnapshot, error) {
	return a.snapshot, nil
}

func TestAutoTakeoverAccountRequiresAuthoritativeReconciliation(t *testing.T) {
	for _, scenario := range []string{"normal", "unknown-order", "cash-mismatch", "position-mismatch", "unexplained-position"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			s, _, h, _ := testStore(t)
			attributionPolicy(t, s)
			a := &takeoverSnapshotAdapter{fakeExecutionAdapter: &fakeExecutionAdapter{}, snapshot: VenueSnapshot{Cash: "0", Positions: map[string]int64{"BTC": 3}}}
			service := &SharedAccount{ctx: ctx, store: s, owner: h, executor: &OwnerExecutor{Store: s, Handle: h, Token: h.Token(), Adapter: a}, opts: SharedExecutionOptions{Adapter: a, TakeoverStrategy: "a", AuthoritativeSnapshot: true}, ready: true}
			borrow := service.Borrow()
			defer borrow.Release()
			event := ExternalPositionEvent{ID: "external", Kind: ExternalCashChange, Instrument: ledgerInstrument(), Side: Buy, Steps: 3, Price: intentPrice("100"), AtMS: 10}
			if scenario == "normal" {
				if applied, err := borrow.ApplyExternalPosition(event); err != nil || !applied {
					t.Fatal(applied, err)
				}
				before, _ := s.Snapshot(ctx)
				if !before.RiskFrozen || service.ready || len(before.Lots) != 1 {
					t.Fatal("automatic adoption implicitly published ready", before)
				}
			} else {
				if _, err := s.ApplyExternalPosition(ctx, event); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "unknown-order":
				a.snapshot.OpenOrders = []QueryResult{{Found: true, Authoritative: true, Complete: true, Receipt: SubmitReceipt{ExchangeID: "unknown"}}}
			case "cash-mismatch":
				a.snapshot.Cash = "1"
			case "position-mismatch":
				a.snapshot.Positions["BTC"] = 4
			case "unexplained-position":
				a.snapshot.Positions["untracked"] = 1
			}
			err := borrow.Reconcile("authoritative", 11)
			after, readErr := s.Snapshot(ctx)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if scenario == "normal" {
				if err != nil || !service.ready || after.RiskFrozen || len(after.Lots) != 1 {
					t.Fatal("authoritative consistent account not admitted", after, err)
				}
			} else if err == nil || service.ready || !after.RiskFrozen || len(after.Lots) != 0 {
				t.Fatal("unproven venue state automatically adopted or admitted", after, err)
			}
		})
	}
}
