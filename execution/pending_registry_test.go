package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/shopspring/decimal"
)

func TestPendingBookRejectsInvalidDefinitions(t *testing.T) {
	store, _, _, _ := testStore(t)
	for name, mutate := range map[string]func(*[]PendingDefinition){
		"duplicate-lot": func(defs *[]PendingDefinition) { *defs = append(*defs, (*defs)[0]) },
		"duplicate-intent": func(defs *[]PendingDefinition) {
			other := PendingDefinition{Order: *pendingTestOrder(store, "b"), Instrument: ledgerInstrument()}
			other.Order.Entry.ID = (*defs)[0].Order.Entry.ID
			*defs = append(*defs, other)
		},
		"instrument": func(defs *[]PendingDefinition) { (*defs)[0].Instrument.QuantityStep = decimal.Zero },
		"policy":     func(defs *[]PendingDefinition) { (*defs)[0].Policy.Mode = "invalid" },
		"entry":      func(defs *[]PendingDefinition) { (*defs)[0].Order.Entry.QuantitySteps = 0 },
		"identity":   func(defs *[]PendingDefinition) { (*defs)[0].Order.Entry.Lot = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			definitions := []PendingDefinition{{Order: *pendingTestOrder(store, "a"), Instrument: ledgerInstrument()}}
			mutate(&definitions)
			if _, err := PendingBookCheckpoint(definitions); err == nil {
				t.Fatal("invalid book checkpoint accepted")
			}
			body, err := json.Marshal(definitions)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.SaveStrategyCheckpoint(context.Background(), PendingBookStrategy, PendingBookName, body); err != nil {
				t.Fatal(err)
			}
			if _, err := store.LoadPendingBook(context.Background()); err == nil {
				t.Fatal("invalid persisted book accepted")
			}
		})
	}
}

func saveTestPendingBook(t *testing.T, store *Store, definitions []PendingDefinition) {
	t.Helper()
	checkpoint, err := PendingBookCheckpoint(definitions)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveStrategyCheckpoint(context.Background(), checkpoint.Strategy, checkpoint.Name, checkpoint.Payload); err != nil {
		t.Fatal(err)
	}
}

func TestPendingBookPersistsExecutionDefinitions(t *testing.T) {
	store, _, _, path := testStore(t)
	definitions := []PendingDefinition{
		{Order: *pendingTestOrder(store, "b"), Instrument: ledgerInstrument(), Policy: OrderPolicy{Mode: ExchangeFirst}},
		{Order: *pendingTestOrder(store, "a"), Instrument: ledgerInstrument(), Specs: []PositionProtection{{Kind: TrailingProtection, TrailingPercent: intentPrice("5")}}},
	}
	definitions[1].Order.ProtectionLevels = map[string]decimal.Decimal{string(TrailingProtection): intentPrice("110")}
	saveTestPendingBook(t, store, definitions)
	key := store.Account()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restored, err := OpenStore(path, key)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	book, err := restored.LoadPendingBook(context.Background())
	if err != nil || len(book) != 2 {
		t.Fatal(book, err)
	}
	if book[0].Order.Strategy != "a" || !book[0].Order.ProtectionLevels[string(TrailingProtection)].Equal(intentPrice("110")) || len(book[0].Specs) != 1 || book[1].Policy.Mode != ExchangeFirst {
		t.Fatal("execution definition changed across reopen", book)
	}
	if definitions[0].Order.Strategy != "b" {
		t.Fatal("checkpoint reordered caller definitions")
	}
}

func TestPendingExecutionEmptyBookDoesNotPreparePlan(t *testing.T) {
	borrow, _ := strategyService(t, true)
	for _, persisted := range []bool{false, true} {
		if persisted {
			saveTestPendingBook(t, borrow.service.store, nil)
		}
		if err := borrow.AdvancePendingExecution(context.Background(), 12); err != nil {
			t.Fatal(err)
		}
		if _, err := borrow.LatestPlan(context.Background()); !errors.Is(err, sql.ErrNoRows) {
			t.Fatal("empty pending book prepared a plan", err)
		}
	}
}

func TestPendingExecutionUsesQuoteReceiptDecisionTime(t *testing.T) {
	borrow, _ := strategyService(t, true)
	definition := PendingDefinition{Order: *pendingTestOrder(borrow.service.store, "a"), Instrument: ledgerInstrument()}
	saveTestPendingBook(t, borrow.service.store, []PendingDefinition{definition})
	market := borrow.service.markets["BTC"]
	market.quote = func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: 14, ReceivedMS: 15, ValidUntilMS: 112, Bar: 5}, nil
	}
	borrow.service.markets["BTC"] = market
	if err := borrow.AdvancePendingExecution(context.Background(), 12); err != nil {
		t.Fatal(err)
	}
	plan, err := borrow.LatestPlan(context.Background())
	if err != nil || plan.DecisionMS < 15 {
		t.Fatal("decision precedes quote receipt", plan.DecisionMS, err)
	}
}

func TestPendingExecutionRiskFailureDoesNotAdvanceBook(t *testing.T) {
	borrow, _ := strategyService(t, true)
	store := borrow.service.store
	definition := PendingDefinition{Order: *pendingTestOrder(store, "a"), Instrument: ledgerInstrument()}
	definition.Order.Entry.QuantitySteps = 100
	saveTestPendingBook(t, store, []PendingDefinition{definition})
	before, err := store.StrategyCheckpoint(context.Background(), PendingBookStrategy, PendingBookName)
	if err != nil {
		t.Fatal(err)
	}
	if err := borrow.AdvancePendingExecution(context.Background(), 12); err == nil {
		t.Fatal("account risk admitted oversized standalone entry")
	}
	after, err := store.StrategyCheckpoint(context.Background(), PendingBookStrategy, PendingBookName)
	if err != nil || string(after) != string(before) {
		t.Fatal("risk rejection advanced the durable book", string(after), err)
	}
	if _, err := borrow.LatestPlan(context.Background()); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("risk rejection committed a plan", err)
	}
}

func TestPendingExecutionRestartTickNeedsNoFacade(t *testing.T) {
	borrow, venue := strategyService(t, false)
	store := borrow.service.store
	definition := PendingDefinition{Order: *pendingTestOrder(store, "a"), Instrument: ledgerInstrument(), Specs: []PositionProtection{{Kind: StopLossProtection, Level: intentPrice("95")}}}
	definition.Order.Entry.Conditions.Stop = intentPrice("105")
	saveTestPendingBook(t, store, []PendingDefinition{definition})
	opts := borrow.service.opts
	borrow.Release()
	if err := borrow.service.Close(); err != nil {
		t.Fatal(err)
	}
	registry := &AccountRegistry{}
	owner, err := registry.Acquire(definition.Order.Entry.Account)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewSharedAccount(owner, opts)
	if err != nil {
		t.Fatal(err)
	}
	account := service.Borrow()
	t.Cleanup(func() { account.Release(); registry.Close(); service.Close() })
	if err := account.Reconcile("restart", 11); err != nil {
		t.Fatal(err)
	}
	price := "110"
	if err := account.RegisterAccountQuotes([]Instrument{ledgerInstrument()}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		return VisibleQuote{Bid: intentPrice(price), Ask: intentPrice(price), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := account.AdvancePendingExecution(context.Background(), 12); err != nil {
		t.Fatal(err)
	}
	snapshot, err := account.Snapshot(context.Background())
	if err != nil || len(snapshot.Lots) != 1 || snapshot.Lots[0].SignedSteps != 10 {
		t.Fatal("account tick did not execute persisted stop entry", snapshot.Lots, err)
	}
	price = "90"
	if err := account.AdvancePendingExecution(context.Background(), 13); err != nil {
		t.Fatal(err)
	}
	book, err := service.store.LoadPendingBook(context.Background())
	if err != nil || len(book) != 1 || book[0].Order.LastProtection != StopLossProtection || !book[0].Order.Canceled || book[0].Order.Desired != 0 {
		t.Fatal("standalone protection was not checkpointed", book, err)
	}
	snapshot, err = account.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, lot := range snapshot.Lots {
		if lot.SignedSteps != 0 {
			t.Fatal("standalone stop did not close lot", snapshot.Lots)
		}
	}
	venueSnapshot, err := venue.Snapshot(context.Background())
	if err != nil || venueSnapshot.Positions["BTC"] != 0 {
		t.Fatal("standalone stop did not close venue exposure", venueSnapshot, err)
	}
}

func TestPendingExecutionUnavailableSymbolRiskBoundary(t *testing.T) {
	for _, scenario := range []string{"dormant-missing", "dormant-stale", "carried-missing"} {
		t.Run(scenario, func(t *testing.T) {
			borrow, _ := strategyService(t, false)
			service := borrow.service
			ctx := context.Background()
			active := PendingDefinition{Order: *pendingTestOrder(service.store, "a"), Instrument: ledgerInstrument(), Specs: []PositionProtection{{Kind: StopLossProtection, Level: intentPrice("95")}}}
			active.Order.Entry.Conditions.Stop = intentPrice("105")
			dormant := PendingDefinition{Order: *pendingTestOrder(service.store, "b"), Instrument: ledgerInstrument()}
			dormant.Instrument.ID = "ETH"
			dormant.Order.Entry.Instrument = "ETH"
			dormant.Order.Entry.Conditions.Stop = intentPrice("200")
			if scenario == "carried-missing" {
				dormant.Order.Entry.Conditions.Stop = intentPrice("90")
			}
			saveTestPendingBook(t, service.store, []PendingDefinition{active, dormant})
			price := "110"
			ethAvailable := true
			eth := dormant.Instrument
			if err := borrow.RegisterAccountQuotes([]Instrument{eth}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
				if !ethAvailable {
					if scenario == "dormant-stale" {
						return VisibleQuote{Bid: intentPrice("1000"), Ask: intentPrice("1000"), AtMS: 0, ReceivedMS: 0, ValidUntilMS: 1, Bar: now}, nil
					}
					return VisibleQuote{}, errors.New("ETH unavailable")
				}
				return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 100, Bar: now}, nil
			}, nil); err != nil {
				t.Fatal(err)
			}
			market := service.markets["BTC"]
			market.quote = func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
				return VisibleQuote{Bid: intentPrice(price), Ask: intentPrice(price), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 100, Bar: now}, nil
			}
			service.markets["BTC"] = market
			if err := borrow.AdvancePendingExecution(ctx, 12); err != nil {
				t.Fatal(err)
			}
			before, err := service.store.LoadPendingBook(ctx)
			if err != nil {
				t.Fatal(err)
			}
			ethAvailable = false
			price = "90"
			err = borrow.AdvancePendingExecution(ctx, 13)
			if scenario == "carried-missing" {
				if err == nil {
					t.Fatal("unvalued carried exposure bypassed account risk")
				}
				after, loadErr := service.store.LoadPendingBook(ctx)
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				beforeBody, _ := json.Marshal(before)
				afterBody, _ := json.Marshal(after)
				if string(beforeBody) != string(afterBody) {
					t.Fatal("rejected risk advanced persisted protections")
				}
				snapshot, loadErr := service.store.Snapshot(ctx)
				if loadErr != nil || snapshot.RiskFrozen || !service.Reconciled() {
					t.Fatal("missing public mark froze ledger", snapshot.RiskFrozen, loadErr)
				}
				return
			}
			if err != nil {
				t.Fatal("dormant unavailable symbol blocked protective exit", err)
			}
			after, err := service.store.LoadPendingBook(ctx)
			if err != nil || len(after) != 2 {
				t.Fatal("tick dropped unavailable instrument", after, err)
			}
			if !after[0].Order.Canceled || after[0].Order.LastProtection != StopLossProtection {
				t.Fatal("healthy instrument protection did not advance", after[0])
			}
			oldBody, _ := json.Marshal(before[1])
			newBody, _ := json.Marshal(after[1])
			if string(oldBody) != string(newBody) {
				t.Fatal("unavailable dormant order advanced using an invalid quote")
			}
		})
	}
}
