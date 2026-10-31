package biz

import (
	"context"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/shopspring/decimal"
)

func TestSharedPendingDefinitionsPreserveUnprojectedAccountLots(t *testing.T) {
	ctx := context.Background()
	account := execution.AccountKey{VenueSessionIdentity: "takeover-book", Account: "default", SettlementDomain: "USD"}
	store, err := execution.NewMemoryStore(account)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	instrument := execution.Instrument{ID: "BTC", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.RequireFromString("0.1"), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.RequireFromString("0.01"), MoneyScale: 8}
	definition := execution.PendingDefinition{Instrument: instrument, Order: execution.PendingOrder{Strategy: "ts", Lot: "external/fresh", Desired: 3,
		Entry: execution.EligibleIntent{ID: "adopted-entry", Account: account, Strategy: "ts", Lot: "external/fresh", Instrument: "BTC", Side: execution.Buy, Kind: execution.EntryIntent, QuantitySteps: 3, FilledSteps: 3, State: execution.Filled}}}
	checkpoint, err := execution.PendingBookCheckpoint([]execution.PendingDefinition{definition})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveStrategyCheckpoint(ctx, checkpoint.Strategy, checkpoint.Name, checkpoint.Payload); err != nil {
		t.Fatal(err)
	}
	state := sharedTSCheckpoint{store: store, ctx: ctx, Orders: map[string]*sharedTSOrder{}}
	manager := &SharedOrderMgr{config: &SharedOrderBridgeConfig{Instruments: map[string]execution.Instrument{"BTC": instrument}}}
	definitions, err := manager.pendingDefinitions(&state)
	if err != nil || len(definitions) != 1 || definitions[0].Order.Desired != 3 {
		t.Fatal("missing facade discarded an authoritative account lot", definitions, err)
	}
	// Coordination updates must survive the subsequent checkpoint collection;
	// rereading the stored book here would restore the obsolete holding target.
	key := sharedPendingKey{"ts", "external/fresh"}
	updated := state.carriedPending[key]
	updated.Order.Desired = 0
	state.carriedPending[key] = updated
	definitions, err = manager.pendingDefinitions(&state)
	if err != nil || len(definitions) != 1 || definitions[0].Order.Desired != 0 {
		t.Fatal("checkpoint collection reverted coordinated account state", definitions, err)
	}
	// Once projected, facade mutations supersede the carried version exactly once.
	state.Orders["ts/1"] = &sharedTSOrder{ID: 1, Symbol: "BTC", PendingOrder: definition.Order}
	definitions, err = manager.pendingDefinitions(&state)
	if err != nil || len(definitions) != 1 || definitions[0].Order.Desired != 3 || len(state.carriedPending) != 0 {
		t.Fatal("projected account lot duplicated or lost facade state", definitions, err)
	}
	definition.Order.Desired = 0
	checkpoint, err = execution.PendingBookCheckpoint([]execution.PendingDefinition{definition})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveStrategyCheckpoint(ctx, checkpoint.Strategy, checkpoint.Name, checkpoint.Payload); err != nil {
		t.Fatal(err)
	}
	state = sharedTSCheckpoint{store: store, ctx: ctx, Orders: map[string]*sharedTSOrder{}}
	definitions, err = manager.pendingDefinitions(&state)
	if err != nil || len(definitions) != 0 {
		t.Fatal("settled history was reintroduced as an active zero target", definitions, err)
	}
}
