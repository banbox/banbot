package biz

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/banbox/banbot/execution"
)

func TestSharedPriorEmptyCanceledEntryRemainsHistoryNotExecutableIntent(t *testing.T) {
	ctx := context.Background()
	store, err := execution.NewMemoryStore(execution.AccountKey{VenueSessionIdentity: "canceled-upgrade", Account: "default", SettlementDomain: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	old := sharedTSCheckpoint{Version: "v1", Orders: map[string]*sharedTSOrder{
		"ts/1": {ID: 1, Symbol: "BTC", PendingOrder: execution.PendingOrder{Strategy: "ts", Lot: "legacy/ts/1", Canceled: true, Entry: execution.EligibleIntent{State: execution.Canceled}}},
	}}
	body, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveStrategyCheckpoint(ctx, sharedCheckpointStrategy, "legacy-ts", body); err != nil {
		t.Fatal(err)
	}
	state, err := loadSharedCheckpoint(store, ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	manager := &SharedOrderMgr{config: &SharedOrderBridgeConfig{}}
	definitions, err := manager.pendingDefinitions(&state)
	if err != nil || len(definitions) != 0 || state.Orders["ts/1"] == nil || state.Orders["ts/1"].Entry.QuantitySteps != 0 {
		t.Fatal("inert legacy history became executable or was rewritten", definitions, state.Orders, err)
	}
	if _, err := execution.PendingBookCheckpoint(definitions); err != nil {
		t.Fatal("inert history blocked account registry upgrade", err)
	}
	state.Orders["ts/1"].Canceled = false
	if _, err := manager.pendingDefinitions(&state); err == nil {
		t.Fatal("active invalid source was silently omitted")
	}
}
