package biz

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/execution"
)

func TestSharedCheckpointSplitsLegacyFactsAndReadsCold(t *testing.T) {
	ctx := context.Background()
	store, err := execution.NewMemoryStoreWithHistory(execution.AccountKey{VenueSessionIdentity: "legacy-checkpoint", Account: "default", SettlementDomain: "USD"}, filepath.Join(t.TempDir(), "history.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	old := sharedTSCheckpoint{Version: "v1", Serial: 100, Orders: map[string]*sharedTSOrder{
		"ts/1": {ID: 1, Strategy: "ts", Lot: "imported-lot", Canceled: true, Entry: execution.EligibleIntent{State: execution.Canceled}},
		"ts/2": {ID: 2, Strategy: "ts", Lot: "legacy/ts/2", Entry: execution.EligibleIntent{State: execution.PendingCondition}},
	}}
	command, err := sharedCommand(&old, "entry-1", "original")
	if err != nil {
		t.Fatal(err)
	}
	command.Orders = []int64{1}
	old.Commands["entry-1"] = command
	body, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveStrategyCheckpoint(ctx, sharedCheckpointStrategy, "legacy-ts", body); err != nil {
		t.Fatal(err)
	}
	state, err := loadSharedCheckpoint(store, ctx, "v1")
	if err != nil || len(state.Orders) != 2 {
		t.Fatalf("legacy checkpoint import: %v %v", state, err)
	}
	checkpoint, err := splitSharedCheckpoint(state, execution.AccountSnapshot{})
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range checkpoint.Records {
		if err := store.SaveStrategyCheckpoint(ctx, record.Strategy, record.Name, record.Payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.SaveStrategyCheckpoint(ctx, checkpoint.Strategy, checkpoint.Name, checkpoint.Payload); err != nil {
		t.Fatal(err)
	}
	state, err = loadSharedCheckpoint(store, ctx, "v1")
	if err != nil || len(state.Orders) != 1 || state.Orders["ts/2"] == nil || len(state.Commands) != 0 {
		t.Fatalf("closed history remained active: %v %v", state, err)
	}
	if err := state.loadLot("ts", "imported-lot"); err != nil || state.Orders["ts/1"] == nil {
		t.Fatal("nonstandard migrated lot lost its metadata", err)
	}
	retried, err := sharedCommand(&state, "entry-1", "original")
	if !errors.Is(err, errSharedCommandReplay) || len(retried.Orders) != 1 || retried.Orders[0] != 1 {
		t.Fatal("cold command lost original result", retried, err)
	}
	if _, err := sharedCommand(&state, "entry-1", "changed"); err == nil {
		t.Fatal("changed command reused stable identity")
	}
	stats, err := store.MemoryHistoryStats(ctx)
	if err != nil || stats.HotRecords > 2 || stats.ColdRecords < 6 {
		t.Fatalf("compatibility history remains hot: %+v %v", stats, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	state.Commands = nil
	if _, err := sharedCommand(&state, "entry-1", "original"); err == nil || errors.Is(err, errSharedCommandReplay) {
		t.Fatal("cold read failure became a retry or missing command", err)
	}
}
