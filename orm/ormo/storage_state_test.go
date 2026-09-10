package ormo

import (
	"path/filepath"
	"testing"
)

func TestOrderStateStorageIsolation(t *testing.T) {
	first, second := NewOrderState(), NewOrderState()
	for index, state := range []*OrderState{first, second} {
		state.BindTradesPath(filepath.Join(t.TempDir(), "orders.db"))
		if err := InitLiveTasksWithState(state, []string{"default"}, "runtime", false); err != nil {
			t.Fatal(err)
		}
		task := state.GetTask("default")
		if task == nil || task.ID <= 0 {
			t.Fatal("live task was not persisted")
		}
		items := []*WalletSnapshotItem{{Coin: "USDT", Available: float64(index + 1)}}
		if err := SaveWalletSnapshot(task.ID, "default", 1000, items, &WalletSnapshotSummary{}, state); err != nil {
			t.Fatal(err)
		}
	}
	for index, state := range []*OrderState{first, second} {
		items, _, err := LoadLatestWalletSnapshot(state.GetTaskID("default"), "default", state)
		if err != nil || len(items) != 1 || items[0].Available != float64(index+1) {
			t.Fatalf("runtime %d snapshot: %v, %v", index, items, err)
		}
	}
	if _, _, err := NewOrderState().Conn(true); err == nil {
		t.Fatal("unbound order state used the legacy database")
	}
}
