package ormo

import (
	"path/filepath"
	"testing"

	"github.com/banbox/banexg"
)

func archiveView(taskID, id int64, filled float64) *InOutOrder {
	return &InOutOrder{
		IOrder: &IOrder{ID: id, TaskID: taskID, Symbol: "BTC/USDT:USDT", Sid: 1, Timeframe: "15m", Status: InOutStatusFullExit,
			EnterTag: "long", ExitTag: "tp", InitPrice: 100, QuoteCost: 100, Leverage: 2, EnterAt: 1000, ExitAt: 2000,
			Strategy: "demo", Profit: 9.5, ProfitRate: 0.095},
		Enter: &ExOrder{ID: 77, InoutID: id, Symbol: "BTC/USDT:USDT", Enter: true, OrderType: banexg.OdTypeMarket,
			Side: banexg.OdSideBuy, CreateAt: 1000, UpdateAt: 1001, Price: 100, Average: 100, Amount: 1, Filled: filled, Status: OdStatusClosed, Fee: 0.05},
		Exit: &ExOrder{ID: 78, InoutID: id, Symbol: "BTC/USDT:USDT", OrderType: banexg.OdTypeMarket,
			Side: banexg.OdSideSell, CreateAt: 2000, UpdateAt: 2000, Price: 110, Average: 110, Amount: 1, Filled: filled, Status: OdStatusClosed, Fee: 0.05},
		Info: map[string]interface{}{"shared_lot": "ts/s/1"},
	}
}

func TestArchiveOrderWritesOneHistoricalRowPerKey(t *testing.T) {
	state := NewOrderState()
	state.BindTradesPath(filepath.Join(t.TempDir(), "orders.db"))
	if err := InitLiveTasksWithState(state, []string{"default"}, "runtime", false); err != nil {
		t.Fatal(err)
	}
	taskID := state.GetTaskID("default")
	view := archiveView(taskID, 9_000_000_001, 1)

	rowID, err := state.ArchiveOrder(view, "acct/ts/lot-1")
	if err != nil || rowID <= 0 {
		t.Fatalf("archive: id=%d err=%v", rowID, err)
	}
	// The projection keeps its own identity and gains no archive marker.
	if view.ID != 9_000_000_001 || view.Enter.ID != 77 || view.Info[OdInfoArchiveKey] != nil {
		t.Fatalf("projection was modified: id=%d enter=%d info=%v", view.ID, view.Enter.ID, view.Info)
	}
	// A repeated call (crash before the caller recorded success) reuses the row.
	again, err := state.ArchiveOrder(view, "acct/ts/lot-1")
	if err != nil || again != rowID {
		t.Fatalf("repeat archive: id=%d want %d err=%v", again, rowID, err)
	}
	// Another position closing at the same instant is a different row.
	other, err := state.ArchiveOrder(archiveView(taskID, 9_000_000_002, 1), "acct/ts/lot-2")
	if err != nil || other == rowID || other <= 0 {
		t.Fatalf("second lot: id=%d err=%v", other, err)
	}
	// Never-filled and still-open views are not history.
	if id, err := state.ArchiveOrder(archiveView(taskID, 3, 0), "acct/ts/lot-3"); err != nil || id != 0 {
		t.Fatalf("unfilled order archived: id=%d err=%v", id, err)
	}
	open := archiveView(taskID, 4, 1)
	open.Status = InOutStatusFullEnter
	if id, err := state.ArchiveOrder(open, "acct/ts/lot-4"); err != nil || id != 0 {
		t.Fatalf("open order archived: id=%d err=%v", id, err)
	}
	if _, err := state.ArchiveOrder(view, ""); err == nil {
		t.Fatal("archive without a key accepted")
	}

	sess, conn, err := state.Conn(false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rows, err := sess.GetOrders(GetOrdersArgs{TaskID: taskID, Status: 2})
	if err != nil || len(rows) != 2 {
		t.Fatalf("history rows=%d err=%v", len(rows), err)
	}
	for _, row := range rows {
		if row.ID != rowID {
			continue
		}
		if row.Strategy != "demo" || row.ExitTag != "tp" || row.Profit != 9.5 || row.Enter == nil || row.Exit == nil ||
			row.Enter.Average != 100 || row.Exit.Average != 110 || row.Enter.InoutID != rowID || row.Exit.InoutID != rowID ||
			row.GetInfoString(OdInfoArchiveKey) != "acct/ts/lot-1" || row.GetInfoString("shared_lot") != "ts/s/1" {
			t.Fatalf("archived row lost fields: %+v enter=%+v exit=%+v info=%v", row.IOrder, row.Enter, row.Exit, row.Info)
		}
		return
	}
	t.Fatal("archived row not found by ID")
}
