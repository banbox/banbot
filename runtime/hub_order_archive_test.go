package runtime

import (
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

// Orders of the Hub manager are projections of the account ledger; the UI
// reads history from the trades database, so each closed one is copied there
// exactly once, also across a robot restart.
func TestHubClosedOrdersAreArchivedOnceToTradesDatabase(t *testing.T) {
	f := newHubTSFixture(t, nil)
	f.rt.Orders.BindTradesPath(filepath.Join(t.TempDir(), "orders.db"))
	biz.EnsureTimeSeriesOrderTask(f.rt.BizDeps())
	taskID := f.rt.Orders.GetTaskID("default")
	if taskID < 0 {
		t.Fatal("live runtime did not get a trades database task")
	}
	history := func() []*ormo.InOutOrder {
		t.Helper()
		sess, conn, err := f.rt.Orders.Conn(false)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		rows, err := sess.GetOrders(ormo.GetOrdersArgs{TaskID: taskID, Status: 2})
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}
	symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
	od, err := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "open", Tag: "remote"})
	if err != nil || od == nil || od.Enter.Filled != 1 {
		t.Fatal(od, err)
	}
	if rows := history(); len(rows) != 0 {
		t.Fatalf("open order must not be archived: %d rows", len(rows))
	}
	if od, err = f.manager.ExitOrder(od, &strat.ExitReq{CommandID: "close", Tag: "done"}); err != nil || od.Status != ormo.InOutStatusFullExit {
		t.Fatal(od, err)
	}
	// The projection that closes the lot may still be one poll behind.
	if err := f.manager.UpdateByDataSeries(nil, &orm.DataSeries{Sid: 1, TimeFrame: "ws"}); err != nil {
		t.Fatal(err)
	}
	rows := history()
	if len(rows) != 1 {
		t.Fatalf("closed order rows=%d, want 1", len(rows))
	}
	row := rows[0]
	if row.Symbol != "BTC" || row.Strategy != "legacy" || row.EnterTag != "remote" || row.Status != ormo.InOutStatusFullExit ||
		row.ExitAt <= 0 || row.Enter == nil || row.Exit == nil || row.Enter.Filled != 1 || row.Exit.Filled != 1 ||
		row.Enter.Average != 100 || row.Exit.Average != 100 || row.Enter.TaskID != taskID || row.Exit.TaskID != taskID ||
		row.GetInfoString(ormo.OdInfoArchiveKey) == "" {
		t.Fatalf("archived row lost fields: %+v enter=%+v exit=%+v info=%v", row.IOrder, row.Enter, row.Exit, row.Info)
	}
	// The row has its own database identity; the projection keeps the facade ID.
	if row.ID == od.ID {
		t.Fatalf("archived row reused the facade order ID %d", od.ID)
	}
	if err := f.manager.UpdateByDataSeries(nil, &orm.DataSeries{Sid: 1, TimeFrame: "ws"}); err != nil {
		t.Fatal(err)
	}
	if e := f.manager.CleanUp(); e != nil {
		t.Fatal(e)
	}
	restarted, e2 := biz.NewHubOrderMgr(f.rt.BizDeps(), f.bridge, nil)
	if e2 != nil {
		t.Fatal(e2)
	}
	defer restarted.CleanUp()
	if err := restarted.UpdateByDataSeries(nil, &orm.DataSeries{Sid: 1, TimeFrame: "ws"}); err != nil {
		t.Fatal(err)
	}
	if rows = history(); len(rows) != 1 {
		t.Fatalf("closed order archived %d times after restart", len(rows))
	}
}
