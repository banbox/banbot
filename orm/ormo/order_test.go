package ormo

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
)

var testDBDir string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "banbot-ormo-test-")
	if err != nil {
		panic(err)
	}
	testDBDir = dir
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func newTestOrder() *InOutOrder {
	return &InOutOrder{
		IOrder: &IOrder{
			TaskID:    1,
			Symbol:    "BTC/USDT",
			Timeframe: "1m",
			Status:    InOutStatusInit,
			EnterTag:  "test-enter",
			InitPrice: 100,
			QuoteCost: 100,
			Leverage:  1,
			Strategy:  "test",
			Info:      `{"LocalTrigger":1}`,
		},
		Enter: &ExOrder{
			TaskID:    1,
			Symbol:    "BTC/USDT",
			Enter:     true,
			OrderType: "limit",
			Side:      "buy",
			Price:     100,
			Amount:    1,
			Status:    OdStatusInit,
		},
		Info:       map[string]interface{}{"LocalTrigger": float64(1)},
		DirtyMain:  true,
		DirtyEnter: true,
	}
}

func useTempTradeDB(t *testing.T) {
	t.Helper()
	orm.SetDbPath(orm.DbTrades, filepath.Join(testDBDir, t.Name()+".db"))
}

func initApp() *errs.Error {
	var args config.CmdArgs
	return config.LoadConfig(&args)
}

func TestGetOrders(t *testing.T) {
	err := initApp()
	if err != nil {
		panic(err)
	}
	useTempTradeDB(t)
	sess, conn, err := Conn(orm.DbTrades, true)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	sess.GetOrders(GetOrdersArgs{})
}

func TestSaveToDbRestoresIDsWhenEnterInsertFails(t *testing.T) {
	useTempTradeDB(t)
	_, conn, err := Conn(orm.DbTrades, true)
	if err != nil {
		t.Fatalf("open trade database: %v", err)
	}
	_, errRaw := conn.ExecContext(context.Background(), `
		CREATE TRIGGER fail_exorder_insert
		BEFORE INSERT ON exorder
		BEGIN
			SELECT RAISE(ABORT, 'forced exorder insert failure');
		END`)
	conn.Close()
	if errRaw != nil {
		t.Fatalf("create failure trigger: %v", errRaw)
	}

	od := newTestOrder()
	od.DirtyInfo = true
	if err := od.saveToDb(); err == nil {
		t.Fatal("expected forced enter insert failure")
	}
	if od.ID != 0 || od.Enter.ID != 0 || od.Enter.InoutID != 0 {
		t.Fatalf("failed insert retained generated IDs: order=%d enter=%d inout=%d", od.ID, od.Enter.ID, od.Enter.InoutID)
	}
	if !od.DirtyInfo || !od.DirtyMain || !od.DirtyEnter {
		t.Fatalf("failed insert must remain retryable, dirty info=%v main=%v enter=%v", od.DirtyInfo, od.DirtyMain, od.DirtyEnter)
	}

	_, conn, err = Conn(orm.DbTrades, true)
	if err != nil {
		t.Fatalf("reopen trade database: %v", err)
	}
	defer conn.Close()
	var count int
	if errRaw = conn.QueryRowContext(context.Background(), "select count(*) from iorder").Scan(&count); errRaw != nil {
		t.Fatalf("count persisted main orders: %v", errRaw)
	}
	if count != 0 {
		t.Fatalf("main insert escaped rolled-back transaction: count=%d", count)
	}
}

func TestSaveToDbPersistsRemovalOfLastInfoValue(t *testing.T) {
	useTempTradeDB(t)
	od := newTestOrder()
	if err := od.saveToDb(); err != nil {
		t.Fatalf("save initial order: %v", err)
	}

	od.SetInfo("LocalTrigger", nil)
	if err := od.saveToDb(); err != nil {
		t.Fatalf("save removed info: %v", err)
	}

	sess, conn, err := Conn(orm.DbTrades, true)
	if err != nil {
		t.Fatalf("open trade database: %v", err)
	}
	defer conn.Close()
	orders, err := sess.GetOrders(GetOrdersArgs{TaskID: od.TaskID})
	if err != nil {
		t.Fatalf("reload order: %v", err)
	}
	if len(orders) != 1 {
		t.Fatalf("expected one reloaded order, got %d", len(orders))
	}
	got := orders[0]
	if got.IOrder.Info != "" {
		t.Fatalf("expected persisted info text to be empty, got %q", got.IOrder.Info)
	}
	if _, exists := got.Info["LocalTrigger"]; exists {
		t.Fatal("removed LocalTrigger was restored after database round trip")
	}
}

func TestSaveToDbRollsBackMainWhenEnterUpdateFails(t *testing.T) {
	useTempTradeDB(t)
	od := newTestOrder()
	if err := od.saveToDb(); err != nil {
		t.Fatalf("save initial order: %v", err)
	}

	_, conn, err := Conn(orm.DbTrades, true)
	if err != nil {
		t.Fatalf("open trade database: %v", err)
	}
	_, errRaw := conn.ExecContext(context.Background(), `
		CREATE TRIGGER fail_exorder_update
		BEFORE UPDATE ON exorder
		BEGIN
			SELECT RAISE(ABORT, 'forced exorder update failure');
		END`)
	conn.Close()
	if errRaw != nil {
		t.Fatalf("create failure trigger: %v", errRaw)
	}

	od.EnterTag = "changed-enter"
	od.Enter.Price = 123
	od.DirtyMain = true
	od.DirtyEnter = true
	if err := od.saveToDb(); err == nil {
		t.Fatal("expected forced enter update failure")
	}
	if !od.DirtyMain || !od.DirtyEnter {
		t.Fatalf("failed save must remain retryable, dirty main=%v enter=%v", od.DirtyMain, od.DirtyEnter)
	}

	_, conn, err = Conn(orm.DbTrades, true)
	if err != nil {
		t.Fatalf("reopen trade database: %v", err)
	}
	defer conn.Close()
	var enterTag string
	if errRaw = conn.QueryRowContext(context.Background(), "select enter_tag from iorder where id = ?", od.ID).Scan(&enterTag); errRaw != nil {
		t.Fatalf("read persisted main order: %v", errRaw)
	}
	if enterTag != "test-enter" {
		t.Fatalf("main update escaped rolled-back transaction: got %q", enterTag)
	}
	var price float64
	if errRaw = conn.QueryRowContext(context.Background(), "select price from exorder where id = ?", od.Enter.ID).Scan(&price); errRaw != nil {
		t.Fatalf("read persisted enter order: %v", errRaw)
	}
	if price != 100 {
		t.Fatalf("enter price changed despite rollback: got %v", price)
	}
}
