package ormo

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	botexg "github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
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

func newFilledTestOrder() *InOutOrder {
	order := newTestOrder()
	order.Status = InOutStatusFullEnter
	order.Enter.Average = 100
	order.Enter.Filled = 1
	return order
}

type clientIDExchangeStub struct {
	banexg.BanExchange
	id string
}

type runtimeOrderExchangeStub struct {
	clientIDExchangeStub
	feeCalls int
}

func (s *runtimeOrderExchangeStub) CalculateFee(string, string, string, float64, float64, bool, map[string]interface{}) (*banexg.Fee, *errs.Error) {
	s.feeCalls++
	return &banexg.Fee{Currency: "USDT", Cost: 1, QuoteCost: 1}, nil
}

func (s *clientIDExchangeStub) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: s.id}
}

func (s *clientIDExchangeStub) BuildClientOrderID(namespace string, orderID int64, clientID string, _ bool) string {
	if s.id == "okx" {
		return fmt.Sprintf("abcdef%012d0000", orderID)
	}
	return fmt.Sprintf("%s_%d_%s", namespace, orderID, clientID)
}

func TestClientIDDelegatesFormatToExchangeBoundary(t *testing.T) {
	oldName, oldExgName, oldDefault := config.Name, core.ExgName, botexg.Default
	t.Cleanup(func() {
		config.Name, core.ExgName, botexg.Default = oldName, oldExgName, oldDefault
	})
	config.Name = "bot"
	order := newTestOrder()
	order.ID = 42

	botexg.Default = &clientIDExchangeStub{id: "binance"}
	if got := order.ClientId(false); got != "bot_42_" {
		t.Fatalf("standard client ID = %q, want bot_42_", got)
	}

	botexg.Default = &clientIDExchangeStub{id: "okx"}
	got := order.ClientId(false)
	if len(got) != 22 || strings.Contains(got, "_") {
		t.Fatalf("compact client ID = %q, want 22 alphanumeric characters", got)
	}

	botexg.Default = nil
	core.ExgName = "okx"
	got = order.ClientId(false)
	if len(got) != 22 || strings.Contains(got, "_") {
		t.Fatalf("legacy compact client ID = %q, want 22 alphanumeric characters", got)
	}
}

func TestExplicitOrderUsesBoundRuntimeDependencies(t *testing.T) {
	oldDefault, oldName, oldExgName, oldSimCount := botexg.Default, config.Name, core.ExgName, core.NewNumInSim
	t.Cleanup(func() {
		botexg.Default, config.Name, core.ExgName, core.NewNumInSim = oldDefault, oldName, oldExgName, oldSimCount
	})
	botexg.Default = nil
	config.Name, core.ExgName, core.NewNumInSim = "legacy", "legacy-exchange", 73

	coreState, err := core.NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer coreState.Close()
	coreState.SetRunMode(core.RunModeBackTest)
	coreState.BeginSimOrderMatch()
	defer coreState.EndSimOrderMatch()
	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(100_000)
	prices := com.NewPriceState("runtime-exchange")
	prices.SetPriceAt(clock.TimeMS(), "BTC/USDT", 111, 109)
	exchange := &runtimeOrderExchangeStub{clientIDExchangeStub: clientIDExchangeStub{id: "runtime-exchange"}}
	state := NewOrderState()
	state.BindCore(coreState)
	state.BindRuntime(clock, prices, exchange, &config.Config{
		Name:     "runtime-name",
		Exchange: &config.ExchangeConfig{Name: "runtime-exchange"},
	})
	order := newFilledTestOrder()
	order.EnterAt = 0
	order.BindState(state)

	if err := order.UpdateFee(100, true); err != nil {
		t.Fatal(err)
	}
	if exchange.feeCalls != 1 {
		t.Fatalf("runtime exchange fee calls = %d, want 1", exchange.feeCalls)
	}
	if got := order.ClientId(false); got != "runtime-name_0_" {
		t.Fatalf("runtime client ID = %q, want runtime-name_0_", got)
	}
	if !order.CanClose() {
		t.Fatal("runtime clock was not used by CanClose")
	}
	order.SetExit(0, "runtime-exit", banexg.OdTypeMarket, 0)
	if got := coreState.NewSimOrderCount(); got != 1 {
		t.Fatalf("runtime simulation counter = %d, want 1", got)
	}
	if core.NewNumInSim != 73 {
		t.Fatalf("legacy simulation counter changed to %d", core.NewNumInSim)
	}

	clock.SetTimeMS(100_001)
	if err := order.LocalExit(0, "runtime-local-exit", 0, "", ""); err != nil {
		t.Fatal(err)
	}
	if got := order.Exit.Price; got != 110 {
		t.Fatalf("runtime price = %v, want 110", got)
	}
}

func enableStrictHistoricalOrderMetricsTest(t *testing.T) {
	t.Helper()
	originalMode, originalData, originalCoverage := core.BackTestMode, config.Data, config.HistoricalCoverage
	t.Cleanup(func() {
		core.BackTestMode, config.Data, config.HistoricalCoverage = originalMode, originalData, originalCoverage
	})
	core.BackTestMode = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true
	config.Data.BTLegacyOrderMetrics = true
	config.HistoricalCoverage = &config.HistoricalCoverageConfig{
		BaselineEndMS: 2,
		Bars: map[string]map[string][]config.HistoricalCoverageRange{
			"BTC/USDT:USDT": {"1h": {{StartMS: 0, StopMS: 2}}},
		},
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

func TestTriggerStateClientIDSurvivesDecodeAndClone(t *testing.T) {
	state := decodeTriggerState(map[string]interface{}{
		"price":     float64(88),
		"order_id":  "trigger-order",
		"client_id": "ban_138_428_",
	})
	if state == nil || state.ClientId != "ban_138_428_" {
		t.Fatalf("decoded trigger client ID mismatch: %+v", state)
	}
	clone := state.Clone()
	if clone.ClientId != state.ClientId || clone.OrderId != state.OrderId {
		t.Fatalf("cloned trigger identity mismatch: %+v", clone)
	}
}

func TestUpdateProfitsPreservesLegacyDrawdownRate(t *testing.T) {
	enableStrictHistoricalOrderMetricsTest(t)

	legacy := newFilledTestOrder()
	legacy.UpdateProfits(110)
	legacy.UpdateProfits(95)
	if math.Abs(legacy.MaxDrawDown-1.5) > 1e-12 {
		t.Fatalf("legacy max drawdown = %v, want 1.5", legacy.MaxDrawDown)
	}

	continuousLoss := newFilledTestOrder()
	continuousLoss.UpdateProfits(95)
	if math.Abs(continuousLoss.MaxDrawDown-0.05) > 1e-12 {
		t.Fatalf("legacy loss drawdown = %v, want 0.05", continuousLoss.MaxDrawDown)
	}

	config.Data.BTLegacyOrderMetrics = false
	current := newFilledTestOrder()
	current.UpdateProfits(110)
	current.UpdateProfits(95)
	if current.MaxDrawDown != -5 {
		t.Fatalf("current max drawdown = %v, want -5", current.MaxDrawDown)
	}
}

func TestUpdateProfitsRejectsLegacyMetricsOutsideStrictHistoricalReplay(t *testing.T) {
	enableStrictHistoricalOrderMetricsTest(t)
	strictCoverage := config.HistoricalCoverage
	for _, test := range []struct {
		name   string
		change func()
	}{
		{name: "non-strict", change: func() { config.Data.BTStrict = false }},
		{name: "download-enabled", change: func() { config.Data.BTNoKlineDownload = false }},
		{name: "missing coverage", change: func() { config.HistoricalCoverage = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			core.BackTestMode = true
			config.Data = config.Config{BTLegacyOrderMetrics: true, BTStrict: true, BTNoKlineDownload: true}
			config.HistoricalCoverage = strictCoverage
			test.change()
			order := newFilledTestOrder()
			order.UpdateProfits(110)
			order.UpdateProfits(95)
			if order.MaxDrawDown != -5 {
				t.Fatalf("current max drawdown = %v, want -5", order.MaxDrawDown)
			}
		})
	}
}
