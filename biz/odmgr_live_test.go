package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/bybit"
	"github.com/banbox/banexg/errs"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "banbot-biz-test-")
	if err != nil {
		panic(err)
	}
	orm.SetDbPath(orm.DbTrades, filepath.Join(dir, "trades.db"))
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type issue138Exchange struct {
	banexg.BanExchange
	cancelOrder     func(id, symbol string, params map[string]interface{}) (*banexg.Order, *errs.Error)
	fetchOrder      func(symbol, id string, params map[string]interface{}) (*banexg.Order, *errs.Error)
	fetchOrders     func(symbol string, since int64, limit int, params map[string]interface{}) ([]*banexg.Order, *errs.Error)
	fetchPositions  func(symbols []string, params map[string]interface{}) ([]*banexg.Position, *errs.Error)
	createOrder     func(symbol, odType, side string, amount, price float64, params map[string]interface{}) (*banexg.Order, *errs.Error)
	fetchOpenOrders func(symbol string, since int64, limit int, params map[string]interface{}) ([]*banexg.Order, *errs.Error)
	fetchTickers    func(symbols []string, params map[string]interface{}) ([]*banexg.Ticker, *errs.Error)
}

func (e *issue138Exchange) CancelOrder(id, symbol string, params map[string]interface{}) (*banexg.Order, *errs.Error) {
	return e.cancelOrder(id, symbol, params)
}

func (e *issue138Exchange) FetchOrder(symbol, id string, params map[string]interface{}) (*banexg.Order, *errs.Error) {
	return e.fetchOrder(symbol, id, params)
}

func (e *issue138Exchange) FetchOrders(symbol string, since int64, limit int, params map[string]interface{}) ([]*banexg.Order, *errs.Error) {
	return e.fetchOrders(symbol, since, limit, params)
}

func (e *issue138Exchange) FetchAccountPositions(symbols []string, params map[string]interface{}) ([]*banexg.Position, *errs.Error) {
	if e.fetchPositions == nil {
		return nil, nil
	}
	return e.fetchPositions(symbols, params)
}

func (e *issue138Exchange) CreateOrder(symbol, odType, side string, amount, price float64, params map[string]interface{}) (*banexg.Order, *errs.Error) {
	return e.createOrder(symbol, odType, side, amount, price, params)
}

func (e *issue138Exchange) FetchOpenOrders(symbol string, since int64, limit int, params map[string]interface{}) ([]*banexg.Order, *errs.Error) {
	if e.fetchOpenOrders == nil {
		return nil, nil
	}
	return e.fetchOpenOrders(symbol, since, limit, params)
}

func (e *issue138Exchange) FetchTickers(symbols []string, params map[string]interface{}) ([]*banexg.Ticker, *errs.Error) {
	return e.fetchTickers(symbols, params)
}

func (e *issue138Exchange) CalculateFee(string, string, string, float64, float64, bool, map[string]interface{}) (*banexg.Fee, *errs.Error) {
	return &banexg.Fee{}, nil
}

func issue138Err(code int) *errs.Error {
	return errs.NewMsg(code, "exchange error")
}

func issue138Order(id string) *ormo.InOutOrder {
	return &ormo.InOutOrder{
		IOrder: &ormo.IOrder{
			ID:        138,
			Symbol:    "ISSUE138/USDT:USDT",
			Status:    ormo.InOutStatusInit,
			InitPrice: 100,
			EnterAt:   100,
		},
		Enter: &ormo.ExOrder{
			Enter:     true,
			OrderType: banexg.OdTypeLimit,
			OrderID:   id,
			Side:      banexg.OdSideBuy,
			Price:     100,
			Amount:    2,
			Status:    ormo.OdStatusInit,
			UpdateAt:  100,
		},
	}
}

func withIssue138Exchange(t *testing.T, exchange banexg.BanExchange) {
	t.Helper()
	oldExchange := exg.Default
	oldLiveMode := core.LiveMode
	oldEnvReal := core.EnvReal
	exg.Default = exchange
	core.LiveMode = false
	core.EnvReal = false
	t.Cleanup(func() {
		exg.Default = oldExchange
		core.LiveMode = oldLiveMode
		core.EnvReal = oldEnvReal
	})
}

func newIssue163BybitAdapter(t *testing.T, handler http.HandlerFunc) *bybit.Bybit {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	exchange, err := bybit.New(nil)
	if err != nil {
		t.Fatalf("create Bybit adapter: %v", err)
	}
	market := &banexg.Market{
		ID: "BTCUSDT", Symbol: "BTC/USDT:USDT", Base: "BTC", Quote: "USDT", Settle: "USDT",
		Type: banexg.MarketLinear, Linear: true, Contract: true, Swap: true, Active: true,
		ContractSize: 1,
		Precision: &banexg.Precision{
			Price: 0.1, Amount: 0.001,
			ModePrice: banexg.PrecModeTickSize, ModeAmount: banexg.PrecModeTickSize,
		},
	}
	exchange.Markets = banexg.MarketMap{market.Symbol: market}
	exchange.MarketsById = banexg.MarketArrMap{market.ID: {market}}
	exchange.MarketType = banexg.MarketLinear
	exchange.Accounts = map[string]*banexg.Account{
		"issue163-bybit-create": {
			Name:  "issue163-bybit-create",
			Creds: &banexg.Credential{ApiKey: "test-key", Secret: "test-secret"},
		},
	}
	exchange.DefAccName = "issue163-bybit-create"
	exchange.EnableRateLimit = banexg.BoolFalse
	exchange.SetOnHost(func(string) string { return server.URL })
	return exchange
}

func withIssue163BybitRuntime(t *testing.T, exchange *bybit.Bybit) {
	t.Helper()
	withIssue138Exchange(t, exchange)
	oldExgName, oldMarket, oldContract := core.ExgName, core.Market, core.IsContract
	oldStake := append([]string(nil), config.StakeCurrency...)
	core.ExgName, core.Market, core.IsContract = "bybit", banexg.MarketLinear, true
	config.StakeCurrency = []string{"USDT"}
	t.Cleanup(func() {
		core.ExgName, core.Market, core.IsContract = oldExgName, oldMarket, oldContract
		config.StakeCurrency = oldStake
	})
}

func TestApplyHisOrderDoesNotMisattributeClosedExitToOpenSibling(t *testing.T) {
	symbol := "ISSUE162/USDT:USDT"
	withIssue138Exchange(t, &issue138Exchange{})

	closed := issue138Order("closed-entry")
	closed.ID = 1621
	closed.Symbol = symbol
	closed.Status = ormo.InOutStatusFullExit
	closed.Enter.Symbol = symbol
	closed.Enter.Filled = 1
	closed.Enter.Average = 100
	closed.Enter.Status = ormo.OdStatusClosed
	closed.Exit = &ormo.ExOrder{
		OrderID: "closed-exit", Symbol: symbol, Filled: 1, Amount: 1,
		Average: 90, Status: ormo.OdStatusClosed,
	}
	sibling := issue138Order("sibling-entry")
	sibling.ID = 1622
	sibling.Symbol = symbol
	sibling.Status = ormo.InOutStatusFullEnter
	sibling.Enter.Symbol = symbol
	sibling.Enter.Filled = 1
	sibling.Enter.Average = 100
	sibling.Enter.Status = ormo.OdStatusClosed
	sibling.Profit = 7
	openOds := map[int64]*ormo.InOutOrder{sibling.ID: sibling}
	knownOds, exIDMap := indexHistoricalOrders(symbol, []*ormo.InOutOrder{closed}, openOds)

	mgr := newLiveOrderMgr("issue162", func(*ormo.InOutOrder, bool) {})
	history := &banexg.Order{
		ID: "closed-exit", Symbol: symbol, PositionSide: banexg.PosSideLong, Side: banexg.OdSideSell,
		Type: banexg.OdTypeMarket, Status: banexg.OdStatusFilled, Amount: 1, Filled: 1, Average: 90,
		Timestamp: 200,
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if err := mgr.applyHisOrder(openOds, knownOds, exIDMap, history, ""); err != nil {
			t.Fatalf("apply historical order attempt %d: %v", attempt, err)
		}
	}
	if sibling.Exit != nil || sibling.Status != ormo.InOutStatusFullEnter || sibling.Profit != 7 {
		t.Fatalf("closed exit was applied to open sibling: status=%d profit=%v exit=%+v",
			sibling.Status, sibling.Profit, sibling.Exit)
	}

	legacySibling := sibling.Clone()
	remaining, _, _ := mgr.tryFillExit(legacySibling, history.Filled, history.Average, history.Timestamp,
		history.ID, history.Type, "", 0, 0)
	if remaining != 0 || legacySibling.Exit == nil || legacySibling.Status != ormo.InOutStatusFullExit {
		t.Fatal("legacy unmatched-exit fallback no longer demonstrates the reported sibling mutation")
	}
}

func TestHistoricalOrderIndexPrecedenceAndAmbiguity(t *testing.T) {
	pair := "ISSUE162/USDT:USDT"
	makeOrder := func(id int64, exID string) *ormo.InOutOrder {
		od := issue138Order(exID)
		od.ID = id
		od.Symbol = pair
		od.Enter.Symbol = pair
		return od
	}
	closedA := makeOrder(1623, "shared-id")
	closedB := makeOrder(1624, "shared-id")
	_, exIDs := indexHistoricalOrders(pair, []*ormo.InOutOrder{closedA, closedB}, nil)
	if od, exists := exIDs["shared-id"]; !exists || od != nil {
		t.Fatalf("duplicate closed exchange ID must be explicitly ambiguous: exists=%v order=%+v", exists, od)
	}

	open := makeOrder(1625, "shared-id")
	_, exIDs = indexHistoricalOrders(pair, []*ormo.InOutOrder{closedA, closedB}, map[int64]*ormo.InOutOrder{open.ID: open})
	if exIDs["shared-id"] != open {
		t.Fatal("unique open order did not take precedence over closed exchange IDs")
	}

	openB := makeOrder(1626, "shared-id")
	_, exIDs = indexHistoricalOrders(pair, nil, map[int64]*ormo.InOutOrder{open.ID: open, openB.ID: openB})
	if od, exists := exIDs["shared-id"]; !exists || od != nil {
		t.Fatalf("duplicate open exchange ID must be explicitly ambiguous: exists=%v order=%+v", exists, od)
	}
}

func TestAmbiguousHistoricalOrderCannotMutateSibling(t *testing.T) {
	pair := "ISSUE162/USDT:USDT"
	closedA := issue138Order("duplicate-exit")
	closedA.ID, closedA.Symbol, closedA.Enter.Symbol = 1627, pair, pair
	closedA.Exit = &ormo.ExOrder{OrderID: "duplicate-exit", Symbol: pair}
	closedB := closedA.Clone()
	closedB.ID = 1628
	sibling := issue138Order("sibling-entry")
	sibling.ID, sibling.Symbol, sibling.Enter.Symbol = 1629, pair, pair
	sibling.Status = ormo.InOutStatusFullEnter
	sibling.Enter.Status = ormo.OdStatusClosed
	sibling.Enter.Filled = 1
	openOds := map[int64]*ormo.InOutOrder{sibling.ID: sibling}
	knownOds, exIDs := indexHistoricalOrders(pair, []*ormo.InOutOrder{closedA, closedB}, openOds)

	err := newLiveOrderMgr("issue162-ambiguous", func(*ormo.InOutOrder, bool) {}).applyHisOrder(
		openOds, knownOds, exIDs, &banexg.Order{
			ID: "duplicate-exit", Symbol: pair, PositionSide: banexg.PosSideLong, Side: banexg.OdSideSell,
			Type: banexg.OdTypeMarket, Status: banexg.OdStatusFilled, Filled: 1, Amount: 1, Average: 90,
		}, "")
	if err != nil {
		t.Fatalf("apply ambiguous historical order: %v", err)
	}
	if sibling.Exit != nil || sibling.Status != ormo.InOutStatusFullEnter {
		t.Fatalf("ambiguous history mutated sibling: status=%d exit=%+v", sibling.Status, sibling.Exit)
	}
}

func TestSyncPairOrdersRecognizesRecentClosedOrder(t *testing.T) {
	pair := "ISSUE162/USDT:USDT"
	since := int64(1_700_000_000_000)
	fetchCalls := 0
	withIssue138Exchange(t, &issue138Exchange{
		fetchOrders: func(symbol string, gotSince int64, limit int, params map[string]interface{}) ([]*banexg.Order, *errs.Error) {
			fetchCalls++
			if symbol != pair || gotSince != since || limit != 300 ||
				params[banexg.ParamDirection] != "endToStart" || params[banexg.ParamLoopIntv] != int64(7*24*time.Hour/time.Millisecond) {
				t.Fatalf("unexpected FetchOrders request: symbol=%s since=%d limit=%d params=%v", symbol, gotSince, limit, params)
			}
			return []*banexg.Order{{
				ID: "closed-exit", Symbol: pair, PositionSide: banexg.PosSideLong, Side: banexg.OdSideSell,
				Type: banexg.OdTypeMarket, Status: banexg.OdStatusFilled, Amount: 1, Filled: 1,
				Average: 90, Timestamp: since + 1,
			}}, nil
		},
	})
	closed := issue138Order("closed-entry")
	closed.ID, closed.Symbol, closed.Enter.Symbol = 1630, pair, pair
	closed.Status = ormo.InOutStatusFullExit
	closed.Enter.Status, closed.Enter.Filled, closed.Enter.Average = ormo.OdStatusClosed, 1, 100
	closed.Exit = &ormo.ExOrder{
		OrderID: "closed-exit", Symbol: pair, Amount: 1, Filled: 1, Average: 90, Status: ormo.OdStatusClosed,
	}
	sibling := issue138Order("sibling-entry")
	sibling.ID, sibling.Symbol, sibling.Enter.Symbol = 1631, pair, pair
	sibling.Status = ormo.InOutStatusFullEnter
	sibling.Enter.Status, sibling.Enter.Filled, sibling.Enter.Average = ormo.OdStatusClosed, 1, 100
	sibling.Profit = 11
	openOds := map[int64]*ormo.InOutOrder{sibling.ID: sibling}
	mgr := newLiveOrderMgr("issue162-sync", func(*ormo.InOutOrder, bool) {})

	for attempt := 1; attempt <= 2; attempt++ {
		if err := mgr.syncPairOrders(pair, "", &banexg.Position{Contracts: 1}, nil, since, openOds,
			[]*ormo.InOutOrder{closed}); err != nil {
			t.Fatalf("sync attempt %d: %v", attempt, err)
		}
	}
	if fetchCalls != 2 {
		t.Fatalf("FetchOrders calls = %d, want 2", fetchCalls)
	}
	if sibling.Exit != nil || sibling.Status != ormo.InOutStatusFullEnter || sibling.Profit != 11 {
		t.Fatalf("repeat reconciliation mutated sibling: status=%d profit=%v exit=%+v",
			sibling.Status, sibling.Profit, sibling.Exit)
	}
}

func TestLoadRecentClosedOrdersUsesExchangeHistoryWindow(t *testing.T) {
	now := int64(1_700_000_000_000)
	monthMS := int64(30 * 24 * time.Hour / time.Millisecond)
	since := exchangeOrderHistorySince(now, 0)
	if since != now-monthMS {
		t.Fatalf("history since = %d, want %d", since, now-monthMS)
	}
	if got := exchangeOrderHistorySince(now, now-1000); got != now-1000 {
		t.Fatalf("newer local boundary not preserved: got %d", got)
	}

	sess, conn, err := ormo.Conn(orm.DbTrades, true)
	if err != nil {
		t.Fatalf("open trade repository: %v", err)
	}
	ctx := context.Background()
	task, addErr := sess.AddTask(ctx, ormo.AddTaskParams{Mode: "test", Name: t.Name(), CreateAt: now})
	if addErr != nil {
		t.Fatalf("insert test task: %v", addErr)
	}
	taskID := task.ID
	insertClosed := func(exitAt int64, exID string) int64 {
		id, addErr := sess.AddIOrder(ctx, ormo.AddIOrderParams{
			TaskID: taskID, Symbol: "ISSUE162/USDT:USDT", Timeframe: "1m",
			Status: ormo.InOutStatusFullExit, EnterAt: exitAt - 1000, ExitAt: exitAt,
		})
		if addErr != nil {
			t.Fatalf("insert closed order: %v", addErr)
		}
		for _, sub := range []ormo.AddExOrderParams{
			{TaskID: taskID, InoutID: id, Symbol: "ISSUE162/USDT:USDT", Enter: true,
				OrderID: exID + "-entry", Side: banexg.OdSideBuy, Amount: 1, Filled: 1, Status: ormo.OdStatusClosed},
			{TaskID: taskID, InoutID: id, Symbol: "ISSUE162/USDT:USDT",
				OrderID: exID, Side: banexg.OdSideSell, Amount: 1, Filled: 1, Status: ormo.OdStatusClosed},
		} {
			if _, addErr = sess.AddExOrder(ctx, sub); addErr != nil {
				t.Fatalf("insert exchange sub-order: %v", addErr)
			}
		}
		return id
	}
	recentID := insertClosed(since+1, "recent-exit")
	insertClosed(since-1, "old-exit")
	conn.Close()

	orders, loadErr := loadRecentClosedOrders(taskID, since)
	if loadErr != nil {
		t.Fatalf("load recent closed orders: %v", loadErr)
	}
	if len(orders) != 1 || orders[0].ID != recentID || orders[0].Exit == nil || orders[0].Exit.OrderID != "recent-exit" {
		t.Fatalf("unexpected recent closed orders: %+v", orders)
	}
}

func TestApplyHisOrderMatchesRecentClosedByClientID(t *testing.T) {
	pair := "ISSUE162/USDT:USDT"
	oldName := config.Name
	config.Name = "ban"
	t.Cleanup(func() { config.Name = oldName })
	withIssue138Exchange(t, &issue138Exchange{})

	closed := issue138Order("closed-entry")
	closed.ID, closed.Symbol, closed.Enter.Symbol = 1632, pair, pair
	closed.Status = ormo.InOutStatusFullExit
	closed.Enter.Status, closed.Enter.Filled, closed.Enter.Average = ormo.OdStatusClosed, 1, 100
	sibling := issue138Order("sibling-entry")
	sibling.ID, sibling.Symbol, sibling.Enter.Symbol = 1633, pair, pair
	sibling.Status = ormo.InOutStatusFullEnter
	sibling.Enter.Status, sibling.Enter.Filled = ormo.OdStatusClosed, 1
	openOds := map[int64]*ormo.InOutOrder{sibling.ID: sibling}
	knownOds, exIDs := indexHistoricalOrders(pair, []*ormo.InOutOrder{closed}, openOds)

	err := newLiveOrderMgr("issue162-client", func(*ormo.InOutOrder, bool) {}).applyHisOrder(
		openOds, knownOds, exIDs, &banexg.Order{
			ID: "changed-exchange-id", ClientOrderID: "ban_1632_1_", Symbol: pair,
			PositionSide: banexg.PosSideLong, Side: banexg.OdSideBuy, Type: banexg.OdTypeMarket,
			Status: banexg.OdStatusFilled, Amount: 1, Filled: 1, Average: 100,
		}, "")
	if err != nil {
		t.Fatalf("apply historical client-ID match: %v", err)
	}
	if sibling.Exit != nil || sibling.Status != ormo.InOutStatusFullEnter {
		t.Fatalf("client-ID match mutated sibling: status=%d exit=%+v", sibling.Status, sibling.Exit)
	}
}

func TestApplyHisOrderReconcilesOfflineTriggerToItsOwnOrder(t *testing.T) {
	pair := "ISSUE162/USDT:USDT"
	withIssue138Exchange(t, &issue138Exchange{})
	target := issue138Order("target-entry")
	target.ID, target.Symbol, target.Enter.Symbol = 1634, pair, pair
	target.Status = ormo.InOutStatusFullEnter
	target.Enter.Status, target.Enter.Filled, target.Enter.Average = ormo.OdStatusClosed, 1, 100
	target.SetInfo(ormo.OdInfoStopLoss, &ormo.TriggerState{
		ExitTrigger: &ormo.ExitTrigger{Price: 90}, OrderId: "offline-stop", ClientId: "ban_1634_1_",
	})
	sibling := issue138Order("sibling-entry")
	sibling.ID, sibling.Symbol, sibling.Enter.Symbol = 1635, pair, pair
	sibling.Status = ormo.InOutStatusFullEnter
	sibling.Enter.Status, sibling.Enter.Filled, sibling.Enter.Average = ormo.OdStatusClosed, 1, 100
	sibling.Profit = 13
	openOds := map[int64]*ormo.InOutOrder{target.ID: target, sibling.ID: sibling}
	knownOds, exIDs := indexHistoricalOrders(pair, nil, openOds)
	if exIDs["offline-stop"] != target {
		t.Fatal("persisted trigger exchange ID was not indexed")
	}

	err := newLiveOrderMgr("issue162-trigger", func(*ormo.InOutOrder, bool) {}).applyHisOrder(
		openOds, knownOds, exIDs, &banexg.Order{
			ID: "offline-stop", ClientOrderID: "ban_1634_1_", Symbol: pair,
			PositionSide: banexg.PosSideLong, Side: banexg.OdSideSell, Type: banexg.OdTypeStopMarket,
			Status: banexg.OdStatusFilled, Amount: 1, Filled: 1, Average: 90, Timestamp: 200,
		}, "")
	if err != nil {
		t.Fatalf("apply offline trigger fill: %v", err)
	}
	if target.Exit == nil || target.Exit.OrderID != "offline-stop" || target.ExitTag != core.ExitTagStopLoss ||
		target.Status != ormo.InOutStatusFullExit || target.Profit != -10 {
		t.Fatalf("offline trigger not applied to target: status=%d tag=%s profit=%v exit=%+v",
			target.Status, target.ExitTag, target.Profit, target.Exit)
	}
	if stop := target.GetStopLoss(); stop == nil || stop.OrderId != "" || stop.ClientId != "" {
		t.Fatalf("filled trigger identity was not cleared: %+v", stop)
	}
	if sibling.Exit != nil || sibling.Status != ormo.InOutStatusFullEnter || sibling.Profit != 13 {
		t.Fatalf("offline trigger mutated sibling: status=%d profit=%v exit=%+v",
			sibling.Status, sibling.Profit, sibling.Exit)
	}
}

func TestApplyHisOrderInstallsMissingFullEnterTriggers(t *testing.T) {
	created := make([]string, 0, 2)
	exchange := &issue138Exchange{
		createOrder: func(symbol, odType, side string, amount, price float64, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			trigger := ""
			if _, ok := params[banexg.ParamStopLossPrice]; ok {
				trigger = ormo.OdInfoStopLoss
			} else if _, ok := params[banexg.ParamTakeProfitPrice]; ok {
				trigger = ormo.OdInfoTakeProfit
			}
			created = append(created, trigger)
			return &banexg.Order{ID: trigger + "-order", Symbol: symbol, Status: banexg.OdStatusOpen}, nil
		},
	}
	withIssue138Exchange(t, exchange)

	od := issue138Order("offline-entry")
	od.ID = 1631
	od.Status = ormo.InOutStatusPartEnter
	od.Enter.Filled = 1
	od.SetInfo(ormo.OdInfoStopLoss, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: 90}})
	od.SetInfo(ormo.OdInfoTakeProfit, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: 110}})
	openOds := map[int64]*ormo.InOutOrder{od.ID: od}
	knownOds, exIDs := indexHistoricalOrders(od.Symbol, nil, openOds)
	mgr := newLiveOrderMgr("issue163-transition", func(*ormo.InOutOrder, bool) {})

	err := mgr.applyHisOrder(openOds, knownOds, exIDs, &banexg.Order{
		ID: od.Enter.OrderID, Symbol: od.Symbol, Side: banexg.OdSideBuy,
		Status: banexg.OdStatusFilled, Amount: 2, Filled: 2, Average: 101, Timestamp: 200,
	}, "")
	if err != nil {
		t.Fatalf("apply historical entry: %v", err)
	}
	mgr.healMissingFullEnterTriggers([]*ormo.InOutOrder{od}, []*banexg.Order{})
	if od.Status != ormo.InOutStatusFullEnter {
		t.Fatalf("order status = %d, want FullEnter", od.Status)
	}
	if len(created) != 2 || od.GetStopLoss().OrderId != ormo.OdInfoStopLoss+"-order" ||
		od.GetTakeProfit().OrderId != ormo.OdInfoTakeProfit+"-order" {
		t.Fatalf("missing restart protections: created=%v sl=%+v tp=%+v", created, od.GetStopLoss(), od.GetTakeProfit())
	}
}

func TestHealMissingFullEnterTriggersRetriesWithoutDuplicates(t *testing.T) {
	createCalls := 0
	clientIDs := make([]string, 0, 2)
	exchange := &issue138Exchange{
		createOrder: func(symbol, odType, side string, amount, price float64, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			createCalls++
			clientID, _ := params[banexg.ParamClientOrderId].(string)
			clientIDs = append(clientIDs, clientID)
			if createCalls == 1 {
				return nil, issue138Err(errs.CodeRunTime)
			}
			return &banexg.Order{ID: "healed-stop", ClientOrderID: clientID, Symbol: symbol, Status: banexg.OdStatusOpen}, nil
		},
	}
	withIssue138Exchange(t, exchange)
	account := "issue163-retry"
	mgr := newLiveOrderMgr(account, func(*ormo.InOutOrder, bool) {})
	oldEnvReal := core.EnvReal
	core.EnvReal = true
	oldMgr, hadMgr := accLiveOdMgrs[account]
	accLiveOdMgrs[account] = mgr
	od := issue138Order("filled-entry")
	od.ID = 1632
	od.Status = ormo.InOutStatusFullEnter
	od.Enter.Filled = 2
	od.Enter.Status = ormo.OdStatusClosed
	od.SetInfo(ormo.OdInfoStopLoss, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: 90}})
	openOds, openLock := ormo.GetOpenODs(account)
	openLock.Lock()
	openOds[od.ID] = od
	openLock.Unlock()
	t.Cleanup(func() {
		openLock.Lock()
		delete(openOds, od.ID)
		openLock.Unlock()
		if hadMgr {
			accLiveOdMgrs[account] = oldMgr
		} else {
			delete(accLiveOdMgrs, account)
		}
		core.EnvReal = oldEnvReal
	})

	verifyAccountTriggerOds(account)
	if createCalls != 1 || od.GetStopLoss().OrderId != "" || od.GetStopLoss().ClientId == "" {
		t.Fatalf("failed attempt was not retained for retry: calls=%d state=%+v", createCalls, od.GetStopLoss())
	}
	verifyAccountTriggerOds(account)
	verifyAccountTriggerOds(account)
	if createCalls != 2 || od.GetStopLoss().OrderId != "healed-stop" {
		t.Fatalf("trigger retry was not idempotent: calls=%d state=%+v", createCalls, od.GetStopLoss())
	}
	if len(clientIDs) != 2 || clientIDs[0] == "" || clientIDs[0] != clientIDs[1] {
		t.Fatalf("retry client IDs = %v, want one stable non-empty ID", clientIDs)
	}
}

func TestHealMissingFullEnterTriggersSkipsIneligibleOrders(t *testing.T) {
	createCalls := 0
	withIssue138Exchange(t, &issue138Exchange{
		createOrder: func(string, string, string, float64, float64, map[string]interface{}) (*banexg.Order, *errs.Error) {
			createCalls++
			return &banexg.Order{ID: "unexpected"}, nil
		},
	})
	mgr := newLiveOrderMgr("issue163-skips", func(*ormo.InOutOrder, bool) {})
	makeOrder := func(id int64, status int64, trigger *ormo.TriggerState) *ormo.InOutOrder {
		od := issue138Order("filled-entry")
		od.ID = id
		od.Status = status
		od.Enter.Filled = 2
		od.Enter.Status = ormo.OdStatusClosed
		od.SetInfo(ormo.OdInfoStopLoss, trigger)
		return od
	}
	existing := makeOrder(1633, ormo.InOutStatusFullEnter, &ormo.TriggerState{
		ExitTrigger: &ormo.ExitTrigger{Price: 90}, OrderId: "existing-stop",
	})
	trailing := makeOrder(1634, ormo.InOutStatusFullEnter, &ormo.TriggerState{
		ExitTrigger: &ormo.ExitTrigger{Price: 90, Tag: core.ExitTagTrailingStop},
	})
	invalid := makeOrder(1635, ormo.InOutStatusFullEnter, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{}})
	exiting := makeOrder(1636, ormo.InOutStatusFullEnter, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: 90}})
	exiting.Exit = &ormo.ExOrder{OrderID: "pending-exit"}
	closed := makeOrder(1637, ormo.InOutStatusFullExit, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: 90}})
	partial := makeOrder(1642, ormo.InOutStatusPartEnter, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: 90}})

	mgr.healMissingFullEnterTriggers([]*ormo.InOutOrder{existing, trailing, invalid, exiting, closed, partial}, nil)
	if createCalls != 0 {
		t.Fatalf("created %d triggers for ineligible orders", createCalls)
	}
}

func TestHealMissingFullEnterTriggersRestoresExistingSide(t *testing.T) {
	oldName := config.Name
	config.Name = "ban"
	t.Cleanup(func() { config.Name = oldName })
	created := make([]string, 0, 1)
	withIssue138Exchange(t, &issue138Exchange{
		createOrder: func(symbol, odType, side string, amount, price float64, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			created = append(created, odType)
			return &banexg.Order{ID: "new-tp", ClientOrderID: params[banexg.ParamClientOrderId].(string), Status: banexg.OdStatusOpen}, nil
		},
	})
	od := issue138Order("filled-entry")
	od.ID = 1638
	od.Status = ormo.InOutStatusFullEnter
	od.Enter.Filled = 2
	od.Enter.Status = ormo.OdStatusClosed
	od.SetInfo(ormo.OdInfoStopLoss, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: 90}})
	od.SetInfo(ormo.OdInfoTakeProfit, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: 110}})
	mgr := newLiveOrderMgr("issue163-one-side", func(*ormo.InOutOrder, bool) {})
	mgr.healMissingFullEnterTriggers([]*ormo.InOutOrder{od}, []*banexg.Order{{
		ID: "existing-sl", ClientOrderID: "ban_1638_7_", Symbol: od.Symbol,
		Type: banexg.OdTypeStopMarket, Side: banexg.OdSideSell, PositionSide: banexg.PosSideLong,
		TriggerPrice: 90, Amount: 2, Status: banexg.OdStatusOpen,
	}})
	if od.GetStopLoss().OrderId != "existing-sl" || od.GetTakeProfit().OrderId != "new-tp" {
		t.Fatalf("unexpected reconciled triggers: sl=%+v tp=%+v", od.GetStopLoss(), od.GetTakeProfit())
	}
	if len(created) != 1 || created[0] != banexg.OdTypeTakeProfitMarket {
		t.Fatalf("created triggers = %v, want one unified take-profit market order", created)
	}
}

func TestVerifyFullEnterTriggersSkipsUncertainSnapshot(t *testing.T) {
	oldExgName, oldMarket := core.ExgName, core.Market
	core.ExgName, core.Market = "binance", banexg.MarketLinear
	t.Cleanup(func() { core.ExgName, core.Market = oldExgName, oldMarket })
	createCalls := 0
	withIssue138Exchange(t, &issue138Exchange{
		fetchOpenOrders: func(_ string, _ int64, _ int, params map[string]interface{}) ([]*banexg.Order, *errs.Error) {
			if params[banexg.ParamFullSnapshot] == true {
				return nil, issue138Err(errs.CodeRunTime)
			}
			t.Fatal("open orders requested without full snapshot")
			return nil, nil
		},
		createOrder: func(string, string, string, float64, float64, map[string]interface{}) (*banexg.Order, *errs.Error) {
			createCalls++
			return &banexg.Order{ID: "unsafe"}, nil
		},
	})
	account := "issue163-query-fail"
	mgr := newLiveOrderMgr(account, func(*ormo.InOutOrder, bool) {})
	oldEnvReal := core.EnvReal
	core.EnvReal = true
	oldMgr, hadMgr := accLiveOdMgrs[account]
	accLiveOdMgrs[account] = mgr
	od := issue138Order("filled-entry")
	od.ID = 1639
	od.Status = ormo.InOutStatusFullEnter
	od.Enter.Filled = 2
	od.Enter.Status = ormo.OdStatusClosed
	od.SetInfo(ormo.OdInfoStopLoss, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: 90}})
	openOds, openLock := ormo.GetOpenODs(account)
	openLock.Lock()
	openOds[od.ID] = od
	openLock.Unlock()
	t.Cleanup(func() {
		openLock.Lock()
		delete(openOds, od.ID)
		openLock.Unlock()
		if hadMgr {
			accLiveOdMgrs[account] = oldMgr
		} else {
			delete(accLiveOdMgrs, account)
		}
		core.EnvReal = oldEnvReal
	})

	verifyAccountTriggerOds(account)
	if createCalls != 0 || od.GetStopLoss().OrderId != "" {
		t.Fatalf("uncertain exchange snapshot created a trigger: calls=%d state=%+v", createCalls, od.GetStopLoss())
	}
}

func TestFetchAccountOpenOrdersScopesCompleteSnapshots(t *testing.T) {
	withIssue138Exchange(t, &issue138Exchange{
		fetchOpenOrders: func(symbol string, since int64, limit int, params map[string]interface{}) ([]*banexg.Order, *errs.Error) {
			account := params[banexg.ParamAccount].(string)
			if symbol != "" || since != 10 || limit != openOrderSnapshotLimit || params[banexg.ParamFullSnapshot] != true {
				t.Fatalf("unexpected snapshot request: symbol=%q since=%d limit=%d params=%v", symbol, since, limit, params)
			}
			return []*banexg.Order{{ID: account + "-snapshot"}}, nil
		},
	})
	for _, account := range []string{"issue163-account-a", "issue163-account-b"} {
		orders, err := fetchAccountOpenOrders(account, 10)
		if err != nil {
			t.Fatalf("fetch %s snapshot: %v", account, err)
		}
		if len(orders) != 1 || orders[0].ID != account+"-snapshot" {
			t.Fatalf("cross-account or incomplete snapshot for %s: %+v", account, orders)
		}
	}
}

func TestEditTriggerOrderUsesBybitConditionalRequest(t *testing.T) {
	tests := []struct {
		name              string
		key               string
		triggerPrice      float64
		limitPrice        float64
		wantOrderType     string
		wantDirection     float64
		forbiddenPriceKey string
	}{
		{"stop market", ormo.OdInfoStopLoss, 90, 0, "Market", 2, "stopLoss"},
		{"take profit limit", ormo.OdInfoTakeProfit, 110, 111, "Limit", 1, "takeProfit"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			requestCh := make(chan map[string]interface{}, 1)
			exchange := newIssue163BybitAdapter(t, func(w http.ResponseWriter, r *http.Request) {
				var body map[string]interface{}
				_ = json.NewDecoder(r.Body).Decode(&body)
				requestCh <- body
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"retCode":0,"retMsg":"OK","result":{"orderId":"bybit-trigger","orderLinkId":"bybit-client"},"retExtInfo":{},"time":1700000000000}`))
			})
			withIssue163BybitRuntime(t, exchange)
			od := issue138Order("filled-entry")
			od.ID = 1645
			od.Symbol = "BTC/USDT:USDT"
			od.Status = ormo.InOutStatusFullEnter
			od.Enter.Symbol = od.Symbol
			od.Enter.Filled = 2
			od.Enter.Average = 100
			od.Enter.Status = ormo.OdStatusClosed
			od.SetInfo(tc.key, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{
				Price: tc.triggerPrice, Limit: tc.limitPrice,
			}})

			mgr := newLiveOrderMgr("issue163-bybit-create", func(*ormo.InOutOrder, bool) {})
			mgr.editTriggerOd(od, tc.key)
			var request map[string]interface{}
			select {
			case request = <-requestCh:
			case <-time.After(time.Second):
				t.Fatal("Bybit create-order request was not sent")
			}
			if request["orderType"] != tc.wantOrderType || request["triggerPrice"] != fmt.Sprint(tc.triggerPrice) ||
				request["triggerDirection"] != tc.wantDirection {
				t.Fatalf("unexpected Bybit conditional request: %#v", request)
			}
			if _, ok := request[tc.forbiddenPriceKey]; ok {
				t.Fatalf("attached %s leaked into conditional request: %#v", tc.forbiddenPriceKey, request)
			}
			if tc.limitPrice > 0 && request["price"] != fmt.Sprint(tc.limitPrice) {
				t.Fatalf("Bybit conditional limit price = %v, want %v", request["price"], tc.limitPrice)
			}
		})
	}
}

func TestHealMissingFullEnterTriggersRejectsWrongClientAndPrice(t *testing.T) {
	oldName := config.Name
	config.Name = "ban"
	t.Cleanup(func() { config.Name = oldName })
	createCalls := 0
	withIssue138Exchange(t, &issue138Exchange{
		createOrder: func(string, string, string, float64, float64, map[string]interface{}) (*banexg.Order, *errs.Error) {
			createCalls++
			return &banexg.Order{ID: "unsafe-new-trigger"}, nil
		},
	})
	od := issue138Order("filled-entry")
	od.ID = 1643
	od.Status = ormo.InOutStatusFullEnter
	od.Enter.Filled = 2
	od.Enter.Status = ormo.OdStatusClosed
	od.SetInfo(ormo.OdInfoStopLoss, &ormo.TriggerState{
		ExitTrigger: &ormo.ExitTrigger{Price: 90}, ClientId: "ban_1643_8_",
	})
	mgr := newLiveOrderMgr("issue163-stale-trigger", func(*ormo.InOutOrder, bool) {})
	mgr.healMissingFullEnterTriggers([]*ormo.InOutOrder{od}, []*banexg.Order{{
		ID: "stale-sl", ClientOrderID: "ban_1643_7_", Symbol: od.Symbol,
		Type: banexg.OdTypeStopMarket, Side: banexg.OdSideSell, PositionSide: banexg.PosSideLong,
		TriggerPrice: 80, Amount: 2, Status: banexg.OdStatusOpen,
	}})
	if createCalls != 0 || od.GetStopLoss().OrderId != "" {
		t.Fatalf("stale trigger was trusted or duplicated: creates=%d state=%+v", createCalls, od.GetStopLoss())
	}
}

func TestHealMissingFullEnterTriggersPrefersPersistedClientID(t *testing.T) {
	oldName := config.Name
	config.Name = "ban"
	t.Cleanup(func() { config.Name = oldName })
	withIssue138Exchange(t, &issue138Exchange{
		createOrder: func(string, string, string, float64, float64, map[string]interface{}) (*banexg.Order, *errs.Error) {
			t.Fatal("exact exchange trigger should be restored")
			return nil, nil
		},
	})
	od := issue138Order("filled-entry")
	od.ID = 1644
	od.Status = ormo.InOutStatusFullEnter
	od.Enter.Filled = 2
	od.Enter.Status = ormo.OdStatusClosed
	od.SetInfo(ormo.OdInfoStopLoss, &ormo.TriggerState{
		ExitTrigger: &ormo.ExitTrigger{Price: 90}, ClientId: "ban_1644_8_",
	})
	mgr := newLiveOrderMgr("issue163-exact-trigger", func(*ormo.InOutOrder, bool) {})
	mgr.healMissingFullEnterTriggers([]*ormo.InOutOrder{od}, []*banexg.Order{
		{
			ID: "stale-sl", ClientOrderID: "ban_1644_7_", Symbol: od.Symbol,
			Type: banexg.OdTypeStopMarket, Side: banexg.OdSideSell, PositionSide: banexg.PosSideLong,
			TriggerPrice: 80, Amount: 2, Status: banexg.OdStatusOpen,
		},
		{
			ID: "exact-sl", ClientOrderID: "ban_1644_8_", Symbol: od.Symbol,
			Type: banexg.OdTypeStopMarket, Side: banexg.OdSideSell, PositionSide: banexg.PosSideLong,
			TriggerPrice: 90, Amount: 2, Status: banexg.OdStatusOpen,
		},
	})
	if od.GetStopLoss().OrderId != "exact-sl" || od.GetStopLoss().ClientId != "ban_1644_8_" {
		t.Fatalf("persisted client ID was not preferred: %+v", od.GetStopLoss())
	}
}

func TestHealMissingFullEnterTriggerRecoversDuplicateClientID(t *testing.T) {
	oldExgName, oldMarket := core.ExgName, core.Market
	core.ExgName, core.Market = "binance", banexg.MarketLinear
	t.Cleanup(func() { core.ExgName, core.Market = oldExgName, oldMarket })
	fetchCalls := 0
	withIssue138Exchange(t, &issue138Exchange{
		createOrder: func(string, string, string, float64, float64, map[string]interface{}) (*banexg.Order, *errs.Error) {
			return nil, issue138Err(errs.CodeDuplicateRequest)
		},
		fetchOrder: func(symbol, id string, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			fetchCalls++
			clientID, _ := params[banexg.ParamClientOrderId].(string)
			if id != "" || clientID == "" || params[banexg.ParamAlgoOrder] != true {
				t.Fatalf("unexpected duplicate lookup: id=%q params=%v", id, params)
			}
			return &banexg.Order{ID: "existing-after-timeout", ClientOrderID: clientID, Status: banexg.OdStatusOpen}, nil
		},
	})
	od := issue138Order("filled-entry")
	od.ID = 1640
	od.Status = ormo.InOutStatusFullEnter
	od.Enter.Filled = 2
	od.Enter.Status = ormo.OdStatusClosed
	od.SetInfo(ormo.OdInfoStopLoss, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: 90}, ClientId: "ban_1640_8_"})
	mgr := newLiveOrderMgr("issue163-duplicate", func(*ormo.InOutOrder, bool) {})
	mgr.healMissingFullEnterTriggers([]*ormo.InOutOrder{od}, []*banexg.Order{})
	if fetchCalls != 1 || od.GetStopLoss().OrderId != "existing-after-timeout" {
		t.Fatalf("duplicate create was not reconciled: fetches=%d state=%+v", fetchCalls, od.GetStopLoss())
	}
}

func TestHealMissingFullEnterTriggersConcurrentRetryIsSingleCreate(t *testing.T) {
	createCalls := 0
	withIssue138Exchange(t, &issue138Exchange{
		createOrder: func(string, string, string, float64, float64, map[string]interface{}) (*banexg.Order, *errs.Error) {
			createCalls++
			return &banexg.Order{ID: "single-trigger", Status: banexg.OdStatusOpen}, nil
		},
	})
	od := issue138Order("filled-entry")
	od.ID = 1641
	od.Status = ormo.InOutStatusFullEnter
	od.Enter.Filled = 2
	od.Enter.Status = ormo.OdStatusClosed
	od.SetInfo(ormo.OdInfoStopLoss, &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: 90}})
	_ = od.Key()
	mgr := newLiveOrderMgr("issue163-race", func(*ormo.InOutOrder, bool) {})
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mgr.healMissingFullEnterTriggers([]*ormo.InOutOrder{od}, []*banexg.Order{})
		}()
	}
	wg.Wait()
	if createCalls != 1 || od.GetStopLoss().OrderId != "single-trigger" {
		t.Fatalf("concurrent heal duplicated trigger: calls=%d state=%+v", createCalls, od.GetStopLoss())
	}
}

func TestParseClient(t *testing.T) {
	config.Name = "big"
	res := getClientOrderId("big_1176_747_")
	fmt.Println(res)
}

func TestHandleMyTradeClaimsTriggeredBinanceExitWithNewOrderID(t *testing.T) {
	withIssue138Exchange(t, &issue138Exchange{})
	oldName := config.Name
	config.Name = "ban"
	t.Cleanup(func() { config.Name = oldName })

	tests := []struct {
		name       string
		tradeType  string
		triggerKey string
		triggerID  string
		exitTag    string
	}{
		{
			name:       "stop loss",
			tradeType:  banexg.OdTypeMarket,
			triggerKey: ormo.OdInfoStopLoss,
			triggerID:  "2000000597523938",
			exitTag:    core.ExitTagStopLoss,
		},
		{
			name:       "take profit",
			tradeType:  banexg.OdTypeMarket,
			triggerKey: ormo.OdInfoTakeProfit,
			triggerID:  "2000000597523939",
			exitTag:    core.ExitTagTakeProfit,
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			od := issue138Order("entry-order")
			od.ID += int64(index)
			od.Status = ormo.InOutStatusFullEnter
			od.Enter.Status = ormo.OdStatusClosed
			od.Enter.Filled = 0.35
			od.Enter.Average = 90
			od.SetInfo(test.triggerKey, &ormo.TriggerState{
				ExitTrigger: &ormo.ExitTrigger{Price: 88.02},
				OrderId:     test.triggerID,
				ClientId:    fmt.Sprintf("ban_%d_428_", od.ID),
			})

			core.PairsMap[od.Symbol] = true
			t.Cleanup(func() { delete(core.PairsMap, od.Symbol) })
			openOds, openLock := ormo.GetOpenODs(config.DefAcc)
			openLock.Lock()
			openOds[od.ID] = od
			openLock.Unlock()
			t.Cleanup(func() {
				openLock.Lock()
				delete(openOds, od.ID)
				openLock.Unlock()
			})

			mgr := newLiveOrderMgr("issue136-"+test.name, func(*ormo.InOutOrder, bool) {})
			actualOrderID := fmt.Sprintf("108710997%d", index+1)
			mgr.handleMyTrade(&banexg.MyTrade{
				Trade: banexg.Trade{
					ID:        fmt.Sprintf("4612254%d", index+4),
					Symbol:    od.Symbol,
					Side:      banexg.OdSideSell,
					Type:      test.tradeType,
					Amount:    0.35,
					Order:     actualOrderID,
					Timestamp: 1773196328529,
				},
				Filled:   0.35,
				ClientID: fmt.Sprintf("ban_%d_428_", od.ID),
				Average:  88.01,
				State:    banexg.OdStatusFilled,
				PosSide:  banexg.PosSideLong,
			})

			if od.Exit == nil {
				t.Fatal("triggered trade did not create an exit sub-order")
			}
			if od.Exit.OrderID != actualOrderID || od.ExitTag != test.exitTag || od.Exit.Filled != 0.35 ||
				od.Status != ormo.InOutStatusFullExit {
				t.Fatalf("triggered exit mismatch: orderID=%q tag=%q filled=%v status=%d",
					od.Exit.OrderID, od.ExitTag, od.Exit.Filled, od.Status)
			}
			if trigger := od.GetExitTrigger(test.triggerKey); trigger == nil || trigger.OrderId != "" || trigger.ClientId != "" {
				t.Fatalf("completed trigger was not cleared: %+v", trigger)
			}
			mgr.lockExgIdMap.Lock()
			mapped := mgr.exgIdMap[od.Symbol+actualOrderID]
			mgr.lockExgIdMap.Unlock()
			if mapped != od {
				t.Fatal("actual exchange order ID was not mapped to the in/out order")
			}
		})
	}
}

func TestMatchTriggerExitTradeRejectsUnrelatedTrade(t *testing.T) {
	oldName := config.Name
	config.Name = "ban"
	t.Cleanup(func() { config.Name = oldName })
	od := issue138Order("entry-order")
	od.Status = ormo.InOutStatusFullEnter
	od.SetInfo(ormo.OdInfoStopLoss, &ormo.TriggerState{
		ExitTrigger: &ormo.ExitTrigger{Price: 88.02},
		OrderId:     "trigger-order",
		ClientId:    "ban_138_1_",
	})

	for _, trade := range []*banexg.MyTrade{
		{
			Trade:    banexg.Trade{Order: "actual-order", Side: banexg.OdSideSell, Type: banexg.OdTypeMarket},
			ClientID: "ban_999_1_",
		},
		{
			Trade:    banexg.Trade{Order: "actual-order", Side: banexg.OdSideBuy, Type: banexg.OdTypeMarket},
			ClientID: "ban_138_1_",
		},
	} {
		if tag, ok := matchTriggerExitTrade(od, trade); ok {
			t.Fatalf("unrelated trigger trade matched as %q: %+v", tag, trade)
		}
	}
}

func TestRestoreInOutOrderKeepsVirtualTriggerWhenMarketDataIsCold(t *testing.T) {
	exchange := &issue138Exchange{}
	withIssue138Exchange(t, exchange)
	oldPutLimitSecs := config.PutLimitSecs
	config.PutLimitSecs = 180
	t.Cleanup(func() { config.PutLimitSecs = oldPutLimitSecs })

	od := issue138Order("")
	od.SetInfo(ormo.OdInfoStopAfter, time.Now().Add(time.Hour).UnixMilli())
	cacheKey := fmt.Sprintf("%s_%d", od.Symbol, 50)
	lockPairVolMap.Lock()
	oldCache, hadCache := pairVolMap[cacheKey]
	pairVolMap[cacheKey] = &PairValItem{ExpireMS: time.Now().Add(time.Hour).UnixMilli()}
	lockPairVolMap.Unlock()
	t.Cleanup(func() {
		lockPairVolMap.Lock()
		if hadCache {
			pairVolMap[cacheKey] = oldCache
		} else {
			delete(pairVolMap, cacheKey)
		}
		lockPairVolMap.Unlock()
	})

	mgr := newLiveOrderMgr("issue138-restore", func(*ormo.InOutOrder, bool) {})
	if err := mgr.restoreInOutOrder(od, nil); err != nil {
		t.Fatalf("restore virtual trigger: %v", err)
	}
	if od.Status != ormo.InOutStatusInit || od.Exit != nil {
		t.Fatalf("virtual trigger was closed during restore: status=%d exit=%v", od.Status, od.Exit)
	}
	triggers, lock := ormo.GetTriggerODs(mgr.Account)
	lock.Lock()
	restored := triggers[od.Symbol][od.ID]
	delete(triggers, od.Symbol)
	lock.Unlock()
	if restored != od {
		t.Fatal("virtual trigger was not re-registered")
	}
	if od.GetInfoInt64(odInfoLocalTrigger) != 1 {
		t.Fatal("legacy virtual trigger was not migrated to a persistent marker")
	}
}

func TestRestoreInOutOrderKeepsMarkedTriggerWithoutStopAfter(t *testing.T) {
	exchange := &issue138Exchange{}
	withIssue138Exchange(t, exchange)
	od := issue138Order("")
	od.SetInfo(odInfoLocalTrigger, int64(1))

	mgr := newLiveOrderMgr("issue138-marked-trigger", func(*ormo.InOutOrder, bool) {})
	if err := mgr.restoreInOutOrder(od, nil); err != nil {
		t.Fatalf("restore marked trigger: %v", err)
	}
	triggers, lock := ormo.GetTriggerODs(mgr.Account)
	lock.Lock()
	restored := triggers[od.Symbol][od.ID]
	delete(triggers, od.Symbol)
	lock.Unlock()
	if restored != od {
		t.Fatal("marked trigger without StopAfter was not restored")
	}
}

func TestRestoreInOutOrderDoesNotGuessUnmarkedLimitIsVirtual(t *testing.T) {
	exchange := &issue138Exchange{}
	withIssue138Exchange(t, exchange)
	od := issue138Order("")

	mgr := newLiveOrderMgr("issue138-unmarked-limit", func(*ormo.InOutOrder, bool) {})
	if err := mgr.restoreInOutOrder(od, nil); err != nil {
		t.Fatalf("restore unmarked limit: %v", err)
	}
	if od.Status < ormo.InOutStatusFullExit {
		t.Fatalf("unmarked limit without compatibility evidence was left open: status=%d", od.Status)
	}
}

func TestRestoreInOutOrderClaimsAcceptedOrderByClientID(t *testing.T) {
	exchange := &issue138Exchange{}
	withIssue138Exchange(t, exchange)
	oldName := config.Name
	config.Name = "issue138bot"
	t.Cleanup(func() { config.Name = oldName })
	od := issue138Order("")
	od.SetInfo(ormo.OdInfoStopAfter, time.Now().Add(time.Hour).UnixMilli())
	exOd := &banexg.Order{
		ID: "accepted-order", ClientOrderID: "issue138bot_138_1_", Symbol: od.Symbol,
		Side: od.Enter.Side, Status: banexg.OdStatusPartFilled, Amount: 2, Filled: 1,
		Average: 101, LastUpdateTimestamp: 200,
	}

	callbackCalls := 0
	mgr := newLiveOrderMgr("issue138-accepted", func(*ormo.InOutOrder, bool) { callbackCalls++ })
	if err := mgr.restoreInOutOrder(od, map[string]*banexg.Order{exOd.ID: exOd}); err != nil {
		t.Fatalf("restore accepted order: %v", err)
	}
	if od.Enter.OrderID != exOd.ID {
		t.Fatalf("accepted exchange order was not claimed: orderID=%q", od.Enter.OrderID)
	}
	if od.Enter.Filled != 1 || od.Enter.Status != ormo.OdStatusPartOK || callbackCalls != 1 {
		t.Fatalf("accepted partial fill was not applied: filled=%v status=%d callbacks=%d",
			od.Enter.Filled, od.Enter.Status, callbackCalls)
	}
	triggers, lock := ormo.GetTriggerODs(mgr.Account)
	lock.Lock()
	_, registered := triggers[od.Symbol][od.ID]
	delete(triggers, od.Symbol)
	lock.Unlock()
	if registered {
		t.Fatal("accepted exchange order was also registered as a local trigger")
	}
}

func TestRestoreInOutOrderRejectsMultipleClientIDMatches(t *testing.T) {
	exchange := &issue138Exchange{}
	withIssue138Exchange(t, exchange)
	oldName := config.Name
	config.Name = "issue138bot"
	t.Cleanup(func() { config.Name = oldName })
	od := issue138Order("")
	orders := map[string]*banexg.Order{
		"accepted-a": {
			ID: "accepted-a", ClientOrderID: "issue138bot_138_1_", Symbol: od.Symbol,
			Side: od.Enter.Side, Status: banexg.OdStatusOpen,
		},
		"accepted-b": {
			ID: "accepted-b", ClientOrderID: "issue138bot_138_2_", Symbol: od.Symbol,
			Side: od.Enter.Side, Status: banexg.OdStatusOpen,
		},
	}

	mgr := newLiveOrderMgr("issue138-multiple-accepted", func(*ormo.InOutOrder, bool) {})
	err := mgr.restoreInOutOrder(od, orders)
	if err == nil {
		t.Fatal("multiple matching exchange orders were claimed nondeterministically")
	}
	if err.Data != restoreAmbiguousExgData {
		t.Fatalf("ambiguous exchange match was not classified as startup-blocking: data=%v", err.Data)
	}
	if od.Enter.OrderID != "" || od.GetInfoInt64(odInfoLocalTrigger) != 0 {
		t.Fatalf("ambiguous exchange matches mutated local order: orderID=%q marker=%d",
			od.Enter.OrderID, od.GetInfoInt64(odInfoLocalTrigger))
	}
}

func TestRestoreInOutOrderDoesNotRegisterMarketOrderAsVirtualTrigger(t *testing.T) {
	exchange := &issue138Exchange{}
	withIssue138Exchange(t, exchange)
	od := issue138Order("")
	od.Enter.OrderType = banexg.OdTypeMarket
	od.Enter.Price = 0

	mgr := newLiveOrderMgr("issue138-restore-market", func(*ormo.InOutOrder, bool) {})
	if err := mgr.restoreInOutOrder(od, nil); err != nil {
		t.Fatalf("restore market entry: %v", err)
	}
	if od.Status < ormo.InOutStatusFullExit {
		t.Fatalf("unsubmitted market entry was left open: status=%d", od.Status)
	}
	triggers, lock := ormo.GetTriggerODs(mgr.Account)
	lock.Lock()
	_, registered := triggers[od.Symbol][od.ID]
	delete(triggers, od.Symbol)
	lock.Unlock()
	if registered {
		t.Fatal("unsubmitted market entry was registered as a virtual trigger")
	}
}

func TestHandleOrderQueueRequeuesMarkedTriggerWhenPriceUnavailable(t *testing.T) {
	exchange := &issue138Exchange{
		fetchTickers: func([]string, map[string]interface{}) ([]*banexg.Ticker, *errs.Error) {
			return nil, issue138Err(-1000)
		},
	}
	withIssue138Exchange(t, exchange)
	oldExgName, oldMarket := core.ExgName, core.Market
	core.ExgName, core.Market = "issue138", "issue138"
	core.LiveMode = true
	t.Cleanup(func() {
		core.ExgName, core.Market = oldExgName, oldMarket
	})
	od := issue138Order("")
	od.SetInfo(odInfoLocalTrigger, int64(1))
	mgr := newLiveOrderMgr("issue138-price-failure", func(*ormo.InOutOrder, bool) {})

	mgr.handleOrderQueue(od, ormo.OdActionEnter)

	triggers, lock := ormo.GetTriggerODs(mgr.Account)
	lock.Lock()
	requeued := triggers[od.Symbol][od.ID]
	delete(triggers, od.Symbol)
	lock.Unlock()
	if requeued != od || od.GetInfoInt64(odInfoLocalTrigger) != 1 {
		t.Fatalf("price failure lost local trigger ownership: requeued=%v marker=%d",
			requeued == od, od.GetInfoInt64(odInfoLocalTrigger))
	}
}

func TestWatchMyTradesRestartsAndStops(t *testing.T) {
	tests := []struct {
		name  string
		first func() (chan *banexg.MyTrade, *errs.Error)
	}{
		{
			name: "subscription error",
			first: func() (chan *banexg.MyTrade, *errs.Error) {
				return nil, issue138Err(errs.CodeWsReadFail)
			},
		},
		{
			name: "closed stream",
			first: func() (chan *banexg.MyTrade, *errs.Error) {
				out := make(chan *banexg.MyTrade)
				close(out)
				return out, nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldCtx := core.Ctx
			ctx, cancel := context.WithCancel(context.Background())
			core.Ctx = ctx
			t.Cleanup(func() {
				cancel()
				core.Ctx = oldCtx
			})

			calls := 0
			mgr := newLiveOrderMgr("watch-retry", func(*ormo.InOutOrder, bool) {})
			done := make(chan struct{})
			go func() {
				mgr.watchMyTradesLoop(func(map[string]interface{}) (chan *banexg.MyTrade, *errs.Error) {
					calls++
					if calls == 1 {
						return test.first()
					}
					cancel()
					return make(chan *banexg.MyTrade), nil
				}, time.Millisecond)
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("WatchMyTrades did not stop after context cancellation")
			}
			if calls != 2 {
				t.Fatalf("WatchMyTrades calls = %d, want 2", calls)
			}
		})
	}
}

func TestWatchMyTradesStopsDuringRetryDelay(t *testing.T) {
	oldCtx := core.Ctx
	ctx, cancel := context.WithCancel(context.Background())
	core.Ctx = ctx
	t.Cleanup(func() {
		cancel()
		core.Ctx = oldCtx
	})

	calls := 0
	mgr := newLiveOrderMgr("watch-stop", func(*ormo.InOutOrder, bool) {})
	done := make(chan struct{})
	go func() {
		mgr.watchMyTradesLoop(func(map[string]interface{}) (chan *banexg.MyTrade, *errs.Error) {
			calls++
			cancel()
			return nil, issue138Err(errs.CodeWsReadFail)
		}, time.Hour)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WatchMyTrades retry delay ignored context cancellation")
	}
	if calls != 1 {
		t.Fatalf("WatchMyTrades calls = %d, want 1", calls)
	}
}

func withIsolatedOrderState(t *testing.T) {
	t.Helper()
	backup := ormo.BackupVars()
	ormo.ResetVars()
	t.Cleanup(func() { ormo.RestoreVars(backup) })
}
func makeEnteredOrder(id int64, symbol, enterID string, amount float64) *ormo.InOutOrder {
	od := issue138Order(enterID)
	od.ID = id
	od.EnterAt = id
	od.Symbol = symbol
	od.Enter.Symbol = symbol
	od.Status = ormo.InOutStatusFullEnter
	od.Enter.Filled = amount
	od.Enter.Amount = amount
	od.Enter.Average = 100
	od.Enter.Status = ormo.OdStatusClosed
	return od
}

func TestSyncLocalOrdersReconcilesSubmittedExitWithOlderCreatedTimestamp(t *testing.T) {
	withIsolatedOrderState(t)
	const account = "exit-reconcile"
	const symbol = "EXITREC/USDT:USDT"
	target := makeEnteredOrder(2101, symbol, "target-entry", 2)
	target.Exit = &ormo.ExOrder{
		OrderID: "target-exit", Symbol: symbol, Side: banexg.OdSideSell,
		OrderType: banexg.OdTypeMarket, Amount: 2, Status: ormo.OdStatusInit, UpdateAt: 200,
	}
	sibling := makeEnteredOrder(2102, symbol, "sibling-entry", 10)
	fetchCalls := 0
	withIssue138Exchange(t, &issue138Exchange{
		fetchOrder: func(gotSymbol, id string, _ map[string]interface{}) (*banexg.Order, *errs.Error) {
			fetchCalls++
			if gotSymbol != symbol || id != "target-exit" {
				t.Fatalf("unexpected FetchOrder(%q, %q)", gotSymbol, id)
			}
			return &banexg.Order{
				ID: id, Symbol: symbol, Side: banexg.OdSideSell, Type: banexg.OdTypeMarket,
				Status: banexg.OdStatusFilled, Amount: 2, Filled: 2, Average: 99,
				Timestamp: 100, LastTradeTimestamp: 101, LastUpdateTimestamp: 102,
			}, nil
		},
		fetchPositions: func([]string, map[string]interface{}) ([]*banexg.Position, *errs.Error) {
			return []*banexg.Position{{Symbol: symbol, Side: banexg.PosSideLong, Contracts: 10}}, nil
		},
	})
	mgr := newLiveOrderMgr(account, func(*ormo.InOutOrder, bool) {})
	openOds, lock := ormo.GetOpenODs(account)
	lock.Lock()
	openOds[target.ID] = target
	openOds[sibling.ID] = sibling
	lock.Unlock()
	t.Cleanup(func() {
		lock.Lock()
		delete(openOds, target.ID)
		delete(openOds, sibling.ID)
		lock.Unlock()
	})

	closed, err := mgr.SyncLocalOrders()
	if err != nil {
		t.Fatalf("SyncLocalOrders: %v", err)
	}
	if fetchCalls != 1 {
		t.Fatalf("FetchOrder calls = %d, want 1", fetchCalls)
	}
	if len(closed) != 0 {
		t.Fatalf("no_match closed orders = %d, want 0", len(closed))
	}
	if target.Status != ormo.InOutStatusFullExit || target.Exit == nil || target.Exit.Filled != 2 {
		t.Fatalf("submitted exit was not reconciled to its own order: status=%d exit=%+v", target.Status, target.Exit)
	}
	if sibling.Status != ormo.InOutStatusFullEnter || sibling.Exit != nil || sibling.HoldAmount() != 10 {
		t.Fatalf("submitted exit mutated sibling: status=%d exit=%+v hold=%v", sibling.Status, sibling.Exit, sibling.HoldAmount())
	}
}

func TestSyncLocalOrdersDefersNoMatchWhileExitIsUnresolved(t *testing.T) {
	withIsolatedOrderState(t)
	const account = "exit-pending"
	const symbol = "EXITPENDING/USDT:USDT"
	target := makeEnteredOrder(2201, symbol, "target-entry", 2)
	target.Exit = &ormo.ExOrder{
		OrderID: "target-exit", Symbol: symbol, Side: banexg.OdSideSell,
		OrderType: banexg.OdTypeMarket, Amount: 2, Status: ormo.OdStatusInit,
	}
	sibling := makeEnteredOrder(2202, symbol, "sibling-entry", 10)
	withIssue138Exchange(t, &issue138Exchange{
		fetchOrder: func(string, string, map[string]interface{}) (*banexg.Order, *errs.Error) {
			return &banexg.Order{
				ID: "target-exit", Symbol: symbol, Side: banexg.OdSideSell, Type: banexg.OdTypeMarket,
				Status: banexg.OdStatusOpen, Amount: 2, Filled: 0, LastUpdateTimestamp: btime.UTCStamp(),
			}, nil
		},
		fetchPositions: func([]string, map[string]interface{}) ([]*banexg.Position, *errs.Error) {
			return []*banexg.Position{{Symbol: symbol, Side: banexg.PosSideLong, Contracts: 10}}, nil
		},
	})
	mgr := newLiveOrderMgr(account, func(*ormo.InOutOrder, bool) {})
	openOds, lock := ormo.GetOpenODs(account)
	lock.Lock()
	openOds[target.ID] = target
	openOds[sibling.ID] = sibling
	lock.Unlock()
	t.Cleanup(func() {
		lock.Lock()
		delete(openOds, target.ID)
		delete(openOds, sibling.ID)
		lock.Unlock()
	})

	closed, err := mgr.SyncLocalOrders()
	if err != nil {
		t.Fatalf("SyncLocalOrders: %v", err)
	}
	if len(closed) != 0 || target.Status != ormo.InOutStatusFullEnter || sibling.Status != ormo.InOutStatusFullEnter || sibling.Exit != nil {
		t.Fatalf("unresolved submitted exit allowed a no_match close: closed=%d target=%d sibling=%d siblingExit=%+v",
			len(closed), target.Status, sibling.Status, sibling.Exit)
	}
}

func TestSyncLocalOrdersKeepsNoMatchFallbackWithoutSubmittedExit(t *testing.T) {
	withIsolatedOrderState(t)
	const account = "exit-fallback"
	const symbol = "EXITFALLBACK/USDT:USDT"
	od := makeEnteredOrder(2301, symbol, "entry", 2)
	com.SetPrice(symbol, 100, 100)
	withIssue138Exchange(t, &issue138Exchange{
		fetchPositions: func([]string, map[string]interface{}) ([]*banexg.Position, *errs.Error) {
			return nil, nil
		},
	})
	mgr := newLiveOrderMgr(account, func(*ormo.InOutOrder, bool) {})
	openOds, lock := ormo.GetOpenODs(account)
	lock.Lock()
	openOds[od.ID] = od
	lock.Unlock()
	t.Cleanup(func() {
		lock.Lock()
		delete(openOds, od.ID)
		lock.Unlock()
	})

	closed, err := mgr.SyncLocalOrders()
	if err != nil {
		t.Fatalf("SyncLocalOrders: %v", err)
	}
	if len(closed) != 1 || closed[0] != od || od.ExitTag != core.ExitTagNoMatch || od.Status != ormo.InOutStatusFullExit {
		t.Fatalf("legacy no_match fallback changed: closed=%+v status=%d tag=%q", closed, od.Status, od.ExitTag)
	}
}

func TestReconcilePendingMarketEntryFill(t *testing.T) {
	fetchCalls := 0
	exchange := &issue138Exchange{
		fetchOrder: func(symbol, id string, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			fetchCalls++
			return &banexg.Order{
				ID: id, Symbol: symbol, Type: banexg.OdTypeMarket, Status: banexg.OdStatusFilled,
				Amount: 2, Filled: 2, Average: 101, LastUpdateTimestamp: btime.UTCStamp(),
			}, nil
		},
	}
	withIssue138Exchange(t, exchange)
	callbackCalls := 0
	mgr := newLiveOrderMgr("market-entry-reconcile", func(*ormo.InOutOrder, bool) { callbackCalls++ })
	od := issue138Order("filled-market-order")
	od.Enter.OrderType = banexg.OdTypeMarket
	od.Enter.UpdateAt = btime.UTCStamp() - pendingMarketEntryReconcileAfter.Milliseconds() - 1

	mgr.reconcilePendingMarketEntries([]*ormo.InOutOrder{od})

	if fetchCalls != 1 {
		t.Fatalf("FetchOrder calls = %d, want 1", fetchCalls)
	}
	if od.Enter.Filled != 2 || od.Enter.Average != 101 || od.Enter.Status != ormo.OdStatusClosed ||
		od.Status != ormo.InOutStatusFullEnter {
		t.Fatalf("market entry was not reconciled: filled=%v average=%v enterStatus=%d status=%d",
			od.Enter.Filled, od.Enter.Average, od.Enter.Status, od.Status)
	}
	if callbackCalls != 1 {
		t.Fatalf("reconcile callbacks = %d, want 1", callbackCalls)
	}
}

func TestReconcilePendingMarketEntriesSkipsUnrelatedOrders(t *testing.T) {
	fetchCalls := 0
	withIssue138Exchange(t, &issue138Exchange{
		fetchOrder: func(string, string, map[string]interface{}) (*banexg.Order, *errs.Error) {
			fetchCalls++
			return nil, nil
		},
	})
	mgr := newLiveOrderMgr("market-entry-scope", func(*ormo.InOutOrder, bool) {})
	oldUpdate := btime.UTCStamp() - pendingMarketEntryReconcileAfter.Milliseconds() - 1
	limitOrder := issue138Order("limit-order")
	limitOrder.Enter.UpdateAt = oldUpdate
	freshMarket := issue138Order("fresh-market")
	freshMarket.Enter.OrderType = banexg.OdTypeMarket
	freshMarket.Enter.UpdateAt = btime.UTCStamp()
	partialMarket := issue138Order("partial-market")
	partialMarket.Enter.OrderType = banexg.OdTypeMarket
	partialMarket.Enter.UpdateAt = oldUpdate
	partialMarket.Enter.Filled = 1

	mgr.reconcilePendingMarketEntries([]*ormo.InOutOrder{limitOrder, freshMarket, partialMarket})

	if fetchCalls != 0 {
		t.Fatalf("FetchOrder calls = %d, want 0", fetchCalls)
	}
}

func TestCancelEnterToLocalTriggerReconcilesFilledOrderAfterUnknownOrder(t *testing.T) {
	fetchCalls := 0
	exchange := &issue138Exchange{
		cancelOrder: func(string, string, map[string]interface{}) (*banexg.Order, *errs.Error) {
			return nil, issue138Err(errs.CodeOrderNotCancelable)
		},
		fetchOrder: func(symbol, id string, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			fetchCalls++
			return &banexg.Order{
				ID: id, Symbol: symbol, Status: banexg.OdStatusFilled,
				Amount: 2, Filled: 2, Average: 101, Timestamp: 50, LastUpdateTimestamp: 200,
			}, nil
		},
	}
	withIssue138Exchange(t, exchange)
	mgr := newLiveOrderMgr("issue138-reconcile", func(*ormo.InOutOrder, bool) {})
	od := issue138Order("filled-order")
	od.Enter.UpdateAt = 250

	if changed, err := cancelEnterToLocalTrigger(mgr, od, 600); err != nil || !changed {
		t.Fatal("expected reconciled order to be reported as changed")
	}
	if fetchCalls != 1 {
		t.Fatalf("FetchOrder calls = %d, want 1", fetchCalls)
	}
	if od.Enter.Filled != 2 || od.Enter.Status != ormo.OdStatusClosed || od.Status != ormo.InOutStatusFullEnter {
		t.Fatalf("filled exchange order was not reconciled: filled=%v enterStatus=%d status=%d",
			od.Enter.Filled, od.Enter.Status, od.Status)
	}
	if od.Enter.OrderID != "filled-order" {
		t.Fatalf("reconciliation cleared exchange order id: %q", od.Enter.OrderID)
	}
}

func TestCancelEnterToLocalTriggerRollsBackCanceledOrderAfterUnknownOrder(t *testing.T) {
	exchange := &issue138Exchange{
		cancelOrder: func(string, string, map[string]interface{}) (*banexg.Order, *errs.Error) {
			return nil, issue138Err(errs.CodeOrderNotCancelable)
		},
		fetchOrder: func(symbol, id string, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			return &banexg.Order{
				ID: id, Symbol: symbol, Status: banexg.OdStatusCanceled,
				Amount: 2, Timestamp: 200,
			}, nil
		},
	}
	withIssue138Exchange(t, exchange)
	mgr := newLiveOrderMgr("issue138-canceled", func(*ormo.InOutOrder, bool) {})
	od := issue138Order("canceled-order")

	if changed, err := cancelEnterToLocalTrigger(mgr, od, 600); err != nil || !changed {
		t.Fatal("expected canceled order to be reported as changed")
	}
	if od.Enter.OrderID != "" || od.Enter.Status != ormo.OdStatusInit || od.Status != ormo.InOutStatusInit {
		t.Fatalf("canceled exchange order was not rolled back: orderID=%q enterStatus=%d status=%d",
			od.Enter.OrderID, od.Enter.Status, od.Status)
	}
	triggers, lock := ormo.GetTriggerODs(mgr.Account)
	lock.Lock()
	restored := triggers[od.Symbol][od.ID]
	delete(triggers, od.Symbol)
	lock.Unlock()
	if restored != od {
		t.Fatal("canceled exchange order was not restored as a local trigger")
	}
}

func TestCancelEnterToLocalTriggerRollsBackConfirmedZeroFillCancel(t *testing.T) {
	exchange := &issue138Exchange{
		cancelOrder: func(id, symbol string, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			return &banexg.Order{ID: id, Symbol: symbol, Status: banexg.OdStatusCanceled, Timestamp: 200}, nil
		},
	}
	withIssue138Exchange(t, exchange)
	mgr := newLiveOrderMgr("issue138-confirmed-cancel", func(*ormo.InOutOrder, bool) {})
	od := issue138Order("confirmed-canceled-order")

	if changed, err := cancelEnterToLocalTrigger(mgr, od, 600); err != nil || !changed {
		t.Fatal("confirmed zero-fill cancel was not rolled back")
	}
	if od.Enter.OrderID != "" || od.Enter.Status != ormo.OdStatusInit || od.Status != ormo.InOutStatusInit {
		t.Fatalf("confirmed cancel was not reset to a local trigger: orderID=%q enterStatus=%d status=%d",
			od.Enter.OrderID, od.Enter.Status, od.Status)
	}
	if od.GetInfoInt64(odInfoLocalTrigger) != 1 {
		t.Fatal("confirmed cancel did not persist local trigger ownership")
	}
	triggers, lock := ormo.GetTriggerODs(mgr.Account)
	lock.Lock()
	rolledBack := triggers[od.Symbol][od.ID]
	delete(triggers, od.Symbol)
	lock.Unlock()
	if rolledBack != od {
		t.Fatal("confirmed cancel was not registered in the trigger map")
	}
}

func TestCancelEnterToLocalTriggerAppliesRacedPartialFill(t *testing.T) {
	createCalls := 0
	createdAmount := 0.0
	exchange := &issue138Exchange{
		cancelOrder: func(id, symbol string, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			return &banexg.Order{
				ID: id, Symbol: symbol, Status: banexg.OdStatusCanceled,
				Amount: 2, Filled: 1, Average: 101, LastUpdateTimestamp: 200,
			}, nil
		},
		createOrder: func(symbol, odType, side string, amount, price float64, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			createCalls++
			createdAmount = amount
			return &banexg.Order{ID: "raced-fill-stop", Symbol: symbol, Status: banexg.OdStatusOpen}, nil
		},
	}
	withIssue138Exchange(t, exchange)
	callbackCalls := 0
	mgr := newLiveOrderMgr("issue138-raced-partial", func(*ormo.InOutOrder, bool) { callbackCalls++ })
	od := issue138Order("raced-partial-order")
	od.Enter.UpdateAt = 250
	if err := od.SetStopLoss(&ormo.ExitTrigger{Price: 90, Rate: 0.5}); err != nil {
		t.Fatalf("set stop loss: %v", err)
	}

	if changed, err := cancelEnterToLocalTrigger(mgr, od, 600); err != nil || !changed {
		t.Fatal("raced partial fill was not applied")
	}
	if od.Enter.Filled != 1 || od.Enter.Status != ormo.OdStatusClosed || od.Status != ormo.InOutStatusFullEnter {
		t.Fatalf("raced partial fill was lost: filled=%v enterStatus=%d status=%d",
			od.Enter.Filled, od.Enter.Status, od.Status)
	}
	if callbackCalls != 1 || createCalls != 1 || od.GetInfoInt64(odInfoLocalTrigger) != 0 {
		t.Fatalf("raced fill finalization mismatch: callbacks=%d protections=%d marker=%d",
			callbackCalls, createCalls, od.GetInfoInt64(odInfoLocalTrigger))
	}
	if createdAmount != 0.5 {
		t.Fatalf("partial-fill stop amount = %v, want 0.5", createdAmount)
	}
}

func TestCancelEnterToLocalTriggerPreservesOrderOnEmptyCancelResponse(t *testing.T) {
	exchange := &issue138Exchange{
		cancelOrder: func(string, string, map[string]interface{}) (*banexg.Order, *errs.Error) {
			return nil, nil
		},
	}
	withIssue138Exchange(t, exchange)
	mgr := newLiveOrderMgr("issue138-empty-cancel", func(*ormo.InOutOrder, bool) {})
	od := issue138Order("empty-cancel-order")

	if changed, err := cancelEnterToLocalTrigger(mgr, od, 600); err == nil || changed {
		t.Fatal("empty cancel response was treated as confirmed cancellation")
	}
	if od.Enter.OrderID != "empty-cancel-order" || od.Enter.Status != ormo.OdStatusInit || od.Status != ormo.InOutStatusInit {
		t.Fatalf("empty cancel response mutated order: orderID=%q enterStatus=%d status=%d",
			od.Enter.OrderID, od.Enter.Status, od.Status)
	}
}

func TestCancelTimeoutEnterReconcilesFilledOrderAfterUnknownOrder(t *testing.T) {
	createCalls := 0
	createdClientID := ""
	exchange := &issue138Exchange{
		cancelOrder: func(string, string, map[string]interface{}) (*banexg.Order, *errs.Error) {
			return nil, issue138Err(errs.CodeOrderNotCancelable)
		},
		fetchOrder: func(symbol, id string, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			return &banexg.Order{
				ID: id, Symbol: symbol, Status: banexg.OdStatusFilled,
				Amount: 2, Filled: 2, Average: 101, Timestamp: 50, LastUpdateTimestamp: 200,
			}, nil
		},
		createOrder: func(symbol, odType, side string, amount, price float64, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			createCalls++
			createdClientID, _ = params[banexg.ParamClientOrderId].(string)
			return &banexg.Order{
				ID: "stop-loss-order", ClientOrderID: createdClientID, Symbol: symbol, Status: banexg.OdStatusOpen,
			}, nil
		},
	}
	withIssue138Exchange(t, exchange)
	callbackCalls := 0
	mgr := newLiveOrderMgr("issue138-timeout-filled", func(*ormo.InOutOrder, bool) { callbackCalls++ })
	od := issue138Order("filled-timeout-order")
	od.Enter.UpdateAt = 250
	if err := od.SetStopLoss(&ormo.ExitTrigger{Price: 90}); err != nil {
		t.Fatalf("set stop loss: %v", err)
	}

	cancelTimeoutEnter(mgr, od)
	cancelTimeoutEnter(mgr, od)

	if od.Enter.Filled != 2 || od.Enter.Status != ormo.OdStatusClosed || od.Status != ormo.InOutStatusFullEnter {
		t.Fatalf("filled timeout order was not reconciled: filled=%v enterStatus=%d status=%d",
			od.Enter.Filled, od.Enter.Status, od.Status)
	}
	if od.Exit != nil {
		t.Fatalf("filled exchange order was closed locally: exit=%v", od.Exit)
	}
	if callbackCalls != 1 {
		t.Fatalf("entry callback calls = %d, want 1", callbackCalls)
	}
	if createCalls != 1 || od.GetStopLoss().OrderId != "stop-loss-order" {
		t.Fatalf("protective stop loss was not installed: calls=%d state=%+v", createCalls, od.GetStopLoss())
	}
	if createdClientID == "" || od.GetStopLoss().ClientId != createdClientID {
		t.Fatalf("protective stop client ID was not persisted: created=%q state=%+v", createdClientID, od.GetStopLoss())
	}
	openOds, lock := ormo.GetOpenODs(config.DefAcc)
	lock.Lock()
	saved := openOds[od.ID]
	delete(openOds, od.ID)
	lock.Unlock()
	if saved != od {
		t.Fatal("reconciled order was not saved immediately")
	}
}

func TestCancelTimeoutEnterAppliesSuccessfulPartialCancelOnce(t *testing.T) {
	cancelCalls := 0
	createCalls := 0
	exchange := &issue138Exchange{
		cancelOrder: func(id, symbol string, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			cancelCalls++
			return &banexg.Order{
				ID: id, Symbol: symbol, Status: banexg.OdStatusCanceled,
				Amount: 2, Filled: 1, Average: 101, LastUpdateTimestamp: 200,
			}, nil
		},
		createOrder: func(symbol, odType, side string, amount, price float64, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			createCalls++
			return &banexg.Order{ID: "successful-cancel-stop", Symbol: symbol, Status: banexg.OdStatusOpen}, nil
		},
	}
	withIssue138Exchange(t, exchange)
	callbackCalls := 0
	mgr := newLiveOrderMgr("issue138-successful-partial", func(*ormo.InOutOrder, bool) { callbackCalls++ })
	od := issue138Order("successful-partial-order")
	od.Enter.UpdateAt = 250
	if err := od.SetStopLoss(&ormo.ExitTrigger{Price: 90}); err != nil {
		t.Fatalf("set stop loss: %v", err)
	}

	cancelTimeoutEnter(mgr, od)
	cancelTimeoutEnter(mgr, od)

	if od.Enter.Filled != 1 || od.Enter.Status != ormo.OdStatusClosed || od.Status != ormo.InOutStatusFullEnter {
		t.Fatalf("successful partial cancel was not applied: filled=%v enterStatus=%d status=%d",
			od.Enter.Filled, od.Enter.Status, od.Status)
	}
	if cancelCalls != 1 || callbackCalls != 1 || createCalls != 1 {
		t.Fatalf("successful cancel was not idempotent: cancels=%d callbacks=%d protections=%d",
			cancelCalls, callbackCalls, createCalls)
	}
	if od.Exit != nil {
		t.Fatalf("partial fill was closed locally after successful cancel: exit=%v", od.Exit)
	}
}

func TestCancelTimeoutEnterAppliesSuccessfulZeroFillCancelOnce(t *testing.T) {
	cancelCalls := 0
	exchange := &issue138Exchange{
		cancelOrder: func(id, symbol string, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			cancelCalls++
			return &banexg.Order{
				ID: id, Symbol: symbol, Status: banexg.OdStatusCanceled,
				Amount: 2, LastUpdateTimestamp: 200,
			}, nil
		},
	}
	withIssue138Exchange(t, exchange)
	callbackCalls := 0
	mgr := newLiveOrderMgr("issue138-successful-zero", func(*ormo.InOutOrder, bool) { callbackCalls++ })
	od := issue138Order("successful-zero-order")
	od.Enter.UpdateAt = 250

	cancelTimeoutEnter(mgr, od)
	cancelTimeoutEnter(mgr, od)

	if od.Enter.Filled != 0 || od.Enter.Status != ormo.OdStatusClosed || od.Status < ormo.InOutStatusFullExit {
		t.Fatalf("successful zero-fill cancel was not applied: filled=%v enterStatus=%d status=%d",
			od.Enter.Filled, od.Enter.Status, od.Status)
	}
	if cancelCalls != 1 || callbackCalls != 1 {
		t.Fatalf("successful zero-fill cancel was not idempotent: cancels=%d callbacks=%d", cancelCalls, callbackCalls)
	}
	if od.Exit != nil {
		t.Fatalf("authoritative zero-fill cancel also created a local exit: exit=%v", od.Exit)
	}
}

func TestCancelTimeoutEnterDoesNotCloseLocallyWhenReconciliationFails(t *testing.T) {
	exchange := &issue138Exchange{
		cancelOrder: func(string, string, map[string]interface{}) (*banexg.Order, *errs.Error) {
			return nil, issue138Err(errs.CodeOrderNotCancelable)
		},
		fetchOrder: func(string, string, map[string]interface{}) (*banexg.Order, *errs.Error) {
			return nil, issue138Err(-1000)
		},
	}
	withIssue138Exchange(t, exchange)
	mgr := newLiveOrderMgr("issue138-timeout", func(*ormo.InOutOrder, bool) {})
	od := issue138Order("unknown-order")

	cancelTimeoutEnter(mgr, od)

	if od.Status != ormo.InOutStatusInit || od.Enter.Status != ormo.OdStatusInit || od.Exit != nil {
		t.Fatalf("order was closed without authoritative exchange state: status=%d enterStatus=%d exit=%v",
			od.Status, od.Enter.Status, od.Exit)
	}
	if od.Enter.OrderID != "unknown-order" {
		t.Fatalf("failed reconciliation cleared exchange order id: %q", od.Enter.OrderID)
	}
}

func TestCancelTimeoutEnterRejectsStaleReconciliation(t *testing.T) {
	exchange := &issue138Exchange{
		cancelOrder: func(string, string, map[string]interface{}) (*banexg.Order, *errs.Error) {
			return nil, issue138Err(errs.CodeOrderNotCancelable)
		},
		fetchOrder: func(symbol, id string, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			return &banexg.Order{
				ID: id, Symbol: symbol, Status: banexg.OdStatusCanceled,
				Amount: 2, Filled: 0, Timestamp: 200,
			}, nil
		},
	}
	withIssue138Exchange(t, exchange)
	mgr := newLiveOrderMgr("issue138-stale", func(*ormo.InOutOrder, bool) {})
	od := issue138Order("part-filled-order")
	od.Enter.Filled = 1
	od.Enter.Average = 100
	od.Enter.Status = ormo.OdStatusPartOK
	od.Status = ormo.InOutStatusPartEnter

	cancelTimeoutEnter(mgr, od)

	if od.Enter.Filled != 1 || od.Enter.Status != ormo.OdStatusPartOK || od.Status != ormo.InOutStatusPartEnter {
		t.Fatalf("stale exchange snapshot regressed local fill: filled=%v enterStatus=%d status=%d",
			od.Enter.Filled, od.Enter.Status, od.Status)
	}
	if od.Exit != nil {
		t.Fatalf("stale reconciliation closed the order locally: exit=%v", od.Exit)
	}
}

func TestApplyAuthoritativeEnterOrderIgnoresDuplicatePartialSnapshot(t *testing.T) {
	exchange := &issue138Exchange{}
	withIssue138Exchange(t, exchange)
	callbackCalls := 0
	mgr := newLiveOrderMgr("issue138-duplicate-partial", func(*ormo.InOutOrder, bool) { callbackCalls++ })
	od := issue138Order("partial-order")
	res := &banexg.Order{
		ID: od.Enter.OrderID, Symbol: od.Symbol, Status: banexg.OdStatusPartFilled,
		Amount: 2, Filled: 1, Average: 101, LastUpdateTimestamp: 200,
	}

	if err := mgr.applyAuthoritativeEnterOrder(od, res); err != nil {
		t.Fatalf("apply first partial snapshot: %v", err)
	}
	if err := mgr.applyAuthoritativeEnterOrder(od, res); err != nil {
		t.Fatalf("apply duplicate partial snapshot: %v", err)
	}
	if od.Enter.Filled != 1 || od.Enter.Status != ormo.OdStatusPartOK || od.Status != ormo.InOutStatusPartEnter {
		t.Fatalf("partial state mismatch: filled=%v enterStatus=%d status=%d",
			od.Enter.Filled, od.Enter.Status, od.Status)
	}
	if callbackCalls != 1 {
		t.Fatalf("duplicate partial snapshot callbacks = %d, want 1", callbackCalls)
	}
}

func TestUpdateOdByExgResIgnoresDuplicatePartialSnapshot(t *testing.T) {
	withIssue138Exchange(t, &issue138Exchange{})
	callbackCalls := 0
	mgr := newLiveOrderMgr("issue138-direct-duplicate-partial", func(*ormo.InOutOrder, bool) { callbackCalls++ })
	od := issue138Order("partial-order")
	res := &banexg.Order{
		ID: od.Enter.OrderID, Symbol: od.Symbol, Status: banexg.OdStatusPartFilled,
		Amount: 2, Filled: 1, Average: 101, Timestamp: 200,
	}

	if err := mgr.updateOdByExgRes(od, true, res); err != nil {
		t.Fatalf("apply first direct partial snapshot: %v", err)
	}
	if err := mgr.updateOdByExgRes(od, true, res); err != nil {
		t.Fatalf("apply duplicate direct partial snapshot: %v", err)
	}
	if od.Enter.Filled != 1 || od.Enter.Status != ormo.OdStatusPartOK || od.Status != ormo.InOutStatusPartEnter {
		t.Fatalf("direct partial state mismatch: filled=%v enterStatus=%d status=%d",
			od.Enter.Filled, od.Enter.Status, od.Status)
	}
	if callbackCalls != 1 {
		t.Fatalf("duplicate direct partial callbacks = %d, want 1", callbackCalls)
	}
}

func TestUpdateOdByExgResAdvancesSameFillToTerminalOnce(t *testing.T) {
	for _, terminalStatus := range []string{banexg.OdStatusCanceled, banexg.OdStatusFilled} {
		t.Run(terminalStatus, func(t *testing.T) {
			withIssue138Exchange(t, &issue138Exchange{})
			callbackCalls := 0
			mgr := newLiveOrderMgr("issue138-direct-terminal-"+terminalStatus, func(*ormo.InOutOrder, bool) { callbackCalls++ })
			od := issue138Order("partial-order")
			partial := &banexg.Order{
				ID: od.Enter.OrderID, Symbol: od.Symbol, Status: banexg.OdStatusPartFilled,
				Amount: 2, Filled: 1, Average: 101, Timestamp: 200,
			}
			terminal := *partial
			terminal.Status = terminalStatus
			terminal.Timestamp = 300

			if err := mgr.updateOdByExgRes(od, true, partial); err != nil {
				t.Fatalf("apply partial snapshot: %v", err)
			}
			if err := mgr.updateOdByExgRes(od, true, &terminal); err != nil {
				t.Fatalf("advance to terminal snapshot: %v", err)
			}
			if err := mgr.updateOdByExgRes(od, true, &terminal); err != nil {
				t.Fatalf("replay terminal snapshot: %v", err)
			}
			if od.Enter.Filled != 1 || od.Enter.Status != ormo.OdStatusClosed || od.Status != ormo.InOutStatusFullEnter {
				t.Fatalf("terminal state mismatch: filled=%v enterStatus=%d status=%d",
					od.Enter.Filled, od.Enter.Status, od.Status)
			}
			if callbackCalls != 2 {
				t.Fatalf("partial plus terminal callbacks = %d, want 2", callbackCalls)
			}
		})
	}
}

func TestUpdateOdByExgResAcceptsNewTradeAtSameCumulativeFill(t *testing.T) {
	withIssue138Exchange(t, &issue138Exchange{})
	callbackCalls := 0
	mgr := newLiveOrderMgr("issue138-direct-new-trade", func(*ormo.InOutOrder, bool) { callbackCalls++ })
	od := issue138Order("partial-order")
	partial := &banexg.Order{
		ID: od.Enter.OrderID, Symbol: od.Symbol, Status: banexg.OdStatusPartFilled,
		Amount: 2, Filled: 1, Average: 101, Timestamp: 200,
	}
	withTrade := *partial
	withTrade.Timestamp = 300
	withTrade.Trades = []*banexg.Trade{{ID: "trade-2", Symbol: od.Symbol, Timestamp: 300}}

	if err := mgr.updateOdByExgRes(od, true, partial); err != nil {
		t.Fatalf("apply partial snapshot: %v", err)
	}
	if err := mgr.updateOdByExgRes(od, true, &withTrade); err != nil {
		t.Fatalf("apply new trade snapshot: %v", err)
	}
	if err := mgr.updateOdByExgRes(od, true, &withTrade); err != nil {
		t.Fatalf("replay new trade snapshot: %v", err)
	}
	if callbackCalls != 2 {
		t.Fatalf("partial plus new trade callbacks = %d, want 2", callbackCalls)
	}
}

func TestTrailingStopUsesCurrentHoldAmount(t *testing.T) {
	createdAmount := 0.0
	exchange := &issue138Exchange{
		createOrder: func(symbol, odType, side string, amount, price float64, params map[string]interface{}) (*banexg.Order, *errs.Error) {
			createdAmount = amount
			return &banexg.Order{ID: "trailing-order", Symbol: symbol, Status: banexg.OdStatusOpen}, nil
		},
	}
	withIssue138Exchange(t, exchange)
	mgr := newLiveOrderMgr("issue138-trailing-amount", func(*ormo.InOutOrder, bool) {})
	od := issue138Order("filled-entry")
	od.Enter.Filled = 1
	od.Enter.Status = ormo.OdStatusClosed
	od.Status = ormo.InOutStatusFullEnter
	od.SetInfo(ormo.OdInfoCallbackPct, 1.0)

	mgr.setTrailingStop(od)

	if createdAmount != 1 {
		t.Fatalf("trailing stop amount = %v, want current hold 1", createdAmount)
	}
}

func TestIsFarEnterRejectsSubmittedExchangeOrder(t *testing.T) {
	od := issue138Order("submitted-order")
	od.SetInfo(ormo.OdInfoStopAfter, time.Now().Add(time.Hour).UnixMilli())
	if isFarEnter(od) {
		t.Fatal("submitted exchange order was classified as a local trigger")
	}
}

func TestEditOrderKeepsSubmittedEntryOnExchangeQueue(t *testing.T) {
	mgr := newLiveOrderMgr("issue138-submitted-edit", func(*ormo.InOutOrder, bool) {})
	od := issue138Order("submitted-order")
	od.SetInfo(ormo.OdInfoStopAfter, time.Now().Add(time.Hour).UnixMilli())

	mgr.EditOrder(od, ormo.OdActionLimitEnter)

	select {
	case item := <-mgr.queue:
		if item.Order != od || item.Action != ormo.OdActionLimitEnter {
			t.Fatalf("unexpected edit queue item: %+v", item)
		}
	default:
		t.Fatal("submitted entry edit was not queued for exchange handling")
	}
	triggers, lock := ormo.GetTriggerODs(mgr.Account)
	lock.Lock()
	_, registered := triggers[od.Symbol][od.ID]
	delete(triggers, od.Symbol)
	lock.Unlock()
	if registered {
		t.Fatal("submitted entry edit was also registered as a local trigger")
	}
}

func TestRestoreInOutOrderFinishesPendingCanceledRollback(t *testing.T) {
	exchange := &issue138Exchange{}
	withIssue138Exchange(t, exchange)
	mgr := newLiveOrderMgr("issue138-pending-rollback", func(*ormo.InOutOrder, bool) {})
	od := issue138Order("canceled-before-save")
	od.SetInfo(odInfoLocalTrigger, int64(1))
	exOd := &banexg.Order{
		ID: od.Enter.OrderID, Symbol: od.Symbol, Status: banexg.OdStatusCanceled,
		Amount: 2, Filled: 0, Timestamp: 200,
	}

	if err := mgr.restoreInOutOrder(od, map[string]*banexg.Order{exOd.ID: exOd}); err != nil {
		t.Fatalf("restore pending canceled rollback: %v", err)
	}
	if od.Enter.OrderID != "" || od.Enter.Status != ormo.OdStatusInit || od.Status != ormo.InOutStatusInit {
		t.Fatalf("pending rollback state mismatch: orderID=%q enterStatus=%d status=%d",
			od.Enter.OrderID, od.Enter.Status, od.Status)
	}
	triggers, lock := ormo.GetTriggerODs(mgr.Account)
	lock.Lock()
	restored := triggers[od.Symbol][od.ID]
	delete(triggers, od.Symbol)
	lock.Unlock()
	if restored != od {
		t.Fatal("pending canceled rollback was not restored as a local trigger")
	}
}
