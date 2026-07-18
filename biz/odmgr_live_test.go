package biz

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
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
	cancelOrder  func(id, symbol string, params map[string]interface{}) (*banexg.Order, *errs.Error)
	fetchOrder   func(symbol, id string, params map[string]interface{}) (*banexg.Order, *errs.Error)
	createOrder  func(symbol, odType, side string, amount, price float64, params map[string]interface{}) (*banexg.Order, *errs.Error)
	fetchTickers func(symbols []string, params map[string]interface{}) ([]*banexg.Ticker, *errs.Error)
}

func (e *issue138Exchange) CancelOrder(id, symbol string, params map[string]interface{}) (*banexg.Order, *errs.Error) {
	return e.cancelOrder(id, symbol, params)
}

func (e *issue138Exchange) FetchOrder(symbol, id string, params map[string]interface{}) (*banexg.Order, *errs.Error) {
	return e.fetchOrder(symbol, id, params)
}

func (e *issue138Exchange) CreateOrder(symbol, odType, side string, amount, price float64, params map[string]interface{}) (*banexg.Order, *errs.Error) {
	return e.createOrder(symbol, odType, side, amount, price, params)
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
