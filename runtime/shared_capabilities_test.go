package runtime

import (
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestSharedForceExitPreservesLotOwnership(t *testing.T) {
	f := newSharedTriggerFixture(t)
	first := f.entry(t, &strat.EnterReq{Tag: "first", Amount: 1})
	f.entry(t, &strat.EnterReq{Tag: "other", Amount: 1})
	if _, err := f.manager.ExitOrder(first, &strat.ExitReq{Tag: "force", Force: true}); err != nil {
		t.Fatal(err)
	}
	if got := f.adapter.Metrics().Fills; got != 3 {
		t.Fatalf("force exit fills %d", got)
	}
}

func TestSharedCloseDelayUsesLastCommittedEntryFill(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(fmt.Sprint(force), func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			f.job.TimeFrame = "1m"
			// Acceptance precedes the actual visible limit fill by 100ms.
			od := f.entry(t, &strat.EnterReq{Tag: "delayed", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
			f.observe(t, 90, 100)
			if od.Enter.UpdateAt != 200 {
				t.Fatalf("entry fill timestamp %d", od.Enter.UpdateAt)
			}
			f.rt.Clock.SetTimeMS(54200)
			if _, err := f.manager.ExitOpenOrders(od.Symbol, &strat.ExitReq{Tag: "boundary", EnterTag: "delayed", Force: force}); err != nil {
				t.Fatal(err)
			}
			if force {
				if f.steps(t) != 0 {
					t.Fatal("force exit obeyed close delay")
				}
				return
			}
			if f.steps(t) != 10 {
				t.Fatal("close delay used acceptance rather than fill time")
			}
			f.rt.Clock.SetTimeMS(54201)
			if _, err := f.manager.ExitOpenOrders(od.Symbol, &strat.ExitReq{Tag: "after", EnterTag: "delayed"}); err != nil {
				t.Fatal(err)
			}
			if f.steps(t) != 0 {
				t.Fatal("close delay did not expire")
			}
		})
	}
}

func TestSharedDirectOrderExitBypassesDelayPreservingStyleAndCommand(t *testing.T) {
	for _, orderAPI := range []bool{false, true} {
		for _, style := range []int{core.OrderTypeMarket, core.OrderTypeLimit, core.OrderTypeLimitMaker} {
			t.Run(fmt.Sprintf("ExitOrder=%v/style=%d", orderAPI, style), func(t *testing.T) {
				f := newSharedTriggerFixture(t)
				f.job.TimeFrame = "1m"
				od := f.entry(t, &strat.EnterReq{Tag: "direct", Amount: 1})
				f.rt.Clock.SetTimeMS(101)
				req := &strat.ExitReq{CommandID: "direct-exit", StratName: "legacy", Tag: "direct-exit", OrderType: style}
				if !orderAPI {
					req.OrderID = od.ID
				}
				if core.IsLimitOrder(style) {
					req.Limit = 110
				}
				original := *req
				exit := func(request *strat.ExitReq) error {
					if orderAPI {
						if _, err := f.manager.ExitOrder(od, request); err != nil {
							return err
						}
					} else if _, err := f.manager.ExitOpenOrders(od.Symbol, request); err != nil {
						return err
					}
					return nil
				}
				if err := exit(req); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(*req, original) {
					t.Fatal("direct exit mutated caller request")
				}
				if od.Exit == nil || od.Exit.OrderType != core.OrderTypeEnums[style] || od.Exit.Price != req.Limit {
					t.Fatalf("direct exit was delayed or lost requested style: %+v", od.Exit)
				}
				wantSteps, wantFills := int64(10), 1
				if style == core.OrderTypeMarket {
					wantSteps, wantFills = 0, 2
				}
				if f.steps(t) != wantSteps || f.adapter.Metrics().Fills != wantFills {
					t.Fatal("direct exit changed limit semantics or retained delay")
				}
				if err := exit(req); err != nil || f.adapter.Metrics().Fills != wantFills {
					t.Fatal("direct exit command retry traded twice", err)
				}
				changed := *req
				changed.Force = true
				if err := exit(&changed); err == nil {
					t.Fatal("auto-Force changed the original command identity")
				}
				if core.IsLimitOrder(style) {
					f.observe(t, 110, 2)
					if f.steps(t) != 0 || f.adapter.Metrics().Fills != 2 {
						t.Fatal("direct limit exit did not fill on later touch")
					}
				}
			})
		}
	}
}

func TestSharedLimitExitEditReevaluatesOwnRemainingQuantity(t *testing.T) {
	f := newSharedTriggerFixture(t)
	od := f.entry(t, &strat.EnterReq{Tag: "edit", Amount: 1})
	od, err := f.manager.ExitOrder(od, &strat.ExitReq{Tag: "limit", OrderType: core.OrderTypeLimit, Limit: 110, ExitRate: .5})
	if err != nil {
		t.Fatal(err)
	}
	if f.steps(t) != 10 || od.Exit == nil {
		t.Fatal("pending limit exit projection missing")
	}
	od.Exit.Price = 90
	f.manager.EditOrder(od, ormo.OdActionLimitExit)
	if err := f.manager.(*biz.SharedOrderMgr).LastError(); err != nil {
		t.Fatal(err)
	}
	if f.steps(t) != 5 {
		t.Fatal("limit edit changed requested reduction")
	}
}

func TestSharedRelayReopensRemainingAmountWithFreshIdentity(t *testing.T) {
	f := newSharedTriggerFixture(t)
	source := &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 99, Symbol: "BTC", Sid: 1, Timeframe: "ws", Strategy: "legacy", EnterTag: "relay", Leverage: 2}, Enter: &ormo.ExOrder{Amount: 1, Filled: 1, Price: 80}, Exit: &ormo.ExOrder{Filled: .4}, Info: map[string]any{"source": "kept"}}
	if err := f.manager.RelayOrders([]*ormo.InOutOrder{source}); err != nil {
		t.Fatal(err)
	}
	if f.steps(t) != 6 || f.adapter.Metrics().Fills != 1 {
		t.Fatal("relay did not reopen remaining .6 at current venue")
	}
	if err := f.manager.RelayOrders([]*ormo.InOutOrder{source}); err != nil || f.adapter.Metrics().Fills != 1 {
		t.Fatal("relay replay traded twice", err)
	}
	source.Info["source"] = "changed"
	if err := f.manager.RelayOrders([]*ormo.InOutOrder{source}); err == nil {
		t.Fatal("relay identity accepted altered source metadata")
	}
	rows, lock := f.rt.Orders.GetOpenODs("default")
	lock.Lock()
	defer lock.Unlock()
	for _, row := range rows {
		if row.ID == source.ID || row.Enter.Average != 100 || row.EnterTag != "relay" || row.Info["source"] != "kept" {
			t.Fatal("relay copied old identity/fills or lost metadata", row)
		}
	}
}

func TestSharedRelayPreservesLimitStyleAtCurrentSidePrice(t *testing.T) {
	for _, style := range []string{"limit", "limit_maker"} {
		for _, short := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/short=%v", style, short), func(t *testing.T) {
				f := newSharedTriggerFixture(t)
				f.bridge.Quote = func(_ string, now int64) (execution.VisibleQuote, error) {
					return execution.VisibleQuote{Bid: f.price.Sub(decimal.NewFromInt(1)), Ask: f.price.Add(decimal.NewFromInt(1)), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 1000, Bar: f.bar}, nil
				}
				manager, err := biz.NewSharedOrderMgr(f.rt.BizDeps(), f.rt.SharedExecution(), f.bridge, nil)
				if err != nil {
					t.Fatal(err)
				}
				f.manager = manager
				source := &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 99, TaskID: 4, Symbol: "BTC", Sid: 1, Timeframe: "ws", Strategy: "legacy", Short: short, EnterTag: "relay-style"}, Enter: &ormo.ExOrder{OrderType: style, Price: 80, Amount: 1, Filled: 1}}
				if err := manager.RelayOrders([]*ormo.InOutOrder{source}); err != nil {
					t.Fatal(err)
				}
				if f.steps(t) != 0 || f.adapter.Metrics().Fills != 0 {
					t.Fatal("limit relay was silently executed as a market order")
				}
				wantPrice := 99.0
				if short {
					wantPrice = 101
				}
				rows, lock := f.rt.Orders.GetOpenODs("default")
				lock.Lock()
				for _, row := range rows {
					if row.Enter.OrderType != style || row.Enter.Price != wantPrice {
						t.Errorf("relay lost current side price/style: %+v", row.Enter)
					}
				}
				lock.Unlock()
				if err := manager.RelayOrders([]*ormo.InOutOrder{source}); err != nil {
					t.Fatal("relay retry was not idempotent", err)
				}
				price := 98.0
				wantSteps := int64(10)
				if short {
					price, wantSteps = 102, -10
				}
				f.observe(t, price, 2)
				if got := f.steps(t); got != wantSteps {
					t.Fatalf("relay did not fill at later quote: %d", got)
				}
				if source.Enter.OrderType != style || source.Enter.Price != 80 {
					t.Fatal("relay mutated source order")
				}
			})
		}
	}
}

func TestSharedRelayRejectsUnsupportedStyle(t *testing.T) {
	f := newSharedTriggerFixture(t)
	source := &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 99, Symbol: "BTC", Sid: 1, Timeframe: "ws", Strategy: "legacy"}, Enter: &ormo.ExOrder{OrderType: "unknown", Amount: 1, Filled: 1}}
	if err := f.manager.RelayOrders([]*ormo.InOutOrder{source}); err == nil || f.adapter.Metrics().Fills != 0 {
		t.Fatal("unsupported relay style traded instead of being rejected", err)
	}
}

func TestSharedEntryPreservesConfiguredDefaultsWithoutMutatingRequest(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			deps := f.rt.BizDeps()
			deps.Config = config.NewSnapshot(&config.Config{StopEnterBars: 2, Leverage: 5, Accounts: map[string]*config.AccountConfig{"default": {Leverage: 7}}})
			manager, err := biz.NewSharedOrderMgr(deps, f.rt.SharedExecution(), f.bridge, nil)
			if err != nil {
				t.Fatal(err)
			}
			f.manager = manager
			req := &strat.EnterReq{StratName: "legacy", Tag: "defaults", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90}
			wantLeverage := 7.0
			if explicit {
				req.StopBars, req.Leverage = 4, 3
				wantLeverage = 3
			}
			original := *req
			od, entryErr := manager.EnterOrder(f.job.Symbol, f.job.TimeFrame, req)
			if entryErr != nil || od == nil {
				t.Fatal("entry failed", entryErr)
			}
			if od.Leverage != wantLeverage || !reflect.DeepEqual(*req, original) {
				t.Fatalf("entry defaults or copy boundary lost: leverage=%g request=%+v", od.Leverage, req)
			}
			f.observe(t, 100, 3)
			f.observe(t, 90, 4)
			wantSteps := int64(0)
			if explicit {
				wantSteps = 10
			}
			if got := f.steps(t); got != wantSteps {
				t.Fatalf("entry expiry defaults changed: got %d want %d", got, wantSteps)
			}
		})
	}
}

func TestSharedEntryResolvesConfiguredOrderStyle(t *testing.T) {
	for _, style := range []string{"limit", "limit_maker"} {
		for _, short := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/short=%v", style, short), func(t *testing.T) {
				f := newSharedTriggerFixture(t)
				f.bridge.Quote = func(_ string, now int64) (execution.VisibleQuote, error) {
					return execution.VisibleQuote{Bid: f.price.Sub(decimal.NewFromInt(1)), Ask: f.price.Add(decimal.NewFromInt(1)), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 1000, Bar: f.bar}, nil
				}
				deps := f.rt.BizDeps()
				deps.Config = config.NewSnapshot(&config.Config{OrderType: style, StopEnterBars: 2})
				manager, err := biz.NewSharedOrderMgr(deps, f.rt.SharedExecution(), f.bridge, nil)
				if err != nil {
					t.Fatal(err)
				}
				f.manager = manager
				req := &strat.EnterReq{CommandID: "default-style", StratName: "legacy", Tag: "default-style", Short: short, Amount: 1}
				od, entryErr := manager.EnterOrder(f.job.Symbol, f.job.TimeFrame, req)
				if entryErr != nil || od == nil {
					t.Fatal("default style entry failed", entryErr)
				}
				wantPrice := 99.0
				if short {
					wantPrice = 101
				}
				if od.Enter.OrderType != style || od.Enter.Price != wantPrice || f.adapter.Metrics().Fills != 0 || od.Leverage != 1 {
					t.Fatalf("configured default style lost: %+v leverage=%g", od.Enter, od.Leverage)
				}
				f.price = decimal.NewFromInt(105)
				if _, retryErr := manager.EnterOrder(f.job.Symbol, f.job.TimeFrame, req); retryErr != nil {
					t.Fatal("default price entered stable command identity", retryErr)
				}
				if req.OrderType != core.OrderTypeEmpty || req.Limit != 0 || req.Leverage != 0 {
					t.Fatal("default assembly mutated original request")
				}
				f.observe(t, 100, 3)
				laterPrice := 90.0
				if short {
					laterPrice = 110
				}
				f.observe(t, laterPrice, 4)
				if f.steps(t) != 0 || f.adapter.Metrics().Fills != 0 {
					t.Fatal("configured default limit ignored stop_enter_bars")
				}
				market := &strat.EnterReq{StratName: "legacy", Tag: "explicit-market", Short: short, Amount: 1, OrderType: core.OrderTypeMarket}
				row, marketErr := manager.EnterOrder(f.job.Symbol, f.job.TimeFrame, market)
				if marketErr != nil || row == nil || row.Enter.OrderType != "market" || row.Enter.Price != 0 || f.adapter.Metrics().Fills != 1 {
					t.Fatal("config overrode explicit market style", marketErr)
				}
			})
		}
	}
}

func TestSharedExitResolvesConfiguredStyleWithoutForceDowngrade(t *testing.T) {
	for _, style := range []string{"limit", "limit_maker"} {
		for _, short := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/short=%v", style, short), func(t *testing.T) {
				f := newSharedTriggerFixture(t)
				f.bridge.Quote = func(_ string, now int64) (execution.VisibleQuote, error) {
					return execution.VisibleQuote{Bid: f.price.Sub(decimal.NewFromInt(1)), Ask: f.price.Add(decimal.NewFromInt(1)), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 1000, Bar: f.bar}, nil
				}
				deps := f.rt.BizDeps()
				deps.Config = config.NewSnapshot(&config.Config{OrderType: style})
				manager, err := biz.NewSharedOrderMgr(deps, f.rt.SharedExecution(), f.bridge, nil)
				if err != nil {
					t.Fatal(err)
				}
				f.manager = manager
				od := f.entry(t, &strat.EnterReq{Tag: "exit-default", Short: short, Amount: 1, OrderType: core.OrderTypeMarket})
				req := &strat.ExitReq{CommandID: "default-exit", Tag: "default-exit", Force: true}
				original := *req
				pending, exitErr := manager.ExitOrder(od, req)
				if exitErr != nil {
					t.Fatal(exitErr)
				}
				wantPrice := 101.0
				if short {
					wantPrice = 99
				}
				if pending.Exit == nil || pending.Exit.Price != wantPrice || pending.Exit.OrderType != style || f.adapter.Metrics().Fills != 1 {
					t.Fatalf("forced exit lost configured passive style: %+v", pending.Exit)
				}
				f.price = decimal.NewFromInt(105)
				if _, retryErr := manager.ExitOrder(od, req); retryErr != nil || f.adapter.Metrics().Fills != 1 {
					t.Fatal("default exit retry repriced or resubmitted", retryErr)
				}
				if !reflect.DeepEqual(*req, original) {
					t.Fatal("exit default assembly mutated request")
				}
				laterPrice := 110.0
				if short {
					laterPrice = 90
				}
				f.observe(t, laterPrice, 2)
				if got := f.steps(t); got != 0 || f.adapter.Metrics().Fills != 2 {
					t.Fatalf("default exit did not fill at later quote: %d", got)
				}
				second := f.entry(t, &strat.EnterReq{Tag: "explicit-exit", Short: short, Amount: 1, OrderType: core.OrderTypeMarket})
				if _, explicitErr := manager.ExitOrder(second, &strat.ExitReq{Tag: "explicit-market", Force: true, OrderType: core.OrderTypeMarket}); explicitErr != nil || f.steps(t) != 0 || f.adapter.Metrics().Fills != 4 {
					t.Fatal("configured exit style overrode explicit market", explicitErr)
				}
			})
		}
	}
}

func TestSharedRequestAcceptedBeforeFill(t *testing.T) {
	f := newSharedTriggerFixture(t)
	var kinds []int
	f.job.Strat.OnOrderChange = func(_ *strat.StratJob, _ *ormo.InOutOrder, kind int) { kinds = append(kinds, kind) }
	f.entry(t, &strat.EnterReq{Tag: "pending", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
	if len(kinds) != 1 || kinds[0] != strat.OdChgEnter {
		t.Fatal("pending request acceptance missing", kinds)
	}
	f.observe(t, 90, 2)
	if len(kinds) != 3 || kinds[1] != strat.OdChgOrderChanged || kinds[2] != strat.OdChgEnterFill {
		t.Fatal("fill event mapping incorrect", kinds)
	}
}

func TestSharedOrderSubscribersReceiveRealLifecycle(t *testing.T) {
	f := newSharedTriggerFixture(t)
	var kinds []int
	f.rt.Strategies.AccOdSubs["default"] = []strat.FnOdChange{func(_ string, od *ormo.InOutOrder, kind int) {
		kinds = append(kinds, kind)
		if kind == strat.OdChgEnter && (od.Enter.Filled != 0 || od.Enter.UpdateAt != 0 || od.Enter.Average != 0) {
			t.Error("accepted callback contains future fill metadata")
		}
		if kind == strat.OdChgEnterFill && od.Enter.UpdateAt != 100 {
			t.Error("fill callback lost real timestamp")
		}
	}}
	f.entry(t, &strat.EnterReq{Tag: "lifecycle", Amount: 1})
	if !slices.Equal(kinds, []int{strat.OdChgEnter, strat.OdChgOrderChanged, strat.OdChgEnterFill}) {
		t.Fatal("runtime order subscriptions missing lifecycle", kinds)
	}
}

func TestSharedMakerRestsAndFillsOnLaterVisibleQuote(t *testing.T) {
	f := newSharedTriggerFixture(t)
	f.entry(t, &strat.EnterReq{Tag: "maker", Amount: 1, OrderType: core.OrderTypeLimitMaker, Limit: 90})
	if f.steps(t) != 0 || f.adapter.Metrics().Fills != 0 {
		t.Fatal("maker filled at acceptance")
	}
	f.observe(t, 95, 2)
	if f.steps(t) != 0 {
		t.Fatal("maker filled before visible touch")
	}
	f.observe(t, 90, 3)
	if f.steps(t) != 10 || f.adapter.Metrics().Fills != 1 {
		t.Fatal("maker did not fill at later visible touch")
	}
}

func TestSharedCommandsReplayAndRejectPayloadReuse(t *testing.T) {
	f := newSharedTriggerFixture(t)
	req := &strat.EnterReq{StratName: "legacy", Tag: "stable", Amount: 1, CommandID: "entry-command"}
	first, err := f.manager.EnterOrder(f.job.Symbol, f.job.TimeFrame, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.manager.EnterOrder(f.job.Symbol, f.job.TimeFrame, req)
	if err != nil || second.ID != first.ID || f.adapter.Metrics().Fills != 1 {
		t.Fatal("entry replay traded twice", second, err)
	}
	req.Amount = 2
	if _, err := f.manager.EnterOrder(f.job.Symbol, f.job.TimeFrame, req); err == nil {
		t.Fatal("same ID different payload accepted")
	}
	exit := &strat.ExitReq{CommandID: "exit-command", Tag: "stable-exit", ExitRate: .5}
	if _, err := f.manager.ExitOrder(first, exit); err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.ExitOrder(first, exit); err != nil {
		t.Fatal(err)
	}
	if f.steps(t) != 5 || f.adapter.Metrics().Fills != 2 {
		t.Fatal("exit replay traded twice")
	}
}

func TestSharedCallbackActionsReceiveStableEventCommands(t *testing.T) {
	f := newSharedTriggerFixture(t)
	var event string
	var generated *strat.ExitReq
	f.job.Strat.OnOrderChange = func(job *strat.StratJob, od *ormo.InOutOrder, kind int) {
		if kind != strat.OdChgEnterFill {
			return
		}
		event = od.Info["shared_event_id"].(string)
		if err := job.CloseOrders(&strat.ExitReq{Tag: "callback-exit", ExitRate: .5}); err != nil {
			t.Error(err)
		}
		_, requests := job.DrainOrderRequests()
		if len(requests) != 1 {
			t.Error("callback action missing")
			return
		}
		generated = requests[0]
	}
	f.entry(t, &strat.EnterReq{Tag: "event", Amount: 1})
	if event == "" || generated == nil || generated.CommandID == "" {
		t.Fatal("event provenance missing")
	}
	if _, err := f.manager.ExitOpenOrders("BTC", generated); err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.ExitOpenOrders("BTC", generated); err != nil {
		t.Fatal(err)
	}
	if f.steps(t) != 5 {
		t.Fatal("callback replay emitted extra action")
	}
}
