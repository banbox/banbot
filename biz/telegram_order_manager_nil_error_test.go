package biz

import (
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

type telegramFakeOrderMgr struct{ IOrderMgr }

func (telegramFakeOrderMgr) ExitOrder(od *ormo.InOutOrder, req *strat.ExitReq) (*ormo.InOutOrder, *errs.Error) {
	return od, nil
}

// The manager methods return the builtin `error`; the command service returns
// *errs.Error. A nil *errs.Error must not surface as a non-nil error (typed-nil),
// otherwise Telegram reports failure for every successful command.
func TestTelegramOrderManagerSuccessReturnsNilError(t *testing.T) {
	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(1000)
	state := &core.State{}
	trading := NewTradingState()
	trading.SetOrderManager("acc", telegramFakeOrderMgr{})
	orders := ormo.NewOrderState()
	open, lock := orders.GetOpenODs("acc")
	lock.Lock()
	open[7] = &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 7, Symbol: "BTC/USDT"}}
	lock.Unlock()
	// The service rate-limits identical source/actor/action within 1s, so each
	// call gets its own manager (own service) over the same runtime state.
	newMgr := func() *TelegramOrderManager {
		return NewTelegramOrderManagerWithRuntimeDeps(RuntimeDeps{
			Core: state, Clock: clock, Orders: orders, Trading: trading,
			Accounts: map[string]*config.AccountConfig{"acc": {}},
		})
	}

	if err := newMgr().CloseOrder("acc", 7); err != nil {
		t.Fatalf("CloseOrder success returned non-nil error: %v", err)
	}
	if err := newMgr().EnableTrading("acc"); err != nil {
		t.Fatalf("EnableTrading success returned non-nil error: %v", err)
	}
	if _, err := newMgr().DisableTrading("acc", 1); err != nil {
		t.Fatalf("DisableTrading success returned non-nil error: %v", err)
	}
	if _, _, err := newMgr().CloseAllOrders("acc"); err != nil {
		t.Fatalf("CloseAllOrders success returned non-nil error: %v", err)
	}
}
