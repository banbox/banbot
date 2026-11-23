package biz

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

type closeAllRecorder struct {
	IOrderMgr
	closed []int64
}

func (m *closeAllRecorder) ExitOrder(od *ormo.InOutOrder, _ *strat.ExitReq) (*ormo.InOutOrder, *errs.Error) {
	m.closed = append(m.closed, od.ID)
	return od, nil
}

// Execution-backed managers keep terminal projections in the open-order
// registry; close-all must neither count nor exit them.
func TestRemoteCloseAllSkipsTerminalProjections(t *testing.T) {
	orders := ormo.NewOrderState()
	open, lock := orders.GetOpenODs("acc")
	lock.Lock()
	for id, status := range map[int64]int64{1: ormo.InOutStatusFullEnter, 2: ormo.InOutStatusPartExit, 3: ormo.InOutStatusFullExit, 4: ormo.InOutStatusDelete} {
		open[id] = &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: id, Status: status, Strategy: "s"}}
	}
	lock.Unlock()
	trading := NewTradingState()
	recorder := &closeAllRecorder{}
	trading.SetOrderManager("acc", recorder)
	svc := NewRemoteCommandServiceWithRuntimeDeps(RuntimeDeps{Orders: orders, Trading: trading, Accounts: map[string]*config.AccountConfig{"acc": {}}})
	res, err := svc.CloseOrders(RemoteCommand{Source: RemoteSourceWeb, Actor: "u", Account: "acc", All: true, Confirmed: true})
	if err != nil || res == nil {
		t.Fatal(res, err)
	}
	if res.CloseNum != 2 || res.FailNum != 0 || len(recorder.closed) != 2 {
		t.Fatalf("closed terminal projections: res=%+v exited=%v", res, recorder.closed)
	}
	for _, id := range recorder.closed {
		if id == 3 || id == 4 {
			t.Fatal("terminal order was exited", id)
		}
	}
}
