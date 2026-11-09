package biz

import (
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type badSymbolExchange struct{ banexg.BanExchange }

func (badSymbolExchange) PriceSymbolParts(string) ([4]string, *errs.Error) {
	return [4]string{}, errs.NewMsg(errs.CodeRunTime, "bad symbol")
}

func TestCutOrderSkipsWalletSplitWhenSymbolPartsFail(t *testing.T) {
	mgr := &OrderMgr{Account: "a", runtimeDeps: true, runtimeCore: &core.State{}, exchange: badSymbolExchange{}}
	od := &ormo.InOutOrder{
		IOrder: &ormo.IOrder{ID: 1, Symbol: "BAD", Strategy: "s"},
		Enter:  &ormo.ExOrder{Amount: 2},
	}
	part := mgr.CutOrder(od, 0.5, 0)
	if part == nil || part.Enter == nil || part.Enter.Amount != 1 {
		t.Fatalf("part = %+v", part)
	}
	if mgr.wallet != nil {
		t.Fatal("wallet split must be skipped without base/quote")
	}
}

func TestCloseAccOrdersMissingManagerReturnsError(t *testing.T) {
	old := accOdMgrs
	accOdMgrs = map[string]IOrderMgr{}
	defer func() { accOdMgrs = old }()
	od := &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 1}}
	ok, fail, err := CloseAccOrders("none", []*ormo.InOutOrder{od}, nil)
	if err == nil || ok != 0 || fail != 1 {
		t.Fatalf("got %d %d %v", ok, fail, err)
	}
}
