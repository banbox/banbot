package live

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type closeToolExchange struct {
	banexg.BanExchange
	openCalls     []string
	positionCalls []string
	cancelCalls   []string
	createCalls   []string
	orders        []*banexg.Order
	positions     []*banexg.Position
}

func (e *closeToolExchange) FetchOpenOrders(_ string, _ int64, _ int, params map[string]interface{}) ([]*banexg.Order, *errs.Error) {
	e.openCalls = append(e.openCalls, params[banexg.ParamAccount].(string))
	return e.orders, nil
}

func (e *closeToolExchange) FetchAccountPositions(_ []string, params map[string]interface{}) ([]*banexg.Position, *errs.Error) {
	e.positionCalls = append(e.positionCalls, params[banexg.ParamAccount].(string))
	return e.positions, nil
}

func (e *closeToolExchange) CancelOrder(id, _ string, params map[string]interface{}) (*banexg.Order, *errs.Error) {
	e.cancelCalls = append(e.cancelCalls, params[banexg.ParamAccount].(string)+":"+id)
	return &banexg.Order{ID: id, Status: "canceled"}, nil
}

func (e *closeToolExchange) CreateOrder(symbol, _ string, _ string, _ float64, _ float64, params map[string]interface{}) (*banexg.Order, *errs.Error) {
	e.createCalls = append(e.createCalls, params[banexg.ParamAccount].(string)+":"+symbol)
	return &banexg.Order{Status: "filled"}, nil
}

func closeToolDeps(exchange banexg.BanExchange) biz.RuntimeDeps {
	state := &core.State{IsContract: true}
	return biz.RuntimeDeps{
		Core: state, Clock: btime.NewClockState(true, nil), Exchange: exchange,
		Config:   config.NewSnapshot(&config.Config{StakeCurrency: []string{"USDT"}}),
		Accounts: map[string]*config.AccountConfig{"a": {}, "b": {}},
	}
}

func TestCloseOrdersWithRuntimeDepsRequiresConfirmationBeforeExchangeCalls(t *testing.T) {
	exchange := &closeToolExchange{
		orders:    []*banexg.Order{{ID: "open", Symbol: "BTC/USDT", Status: "open"}},
		positions: []*banexg.Position{{Symbol: "BTC/USDT", Side: banexg.PosSideLong, Contracts: 1}},
	}
	_, err := CloseOrdersWithRuntimeDeps(closeToolDeps(exchange), TradeCloseRequest{Exchange: true})
	if err == nil {
		t.Fatal("unconfirmed close-all unexpectedly succeeded")
	}
	if len(exchange.openCalls)+len(exchange.positionCalls)+len(exchange.cancelCalls)+len(exchange.createCalls) != 0 {
		t.Fatalf("unconfirmed close touched exchange: %+v", exchange)
	}
}

// Without a bound execution account (embedded or Hub) closing is refused in
// every env and never reaches the venue.
func TestCloseRequiresExecutionOwner(t *testing.T) {
	for _, envReal := range []bool{false, true} {
		for _, raw := range []bool{false, true} {
			exchange := &closeToolExchange{}
			deps := closeToolDeps(exchange)
			deps.Core.EnvReal = envReal
			_, err := CloseOrdersWithRuntimeDeps(deps, TradeCloseRequest{Accounts: []string{"a"}, Exchange: raw, Confirmed: true})
			if err == nil || !strings.Contains(err.Error(), "execution account") {
				t.Fatalf("close without an execution owner = %v, want refusal", err)
			}
			if len(exchange.openCalls)+len(exchange.positionCalls)+len(exchange.cancelCalls)+len(exchange.createCalls) != 0 {
				t.Fatal("rejected close accessed the venue", exchange)
			}
		}
	}
}

func TestCloseAccountsMatchOwnerIgnoresDefaultAccount(t *testing.T) {
	if !closeAccountsMatchOwner([]string{"owner"}, "owner") {
		t.Fatal("owner account must be accepted")
	}
	if closeAccountsMatchOwner([]string{"owner", "other"}, "owner") {
		t.Fatal("foreign account must be rejected")
	}
}
