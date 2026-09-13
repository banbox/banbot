package live

import (
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

func TestCloseOrdersWithRuntimeDepsScopesExchangeCallsToSelectedAccount(t *testing.T) {
	exchange := &closeToolExchange{
		orders:    []*banexg.Order{{ID: "open", Symbol: "BTC/USDT", Status: "open"}},
		positions: []*banexg.Position{{Symbol: "BTC/USDT", Side: banexg.PosSideLong, Contracts: 1}},
	}
	result, err := CloseOrdersWithRuntimeDeps(closeToolDeps(exchange), TradeCloseRequest{
		Accounts: []string{"a"}, Exchange: true, Confirmed: true,
	})
	if err != nil || result.Closed != 1 {
		t.Fatalf("close result = %+v, %v", result, err)
	}
	for _, calls := range [][]string{exchange.openCalls, exchange.positionCalls, exchange.cancelCalls, exchange.createCalls} {
		for _, call := range calls {
			if call[0] != 'a' {
				t.Fatalf("unselected account received exchange call: %q", call)
			}
		}
	}
}

func TestCloseOrdersWithRuntimeDepsHonorsPairFilter(t *testing.T) {
	exchange := &closeToolExchange{
		orders:    []*banexg.Order{{ID: "open", Symbol: "ETH/USDT", Status: "open"}},
		positions: []*banexg.Position{{Symbol: "ETH/USDT", Side: banexg.PosSideLong, Contracts: 1}},
	}
	result, err := CloseOrdersWithRuntimeDeps(closeToolDeps(exchange), TradeCloseRequest{
		Accounts: []string{"a"}, Pairs: []string{"BTC/USDT"}, Exchange: true, Confirmed: true,
	})
	if err != nil || result.Closed != 0 {
		t.Fatalf("filtered close result = %+v, %v", result, err)
	}
	if len(exchange.cancelCalls) != 0 || len(exchange.createCalls) != 0 {
		t.Fatalf("unmatched pair triggered mutations: cancel=%v create=%v", exchange.cancelCalls, exchange.createCalls)
	}
}
