package biz

import (
	"slices"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type issue155Exchange struct {
	banexg.BanExchange
}

func (e *issue155Exchange) CalculateFee(string, string, string, float64, float64, bool, map[string]interface{}) (*banexg.Fee, *errs.Error) {
	return &banexg.Fee{}, nil
}

func TestFillPendingOrdersUsesStableBusinessOrder(t *testing.T) {
	oldExchange := exg.Default
	oldBackTest := core.BackTestMode
	oldEnvReal := core.EnvReal
	oldLiveMode := core.LiveMode
	exg.Default = &issue155Exchange{}
	core.BackTestMode = true
	core.EnvReal = true
	core.LiveMode = false
	t.Cleanup(func() {
		exg.Default = oldExchange
		core.BackTestMode = oldBackTest
		core.EnvReal = oldEnvReal
		core.LiveMode = oldLiveMode
	})

	exs := &orm.ExSymbol{ID: 155, Symbol: "ISSUE155/USDT"}
	evt := orm.NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
		Time: 1_700_000_000_000, Open: 100, High: 100, Low: 100, Close: 100,
	}, nil, true, true)
	permutations := [][]int64{
		{1, 2, 3},
		{3, 2, 1},
		{2, 3, 1},
	}
	costs := map[int64]float64{1: 60, 2: 50, 3: 40}

	for _, permutation := range permutations {
		wallets := &BanWallets{Items: map[string]*ItemWallet{
			"USDT": {Available: 100, Pendings: map[string]float64{}, Frozens: map[string]float64{}},
		}}
		var callbackIDs []int64
		var admittedIDs []int64
		mgr := &LocalOrderMgr{OrderMgr: OrderMgr{Account: config.DefAcc}}
		mgr.callBack = func(od *ormo.InOutOrder, _ bool) {
			callbackIDs = append(callbackIDs, od.ID)
			if _, err := wallets.CostAva(od.Key(), "USDT", costs[od.ID], false, 0.9); err == nil {
				admittedIDs = append(admittedIDs, od.ID)
			}
		}
		orders := make([]*ormo.InOutOrder, 0, len(permutation))
		for _, id := range permutation {
			orders = append(orders, issue155PendingExit(id, exs.Symbol))
		}

		if _, err := mgr.fillPendingOrders(orders, evt); err != nil {
			t.Fatalf("permutation %v: fillPendingOrders() error: %v", permutation, err)
		}
		if !slices.Equal(callbackIDs, []int64{1, 2, 3}) {
			t.Errorf("permutation %v: callback order = %v, want [1 2 3]", permutation, callbackIDs)
		}
		if !slices.Equal(admittedIDs, []int64{1, 3}) {
			t.Errorf("permutation %v: admitted orders = %v, want [1 3]", permutation, admittedIDs)
		}
		if got := wallets.Items["USDT"].Available; got != 0 {
			t.Errorf("permutation %v: available = %v, want 0", permutation, got)
		}
	}
}

func issue155PendingExit(id int64, symbol string) *ormo.InOutOrder {
	return &ormo.InOutOrder{
		IOrder: &ormo.IOrder{
			ID: id, Symbol: symbol, Timeframe: "1m", Status: ormo.InOutStatusFullEnter,
			ExitTag: "issue155", InitPrice: 100, EnterAt: 1_699_999_940_000 + id, Leverage: 1,
		},
		Enter: &ormo.ExOrder{
			Enter: true, Side: banexg.OdSideBuy, Price: 100, Average: 100,
			Amount: 1, Filled: 1, Status: ormo.OdStatusClosed,
		},
		Exit: &ormo.ExOrder{
			Side: banexg.OdSideSell, OrderType: banexg.OdTypeMarket,
			CreateAt: 1_699_999_940_000, Amount: 1, Status: ormo.OdStatusInit,
		},
	}
}
