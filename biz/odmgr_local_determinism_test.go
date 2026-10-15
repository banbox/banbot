package biz

import (
	"slices"
	"testing"

	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type deterministicFillExchange struct {
	banexg.BanExchange
}

func (e *deterministicFillExchange) CalculateFee(string, string, string, float64, float64, bool,
	map[string]interface{}) (*banexg.Fee, *errs.Error) {
	return &banexg.Fee{}, nil
}

func TestLegacyFillPendingOrdersUsesStableBusinessOrder(t *testing.T) {
	testFillPendingOrdersUsesStableBusinessOrder(t, true, false)
}

func TestDeterministicFillPendingOrdersUsesStableBusinessOrder(t *testing.T) {
	testFillPendingOrdersUsesStableBusinessOrder(t, false, true)
}

func TestFrozenReplayFillPendingOrdersPreservesSuppliedBusinessOrder(t *testing.T) {
	oldExchange := exg.Default
	oldBackTest := core.BackTestMode
	oldEnvReal := core.EnvReal
	oldLiveMode := core.LiveMode
	oldData := config.Data
	oldPairs := config.Data.Pairs
	oldPairFilters := config.Data.PairFilters
	oldPairMgr := config.Data.PairMgr
	exg.Default = &deterministicFillExchange{}
	core.BackTestMode = true
	core.EnvReal = true
	core.LiveMode = false
	config.Data.BTLegacyWallet = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true
	config.Data.Pairs = []string{"DETERMINISTIC/USDT"}
	config.Data.PairFilters = nil
	config.Data.PairMgr = &config.PairMgrConfig{}
	t.Cleanup(func() {
		exg.Default = oldExchange
		core.BackTestMode = oldBackTest
		core.EnvReal = oldEnvReal
		core.LiveMode = oldLiveMode
		config.Data = oldData
		config.Data.Pairs = oldPairs
		config.Data.PairFilters = oldPairFilters
		config.Data.PairMgr = oldPairMgr
	})

	exs := &orm.ExSymbol{ID: 155, Symbol: "DETERMINISTIC/USDT"}
	evt := orm.NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
		Time: 1_700_000_000_000, Open: 100, High: 100, Low: 100, Close: 100,
	}, nil, true, true)
	wallets := &BanWallets{Items: map[string]*ItemWallet{
		"USDT": {Available: 100, Pendings: map[string]float64{}, Frozens: map[string]float64{}},
	}}
	var callbackIDs []int64
	var admittedIDs []int64
	mgr := &LocalOrderMgr{OrderMgr: OrderMgr{Account: config.DefAcc}}
	mgr.callBack = func(od *ormo.InOutOrder, _ bool) {
		callbackIDs = append(callbackIDs, od.ID)
		if _, err := wallets.CostAva(od.Key(), "USDT", map[int64]float64{1: 60, 2: 50, 3: 40}[od.ID], false, 0.9); err == nil {
			admittedIDs = append(admittedIDs, od.ID)
		}
	}
	orders := []*ormo.InOutOrder{
		deterministicPendingExit(3, exs.Symbol),
		deterministicPendingExit(2, exs.Symbol),
		deterministicPendingExit(1, exs.Symbol),
	}

	if _, err := mgr.fillPendingOrders(orders, evt); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(callbackIDs, []int64{3, 2, 1}) {
		t.Fatalf("callback order = %v, want supplied order [3 2 1]", callbackIDs)
	}
	if !slices.Equal(admittedIDs, []int64{3, 2}) {
		t.Fatalf("admitted orders = %v, want supplied-order result [3 2]", admittedIDs)
	}
}

func TestFrozenReplayCallbackOrdersPreserveSuppliedOrder(t *testing.T) {
	oldBackTest, oldData, oldPairs, oldFilters, oldMgr := core.BackTestMode, config.Data,
		config.Pairs, config.PairFilters, config.PairMgr
	core.BackTestMode = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true
	config.Data.Pairs = []string{"DETERMINISTIC/USDT"}
	config.Data.PairFilters = nil
	config.Data.PairMgr = &config.PairMgrConfig{}
	t.Cleanup(func() {
		core.BackTestMode, config.Data = oldBackTest, oldData
		config.Data.Pairs, config.Data.PairFilters, config.Data.PairMgr = oldPairs, oldFilters, oldMgr
	})

	for _, permutation := range [][]int64{{3, 1, 2}, {2, 3, 1}, {1, 2, 3}} {
		orders := make([]*ormo.InOutOrder, 0, len(permutation))
		for _, id := range permutation {
			orders = append(orders, &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: id}})
		}
		sortOrdersForBacktest(orders)
		if got := orderIDs(orders); !slices.Equal(got, permutation) {
			t.Fatalf("permutation %v callback orders = %v, want supplied order", permutation, got)
		}
	}
}

func TestFrozenReplayMapRescanCanonicalizesOrder(t *testing.T) {
	oldMode, oldData := core.BackTestMode, config.Data
	oldPairs, oldFilters, oldMgr := config.Pairs, config.PairFilters, config.PairMgr
	core.BackTestMode = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true
	config.Data.Pairs = []string{"DETERMINISTIC/USDT"}
	config.Data.PairFilters = nil
	config.Data.PairMgr = &config.PairMgrConfig{}
	t.Cleanup(func() {
		core.BackTestMode, config.Data = oldMode, oldData
		config.Data.Pairs, config.Data.PairFilters, config.Data.PairMgr = oldPairs, oldFilters, oldMgr
	})

	for _, permutation := range [][]int64{{3, 1, 2}, {2, 3, 1}, {1, 2, 3}} {
		orders := make([]*ormo.InOutOrder, 0, len(permutation))
		for _, id := range permutation {
			orders = append(orders, &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: id}})
		}
		sortMapOrdersForBacktest(orders)
		if got := orderIDs(orders); !slices.Equal(got, []int64{1, 2, 3}) {
			t.Fatalf("permutation %v map rescan order = %v, want [1 2 3]", permutation, got)
		}
	}
}

func TestFrozenReplayMapBoundaryPreservesSuppliedOrder(t *testing.T) {
	oldMode, oldData := core.BackTestMode, config.Data
	oldPairs, oldFilters, oldMgr := config.Pairs, config.PairFilters, config.PairMgr
	core.BackTestMode = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true
	config.Data.Pairs = []string{"DETERMINISTIC/USDT"}
	config.Data.PairFilters = nil
	config.Data.PairMgr = &config.PairMgrConfig{}
	t.Cleanup(func() {
		core.BackTestMode, config.Data = oldMode, oldData
		config.Data.Pairs, config.Data.PairFilters, config.Data.PairMgr = oldPairs, oldFilters, oldMgr
	})
	orders := []*ormo.InOutOrder{
		{IOrder: &ormo.IOrder{ID: 3}},
		{IOrder: &ormo.IOrder{ID: 1}},
		{IOrder: &ormo.IOrder{ID: 2}},
	}
	if got := executionOrderView(orders); !slices.Equal(orderIDs(got), []int64{3, 1, 2}) {
		t.Fatalf("frozen map boundary changed supplied order: %v", orderIDs(got))
	}
}

func testFillPendingOrdersUsesStableBusinessOrder(t *testing.T, legacy, deterministic bool) {
	oldExchange := exg.Default
	oldBackTest := core.BackTestMode
	oldEnvReal := core.EnvReal
	oldLiveMode := core.LiveMode
	oldData := config.Data
	exg.Default = &deterministicFillExchange{}
	core.BackTestMode = true
	core.EnvReal = true
	core.LiveMode = false
	config.Data.BTLegacyWallet = legacy
	config.Data.BTStrict = deterministic
	if legacy {
		config.Data.BTLegacyWallet = true
	}
	t.Cleanup(func() {
		exg.Default = oldExchange
		core.BackTestMode = oldBackTest
		core.EnvReal = oldEnvReal
		core.LiveMode = oldLiveMode
		config.Data = oldData
	})

	exs := &orm.ExSymbol{ID: 155, Symbol: "DETERMINISTIC/USDT"}
	evt := orm.NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
		Time: 1_700_000_000_000, Open: 100, High: 100, Low: 100, Close: 100,
	}, nil, true, true)
	permutations := [][]int64{{1, 2, 3}, {3, 2, 1}, {2, 3, 1}}
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
			orders = append(orders, deterministicPendingExit(id, exs.Symbol))
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

func TestDeterministicExitAndFillUsesStableBusinessOrder(t *testing.T) {
	oldExchange, oldMode := exg.Default, core.BackTestMode
	oldEnvReal, oldLiveMode := core.EnvReal, core.LiveMode
	oldData := config.Data
	exg.Default = &deterministicFillExchange{}
	core.BackTestMode, core.EnvReal, core.LiveMode = true, true, false
	config.Data.BTLegacyWallet, config.Data.BTStrict = false, true
	com.SetBarPrice("DETERMINISTIC/USDT", 100)
	t.Cleanup(func() {
		exg.Default, core.BackTestMode = oldExchange, oldMode
		core.EnvReal, core.LiveMode = oldEnvReal, oldLiveMode
		config.Data = oldData
	})

	for _, permutation := range [][]int64{{11, 12, 13}, {13, 12, 11}, {12, 13, 11}} {
		var callbackIDs []int64
		mgr := &LocalOrderMgr{OrderMgr: OrderMgr{Account: config.DefAcc}}
		mgr.callBack = func(order *ormo.InOutOrder, _ bool) {
			callbackIDs = append(callbackIDs, order.ID)
		}
		orders := make([]*ormo.InOutOrder, 0, len(permutation))
		for _, id := range permutation {
			order := deterministicPendingExit(id, "DETERMINISTIC/USDT")
			order.ExitTag, order.Exit = "", nil
			orders = append(orders, order)
		}
		if err := mgr.ExitAndFill(orders, &strat.ExitReq{Tag: "deterministic", Force: true}); err != nil {
			t.Fatalf("permutation %v: %v", permutation, err)
		}
		if !slices.Equal(callbackIDs, []int64{11, 12, 13}) {
			t.Fatalf("permutation %v: callback order = %v", permutation, callbackIDs)
		}
	}
}

func TestCompareExitOpenOrdersIsTransitiveAtCentBoundary(t *testing.T) {
	orders := []*ormo.InOutOrder{
		{IOrder: &ormo.IOrder{ID: 1, InitPrice: 1}, Enter: &ormo.ExOrder{Amount: 0, Filled: 0}},
		{IOrder: &ormo.IOrder{ID: 2, InitPrice: 1}, Enter: &ormo.ExOrder{Amount: 0.004, Filled: 0}},
		{IOrder: &ormo.IOrder{ID: 3, InitPrice: 1}, Enter: &ormo.ExOrder{Amount: 0.008, Filled: 0}},
	}
	want := []int64{3, 1, 2}
	for _, permutation := range [][]*ormo.InOutOrder{
		{orders[0], orders[1], orders[2]},
		{orders[2], orders[1], orders[0]},
		{orders[1], orders[0], orders[2]},
	} {
		slices.SortFunc(permutation, func(a, b *ormo.InOutOrder) int {
			return compareExitOpenOrders(a, b, false)
		})
		if got := orderIDs(permutation); !slices.Equal(got, want) {
			t.Fatalf("cent-boundary order = %v, want %v", got, want)
		}
	}
}

func deterministicPendingExit(id int64, symbol string) *ormo.InOutOrder {
	return &ormo.InOutOrder{
		IOrder: &ormo.IOrder{
			ID: id, Symbol: symbol, Timeframe: "1m", Status: ormo.InOutStatusFullEnter,
			ExitTag: "deterministic", InitPrice: 100, EnterAt: 1_699_999_940_000 + id, Leverage: 1,
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
