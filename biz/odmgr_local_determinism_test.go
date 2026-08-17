package biz

import (
	"crypto/sha256"
	"fmt"
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
	oldPairs := config.Pairs
	oldPairFilters := config.PairFilters
	oldPairMgr := config.PairMgr
	exg.Default = &deterministicFillExchange{}
	core.BackTestMode = true
	core.EnvReal = true
	core.LiveMode = false
	config.Data.BTLegacyWallet = true
	config.Data.BTStrict = true
	config.Data.BTNoKlineDownload = true
	config.Pairs = []string{"DETERMINISTIC/USDT"}
	config.PairFilters = nil
	config.PairMgr = &config.PairMgrConfig{}
	t.Cleanup(func() {
		exg.Default = oldExchange
		core.BackTestMode = oldBackTest
		core.EnvReal = oldEnvReal
		core.LiveMode = oldLiveMode
		config.Data = oldData
		config.Pairs = oldPairs
		config.PairFilters = oldPairFilters
		config.PairMgr = oldPairMgr
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

func TestFrozenReplayCallbackOrdersCanonicalizeMapRescan(t *testing.T) {
	oldBackTest, oldData, oldPairs, oldFilters, oldMgr := core.BackTestMode, config.Data,
		config.Pairs, config.PairFilters, config.PairMgr
	core.BackTestMode = true
	config.Data.BTStrict = true
	config.Pairs = []string{"DETERMINISTIC/USDT"}
	config.PairFilters = nil
	config.PairMgr = &config.PairMgrConfig{}
	t.Cleanup(func() {
		core.BackTestMode, config.Data = oldBackTest, oldData
		config.Pairs, config.PairFilters, config.PairMgr = oldPairs, oldFilters, oldMgr
	})

	for _, permutation := range [][]int64{{3, 1, 2}, {2, 3, 1}, {1, 2, 3}} {
		orders := make([]*ormo.InOutOrder, 0, len(permutation))
		for _, id := range permutation {
			orders = append(orders, &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: id}})
		}
		sortOrdersForBacktest(orders)
		if got := orderIDs(orders); !slices.Equal(got, []int64{1, 2, 3}) {
			t.Fatalf("permutation %v callback orders = %v, want [1 2 3]", permutation, got)
		}
	}
}

func TestFrozenReplayMapBoundaryKeepsOrderAndEquityHashesStable(t *testing.T) {
	oldExchange, oldBackTest, oldEnvReal, oldLiveMode := exg.Default, core.BackTestMode, core.EnvReal, core.LiveMode
	oldData, oldPairs, oldFilters, oldMgr := config.Data, config.Pairs, config.PairFilters, config.PairMgr
	exg.Default = &deterministicFillExchange{}
	core.BackTestMode, core.EnvReal, core.LiveMode = true, true, false
	config.Data.BTLegacyWallet, config.Data.BTStrict, config.Data.BTNoKlineDownload = true, true, true
	config.Pairs = []string{"DETERMINISTIC/USDT"}
	config.PairFilters = nil
	config.PairMgr = &config.PairMgrConfig{}
	t.Cleanup(func() {
		exg.Default, core.BackTestMode, core.EnvReal, core.LiveMode = oldExchange, oldBackTest, oldEnvReal, oldLiveMode
		config.Data, config.Pairs, config.PairFilters, config.PairMgr = oldData, oldPairs, oldFilters, oldMgr
	})

	exs := &orm.ExSymbol{ID: 155, Symbol: "DETERMINISTIC/USDT"}
	evt := orm.NewDataSeriesFromKline(exs, "1m", &banexg.Kline{
		Time: 1_700_000_000_000, Open: 100, High: 100, Low: 100, Close: 100,
	}, nil, true, true)
	costs := map[int64]float64{1: 60, 2: 50, 3: 40}
	var wantHash [sha256.Size]byte

	for run, permutation := range [][]int64{{3, 1, 2}, {2, 3, 1}, {1, 2, 3}} {
		wallets := &BanWallets{Items: map[string]*ItemWallet{
			"USDT": {Available: 100, Pendings: map[string]float64{}, Frozens: map[string]float64{}},
		}}
		var executedIDs, admittedIDs []int64
		job := &strat.StratJob{
			Strat: &strat.TradeStrat{HedgeOff: true}, Symbol: exs, TimeFrame: "1m",
			CloseLong: true, CloseShort: true,
		}
		mgr := &LocalOrderMgr{OrderMgr: OrderMgr{Account: config.DefAcc}}
		mgr.callBack = func(od *ormo.InOutOrder, _ bool) {
			executedIDs = append(executedIDs, od.ID)
			if _, err := wallets.CostAva(od.Key(), "USDT", costs[od.ID], false, 0.9); err == nil {
				admittedIDs = append(admittedIDs, od.ID)
			}
			if job.Strat.HedgeOff {
				closeSideOrders(job, !od.Short)
			}
		}
		ordersByID := make(map[int64]*ormo.InOutOrder, len(permutation))
		for _, id := range permutation {
			order := deterministicPendingExit(id, exs.Symbol)
			order.EnterAt = 1_699_999_940_000
			order.Short = id == 2
			ordersByID[id] = order
		}

		executionOrders := executionOpenOrders(ordersByID)
		job.UpdateOrders(executionOrders)
		if _, err := mgr.fillPendingOrders(executionOrders, evt); err != nil {
			t.Fatalf("permutation %v: fillPendingOrders() error: %v", permutation, err)
		}
		hedgeExitIDs := make([]int64, len(job.Exits))
		for index, exit := range job.Exits {
			hedgeExitIDs[index] = exit.OrderID
		}
		if !slices.Equal(executedIDs, []int64{1, 2, 3}) || !slices.Equal(admittedIDs, []int64{1, 3}) ||
			!slices.Equal(hedgeExitIDs, []int64{2, 3}) || wallets.Items["USDT"].Available != 0 {
			t.Fatalf("permutation %v: orders=%v admitted=%v hedge_exits=%v equity=%v", permutation,
				executedIDs, admittedIDs, hedgeExitIDs, wallets.Items["USDT"].Available)
		}
		summary := fmt.Sprintf("orders=%v;admitted=%v;hedge_exits=%v;equity=%.8f", executedIDs,
			admittedIDs, hedgeExitIDs, wallets.Items["USDT"].Available)
		gotHash := sha256.Sum256([]byte(summary))
		if run == 0 {
			wantHash = gotHash
			continue
		}
		if gotHash != wantHash {
			t.Fatalf("permutation %v hash = %x, want %x; summary=%s", permutation, gotHash, wantHash, summary)
		}
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
