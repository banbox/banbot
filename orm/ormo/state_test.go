package ormo

import (
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/sasha-s/go-deadlock"
)

func TestOrderStateIsolatesSameAccountAndFakeIDs(t *testing.T) {
	first := NewOrderState()
	second := NewOrderState()

	firstOpen, firstOpenLock := first.GetOpenODs("shared")
	secondOpen, secondOpenLock := second.GetOpenODs("shared")
	if firstOpenLock == secondOpenLock {
		t.Fatal("states share an open-order account lock")
	}
	firstOpen[1] = &InOutOrder{}
	if _, ok := secondOpen[1]; ok {
		t.Fatal("open orders leaked between states")
	}

	firstTrigger, firstTriggerLock := first.GetTriggerODs("shared")
	secondTrigger, secondTriggerLock := second.GetTriggerODs("shared")
	if firstTriggerLock == secondTriggerLock {
		t.Fatal("states share a trigger-order account lock")
	}
	firstTrigger["BTC/USDT"] = map[int64]*InOutOrder{2: &InOutOrder{}}
	if _, ok := secondTrigger["BTC/USDT"]; ok {
		t.Fatal("trigger orders leaked between states")
	}

	first.SetSyncStamp("shared", 42)
	if second.GetSyncStamp("shared") != 0 {
		t.Fatal("sync stamps leaked between states")
	}
	if first.GetOrderLock("order-key") == second.GetOrderLock("order-key") {
		t.Fatal("states share an order lock")
	}

	first.SetTask("shared", &BotTask{ID: 9})
	if second.GetTask("shared") != nil || second.GetTaskAcc(9) != "" {
		t.Fatal("task registry leaked between states")
	}

	order := &InOutOrder{IOrder: &IOrder{ID: 7}}
	if !first.AddHistoricalOrder(order) || first.AddHistoricalOrder(order) {
		t.Fatal("historical order dedupe did not hold")
	}
	if len(second.HistoricalOrders()) != 0 {
		t.Fatal("historical orders leaked between states")
	}

	first.SetEditListener(func(*InOutOrder, string) {})
	if second.GetEditListener() != nil {
		t.Fatal("edit listener leaked between states")
	}

	if first.NextFakeID() != 1 || second.NextFakeID() != 1 || first.NextFakeID() != 2 {
		t.Fatal("fake IDs are not independently allocated")
	}
}

func TestOrderStateResetIsScoped(t *testing.T) {
	first := NewOrderState()
	second := NewOrderState()

	firstOpen, _ := first.GetOpenODs("shared")
	firstOpen[1] = &InOutOrder{}
	first.SetSyncStamp("shared", 42)
	first.SetTask("shared", &BotTask{ID: 9})
	first.AddHistoricalOrder(&InOutOrder{IOrder: &IOrder{ID: 7}})
	first.SetEditListener(func(*InOutOrder, string) {})
	first.NextFakeID()

	secondOpen, _ := second.GetOpenODs("shared")
	secondOpen[2] = &InOutOrder{}
	second.SetSyncStamp("shared", 84)
	second.SetTask("shared", &BotTask{ID: 10})
	second.AddHistoricalOrder(&InOutOrder{IOrder: &IOrder{ID: 8}})
	second.SetEditListener(func(*InOutOrder, string) {})
	second.NextFakeID()

	first.Reset()
	resetOpen, _ := first.GetOpenODs("shared")
	if len(resetOpen) != 0 || first.GetSyncStamp("shared") != 0 || first.GetTask("shared") != nil ||
		len(first.HistoricalOrders()) != 0 || first.GetEditListener() != nil || first.NextFakeID() != 1 {
		t.Fatal("reset did not clear the first state")
	}

	if len(secondOpen) != 1 || second.GetSyncStamp("shared") != 84 || second.GetTask("shared") == nil ||
		len(second.HistoricalOrders()) != 1 || second.GetEditListener() == nil || second.NextFakeID() != 2 {
		t.Fatal("reset of the first state affected the second state")
	}
}

func TestLegacyStateUsesLegacyRegistries(t *testing.T) {
	oldOpen := accOpenODs
	oldSync := accSyncStamps
	oldTrigger := accTriggerODs
	oldOpenLocks := lockOpenMap
	oldTriggerLocks := lockTriggerMap
	oldOrderLocks := lockOds
	oldHist := HistODs
	oldDone := doneODs
	oldFakeID := FakeOdId
	oldListener := OdEditListener
	oldTasks := accTasks
	oldTaskAccounts := taskIdAccMap
	t.Cleanup(func() {
		accOpenODs = oldOpen
		accSyncStamps = oldSync
		accTriggerODs = oldTrigger
		lockOpenMap = oldOpenLocks
		lockTriggerMap = oldTriggerLocks
		lockOds = oldOrderLocks
		HistODs = oldHist
		doneODs = oldDone
		FakeOdId = oldFakeID
		OdEditListener = oldListener
		accTasks = oldTasks
		taskIdAccMap = oldTaskAccounts
	})

	accOpenODs = make(map[string]map[int64]*InOutOrder)
	accSyncStamps = make(map[string]int64)
	accTriggerODs = make(map[string]map[string]map[int64]*InOutOrder)
	lockOpenMap = make(map[string]*deadlock.Mutex)
	lockTriggerMap = make(map[string]*deadlock.Mutex)
	lockOds = make(map[string]*deadlock.Mutex)
	HistODs = nil
	doneODs = make(map[int64]bool)
	FakeOdId = 1
	OdEditListener = nil
	accTasks = make(map[string]*BotTask)
	taskIdAccMap = make(map[int64]string)

	legacy := legacyStateView()
	orders, _ := legacy.GetOpenODs("legacy")
	orders[1] = &InOutOrder{}
	legacy.SetSyncStamp("legacy", 7)
	legacy.SetTask("legacy", &BotTask{ID: 11})
	legacy.AddHistoricalOrder(&InOutOrder{IOrder: &IOrder{ID: 12}})
	legacy.NextFakeID()

	if len(accOpenODs["legacy"]) != 1 || accSyncStamps["legacy"] != 7 || accTasks["legacy"].ID != 11 ||
		len(HistODs) != 1 || FakeOdId != 2 {
		t.Fatal("legacy state view is not backed by compatibility registries")
	}
}

func TestInitTasksWithStateAssignsDistinctAccountIDs(t *testing.T) {
	state := NewOrderState()
	if err := InitTasksWithState(state, []string{"z-account", "a-account"}, "live", 10, 20, false); err != nil {
		t.Fatal(err)
	}
	first := state.GetTaskID("a-account")
	second := state.GetTaskID("z-account")
	if first == second || first >= 0 || second >= 0 {
		t.Fatalf("runtime task IDs = %d/%d, want distinct negative IDs", first, second)
	}
	if state.GetTaskAcc(first) != "a-account" || state.GetTaskAcc(second) != "z-account" {
		t.Fatalf("runtime task reverse index mismatch: %q/%q", state.GetTaskAcc(first), state.GetTaskAcc(second))
	}
	if got := state.GetTask("a-account"); got == nil || got.Mode != "live" || got.StartAt != 10 || got.StopAt != 20 {
		t.Fatalf("runtime task metadata = %#v", got)
	}
}

func TestOrderStateSimulationCounterDoesNotUseLegacyCore(t *testing.T) {
	oldMatch, oldCount := core.SimOrderMatch, core.NewNumInSim
	t.Cleanup(func() {
		core.SimOrderMatch, core.NewNumInSim = oldMatch, oldCount
	})
	core.SimOrderMatch = true
	core.NewNumInSim = 0
	runtimeCore := &core.State{SimOrderMatch: true}
	state := NewOrderState()
	state.BindCore(runtimeCore)
	order := &InOutOrder{IOrder: &IOrder{TaskID: -1, Status: InOutStatusInit}}
	order.BindState(state)
	if err := order.Save(); err != nil {
		t.Fatal(err)
	}
	if runtimeCore.NewNumInSim != 1 || core.NewNumInSim != 0 {
		t.Fatalf("simulation counters runtime=%d legacy=%d, want 1/0", runtimeCore.NewNumInSim, core.NewNumInSim)
	}
}
