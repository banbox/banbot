package ormo

import "testing"

func TestOrderStateTriggerAndDirtySaveAreScoped(t *testing.T) {
	oldOpen := accOpenODs
	accOpenODs = make(map[string]map[int64]*InOutOrder)
	t.Cleanup(func() { accOpenODs = oldOpen })

	first := NewOrderState()
	second := NewOrderState()
	order := &InOutOrder{
		IOrder: &IOrder{ID: 7, TaskID: 99, Status: InOutStatusFullEnter, Symbol: "STATE/USDT"},
	}
	order.DirtyMain = true
	first.SetTask("shared", &BotTask{ID: 99})
	firstOpen, firstOpenLock := first.GetOpenODs("shared")
	firstOpenLock.Lock()
	firstOpen[order.ID] = order
	firstOpenLock.Unlock()

	first.AddTriggerOd("shared", order)
	firstTriggers, firstTriggerLock := first.GetTriggerODs("shared")
	secondTriggers, secondTriggerLock := second.GetTriggerODs("shared")
	firstTriggerLock.Lock()
	_, firstHasTrigger := firstTriggers[order.Symbol][order.ID]
	firstTriggerLock.Unlock()
	secondTriggerLock.Lock()
	_, secondHasTrigger := secondTriggers[order.Symbol][order.ID]
	secondTriggerLock.Unlock()
	if !firstHasTrigger || secondHasTrigger {
		t.Fatalf("trigger registry leaked: first=%v second=%v", firstHasTrigger, secondHasTrigger)
	}
	if order.state != first {
		t.Fatal("trigger registration did not bind the order to its owning state")
	}

	if err := first.SaveDirtyODs("unused", "shared"); err != nil {
		t.Fatalf("save dirty orders: %v", err)
	}
	if got := firstOpen[order.ID]; got != order {
		t.Fatalf("dirty save removed a live order: %#v", got)
	}
	if len(accOpenODs["shared"]) != 0 {
		t.Fatalf("dirty save leaked into legacy state: %#v", accOpenODs["shared"])
	}
	secondOpen, _ := second.GetOpenODs("shared")
	if len(secondOpen) != 0 {
		t.Fatalf("dirty save leaked into second state: %#v", secondOpen)
	}
}
