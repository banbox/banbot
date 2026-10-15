package ormo

import "testing"

func TestCancelExitTriggerNotifiesSoftwareEditor(t *testing.T) {
	for _, key := range []string{OdInfoStopLoss, OdInfoTakeProfit} {
		for _, cancelWithNil := range []bool{true, false} {
			for _, initial := range []string{"software", "exchange", "absent"} {
				name := key + "/" + initial + "/zero"
				if cancelWithNil {
					name = key + "/" + initial + "/nil"
				}
				t.Run(name, func(t *testing.T) {
					state := NewOrderState()
					order := &InOutOrder{
						IOrder: &IOrder{ID: 1, Status: InOutStatusFullEnter},
						Info:   make(map[string]interface{}),
					}
					order.BindState(state)
					if initial != "absent" {
						trigger := &TriggerState{ExitTrigger: &ExitTrigger{Price: 95}}
						if initial == "exchange" {
							trigger.OrderId = "exchange-trigger"
						}
						order.Info[key] = trigger
					}
					notifications := 0
					state.SetSoftwareEditListener(func(got *InOutOrder, action string) {
						notifications++
						if got != order || action != key {
							t.Fatalf("unexpected notification: order=%p action=%s", got, action)
						}
						trigger := got.GetExitTrigger(key)
						if initial == "exchange" {
							if trigger == nil || trigger.Price != 0 || trigger.OrderId != "exchange-trigger" {
								t.Fatalf("exchange cancellation state not available to listener: %+v", trigger)
							}
						} else if trigger != nil {
							t.Fatalf("software trigger still present in listener: %+v", trigger)
						}
					})
					var args *ExitTrigger
					if !cancelWithNil {
						args = &ExitTrigger{}
					}
					cancel := order.SetStopLoss
					if key == OdInfoTakeProfit {
						cancel = order.SetTakeProfit
					}
					if err := cancel(args); err != nil {
						t.Fatalf("cancel trigger: %v", err)
					}
					if notifications != 1 {
						t.Fatalf("cancellation notifications = %d, want 1", notifications)
					}
				})
			}
		}
	}
}
