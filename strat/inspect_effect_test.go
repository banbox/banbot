package strat

import (
	"slices"
	"testing"

	"github.com/banbox/banbot/orm/ormo"
)

func TestInspectionEffectHookInterceptsOrderMutationAPIs(t *testing.T) {
	var effects []string
	job := NewInspectionJob(nil, nil, nil, "", "", func(name string) { effects = append(effects, name) })
	_ = job.OpenOrder(nil)
	_ = job.CloseOrders(nil)
	_ = job.SetAllStopLoss(0, nil)
	_ = job.SetAllTakeProfit(0, nil)
	job.UpdateOrders([]*ormo.InOutOrder{{}})
	job.InitBar(nil)
	want := []string{"OpenOrder", "CloseOrders", "SetAllStopLoss", "SetAllTakeProfit", "UpdateOrders", "InitBar"}
	if !slices.Equal(effects, want) {
		t.Fatalf("inspection effects = %v, want %v", effects, want)
	}
	if job.IsWarmUp || job.MaxOpenLong != 0 || job.MaxOpenShort != 0 || job.OrderNum != 0 {
		t.Fatalf("effect hook changed real zero-value startup state: %+v", job)
	}
}
