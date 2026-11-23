package biz

import (
	"sort"

	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

// refreshJobForCallback gives a job the same order view the legacy
// subscription installs before OnOrderChange (job.UpdateOrders): the open-order
// registry plus the event's own order view. Hub publishes the registry only at
// the end of a projection page, so without this a callback that queues
// CloseOrders for the order that just filled would see an empty list and the
// request would be dropped. It takes no manager mutex.
func (m *HubOrderMgr) refreshJobForCallback(job *strat.StratJob, event *ormo.InOutOrder) {
	if job == nil || job.Strat == nil || job.Symbol == nil || event == nil {
		return
	}
	ods, lock := m.deps.Orders.GetOpenODs(m.deps.DefaultAccount)
	lock.Lock()
	items := make([]*ormo.InOutOrder, 0, len(ods)+1)
	for _, od := range ods {
		if od.ID != event.ID {
			items = append(items, od)
		}
	}
	lock.Unlock()
	items = append(items, event)
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	var long, short []*ormo.InOutOrder
	for _, od := range items {
		if od.Strategy == job.Strat.Name && od.Symbol == job.Symbol.Symbol && od.Timeframe == job.TimeFrame && od.Status < ormo.InOutStatusFullExit {
			if od.Short {
				short = append(short, od)
			} else {
				long = append(long, od)
			}
		}
	}
	job.SetProjectedOrders(long, short)
}
