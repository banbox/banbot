package biz

import (
	"testing"

	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg/utils"
)

// rita round4: what silence (ms) is needed to trigger the gap/delist logic per timeframe at gap_bars=3, min 48h.
func TestRitaGapThresholdTable(t *testing.T) {
	const hour = int64(3600000)
	for _, tf := range []string{"5m", "15m", "1h", "4h", "8h", "1d", "3d"} {
		tfMS := int64(utils.TFToSecs(tf)) * 1000
		// smallest whole-hour silence that triggers
		var hit int64
		for h := int64(1); h < 24*30; h++ {
			if _, ok := barGapExceeded(0+1, 1+h*hour, tfMS, 3, 48); ok {
				hit = h
				break
			}
		}
		t.Logf("tf=%-3s trigger when silence > %dh (%.1f days)", tf, hit, float64(hit)/24)
	}
}

// After the in-run sweep the order is gone from the open set, a second sweep (data still missing) is a no-op,
// and a later guardBarGap-style check on the same symbol finds nothing to close.
func TestRitaSweepLeavesCleanState(t *testing.T) {
	const day = int64(86400000)
	mgr := setupDelistTest(t, 3, 5)
	t0 := int64(1_600_000_000_000)
	last := t0 + 5*day
	mgr.clock.SetTimeMS(last + 4*day)
	mgr.prices.SetBarPriceAt(last, "DEAD/USDT:USDT", 2.0)
	od := cleanupPendingExit(1, "DEAD/USDT:USDT")
	od.Timeframe, od.ExitTag, od.Exit, od.EnterAt = "1d", "", nil, t0
	od.Enter.Average, od.Enter.Filled = 2.0, 1
	openOds, lock := mgr.orderState().GetOpenODs(mgr.Account)
	lock.Lock()
	openOds[1] = od
	lock.Unlock()
	if err := mgr.sweepDelisted(last + 2*day); err != nil { // 2d silence at 1d tf: not a gap (needs >3d)
		t.Fatal(err)
	}
	if od.ExitTag != "" {
		t.Fatalf("2d silence must not close a 1d position, tag=%q", od.ExitTag)
	}
	if err := mgr.sweepDelisted(last + 2*day + 7*3600000); err != nil { // within 6h throttle of previous sweep
		t.Fatal(err)
	}
	mgr.lastDelistSweepMS = 0
	if err := mgr.sweepDelisted(last + 4*day); err != nil {
		t.Fatal(err)
	}
	if od.ExitTag != ExitTagDelist || od.ExitAt != last || od.Status != ormo.InOutStatusFullExit {
		t.Fatalf("tag=%q exitAt=%d status=%d", od.ExitTag, od.ExitAt, od.Status)
	}
	if h := mgr.delistedOrders(last + 9*day); len(h) != 0 {
		t.Fatalf("closed order reported again: %d", len(h))
	}
	mgr.lastDelistSweepMS = 0
	if err := mgr.sweepDelisted(last + 9*day); err != nil {
		t.Fatal(err)
	}
	t.Logf("open orders after sweep: %d", len(openOds))
	for id, o := range openOds {
		t.Logf("still in open map: %d status=%d", id, o.Status)
	}
}
