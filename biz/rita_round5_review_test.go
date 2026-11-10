package biz

import (
	"math"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

// rita round5 (1): a recovery bar whose OPEN is a >50% bad-data spike is applied as-is (one-sided worse-of):
// long -> exit at the spiked-down open (huge loss), spike up -> last close. Documents the behaviour.
func TestRitaGapBadOpenSpike(t *testing.T) {
	const hour = int64(3600000)
	t0 := int64(1_600_000_000_000)
	last := t0 + 10*hour
	for _, tc := range []struct {
		short bool
		open  float64
		want  float64
	}{{false, 0.2, 0.2}, {false, 20, 2}, {true, 20, 20}, {true, 0.2, 2}} {
		mgr := setup071(t, 0, 100)
		rec := last + 100*hour
		mgr.clock.SetTimeMS(rec)
		mgr.prices.SetBarPriceAt(last, "DEAD/USDT:USDT", 2.0)
		od := open071(mgr, 1, tc.short, t0)
		if err := mgr.ExitAndFill([]*ormo.InOutOrder{od}, &strat.ExitReq{Tag: ExitTagDataGap, Dirt: core.OdDirtBoth, Force: true, GapNextOpen: tc.open, GapRecoverMS: rec}); err != nil {
			t.Fatal(err)
		}
		if math.Abs(od.Exit.Average-tc.want) > 1e-9 {
			t.Fatalf("short=%v open=%v exit %v want %v", tc.short, tc.open, od.Exit.Average, tc.want)
		}
		if od.ExitAt < last || od.ExitAt > rec {
			t.Fatalf("ExitAt %d outside [last %d, recovery %d]", od.ExitAt, last, rec)
		}
	}
}

// rita round5 (5): in a multi-symbol run the 6h sweep fires as soon as the silence passes the confirmation delay,
// BEFORE the recovery bar arrives (hole of 100h > 48h+6h). The sweep books the delist rule (last close -/+ penalty),
// not the data_gap worse-of rule. For a long that sat through a -50% hole this is ~49% better than data_gap
// (optimistic); for a +50% hole it is 1% worse (conservative).
func TestRitaSweepPreemptsGapRule(t *testing.T) {
	const hour = int64(3600000)
	t0 := int64(1_600_000_000_000)
	last := t0 + 10*hour
	mgr := setup071(t, 0, 100)
	mgr.clock.SetTimeMS(last + 60*hour)
	mgr.prices.SetBarPriceAt(last, "DEAD/USDT:USDT", 2.0)
	od := open071(mgr, 1, false, t0)
	if err := mgr.sweepDelisted(last + 60*hour); err != nil {
		t.Fatal(err)
	}
	if od.ExitTag != ExitTagDelist {
		t.Fatalf("sweep tag %q, want delist (preempts data_gap)", od.ExitTag)
	}
	swept := od.Exit.Average // 1.98
	// what the data_gap rule would have booked if the hole later recovered at open 1.0 (-50%)
	gapRule := math.Min(2.0, 1.0)
	t.Logf("sweep exit %.4f vs data_gap rule %.4f: optimism %.1f%% of entry-price notional on a -50%% hole", swept, gapRule, (swept-gapRule)/2*100)
	if swept <= gapRule {
		t.Fatalf("expected sweep exit to be more favourable than data_gap on a crash hole")
	}
}
