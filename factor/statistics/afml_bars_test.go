package statistics

import (
	"reflect"
	"testing"
)

func TestAFMLStandardActivityBarsConserveTicksAndVolume(t *testing.T) {
	ticks := []ActivityTick{{1, 100, 2, 1}, {2, 101, 3, -1}, {3, 99, 1, 1}}
	bars, err := StandardActivityBars(ticks, "volume", 4, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(bars) != 2 || !bars[0].Complete || bars[1].Complete || bars[0].End != 1 || bars[0].Volume != 5 || bars[1].Volume != 1 {
		t.Fatalf("indivisible overshoot/partial bars wrong %+v", bars)
	}
	afmlNear(t, bars[0].VWAP, 100.6)
	whole := []ActivityTick{{1, 100, 2, 1}, {1, 100, 2, 1}}
	split := []ActivityTick{{1, 100, 1, 1}, {1, 100, 1, 1}, {1, 100, 1, 1}, {1, 100, 1, 1}}
	for _, clock := range []string{"volume", "dollar"} {
		threshold := 4.
		if clock == "dollar" {
			threshold = 400
		}
		a, err := StandardActivityBars(whole, clock, threshold, false)
		if err != nil {
			t.Fatal(err)
		}
		b, err := StandardActivityBars(split, clock, threshold, false)
		if err != nil || len(a) != 1 || len(b) != 1 || a[0].DollarVolume != b[0].DollarVolume || a[0].Volume != b[0].Volume {
			t.Fatalf("fragmentation changed volume/dollars %v %+v %+v", err, a, b)
		}
	}
	if _, err = StandardActivityBars(ticks, "unknown", 1, true); err == nil {
		t.Fatal("unknown clock accepted")
	}
}

func TestAFMLInformationRunsNotConsecutiveOrImbalance(t *testing.T) {
	sides := []int{1, -1, 1, -1, 1, 1, 1}
	ticks := make([]ActivityTick, len(sides))
	for i, side := range sides {
		ticks[i] = ActivityTick{int64(i), 100, 1, side}
	}
	c := InformationBarConfig{Clock: "tick", Kind: "runs", ExpectedTicks: 6, ExpectedSignedActivity: .5, BuyProbability: .5, BuyActivity: 1, SellActivity: 1, LengthAlpha: .1, ActivityAlpha: .1, IncludePartial: true}
	runs, err := InformationActivityBars(ticks, c)
	if err != nil {
		t.Fatal(err)
	}
	if runs[0].End != 4 || !runs[0].Complete || runs[0].Threshold != 3 {
		t.Fatalf("runs mistakenly used consecutive length %+v", runs[0])
	}
	c.Kind = "imbalance"
	imbalance, err := InformationActivityBars(ticks, c)
	if err != nil || imbalance[0].End != 6 {
		t.Fatalf("signed imbalance mismatch %v %+v", err, imbalance)
	}
	c.Kind = "runs"
	prefix, err := InformationActivityBars(ticks[:5], c)
	if err != nil || !reflect.DeepEqual(prefix[0], runs[0]) {
		t.Fatalf("future ticks changed frozen completed bar %v", err)
	}
	for _, clock := range []string{"volume", "dollar"} {
		c.Clock = clock
		if clock == "dollar" {
			c.BuyActivity, c.SellActivity = 100, 100
		}
		got, err := InformationActivityBars(ticks, c)
		if err != nil || got[0].End != 4 {
			t.Fatalf("runs clock %s wrong %v %+v", clock, err, got)
		}
	}
}

func TestAFMLTickRuleCarriesFlatPriceSign(t *testing.T) {
	c := InformationBarConfig{Clock: "tick", Kind: "runs", ExpectedTicks: 4, BuyProbability: .5, BuyActivity: 1, SellActivity: 1, LengthAlpha: .2, ActivityAlpha: .2}
	got, err := InformationActivityBars([]ActivityTick{{1, 100, 1, 0}, {2, 99, 1, 0}, {3, 99, 1, 0}}, c)
	if err != nil || len(got) != 1 || got[0].End != 2 {
		t.Fatalf("flat-price trade lost prior sell sign %v %+v", err, got)
	}
}
