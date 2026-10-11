package statistics

import (
	"math"
	"testing"
)

func flow(ms int64, kind string, lots int64, side int) OrderFlowEvent {
	return OrderFlowEvent{ms, ms, ms, kind, lots, side}
}

func TestOrderFlowSourceFeatures(t *testing.T) {
	e := []OrderFlowEvent{flow(0, "trade", 10, 1), flow(1000, "trade", 9, -1), flow(2000, "trade", 25, 0), flow(60000, "trade", 10, -1), flow(120000, "trade", 10, 1), flow(180000, "trade", 10, -1), flow(20000, "cancel", 1, 0), flow(21000, "cancel", 1, 0), flow(22000, "limit", 2, 1), flow(23000, "market", 3, -1), {24000, 24000, 300000, "trade", 10000, 1}}
	r, err := OrderFlowWindow(e, []int64{10, 25}, 0, 240000, 240000, 330, 2)
	if err != nil {
		t.Fatal(err)
	}
	if r.Trades != 6 || r.RoundTrades != 5 || r.UnknownSideTrades != 1 || r.Messages != 4 || r.CancelFraction != .5 || r.LimitFraction != .25 || r.MarketFraction != .25 {
		t.Fatal(r)
	}
	if r.VolumeByHourSecond[5][0] != 40 || r.VolumeByHourSecond[5][1] != 9 || r.SizeFrequency[9] != 1 {
		t.Fatal(r)
	}
	if r.MinuteVolume[0] != 44 || r.EarlyMinuteVolume[0] != 19 || math.Abs(r.CancelPerSecond-2./240) > 1e-12 {
		t.Fatal(r)
	}
	if math.Abs(r.EarlyMinuteImbalance[0]-1./19) > 1e-12 || !r.HasEarlyImbalanceLag1 || r.EarlyImbalanceLag1 >= 0 {
		t.Fatal(r)
	}
	// A missing minute must never become an adjacent pair.
	gap, err := OrderFlowWindow([]OrderFlowEvent{flow(0, "trade", 1, 1), flow(120000, "trade", 1, -1), flow(240000, "trade", 1, 1)}, nil, 0, 300000, 300000, 0, 1)
	if err != nil || gap.HasEarlyImbalanceLag1 {
		t.Fatalf("compressed missing minutes %+v %v", gap, err)
	}
	partial, err := OrderFlowWindow([]OrderFlowEvent{flow(61000, "trade", 1, 1)}, nil, 61000, 119999, 119999, 0, 2)
	if err != nil || len(partial.EarlyMinuteImbalance) != 0 {
		t.Fatal("partial minute included", partial, err)
	}
}

func TestKSExactTiesAndChronology(t *testing.T) {
	a := []PITScalar{scalar(1, 1), scalar(2, 1), scalar(3, 2)}
	b := []PITScalar{scalar(1, 1), scalar(2, 2), scalar(3, 2), {4, 4, 20, 100}}
	r, err := TwoSampleKS(a, b, 10)
	if err != nil || math.Abs(r.Statistic-1./3) > 1e-12 || r.RightSamples != 3 {
		t.Fatalf("tied KS %+v %v", r, err)
	}
	same, err := TwoSampleKS(a, a, 10)
	if err != nil || same.Statistic != 0 {
		t.Fatal(same, err)
	}
	separate, err := TwoSampleKS([]PITScalar{scalar(1, 1)}, []PITScalar{scalar(1, 2)}, 10)
	if err != nil || separate.Statistic != 1 {
		t.Fatal(separate, err)
	}
	if _, err = TwoSampleKS(a, []PITScalar{{1, 1, 20, 1}}, 10); err == nil {
		t.Fatal("empty visible sample accepted")
	}
}

func TestOrderFlowValidation(t *testing.T) {
	for _, e := range []OrderFlowEvent{flow(1, "invalid", 1, 0), flow(1, "trade", 0, 1), flow(1, "trade", 1, 2), {1, 2, 1, "trade", 1, 1}} {
		if _, err := OrderFlowWindow([]OrderFlowEvent{e}, nil, 0, 100, 100, 0, 1); err == nil {
			t.Fatal("invalid event accepted", e)
		}
	}
	r, err := OrderFlowWindow(nil, nil, 0, 100, 100, 0, 1)
	if err != nil || r.HasTradeFraction || r.HasMessageFractions {
		t.Fatal(r, err)
	}
}
