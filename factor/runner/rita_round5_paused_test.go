package runner

import (
	"context"
	"testing"
)

// rita round5 (4): characterization of the paused-ratio gate. When ONE leg of a single pair dies, the paused
// periods are also counted as Decisions (the surviving leg still decides), while bansite computes
// pausedRatio = Paused/(Decisions+Paused). With Paused <= Decisions that ratio is < 50% whatever the share of
// paused periods, so the FactorMaxPausedRatio low_sample gate cannot fire (only the both-legs-dead path, which
// does not count Decisions, can reach it). Issue rita-072.
func TestRitaPausedRatioDenominatorDoubleCount(t *testing.T) {
	const bars = 14
	c := ritaHoleConfig(t, []int32{1, 3}, nil, map[int32]int64{3: 3}, bars) // leg 3 dies after bar 3
	c.PITPairs = [][2]int32{{1, 3}}
	res, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	trueRatio := float64(res.PausedPeriods) / float64(res.Decisions)
	siteRatio := float64(res.PausedPeriods) / float64(res.Decisions+res.PausedPeriods)
	t.Logf("paused=%d decisions=%d true=%.2f bansite=%.2f", res.PausedPeriods, res.Decisions, trueRatio, siteRatio)
	if res.PausedPeriods > res.Decisions {
		t.Skip("paused periods are not part of Decisions: formula correct")
	}
	if trueRatio > 0.5 && siteRatio > 0.5 {
		t.Fatalf("gate fires: unexpected")
	}
	if trueRatio <= 0.5 {
		t.Fatalf("scenario did not make >50%% of periods paused: %.2f", trueRatio)
	}
}
