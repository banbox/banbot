package runner

import (
	"context"
	"testing"

	"github.com/banbox/banbot/factor"
)

func policyPITConfig(t *testing.T, minRatio float64) Config {
	const bars = 24
	c := pitConfigFrom(t, map[int32]int64{3: 10}, map[int32]int64{1: bars, 2: bars, 3: bars}, bars, minRatio)
	c.Manifest.Portfolio.Policy = "lifecycle-v1"
	c.Manifest.Portfolio.LongNotional = 1
	c.Manifest.Portfolio.ShortNotional = 0
	c.Manifest.Portfolio.Rebalance = &factor.RebalanceConfig{EveryBars: 2}
	c.Manifest.Portfolio.Transition = &factor.TransitionConfig{Mode: "direct"}
	c.Manifest.Portfolio.Holding = &factor.HoldingConfig{MinBars: 2, MaxBars: 8}
	return c
}

// 081: under the lifecycle policy an instrument that is not listed yet is dropped from the point-in-time universe
// instead of voiding every decision before its listing.
func TestPolicyPITDropsNotYetListedInstrument(t *testing.T) {
	result, err := Run(context.Background(), policyPITConfig(t, 0), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Incomplete != 0 || result.UniverseDrops == 0 || result.DroppedBySID[3] != result.UniverseDrops {
		t.Fatalf("incomplete=%d drops=%d by=%v", result.Incomplete, result.UniverseDrops, result.DroppedBySID)
	}
	if result.DelistExits != 0 {
		t.Fatalf("nothing was held, got exits %d", result.DelistExits)
	}
}

// With PIT off (ratio < 0) the pre-listing decisions stay skipped.
func TestPolicyPITDisabledStillSkips(t *testing.T) {
	result, err := Run(context.Background(), policyPITConfig(t, -1), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Incomplete == 0 || result.UniverseDrops != 0 {
		t.Fatalf("incomplete=%d drops=%d", result.Incomplete, result.UniverseDrops)
	}
}

// 081: manifest.costs.funding_apr charges long notional; zero by default.
func TestFundingAprChargesLongBook(t *testing.T) {
	const bars = 14
	for _, apr := range []float64{0, 50} {
		c := pitConfig(t, map[int32]int64{1: bars, 2: bars, 3: bars}, bars, 0)
		c.Manifest.Costs.FundingApr = apr
		result, err := Run(context.Background(), c, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if (apr == 0) != (result.Book.Funding == 0) || result.Book.Funding < 0 {
			t.Fatalf("apr=%v funding=%v", apr, result.Book.Funding)
		}
	}
}
