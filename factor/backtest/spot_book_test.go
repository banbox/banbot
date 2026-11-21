package backtest

import (
	"math"
	"strings"
	"testing"

	"github.com/banbox/banbot/factor"
)

func TestBookSpotRejectsShortAndNeverAccruesPerpetualFunding(t *testing.T) {
	const (
		sidSpot = int32(1)
		sidPerp = int32(2)
	)
	b, err := NewBook(10000)
	if err != nil {
		t.Fatal(err)
	}
	b.SetSpotSIDs(sidSpot)
	b.EnableAttribution()

	// 1. Attempting to short the spot SID via Execute is deterministically rejected without mutating state.
	shortSpotTarget, err := factor.NewTargetPortfolio(factor.PortfolioSpec{
		StrategyID: "carry", AccountID: "a", DecisionTime: 10, ExecutableAt: 11, ExpireAt: 100,
		PlanSequence: 1, SnapshotID: "s", PlanHash: "p", FactorPlanHash: "f", UniverseVersion: "u",
		Budget: factor.FrozenBudget{Version: "v", Currency: "USDT", NAV: 10000}, Mode: factor.Full,
	}, map[int32]float64{sidSpot: -0.5, sidPerp: 0.5})
	if err != nil {
		t.Fatal(err)
	}
	quotes11 := map[int32]Quote{
		sidSpot: {AtMS: 11, AvailableAt: 11, Price: 50000},
		sidPerp: {AtMS: 11, AvailableAt: 11, Price: 50100},
	}
	if err := b.Execute(shortSpotTarget, quotes11, 11, 0.001, 0); err == nil || !strings.Contains(err.Error(), "spot") {
		t.Fatalf("expected short spot weight to fail, got %v", err)
	}
	if s := b.State(); s.NAV != 10000 || len(s.Quantities) != 0 {
		t.Fatalf("failed spot short mutated book state: %+v", s)
	}

	// 2. Attempting to short the spot SID via ExecuteAllocation is also rejected.
	allocTarget, err := factor.NewPortfolioTarget(factor.PortfolioSpec{
		StrategyID: "carry", AccountID: "a", DecisionTime: 10, ExecutableAt: 11, ExpireAt: 100,
		PlanSequence: 1, SnapshotID: "s", PlanHash: "p", FactorPlanHash: "f", UniverseVersion: "u",
		Budget: factor.FrozenBudget{Version: "v", Currency: "USDT", NAV: 10000}, Mode: factor.Full,
	}, map[int32]factor.Allocation{
		sidSpot: {Basis: factor.AbsoluteQuantity, Value: "-0.1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.ExecuteAllocation(allocTarget, quotes11, 11, 0, 0); err == nil || !strings.Contains(err.Error(), "spot") {
		t.Fatalf("expected short spot allocation to fail, got %v", err)
	}

	// 3. Execute delta-neutral Cash-and-Carry: +0.5 Long Spot (0.1 BTC @ $50,000) + -0.501 Short Perp (-0.1 BTC @ $50,100).
	carryTarget, err := factor.NewTargetPortfolio(factor.PortfolioSpec{
		StrategyID: "carry", AccountID: "a", DecisionTime: 10, ExecutableAt: 11, ExpireAt: 100,
		PlanSequence: 1, SnapshotID: "s", PlanHash: "p", FactorPlanHash: "f", UniverseVersion: "u",
		Budget: factor.FrozenBudget{Version: "v", Currency: "USDT", NAV: 10000}, Mode: factor.Full,
	}, map[int32]float64{sidSpot: 0.5, sidPerp: -0.501})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Execute(carryTarget, quotes11, 11, 0.001, 0); err != nil {
		t.Fatalf("Execute(cash-and-carry) failed: %v", err)
	}
	// Entry fees: 0.001 * (5000 + 5010) = $10.01.
	s := b.State()
	if math.Abs(s.NAV-9989.99) > 1e-9 || math.Abs(s.Quantities[sidSpot]-0.1) > 1e-12 || math.Abs(s.Quantities[sidPerp]+0.1) > 1e-12 {
		t.Fatalf("unexpected entry state: %+v", s)
	}

	// 4. Mark both legs at $55,000 (basis $100 converges to $0 while BTC rises $5,000):
	// Spot PnL = +0.1 * (55000 - 50000) = +$500.00; Perp PnL = -0.1 * (55000 - 50100) = -$490.00 => net price PnL = +$10.00!
	if err := b.Mark(sidSpot, Quote{AtMS: 20, AvailableAt: 20, Price: 55000}, 20); err != nil {
		t.Fatal(err)
	}
	if err := b.Mark(sidPerp, Quote{AtMS: 20, AvailableAt: 20, Price: 55000}, 20); err != nil {
		t.Fatal(err)
	}

	// 5. Deliver funding (+0.01 = 1%) to BOTH SIDs: Spot must accrue 0, Perp (-0.1 @ $55,000) earns +$55.00 (cost = -55.00)!
	if err := b.ApplyFunding(Funding{ID: "f-spot", SID: sidSpot, AtMS: 20, AvailableAt: 20, Rate: 0.01}, 20); err != nil {
		t.Fatal(err)
	}
	if b.State().Funding != 0 {
		t.Fatalf("spot leg accrued perpetual funding: %v", b.State().Funding)
	}
	if err := b.ApplyFunding(Funding{ID: "f-perp", SID: sidPerp, AtMS: 20, AvailableAt: 20, Rate: 0.01}, 20); err != nil {
		t.Fatal(err)
	}

	// Expected NAV = 10000 - 10.01 (fees) + 10.00 (basis convergence) + 55.00 (short perp funding) = 10054.99!
	s = b.State()
	if math.Abs(s.Funding-(-55.0)) > 1e-9 {
		t.Fatalf("Funding = %.8f, want -55.0", s.Funding)
	}
	if math.Abs(s.NAV-10054.99) > 1e-9 {
		t.Fatalf("NAV = %.8f, want 10054.99", s.NAV)
	}
	attr := b.Attribution()
	if attr == nil || math.Abs(attr.LongPnL-500.0) > 1e-9 || math.Abs(attr.ShortPnL-(-490.0)) > 1e-9 || math.Abs(attr.Funding-(-55.0)) > 1e-9 || math.Abs(attr.Residual) > 1e-9 {
		t.Fatalf("attribution mismatch: %+v", attr)
	}

	// 6. Verify constant funding_apr also charges 0 on unleveraged spot holdings!
	bSpotOnly, err := NewBook(10000)
	if err != nil {
		t.Fatal(err)
	}
	bSpotOnly.SetSpotSIDs(sidSpot)
	bSpotOnly.SetFundingApr(20) // 20% APR on net long contract notional — must NOT charge spot!
	spotOnlyTarget, err := factor.NewTargetPortfolio(factor.PortfolioSpec{
		StrategyID: "spot-hold", AccountID: "a", DecisionTime: 10, ExecutableAt: 11, ExpireAt: 100,
		PlanSequence: 1, SnapshotID: "s", PlanHash: "p", FactorPlanHash: "f", UniverseVersion: "u",
		Budget: factor.FrozenBudget{Version: "v", Currency: "USDT", NAV: 10000}, Mode: factor.Full,
	}, map[int32]float64{sidSpot: 1.0})
	if err != nil {
		t.Fatal(err)
	}
	if err := bSpotOnly.Execute(spotOnlyTarget, map[int32]Quote{sidSpot: {AtMS: 11, AvailableAt: 11, Price: 50000}}, 11, 0, 0); err != nil {
		t.Fatal(err)
	}
	dayMS := int64(24 * 3600 * 1000)
	if err := bSpotOnly.Mark(sidSpot, Quote{AtMS: 11 + dayMS, AvailableAt: 11 + dayMS, Price: 50000}, 11+dayMS); err != nil {
		t.Fatal(err)
	}
	if st := bSpotOnly.State(); st.Funding != 0 || st.NAV != 10000 {
		t.Fatalf("constant funding_apr charged unleveraged spot position: %+v", st)
	}
}
