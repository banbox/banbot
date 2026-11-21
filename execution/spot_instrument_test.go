package execution

import (
	"context"
	"strings"
	"testing"

	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
)

func testSpotInstrument() Instrument {
	return Instrument{
		ID:                 "BTC/USDT",
		Version:            "v1",
		Valuation:          ValuationSpot,
		SettlementCurrency: "USDT",
		QuantityStep:       intentPrice("0.001"),
		ContractSize:       intentPrice("1"),
		PriceTick:          intentPrice("0.01"),
		MoneyScale:         8,
		MinSteps:           1,
		MinNotional:        intentPrice("5"),
	}
}

func testPerpInstrument() Instrument {
	return Instrument{
		ID:                 "BTC/USDT:USDT",
		Version:            "v1",
		Valuation:          ValuationLinearPerpetual,
		SettlementCurrency: "USDT",
		QuantityStep:       intentPrice("0.001"),
		ContractSize:       intentPrice("1"),
		PriceTick:          intentPrice("0.01"),
		MoneyScale:         8,
		MinSteps:           1,
		MinNotional:        intentPrice("5"),
	}
}

func TestBanexgSpotInstrumentMetadataAndCollateral(t *testing.T) {
	spotMarket := &banexg.Market{
		ID:     "BTCUSDT",
		Symbol: "BTC/USDT",
		Base:   "BTC",
		Quote:  "USDT",
		Type:   banexg.MarketSpot,
		Spot:   true,
		Precision: &banexg.Precision{
			Amount:     0.001,
			ModeAmount: banexg.PrecModeTickSize,
			Price:      2,
			ModePrice:  banexg.PrecModeDecimalPlace,
		},
		Limits: &banexg.MarketLimits{
			Amount: &banexg.LimitRange{Min: 0.001},
			Cost:   &banexg.LimitRange{Min: 5},
		},
	}
	currency := &banexg.Currency{Code: "USDT", Precision: 8, PrecMode: banexg.PrecModeDecimalPlace}
	inst, err := InstrumentFromBanexgMarket("BTC/USDT", spotMarket, currency)
	if err != nil {
		t.Fatalf("InstrumentFromBanexgMarket(spot) failed: %v", err)
	}
	if inst.Valuation != ValuationSpot || !inst.IsSpot() || inst.ShortAllowed() || inst.HasPerpetualFunding() {
		t.Fatalf("unexpected spot flags: %+v", inst)
	}
	if inst.SettlementCurrency != "USDT" || !inst.ContractSize.Equal(intentPrice("1")) || !inst.QuantityStep.Equal(intentPrice("0.001")) || !inst.PriceTick.Equal(intentPrice("0.01")) {
		t.Fatalf("unexpected spot units: %+v", inst)
	}
	collateral := inst.Collateral(100, intentPrice("50000"), intentPrice("0.2"))
	if !collateral.Equal(intentPrice("5000")) {
		t.Fatalf("spot collateral = %s, want 5000 (100%% notional)", collateral.String())
	}

	for _, tc := range []string{"contract-size-2", "nonempty-settle", "wrong-currency", "contract-and-spot"} {
		t.Run(tc, func(t *testing.T) {
			bad, cur := *spotMarket, *currency
			switch tc {
			case "contract-size-2":
				bad.ContractSize = 2
			case "nonempty-settle":
				bad.Settle = "USDT"
			case "wrong-currency":
				cur.Code = "USD"
			case "contract-and-spot":
				bad.Contract = true
			}
			if _, err := InstrumentFromBanexgMarket("BTC/USDT", &bad, &cur); err == nil {
				t.Fatalf("expected invalid spot metadata %q to fail", tc)
			}
		})
	}
}

func TestSpotShortingAndFundingAreDeterministicallyRejected(t *testing.T) {
	ctx := context.Background()
	spot := testSpotInstrument()
	store, _, _, _ := testStore(t)

	if _, err := store.ApplyCashEvent(ctx, CashEvent{
		ID:           "deposit",
		Kind:         Reconciliation,
		AccountDelta: intentPrice("10000"),
		Postings:     []CashPosting{{Strategy: "carry", Amount: intentPrice("10000")}},
		AtMS:         1,
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}

	risk := PortfolioRisk{
		Marks:               map[string]decimal.Decimal{spot.ID: intentPrice("50000")},
		MarginRate:          intentPrice("0.2"),
		MaxAccountMargin:    intentPrice("10000"),
		MaxVirtualGross:     intentPrice("20000"),
		StrategyGrossLimits: map[StrategyID]decimal.Decimal{"carry": intentPrice("20000")},
	}
	obs := ExecutionObservation{Price: intentPrice("50000"), Bid: intentPrice("50000"), Ask: intentPrice("50000"), AtMS: 10, ValidUntilMS: 100}

	if _, err := Coordinate(snap, []ExecutableTarget{{Strategy: "carry", Lot: "spot-leg", SignedSteps: -10}}, spot, obs, risk); err == nil || !strings.Contains(err.Error(), "spot") {
		t.Fatalf("expected Coordinate to reject short spot target, got %v", err)
	}

	paper, err := NewPaperAdapter(intentPrice("10000"), decimal.Zero, decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	_, submitErr := paper.submitLocked(ctx, OrderIntent{
		ID:          "short-spot",
		PlanID:      "p1",
		Instrument:  spot,
		Side:        Sell,
		Steps:       10,
		Observation: obs,
		SubmitAtMS:  10,
	}, "client-short-spot")
	if submitErr == nil || !strings.Contains(submitErr.Error(), "spot") {
		t.Fatalf("expected PaperAdapter to reject short spot sell, got %v", submitErr)
	}

	if _, err := store.ApplyFunding(ctx, FundingSettlement{
		ID:            "spot-funding",
		Instrument:    spot,
		Mark:          intentPrice("50000"),
		Rate:          intentPrice("0.001"),
		AccountAmount: intentPrice("0"),
		AtMS:          20,
	}); err == nil || !strings.Contains(err.Error(), "spot") {
		t.Fatalf("expected Store.ApplyFunding to reject spot funding, got %v", err)
	}
}

func TestUnifiedSpotAndLinearCashAndCarryLedgerReconcilesToTheCent(t *testing.T) {
	ctx := context.Background()
	spot := testSpotInstrument()
	perp := testPerpInstrument()

	store, _, handle, _ := testStore(t)
	paper, err := NewPaperAdapter(intentPrice("10000"), intentPrice("0.001"), decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	exec := &OwnerExecutor{Handle: handle, Token: handle.Token(), Store: store, Adapter: paper}

	if _, err := store.ApplyCashEvent(ctx, CashEvent{
		ID:           "init-cash",
		Kind:         Reconciliation,
		AccountDelta: intentPrice("10000"),
		Postings:     []CashPosting{{Strategy: "carry", Amount: intentPrice("10000")}},
		AtMS:         1,
	}); err != nil {
		t.Fatal(err)
	}

	risk := PortfolioRisk{
		Marks: map[string]decimal.Decimal{
			spot.ID: intentPrice("50000"),
			perp.ID: intentPrice("50100"),
		},
		MarginRate:          intentPrice("0.2"), // 5x on linear perpetual
		MaxAccountMargin:    intentPrice("10000"),
		MaxVirtualGross:     intentPrice("20000"),
		StrategyGrossLimits: map[StrategyID]decimal.Decimal{"carry": intentPrice("20000")},
	}
	spotObs := ExecutionObservation{Price: intentPrice("50000"), Bid: intentPrice("50000"), Ask: intentPrice("50000"), AtMS: 10, ValidUntilMS: 100, Bar: 1}
	perpObs := ExecutionObservation{Price: intentPrice("50100"), Bid: intentPrice("50100"), Ask: intentPrice("50100"), AtMS: 12, ValidUntilMS: 100, Bar: 1}

	snap, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	coordSpot, err := Coordinate(snap, []ExecutableTarget{{Strategy: "carry", Lot: "spot-leg", SignedSteps: 100}}, spot, spotObs, risk)
	if err != nil {
		t.Fatalf("Coordinate(spot buy $5,000) failed: %v", err)
	}
	if !coordSpot.AccountMargin.Equal(intentPrice("5000")) {
		t.Fatalf("spot AccountMargin = %s, want 5000", coordSpot.AccountMargin.String())
	}

	plan1 := Plan{
		ID:                    "plan-spot-entry",
		Sequence:              1,
		DecisionMS:            10,
		ExpiresMS:             100,
		Intents:               []EligibleIntent{{ID: "vi-spot-1", Account: store.key, Strategy: "carry", Lot: "spot-leg", Instrument: spot.ID, Kind: EntryIntent, Side: Buy, QuantitySteps: 100, State: Eligible, Conditions: IntentConditions{CreatedBar: 1}}},
		Targets:               []ExecutableTarget{{Strategy: "carry", Lot: "spot-leg", Instrument: spot.ID, SignedSteps: 100}},
		RequestHash:           "h1",
		Instruments:           []Instrument{spot},
		RebalancedInstruments: []string{spot.ID},
		RiskValidUntilMS:      100,
	}
	orderSpot := OrderIntent{
		ID:          "ord-spot-1",
		PlanID:      plan1.ID,
		Instrument:  spot,
		Side:        Buy,
		Steps:       100,
		Observation: spotObs,
		Risk:        &risk,
		Allocations: []FillAllocation{{ID: "alloc-spot-1", IntentID: "vi-spot-1", Strategy: "carry", Lot: "spot-leg", Side: Buy, Kind: EntryIntent, Steps: 100}},
	}
	if err := exec.SubmitEligible(plan1, orderSpot, 11); err != nil {
		t.Fatalf("SubmitEligible(spot) failed: %v", err)
	}

	snap, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Over-margined perp leg ($5,000 spot + $6,012 perp margin > $9,995 equity) must be rejected.
	if _, err := Coordinate(snap, []ExecutableTarget{{Strategy: "carry", Lot: "perp-leg", SignedSteps: -600}}, perp, perpObs, risk); err == nil {
		t.Fatal("expected over-margined perp leg alongside unleveraged spot to be rejected")
	}

	coordPerp, err := Coordinate(snap, []ExecutableTarget{{Strategy: "carry", Lot: "perp-leg", SignedSteps: -100}}, perp, perpObs, risk)
	if err != nil {
		t.Fatalf("Coordinate(perp short $5,010) failed: %v", err)
	}
	if !coordPerp.AccountMargin.Equal(intentPrice("6002")) {
		t.Fatalf("combined spot+perp AccountMargin = %s, want 6002", coordPerp.AccountMargin.String())
	}

	plan2 := Plan{
		ID:                    "plan-perp-entry",
		Sequence:              2,
		DecisionMS:            12,
		ExpiresMS:             100,
		Intents:               []EligibleIntent{{ID: "vi-perp-1", Account: store.key, Strategy: "carry", Lot: "perp-leg", Instrument: perp.ID, Kind: EntryIntent, Side: Sell, QuantitySteps: 100, State: Eligible, Conditions: IntentConditions{CreatedBar: 1}}},
		Targets:               []ExecutableTarget{{Strategy: "carry", Lot: "perp-leg", Instrument: perp.ID, SignedSteps: -100}},
		CarriedTargets:        []ExecutableTarget{{Strategy: "carry", Lot: "spot-leg", Instrument: spot.ID, SignedSteps: 100}},
		RequestHash:           "h2",
		Instruments:           []Instrument{spot, perp},
		RebalancedInstruments: []string{perp.ID},
		RiskValidUntilMS:      100,
	}
	orderPerp := OrderIntent{
		ID:          "ord-perp-1",
		PlanID:      plan2.ID,
		Instrument:  perp,
		Side:        Sell,
		Steps:       100,
		Observation: perpObs,
		Risk:        &risk,
		Allocations: []FillAllocation{{ID: "alloc-perp-1", IntentID: "vi-perp-1", Strategy: "carry", Lot: "perp-leg", Side: Sell, Kind: EntryIntent, Steps: 100}},
	}
	if err := exec.SubmitEligible(plan2, orderPerp, 13); err != nil {
		t.Fatalf("SubmitEligible(perp) failed: %v", err)
	}

	// Fees @ 0.1%: spot fee = 5.00, perp fee = 5.01 => total fees = 10.01.
	// SettledCash = 10000 - 5000 (spot outlay) - 5.00 - 5.01 = 4989.99.
	// Entry Equity = 4989.99 + 5000 (spot value) + 0 (perp unrealized) = 9989.99.
	snap, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !snap.AccountSettledCash.Equal(intentPrice("4989.99")) {
		t.Fatalf("entry AccountSettledCash = %s, want 4989.99", snap.AccountSettledCash.String())
	}
	eqEntry, err := snap.Equity(risk.Marks)
	if err != nil || !eqEntry.Equal(intentPrice("9989.99")) {
		t.Fatalf("entry Equity = %s (err=%v), want 9989.99", eqEntry.String(), err)
	}

	// Settle +1% (+0.01) positive funding on short Perp at $55,000 => +$55.00; Spot receives $0.
	if _, err := store.ApplyFunding(ctx, FundingSettlement{
		ID:            "perp-funding-1",
		Instrument:    perp,
		Mark:          intentPrice("55000"),
		Rate:          intentPrice("0.01"),
		AccountAmount: intentPrice("55"),
		AtMS:          20,
	}); err != nil {
		t.Fatal(err)
	}

	// Converged at $55,000:
	// 10000 - 10.01 (fees) + 10.00 (basis $50,100 -> $50,000 convergence on 0.1 BTC) + 55.00 (funding) = 10054.99!
	snap, err = store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	marksConverged := map[string]decimal.Decimal{
		spot.ID: intentPrice("55000"),
		perp.ID: intentPrice("55000"),
	}
	eqConverged, err := snap.Equity(marksConverged)
	if err != nil || !eqConverged.Equal(intentPrice("10054.99")) {
		t.Fatalf("converged Equity = %s (err=%v), want exact 10054.99", eqConverged.String(), err)
	}
	if !snap.PnLReclassification.IsZero() {
		t.Fatalf("PnLReclassification = %s, want 0", snap.PnLReclassification.String())
	}
}
