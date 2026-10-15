package execution

import (
	"github.com/shopspring/decimal"
	"testing"
)

func TestCarriedOptionRiskUsesExposureNotAbsoluteSteps(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		old, target             int64
		mark, gross, collateral string
	}{
		{"long-to-short", 10, -5, "5", "500", "500"},
		{"short-to-long", -1, 2, "5", "100", "100"},
		{"independent-gross-collateral", 4, -5, "150", "750", "600"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			i := cashOption("call")
			lot := VirtualLot{Instrument: i, Strategy: "a", ID: "oa", SignedSteps: tc.old}
			snapshot := AccountSnapshot{Lots: []VirtualLot{lot}, ActualPositions: []VirtualLot{lot}}
			targets := []ExecutableTarget{{Instrument: i.ID, Strategy: "a", Lot: "oa", SignedSteps: tc.target}}
			lots, positions, err := reservedCarriedExposure(snapshot, targets, []Instrument{i})
			if err != nil {
				t.Fatal(err)
			}
			mark := intentPrice(tc.mark)
			for _, values := range [][]exposureReservation{lots, positions} {
				if len(values) != 1 {
					t.Fatal(values)
				}
				got := values[0].collateral(mark, decimal.RequireFromString("0.1"))
				gross := values[0].riskGross(mark)
				if !got.Equal(intentPrice(tc.collateral)) || !gross.Equal(intentPrice(tc.gross)) {
					t.Fatalf("gross=%s collateral=%s; want gross=%s collateral=%s", gross, got, tc.gross, tc.collateral)
				}
			}
		})
	}
}

func TestCarriedRiskPreservesPendingReductionsAndAccountNet(t *testing.T) {
	i := cashOption("call")
	mark, rate := intentPrice("5"), intentPrice("0.1")
	old := VirtualLot{Instrument: i, Strategy: "a", ID: "oa", SignedSteps: -1}
	snapshot := AccountSnapshot{Lots: []VirtualLot{old}, ActualPositions: []VirtualLot{old}, Orders: []StoredOrder{{State: OrderAcknowledged, Intent: OrderIntent{Instrument: i, Side: Buy, Steps: 3, Allocations: []FillAllocation{{ID: "alloc", Strategy: "a", Lot: "oa", Side: Buy, Steps: 3}}}}}}
	targets := []ExecutableTarget{{Instrument: i.ID, Strategy: "a", Lot: "oa", SignedSteps: 2}}
	lots, positions, err := reservedCarriedExposure(snapshot, targets, []Instrument{i})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []exposureReservation{lots[0], positions[0]} {
		if r.SignedSteps != 2 || !r.collateral(mark, rate).Equal(intentPrice("100")) {
			t.Fatalf("pending projection=%d collateral=%s", r.SignedSteps, r.collateral(mark, rate))
		}
	}
	// Without carried declarations the acknowledged reduction still projects normally.
	lots, positions, err = reservedCarriedExposure(snapshot, nil, []Instrument{i})
	if err != nil || !lots[0].collateral(mark, rate).Equal(intentPrice("10")) || !positions[0].collateral(mark, rate).Equal(intentPrice("10")) {
		t.Fatal(lots, positions, err)
	}
	// Strategies cannot reuse each other's collateral, while account net can offset.
	snapshot = AccountSnapshot{Lots: []VirtualLot{{Instrument: i, Strategy: "a", ID: "oa", SignedSteps: 10}, {Instrument: i, Strategy: "b", ID: "ob", SignedSteps: -10}}, ActualPositions: []VirtualLot{{Instrument: i}}}
	targets = []ExecutableTarget{{Instrument: i.ID, Strategy: "a", Lot: "oa", SignedSteps: -5}, {Instrument: i.ID, Strategy: "b", Lot: "ob", SignedSteps: 5}}
	lots, positions, err = reservedCarriedExposure(snapshot, targets, []Instrument{i})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range lots {
		want := "500"
		if r.Strategy == "b" {
			want = "1000"
		}
		if !r.collateral(mark, rate).Equal(intentPrice(want)) {
			t.Fatalf("strategy %s collateral=%s want=%s", r.Strategy, r.collateral(mark, rate), want)
		}
	}
	if len(positions) != 1 || !positions[0].collateral(mark, rate).IsZero() {
		t.Fatal("account net not independently reserved", positions)
	}
}

func TestCarriedOptionReservationRechecksWholeAccountBudget(t *testing.T) {
	option := cashOption("call")
	perpetual := ledgerInstrument()
	perpetual.ID = "PERP"
	perpetual.QuantityStep = decimal.NewFromInt(1)
	perpetual.ContractSize = decimal.NewFromInt(1)
	lot := VirtualLot{Instrument: option, Strategy: "a", ID: "oa", SignedSteps: 10}
	snapshot := AccountSnapshot{Lots: []VirtualLot{lot}, ActualPositions: []VirtualLot{lot}, AccountSettledCash: intentPrice("1000"), SyntheticStrategyCash: map[StrategyID]decimal.Decimal{"a": intentPrice("1000")}}
	carried := ExecutableTarget{Instrument: option.ID, Strategy: "a", Lot: "oa", SignedSteps: -5}
	plan := Plan{RiskValidUntilMS: 100, Instruments: []Instrument{option, perpetual}, RebalancedInstruments: []string{perpetual.ID}, CarriedTargets: []ExecutableTarget{carried}, Targets: []ExecutableTarget{carried, {Instrument: perpetual.ID, Strategy: "a", Lot: "pa", SignedSteps: 1}}}
	risk := PortfolioRisk{MarginRate: intentPrice("1"), MaxAccountMargin: intentPrice("550"), MaxVirtualGross: intentPrice("10000"), StrategyGrossLimits: map[StrategyID]decimal.Decimal{"a": intentPrice("10000")}, Marks: map[string]decimal.Decimal{option.ID: intentPrice("5"), perpetual.ID: intentPrice("100")}}
	if err := validatePlanReservation(snapshot, plan, risk, 1); err == nil {
		t.Fatal("accepted margin 600 against cap 550")
	}
	risk.MaxAccountMargin = intentPrice("600")
	if err := validatePlanReservation(snapshot, plan, risk, 1); err != nil {
		t.Fatal("rejected exact margin 600", err)
	}
	risk.StrategyGrossLimits["a"] = intentPrice("550")
	if err := validatePlanReservation(snapshot, plan, risk, 1); err == nil {
		t.Fatal("accepted strategy gross 600 against cap 550")
	}
	// Grandfather an untouched reservation under lowered gross/account caps.
	plan.Targets[1].SignedSteps = 0
	risk.MaxAccountMargin = intentPrice("1")
	risk.StrategyGrossLimits["a"] = intentPrice("1")
	risk.MaxVirtualGross = intentPrice("1")
	if err := validatePlanReservation(snapshot, plan, risk, 1); err != nil {
		t.Fatal("unchanged carried reservation must not prevent reducing changed exposure", err)
	}
}

func TestCarriedPerpetualReservationKeepsMarkedMargin(t *testing.T) {
	i := ledgerInstrument()
	i.QuantityStep = decimal.NewFromInt(1)
	i.ContractSize = decimal.NewFromInt(1)
	lot := VirtualLot{Instrument: i, Strategy: "a", ID: "pa", SignedSteps: 10}
	snapshot := AccountSnapshot{Lots: []VirtualLot{lot}, ActualPositions: []VirtualLot{lot}}
	lots, positions, err := reservedCarriedExposure(snapshot, []ExecutableTarget{{Instrument: i.ID, Strategy: "a", Lot: "pa", SignedSteps: -5}}, []Instrument{i})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []exposureReservation{lots[0], positions[0]} {
		if !r.riskGross(intentPrice("100")).Equal(intentPrice("1000")) || !r.collateral(intentPrice("100"), intentPrice("0.1")).Equal(intentPrice("100")) {
			t.Fatal("perpetual marked reservation changed", r)
		}
	}
}
