package factor

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func policyFixture(t *testing.T, bar int64, sequence uint64, weights map[int32]float64) PortfolioContext {
	t.Helper()
	f, u, s := portfolioFixture()
	s.DecisionTime = bar
	s.ExecutableAt = bar + 1
	s.ExpireAt = bar + 999
	s.PlanSequence = sequence
	f.DecisionTime = bar
	f.GridTime = bar
	ideal, err := NewTargetPortfolio(s, weights)
	if err != nil {
		t.Fatal(err)
	}
	return PortfolioContext{Frame: f, Universe: u, Spec: s, Ideal: ideal, GridTime: bar, BarMillis: 1000, Positions: map[int32]PositionEvidence{}, Marks: map[int32]float64{1: 100, 2: 100, 24: 100}, AssetNames: map[int32]string{1: "A", 2: "B", 24: "Z"}, SIDMappingVersion: "sid-v1"}
}
func baseLifecycle() PortfolioPolicyConfig {
	return PortfolioPolicyConfig{Policy: "lifecycle-v1", LongNotional: 1, Selection: SelectionConfig{LongK: 1}}
}
func proposePolicy(t *testing.T, p PortfolioPolicy, c PortfolioContext, state json.RawMessage) PortfolioProposal {
	t.Helper()
	result, err := p.Propose(c, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.NextState) == 0 {
		t.Fatal("missing checkpoint")
	}
	return result
}
func TestAllocationPrecisionImmutabilityAndExplicitAdapter(t *testing.T) {
	_, _, spec := portfolioFixture()
	input := map[int32]Allocation{1: {AbsoluteQuantity, "+0009007199254740993.12500"}, 2: {NAVFraction, "0.50"}}
	target, err := NewPortfolioTarget(spec, input)
	if err != nil {
		t.Fatal(err)
	}
	input[1] = Allocation{AbsoluteQuantity, "0"}
	got := target.Allocations()
	if got[1].Value != "9007199254740993.125" {
		t.Fatal(got)
	}
	got[1] = Allocation{AbsoluteQuantity, "1"}
	if target.Allocations()[1].Value != "9007199254740993.125" {
		t.Fatal("mutable")
	}
	if _, err = target.AsWeightPortfolio(); err == nil {
		t.Fatal("quantity silently converted")
	}
	encoded, _ := json.Marshal(target)
	var restored PortfolioTarget
	if err = json.Unmarshal(encoded, &restored); err != nil || restored.ID() != target.ID() {
		t.Fatalf("roundtrip %v", err)
	}
	scaled, err := ScaleQuantity("9007199254740993.125", 7, 8, "0.001")
	if err != nil || scaled != "7881299347898368.984" {
		t.Fatalf("exact decimal scale %s %v", scaled, err)
	}
	clamped, err := ClampQuantityMagnitude("-9007199254740993.125", "9007199254740993.124")
	if err != nil || clamped != "-9007199254740993.124" {
		t.Fatalf("clamp %s %v", clamped, err)
	}
}
func TestLifecycleQuantityLinearExitCurrentNAVNoBuybackAndAtomicCandidate(t *testing.T) {
	config := baseLifecycle()
	config.Holding.MinBars = 16
	config.Rebalance.EveryBars = 2
	config.Transition = TransitionConfig{Mode: "linear-exit", Basis: "quantity", ExitSteps: 8}
	p, err := NewLifecyclePolicy(config)
	if err != nil {
		t.Fatal(err)
	}
	entry := policyFixture(t, 1000, 1, map[int32]float64{1: 1})
	initial := proposePolicy(t, p, entry, nil)
	state := initial.NextState
	for j := 1; j <= 8; j++ {
		grid := int64(17000 + (j-1)*2000)
		c := policyFixture(t, grid, uint64(j+1), map[int32]float64{2: 1})
		c.Spec.Budget.NAV = 10000
		c.Ideal, _ = NewTargetPortfolio(c.Spec, map[int32]float64{2: 1})
		q := floatDecimal(float64(9 - j))
		c.Positions[1] = PositionEvidence{Quantity: q, FirstFillTime: 1000, Quantum: "1"}
		a := proposePolicy(t, p, c, state)
		again := proposePolicy(t, p, c, state)
		if a.Target.ID() != again.Target.ID() || !reflect.DeepEqual(a.NextState, again.NextState) {
			t.Fatal("unaccepted proposal mutates state")
		}
		want := floatDecimal(float64(8 - j))
		if got := a.Target.Allocations()[1]; got.Basis != AbsoluteQuantity || got.Value != want {
			t.Fatalf("step %d: %#v want %s", j, got, want)
		}
		if w := mustQuantity(a.Target.Allocations()[2].Value); w+float64(8-j)*100/c.Spec.Budget.NAV > 1+1e-12 {
			t.Fatal("tail plus new budget exceeds NAV")
		}
		state = a.NextState
		duplicate := proposePolicy(t, p, c, state)
		if duplicate.Target != nil {
			t.Fatal("duplicate grid repeated exit")
		}
	}
}
func TestLifecycleMaximumAgeBypassesScheduleAndMinimumAge(t *testing.T) {
	config := baseLifecycle()
	config.Rebalance.EveryBars = 8
	config.Holding.MinBars = 1
	config.Holding.MaxBars = 3
	config.Transition = TransitionConfig{Mode: "linear-exit", Basis: "weight", ExitSteps: 8}
	p, err := NewLifecyclePolicy(config)
	if err != nil {
		t.Fatal(err)
	}
	entry := policyFixture(t, 1000, 1, map[int32]float64{1: 1})
	a := proposePolicy(t, p, entry, nil)
	c := policyFixture(t, 4000, 2, map[int32]float64{1: 1})
	c.Positions[1] = PositionEvidence{Quantity: "10", FirstFillTime: 1000}
	result := proposePolicy(t, p, c, a.NextState)
	if result.Target == nil || result.Target.Allocations()[1].Value != "0" {
		t.Fatal("max age waits for ordinary schedule")
	}
}
func TestLifecyclePendingReconcileAndMissingScoreDoNotAdvance(t *testing.T) {
	config := baseLifecycle()
	config.Transition = TransitionConfig{Mode: "linear-exit", Basis: "quantity", ExitSteps: 8}
	p, _ := NewLifecyclePolicy(config)
	entry := policyFixture(t, 1000, 1, map[int32]float64{1: 1})
	a := proposePolicy(t, p, entry, nil)
	c := policyFixture(t, 2000, 2, map[int32]float64{2: 1})
	c.Positions[1] = PositionEvidence{Quantity: "8", FirstFillTime: 1000, IncreasingPending: true}
	waiting := proposePolicy(t, p, c, a.NextState)
	if !reflect.DeepEqual(waiting.ReconcileSIDs, []int32{1}) {
		t.Fatal(waiting.ReconcileSIDs)
	}
	s, _ := DecodeLifecycleState(waiting.NextState)
	if s.Assets[1].ExitStep != 0 {
		t.Fatal("pending made quantity exit anchor")
	}
	c.GridTime = 3000
	c.Spec.DecisionTime = 3000
	c.Spec.ExecutableAt = 3001
	c.Spec.ExpireAt = 3999
	c.Spec.PlanSequence = 3
	c.Frame.DecisionTime = 3000
	c.Ideal = nil
	c.Frame.Values["score"] = nil
	c.Positions[1] = PositionEvidence{Quantity: "8", FirstFillTime: 1000}
	missing := proposePolicy(t, p, c, waiting.NextState)
	s, _ = DecodeLifecycleState(missing.NextState)
	if s.Assets[1].ExitStep != 0 || missing.Target != nil {
		t.Fatal("missing score advanced ordinary exit")
	}
}
func TestCohortSeedAllPartialFillActual4Plan8DoesNotBuyBack(t *testing.T) {
	config := baseLifecycle()
	config.Rebalance.EveryBars = 2
	config.Transition = TransitionConfig{Mode: "cohort", PeriodBars: 16, Startup: "seed-all"}
	p, _ := NewLifecyclePolicy(config)
	entry := policyFixture(t, 1000, 1, map[int32]float64{1: 1})
	entry.Spec.Budget.NAV = 800
	entry.Ideal, _ = NewTargetPortfolio(entry.Spec, map[int32]float64{1: 1})
	a := proposePolicy(t, p, entry, nil)
	if a.Target.Allocations()[1].Value != "8" {
		t.Fatal(a.Target.Allocations())
	}
	c := policyFixture(t, 3000, 2, map[int32]float64{})
	c.Positions[1] = PositionEvidence{Quantity: "4", FirstFillTime: 1001}
	result := proposePolicy(t, p, c, a.NextState)
	if got := result.Target.Allocations()[1].Value; got != "3.5" {
		t.Fatalf("actual4-plan8 expiry target %s, want 3.5", got)
	}
	s, _ := DecodeLifecycleState(result.NextState)
	total := 0.0
	for _, cohort := range s.Cohorts {
		for _, x := range cohort.Contributions {
			total += mustQuantity(x.Filled)
		}
	}
	if total != 4 {
		t.Fatal("invented fills", total)
	}
}
func TestCohortExpiryNewBatchInternalTransferConservesNoExternalFill(t *testing.T) {
	config := baseLifecycle()
	config.Rebalance.EveryBars = 2
	config.Transition = TransitionConfig{Mode: "cohort", PeriodBars: 16, Startup: "seed-all"}
	p, _ := NewLifecyclePolicy(config)
	entry := policyFixture(t, 1000, 1, map[int32]float64{1: 1})
	entry.Spec.Budget.NAV = 800
	entry.Ideal, _ = NewTargetPortfolio(entry.Spec, map[int32]float64{1: 1})
	a := proposePolicy(t, p, entry, nil)
	c := policyFixture(t, 3000, 2, map[int32]float64{1: 1})
	c.Spec.Budget.NAV = 800
	c.Ideal, _ = NewTargetPortfolio(c.Spec, map[int32]float64{1: 1})
	c.Positions[1] = PositionEvidence{Quantity: "8", FirstFillTime: 1001}
	result := proposePolicy(t, p, c, a.NextState)
	if result.Target.Allocations()[1].Value != "8" {
		t.Fatal(result.Target.Allocations())
	}
	s, _ := DecodeLifecycleState(result.NextState)
	if len(s.Cohorts) != 8 {
		t.Fatalf("cohorts %d", len(s.Cohorts))
	}
	sum := 0.0
	newFilled := 0.0
	for _, cohort := range s.Cohorts {
		for _, x := range cohort.Contributions {
			sum += mustQuantity(x.Filled)
			if cohort.Created == 3000 {
				newFilled += mustQuantity(x.Filled)
			}
		}
	}
	if sum != 8 || newFilled != 1 || s.Assets[1].FirstFillTime != 1001 {
		t.Fatalf("transfer %g new %g firstfill %d", sum, newFilled, s.Assets[1].FirstFillTime)
	}
}
func TestPolicyConfigStrictAndAssetZeroOverride(t *testing.T) {
	for _, transition := range []TransitionConfig{{Mode: "direct", ExitSteps: 8}, {Mode: "cohort", PeriodBars: 16, Basis: "quantity"}, {Mode: "linear-exit", Basis: "quantity"}, {Mode: "geometric", Ratio: .875}} {
		config := baseLifecycle()
		config.Transition = transition
		if _, err := NormalizePortfolioPolicyConfig(config); err == nil {
			t.Fatal("invalid transition accepted", transition)
		}
	}
	zero := 0
	config := baseLifecycle()
	config.Holding.MinBars = 16
	config.Holding.ByAsset = map[string]HoldingOverride{"A": {MinBars: &zero}}
	policy, err := NewLifecyclePolicy(config)
	if err != nil {
		t.Fatal(err)
	}
	override := config.Holding.ByAsset["A"]
	override.MinBars = new(int)
	config.Holding.ByAsset["A"] = override
	c := policyFixture(t, 1000, 1, nil)
	if policy.(*LifecyclePolicy).holding(c, 1).MinBars != 0 {
		t.Fatal("explicit zero discarded")
	}
}
func TestSideIndependentSelectorStableTiesAndInverseVolatility(t *testing.T) {
	f, u, spec := portfolioFixture()
	config := baseLifecycle()
	config.Selection.LongK = 24
	p, _, err := SelectPortfolio(f, u, spec, config)
	if err != nil || p == nil || len(p.Targets()) != 24 {
		t.Fatalf("long only required second tail: %v", err)
	}
	selected := map[int32]float64{1: 1, 2: 1}
	config.Allocation.Method = "inverse-volatility"
	allocated, err := AllocateSelected(selected, config, 1000, nil, map[int32]float64{1: 1, 2: 2})
	if err != nil || math.Abs(allocated[1]-2.0/3) > 1e-12 {
		t.Fatal(allocated, err)
	}
	f.Values["score"][1] = Numeric{0, Valid}
	f.Values["score"][2] = Numeric{0, Valid}
	config.ShortNotional = 1
	config.LongNotional = 0
	config.Selection.LongK = 0
	config.Selection.ShortK = 1
	p, _, err = SelectPortfolio(f, u, spec, config)
	if err != nil || p.Targets()[1] != -1 {
		t.Fatal("short SID tie unstable")
	}
}
func TestScheduleAnchorDurationCalendarAndRemapping(t *testing.T) {
	config := RebalanceConfig{EveryBars: 2, Anchor: 1000}
	due, round, e := RebalanceDue(config, 3000, 1000, 0, true)
	if e != nil || !due || round != 1 {
		t.Fatal(due, round, e)
	}
	due, _, _ = RebalanceDue(config, 3500, 1000, round, true)
	if due {
		t.Fatal("data delay adds extra round")
	}
	due, _, e = RebalanceDue(RebalanceConfig{Duration: "2h"}, 7200000, 3600000, 0, true)
	if e != nil || !due {
		t.Fatal("duration", e)
	}
	_, _, e = RebalanceDue(RebalanceConfig{Calendar: "weekly", Timezone: "UTC", CalendarVersion: "iso-v1"}, 1700000000000, 1000, 0, true)
	if e != nil {
		t.Fatal(e)
	}
	p, _ := NewLifecyclePolicy(baseLifecycle())
	c := policyFixture(t, 1000, 1, map[int32]float64{1: 1})
	a := proposePolicy(t, p, c, nil)
	next := policyFixture(t, 2000, 2, map[int32]float64{1: 1})
	next.AssetNames[25] = "NEW"
	a = proposePolicy(t, p, next, a.NextState)
	next = policyFixture(t, 3000, 3, map[int32]float64{1: 1})
	next.AssetNames[1] = "REMAP"
	if _, e = p.Propose(next, a.NextState); e == nil {
		t.Fatal("SID remap accepted")
	}
}

func TestCohortLateFillRetainsOriginalOutstandingBatch(t *testing.T) {
	cohorts := []PortfolioCohort{{ID: "old", Created: 1000, EntryUntil: 2000, Expires: 2000, Contributions: []CohortContribution{{SID: 1, Planned: "2", Filled: "1", Outstanding: true}}}, {ID: "new", Created: 3000, EntryUntil: 4000, Expires: 10000, Contributions: []CohortContribution{{SID: 1, Planned: "2", Filled: "0"}}}}
	if err := ReconcileCohortContributions(cohorts, map[int32]PositionEvidence{1: {Quantity: "2", PendingUnknown: true}}, 3000); err != nil {
		t.Fatal(err)
	}
	if cohorts[0].Contributions[0].Filled != "2" || cohorts[1].Contributions[0].Filled != "0" {
		t.Fatal("late fill assigned to new cohort", cohorts)
	}
}
func TestCohortLedgerOriginSeparatesSeveralOutstandingPlans(t *testing.T) {
	cohorts := []PortfolioCohort{{ID: "older", PlanSequence: 1, Created: 1000, EntryUntil: 2000, Expires: 2000, Contributions: []CohortContribution{{SID: 1, Planned: "2", Filled: "1", Outstanding: true}}}, {ID: "old", PlanSequence: 2, Created: 2000, EntryUntil: 3000, Expires: 3000, Contributions: []CohortContribution{{SID: 1, Planned: "2", Filled: "1", Outstanding: true}}}, {ID: "new", PlanSequence: 3, Created: 4000, EntryUntil: 5000, Expires: 10000, Contributions: []CohortContribution{{SID: 1, Planned: "2", Filled: "0"}}}}
	evidence := map[int32]PositionEvidence{1: {Quantity: "3", PendingUnknown: true, FillEvents: []PositionFillEvidence{{Quantity: "1", LedgerCursor: 10, PlanSequence: 2, AtMS: 4000}}}}
	if err := ReconcileCohortContributions(cohorts, evidence, 4000); err != nil {
		t.Fatal(err)
	}
	if cohorts[0].Contributions[0].Filled != "1" || cohorts[1].Contributions[0].Filled != "2" || cohorts[2].Contributions[0].Filled != "0" {
		t.Fatal("ledger origin ignored", cohorts)
	}
}
func TestFillAgeStartsAtObservedFillAfterDecisionGrid(t *testing.T) {
	config := baseLifecycle()
	config.Holding.MinBars = 2
	p, _ := NewLifecyclePolicy(config)
	entry := policyFixture(t, 1000, 1, map[int32]float64{1: 1})
	a := proposePolicy(t, p, entry, nil)
	c := policyFixture(t, 3000, 2, map[int32]float64{2: 1})
	c.Positions[1] = PositionEvidence{Quantity: "10", FirstFillTime: 1001}
	a = proposePolicy(t, p, c, a.NextState)
	if a.Target.Allocations()[1].Value != "1" {
		t.Fatal("submission time used as fill age")
	}
	c = policyFixture(t, 4000, 3, map[int32]float64{2: 1})
	c.Positions[1] = PositionEvidence{Quantity: "10", FirstFillTime: 1001}
	a = proposePolicy(t, p, c, a.NextState)
	if a.Target.Allocations()[1].Value != "0" {
		t.Fatal("first eligible grid did not exit")
	}
}
func TestRiskOnlyMaximumUsesPatchAndAllowsCompleteSameGrid(t *testing.T) {
	config := baseLifecycle()
	config.Holding.MaxBars = 3
	config.Selection.LongK = 2
	p, _ := NewLifecyclePolicy(config)
	entry := policyFixture(t, 1000, 1, map[int32]float64{1: .5, 2: .5})
	a := proposePolicy(t, p, entry, nil)
	c := policyFixture(t, 5000, 2, nil)
	c.RiskOnly = true
	c.Spec.Budget.NAV = 2000
	c.Ideal = nil
	c.Frame.Values["score"] = nil
	c.Positions = map[int32]PositionEvidence{1: {Quantity: "5", FirstFillTime: 1001}, 2: {Quantity: "5", FirstFillTime: 3001}}
	risk := proposePolicy(t, p, c, a.NextState)
	if risk.Target == nil || risk.Target.Spec().Mode != Patch || len(risk.Target.Allocations()) != 1 || risk.Target.Allocations()[1].Value != "0" {
		t.Fatal("risk-only resize unrelated positions", risk.Target)
	}
	s, _ := DecodeLifecycleState(risk.NextState)
	if s.LastGrid != 1000 || s.LastRiskGrid != 5000 || s.Assets[2].Last.Value != "0.5" {
		t.Fatal("risk checkpoint consumed ordinary grid/changed unaffected state", s)
	}
	c = policyFixture(t, 5000, 3, map[int32]float64{2: 1})
	c.Positions = map[int32]PositionEvidence{1: {Quantity: "5", FirstFillTime: 1001}, 2: {Quantity: "5", FirstFillTime: 3001}}
	complete := proposePolicy(t, p, c, risk.NextState)
	if complete.Target == nil || complete.Target.Spec().Mode != Full {
		t.Fatal("complete same grid skipped after incomplete risk check")
	}
}
func TestCohortCurrentNAVLateOriginUsesFrozenOriginalDemand(t *testing.T) {
	cohorts := []PortfolioCohort{{ID: "old", PlanSequence: 1, Created: 1000, EntryUntil: 2000, Expires: 10000, Contributions: []CohortContribution{{SID: 1, Planned: "1", Filled: "1", Outstanding: true, EntrySequences: []uint64{1, 2}, EntryPlanned: map[uint64]string{1: "2", 2: "1"}}}}}
	evidence := map[int32]PositionEvidence{1: {Quantity: "2", FillEvents: []PositionFillEvidence{{Quantity: "1", PlanSequence: 1, LedgerCursor: 10}}}}
	if err := ReconcileCohortContributions(cohorts, evidence, 3000); err != nil {
		t.Fatal("later NAV resize invalidated frozen old fill demand", err)
	}
	if cohorts[0].Contributions[0].Filled != "2" {
		t.Fatal(cohorts)
	}
}
func TestRankRetentionDropoutAndMissingHeldScore(t *testing.T) {
	config := baseLifecycle()
	config.Selection.RetainRank = 2
	p, _ := NewLifecyclePolicy(config)
	entry := policyFixture(t, 1000, 1, map[int32]float64{23: 1})
	entry.Marks[23] = 100
	a := proposePolicy(t, p, entry, nil)
	c := policyFixture(t, 2000, 2, nil)
	c.Ideal = nil
	c.Marks[23] = 100
	c.Positions[23] = PositionEvidence{Quantity: "10", FirstFillTime: 1001}
	retained := proposePolicy(t, p, c, a.NextState)
	if retained.Target.Allocations()[23].Value != "1" || retained.Target.Allocations()[24].Value != "" {
		t.Fatal("rank buffer did not keep incumbent", retained.Target.Allocations())
	}
	config = baseLifecycle()
	config.Selection.LongK = 2
	config.Selection.Dropout = 1
	p, _ = NewLifecyclePolicy(config)
	entry = policyFixture(t, 1000, 1, map[int32]float64{1: .5, 2: .5})
	a = proposePolicy(t, p, entry, nil)
	c = policyFixture(t, 2000, 2, nil)
	c.Ideal = nil
	c.Positions = map[int32]PositionEvidence{1: {Quantity: "5", FirstFillTime: 1001}, 2: {Quantity: "5", FirstFillTime: 1001}}
	c.Marks[23] = 100
	dropout := proposePolicy(t, p, c, a.NextState)
	if dropout.Target.Allocations()[1].Value != "0" || dropout.Target.Allocations()[2].Value == "0" {
		t.Fatal("dropout did not replace weakest held rank", dropout.Target.Allocations())
	}
	config = baseLifecycle()
	p, _ = NewLifecyclePolicy(config)
	entry = policyFixture(t, 1000, 1, map[int32]float64{1: 1})
	a = proposePolicy(t, p, entry, nil)
	c = policyFixture(t, 2000, 2, nil)
	c.Ideal = nil
	delete(c.Frame.Values["score"], 1)
	c.Positions[1] = PositionEvidence{Quantity: "10", FirstFillTime: 1001}
	missing := proposePolicy(t, p, c, a.NextState)
	if missing.Target.Allocations()[1].Value != "1" || missing.Target.Allocations()[24].Value != "0" {
		t.Fatal("missing held score treated as worst rank", missing.Target.Allocations())
	}
}
func TestQuantityExitMissingScoreAndOffScheduleRiskReductionCannotBuyBack(t *testing.T) {
	config := baseLifecycle()
	config.Rebalance.EveryBars = 2
	config.Transition = TransitionConfig{Mode: "linear-exit", Basis: "quantity", ExitSteps: 8}
	p, _ := NewLifecyclePolicy(config)
	entry := policyFixture(t, 1000, 1, map[int32]float64{1: 1})
	a := proposePolicy(t, p, entry, nil)
	c := policyFixture(t, 3000, 2, map[int32]float64{2: 1})
	c.Positions[1] = PositionEvidence{Quantity: "8", FirstFillTime: 1001}
	a = proposePolicy(t, p, c, a.NextState)
	if a.Target.Allocations()[1].Value != "7" {
		t.Fatal(a.Target.Allocations())
	}
	c = policyFixture(t, 3500, 3, nil)
	c.Ideal = nil
	c.Positions[1] = PositionEvidence{Quantity: "3", FirstFillTime: 1001}
	delete(c.Frame.Values["score"], 1)
	risk := proposePolicy(t, p, c, a.NextState)
	if risk.Target == nil || risk.Target.Allocations()[1].Value != "3" {
		t.Fatal("off-schedule risk bought back quantity tail", risk.Target)
	}
	s, _ := DecodeLifecycleState(risk.NextState)
	if s.Assets[1].ExitStep != 1 {
		t.Fatal("risk clamp advanced ordinary exit step")
	}
}

func removePolicySID(c *PortfolioContext, sid int32) {
	delete(c.AssetNames, sid)
	delete(c.Positions, sid)
	delete(c.Marks, sid)
	for _, pool := range []*[]int32{&c.Universe.Investable, &c.Universe.Reference, &c.Universe.Tradable, &c.Universe.Evaluation, &c.Universe.Tracked} {
		kept := (*pool)[:0]
		for _, value := range *pool {
			if value != sid {
				kept = append(kept, value)
			}
		}
		*pool = kept
	}
}
func TestReleasedLifecyclePrunesAbsentPositionAfterEveryScopeAndCooldown(t *testing.T) {
	for _, kind := range []string{"released", "explicit-flat", "live-quantity", "pending", "unknown", "increasing", "cooldown", "nonzero-target", "name", "investable", "reference", "tradable", "evaluation", "tracked"} {
		t.Run(kind, func(t *testing.T) {
			p, _ := NewLifecyclePolicy(baseLifecycle())
			entry := policyFixture(t, 1000, 1, map[int32]float64{1: 1})
			initial := proposePolicy(t, p, entry, nil)
			s, _ := DecodeLifecycleState(initial.NextState)
			s.Assets[1].Last = Allocation{AbsoluteQuantity, "0"}
			s.Assets[1].Forced = true
			s.Assets[1].CooldownUntil = 2000
			state, _ := json.Marshal(s)
			c := policyFixture(t, 3000, 2, map[int32]float64{2: 1})
			removePolicySID(&c, 1)
			switch kind {
			case "explicit-flat":
				c.Positions[1] = PositionEvidence{Quantity: "0", PendingQuantity: "0"}
			case "live-quantity":
				c.Positions[1] = PositionEvidence{Quantity: "1", FirstFillTime: 1001}
			case "pending":
				c.Positions[1] = PositionEvidence{Quantity: "0", PendingQuantity: "1"}
			case "unknown":
				c.Positions[1] = PositionEvidence{Quantity: "0", PendingUnknown: true}
			case "increasing":
				c.Positions[1] = PositionEvidence{Quantity: "0", IncreasingPending: true}
			case "cooldown":
				s.Assets[1].CooldownUntil = 4000
				state, _ = json.Marshal(s)
			case "nonzero-target":
				s.Assets[1].Last = Allocation{AbsoluteQuantity, "1"}
				state, _ = json.Marshal(s)
			case "name":
				c.AssetNames[1] = "A"
			case "investable":
				c.Universe.Investable = append(c.Universe.Investable, 1)
			case "reference":
				c.Universe.Reference = append(c.Universe.Reference, 1)
			case "tradable":
				c.Universe.Tradable = append(c.Universe.Tradable, 1)
			case "evaluation":
				c.Universe.Evaluation = append(c.Universe.Evaluation, 1)
			case "tracked":
				c.Universe.Tracked = append(c.Universe.Tracked, 1)
			}
			result := proposePolicy(t, p, c, state)
			next, _ := DecodeLifecycleState(result.NextState)
			shouldPrune := kind == "released" || kind == "explicit-flat"
			_, present := next.Assets[1]
			if present == shouldPrune {
				t.Fatalf("lifecycle presence %v want prune %v", present, shouldPrune)
			}
			if shouldPrune {
				if _, ok := next.AssetIdentities[1]; ok {
					t.Fatal("released identity retained")
				}
				if _, ok := result.Target.Allocations()[1]; ok {
					t.Fatal("released zero SID re-emitted, requiring unsubscribed quote")
				}
			}
		})
	}
}
func TestReleasedCohortLifecyclePreservesActiveAndUnknownThenPrunesSettled(t *testing.T) {
	for _, kind := range []string{"active", "expired-filled", "expired-outstanding", "expired-unknown"} {
		t.Run(kind, func(t *testing.T) {
			config := baseLifecycle()
			config.Transition = TransitionConfig{Mode: "cohort", PeriodBars: 16}
			p, _ := NewLifecyclePolicy(config)
			entry := policyFixture(t, 1000, 1, map[int32]float64{1: 1})
			initial := proposePolicy(t, p, entry, nil)
			s, _ := DecodeLifecycleState(initial.NextState)
			s.Assets[1].Last = Allocation{AbsoluteQuantity, "0"}
			s.Assets[1].CooldownUntil = 2000
			s.Cohorts[0].Contributions[0].Filled = "0"
			s.Cohorts[0].Expires = 6000
			switch kind {
			case "expired-filled":
				s.Cohorts[0].Expires = 2000
				s.Cohorts[0].Contributions[0].Filled = "1"
			case "expired-outstanding", "expired-unknown":
				s.Cohorts[0].Expires = 2000
				s.Cohorts[0].Contributions[0].Outstanding = true
			}
			state, _ := json.Marshal(s)
			c := policyFixture(t, 3000, 2, map[int32]float64{2: 1})
			removePolicySID(&c, 1)
			if kind == "expired-unknown" {
				c.Positions[1] = PositionEvidence{Quantity: "0", PendingUnknown: true}
			}
			result := proposePolicy(t, p, c, state)
			next, _ := DecodeLifecycleState(result.NextState)
			_, present := next.Assets[1]
			shouldKeep := kind == "active" || kind == "expired-unknown"
			if present != shouldKeep {
				t.Fatalf("cohort lifecycle kept %v, want %v", present, shouldKeep)
			}
			if !shouldKeep {
				if _, ok := result.Target.Allocations()[1]; ok {
					t.Fatal("settled cohort zero re-emitted")
				}
			}
		})
	}
}
func TestGeometricDistinctFromLinearAndTargetStepMinimumProtection(t *testing.T) {
	config := baseLifecycle()
	config.Transition = TransitionConfig{Mode: "geometric", Basis: "quantity", Ratio: .875, FinalThreshold: .01}
	p, _ := NewLifecyclePolicy(config)
	entry := policyFixture(t, 1000, 1, map[int32]float64{1: 1})
	state := proposePolicy(t, p, entry, nil).NextState
	for step := 1; step <= 8; step++ {
		c := policyFixture(t, int64(step+1)*1000, uint64(step+1), map[int32]float64{2: 1})
		c.Positions[1] = PositionEvidence{Quantity: "8", FirstFillTime: 1001, Quantum: "0.000000001"}
		a := proposePolicy(t, p, c, state)
		state = a.NextState
		if step == 8 {
			q := mustQuantity(a.Target.Allocations()[1].Value)
			if math.Abs(q-8*math.Pow(.875, 8)) > 1e-8 || q == 0 {
				t.Fatalf("geometric became linear %g", q)
			}
		}
	}
	config = baseLifecycle()
	config.Holding.MinBars = 3
	config.Transition = TransitionConfig{Mode: "target-step", Alpha: .125}
	p, _ = NewLifecyclePolicy(config)
	a := proposePolicy(t, p, entry, nil)
	c := policyFixture(t, 2000, 2, map[int32]float64{2: 1})
	c.Positions[1] = PositionEvidence{Quantity: "10", FirstFillTime: 1001}
	protected := proposePolicy(t, p, c, a.NextState)
	if protected.Target.Allocations()[1].Value != "1" || protected.Target.Allocations()[2].Value != "0" {
		t.Fatal("target-step bypassed minimum age budget", protected.Target.Allocations())
	}
}
func TestWeightExitRevaluesCurrentNAVAndResumeDoesNotRestore(t *testing.T) {
	config := baseLifecycle()
	config.Transition = TransitionConfig{Mode: "linear-exit", Basis: "weight", ExitSteps: 8, OnReselect: "resume"}
	p, _ := NewLifecyclePolicy(config)
	entry := policyFixture(t, 1000, 1, map[int32]float64{1: 1})
	state := proposePolicy(t, p, entry, nil).NextState
	c := policyFixture(t, 2000, 2, map[int32]float64{2: 1})
	c.Positions[1] = PositionEvidence{Quantity: "10", FirstFillTime: 1001}
	exit := proposePolicy(t, p, c, state)
	if exit.Target.Allocations()[1] != (Allocation{NAVFraction, "0.875"}) {
		t.Fatal(exit.Target.Allocations())
	}
	for step := 3; step <= 4; step++ {
		c = policyFixture(t, int64(step)*1000, uint64(step), map[int32]float64{1: 1})
		c.Spec.Budget.NAV = 2000
		c.Ideal, _ = NewTargetPortfolio(c.Spec, map[int32]float64{1: 1})
		c.Positions[1] = PositionEvidence{Quantity: "8.75", FirstFillTime: 1001}
		exit = proposePolicy(t, p, c, exit.NextState)
		if exit.Target.Allocations()[1].Value != "0.875" {
			t.Fatal("resume silently restored", exit.Target.Allocations())
		}
	}
}
func TestTurnoverAfterCapsReportsConflictAndNeverBuysBackQuantityExit(t *testing.T) {
	config := baseLifecycle()
	config.Allocation.TurnoverLimit = .01
	p, _ := NewLifecyclePolicy(config)
	entry := policyFixture(t, 1000, 1, map[int32]float64{1: 1})
	a := proposePolicy(t, p, entry, nil)
	c := policyFixture(t, 2000, 2, map[int32]float64{2: 1})
	c.Positions[1] = PositionEvidence{Quantity: "2", FirstFillTime: 1001}
	c.CapitalLimit = 100
	c.Previous = a.Target
	result := proposePolicy(t, p, c, a.NextState)
	gross := 0.0
	for sid, allocation := range result.Target.Allocations() {
		w, _ := allocationWeight(allocation, c, sid)
		gross += math.Abs(w)
		if allocation.Basis == AbsoluteQuantity && mustQuantity(allocation.Value) > 2 {
			t.Fatal("turnover revived risk-reduced quantity")
		}
	}
	if gross > .1+1e-12 {
		t.Fatal("turnover violates risk caps", gross)
	}
}
