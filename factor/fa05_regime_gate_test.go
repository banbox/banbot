package factor

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/banbox/banbot/orm"
)

func TestCsMeanAndCsSumBreadthReferencePoolAndValidation(t *testing.T) {
	closeNode := Field("prices", "close", "1h")
	sma3 := SMA(closeNode, 3)
	above := Gt(closeNode, sma3)
	ret1 := Return(closeNode, 1)

	plan, err := New().
		Add("mean_close", CsMean(closeNode)).
		Add("sum_close", CsSum(closeNode)).
		Add("breadth", CsMean(above)).
		Add("above_count", CsSum(above)).
		Add("mkt_ret", CsMean(ret1)).
		Compile()
	if err != nil {
		t.Fatal(err)
	}

	// 4 assets + 1 invalid asset across 4 bars:
	// Bar 1 (t=1000): [100, 100, 100, 100, nil] -> mean=100, sum=400, sma3/ret1 Warmup
	// Bar 2 (t=2000): [110, 90, 100, 100, nil]  -> ret1=[+0.1, -0.1, 0, 0] -> mkt_ret=0
	// Bar 3 (t=3000): [120, 80, 105, 95, nil]   -> sma3=[110, 90, 101.67, 98.33], above=[1, 0, 1, 0] -> breadth=0.5, count=2
	// Bar 4 (t=4000): [90, 70, 80, 80, nil]     -> sma3=[106.67, 80, 95, 91.67], above=[0, 0, 0, 0] -> breadth=0, count=0
	prices := [][]any{
		{100.0, 100.0, 100.0, 100.0, nil},
		{110.0, 90.0, 100.0, 100.0, nil},
		{120.0, 80.0, 105.0, 95.0, nil},
		{90.0, 70.0, 80.0, 80.0, nil},
	}
	var snaps []*Snapshot
	for i, row := range prices {
		snaps = append(snaps, testSnapshot(t, int64(i+1)*1000, map[int32]map[string]any{
			1: {"close": row[0]},
			2: {"close": row[1]},
			3: {"close": row[2]},
			4: {"close": row[3]},
			5: {"close": row[4]},
		}))
	}

	batch, err := plan.Batch(snaps, len(snaps))
	if err != nil {
		t.Fatal(err)
	}
	sess, err := NewSession(plan)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range snaps {
		f, err := sess.Evaluate(s)
		if err != nil {
			t.Fatal(err)
		}
		compareTechnicalFrames(t, batch[i], f, fmt.Sprintf("cs_mean_sum/%d", i))
	}

	for sid := int32(1); sid <= 4; sid++ {
		compareNumeric(t, Numeric{100, Valid}, batch[0].Values["mean_close"][sid], "bar0 mean")
		compareNumeric(t, Numeric{400, Valid}, batch[0].Values["sum_close"][sid], "bar0 sum")
		compareNumeric(t, Numeric{0, Valid}, batch[1].Values["mkt_ret"][sid], "bar1 mkt_ret")
		compareNumeric(t, Numeric{0.5, Valid}, batch[2].Values["breadth"][sid], "bar2 breadth")
		compareNumeric(t, Numeric{2, Valid}, batch[2].Values["above_count"][sid], "bar2 above_count")
		compareNumeric(t, Numeric{0, Valid}, batch[3].Values["breadth"][sid], "bar3 breadth")
		compareNumeric(t, Numeric{0, Valid}, batch[3].Values["above_count"][sid], "bar3 above_count")
	}
	if batch[2].Values["breadth"][5].Validity != Null {
		t.Fatalf("null asset should preserve Null, got %+v", batch[2].Values["breadth"][5])
	}

	// Separate Reference vs Investable pool: Reference={1,2} with values 10 and 30 (mean=20, sum=40);
	// Investable={1,2,3} where SID 3 has value 1000 (excluded from Reference fit, receives broadcast 20 and 40).
	refPlan, err := New().Add("mean", CsMean(closeNode)).Add("sum", CsSum(closeNode)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	var rows []VersionRecord
	var reqs []Requirement
	for sid, val := range map[int32]float64{1: 10, 2: 30, 3: 1000} {
		rows = append(rows, Record(orm.DataSeries{Source: "prices", Sid: sid, TimeMS: 0, EndMS: 1000, TimeFrame: "1h", Closed: true, Values: map[string]any{"close": val}}, 1, 1000, 1000, "v1"))
		reqs = append(reqs, Requirement{SID: sid, Source: "prices", TimeFrame: "1h", EventTime: 1000})
	}
	refSnap, err := Freeze(SnapshotSpec{
		GridTime: 1000, DecisionTime: 1000, ReplayTime: 1000,
		Universe:         Universe{Version: "u-ref", Reference: []int32{1, 2}, Investable: []int32{1, 2, 3}, Tradable: []int32{1, 2, 3}, Evaluation: []int32{1, 2, 3}, Static: true},
		SIDMap:           map[int32]string{1: "A", 2: "B", 3: "C"},
		Schemas:          map[string]string{"prices": "s1"},
		SourceVersions:   map[string]string{"prices": "v1"},
		VisibilityPolicy: "available-at",
	}, rows, reqs)
	if err != nil {
		t.Fatal(err)
	}
	refSess, _ := NewSession(refPlan)
	refFrame, err := refSess.Evaluate(refSnap)
	if err != nil {
		t.Fatal(err)
	}
	compareNumeric(t, Numeric{20, Valid}, refFrame.Values["mean"][3], "reference mean broadcast to investable")
	compareNumeric(t, Numeric{40, Valid}, refFrame.Values["sum"][3], "reference sum broadcast to investable")

	// Rejections: scalar parameter on cs-mean/cs-sum.
	badMean := CsMean(closeNode)
	badMean.Spec.Parameters["extra"] = 1
	badSum := CsSum(closeNode)
	badSum.Spec.Parameters["extra"] = 1
	for name, bad := range map[string]*Node{
		"mean_param": badMean,
		"sum_param":  badSum,
	} {
		if _, err := New().Add("v", bad).Compile(); err == nil {
			t.Fatalf("accepted invalid node %s", name)
		}
	}
}

func TestAbsoluteScoreGateTopBottomKAndLifecyclePolicy(t *testing.T) {
	zero := 0.0

	// 1. TopBottomKGated: distinguish nil vs &0.0, partial cash slots, rescale+asset_cap, and bear-market full cash exit.
	makeSmallFixture := func(scores map[int32]float64) (Frame, Universe, PortfolioSpec) {
		u := Universe{Version: "u1", Investable: []int32{1, 2, 3, 4}, Tradable: []int32{1, 2, 3, 4}, Reference: []int32{1, 2, 3, 4}, Static: true}
		vals := map[int32]Numeric{}
		for sid, sc := range scores {
			vals[sid] = Numeric{sc, Valid}
		}
		f := Frame{SnapshotID: "snap", PlanHash: "fp", DecisionTime: 1000, GridTime: 1000, Values: map[string]map[int32]Numeric{"score": vals}}
		s := PortfolioSpec{
			StrategyID: "strat", AccountID: "acc", DecisionTime: 1000, ExecutableAt: 1001, ExpireAt: 2000,
			PlanSequence: 1, SnapshotID: "snap", PlanHash: "sp", FactorPlanHash: "fp", UniverseVersion: "u1",
			Budget: FrozenBudget{Version: "b1", Currency: "USDT", NAV: 1000}, Mode: Full,
		}
		return f, u, s
	}

	// All negative scores (distinct): [-0.02, -0.05, -0.10, -0.20]
	fBear, u, s1 := makeSmallFixture(map[int32]float64{1: -0.02, 2: -0.05, 3: -0.10, 4: -0.20})
	// Omitted gate (nil) still buys top 2 least-negative coins:
	ungated, _, err := TopBottomKGated(fBear, "score", u, s1, 2, 1.0, 0, ScoreGate{LongMinScore: nil})
	if err != nil || ungated == nil || ungated.Targets()[1] != 0.5 || ungated.Targets()[2] != 0.5 {
		t.Fatalf("nil LongMinScore should remain ungated, got %v %v", ungated, err)
	}
	// Explicit LongMinScore = &0.0 exits 100% to cash (non-nil empty TargetPortfolio, EffectiveTargets zeroes previous!):
	prevSpec := s1
	prev, _ := NewTargetPortfolio(prevSpec, map[int32]float64{1: 0.5, 2: 0.5})
	s2 := s1
	s2.PlanSequence = 2
	gatedBear, _, err := TopBottomKGated(fBear, "score", u, s2, 2, 1.0, 0, ScoreGate{LongMinScore: &zero})
	if err != nil || gatedBear == nil || len(gatedBear.Targets()) != 0 {
		t.Fatalf("LongMinScore=0 on all-negative scores must emit empty cash target, got %v %v", gatedBear, err)
	}
	eff, err := gatedBear.EffectiveTargets(prev)
	if err != nil || eff[1] != 0 || eff[2] != 0 {
		t.Fatalf("empty Full target must zero prior long positions, got %v %v", eff, err)
	}

	// Constant negative scores (e.g. Where(ret > 0, ret, -1)) must ALSO exit to cash instead of skipping on constant-scores!
	fConstNeg, _, sConst := makeSmallFixture(map[int32]float64{1: -1, 2: -1, 3: -1, 4: -1})
	gatedConstNeg, _, err := TopBottomKGated(fConstNeg, "score", u, sConst, 2, 1.0, 0, ScoreGate{LongMinScore: &zero})
	if err != nil || gatedConstNeg == nil || len(gatedConstNeg.Targets()) != 0 {
		t.Fatalf("constant negative scores below gate must exit to cash, got %v %v", gatedConstNeg, err)
	}

	// Partial positive scores: 1 coin >= 0 (SID 1 = 0.0, exact boundary >= 0!), k=4, LongNotional=1.0
	fOnePos, _, sPart := makeSmallFixture(map[int32]float64{1: 0.0, 2: -0.01, 3: -0.05, 4: -0.10})
	partCash, _, err := TopBottomKGated(fOnePos, "score", u, sPart, 4, 1.0, 0, ScoreGate{LongMinScore: &zero})
	if err != nil || partCash == nil || len(partCash.Targets()) != 1 || math.Abs(partCash.Targets()[1]-0.25) > 1e-12 {
		t.Fatalf("1 of 4 slots passing >=0 should get 0.25 weight (0.75 cash), got %v %v", partCash, err)
	}
	partRescale, _, err := TopBottomKGated(fOnePos, "score", u, sPart, 4, 1.0, 0, ScoreGate{LongMinScore: &zero, UnfilledSlotPolicy: "rescale", AssetCap: 0.4})
	if err != nil || partRescale == nil || math.Abs(partRescale.Targets()[1]-0.4) > 1e-12 {
		t.Fatalf("rescale with AssetCap=0.4 should clamp single passing asset to 0.4, got %v %v", partRescale, err)
	}

	// ShortMaxScore = &0.0: only 1 coin <= 0 (SID 4 = -0.05), k=2, ShortNotional=0.8
	fShort, _, sShort := makeSmallFixture(map[int32]float64{1: 0.30, 2: 0.20, 3: 0.10, 4: -0.05})
	shortGated, _, err := TopBottomKGated(fShort, "score", u, sShort, 2, 0, 0.8, ScoreGate{ShortMaxScore: &zero})
	if err != nil || shortGated == nil || len(shortGated.Targets()) != 1 || math.Abs(shortGated.Targets()[4]+0.4) > 1e-12 {
		t.Fatalf("ShortMaxScore=0 with 1 of 2 passing should allocate -0.4, got %v %v", shortGated, err)
	}

	// 2. LifecyclePolicy path: bull -> partial -> bear (with RetainRank set!) -> liquidates to "0" without skipping.
	cfg := PortfolioPolicyConfig{
		Policy:       "lifecycle-v1",
		LongNotional: 1.0,
		Selection: SelectionConfig{
			LongK:        2,
			LongMinScore: &zero,
			RetainRank:   4, // RetainRank must NOT override LongMinScore when score < 0!
		},
	}
	policy, err := NewLifecyclePolicy(cfg)
	if err != nil {
		t.Fatal(err)
	}
	assetNames := map[int32]string{1: "A", 2: "B", 3: "C", 4: "D"}
	marks := map[int32]float64{1: 100, 2: 100, 3: 100, 4: 100}

	// Round 1 (t=1000): SID 1 (+0.2) and SID 2 (+0.1) positive -> each gets 0.5
	f1, _, sp1 := makeSmallFixture(map[int32]float64{1: 0.2, 2: 0.1, 3: -0.1, 4: -0.2})
	p1 := proposePolicy(t, policy, PortfolioContext{
		Frame: f1, Universe: u, Spec: sp1, GridTime: 1000, BarMillis: 1000,
		Positions: map[int32]PositionEvidence{}, Marks: marks, AssetNames: assetNames, SIDMappingVersion: "sid-v1",
	}, nil)
	if p1.Target.Allocations()[1].Value != "0.5" || p1.Target.Allocations()[2].Value != "0.5" {
		t.Fatalf("round 1 expected 0.5/0.5, got %+v", p1.Target.Allocations())
	}

	// Round 2 (t=2000): only SID 1 (+0.05) >= 0; SID 2 (-0.05) drops below 0 -> SID 1 stays 0.5, SID 2 exits to 0, 0.5 cash!
	f2, _, sp2 := makeSmallFixture(map[int32]float64{1: 0.05, 2: -0.05, 3: -0.1, 4: -0.2})
	sp2.DecisionTime, sp2.ExecutableAt, sp2.ExpireAt, sp2.PlanSequence = 2000, 2001, 2999, 2
	f2.DecisionTime, f2.GridTime = 2000, 2000
	p2 := proposePolicy(t, policy, PortfolioContext{
		Frame: f2, Universe: u, Spec: sp2, GridTime: 2000, BarMillis: 1000,
		Positions: map[int32]PositionEvidence{
			1: {Quantity: "5", FirstFillTime: 1001},
			2: {Quantity: "5", FirstFillTime: 1001},
		},
		Marks: marks, AssetNames: assetNames, SIDMappingVersion: "sid-v1",
	}, p1.NextState)
	if p2.Target.Allocations()[1].Value != "0.5" || p2.Target.Allocations()[2].Value != "0" {
		t.Fatalf("round 2 expected SID 1=0.5, SID 2=0, got %+v", p2.Target.Allocations())
	}

	// Round 3 (t=3000): ALL coins negative [-0.01, -0.05, -0.10, -0.20] -> SID 1 must ALSO exit to "0"!
	f3, _, sp3 := makeSmallFixture(map[int32]float64{1: -0.01, 2: -0.05, 3: -0.10, 4: -0.20})
	sp3.DecisionTime, sp3.ExecutableAt, sp3.ExpireAt, sp3.PlanSequence = 3000, 3001, 3999, 3
	f3.DecisionTime, f3.GridTime = 3000, 3000
	p3 := proposePolicy(t, policy, PortfolioContext{
		Frame: f3, Universe: u, Spec: sp3, GridTime: 3000, BarMillis: 1000,
		Positions: map[int32]PositionEvidence{
			1: {Quantity: "5", FirstFillTime: 1001},
		},
		Marks: marks, AssetNames: assetNames, SIDMappingVersion: "sid-v1",
	}, p2.NextState)
	if p3.Target == nil || p3.Target.Allocations()[1].Value != "0" {
		t.Fatalf("round 3 all-negative bear market must exit SID 1 to 0, got %+v", p3.Target)
	}
}

func TestExposureScalesHelperAndConfigHashStability(t *testing.T) {
	u := Universe{Version: "u1", Reference: []int32{1, 2}, Investable: []int32{1, 2}, Tradable: []int32{1, 2}, Static: true}
	f := Frame{
		SnapshotID: "snap", PlanHash: "fp", DecisionTime: 1000, GridTime: 1000,
		Values: map[string]map[int32]Numeric{
			"score":          {1: {0.5, Valid}, 2: {-0.5, Valid}},
			"exposure_scale": {1: {0.5, Valid}, 2: {0.5, Valid}},
			"long_scale":     {1: {1.5, Valid}, 2: {1.5, Valid}},  // clamped to 1.0 -> 0.5 * 1.0 = 0.5
			"short_scale":    {1: {-0.2, Valid}, 2: {-0.2, Valid}}, // clamped to 0.0 -> 0.5 * 0.0 = 0.0
		},
	}
	ls, ss, ok := ExposureScales(f, u)
	if !ok || math.Abs(ls-0.5) > 1e-12 || ss != 0 {
		t.Fatalf("unexpected scales: ls=%g ss=%g ok=%v", ls, ss, ok)
	}

	// All invalid scale values fall back to 1.0
	fInvalid := Frame{Values: map[string]map[int32]Numeric{
		"long_scale": {1: {math.NaN(), Warmup}, 2: {math.NaN(), Missing}},
	}}
	ls, ss, ok = ExposureScales(fInvalid, u)
	if !ok || ls != 1 || ss != 1 {
		t.Fatalf("all-invalid scale must default to 1.0, got ls=%g ss=%g ok=%v", ls, ss, ok)
	}

	// LifecyclePolicy ConfigHash must stay invariant across dynamic exposure changes across rounds!
	cfg := PortfolioPolicyConfig{Policy: "lifecycle-v1", LongNotional: 1.0, Selection: SelectionConfig{LongK: 1}}
	policy, err := NewLifecyclePolicy(cfg)
	if err != nil {
		t.Fatal(err)
	}
	spec1 := PortfolioSpec{
		StrategyID: "s", AccountID: "a", DecisionTime: 1000, ExecutableAt: 1001, ExpireAt: 1999,
		PlanSequence: 1, SnapshotID: "snap", PlanHash: "sp", FactorPlanHash: "fp", UniverseVersion: "u1",
		Budget: FrozenBudget{Version: "b1", Currency: "USDT", NAV: 1000}, Mode: Full,
	}
	r1 := proposePolicy(t, policy, PortfolioContext{
		Frame: f, Universe: u, Spec: spec1, GridTime: 1000, BarMillis: 1000,
		Positions: map[int32]PositionEvidence{}, Marks: map[int32]float64{1: 100, 2: 100},
		AssetNames: map[int32]string{1: "A", 2: "B"}, SIDMappingVersion: "sid-v1",
	}, nil)
	if r1.Target.Allocations()[1].Value != "0.5" {
		t.Fatalf("round 1 scaled allocation want 0.5, got %+v", r1.Target.Allocations())
	}
	var st1 LifecycleState
	_ = json.Unmarshal(r1.NextState, &st1)

	// Round 2: exposure_scale drops to 0.0 -> must succeed with same checkpoint ConfigHash and flatten SID 1 to "0"!
	f2 := CloneFrame(f)
	f2.DecisionTime, f2.GridTime = 2000, 2000
	f2.Values["exposure_scale"] = map[int32]Numeric{1: {0, Valid}, 2: {0, Valid}}
	spec2 := spec1
	spec2.DecisionTime, spec2.ExecutableAt, spec2.ExpireAt, spec2.PlanSequence = 2000, 2001, 2999, 2
	r2 := proposePolicy(t, policy, PortfolioContext{
		Frame: f2, Universe: u, Spec: spec2, GridTime: 2000, BarMillis: 1000,
		Positions: map[int32]PositionEvidence{1: {Quantity: "5", FirstFillTime: 1001}},
		Marks: map[int32]float64{1: 100, 2: 100}, AssetNames: map[int32]string{1: "A", 2: "B"}, SIDMappingVersion: "sid-v1",
	}, r1.NextState)
	var st2 LifecycleState
	_ = json.Unmarshal(r2.NextState, &st2)
	if st1.ConfigHash != st2.ConfigHash || r2.Target.Allocations()[1].Value != "0" {
		t.Fatalf("round 2 ConfigHash or zero-scale exit failed: %s vs %s, target=%+v", st1.ConfigHash, st2.ConfigHash, r2.Target.Allocations())
	}
}
