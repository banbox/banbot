package factor

import (
	"fmt"
	"math"
	"testing"

	"github.com/banbox/banta/tav"
)

func TestRollingTSOperatorsHandCalculatedAndEdgeCases(t *testing.T) {
	x := Field("prices", "x", "1h")
	y := Field("prices", "y", "1h")
	plan, err := New().
		Add("corr3", TsCorr(x, y, 3)).
		Add("cov3", TsCov(x, y, 3)).
		Add("beta_yx3", TsBeta(y, x, 3)).
		Add("beta_xy3", TsBeta(x, y, 3)).
		Add("sum3", TsSum(x, 3)).
		Add("prod3", TsProduct(x, 3)).
		Add("rank3", TsRank(x, 3)).
		Add("rank1", TsRank(x, 1)).
		Add("zscore3", TsZScore(x, 3)).
		Add("argmax3", TsArgMax(x, 3)).
		Add("argmin3", TsArgMin(x, 3)).
		Add("skew3", TsSkew(x, 3)).
		Add("kurt4", TsKurt(x, 4)).
		Add("slope3", TsSlope(x, 3)).
		Add("rsq3", TsRSquare(x, 3)).
		Add("decay3", DecayLinear(x, 3)).
		Add("wma3", WMA(x, 3)).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	// DecayLinear(x, 3) and WMA(x, 3) must share the exact same canonical DAG node via CSE.
	wmaOnly, err := New().Add("wma3", WMA(x, 3)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	decayOnly, err := New().Add("wma3", DecayLinear(x, 3)).Compile()
	if err != nil || wmaOnly.Hash() != decayOnly.Hash() {
		t.Fatalf("DecayLinear is not CSE-identical to WMA: err=%v %s vs %s", err, wmaOnly.Hash(), decayOnly.Hash())
	}

	// Sequence for SID 1:
	// t=0: x=1, y=3
	// t=1: x=2, y=5
	// t=2: x=3, y=7   -> linear y = 2x + 1: corr=1, cov=4/3, beta_yx=2, beta_xy=0.5, sum=6, prod=6, rank=1, zscore=sqrt(1.5), argmax=0, argmin=2, skew=0, slope=1, rsq=1, decay=(1+4+9)/6=14/6
	// t=3: x=4, y=9   -> window4 x=[1,2,3,4]: kurt4 = -1.2
	// t=4: x=6, y=9   -> window3 x=[3,4,6]: mean=13/3, dx=[-4/3,-1/3,5/3], s^2=7/3, skew3 = 10*sqrt(3)/(7*sqrt(7))
	// t=5: x=6, y=9   -> window3 x=[4,6,6]: tied top rank3 = 0.75, argmax=0 (most recent tie), argmin=2
	// t=6: x=0.1, y=0.1
	// t=7: x=0.1, y=0.1
	// t=8: x=0.1, y=0.1 -> flat window [0.1, 0.1, 0.1]: corr=0, cov=0, beta=0, zscore=0, rank3=0.5, skew3=0, slope3=0, rsq3=0
	// t=9: x=0.1, y=0.1 -> flat window4: kurt4=0
	// t=10: x=0, y=2  -> prod3 = 0
	// t=11: x=2, y=4  -> prod3 = 0
	// t=12: x=5, y=10 -> window3 x=[0,2,5]: prod3=0
	// t=13: x=3, y=6  -> window3 x=[2,5,3]: prod3=30 (recovers cleanly after zero!), argmax=1, argmin=2
	xs := []float64{1, 2, 3, 4, 6, 6, 0.1, 0.1, 0.1, 0.1, 0, 2, 5, 3}
	ys := []float64{3, 5, 7, 9, 9, 9, 0.1, 0.1, 0.1, 0.1, 2, 4, 10, 6}
	snapshots := make([]*Snapshot, len(xs))
	for i := range xs {
		snapshots[i] = testSnapshot(t, int64(i+1)*1000, map[int32]map[string]any{
			1: {"x": xs[i], "y": ys[i]},
			2: {"x": float64(i + 1), "y": float64(20 - i)},
		})
	}
	batch, err := plan.Batch(snapshots, len(snapshots))
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(plan)
	if err != nil {
		t.Fatal(err)
	}
	for i, snap := range snapshots {
		frame, err := session.Evaluate(snap)
		if err != nil {
			t.Fatal(err)
		}
		compareTechnicalFrames(t, batch[i], frame, fmt.Sprintf("hand/t=%d", i))
	}

	// Warmup assertions
	for _, col := range []string{"corr3", "cov3", "beta_yx3", "sum3", "prod3", "rank3", "zscore3", "argmax3", "argmin3", "skew3", "slope3", "rsq3"} {
		if batch[1].Values[col][1].Validity != Warmup {
			t.Fatalf("%s at t=1 want Warmup, got %+v", col, batch[1].Values[col][1])
		}
	}
	if batch[2].Values["kurt4"][1].Validity != Warmup {
		t.Fatalf("kurt4 at t=2 want Warmup, got %+v", batch[2].Values["kurt4"][1])
	}
	compareNumeric(t, Numeric{0.5, Valid}, batch[0].Values["rank1"][1], "rank1 period=1")

	// t=2 exact hand values
	compareNumeric(t, Numeric{1, Valid}, batch[2].Values["corr3"][1], "corr3 t=2")
	compareNumeric(t, Numeric{4.0 / 3.0, Valid}, batch[2].Values["cov3"][1], "cov3 t=2")
	compareNumeric(t, Numeric{2, Valid}, batch[2].Values["beta_yx3"][1], "beta_yx3 t=2")
	compareNumeric(t, Numeric{0.5, Valid}, batch[2].Values["beta_xy3"][1], "beta_xy3 t=2")
	compareNumeric(t, Numeric{6, Valid}, batch[2].Values["sum3"][1], "sum3 t=2")
	compareNumeric(t, Numeric{6, Valid}, batch[2].Values["prod3"][1], "prod3 t=2")
	compareNumeric(t, Numeric{1, Valid}, batch[2].Values["rank3"][1], "rank3 t=2")
	compareNumeric(t, Numeric{math.Sqrt(1.5), Valid}, batch[2].Values["zscore3"][1], "zscore3 t=2")
	compareNumeric(t, Numeric{0, Valid}, batch[2].Values["argmax3"][1], "argmax3 t=2")
	compareNumeric(t, Numeric{2, Valid}, batch[2].Values["argmin3"][1], "argmin3 t=2")
	compareNumeric(t, Numeric{0, Valid}, batch[2].Values["skew3"][1], "skew3 symmetric t=2")
	compareNumeric(t, Numeric{1, Valid}, batch[2].Values["slope3"][1], "slope3 t=2")
	compareNumeric(t, Numeric{1, Valid}, batch[2].Values["rsq3"][1], "rsq3 t=2")
	compareNumeric(t, Numeric{14.0 / 6.0, Valid}, batch[2].Values["decay3"][1], "decay3 t=2")

	// SID 2 has negative correlation y = 20 - i vs x = i + 1 -> corr = -1, slope = 1
	compareNumeric(t, Numeric{-1, Valid}, batch[2].Values["corr3"][2], "corr3 negative SID 2")

	// t=3: kurt4 on [1,2,3,4] == -1.2
	compareNumeric(t, Numeric{-1.2, Valid}, batch[3].Values["kurt4"][1], "kurt4 [1,2,3,4]")

	// t=4: skew3 on [3,4,6] == 10*sqrt(3) / (7*sqrt(7))
	compareNumeric(t, Numeric{10.0 * math.Sqrt(3.0) / (7.0 * math.Sqrt(7.0)), Valid}, batch[4].Values["skew3"][1], "skew3 [3,4,6]")

	// t=5: ties on [4,6,6]
	compareNumeric(t, Numeric{0.75, Valid}, batch[5].Values["rank3"][1], "rank3 tie [4,6,6]")
	compareNumeric(t, Numeric{0, Valid}, batch[5].Values["argmax3"][1], "argmax3 tie [4,6,6]")
	compareNumeric(t, Numeric{2, Valid}, batch[5].Values["argmin3"][1], "argmin3 tie [4,6,6]")

	// t=8 & t=9: flat [0.1, 0.1, 0.1, 0.1] must avoid float cancellation and return exact 0 (or 0.5 for rank)
	for _, col := range []string{"corr3", "cov3", "beta_yx3", "beta_xy3", "zscore3", "skew3", "slope3", "rsq3"} {
		compareNumeric(t, Numeric{0, Valid}, batch[8].Values[col][1], "flat "+col)
	}
	compareNumeric(t, Numeric{0.5, Valid}, batch[8].Values["rank3"][1], "flat rank3")
	compareNumeric(t, Numeric{0, Valid}, batch[9].Values["kurt4"][1], "flat kurt4")

	// t=10..13: product zero and recovery
	compareNumeric(t, Numeric{0, Valid}, batch[10].Values["prod3"][1], "prod3 zero")
	compareNumeric(t, Numeric{30, Valid}, batch[13].Values["prod3"][1], "prod3 recovery")
	compareNumeric(t, Numeric{1, Valid}, batch[13].Values["argmax3"][1], "argmax3 interior peak")
	compareNumeric(t, Numeric{2, Valid}, batch[13].Values["argmin3"][1], "argmin3 oldest min")

	// Verify TsArgMax / TsArgMin match tav.HighestBar / tav.LowestBar across full sequence
	wantMax := tav.HighestBar(xs, 3)
	wantMin := tav.LowestBar(xs, 3)
	for i := range xs {
		compareNumeric(t, technicalNumeric(wantMax[i]), batch[i].Values["argmax3"][1], fmt.Sprintf("tav HighestBar %d", i))
		compareNumeric(t, technicalNumeric(wantMin[i]), batch[i].Values["argmin3"][1], fmt.Sprintf("tav LowestBar %d", i))
	}
}

func TestRollingTSMissingGapsForkCommitAndOverflow(t *testing.T) {
	x := Field("prices", "x", "1h")
	y := Field("prices", "y", "1h")
	plan, err := New().
		Add("corr", TsCorr(x, y, 4)).
		Add("cov", TsCov(x, y, 4)).
		Add("beta", TsBeta(y, x, 4)).
		Add("sum", TsSum(x, 4)).
		Add("prod", TsProduct(x, 4)).
		Add("rank", TsRank(x, 4)).
		Add("zscore", TsZScore(x, 4)).
		Add("argmax", TsArgMax(x, 4)).
		Add("argmin", TsArgMin(x, 4)).
		Add("skew", TsSkew(x, 4)).
		Add("kurt", TsKurt(x, 4)).
		Add("slope", TsSlope(x, 4)).
		Add("rsquare", TsRSquare(x, 4)).
		Add("decay", DecayLinear(x, 4)).
		Compile()
	if err != nil {
		t.Fatal(err)
	}

	const n = 90
	all := make([]*Snapshot, n)
	for i := 0; i < n; i++ {
		fx := 50 + 5*math.Sin(float64(i)/3.0) + float64(i)*0.25
		fy := 20 + 3*math.Cos(float64(i)/4.0) - float64(i)*0.15
		fields1 := map[string]any{"x": fx, "y": fy}
		switch i {
		case 2:
			delete(fields1, "x") // Missing
		case 7:
			fields1["y"] = nil // Null on second leg
		case 15:
			fields1["x"] = "bad" // NotNumeric
		case 23:
			fields1["y"] = math.NaN() // NonFinite
		case 31:
			fields1["x"] = math.Inf(1)
		}
		if i >= 40 && i < 52 {
			delete(fields1, "x")
		}
		all[i] = testSnapshot(t, int64(i+1)*1000, map[int32]map[string]any{
			1: fields1,
			2: {"x": fx * 1.5, "y": fy + 3},
		})
	}

	batch, err := plan.Batch(all[:n-1], n-1)
	if err != nil {
		t.Fatal(err)
	}
	fullBatch, err := plan.Batch(all, n)
	if err != nil {
		t.Fatal(err)
	}

	session, err := NewSession(plan)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n-1; i++ {
		frame, err := session.Evaluate(all[i])
		if err != nil {
			t.Fatal(err)
		}
		compareTechnicalFrames(t, batch[i], frame, fmt.Sprintf("gap-parity/i=%d", i))
		if i == 35 {
			candidate, rev, err := session.fork()
			if err != nil {
				t.Fatal(err)
			}
			// Mutate candidate over several steps and discard it; live session must stay unaffected.
			for j := i + 1; j < i+5; j++ {
				if _, err := candidate.Evaluate(all[j]); err != nil {
					t.Fatal(err)
				}
			}
			if session.revision != rev {
				t.Fatal("speculative fork mutated base session revision")
			}
		}
	}
	// Fork + commit on final step and verify exact match with full Batch
	candidate, rev, err := session.fork()
	if err != nil {
		t.Fatal(err)
	}
	lastFrame, err := candidate.Evaluate(all[n-1])
	if err != nil {
		t.Fatal(err)
	}
	if err := session.commit(candidate, rev); err != nil {
		t.Fatal(err)
	}
	compareTechnicalFrames(t, fullBatch[n-1], lastFrame, "fork-commit-final")

	// Check invalid reason propagation at gap timestamps
	if batch[2].Values["corr"][1].Validity != Missing || batch[2].Values["sum"][1].Validity != Missing {
		t.Fatalf("expected Missing at i=2, got corr=%v sum=%v", batch[2].Values["corr"][1], batch[2].Values["sum"][1])
	}
	if batch[7].Values["corr"][1].Validity != Null || batch[7].Values["sum"][1].Validity != Valid {
		t.Fatalf("at i=7 y is Null while x is Valid: corr=%v sum=%v", batch[7].Values["corr"][1], batch[7].Values["sum"][1])
	}
	if batch[15].Values["skew"][1].Validity != NotNumeric {
		t.Fatalf("expected NotNumeric at i=15, got %v", batch[15].Values["skew"][1])
	}
	if batch[23].Values["beta"][1].Validity != NonFinite {
		t.Fatalf("expected NonFinite at i=23, got %v", batch[23].Values["beta"][1])
	}

	// Overflow test: generated overflow must be NonFinite, not Warmup
	ovPlan, err := New().Add("sum", TsSum(x, 2)).Add("prod", TsProduct(x, 2)).Add("cov", TsCov(x, y, 2)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	ovSnaps := []*Snapshot{
		testSnapshot(t, 1000, map[int32]map[string]any{1: {"x": math.MaxFloat64, "y": math.MaxFloat64}}),
		testSnapshot(t, 2000, map[int32]map[string]any{1: {"x": math.MaxFloat64, "y": -math.MaxFloat64}}),
	}
	ovBatch, err := ovPlan.Batch(ovSnaps, len(ovSnaps))
	if err != nil {
		t.Fatal(err)
	}
	ovSess, _ := NewSession(ovPlan)
	for i, s := range ovSnaps {
		f, err := ovSess.Evaluate(s)
		if err != nil {
			t.Fatal(err)
		}
		compareTechnicalFrames(t, ovBatch[i], f, fmt.Sprintf("overflow/%d", i))
	}
	for _, col := range []string{"sum", "prod", "cov"} {
		if ovBatch[0].Values[col][1].Validity != Warmup {
			t.Fatalf("%s t=0 want Warmup, got %v", col, ovBatch[0].Values[col][1])
		}
		if ovBatch[1].Values[col][1].Validity != NonFinite {
			t.Fatalf("%s t=1 overflow want NonFinite, got %v", col, ovBatch[1].Values[col][1])
		}
	}
}

func TestPointwiseSignClipComparisonsAndWhereShortCircuit(t *testing.T) {
	x := Field("prices", "x", "1h")
	y := Field("prices", "y", "1h")
	zero := Constant(0, "1h")
	plan, err := New().
		Add("sign", Sign(x)).
		Add("clip", Clip(x, -2.5, 3.5)).
		Add("gt", Gt(x, y)).
		Add("ge", Ge(x, y)).
		Add("lt", Lt(x, y)).
		Add("le", Le(x, y)).
		Add("safe_log", Where(Gt(x, zero), Log(x), zero)).
		Add("safe_else", Where(Le(x, zero), zero, Sqrt(x))).
		Add("cond_raw", Where(x, Constant(10, "1h"), Constant(-10, "1h"))).
		Compile()
	if err != nil {
		t.Fatal(err)
	}

	snapshots := []*Snapshot{
		testSnapshot(t, 1000, map[int32]map[string]any{
			1: {"x": 4.0, "y": 2.0},          // x > 0, x > y
			2: {"x": -5.0, "y": -5.0},        // x <= 0 -> Log(x) & Sqrt(x) are NonFinite, Where must return 0 Valid!
			3: {"x": 0.0, "y": 1.0},          // x == 0 -> Log(0) is NonFinite, Where must return 0 Valid!
			4: {"y": 1.0},                    // x Missing -> cond Missing -> Where returns Missing!
			5: {"x": nil, "y": 1.0},          // x Null -> cond Null -> Where returns Null!
			6: {"x": math.NaN(), "y": 1.0},   // x NonFinite -> cond NonFinite
		}),
	}
	batch, err := plan.Batch(snapshots, 1)
	if err != nil {
		t.Fatal(err)
	}
	session, _ := NewSession(plan)
	frame, err := session.Evaluate(snapshots[0])
	if err != nil {
		t.Fatal(err)
	}
	compareTechnicalFrames(t, batch[0], frame, "where-pointwise")

	// SID 1: x=4, y=2
	compareNumeric(t, Numeric{1, Valid}, frame.Values["sign"][1], "sign(4)")
	compareNumeric(t, Numeric{3.5, Valid}, frame.Values["clip"][1], "clip(4)")
	compareNumeric(t, Numeric{1, Valid}, frame.Values["gt"][1], "4 > 2")
	compareNumeric(t, Numeric{1, Valid}, frame.Values["ge"][1], "4 >= 2")
	compareNumeric(t, Numeric{0, Valid}, frame.Values["lt"][1], "4 < 2")
	compareNumeric(t, Numeric{0, Valid}, frame.Values["le"][1], "4 <= 2")
	compareNumeric(t, Numeric{math.Log(4), Valid}, frame.Values["safe_log"][1], "safe_log(4)")
	compareNumeric(t, Numeric{2, Valid}, frame.Values["safe_else"][1], "safe_else(4)")
	compareNumeric(t, Numeric{10, Valid}, frame.Values["cond_raw"][1], "cond_raw(4)")

	// SID 2: x=-5, y=-5 -> short-circuit unselected NonFinite branch!
	compareNumeric(t, Numeric{-1, Valid}, frame.Values["sign"][2], "sign(-5)")
	compareNumeric(t, Numeric{-2.5, Valid}, frame.Values["clip"][2], "clip(-5)")
	compareNumeric(t, Numeric{0, Valid}, frame.Values["gt"][2], "-5 > -5")
	compareNumeric(t, Numeric{1, Valid}, frame.Values["ge"][2], "-5 >= -5")
	compareNumeric(t, Numeric{0, Valid}, frame.Values["lt"][2], "-5 < -5")
	compareNumeric(t, Numeric{1, Valid}, frame.Values["le"][2], "-5 <= -5")
	compareNumeric(t, Numeric{0, Valid}, frame.Values["safe_log"][2], "safe_log(-5) short-circuits Log(-5)")
	compareNumeric(t, Numeric{0, Valid}, frame.Values["safe_else"][2], "safe_else(-5) short-circuits Sqrt(-5)")
	compareNumeric(t, Numeric{-10, Valid}, frame.Values["cond_raw"][2], "cond_raw(-5)")

	// SID 3: x=0, y=1 -> cond==0 is false (cond > 0 required for true)
	compareNumeric(t, Numeric{0, Valid}, frame.Values["sign"][3], "sign(0)")
	compareNumeric(t, Numeric{0, Valid}, frame.Values["clip"][3], "clip(0)")
	compareNumeric(t, Numeric{0, Valid}, frame.Values["safe_log"][3], "safe_log(0) short-circuits Log(0)")
	compareNumeric(t, Numeric{-10, Valid}, frame.Values["cond_raw"][3], "cond_raw(0) takes else")

	// SID 4, 5, 6: invalid cond propagates cond's exact Validity
	if frame.Values["safe_log"][4].Validity != Missing || frame.Values["clip"][4].Validity != Missing {
		t.Fatalf("SID 4 Missing not propagated: %+v", frame.Values["safe_log"][4])
	}
	if frame.Values["safe_log"][5].Validity != Null {
		t.Fatalf("SID 5 Null not propagated: %+v", frame.Values["safe_log"][5])
	}
	if frame.Values["safe_log"][6].Validity != NonFinite {
		t.Fatalf("SID 6 NonFinite not propagated: %+v", frame.Values["safe_log"][6])
	}
}

func TestPctRankNormalizationTiesSingleAssetAndValidation(t *testing.T) {
	x := Field("prices", "x", "1h")
	plan, err := New().Add("pct", PctRank(x)).Add("rank", Rank(x)).Compile()
	if err != nil {
		t.Fatal(err)
	}

	// 4 valid assets with ties: [10, 20, 20, 40] -> ranks [0, 1.5, 1.5, 3], pct_ranks [0, 0.5, 0.5, 1.0]
	// plus 1 Null asset (excluded from M, so M=4, not 5!)
	s1 := testSnapshot(t, 1000, map[int32]map[string]any{
		1: {"x": 10.0},
		2: {"x": 20.0},
		3: {"x": 20.0},
		4: {"x": 40.0},
		5: {"x": nil},
	})
	// Only 1 valid asset (M == 1 -> 0.5)
	s2 := testSnapshot(t, 2000, map[int32]map[string]any{
		1: {"x": 99.0},
		2: {"x": nil},
		3: {"x": nil},
		4: {"x": nil},
		5: {"x": nil},
	})
	// All invalid (M == 0 -> Missing/Null preserved)
	s3 := testSnapshot(t, 3000, map[int32]map[string]any{
		1: {"x": nil},
		2: {},
		3: {"x": nil},
		4: {"x": nil},
		5: {"x": nil},
	})

	snaps := []*Snapshot{s1, s2, s3}
	batch, err := plan.Batch(snaps, len(snaps))
	if err != nil {
		t.Fatal(err)
	}
	sess, _ := NewSession(plan)
	for i, s := range snaps {
		f, err := sess.Evaluate(s)
		if err != nil {
			t.Fatal(err)
		}
		compareTechnicalFrames(t, batch[i], f, fmt.Sprintf("pctrank/%d", i))
	}

	compareNumeric(t, Numeric{0, Valid}, batch[0].Values["pct"][1], "pctrank min")
	compareNumeric(t, Numeric{0.5, Valid}, batch[0].Values["pct"][2], "pctrank tie 1")
	compareNumeric(t, Numeric{0.5, Valid}, batch[0].Values["pct"][3], "pctrank tie 2")
	compareNumeric(t, Numeric{1, Valid}, batch[0].Values["pct"][4], "pctrank max")
	if batch[0].Values["pct"][5].Validity != Null {
		t.Fatalf("invalid asset got %+v", batch[0].Values["pct"][5])
	}
	compareNumeric(t, Numeric{0.5, Valid}, batch[1].Values["pct"][1], "pctrank M==1")
	if batch[2].Values["pct"][1].Validity != Null || batch[2].Values["pct"][2].Validity != Missing {
		t.Fatalf("M==0 validity mismatch: %+v", batch[2].Values["pct"])
	}

	// Contract validation rejections
	y := Field("prices", "y", "1h")
	badClipParam := Clip(x, 0, 1)
	delete(badClipParam.Spec.Parameters, "hi")
	badPctParam := PctRank(x)
	badPctParam.Spec.Parameters["extra"] = 1
	badWhereArity := Where(x, y, x)
	badWhereArity.Inputs = []*Node{x, y}
	for name, bad := range map[string]*Node{
		"skew_p2":     TsSkew(x, 2),
		"skew_p0":     TsSkew(x, 0),
		"kurt_p3":     TsKurt(x, 3),
		"corr_p0":     TsCorr(x, y, 0),
		"corr_maxp":   TsCorr(x, y, MaxIndicatorPeriod+1),
		"clip_lo_hi":  Clip(x, 5, 2),
		"clip_nan":    Clip(x, math.NaN(), 1),
		"clip_inf":    Clip(x, -1, math.Inf(1)),
		"clip_missing": badClipParam,
		"pct_param":   badPctParam,
		"where_arity": badWhereArity,
	} {
		if _, err := New().Add("v", bad).Compile(); err == nil {
			t.Fatalf("accepted invalid contract %s", name)
		}
	}
}
