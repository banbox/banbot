package factor

import (
	"fmt"
	"math"
	"reflect"
	"testing"
)

func compareMultiStageNumeric(t *testing.T, want, got Numeric, where string) {
	t.Helper()
	if want.Validity != got.Validity {
		t.Fatalf("%s validity: want %s got %s (%g vs %g)", where, want.Validity, got.Validity, want.Value, got.Value)
	}
	if want.Validity == Valid && math.Abs(want.Value-got.Value) > 1e-9*(1+math.Abs(want.Value)) {
		t.Fatalf("%s value: want %.15g got %.15g", where, want.Value, got.Value)
	}
}

func compareMultiStageFrames(t *testing.T, want, got Frame, where string) {
	t.Helper()
	if len(want.Values) != len(got.Values) {
		t.Fatalf("%s output count mismatch: want %d got %d", where, len(want.Values), len(got.Values))
	}
	for name, wantCol := range want.Values {
		gotCol, ok := got.Values[name]
		if !ok || len(wantCol) != len(gotCol) {
			t.Fatalf("%s/%s asset set mismatch: want %d got %d", where, name, len(wantCol), len(gotCol))
		}
		for sid, wv := range wantCol {
			gv, exists := gotCol[sid]
			if !exists {
				t.Fatalf("%s/%s missing sid %d", where, name, sid)
			}
			compareMultiStageNumeric(t, wv, gv, fmt.Sprintf("%s/%s/sid=%d", where, name, sid))
		}
	}
}

func TestFA02TopologicalStagePartitioningWarmupAndHashDeterminism(t *testing.T) {
	closeNode := Field("prices", "close", "1h")
	openNode := Field("prices", "open", "1h")
	volNode := Field("prices", "volume", "1h")
	sectorField := Field("sector_meta", "sector", "1h")

	// 1. Pure TS -> 1 stage [TS]
	tsOnly, err := New().Add("ema", EMA(closeNode, 5)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if tsOnly.StageCount() != 1 || tsOnly.Stages()[0].Kind != StageTS || tsOnly.WarmupLength() != 4 {
		t.Fatalf("unexpected pure TS stages/warmup: %+v warmup=%d", tsOnly.Stages(), tsOnly.WarmupLength())
	}

	// 2. TS -> CS -> 2 stages [TS, CS]
	tsCS, err := New().Add("z", ZScore(Winsorize(Return(closeNode, 3), 0.05))).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if tsCS.StageCount() != 2 || tsCS.Stages()[0].Kind != StageTS || tsCS.Stages()[1].Kind != StageCS || tsCS.WarmupLength() != 3 {
		t.Fatalf("unexpected TS->CS stages/warmup: %+v warmup=%d", tsCS.Stages(), tsCS.WarmupLength())
	}

	// 3. TS -> CS -> TS -> 3 stages [TS, CS, TS]
	smoothRank, err := New().Add("smooth", SMA(PctRank(volNode), 5)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if smoothRank.StageCount() != 3 || smoothRank.WarmupLength() != 4 || smoothRank.StateRetention() != 5 {
		t.Fatalf("unexpected TS->CS->TS contract: stages=%d warmup=%d retention=%d", smoothRank.StageCount(), smoothRank.WarmupLength(), smoothRank.StateRetention())
	}

	// 4. IVOL: TS -> CS -> TS -> CS -> 4 stages [TS, CS, TS, CS], warmup = 1 + (20 - 1) = 20
	ret1 := Return(closeNode, 1)
	ivol := StdDev(Residual(ret1, openNode), 20, 0)
	ivolScore := ZScore(ivol)
	ivolPlan, err := New().Add("ivol_z", ivolScore).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if ivolPlan.StageCount() != 4 || ivolPlan.WarmupLength() != 20 || ivolPlan.StateRetention() != 20 {
		t.Fatalf("unexpected IVOL contract: stages=%d warmup=%d retention=%d", ivolPlan.StageCount(), ivolPlan.WarmupLength(), ivolPlan.StateRetention())
	}
	wantKinds := []StageKind{StageTS, StageCS, StageTS, StageCS}
	gotStages := ivolPlan.Stages()
	for i, wk := range wantKinds {
		if gotStages[i].Index != i || gotStages[i].Kind != wk || len(gotStages[i].Nodes) == 0 {
			t.Fatalf("stage %d mismatch: %+v", i, gotStages[i])
		}
	}
	// Mutating returned Stages slice must not mutate Plan internal stages.
	gotStages[0].Kind = StageCS
	gotStages[0].Nodes[0] = "mutated"
	if ivolPlan.Stages()[0].Kind != StageTS || ivolPlan.Stages()[0].Nodes[0] == "mutated" {
		t.Fatal("Stages() exposed mutable internal slice")
	}

	// 5. 5-stage pipeline TS -> CS -> TS -> CS -> TS with skip-connections & GroupDemeanByField warmup propagation
	groupDem := GroupDemeanByField(Return(closeNode, 2), sectorField) // warmup 2, stage 1 (CS)
	residMom := TsSum(groupDem, 6)                                   // warmup 2 + 5 = 7, stage 2 (TS)
	csRanked := PctRank(residMom)                                    // warmup 7, stage 3 (CS)
	// Skip-connection combining Stage 0 (volNode), Stage 1 (groupDem), and Stage 3 (csRanked) in Stage 4 (TS)
	finalSmooth := EMA(Add(csRanked, Mul(Constant(0.1, "1h"), groupDem)), 4) // warmup 7 + 3 = 10, stage 4 (TS)
	fiveStage, err := New().
		Add("final", finalSmooth).
		Add("corr_skip", TsCorr(volNode, csRanked, 3)). // max(0, 7) + 2 = 9, stage 4 (TS)
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	if fiveStage.StageCount() != 5 || fiveStage.WarmupLength() != 10 || fiveStage.StateRetention() != 6 {
		t.Fatalf("5-stage contract mismatch: stages=%d warmup=%d retention=%d", fiveStage.StageCount(), fiveStage.WarmupLength(), fiveStage.StateRetention())
	}
	// Verify classification source inherited full DAG warmup length so Live/storage warmup covers historical group grids.
	inputs := fiveStage.Inputs()
	if len(inputs) != 2 {
		t.Fatalf("expected 2 input sources, got %+v", inputs)
	}
	for _, in := range inputs {
		if in.WarmupLength != 10 {
			t.Fatalf("input %s/%s warmup=%d, want 10", in.Source, in.TimeFrame, in.WarmupLength)
		}
	}

	// 6. Hash determinism & distinctness across parameter/topology changes
	ivolPlanDup, err := New().Add("ivol_z", ZScore(StdDev(Residual(Return(Field("prices", "close", "1h"), 1), Field("prices", "open", "1h")), 20, 0))).Compile()
	if err != nil || ivolPlan.Hash() != ivolPlanDup.Hash() {
		t.Fatalf("identical multi-stage DAG produced unstable hash: %v %s vs %s", err, ivolPlan.Hash(), ivolPlanDup.Hash())
	}
	ivolPlanDiffWindow, _ := New().Add("ivol_z", ZScore(StdDev(Residual(Return(Field("prices", "close", "1h"), 1), Field("prices", "open", "1h")), 21, 0))).Compile()
	if ivolPlan.Hash() == ivolPlanDiffWindow.Hash() {
		t.Fatal("different downstream TS window produced colliding plan hash")
	}
}

func TestFA02MultiStageStrategiesHandCalculatedAndAllScalarTSOperators(t *testing.T) {
	closeNode := Field("prices", "close", "1h")
	openNode := Field("prices", "open", "1h")
	volNode := Field("prices", "volume", "1h")

	// Hand-calculable check on 4 assets:
	// At every step t, volume for sid s in {1,2,3,4} rotates so PctRank(volume) is deterministic:
	// Let vol(sid, t) = float64(((int(sid)-1+t)%4)+1)*10 -> ranks {0, 1, 2, 3}, pct_ranks {0, 1/3, 2/3, 1}.
	// Over any 4 consecutive steps, every asset visits {0, 1/3, 2/3, 1} once -> SMA(..., 4) == 0.5 exactly!
	pctVol := PctRank(volNode)
	ret1 := Return(closeNode, 1)
	csDemRet := Sub(ret1, CsMean(ret1))
	res := Residual(ret1, openNode)
	zClose := ZScore(closeNode)
	macdLine, macdSig, macdHist := MACD(zClose, 2, 4, 2)
	bbUp, bbMid, bbLow := BBands(zClose, 4, 2.0, 1.5)

	plan, err := New().
		Add("sma_pctvol4", SMA(pctVol, 4)).
		Add("decay_z4", DecayLinear(zClose, 4)).
		Add("ivol_res", ZScore(StdDev(res, 5, 0))).
		Add("ivol_dem", StdDev(csDemRet, 5, 1)).
		Add("res_mom", TsSum(GroupDemean(ret1, "prices", "sector"), 4)).
		Add("res_z", TsZScore(MultiResidual(ret1, openNode, volNode), 4)).
		Add("cs_corr", TsCorr(PctRank(closeNode), pctVol, 5)).
		Add("cs_cov", TsCov(zClose, pctVol, 5)).
		Add("cs_beta", TsBeta(zClose, pctVol, 5)).
		Add("ema_cs", EMA(zClose, 4)).
		Add("rma_cs", RMA(pctVol, 4)).
		Add("wma_cs", WMA(pctVol, 4)).
		Add("rsi_cs", RSI(zClose, 4)).
		Add("roc_cs", ROC(Add(pctVol, Constant(1, "1h")), 3)).
		Add("mom_cs", MOM(zClose, 3)).
		Add("cci_cs", CCI(zClose, 4)).
		Add("high_cs", Highest(zClose, 4)).
		Add("low_cs", Lowest(zClose, 4)).
		Add("lag_cs", Lag(zClose, 2)).
		Add("ret_cs", Return(Add(pctVol, Constant(1, "1h")), 2)).
		Add("prod_cs", TsProduct(Add(pctVol, Constant(0.5, "1h")), 3)).
		Add("tsrank_cs", TsRank(zClose, 4)).
		Add("argmax_cs", TsArgMax(zClose, 4)).
		Add("argmin_cs", TsArgMin(zClose, 4)).
		Add("skew_cs", TsSkew(zClose, 4)).
		Add("kurt_cs", TsKurt(zClose, 4)).
		Add("slope_cs", TsSlope(zClose, 4)).
		Add("rsq_cs", TsRSquare(zClose, 4)).
		Add("vwma_cs", VWMA(zClose, Add(pctVol, Constant(1, "1h")), 4)).
		Add("obv_cs", OBV(zClose, volNode)).
		Add("macd_l", macdLine).
		Add("macd_s", macdSig).
		Add("macd_h", macdHist).
		Add("bb_u", bbUp).
		Add("bb_m", bbMid).
		Add("bb_l", bbLow).
		// Colliding period*10+ddof pairs (12*10+11 == 131 == 13*10+1) on the same CS node!
		Add("std_12_11", StdDev(zClose, 12, 11)).
		Add("std_13_1", StdDev(zClose, 13, 1)).
		Add("where_cs", Where(Gt(SMA(zClose, 3), Constant(0, "1h")), Clip(zClose, -1.5, 1.5), Sign(zClose))).
		Compile()
	if err != nil {
		t.Fatal(err)
	}

	const T = 36
	snaps := make([]*Snapshot, T)
	for i := 0; i < T; i++ {
		rows := make(map[int32]map[string]any, 4)
		for sid := int32(1); sid <= 4; sid++ {
			s := float64(sid)
			ti := float64(i + 1)
			sector := "G1"
			if sid >= 3 {
				sector = "G2"
			}
			rows[sid] = map[string]any{
				"close":  100 + s*3.5 + math.Sin(ti*s*0.37)*4.2 + 0.4*ti*s,
				"open":   10 + s*1.7 + math.Cos(ti*0.29*s),
				"volume": float64(((int(sid)-1+i)%4)+1) * 10.0,
				"sector": sector,
			}
		}
		snaps[i] = testSnapshot(t, int64(i+1)*3600_000, rows)
	}

	batch, err := plan.Batch(snaps, len(snaps))
	if err != nil {
		t.Fatal(err)
	}
	sess, err := NewSession(plan)
	if err != nil {
		t.Fatal(err)
	}
	for i, snap := range snaps {
		frame, err := sess.Evaluate(snap)
		if err != nil {
			t.Fatal(err)
		}
		compareMultiStageFrames(t, batch[i], frame, fmt.Sprintf("t=%d", i))
	}

	// Verify exact hand calculation: SMA(PctRank(volume), 4) == 0.5 for every asset at all t >= 3!
	for i := 0; i < 3; i++ {
		if batch[i].Values["sma_pctvol4"][1].Validity != Warmup {
			t.Fatalf("t=%d sma_pctvol4 expected Warmup, got %+v", i, batch[i].Values["sma_pctvol4"][1])
		}
	}
	for i := 3; i < T; i++ {
		for sid := int32(1); sid <= 4; sid++ {
			compareMultiStageNumeric(t, Numeric{0.5, Valid}, batch[i].Values["sma_pctvol4"][sid], fmt.Sprintf("hand sma_pctvol4 t=%d sid=%d", i, sid))
		}
	}
	// Verify std_12_11 and std_13_1 did not alias each other!
	if reflect.DeepEqual(batch[T-1].Values["std_12_11"], batch[T-1].Values["std_13_1"]) {
		t.Fatal("StdDev(12,11) and StdDev(13,1) aliased each other")
	}
	// Verify IVOL warmup: ret1 valid at t=1 -> res valid at t=1 -> StdDev(..., 5) valid at t=5 (after 5 warmup bars 0..4)!
	if batch[4].Values["ivol_res"][1].Validity != Warmup || batch[5].Values["ivol_res"][1].Validity != Valid {
		t.Fatalf("IVOL warmup boundary wrong: t=4=%+v t=5=%+v", batch[4].Values["ivol_res"][1], batch[5].Values["ivol_res"][1])
	}
}

func TestFA02DynamicPITUniverseMissingBarsWarmupAndForkCommitIsolation(t *testing.T) {
	closeNode := Field("prices", "close", "1h")
	openNode := Field("prices", "open", "1h")
	volNode := Field("prices", "volume", "1h")

	ret1 := Return(closeNode, 1)
	plan, err := New().
		Add("smooth_rank_z", ZScore(SMA(PctRank(ret1), 4))).
		Add("ema_resid", EMA(Residual(ret1, openNode), 3)).
		Add("cs_corr", TsCorr(PctRank(closeNode), PctRank(volNode), 4)).
		Add("rsi_group", RSI(GroupDemean(closeNode, "prices", "sector"), 3)).
		Add("std_12_11", StdDev(ZScore(closeNode), 12, 11)).
		Compile()
	if err != nil {
		t.Fatal(err)
	}

	makePITSnapshot := func(step int, investable, tracked []int32, version string) *Snapshot {
		at := int64(step+1) * 3600_000
		all := append(append([]int32(nil), investable...), tracked...)
		values := make(map[int32]map[string]any, len(all))
		for _, sid := range all {
			s := float64(sid)
			ti := float64(step + 1)
			sector := "A"
			if sid%2 == 0 {
				sector = "B"
			}
			fields := map[string]any{
				"close":  80 + s*5 + math.Sin(ti*0.41*s)*3 + 0.25*ti*s,
				"open":   20 + s*2 + math.Cos(ti*0.23*s),
				"volume": 500 + s*40 + math.Sin(ti*0.53+s)*80,
				"sector": sector,
			}
			// Inject diverse invalid values mid-series on SID 1 to verify zero NaN poisoning!
			if sid == 1 {
				switch step {
				case 4:
					fields["close"] = nil // Null
				case 9:
					delete(fields, "volume") // Missing
				case 14:
					fields["open"] = "corrupt" // NotNumeric
				case 22:
					fields["close"] = math.NaN() // NonFinite
				case 28:
					fields["volume"] = math.Inf(1) // NonFinite
				}
			}
			values[sid] = fields
		}
		base := testSnapshot(t, at, values)
		spec := base.Spec()
		spec.Universe = Universe{
			Version:    version,
			Reference:  investable,
			Investable: investable,
			Tradable:   investable,
			Evaluation: investable,
			Tracked:    tracked,
			Static:     false,
		}
		for sid := int32(1); sid <= 5; sid++ {
			spec.SIDMap[sid] = fmt.Sprintf("ASSET_%d", sid)
		}
		rows := make([]VersionRecord, 0, len(all))
		reqs := make([]Requirement, 0, len(all))
		for _, sid := range all {
			row, _ := base.Row(sid, "prices", "1h")
			rows = append(rows, row)
			reqs = append(reqs, Requirement{SID: sid, Source: "prices", TimeFrame: "1h", EventTime: at})
		}
		snap, err := Freeze(spec, rows, reqs)
		if err != nil {
			t.Fatal(err)
		}
		return snap
	}

	const T = 44
	snaps := make([]*Snapshot, T)
	for i := 0; i < T; i++ {
		switch {
		case i < 8:
			snaps[i] = makePITSnapshot(i, []int32{1, 2, 3, 4}, nil, "u1")
		case i < 15:
			// Asset 5 enters mid-backtest!
			snaps[i] = makePITSnapshot(i, []int32{1, 2, 3, 4, 5}, nil, "u2")
		case i < 20:
			// Asset 2 leaves Investable/Reference but remains Tracked!
			snaps[i] = makePITSnapshot(i, []int32{1, 3, 4, 5}, []int32{2}, "u3")
		case i < 27:
			// Asset 2 exits completely (state must be purged)!
			snaps[i] = makePITSnapshot(i, []int32{1, 3, 4, 5}, nil, "u4")
		default:
			// Asset 2 re-enters as a fresh listing (must warm up cleanly from scratch)!
			snaps[i] = makePITSnapshot(i, []int32{1, 2, 3, 4, 5}, nil, "u5")
		}
	}

	batch, err := plan.Batch(snaps, len(snaps))
	if err != nil {
		t.Fatal(err)
	}

	sess, err := NewSession(plan)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < T; i++ {
		// Exercise fork/rollback and fork/commit on multi-stage CS->TS states!
		if i == 11 || i == 25 || i == 31 {
			speculative, rev, err := sess.fork()
			if err != nil {
				t.Fatal(err)
			}
			for j := i; j < min(T, i+4); j++ {
				if _, err := speculative.Evaluate(snaps[j]); err != nil {
					t.Fatal(err)
				}
			}
			// Abandon speculative fork; live session must remain untouched and match Batch!
			if sess.revision != rev {
				t.Fatal("abandoned fork mutated base session revision")
			}
		}

		var frame Frame
		if i == 18 || i == 35 {
			// Evaluate through RoundBarrier.Compute (which forks and commits atomically)!
			var barrier RoundBarrier
			now := func() int64 { return snaps[i].Spec().DecisionTime + 10 }
			reqs := make([]Requirement, 0)
			var rows []VersionRecord
			for _, sid := range activeFactorSIDs(snaps[i].Spec()) {
				r, _ := snaps[i].Row(sid, "prices", "1h")
				rows = append(rows, r)
				reqs = append(reqs, Requirement{SID: sid, Source: "prices", TimeFrame: "1h", EventTime: snaps[i].Spec().GridTime})
			}
			tok, err := barrier.Begin(plan.Hash(), snaps[i].Spec(), reqs, now()+1000)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range rows {
				if err := barrier.Observe(tok, r, now()); err != nil {
					t.Fatal(err)
				}
			}
			frame, err = barrier.Compute(tok, sess, now)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			frame, err = sess.Evaluate(snaps[i])
			if err != nil {
				t.Fatal(err)
			}
		}
		compareMultiStageFrames(t, batch[i], frame, fmt.Sprintf("dynamic-pit/t=%d", i))

		// Check PIT entry/exit invariants:
		if i == 8 {
			// Newly listed asset 5 must be in Warmup on downstream TS windows while existing assets 3, 4 are Valid!
			if frame.Values["cs_corr"][5].Validity != Warmup || frame.Values["cs_corr"][3].Validity != Valid {
				t.Fatalf("new PIT entrant warmup mismatch: sid5=%+v sid3=%+v", frame.Values["cs_corr"][5], frame.Values["cs_corr"][3])
			}
		}
		if i == 16 {
			// Tracked asset 2 continues downstream TS history!
			if frame.Values["cs_corr"][2].Validity != Valid {
				t.Fatalf("tracked asset 2 lost CS->TS continuity: %+v", frame.Values["cs_corr"][2])
			}
		}
		if i == 21 {
			if _, exists := frame.Values["cs_corr"][2]; exists || sess.assets[2] != nil {
				t.Fatal("departed untracked asset 2 retained state or leaked into Frame")
			}
		}
		if i == 27 {
			// Re-entered asset 2 must start fresh in Warmup, not reuse pre-delist state!
			if frame.Values["cs_corr"][2].Validity != Warmup || frame.Values["ema_resid"][2].Validity != Warmup {
				t.Fatalf("re-entered asset 2 reused stale history: corr=%+v ema=%+v", frame.Values["cs_corr"][2], frame.Values["ema_resid"][2])
			}
		}
	}

	// Verify Session.Warmup on historical prefix 0..9 + Evaluate on 10..T-1 matches full evaluation!
	warmSess, err := NewSession(plan)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := warmSess.Warmup(snaps[i]); err != nil {
			t.Fatal(err)
		}
	}
	for i := 10; i < T; i++ {
		frame, err := warmSess.Evaluate(snaps[i])
		if err != nil {
			t.Fatal(err)
		}
		compareMultiStageFrames(t, batch[i], frame, fmt.Sprintf("warmup-then-eval/t=%d", i))
	}
}
