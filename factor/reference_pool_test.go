package factor

import (
	"math"
	"reflect"
	"testing"
)

func referencePoolSnapshot(t *testing.T, at int64, values map[int32]map[string]any, reference []int32) *Snapshot {
	t.Helper()
	base := testSnapshot(t, at, values)
	spec := base.Spec()
	spec.Universe.Reference = reference
	spec.Universe.Investable, spec.Universe.Tradable, spec.Universe.Tracked = []int32{1, 2}, []int32{1, 2}, []int32{1, 2}
	spec.Universe.Evaluation = []int32{5}
	spec.SIDMap[5] = "evaluation-only"
	rows := []VersionRecord{}
	needs := []Requirement{}
	for sid, fields := range values {
		rows = append(rows, testRecord(sid, at, fields))
		needs = append(needs, Requirement{SID: sid, Source: "prices", Frequency: "1h", EventTime: at})
	}
	snapshot, err := Freeze(spec, rows, needs)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestReferenceStatisticsApplyToDisjointInvestablePortfolio(t *testing.T) {
	x := Field("prices", "close", "1h")
	plan, err := New().Add("score", ZScore(x)).Add("winsor", Winsorize(x, .1)).Add("quantile", Quantile(x, .5)).Add("group", GroupZScore(x, "prices", "sector")).Add("demean", GroupDemean(x, "prices", "sector")).Add("residual", Residual(x, Field("prices", "x", "1h"))).Compile()
	if err != nil {
		t.Fatal(err)
	}
	values := map[int32]map[string]any{1: {"close": 10.0, "x": 1.0, "sector": "A"}, 2: {"close": 20.0, "x": 2.0, "sector": "A"}, 3: {"close": 30.0, "x": 3.0, "sector": "A"}, 4: {"close": 40.0, "x": 4.0, "sector": "A"}}
	evaluate := func(fields map[int32]map[string]any) Frame {
		session, _ := NewSession(plan)
		frame, err := session.Evaluate(referencePoolSnapshot(t, 1000, fields, []int32{3, 4}))
		if err != nil {
			t.Fatal(err)
		}
		return frame
	}
	frame := evaluate(values)
	for sid, want := range map[int32]float64{1: -5, 2: -3, 3: -1, 4: 1} {
		compareNumeric(t, Numeric{want, Valid}, frame.Values["score"][sid], "reference zscore")
		compareNumeric(t, Numeric{want, Valid}, frame.Values["group"][sid], "reference group zscore")
		compareNumeric(t, Numeric{want * 5, Valid}, frame.Values["demean"][sid], "reference group mean")
		compareNumeric(t, Numeric{35, Valid}, frame.Values["quantile"][sid], "reference quantile")
		compareNumeric(t, Numeric{0, Valid}, frame.Values["residual"][sid], "reference regression")
	}
	compareNumeric(t, Numeric{31, Valid}, frame.Values["winsor"][1], "reference winsor cutoff")
	_, _, portfolioSpec := portfolioFixture()
	portfolioSpec.SnapshotID, portfolioSpec.FactorPlanHash, portfolioSpec.DecisionTime, portfolioSpec.UniverseVersion = frame.SnapshotID, frame.PlanHash, frame.DecisionTime, "universe-v1"
	portfolio, diagnostics, err := TopBottomK(frame, "score", Universe{Version: "universe-v1", Investable: []int32{1, 2}, Tradable: []int32{1, 2}}, portfolioSpec, 1)
	if err != nil || portfolio == nil || len(diagnostics) != 0 || !reflect.DeepEqual(portfolio.Targets(), map[int32]float64{1: -.5, 2: .5}) {
		t.Fatalf("disjoint portfolio: %v %v %v", portfolio, diagnostics, err)
	}
	values[1]["close"] = 1000.0
	activeChanged := evaluate(values)
	compareNumeric(t, frame.Values["score"][3], activeChanged.Values["score"][3], "investable cannot change fit")
	compareNumeric(t, Numeric{193, Valid}, activeChanged.Values["score"][1], "investable transformed using reference fit")
	values[3]["close"] = 20.0
	referenceChanged := evaluate(values)
	compareNumeric(t, Numeric{97, Valid}, referenceChanged.Values["score"][1], "reference changes fit")
	if _, ok := frame.Values["score"][5]; ok {
		t.Fatal("evaluation-only SID synthesized")
	}
}

func TestReferenceRankTiesBoundsAndUnavailableGroups(t *testing.T) {
	x := Field("prices", "close", "1h")
	plan, err := New().Add("rank", Rank(x)).Add("group", GroupZScore(x, "prices", "sector")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		values             [2]float64
		expected           [2]float64
		reference          [2]float64
		firstReferenceRank float64
	}{{[2]float64{1, 3}, [2]float64{-.5, 1.5}, [2]float64{2, 2}, .5}, {[2]float64{2, 2}, [2]float64{.5, .5}, [2]float64{2, 2}, .5}, {[2]float64{30, 15}, [2]float64{.5, -.5}, [2]float64{20, 40}, 0}} {
		values := map[int32]map[string]any{1: {"close": test.values[0], "sector": "uncovered"}, 2: {"close": test.values[1], "sector": nil}, 3: {"close": test.reference[0], "sector": "A"}, 4: {"close": test.reference[1], "sector": "A"}}
		session, _ := NewSession(plan)
		frame, err := session.Evaluate(referencePoolSnapshot(t, 1000, values, []int32{3, 4}))
		if err != nil {
			t.Fatal(err)
		}
		for sid := int32(1); sid <= 2; sid++ {
			compareNumeric(t, Numeric{test.expected[sid-1], Valid}, frame.Values["rank"][sid], "external rank")
		}
		compareNumeric(t, Numeric{test.firstReferenceRank, Valid}, frame.Values["rank"][3], "reference rank preserved")
		if frame.Values["group"][1].Validity != Missing || frame.Values["group"][2].Validity != Null {
			t.Fatalf("group availability: %v", frame.Values["group"])
		}
	}
	values := map[int32]map[string]any{1: {"close": 10.0}, 2: {"close": nil}, 3: {"close": nil}, 4: {"close": math.NaN()}}
	session, _ := NewSession(plan)
	frame, err := session.Evaluate(referencePoolSnapshot(t, 1000, values, []int32{3, 4}))
	if err != nil {
		t.Fatal(err)
	}
	if frame.Values["rank"][1].Validity != Missing || frame.Values["rank"][2].Validity != Null {
		t.Fatalf("empty reference fit: %v", frame.Values["rank"])
	}
}

func TestDisjointReferenceTSCSBatchParitySharingAndBoundedState(t *testing.T) {
	x := ZScore(Field("prices", "close", "1h"))
	plan, err := New().Add("first", EMA(x, 2)).Add("second", EMA(x, 2)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if plan.NodeCount() != 3 {
		t.Fatalf("shared node count %d", plan.NodeCount())
	}
	snapshots := make([]*Snapshot, 80)
	for i := range snapshots {
		snapshots[i] = referencePoolSnapshot(t, int64(i+1)*1000, map[int32]map[string]any{1: {"close": 10.0}, 2: {"close": 20.0}, 3: {"close": 30.0}, 4: {"close": 40.0}}, []int32{3, 4})
	}
	batch, err := plan.Batch(snapshots, len(snapshots))
	if err != nil {
		t.Fatal(err)
	}
	session, _ := NewSession(plan)
	for i, snapshot := range snapshots {
		frame, err := session.Evaluate(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		for sid := int32(1); sid <= 4; sid++ {
			compareNumeric(t, batch[i].Values["first"][sid], frame.Values["first"][sid], "disjoint cached/tav")
		}
		before := session.Updates()
		if _, err := session.Evaluate(snapshot); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, session.Updates()) {
			t.Fatal("consumer recomputed shared state")
		}
		// Count banta's registered intermediate series as well as DAG roots.
		registered := 0
		for _, asset := range session.assets {
			registered += len(asset.env.Items)
		}
		if registered > 4*2*plan.NodeCount() || session.RetainedValues() > registered*2*plan.StateRetention() {
			t.Fatalf("unbounded state: %d series, %d values", registered, session.RetainedValues())
		}
		if i >= 1 {
			compareNumeric(t, Numeric{-5, Valid}, frame.Values["first"][1], "TS-CS-TS outside reference")
		}
	}
	for _, node := range plan.nodes {
		want := uint64(len(snapshots))
		if node.spec.Kind == TS {
			want *= 4
		}
		if session.Updates()[node.id] != want {
			t.Fatalf("shared %s updates=%d expected=%d", node.spec.Operator, session.Updates()[node.id], want)
		}
	}
}

func TestReferenceTransformVersionAndCanonicalSharedIdentity(t *testing.T) {
	x := Field("prices", "close", "1h")
	z := ZScore(x)
	group := GroupZScore(x, "prices", "sector")
	if x.Spec.Version != "builtin-1/banta-0.4.1" || z.Spec.Version != "builtin-2/banta-0.4.1" || group.Spec.Version != z.Spec.Version {
		t.Fatal("changed transforms or unchanged TS version not declared")
	}
	plan, err := New().Add("first", z).Add("second", ZScore(Field("prices", "close", "1h"))).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if plan.NodeCount() != 2 {
		t.Fatal("new reference contract lost canonical sharing")
	}
	old := ZScore(Field("prices", "close", "1h"))
	old.Spec.Version = "builtin-1/banta-0.4.1"
	oldPlan, err := New().Add("first", old).Add("second", old).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if plan.Hash() == oldPlan.Hash() {
		t.Fatal("reference output semantics absent from plan identity")
	}
}
