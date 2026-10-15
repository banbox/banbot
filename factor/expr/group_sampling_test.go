package expr

import (
	"reflect"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
)

func groupSpec(formula, sampling, timeframe string, age int64) Spec {
	s := specFor(formula)
	s.Bindings["classification"] = Binding{Source: "classification", TimeFrame: timeframe, Sampling: sampling, MaxAgeMS: age}
	return s
}

func groupSnapshot(t *testing.T, grid, classificationTime int64, timeframe string) *factor.Snapshot {
	t.Helper()
	var rows []factor.VersionRecord
	var requirements []factor.Requirement
	for _, sid := range []int32{1, 2} {
		rows = append(rows, factor.Record(orm.DataSeries{Source: "prices", Sid: sid, TimeMS: grid - 1, EndMS: grid, Closed: true, TimeFrame: "1h", Values: map[string]any{"close": float64(sid * 2)}}, 1, grid, grid, "v1"))
		rows = append(rows, factor.Record(orm.DataSeries{Source: "classification", Sid: sid, TimeMS: classificationTime - 1, EndMS: classificationTime, Closed: true, TimeFrame: timeframe, Values: map[string]any{"sector": "technology"}}, 1, classificationTime, classificationTime, "v1"))
		requirements = append(requirements, factor.Requirement{SID: sid, Source: "prices", TimeFrame: "1h", EventTime: grid}, factor.Requirement{SID: sid, Source: "classification", TimeFrame: timeframe, EventTime: grid, AsOfLatest: true})
	}
	s, err := factor.Freeze(factor.SnapshotSpec{GridTime: grid, DecisionTime: grid, ReplayTime: grid, Universe: factor.Universe{Version: "u1", Reference: []int32{1, 2}, Investable: []int32{1, 2}, Static: true}, SIDMap: map[int32]string{1: "A", 2: "B"}, Schemas: map[string]string{"prices": "s1", "classification": "s1"}, SourceVersions: map[string]string{"prices": "v1", "classification": "v1"}, VisibilityPolicy: "published-and-received"}, rows, requirements)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestGroupBindingSamplingContract(t *testing.T) {
	for _, formula := range []string{`group.demean(kline.close,"classification","sector")`, `group.zscore(kline.close,"classification","sector")`} {
		t.Run(formula, func(t *testing.T) {
			p := mustCompile(t, groupSpec(formula, "asof", "event", 100))
			if p.Hash() == mustCompile(t, groupSpec(formula, "asof", "event", 200)).Hash() {
				t.Error("classification max age missing from plan identity")
			}
			if p.NodeCount() != 2 {
				t.Fatal("classification must remain raw, outside numeric dependencies")
			}
			for _, input := range p.Inputs() {
				if input.Source == "classification" && (!input.AsOfLatest || input.MaxAge != 100 || !reflect.DeepEqual(input.Fields, []string{"sector"})) {
					t.Errorf("classification subscription lost sampling: %+v", input)
				}
			}
			for _, age := range []int64{100, 101} {
				s := groupSnapshot(t, 1000, 1000-age, "event")
				session, _ := factor.NewSession(p)
				frame, err := session.Evaluate(s)
				batch, batchErr := p.Batch([]*factor.Snapshot{s}, 1)
				if age > 100 {
					if err == nil || batchErr == nil {
						t.Error("expired classification accepted")
					}
				} else {
					if err != nil || batchErr != nil {
						t.Fatalf("valid string classification rejected: %v / %v", err, batchErr)
					}
					if !reflect.DeepEqual(frame.Values, batch[0].Values) || frame.Values["score"][1].Validity != factor.Valid || frame.Values["score"][1].Value != -1 {
						t.Fatalf("unexpected group results: %+v / %+v", frame.Values, batch[0].Values)
					}
				}
			}
		})
	}
}

func TestGroupSameGridSampling(t *testing.T) {
	formula := `group.demean(kline.close,"classification","sector")`
	native := mustCompile(t, groupSpec(formula, "source-events", "1h", 0))
	asof := mustCompile(t, groupSpec(formula, "asof", "1h", 100))
	if native.Hash() == asof.Hash() {
		t.Error("same-grid sampling missing from group identity")
	}
	for _, input := range native.Inputs() {
		if input.Source == "classification" && input.AsOfLatest {
			t.Error("source-events classification projected as latest")
		}
	}
	s := groupSnapshot(t, 1000, 950, "1h")
	session, _ := factor.NewSession(native)
	if _, err := session.Evaluate(s); err == nil {
		t.Error("source-events classification accepted old row")
	}
	if _, err := native.Batch([]*factor.Snapshot{s}, 1); err == nil {
		t.Error("batch source-events classification accepted old row")
	}
	session, _ = factor.NewSession(asof)
	if _, err := session.Evaluate(s); err != nil {
		t.Fatal(err)
	}
}

func TestGroupGoBuilderCompatibility(t *testing.T) {
	price := factor.Field("prices", "close", "1h")
	classification := factor.AsOfField("classification", "sector", "event", "1h", 100)
	goPlan, err := factor.New().Add("score", factor.GroupDemeanByField(price, classification)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	dsl := mustCompile(t, groupSpec(`group.demean(kline.close,"classification","sector")`, "asof", "event", 100))
	if goPlan.Hash() != dsl.Hash() {
		t.Fatal("Go and DSL classification contracts differ")
	}
	classification.Spec.MaxAge = 1
	if goPlan.Hash() != dsl.Hash() {
		t.Fatal("builder mutation changed compiled classification contract")
	}
	s := groupSnapshot(t, 1000, 1, "event")
	for _, n := range []*factor.Node{factor.GroupDemean(price, "classification", "sector", "event"), factor.GroupZScore(price, "classification", "sector", "event")} {
		legacy, err := factor.New().Add("score", n).Compile()
		if err != nil {
			t.Fatal(err)
		}
		session, _ := factor.NewSession(legacy)
		frame, err := session.Evaluate(s)
		batch, batchErr := legacy.Batch([]*factor.Snapshot{s}, 1)
		if err != nil || batchErr != nil {
			t.Fatalf("legacy unlimited-age latest classification rejected: %v / %v", err, batchErr)
		}
		if frame.Values["score"][1].Validity != factor.Valid || !reflect.DeepEqual(frame.Values, batch[0].Values) {
			t.Fatal("legacy group output changed")
		}
	}
	for _, field := range []*factor.Node{nil, factor.ZScore(classification), factor.Field("classification", "sector", "1d"), factor.AsOfField("classification", "sector", "event", "1h", 0)} {
		if _, err := factor.New().Add("score", factor.GroupDemeanByField(price, field)).Compile(); err == nil {
			t.Fatal("invalid raw classification field declaration accepted")
		}
	}
}

func TestGroupRawClassificationValidity(t *testing.T) {
	p := mustCompile(t, specFor(`group.demean(kline.close,"kline","sector")`))
	s := snapshot(t, 1000, map[int32]map[string]any{
		1: {"close": 2.0, "sector": "technology"},
		2: {"close": 4.0, "sector": "technology"},
		3: {"close": 3.0, "sector": nil},
		4: {"close": 3.0},
	})
	session, _ := factor.NewSession(p)
	frame, err := session.Evaluate(s)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := p.Batch([]*factor.Snapshot{s}, 1)
	if err != nil {
		t.Fatal(err)
	}
	for sid, want := range map[int32]factor.Validity{1: factor.Valid, 2: factor.Valid, 3: factor.Null, 4: factor.Missing} {
		if frame.Values["score"][sid].Validity != want || batch[0].Values["score"][sid].Validity != want {
			t.Fatalf("raw classification validity lost for %d", sid)
		}
	}
}
