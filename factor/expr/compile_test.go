package expr

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
)

func specFor(source string) Spec {
	return Spec{SchemaVersion: 1, TimeFrame: "1h", Bindings: map[string]Binding{"kline": {Source: "prices", TimeFrame: "1h"}}, Outputs: map[string]string{"score": source}}
}
func mustCompile(t testing.TB, spec Spec) *factor.Plan {
	t.Helper()
	p, err := Compile(spec)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func snapshot(t testing.TB, event int64, values map[int32]map[string]any) *factor.Snapshot {
	t.Helper()
	sids := make([]int32, 0, len(values))
	sidMap := map[int32]string{}
	var rows []factor.VersionRecord
	var requirements []factor.Requirement
	for sid, fields := range values {
		sids = append(sids, sid)
		sidMap[sid] = string(rune('A' + sid))
		rows = append(rows, factor.Record(orm.DataSeries{Source: "prices", Sid: sid, TimeMS: event - 1, EndMS: event, Closed: true, TimeFrame: "1h", Values: fields}, 1, event, event, "prices-v1"))
		requirements = append(requirements, factor.Requirement{SID: sid, Source: "prices", TimeFrame: "1h", EventTime: event})
	}
	s, err := factor.Freeze(factor.SnapshotSpec{DecisionTime: event, ReplayTime: event, Universe: factor.Universe{Version: "u1", Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Static: true}, SIDMap: sidMap, Schemas: map[string]string{"prices": "s1"}, SourceVersions: map[string]string{"prices": "prices-v1"}, VisibilityPolicy: "published-and-received"}, rows, requirements)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestArithmeticHandCalculation(t *testing.T) {
	s := snapshot(t, 1000, map[int32]map[string]any{1: {"close": 4.0, "some-key": 9.0}})
	for formula, want := range map[string]float64{"1 + 2 * 3": 7, "8 / 4 / 2": 1, "-(kline.close + 2)": -6, "1e-2 + .5": .51, "sqrt(field(\"kline\",\"some-key\"))": 3, "max(abs(-kline.close), 2)": 4, "pow(kline.close,2)": 16, "log(1)": 0, "positive(kline.close)": 4, "ts.lag(kline.close,0)": 4} {
		t.Run(formula, func(t *testing.T) {
			p := mustCompile(t, specFor(formula))
			session, err := factor.NewSession(p)
			if err != nil {
				t.Fatal(err)
			}
			frame, err := session.Evaluate(s)
			if err != nil {
				t.Fatal(err)
			}
			got := frame.Values["score"][1]
			if got.Validity != factor.Valid || math.Abs(got.Value-want) > 1e-12 {
				t.Fatalf("got %+v want %g", got, want)
			}
		})
	}
}

func TestForwardReferencesGoHashAndCSE(t *testing.T) {
	spec := specFor("cs.zscore(factor.mom) - 0.5 * cs.zscore(factor.vol)")
	spec.Params = map[string]float64{"window": 3}
	spec.Lets = map[string]string{"mom": "ts.return(factor.price,param.window)", "price": "kline.close", "vol": "ts.std(ts.return(factor.price,1),param.window,0)", "unused": "kline.open"}
	spec.Outputs["momentum"] = "factor.mom"
	dsl := mustCompile(t, spec)
	price := factor.Field("prices", "close", "1h")
	mom := factor.Return(price, 3)
	vol := factor.StdDev(factor.Return(price, 1), 3, 0)
	goPlan, err := factor.New().Add("momentum", mom).Add("score", factor.Sub(factor.ZScore(mom), factor.Mul(factor.Constant(.5, "1h"), factor.ZScore(vol)))).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if dsl.Hash() != goPlan.Hash() || dsl.NodeCount() != goPlan.NodeCount() {
		t.Fatalf("DSL/Go plans differ: %s/%s (%d/%d)", dsl.Hash(), goPlan.Hash(), dsl.NodeCount(), goPlan.NodeCount())
	}
	if !reflect.DeepEqual(dsl.Inputs()[0].Fields, []string{"close"}) {
		t.Fatal("unused let leaked into subscriptions")
	}
	shared := mustCompile(t, Spec{SchemaVersion: 1, TimeFrame: "1h", Bindings: spec.Bindings, Outputs: map[string]string{"a": "ts.return(kline.close,3)", "b": "ts.return(kline.close,3)"}})
	if shared.NodeCount() != 2 {
		t.Fatalf("CSE node count %d", shared.NodeCount())
	}
}

func TestSessionBatchValidityAndTSCSParity(t *testing.T) {
	spec := specFor("cs.rank(ts.return(kline.close,1))")
	spec.Outputs["ema"] = "ts.ema(kline.close,3)"
	spec.Outputs["std"] = "ts.std(kline.close,3,1)"
	spec.Outputs["raw"] = "kline.close + 1"
	p := mustCompile(t, spec)
	var snapshots []*factor.Snapshot
	for i := 0; i < 15; i++ {
		rows := map[int32]map[string]any{1: {"close": float64(i + 1)}, 2: {"close": float64(2*i + 2)}, 3: {"close": float64(3*i + 3)}}
		switch i {
		case 4:
			rows[1]["close"] = nil
		case 6:
			delete(rows[1], "close")
		case 8:
			rows[1]["close"] = "bad"
		case 10:
			rows[1]["close"] = math.Inf(1)
		}
		snapshots = append(snapshots, snapshot(t, int64(i+1)*1000, rows))
	}
	batch, err := p.Batch(snapshots, len(snapshots))
	if err != nil {
		t.Fatal(err)
	}
	session, err := factor.NewSession(p)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range snapshots {
		got, err := session.Evaluate(s)
		if err != nil {
			t.Fatal(err)
		}
		for name, column := range got.Values {
			for sid, v := range column {
				want := batch[i].Values[name][sid]
				if v.Validity != want.Validity || v.Validity == factor.Valid && math.Abs(v.Value-want.Value) > 1e-10 {
					t.Fatalf("%s t=%d sid=%d got %+v want %+v", name, i, sid, v, want)
				}
			}
		}
	}
	for i, want := range map[int]factor.Validity{4: factor.Null, 6: factor.Missing, 8: factor.NotNumeric, 10: factor.NonFinite} {
		if batch[i].Values["raw"][1].Validity != want {
			t.Fatalf("t=%d lost validity", i)
		}
	}
	// Three identical returns tie at their average zero-based rank, independently of
	// the DSL/Go and Session/Batch equivalence comparisons above.
	if got := batch[1].Values["score"][1]; got.Validity != factor.Valid || got.Value != 1 {
		t.Fatalf("tied rank got %+v", got)
	}
}

func TestRejectInvalidExpressionsWithLocation(t *testing.T) {
	for _, source := range []string{"kline.close garbage", "kline.close;", "kline.close // comment", "kline.close[0]", "label.future", "ts.lead(kline.close,1)", "ts.lag(kline.close,-1)", "ts.return(kline.close,0)", "ts.ema(kline.close,1.5)", "ts.ema(kline.close,10001)", "ts.std(kline.close,2,2)", "cs.quantile(kline.close,1.1)", "cs.winsorize(kline.close,.5)", "param.unknown", "factor.unknown", "missing.close", "field(\"kline\",\"\")", "field(kline.close,\"close\")", "max(kline.close)", "pow(kline.close,1,2)", "\"text\"", "1e999", "a.b.c", "ts.ema(kline.close,kline.window)", "1 /*comment*/ + 2", "1 ^ 2", "(1+2", "ts.return(kline.close,)", strings.Repeat("(", 100) + "1" + strings.Repeat(")", 100)} {
		t.Run(source, func(t *testing.T) {
			_, err := Compile(specFor(source))
			if err == nil || !strings.Contains(err.Error(), "outputs.score:") {
				t.Fatalf("expected source location, got %v", err)
			}
		})
	}
}

func TestUnusedDefinitionsAndBudgets(t *testing.T) {
	for _, lets := range []map[string]string{{"unused": "factor.unknown"}, {"a": "factor.b", "b": "factor.a"}, {"unused": "ts.ema(kline.close,-1)"}, {"score": "1"}} {
		spec := specFor("kline.close")
		spec.Lets = lets
		if _, err := Compile(spec); err == nil {
			t.Fatalf("accepted invalid lets: %v", lets)
		}
	}
	spec := specFor(strings.Repeat("1 + ", maxText/4) + "1")
	if _, err := Compile(spec); err == nil {
		t.Fatal("accepted long expression")
	}
	spec = specFor("1")
	spec.SchemaVersion = 2
	if _, err := Compile(spec); err == nil {
		t.Fatal("accepted unknown schema")
	}
	spec = specFor("param.p")
	spec.Params = map[string]float64{"p": math.NaN()}
	if _, err := Compile(spec); err == nil {
		t.Fatal("accepted non-finite param")
	}
	// Deep reference chains must be bounded independently of parser nesting.
	spec = specFor("factor.a0")
	spec.Lets = map[string]string{}
	for i := 0; i < 100; i++ {
		spec.Lets["a"+strconv.Itoa(i)] = "factor.a" + strconv.Itoa(i+1)
	}
	spec.Lets["a100"] = "1"
	if _, err := Compile(spec); err == nil {
		t.Fatal("accepted deep expanded expression")
	}
}

func TestBindingAndAsOfIdentity(t *testing.T) {
	spec := specFor("daily.value + kline.close")
	spec.Bindings["daily"] = Binding{Source: "fundamental", TimeFrame: "event"}
	if _, err := Compile(spec); err == nil {
		t.Fatal("accepted implicit cross-timeframe sampling")
	}
	b := spec.Bindings["daily"]
	b.Sampling = "asof"
	spec.Bindings["daily"] = b
	if _, err := Compile(spec); err == nil {
		t.Fatal("accepted missing max age")
	}
	b.MaxAgeMS = 1000
	spec.Bindings["daily"] = b
	first := mustCompile(t, spec)
	for _, input := range first.Inputs() {
		if input.Source == "fundamental" && (!input.AsOfLatest || input.MaxAge != 1000 || input.TimeFrame != "event") {
			t.Fatalf("wrong projection %+v", input)
		}
	}
	b.MaxAgeMS = 2000
	spec.Bindings["daily"] = b
	if first.Hash() == mustCompile(t, spec).Hash() {
		t.Fatal("age absent from identity")
	}
}

func TestCachedReferencesStillRespectDepth(t *testing.T) {
	spec := specFor("factor.f100")
	spec.Lets = map[string]string{"f000": "1"}
	for i := 1; i <= 100; i++ {
		spec.Lets[fmt.Sprintf("f%03d", i)] = fmt.Sprintf("abs(factor.f%03d)", i-1)
	}
	if _, err := Compile(spec); err == nil || !strings.Contains(err.Error(), "depth") {
		t.Fatalf("cached references bypassed depth: %v", err)
	}
}

func TestASTAndDeclarationBudgets(t *testing.T) {
	spec := specFor("1")
	spec.Lets = map[string]string{}
	for i := 0; i < 200; i++ {
		spec.Lets[fmt.Sprintf("v%d", i)] = strings.Repeat("1+", 31) + "1"
	}
	if _, err := Compile(spec); err == nil || !strings.Contains(err.Error(), "AST") {
		t.Fatalf("AST budget not enforced: %v", err)
	}
	spec = specFor("1")
	spec.Lets = map[string]string{}
	for i := 0; i < maxDefinitions; i++ {
		spec.Lets[fmt.Sprintf("v%d", i)] = "1"
	}
	if _, err := Compile(spec); err == nil || !strings.Contains(err.Error(), "declaration count") {
		t.Fatalf("definition budget not enforced: %v", err)
	}
}

func TestRestrictedWindowDomainsAndDecimalLiterals(t *testing.T) {
	for _, formula := range []string{"ts.ema(cs.rank(kline.close),2)", "ts.return(abs(cs.zscore(kline.close))+1,2)", "ts.std(group.residual(kline.close,kline.open),3,0)", "group.demean(kline.close,kline.sector)", "group.zscore(kline.close,kline.sector)", "0x1p2", "1_000"} {
		if _, err := Compile(specFor(formula)); err == nil {
			t.Fatalf("accepted %q", formula)
		}
	}
	if p := mustCompile(t, specFor("ts.lag(cs.rank(kline.close),0)")); p.NodeCount() != 2 {
		t.Fatal("zero lag identity failed")
	}
}

func FuzzCompile(f *testing.F) {
	for _, s := range []string{"kline.close", "cs.rank(ts.return(kline.close,2))", "1+2*3", "field(\"kline\",\"x\")", "ts.lag(kline.close,-1)", ""} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, source string) {
		if len(source) > maxText+1 {
			return
		}
		_, _ = Compile(specFor(source))
	})
}
