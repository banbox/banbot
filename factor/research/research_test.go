package research

import (
	"bytes"
	"github.com/banbox/banbot/factor"
	"math"
	"reflect"
	"testing"
)

func number(v float64) factor.Numeric { return factor.Numeric{Value: v, Validity: factor.Valid} }
func researchFixture() (factor.Frame, factor.Universe) {
	u := factor.Universe{Version: "static-v1", Static: true}
	f := factor.Frame{SnapshotID: "snapshot", PlanHash: "dag", DecisionTime: 100, Values: map[string]map[int32]factor.Numeric{"momentum": {}, "volatility": {}}}
	for sid := int32(1); sid <= 24; sid++ {
		u.Investable = append(u.Investable, sid)
		u.Reference = append(u.Reference, sid)
		u.Tradable = append(u.Tradable, sid)
		u.Evaluation = append(u.Evaluation, sid)
		f.Values["momentum"][sid] = number(float64(sid))
		f.Values["volatility"][sid] = number(float64(sid) / 2)
	}
	return f, u
}
func TestCombinationExactAndImmatureICFuturePerturbations(t *testing.T) {
	f, u := researchFixture()
	fixed := ComboSpec{Method: Fixed, Columns: []string{"momentum", "volatility"}, Weights: map[string]float64{"momentum": 1, "volatility": -1}}
	scores, _, err := Combine(f, u, fixed, nil)
	if err != nil || scores[24] != number(12) {
		t.Fatalf("fixed combination: %+v %v", scores[24], err)
	}
	equal := fixed
	equal.Method = Equal
	scores, _, err = Combine(f, u, equal, nil)
	if err != nil || scores[24] != number(18) {
		t.Fatalf("equal combination: %+v %v", scores[24], err)
	}
	f.Values["volatility"][12] = factor.Numeric{Validity: factor.Null}
	scores, _, err = Combine(f, u, fixed, nil)
	if err != nil || scores[12].Validity != factor.Null || len(scores) != 24 {
		t.Fatal("missing factor changed inference pool")
	}
	h, err := NewICHistory(2, "forward", fixed.Columns)
	if err != nil {
		t.Fatal(err)
	}
	sample := ICSample{Column: "momentum", Label: "forward", DecisionTime: 80, MatureAt: 110, AvailableAt: 120, IC: number(.6), Samples: 24}
	for _, asof := range []int64{100, 115} {
		if err := h.Add(asof, sample); err == nil {
			t.Fatal("immature/invisible IC entered weights")
		}
	}
	history := fixed
	history.Method = HistoryIC
	before, diag, err := Combine(f, u, history, h)
	if err != nil || len(diag) != 1 || before[24] != number(18) {
		t.Fatal("no label fallback not equal")
	}
	sample.IC = number(-999)
	if err := h.Add(100, sample); err == nil {
		t.Fatal("future perturbation admitted")
	}
	after, _, _ := Combine(f, u, history, h)
	if after[24] != before[24] || len(after) != 24 {
		t.Fatal("future labels leaked into score/universe")
	}
	sample.IC = number(.6)
	if err := h.Add(120, sample); err != nil {
		t.Fatal(err)
	}
	sample.Column = "volatility"
	sample.IC = number(-.2)
	if err := h.Add(120, sample); err != nil {
		t.Fatal(err)
	}
	f.DecisionTime = 120
	scores, diag, err = Combine(f, u, history, h)
	if err != nil || len(diag) != 0 || math.Abs(scores[24].Value-15) > 1e-12 {
		t.Fatalf("mature IC weights: %+v %v", scores[24], err)
	}
	for i := int64(121); i < 150; i++ {
		s := ICSample{Column: "momentum", Label: "forward", DecisionTime: i, MatureAt: i + 1, AvailableAt: i + 1, IC: number(.5), Samples: 24}
		if err := h.Add(i+1, s); err != nil {
			t.Fatal(err)
		}
	}
	if h.Retained() > 4 {
		t.Fatal("unbounded IC history")
	}
	if _, _, err := Combine(f, u, history, h); err == nil {
		t.Fatal("advanced history silently reconstructed an earlier decision")
	}
}
func TestLabelQueueStreamingMaturityTypesAndBounds(t *testing.T) {
	spec := LabelSpec{Name: "forward", Kind: ExecutableReturn, Horizon: 10, Overlapping: true, PeriodsPerYear: 8760}
	q, err := NewLabelQueue([]LabelSpec{spec}, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	features := map[string]factor.Numeric{"momentum": number(1)}
	if err := q.Schedule(spec.Name, 1, 100, 101, features); err != nil {
		t.Fatal(err)
	}
	features["momentum"] = number(999)
	ready, err := q.Drain(110)
	if err != nil || len(ready) != 0 {
		t.Fatal("unresolved future horizon drained")
	}
	label, err := ReturnLabel(spec, 1, 100, 101, 111, 120, number(10), number(12))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Resolve(label); err != nil {
		t.Fatal(err)
	}
	ready, _ = q.Drain(119)
	if len(ready) != 0 {
		t.Fatal("not-visible label drained")
	}
	ready, err = q.Drain(120)
	if err != nil || len(ready) != 1 || ready[0].Factors["momentum"] != number(1) || math.Abs(ready[0].Label.Value.Value-.2) > 1e-12 {
		t.Fatalf("label realized incorrectly: %+v %v", ready, err)
	}
	if ready, err := q.Drain(120); err != nil || len(ready) != 0 {
		t.Fatal("label emitted twice")
	}
	for sid := int32(2); sid <= 3; sid++ {
		if err := q.Schedule(spec.Name, sid, 120, 121, features); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Schedule(spec.Name, 4, 120, 121, features); err == nil {
		t.Fatal("label capacity exceeded silently")
	}
	if _, err := ReturnLabel(spec, 1, 100, 100, 110, 110, number(1), number(2)); err == nil {
		t.Fatal("decision open reused as executable label")
	}
	stats := spec
	stats.Kind = CloseToClose
	if _, err := ReturnLabel(stats, 1, 100, 101, 111, 111, number(1), number(2)); err == nil {
		t.Fatal("statistical and executable label times conflated")
	}
}
func TestDiagnosticsCoverageRankICDecayRiskAndAfterCosts(t *testing.T) {
	f, u := researchFixture()
	f.Values["momentum"][12] = factor.Numeric{Validity: factor.Null}
	definition := LabelSpec{Name: "close", Kind: CloseToClose, Horizon: 10, PeriodsPerYear: 8760}
	var labels []Label
	for sid := int32(1); sid <= 24; sid++ {
		l, err := ReturnLabel(definition, sid, 100, 100, 110, 110, number(100), number(100+float64(sid)))
		if err != nil {
			t.Fatal(err)
		}
		labels = append(labels, l)
	}
	labels[23].AvailableAt = 200
	spec := EvaluationSpec{AsOf: 120, PrimaryLabel: "close", CostRate: .001, CurrentWeights: map[int32]float64{1: -.5, 24: .5}, Exposures: map[string]map[int32]factor.Numeric{"size": f.Values["volatility"]}}
	r, err := Evaluate(f, u, labels, spec)
	if err != nil {
		t.Fatal(err)
	}
	m := r.Columns["momentum"]
	lm := m.Labels["close"]
	if m.Expected != 24 || m.Valid != 23 || m.Missing[factor.Null] != 1 || m.Coverage != 23.0/24 || lm.Pairs != 22 || math.Abs(lm.IC.Value-1) > 1e-12 || math.Abs(lm.RankIC.Value-1) > 1e-12 {
		t.Fatalf("diagnostics: %+v", m)
	}
	if lm.Monotonicity.Validity != factor.Valid || lm.Monotonicity.Value < .99 || m.Decay()["close"].Value != lm.IC.Value || m.RiskExposures["size"].Value < .999 || len(r.Correlations) != 1 {
		t.Fatalf("missing §8 metrics: %+v", r)
	}
	if r.Performance.Net.Validity != factor.Missing {
		t.Fatal("incomplete labels reported complete PnL")
	}
	labels[23].AvailableAt = 110
	r, err = Evaluate(f, u, labels, spec)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(r.Performance.Gross.Value-.115) > 1e-12 || math.Abs(r.Performance.Net.Value-.114) > 1e-12 || r.Performance.OneWayTurnover != .5 || r.Performance.Costs != .001 || r.Performance.Kind != CloseToClose {
		t.Fatalf("hand calculated costs/NAV return: %+v", r.Performance)
	}
	a, err := NewAccumulator([]string{"momentum", "volatility"}, []string{"close"})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Add(r); err != nil {
		t.Fatal(err)
	}
	for i := int64(101); i <= 110; i++ {
		next := r
		next.DecisionTime = i
		if err := a.Add(next); err != nil {
			t.Fatal(err)
		}
	}
	if a.Slots() != 2 || a.Summary()["momentum"]["close"].Sections != 11 {
		t.Fatal("chunked summaries lost/became unbounded")
	}
	if err := a.Add(r); err == nil {
		t.Fatal("duplicate report double counted")
	}
	labels[0].MatureAt = 100
	labels[0].EndAt = 200
	if _, err := Evaluate(f, u, labels, spec); err == nil {
		t.Fatal("future return forged early maturity")
	}
}
func manifestFixture() ManifestSpec {
	return ManifestSpec{Currency: "USDT", CodeRevision: "rev", FactorPlanHash: "dag", UniverseVersion: "u1", VisibilityPolicy: "published-as-of", ExecutionMode: "research", LatencyAssumption: "evaluation only", StaticUniverse: true, Combo: ComboSpec{Method: Fixed, Columns: []string{"momentum", "volatility"}, Weights: map[string]float64{"momentum": 1, "volatility": -1}}, Portfolio: PortfolioDefinition{K: 10, LongNotional: .5, ShortNotional: .5, Mode: factor.Full}, Labels: []LabelSpec{{Name: "forward", Kind: ExecutableReturn, Horizon: 3600000, Overlapping: true, PeriodsPerYear: 8760}}, Parameters: map[string]float64{"window": 24, "ddof": 1}, Costs: CostSpec{FeeRate: .001, SlippageRate: .002, FundingPolicy: "required-real-series"}, Snapshots: []SnapshotReference{{ID: "snapshot", ContentDigest: "digest", Schemas: map[string]string{"kline": "schema"}, SourceVersions: map[string]string{"kline": "v1"}, Revisions: map[string]uint64{"kline": 1}}}}
}
func TestManifestCompositeIdentityImmutabilityAndPanelStreaming(t *testing.T) {
	s := manifestFixture()
	m, err := BuildManifest(s)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := BuildManifest(s)
	if m.ID() != again.ID() || m.StrategyHash() != again.StrategyHash() || len(m.Diagnostics()) != 2 {
		t.Fatal("manifest unstable/bias warnings omitted")
	}
	for _, mode := range []string{"weights", "events", "live"} {
		run := manifestFixture()
		run.ExecutionMode = mode
		run.LatencyAssumption = "next eligible event"
		run.UniverseVersion = "u2"
		other, err := BuildManifest(run)
		if err != nil || other.StrategyHash() != m.StrategyHash() || other.ID() == m.ID() {
			t.Fatalf("mode/universe changed definition identity: %s %v", mode, err)
		}
	}
	unit := manifestFixture()
	unit.Currency = "BTC"
	unitManifest, _ := BuildManifest(unit)
	if unitManifest.StrategyHash() == m.StrategyHash() {
		t.Fatal("settlement unit omitted from identity")
	}
	s.Combo.Weights["momentum"] = 2
	changed, _ := BuildManifest(s)
	if changed.StrategyHash() == m.StrategyHash() {
		t.Fatal("combiner missing from strategy hash")
	}
	s = manifestFixture()
	s.Labels[0].Horizon++
	changed, _ = BuildManifest(s)
	if changed.StrategyHash() == m.StrategyHash() {
		t.Fatal("label definition missing from strategy hash")
	}
	s = manifestFixture()
	s.Snapshots[0].Revisions["kline"] = 2
	changed, _ = BuildManifest(s)
	if changed.ID() == m.ID() || changed.StrategyHash() != m.StrategyHash() {
		t.Fatal("snapshot revision/hash scope incorrect")
	}
	copy := m.Spec()
	copy.Combo.Weights["momentum"] = 999
	copy.Snapshots[0].Schemas["kline"] = "changed"
	if !reflect.DeepEqual(m.Spec(), manifestFixture()) {
		t.Fatal("manifest maps leaked")
	}
	f, u := researchFixture()
	f.Values["momentum"][1] = factor.Numeric{Value: math.NaN(), Validity: factor.Null}
	var panel bytes.Buffer
	if err := WritePanel(&panel, f, []string{"momentum"}, u.Investable[:2]); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(panel.Bytes(), []byte(`"Value":null`)) || !bytes.Contains(panel.Bytes(), []byte(`"Validity":"null"`)) {
		t.Fatal("panel lost null semantics")
	}
	plan, combo, err := DefaultMomentumVolPlan()
	if err != nil || plan == nil || combo.Weights["volatility"] != -1 {
		t.Fatalf("default registered pipeline: %v", err)
	}
	cfg := DefaultMomentumVolConfig()
	cfg.Window = 12
	other, _, err := MomentumVolPlan(cfg)
	if err != nil || other.Hash() == plan.Hash() {
		t.Fatal("configurable pipeline hash unchanged")
	}
}
