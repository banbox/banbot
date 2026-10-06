package research

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestHistoricalRankQualityEWMAAndFallback(t *testing.T) {
	h, _ := NewICHistory(8, "forward", []string{"a", "b"})
	for i := int64(1); i <= 3; i++ {
		for _, name := range []string{"a", "b"} {
			ic, rank := .1, -.1
			if name == "b" {
				ic, rank = -.2, .2
			}
			if err := h.Add(20, ICSample{Column: name, Label: "forward", DecisionTime: i, MatureAt: i + 1, AvailableAt: i + 1, IC: number(ic), RankIC: number(rank), Samples: 10}); err != nil {
				t.Fatal(err)
			}
		}
	}
	w, ok, err := h.QualityWeights(20, ComboSpec{Method: HistoryRankIC, Columns: []string{"a", "b"}, MinSamples: 3, MinPairs: 10})
	if err != nil || !ok || math.Abs(w["a"]+1.0/3) > 1e-10 || math.Abs(w["b"]-2.0/3) > 1e-10 {
		t.Fatalf("%v %v %v", w, ok, err)
	}
	w, ok, err = h.QualityWeights(20, ComboSpec{Method: HistoryRankIC, Columns: []string{"a", "b"}, MinSamples: 3, Direction: "positive"})
	if err != nil || !ok || w["a"] != 0 || w["b"] != 1 {
		t.Fatalf("positive: %v %v", w, err)
	}
	_, ok, err = h.QualityWeights(20, ComboSpec{Method: HistoryRankIC, Columns: []string{"a", "b"}, MinSamples: 4})
	if err != nil || ok {
		t.Fatal("minimum sample gate bypassed")
	}
	if err := h.Add(20, ICSample{Column: "a", Label: "forward", DecisionTime: 19, MatureAt: 21, AvailableAt: 21, IC: number(-999), RankIC: number(999), Samples: 10}); err == nil {
		t.Fatal("future label accepted")
	}
	h2, _ := NewICHistory(8, "forward", []string{"a"})
	for i, value := range []float64{1, -1} {
		if err := h2.Add(10, ICSample{Column: "a", Label: "forward", DecisionTime: int64(i + 1), MatureAt: int64(i + 2), AvailableAt: int64(i + 2), IC: number(value), Samples: 10}); err != nil {
			t.Fatal(err)
		}
	}
	w, ok, err = h2.QualityWeights(10, ComboSpec{Method: HistoryEWMA, Columns: []string{"a"}, Decay: .75})
	if err != nil || !ok || w["a"] != -1 {
		t.Fatalf("EWMA direction: %v %v", w, err)
	}
	frame, u := researchFixture()
	_, _, err = Combine(frame, u, ComboSpec{Method: HistoryIC, Columns: []string{"momentum"}, Fallback: "error"}, nil)
	if err == nil {
		t.Fatal("strict fallback ignored")
	}
}
func TestCovarianceOptimizerHardLimitsAndTurnoverInfeasible(t *testing.T) {
	estimate, err := EstimateCovariance([][]float64{{1, 2}, {2, 4}, {3, 6}}, .5, false)
	if err != nil || math.Abs(estimate.Matrix[0][1]-1) > 1e-12 || estimate.Matrix[1][1] != 4 {
		t.Fatalf("%+v %v", estimate, err)
	}
	r, err := OptimizePortfolio(OptimizationSpec{ExpectedReturns: []float64{1, .5}, Covariance: [][]float64{{1, 0}, {0, 1}}, RiskAversion: 1, Constraints: RiskConstraints{Gross: 1, MaxWeight: .4, LongOnly: true, Groups: []string{"a", "b"}, GroupCaps: map[string]float64{"a": .3}}})
	if err != nil || r.Status != "feasible" || r.Weights[0] > .3+1e-9 || r.Weights[1] > .4+1e-9 {
		t.Fatalf("%+v %v", r, err)
	}
	r, err = OptimizePortfolio(OptimizationSpec{ExpectedReturns: []float64{1}, Previous: []float64{2}, Covariance: [][]float64{{1}}, RiskAversion: 1, Constraints: RiskConstraints{Gross: 1, MaxWeight: 1, LimitTurnover: true, TurnoverBudget: 0}})
	if err != nil || r.Status != "infeasible" || r.Violations["gross"] != 1 || r.OneWayTurnover != 0 {
		t.Fatalf("must preserve hard turnover: %+v %v", r, err)
	}
	if _, err := OptimizePortfolio(OptimizationSpec{ExpectedReturns: []float64{1, 1}, Covariance: [][]float64{{1, 2}, {2, 1}}, RiskAversion: 1, Constraints: RiskConstraints{Gross: 1}}); err == nil {
		t.Fatal("indefinite risk matrix accepted")
	}
}
func modelFixture() ([]TrainingSample, TrainingWindow) {
	window := TrainingWindow{Start: 1, End: 10, FitAsOf: 10, TestStart: 15, TestEnd: 20, EmbargoMS: 2, MinSamples: 3}
	var rows []TrainingSample
	for i := int64(1); i <= 4; i++ {
		rows = append(rows, TrainingSample{SID: 1, DecisionTime: i, FeaturesAvailableAt: i, LabelEnd: i + 1, MatureAt: i + 1, AvailableAt: i + 1, Features: []float64{float64(i)}, Label: 2 + 3*float64(i)})
	}
	rows = append(rows, TrainingSample{SID: 1, DecisionTime: 8, FeaturesAvailableAt: 8, LabelEnd: 16, MatureAt: 16, AvailableAt: 16, Features: []float64{999}, Label: 999})
	return rows, window
}
func TestModelPITPurgeFuturePerturbationAndPublishRecovery(t *testing.T) {
	rows, window := modelFixture()
	artifact, predictor, err := FitModel("linear-ridge-v1", "manifest", []string{"x"}, nil, rows, window, 11)
	if err != nil {
		t.Fatal(err)
	}
	value, err := predictor.Predict([]float64{5})
	if err != nil || math.Abs(value-17) > 1e-10 {
		t.Fatalf("%v %v", value, err)
	}
	rows[len(rows)-1].Label = math.NaN()
	rows[len(rows)-1].Features = []float64{-1e9}
	changed, _, err := FitModel("linear-ridge-v1", "manifest", []string{"x"}, nil, rows, window, 11)
	if err != nil || changed.ID != artifact.ID {
		t.Fatalf("future changed past model: %v", err)
	}
	if _, err := RestoreModel(artifact, 10); err == nil {
		t.Fatal("unpublished model loaded")
	}
	directory := t.TempDir()
	path, err := PublishModel(directory, artifact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = PublishModel(directory, artifact); err != nil {
		t.Fatal("retry:", err)
	}
	_, restored, err := LoadModel(path, 15)
	if err != nil {
		t.Fatal(err)
	}
	actual, _ := restored.Predict([]float64{5})
	if actual != value {
		t.Fatal("restore changed prediction")
	}
	artifact.Payload = json.RawMessage(`{"Coefficients":[99,99]}`)
	if _, err := RestoreModel(artifact, 15); err == nil {
		t.Fatal("payload corruption accepted")
	}
	windows, err := RollingWindows(1, 100, 20, 10, 10, 2, 3)
	if err != nil || len(windows) != 7 || windows[0].TestStart != 23 || windows[0].End != 20 {
		t.Fatalf("%+v %v", windows, err)
	}
}
func TestLifecycleCapacityAttributionAndHAC(t *testing.T) {
	r, err := SummarizeLifecycles([]TradeLifecycle{{SID: 1, Group: "g", BatchID: "b", EntryAt: 1, ExitRequestedAt: 9, ExitAt: 11, GrossReturn: .1, Fees: .01, Funding: -.005}, {SID: 1, Group: "g", EntryAt: 11, ExitRequestedAt: 20, ExitAt: 23, GrossReturn: -.1, Fees: .01}})
	if err != nil || math.Abs(r.Total.Net+.015) > 1e-12 || r.Total.MeanHoldingMS != 11 || r.Total.MeanExitDelayMS != 2.5 || r.Total.MaxDrawdown < .109999 {
		t.Fatalf("%+v %v", r, err)
	}
	cost, err := EstimateCapacityCost(CapacityCostSpec{FeeRate: .01, ImpactCoefficient: 1, MaxParticipation: .05}, 100, 1000, .1, -100, .01)
	if err != nil || cost.Feasible || cost.Funding != -1 || math.Abs(cost.Impact-math.Sqrt(.1)*10) > 1e-12 {
		t.Fatalf("%+v %v", cost, err)
	}
	a, err := Attribute(AttributionInput{PortfolioGross: .2, BenchmarkReturn: .1, Selection: .05, Transition: .02, Sizing: .03, ExecutionCosts: .01})
	if err != nil || math.Abs(a.Excess-.09) > 1e-12 || math.Abs(a.Residual) > 1e-12 {
		t.Fatalf("%+v %v", a, err)
	}
	mean, se, err := HACMean([]float64{1, 2, 3, 4}, 1)
	if err != nil || mean != 2.5 || se <= 0 {
		t.Fatalf("%v %v %v", mean, se, err)
	}
	capital, err := SummarizeCapital([]CapitalSample{{DecisionTime: 1, NAV: 100, GrossNotional: 80, Cash: 20, ExternalTradedNotional: 0, TargetDeltaNotional: 10}, {DecisionTime: 2, NAV: 100, GrossNotional: 60, Cash: 40, ExternalTradedNotional: 20, TargetDeltaNotional: 20}})
	if err != nil || math.Abs(capital.MeanUtilization-.7) > 1e-12 || math.Abs(capital.MeanCashRatio-.3) > 1e-12 || capital.ExternalTradedNotional != 20 || capital.TargetDeltaNotional != 30 || math.Abs(capital.MeanOneWayTargetTurnover-.075) > 1e-12 {
		t.Fatalf("%+v %v", capital, err)
	}
}
func TestTrialLedgerRecoveryCacheAndParameterPIT(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trials.jsonl")
	ledger, err := OpenTrialLedger(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	trial := Trial{Name: "rotation", ManifestID: "data", AlgorithmVersion: "grid-v1", TrainStart: 1, TrainEnd: 10, SampleCutoff: 10, AvailableAt: 11, TestStart: 12, TestEnd: 20, Scans: 8, Metrics: map[string]float64{"net": .2}, OutOfSample: map[string]float64{"net": .1}, Baseline: "seeded-random-v1", Seed: 42}
	id, err := ledger.Append(trial)
	if err != nil {
		t.Fatal(err)
	}
	again, err := ledger.Append(trial)
	if err != nil || again != id || len(ledger.Trials()) != 1 {
		t.Fatal("non-idempotent trial")
	}
	restored, err := OpenTrialLedger(path, 2)
	if err != nil || len(restored.Trials()) != 1 {
		t.Fatal("ledger restore:", err)
	}
	comparison, err := CompareTrials(restored.Trials(), "net")
	if err != nil || len(comparison) != 1 || comparison[0].OutOfSample != .1 || !comparison[0].HasOutOfSample {
		t.Fatalf("%+v %v", comparison, err)
	}
	file, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	_, _ = file.WriteString("{")
	_ = file.Close()
	if _, err := OpenTrialLedger(path, 2); err == nil {
		t.Fatal("torn trial discarded")
	}
	cache, _ := NewResearchCache(140)
	key := CacheIdentity{ManifestID: "m", AlgorithmVersion: "v", SchemaVersion: "s", UniverseVersion: "u", SourceRevision: "r", Window: "w"}
	value := []byte("hello")
	if err := cache.Put(key, value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'x'
	out, ok := cache.Get(key)
	if !ok || string(out) != "hello" {
		t.Fatal("cache aliases input")
	}
	out[0] = 'x'
	next := key
	next.SourceRevision = "r2"
	if err := cache.Put(next, make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.Get(key); ok {
		t.Fatal("LRU did not evict")
	}
	if cache.Bytes() > 140 {
		t.Fatal("cache exceeds byte budget")
	}
	var observations []ParameterObservation
	for i := int64(1); i <= 4; i++ {
		for _, candidate := range []string{"2h", "4h"} {
			value := .1
			if candidate == "4h" {
				value = .2
			}
			observations = append(observations, ParameterObservation{SID: 1, Group: "liquid", Candidate: candidate, BeginAt: i * 2, EndAt: i*2 + 1, AvailableAt: i*2 + 1, NetReturn: value})
		}
	}
	spec := ParameterSelectionSpec{TrainStart: 1, TrainEnd: 10, AvailableAt: 11, MinIndependentSamples: 3, PriorSamples: 5, ManifestID: "m", AlgorithmVersion: "shrink-v1", Candidates: map[string]map[string]float64{"2h": {"min_bars": 2}, "4h": {"min_bars": 4}, "999h": {"min_bars": 999}}}
	artifact, err := SelectParameters(observations, spec)
	if err != nil || artifact.Global.Candidate != "4h" || artifact.ByAsset[1].Candidate != "4h" || artifact.Scans != 2 {
		t.Fatalf("%+v %v", artifact, err)
	}
	future := append(slicesCloneObservations(observations), ParameterObservation{SID: 2, Candidate: "999h", BeginAt: 20, EndAt: 21, AvailableAt: 21, NetReturn: 1e9})
	same, err := SelectParameters(future, spec)
	if err != nil || !reflect.DeepEqual(same, artifact) {
		t.Fatal("future changed training parameters")
	}
	if _, err := ResolveParameters([]ParameterArtifact{artifact}, 10, "m"); err == nil {
		t.Fatal("future parameter artifact visible")
	}
	if _, err := ResolveParameters([]ParameterArtifact{artifact}, 12, "m"); err != nil {
		t.Fatal(err)
	}
	spec.Candidates["4h"]["min_bars"] = 999
	if artifact.Candidates["4h"]["min_bars"] != 4 || len(artifact.Candidates) != 2 {
		t.Fatal("candidate parameters alias input or include future unseen candidate")
	}
}
func slicesCloneObservations(rows []ParameterObservation) []ParameterObservation {
	return append([]ParameterObservation(nil), rows...)
}
func TestAccumulatorIndependentHorizonChronology(t *testing.T) {
	a, _ := NewAccumulator([]string{"score"}, []string{"short", "long"})
	report := func(at int64, label string) Report {
		return Report{DecisionTime: at, Columns: map[string]ColumnMetrics{"score": {Labels: map[string]LabelMetrics{label: {IC: number(.5), RankIC: number(.5)}}}}}
	}
	for _, r := range []Report{report(100, "short"), report(200, "short"), report(100, "long"), report(200, "long")} {
		if err := a.Add(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Add(report(100, "long")); err == nil {
		t.Fatal("duplicate horizon accepted")
	}
	if a.Summary()["score"]["long"].Sections != 2 {
		t.Fatal("duplicate changed accumulator")
	}
}
func TestFactorMetadataRegistryReturnsOwnedCopy(t *testing.T) {
	var registry FactorRegistry
	metadata := FactorMetadata{Name: "momentum", Version: "v1", Direction: "positive", Sources: []string{"prices"}, Tags: []string{"technical"}}
	if err := registry.Register(metadata); err != nil {
		t.Fatal(err)
	}
	metadata.Sources[0] = "mutated"
	snapshot := registry.Snapshot()
	snapshot[0].Tags[0] = "mutated"
	actual := registry.Snapshot()
	if actual[0].Sources[0] != "prices" || actual[0].Tags[0] != "technical" {
		t.Fatal("metadata aliases caller")
	}
	if err := registry.Register(metadata); err == nil {
		t.Fatal("duplicate metadata accepted")
	}
}
func TestSeededRandomBaselineUsesOnlyFrozenPool(t *testing.T) {
	first, err := RandomBaselineScores([]int32{1, 2, 3, 4, 5}, 42)
	if err != nil {
		t.Fatal(err)
	}
	same, err := RandomBaselineScores([]int32{5, 4, 3, 2, 1, 1}, 42)
	if err != nil || !reflect.DeepEqual(first, same) {
		t.Fatal("baseline depends on pool order")
	}
	changed, _ := RandomBaselineScores([]int32{1, 2, 3, 4, 5}, 43)
	if reflect.DeepEqual(first, changed) {
		t.Fatal("baseline seed ignored")
	}
	if len(first) != 5 {
		t.Fatal("baseline leaked undeclared members")
	}
}
