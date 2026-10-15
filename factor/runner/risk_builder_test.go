package runner

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
)

var riskTestSequence atomic.Int64

func TestRiskBuilderCovariancePublicationAndImmutableRegistration(t *testing.T) {
	name := fmt.Sprintf("risk-test-%d", riskTestSequence.Add(1))
	config := RiskBuilderConfig{RiskAversion: 1, Gross: 1, MaxWeight: .4, LongOnly: true, SIDs: []int32{1, 2}, Covariance: [][]float64{{1, 0}, {0, 1}}, TrainingEnd: 10, AvailableAt: 12}
	if err := RegisterRiskPortfolioBuilder(name, config); err != nil {
		t.Fatal(err)
	}
	config.Covariance[0][0] = -99
	build, ok := portfolioBuilder(name)
	if !ok {
		t.Fatal("builder not registered")
	}
	universe := factor.Universe{Version: "u", Investable: []int32{1, 2}, Tradable: []int32{1, 2}}
	frame := factor.Frame{SnapshotID: "s", PlanHash: "p", DecisionTime: 11, Values: map[string]map[int32]factor.Numeric{"score": {1: {Value: 1, Validity: factor.Valid}, 2: {Value: .5, Validity: factor.Valid}}}}
	spec := factor.PortfolioSpec{StrategyID: "strat", AccountID: "account", DecisionTime: 11, ExecutableAt: 12, ExpireAt: 13, PlanSequence: 1, SnapshotID: "s", PlanHash: "strategy", FactorPlanHash: "p", UniverseVersion: "u", Budget: factor.FrozenBudget{Version: "nav", Currency: "USDT", NAV: 1000}, Mode: factor.Full}
	if _, _, err := build(frame, universe, spec, research.PortfolioDefinition{}); err == nil {
		t.Fatal("unpublished covariance used")
	}
	frame.DecisionTime = 12
	spec.DecisionTime = 12
	spec.ExecutableAt = 13
	spec.ExpireAt = 14
	target, diagnostics, err := build(frame, universe, spec, research.PortfolioDefinition{})
	if err != nil || target == nil || len(diagnostics) != 1 || target.Targets()[1] > .400000001 {
		t.Fatalf("%v %+v %v", target, diagnostics, err)
	}
}
func TestModelBuilderMaturityFutureArtifactAndPredictions(t *testing.T) {
	window := research.TrainingWindow{Start: 1, End: 8, FitAsOf: 8, TestStart: 12, TestEnd: 30, MinSamples: 3}
	var samples []research.TrainingSample
	for i := int64(1); i <= 3; i++ {
		samples = append(samples, research.TrainingSample{SID: 1, DecisionTime: i, FeaturesAvailableAt: i, LabelEnd: i + 1, MatureAt: i + 1, AvailableAt: i + 1, Features: []float64{float64(i)}, Label: -float64(i)})
	}
	artifact, _, err := research.FitModel("linear-ridge-v1", "training", []string{"x"}, nil, samples, window, 10)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("model-test-%d", riskTestSequence.Add(1))
	if err := RegisterModelPortfolioBuilder(name, ModelBuilderConfig{ManifestID: "training", Artifacts: []research.ModelArtifact{artifact}}); err != nil {
		t.Fatal(err)
	}
	build, _ := portfolioBuilder(name)
	u := factor.Universe{Version: "u", Investable: []int32{1, 2}, Tradable: []int32{1, 2}}
	frame := factor.Frame{SnapshotID: "s", PlanHash: "p", DecisionTime: 9, Values: map[string]map[int32]factor.Numeric{"x": {1: {Value: 1, Validity: factor.Valid}, 2: {Value: 2, Validity: factor.Valid}}}}
	spec := factor.PortfolioSpec{StrategyID: "strat", AccountID: "account", DecisionTime: 9, ExecutableAt: 10, ExpireAt: 11, PlanSequence: 1, SnapshotID: "s", PlanHash: "strategy", FactorPlanHash: "p", UniverseVersion: "u", Budget: factor.FrozenBudget{Version: "nav", Currency: "USDT", NAV: 1000}, Mode: factor.Full}
	definition := research.PortfolioDefinition{K: 1, LongNotional: .5, ShortNotional: .5}
	target, diag, err := build(frame, u, spec, definition)
	if err != nil || target != nil || len(diag) != 1 {
		t.Fatalf("unpublished model: %v %v %v", target, diag, err)
	}
	frame.DecisionTime = 10
	spec.DecisionTime = 10
	spec.ExecutableAt = 11
	spec.ExpireAt = 12
	target, diag, err = build(frame, u, spec, definition)
	if err != nil || target == nil || len(diag) != 1 || target.Targets()[1] != .5 || target.Targets()[2] != -.5 {
		t.Fatalf("model ranking: %v %v %v", target, diag, err)
	}
}
func TestRegisteredBuilderConfigurationEntersStrategyHash(t *testing.T) {
	name := fmt.Sprintf("identity-test-%d", riskTestSequence.Add(1))
	if err := RegisterRiskPortfolioBuilder(name, RiskBuilderConfig{RiskAversion: 1, Gross: 1, VolatilityColumn: "volatility"}); err != nil {
		t.Fatal(err)
	}
	c := archiveConfig(t, false)
	c.Manifest.Portfolio.Builder = name
	resolved, err := resolveDecisionPortfolio(c)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Manifest.Portfolio.BuilderConfigHash == "" || resolved.Manifest.Portfolio.BuilderConfigHash != portfolioBuilderIdentity(name) {
		t.Fatal("registered config omitted from portfolio identity")
	}
	plan, combo, err := CompileDefinition(resolved)
	if err != nil {
		t.Fatal(err)
	}
	spec := decisionManifestSpec(resolved, plan, combo)
	spec.ExecutionMode = "weights"
	spec.LatencyAssumption = "test"
	first, err := research.BuildManifest(spec)
	if err != nil {
		t.Fatal(err)
	}
	clone := research.CloneManifestSpec(spec)
	clone.Portfolio.BuilderConfigHash = "changed-configuration"
	changed, err := research.BuildManifest(clone)
	if err != nil {
		t.Fatal(err)
	}
	if first.StrategyHash() == changed.StrategyHash() {
		t.Fatal("native builder config does not affect StrategyHash")
	}
	if err := ValidateLiveConfig(resolved); err != nil {
		t.Fatal("live identity preflight:", err)
	}
	c.Manifest.Portfolio.BuilderConfigHash = "conflict"
	if err := ValidateLiveConfig(c); err == nil {
		t.Fatal("forged registered identity accepted")
	}
}
